"""Local copies of string values, the fetch tokens that guard them, and their byte-bounded LRU."""

import threading
import time
from collections import OrderedDict
from functools import partial
from typing import TYPE_CHECKING, Any, Final, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

# Bytes an entry counts on top of its key and value.
ENTRY_OVERHEAD: Final = 100

# The clock of every tracking time. CLOCK_MONOTONIC stops during a suspend, so copies would look fresh after it.
# CLOCK_BOOTTIME, on Linux, keeps counting.
clock: Final[Callable[[], float]] = (
    partial(time.clock_gettime, time.CLOCK_BOOTTIME) if hasattr(time, "CLOCK_BOOTTIME") else time.monotonic
)


class Copy(NamedTuple):
    """A value read from the primary; ``value`` None marks a key the server did not have."""

    value: bytes | None
    # clock() at which the key expires on the server at the earliest, None without a TTL.
    deadline: float | None
    size: int


class LocalStore:
    """The copies of one tracker, guarded by a lock that is never held during IO."""

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        self._lock = threading.Lock()
        self._copies: OrderedDict[str, Copy] = OrderedDict()
        self._pending: dict[str, object] = {}
        self._bytes = 0
        # Between the +OK to CLIENT TRACKING and the loss of that connection.
        self._active = False
        self._hits = 0
        self._misses = 0
        self._invalidations = 0
        self._flushes = 0

    def activate(self) -> None:
        with self._lock:
            self._active = True

    def deactivate(self) -> None:
        """Drop everything and refuse new fetches until the next :meth:`activate`."""
        with self._lock:
            if self._active:
                self._active = False
                self._clear()

    def flush(self) -> None:
        with self._lock:
            self._clear()

    def invalidate(self, keys: Iterable[str]) -> None:
        """Drop the copies and pending fetches of the keys a push named."""
        with self._lock:
            for key in keys:
                self._drop(key)
                self._invalidations += 1

    def discard(self, keys: Iterable[str]) -> None:
        """Drop the copies and pending fetches of keys this process wrote."""
        with self._lock:
            for key in keys:
                self._drop(key)

    def lookup(self, key: str, now: float) -> Copy | None:
        """Return the live copy of ``key`` and mark it recently used; None if there is none."""
        with self._lock:
            copy = self._copies.get(key)
            if copy is None:
                return None
            if copy.deadline is not None and copy.deadline <= now:
                del self._copies[key]
                self._bytes -= copy.size
                return None
            self._copies.move_to_end(key)
            self._hits += 1
            return copy

    def note_miss(self) -> None:
        with self._lock:
            self._misses += 1

    def begin_fetch(self, key: str) -> object | None:
        """Count a miss, and return the token a fetch of ``key`` passes to :meth:`absorb`; None while inactive.

        Concurrent fetches of a key share one token, and the first to return stores.
        """
        with self._lock:
            self._misses += 1
            if not self._active:
                return None
            return self._pending.setdefault(key, object())

    def forget(self, key: str, token: object | None) -> None:
        """Withdraw the token of a fetch that raised."""
        if token is None:
            return
        with self._lock:
            if self._pending.get(key) is token:
                del self._pending[key]

    def absorb(self, key: str, token: object | None, value: bytes | None, pttl: int, started: float) -> None:
        """Store what a fetch read, unless a push or a write came since its token was issued.

        ``pttl`` is the PTTL reply read after the value, ``started`` the
        :data:`clock` time before the fetch was sent.
        """
        if token is None:
            return
        with self._lock:
            if self._pending.get(key) is not token:
                return
            del self._pending[key]
            if value is None:
                # Only an absent key is stored as absent. A nil with a TTL is a key written
                # between the two commands, or on the MGET path a key of another type.
                if pttl != -2:
                    return
                deadline = None
                size = len(key) + ENTRY_OVERHEAD
            else:
                if pttl < -1:
                    return
                deadline = None if pttl == -1 else started + pttl / 1000
                size = len(value) + len(key) + ENTRY_OVERHEAD
            if size > self.max_bytes // 8:
                return
            old = self._copies.pop(key, None)
            if old is not None:
                self._bytes -= old.size
            self._copies[key] = Copy(value, deadline, size)
            self._bytes += size
            while self._bytes > self.max_bytes:
                _, evicted = self._copies.popitem(last=False)
                self._bytes -= evicted.size

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "entries": len(self._copies),
                "bytes": self._bytes,
                "max_bytes": self.max_bytes,
                "hits": self._hits,
                "misses": self._misses,
                "invalidations": self._invalidations,
                "flushes": self._flushes,
            }

    def _drop(self, key: str) -> None:
        copy = self._copies.pop(key, None)
        if copy is not None:
            self._bytes -= copy.size
        self._pending.pop(key, None)

    def _clear(self) -> None:
        self._copies.clear()
        self._pending.clear()
        self._bytes = 0
        self._flushes += 1
