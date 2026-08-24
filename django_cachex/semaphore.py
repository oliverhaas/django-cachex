"""Weighted semaphores for django-cachex.

One class per backend, each exposing paired sync/async methods to match the
rest of the project's convention (``foo``/``afoo``):

- :class:`Semaphore`: in-process, used by ``LocMemCache``. State lives in a
  ``_SemaphoreRegistry`` shared by every cache with the same LOCATION;
  standalone instances share a process-wide registry.
- :class:`RespSemaphore`: backed by Lua scripts dispatched through any
  ``RespAdapterProtocol`` (redis-py, valkey-py, valkey-glide).
  Constructed by ``cache.semaphore(...)`` / ``cache.asemaphore(...)`` and not
  exposed at the package root.

Acquisition is non-reentrant: one instance holds at most one claim at a time.
Re-acquiring before releasing raises :class:`SemaphoreError`. Create a new
instance per acquire/release lifecycle, the same way :class:`Lock` is used.
"""

import asyncio
import contextlib
import logging
import secrets
import sys
import threading
import time
import warnings
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Self
from weakref import WeakValueDictionary

from django_cachex.exceptions import CachexError

if TYPE_CHECKING:
    from types import FrameType, TracebackType

    from django_cachex.adapters.protocols import RespAdapterProtocol

logger = logging.getLogger(__name__)

_PACKAGE_DIR = str(Path(__file__).parent)


class SemaphoreError(CachexError):
    """Raised when a semaphore operation fails."""


class SemaphoreTimeoutError(SemaphoreError):
    """Raised when ``timeout`` elapses before the caller could acquire."""


def _caller_stacklevel() -> int:
    """``stacklevel`` for a warning that should point at the first non-package frame."""
    # A constant can't serve both entry points: ``cache.semaphore(...)`` sits
    # one frame deeper than a direct ``Semaphore(...)``.
    frame: FrameType | None = sys._getframe(1)
    level = 1
    while frame is not None:
        if not str(frame.f_globals.get("__file__", "")).startswith(_PACKAGE_DIR):
            return level
        frame = frame.f_back
        level += 1
    return level


@dataclass
class _LocalState:
    """Shared state for one (registry, name) pair.

    ``waiters`` is an ``OrderedDict`` of ``_Waiter -> weight`` so head lookup,
    append, and remove are all O(1). Insertion order is preserved, giving FIFO.
    """

    capacity: int
    used: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)
    # ``_Waiter`` is defined below; the forward reference resolves without
    # quoting because Python 3.14 defers annotation evaluation (PEP 649), so
    # @dataclass records the field without resolving the name at class creation.
    waiters: OrderedDict[_Waiter, int] = field(default_factory=OrderedDict)


class _SemaphoreRegistry:
    """Per-LOCATION (or process-wide) registry of :class:`_LocalState` by name.

    Replaces the old ``id(cache)``-keyed module-level dict, which could alias
    between a GC'd cache instance and a newly-created one if Python reused
    the address. Registries are keyed by cache LOCATION, so every
    :class:`~django_cachex.cache.LocMemCache` configured for the same
    LOCATION shares one; standalone :class:`Semaphore` instances share the
    module-level ``_DEFAULT_REGISTRY``.

    Entries are held weakly so dynamically-named semaphores
    (``cache.semaphore(f"job:{id}", ...)``) do not grow the registry without
    bound. Every :class:`Semaphore` keeps a strong reference to its state, and
    so does the frame of any thread parked in ``acquire()``, so a name is
    reclaimed only once nothing can still hold or wait on it. Deleting a live
    entry instead is not an option: an instance created before the delete and
    one created after would accumulate against different budgets.
    """

    def __init__(self) -> None:
        self._states: WeakValueDictionary[str, _LocalState] = WeakValueDictionary()
        self._lock = threading.Lock()

    def get_state(self, name: str, capacity: int) -> _LocalState:
        old_capacity: int | None = None
        with self._lock:
            state = self._states.get(name)
            if state is None:
                state = _LocalState(capacity=capacity)
                self._states[name] = state
                return state
            if state.capacity != capacity:
                old_capacity = state.capacity
                with state.lock:
                    state.capacity = capacity
                    if capacity > old_capacity:
                        # A parked waiter may now fit; without this it waits
                        # out its timeout for an unrelated release.
                        _notify_next(state)
        if old_capacity is not None:
            # Warn AFTER releasing the registry lock: a user warning handler
            # that re-enters get_state would otherwise deadlock.
            warnings.warn(
                (
                    f"semaphore {name!r}: capacity changed from {old_capacity} "
                    f"to {capacity}; new value takes effect on next acquire. "
                    f"In-flight claims are not retroactively rejected."
                ),
                RuntimeWarning,
                stacklevel=_caller_stacklevel(),
            )
        return state


_DEFAULT_REGISTRY = _SemaphoreRegistry()


def _notify_next(state: _LocalState) -> None:
    """Wake the head waiter if its weight now fits."""
    # Loop, don't wake once: a waiter whose loop closed while queued can never
    # be woken, and at the head it would block every release() behind it.
    while state.waiters:
        head_waiter = next(iter(state.waiters))
        head_weight = state.waiters[head_waiter]
        if state.used + head_weight > state.capacity:
            return
        if head_waiter.wake():
            return
        del state.waiters[head_waiter]


class _Waiter:
    """Cross-context wake target: either a sync ``threading.Event`` or an
    async ``asyncio.Future`` paired with the loop that owns it.

    ``wake()`` is thread-safe in both directions. For async waiters it routes
    through ``loop.call_soon_threadsafe`` so a sync ``release()`` on another
    thread can correctly hand control back to the loop the waiter is parked on.
    """

    __slots__ = ("event", "future", "loop")

    def __init__(self, *, async_: bool) -> None:
        if async_:
            self.loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
            self.future: asyncio.Future[None] | None = self.loop.create_future()
            self.event: threading.Event | None = None
        else:
            self.loop = None
            self.future = None
            self.event = threading.Event()

    def wait_sync(self, timeout: float | None) -> bool:
        assert self.event is not None  # noqa: S101
        return self.event.wait(timeout=timeout)

    def clear_sync(self) -> None:
        assert self.event is not None  # noqa: S101
        self.event.clear()

    async def wait_async(self, timeout: float | None) -> bool:
        assert self.future is not None  # noqa: S101
        try:
            if timeout is None:
                await self.future
            else:
                await asyncio.wait_for(asyncio.shield(self.future), timeout=timeout)
        except TimeoutError:
            return False
        else:
            return True

    def wake(self) -> bool:
        """Wake this waiter; False means it is unwakeable and should be dropped."""
        if self.event is not None:
            self.event.set()
            return True
        assert self.future is not None  # noqa: S101
        assert self.loop is not None  # noqa: S101
        if self.loop.is_closed():
            return False
        try:
            self.loop.call_soon_threadsafe(self._set_future_result)
        except RuntimeError:
            # The loop closed between the check and the call.
            return False
        return True

    def _set_future_result(self) -> None:
        assert self.future is not None  # noqa: S101
        if not self.future.done():
            self.future.set_result(None)


def _validate_init(capacity: int, weight: int) -> None:
    if capacity <= 0:
        msg = "capacity must be positive"
        raise ValueError(msg)
    if weight <= 0:
        msg = "weight must be positive"
        raise ValueError(msg)
    if weight > capacity:
        msg = f"weight ({weight}) exceeds capacity ({capacity})"
        raise ValueError(msg)


def _decode_status(result: object) -> str:
    """Lua returns bytes in some clients, str in others; coerce to str."""
    if isinstance(result, (list, tuple)) and result:
        first = result[0]
        if isinstance(first, bytes):
            return first.decode("ascii")
        return str(first)
    if isinstance(result, bytes):
        return result.decode("ascii")
    return str(result)


class Semaphore:
    """In-process weighted semaphore.

    Sync and async callers contending for the same ``(registry, name)`` share
    state, so a sync ``release()`` on one thread can wake an async waiter
    parked on another thread's event loop (via ``loop.call_soon_threadsafe``).

    Non-reentrant: one instance holds at most one claim at a time. Calling
    ``acquire()`` (or ``aacquire()``) on an instance that is already held
    raises :class:`SemaphoreError`.
    """

    def __init__(
        self,
        name: str,
        capacity: int,
        *,
        weight: int = 1,
        lease: float | None = None,  # accepted but ignored on local backend
        timeout: float | None = None,
        _registry: _SemaphoreRegistry | None = None,
    ) -> None:
        _validate_init(capacity, weight)
        del lease
        self.name = name
        self.weight = weight
        self.timeout = timeout
        self._state = (_registry or _DEFAULT_REGISTRY).get_state(name, capacity)
        self._held = False

    # ------------------------------------------------------------------ sync

    def acquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:  # noqa: C901
        if timeout is None:
            timeout = self.timeout
        state = self._state
        waiter: _Waiter | None = None
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            with state.lock:
                # Same critical section as the admit below, so a racing thread
                # cannot see a stale ``_held`` and claim twice.
                if self._held:
                    if waiter is not None:
                        state.waiters.pop(waiter, None)
                        _notify_next(state)
                    msg = f"semaphore {self.name!r} already held by this instance"
                    raise SemaphoreError(msg)
                head_ok = not state.waiters or next(iter(state.waiters)) is waiter
                if head_ok and state.used + self.weight <= state.capacity:
                    if waiter is not None:
                        # We were at the head of the queue; pop ourselves before admitting.
                        state.waiters.pop(waiter)
                    state.used += self.weight
                    self._held = True
                    # A single release can free capacity for several queued
                    # waiters, but each release only wakes the current head.
                    # Now that we have taken our share, wake the next head if
                    # it also fits, otherwise smaller waiters behind us would
                    # block until an unrelated release.
                    _notify_next(state)
                    return True
                if not blocking:
                    return False
                if waiter is None:
                    waiter = _Waiter(async_=False)
                    state.waiters[waiter] = self.weight

            # Wait outside the state lock.
            remaining: float | None
            if deadline is None:
                remaining = None
            else:
                remaining = max(0.0, deadline - time.monotonic())
                if remaining == 0.0:
                    self._remove_waiter_and_notify(waiter)
                    msg = f"semaphore {self.name!r} acquire timed out"
                    raise SemaphoreTimeoutError(msg)
            # No try/except around event.wait() here: threads aren't
            # cooperatively cancelled like coroutines, so there is no
            # CancelledError analogue that could leave a phantom waiter
            # behind. The async path needs that guard; the sync path doesn't.
            waiter.wait_sync(remaining)
            waiter.clear_sync()

    def release(self) -> None:
        state = self._state
        with state.lock:
            # Checked under state.lock: two threads racing release() on the
            # same instance must not both pass and double-decrement ``used``.
            if not self._held:
                msg = "Cannot release a semaphore not held by this instance"
                raise SemaphoreError(msg)
            state.used -= self.weight
            self._held = False
            _notify_next(state)

    # ----------------------------------------------------------------- async

    async def aacquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:  # noqa: C901, PLR0912
        if timeout is None:
            timeout = self.timeout
        state = self._state
        loop = asyncio.get_running_loop()
        waiter: _Waiter | None = None
        deadline = None if timeout is None else loop.time() + timeout

        while True:
            with state.lock:
                # Same critical section as the admit below; see ``acquire``.
                if self._held:
                    if waiter is not None:
                        state.waiters.pop(waiter, None)
                        _notify_next(state)
                    msg = f"semaphore {self.name!r} already held by this instance"
                    raise SemaphoreError(msg)
                head_ok = not state.waiters or next(iter(state.waiters)) is waiter
                if head_ok and state.used + self.weight <= state.capacity:
                    if waiter is not None:
                        state.waiters.pop(waiter)
                    state.used += self.weight
                    self._held = True
                    # Cascade the wake: a single release can free room for more
                    # than one waiter, so hand off to the next head now that we
                    # have taken our share (see the sync ``acquire`` for why).
                    _notify_next(state)
                    return True
                if not blocking:
                    return False
                if waiter is None:
                    waiter = _Waiter(async_=True)
                    state.waiters[waiter] = self.weight

            if deadline is None:
                remaining = None
            else:
                remaining = max(0.0, deadline - loop.time())
                if remaining == 0.0:
                    self._remove_waiter_and_notify(waiter)
                    msg = f"semaphore {self.name!r} acquire timed out"
                    raise SemaphoreTimeoutError(msg)
            try:
                ok = await waiter.wait_async(remaining)
            except BaseException:
                # Cancellation (CancelledError derives from BaseException) or
                # any other propagating exception must not leave a phantom
                # reservation at the head of the queue, or smaller waiters
                # behind it would deadlock.
                self._remove_waiter_and_notify(waiter)
                raise
            if not ok:
                self._remove_waiter_and_notify(waiter)
                msg = f"semaphore {self.name!r} acquire timed out"
                raise SemaphoreTimeoutError(msg)
            # The future is one-shot. After a successful wake, swap in a fresh
            # waiter at the same queue position (preserving FIFO) so the next
            # iteration can park on it if we're not admitted yet.
            new_waiter = _Waiter(async_=True)
            with state.lock:
                # ``_notify_next`` only ever wakes the head, so we were it;
                # reinserting at the front is what preserves FIFO.
                weight = state.waiters.pop(waiter, self.weight)
                state.waiters[new_waiter] = weight
                state.waiters.move_to_end(new_waiter, last=False)
            waiter = new_waiter

    async def arelease(self) -> None:
        # Local backend has no async I/O during release; delegate to sync.
        self.release()

    # ------------------------------------------------------------------ misc

    def _remove_waiter_and_notify(self, waiter: _Waiter) -> None:
        state = self._state
        with state.lock:
            state.waiters.pop(waiter, None)
            _notify_next(state)

    def __enter__(self) -> Self:
        if not self.acquire():
            msg = f"could not acquire semaphore {self.name!r}"
            raise SemaphoreError(msg)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()

    async def __aenter__(self) -> Self:
        if not await self.aacquire():
            msg = f"could not acquire semaphore {self.name!r}"
            raise SemaphoreError(msg)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.arelease()


# Waiter heartbeat TTL; ACQUIRE_LUA reaps queue entries whose key expired.
# Must exceed the longest gap between two polls so a live waiter is never
# reaped. That gap is _MAX_BACKOFF_MS plus its jitter, i.e. 750 ms.
_MAX_BACKOFF_MS = 500
_WAITER_TTL_MS = 5_000


class RespSemaphore:
    """RESP-backed weighted semaphore using Lua scripts via the adapter's
    ``eval()`` / ``aeval()``.

    Constructed by ``cache.semaphore(...)`` or ``cache.asemaphore(...)`` on a
    RESP-backed cache. Tokens are random hex strings minted per call. The
    acquire/release Lua scripts handle budget accounting and queue ordering
    atomically. Cluster mode is supported: all keys per semaphore name share
    a ``{name}`` hash tag so they colocate on one slot.

    Non-reentrant: one instance holds at most one claim at a time. Calling
    ``acquire()`` (or ``aacquire()``) on an instance that is already held
    raises :class:`SemaphoreError`.

    Blocking ``acquire``/``aacquire`` polls with jittered exponential backoff
    (10 ms initial, 500 ms cap plus up to 250 ms of jitter). Worst-case
    admission latency after a release on another process is therefore up to
    ~750 ms even when budget is free; in-process the local
    :class:`Semaphore` wakes immediately. Non-blocking
    acquire (``blocking=False``) round-trips Redis twice on a miss (one
    ACQUIRE_LUA that enqueues, one DEQUEUE_LUA that removes); poll loops on
    ``blocking=False`` are inefficient and contend with the wait queue.
    """

    def __init__(
        self,
        adapter: RespAdapterProtocol,
        name: str,
        capacity: int,
        *,
        weight: int = 1,
        lease: float | None = None,
        timeout: float | None = None,
    ) -> None:
        _validate_init(capacity, weight)
        if lease is None or lease <= 0:
            msg = "lease must be a positive number of seconds (Redis backend)"
            raise ValueError(msg)
        self._adapter = adapter
        self.name = name
        self.capacity = capacity
        self.weight = weight
        self.lease = lease
        self.timeout = timeout
        self._token: str | None = None
        self._claim_lock = threading.Lock()
        prefix = "{" + name + "}"
        self._state_key = f"{prefix}:state"
        self._claims_key = f"{prefix}:claims"
        self._queue_key = f"{prefix}:queue"

    def _claim(self) -> str:
        """Check and mint in one critical section so racing threads cannot both claim."""
        with self._claim_lock:
            if self._token is not None:
                msg = f"semaphore {self.name!r} already held by this instance"
                raise SemaphoreError(msg)
            token = secrets.token_hex(16)
            self._token = token
        return token

    def _held_token(self, verb: str) -> str:
        """Snapshot the held token so the command can't act on a newer one.

        Reading ``self._token`` again at the call site let a racing thread
        install a fresh token in between, so the command ran against a claim
        this instance no longer owned.
        """
        with self._claim_lock:
            token = self._token
            if token is None:
                msg = f"Cannot {verb} a semaphore not held by this instance"
                raise SemaphoreError(msg)
        return token

    def _clear_token(self, token: str) -> None:
        """Drop our claim unless a racing acquire already replaced it."""
        with self._claim_lock:
            if self._token == token:
                self._token = None

    def _warn_if_not_owned(self, result: object) -> None:
        """Report a RELEASE that found no claim: the lease expired mid-work."""
        # Logged, not raised: ``release()`` runs from ``__exit__``, so raising
        # would chain over whatever the body was already reporting.
        if _decode_status(result) != "not_owned":
            return
        logger.warning(
            "semaphore %r: release found no claim; the lease (%.3gs) expired "
            "while the work was still running, so the budget was reclaimed and "
            "handed to another caller. Raise the lease or call extend().",
            self.name,
            self.lease,
        )

    # ------------------------------------------------------------------ sync

    def acquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:  # noqa: C901
        from django_cachex.cache._semaphore_lua import ACQUIRE_LUA, DEQUEUE_LUA

        if timeout is None:
            timeout = self.timeout
        token = self._claim()
        lease_ms = max(1, int(self.lease * 1000))
        deadline = None if timeout is None else time.monotonic() + timeout
        backoff_ms = 10

        def _dequeue_token() -> None:
            # Best-effort queue cleanup on any non-success exit; suppress
            # because we may already be unwinding.
            with contextlib.suppress(Exception):
                self._adapter.eval(DEQUEUE_LUA, 1, self._queue_key, token)

        while True:
            try:
                result = self._adapter.eval(
                    ACQUIRE_LUA,
                    3,
                    self._state_key,
                    self._claims_key,
                    self._queue_key,
                    token,
                    str(self.weight),
                    str(self.capacity),
                    str(lease_ms),
                    str(_WAITER_TTL_MS),
                )
                status = _decode_status(result)
            except BaseException:
                # KeyboardInterrupt must not leave our queue entry behind: a
                # dead head blocks acquirers until the liveness TTL expires.
                _dequeue_token()
                self._clear_token(token)
                raise
            if status == "acquired":
                return True
            if not blocking:
                _dequeue_token()
                self._clear_token(token)
                return False
            if deadline is not None and time.monotonic() >= deadline:
                _dequeue_token()
                self._clear_token(token)
                msg = f"semaphore {self.name!r} acquire timed out"
                raise SemaphoreTimeoutError(msg)
            # Jittered exponential backoff.
            jitter_ms = secrets.randbelow(max(1, backoff_ms // 2 + 1))
            sleep_s = (backoff_ms + jitter_ms) / 1000.0
            if deadline is not None:
                sleep_s = min(sleep_s, max(0.0, deadline - time.monotonic()))
            if sleep_s > 0:
                try:
                    time.sleep(sleep_s)
                except BaseException:
                    _dequeue_token()
                    self._clear_token(token)
                    raise
            backoff_ms = min(_MAX_BACKOFF_MS, int(backoff_ms * 1.5))

    def release(self) -> None:
        from django_cachex.cache._semaphore_lua import RELEASE_LUA

        token = self._held_token("release")
        result = self._adapter.eval(
            RELEASE_LUA,
            3,
            self._state_key,
            self._claims_key,
            self._queue_key,
            token,
        )
        self._clear_token(token)
        self._warn_if_not_owned(result)

    def extend(self, additional_seconds: float) -> bool:
        """Bump the lease TTL of the held claim by ``additional_seconds``.

        Returns True if extended, False if the claim isn't ours (already
        released or reaped).
        """
        from django_cachex.cache._semaphore_lua import EXTEND_LUA

        token = self._held_token("extend")
        additional_ms = max(1, int(additional_seconds * 1000))
        result = self._adapter.eval(
            EXTEND_LUA,
            2,
            self._state_key,
            self._claims_key,
            token,
            str(additional_ms),
        )
        return bool(result)

    # ----------------------------------------------------------------- async

    async def aacquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:  # noqa: C901
        from django_cachex.cache._semaphore_lua import ACQUIRE_LUA, DEQUEUE_LUA

        if timeout is None:
            timeout = self.timeout
        # ``_claim`` holds a plain lock across no awaits, so the sync and async
        # paths can share it without blocking the loop.
        token = self._claim()
        lease_ms = max(1, int(self.lease * 1000))
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        backoff_ms = 10

        async def _dequeue_token() -> None:
            # Best-effort cleanup of our queue entry on any non-success exit
            # (timeout raise, cancellation, other failure). Suppress because
            # we may already be unwinding for a different reason.
            with contextlib.suppress(Exception):
                await self._adapter.aeval(DEQUEUE_LUA, 1, self._queue_key, token)

        while True:
            try:
                result = await self._adapter.aeval(
                    ACQUIRE_LUA,
                    3,
                    self._state_key,
                    self._claims_key,
                    self._queue_key,
                    token,
                    str(self.weight),
                    str(self.capacity),
                    str(lease_ms),
                    str(_WAITER_TTL_MS),
                )
                status = _decode_status(result)
            except BaseException:
                await _dequeue_token()
                self._clear_token(token)
                raise
            if status == "acquired":
                return True
            if not blocking:
                await _dequeue_token()
                self._clear_token(token)
                return False
            if deadline is not None and loop.time() >= deadline:
                await _dequeue_token()
                self._clear_token(token)
                msg = f"semaphore {self.name!r} acquire timed out"
                raise SemaphoreTimeoutError(msg)
            # Jittered exponential backoff.
            jitter_ms = secrets.randbelow(max(1, backoff_ms // 2 + 1))
            sleep_s = (backoff_ms + jitter_ms) / 1000.0
            if deadline is not None:
                sleep_s = min(sleep_s, max(0.0, deadline - loop.time()))
            if sleep_s > 0:
                try:
                    await asyncio.sleep(sleep_s)
                except BaseException:
                    await _dequeue_token()
                    self._clear_token(token)
                    raise
            backoff_ms = min(_MAX_BACKOFF_MS, int(backoff_ms * 1.5))

    async def arelease(self) -> None:
        from django_cachex.cache._semaphore_lua import RELEASE_LUA

        token = self._held_token("release")
        result = await self._adapter.aeval(
            RELEASE_LUA,
            3,
            self._state_key,
            self._claims_key,
            self._queue_key,
            token,
        )
        self._clear_token(token)
        self._warn_if_not_owned(result)

    async def aextend(self, additional_seconds: float) -> bool:
        """Async mirror of :meth:`extend`."""
        from django_cachex.cache._semaphore_lua import EXTEND_LUA

        token = self._held_token("extend")
        additional_ms = max(1, int(additional_seconds * 1000))
        result = await self._adapter.aeval(
            EXTEND_LUA,
            2,
            self._state_key,
            self._claims_key,
            token,
            str(additional_ms),
        )
        return bool(result)

    # ------------------------------------------------------------- managers

    def __enter__(self) -> Self:
        if not self.acquire():
            msg = f"could not acquire semaphore {self.name!r}"
            raise SemaphoreError(msg)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()

    async def __aenter__(self) -> Self:
        if not await self.aacquire():
            msg = f"could not acquire semaphore {self.name!r}"
            raise SemaphoreError(msg)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.arelease()


__all__ = [
    "RespSemaphore",
    "Semaphore",
    "SemaphoreError",
    "SemaphoreTimeoutError",
]
