"""Trackers: the tracked reads, the eviction of own writes, the per-process registry and the fork hook."""

import logging
import os
import threading
from typing import TYPE_CHECKING, Any

from django_cachex.stampede import should_recompute_remaining
from django_cachex.tracking.listener import Listener
from django_cachex.tracking.store import LocalStore, clock

if TYPE_CHECKING:
    from collections.abc import Hashable, Iterable, Sequence

    from django_cachex.stampede import StampedeConfig
    from django_cachex.tracking.config import TrackingConfig

logger = logging.getLogger("django_cachex.tracking")


def store_key(key: str | bytes) -> str:
    """The key a copy is stored under: a str, as the server's bytes decode."""
    return key if isinstance(key, str) else key.decode("utf-8", "surrogateescape")


class Tracker:
    """The copies of one configuration in this process, and the connection that keeps them current."""

    def __init__(self, adapter: Any, config: TrackingConfig) -> None:
        self.config = config
        self.store = LocalStore(config.max_bytes)
        self.listener = Listener(adapter, config.prefix, config.max_staleness, self.store)

    def get(self, inner: Any, key: str | bytes, stampede: StampedeConfig | None) -> bytes | None:
        skey = store_key(key)
        if self.listener.drain():
            now = clock()
            copy = self.store.lookup(skey, now)
            if copy is not None:
                if copy.value is not None and _recompute_hit(copy.deadline, now, stampede):
                    return None
                return copy.value
        token = self.store.begin_fetch(skey)
        started = clock()
        try:
            pipe = inner.pipeline(transaction=False)
            pipe.get(key)
            pipe.pttl(key)
            value, pttl = pipe.execute()
        except BaseException:
            self.store.forget(skey, token)
            raise
        self.store.absorb(skey, token, value, pttl, started)
        if value is not None and _recompute_fetch(pttl, stampede):
            return None
        return value

    def get_many(self, inner: Any, keys: Sequence[str | bytes], stampede: StampedeConfig | None) -> dict[Any, bytes]:
        found: dict[Any, bytes] = {}
        misses: list[str | bytes] = []
        if self.listener.drain():
            now = clock()
            for key in keys:
                copy = self.store.lookup(store_key(key), now)
                if copy is None:
                    misses.append(key)
                elif copy.value is not None and not _recompute_hit(copy.deadline, now, stampede):
                    found[key] = copy.value
        else:
            misses = list(keys)
        if misses:
            found.update(
                (key, value)
                for key, value, pttl in self._fetch(inner, misses)
                if value is not None and not _recompute_fetch(pttl, stampede)
            )
        return found

    def read_many(self, inner: Any, keys: Sequence[str | bytes]) -> list[bytes | None]:
        """Raw values in key order, for the ORM store; keys outside the prefix are read but never kept."""
        values: dict[int, bytes | None] = {}
        misses: list[int] = []
        if self.listener.drain():
            now = clock()
            for index, key in enumerate(keys):
                copy = self.store.lookup(store_key(key), now) if self.config.covers(key) else None
                if copy is None:
                    misses.append(index)
                else:
                    values[index] = copy.value
        else:
            misses = list(range(len(keys)))
        if misses:
            fetched = self._fetch(inner, [keys[index] for index in misses])
            for index, (_, value, _) in zip(misses, fetched, strict=True):
                values[index] = value
        return [values[index] for index in range(len(keys))]

    def has_key(self, inner: Any, key: str | bytes, stampede: StampedeConfig | None) -> bool:
        if self.listener.drain():
            now = clock()
            copy = self.store.lookup(store_key(key), now)
            if copy is not None:
                if stampede is None or copy.value is None or copy.deadline is None:
                    return copy.value is not None
                return copy.deadline - now > stampede.buffer
        self.store.note_miss()
        pipe = inner.pipeline(transaction=False)
        if stampede is None:
            pipe.exists(key)
            return bool(pipe.execute()[0])
        pipe.pttl(key)
        pttl = pipe.execute()[0]
        return pttl == -1 or pttl > stampede.buffer * 1000

    def discard(self, strings: Iterable[str | bytes]) -> None:
        """Drop the copies of the keys among ``strings`` that this tracker covers."""
        keys = [store_key(s) for s in strings if self.config.covers(s)]
        if keys:
            self.store.discard(keys)

    def flush(self) -> None:
        self.store.flush()

    def info(self) -> dict[str, Any]:
        connected, confirmed_at = self.listener.status()
        age = None if confirmed_at is None else clock() - confirmed_at
        return {
            "connected": connected,
            "fresh": connected and age is not None and age <= self.config.max_staleness,
            "prefix": self.config.prefix,
            "max_staleness": self.config.max_staleness,
            **self.store.stats(),
            "reconnects": self.listener.reconnects,
            "last_confirmed_age": age,
        }

    def _fetch(self, inner: Any, keys: list[str | bytes]) -> list[tuple[Any, bytes | None, int | None]]:
        """Read ``keys`` with one MGET and a PTTL per covered key, and keep what can be kept.

        An uncovered key's PTTL is not read, and None in the result.
        """
        covered = [(index, store_key(key)) for index, key in enumerate(keys) if self.config.covers(key)]
        tokens = [self.store.begin_fetch(skey) for _, skey in covered]
        started = clock()
        try:
            pipe = inner.pipeline(transaction=False)
            pipe.execute_command("MGET", *keys)
            for index, _ in covered:
                pipe.pttl(keys[index])
            values, *replies = pipe.execute()
        except BaseException:
            for (_, skey), token in zip(covered, tokens, strict=True):
                self.store.forget(skey, token)
            raise
        pttls: list[int | None] = [None] * len(keys)
        for (index, skey), token, pttl in zip(covered, tokens, replies, strict=True):
            pttls[index] = pttl
            self.store.absorb(skey, token, values[index], pttl, started)
        return list(zip(keys, values, pttls, strict=True))


def _recompute_hit(deadline: float | None, now: float, stampede: StampedeConfig | None) -> bool:
    if stampede is None or deadline is None:
        return False
    return should_recompute_remaining(deadline - now - stampede.buffer, stampede)


def _recompute_fetch(pttl: int | None, stampede: StampedeConfig | None) -> bool:
    if stampede is None or pttl is None or pttl < 0:
        return False
    return should_recompute_remaining(pttl / 1000 - stampede.buffer, stampede)


# Seconds before a read tries again to start a tracking thread that could not start.
START_RETRY = 5.0


class _Registry:
    __slots__ = ("lock", "pid", "retry_at", "start_warned", "trackers")

    def __init__(self) -> None:
        self.pid = os.getpid()
        self.lock = threading.Lock()
        self.trackers: dict[Hashable, Tracker] = {}
        self.retry_at: dict[Hashable, float] = {}
        self.start_warned = False


_registry = _Registry()
# One lock per process id, so a child that forked while a thread held one gets a free one.
_RESET_LOCKS: dict[int, threading.Lock] = {}


def _current() -> _Registry:
    registry = _registry
    if registry.pid != os.getpid():
        registry = _reset_after_fork()
    return registry


def _reset_after_fork() -> _Registry:
    """Give a forked child an empty registry; the parent's trackers let go of their descriptors."""
    global _registry  # noqa: PLW0603
    pid = os.getpid()
    with _RESET_LOCKS.setdefault(pid, threading.Lock()):
        old = _registry
        if old.pid == pid:
            return old
        for tracker in tuple(old.trackers.values()):
            tracker.listener.discard_in_child()
        _registry = _Registry()
        return _registry


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


def registry_key(
    adapter_class: type,
    servers: Sequence[str],
    options: dict[str, Any],
    config: TrackingConfig,
) -> Hashable:
    """Caches with equal keys share one tracker: the same driver, servers, options and tracking settings."""
    return (adapter_class, tuple(servers), repr(sorted(options.items())), config)


def get_tracker(key: Hashable, adapter: Any, config: TrackingConfig) -> Tracker | None:
    """Return this process's tracker for ``key``, creating it and starting its thread on first use.

    None when the thread cannot start, as under a thread limit or in a subinterpreter. The first call after
    ``START_RETRY`` seconds tries again.
    """
    registry = _current()
    tracker = registry.trackers.get(key)
    if tracker is None:
        if clock() < registry.retry_at.get(key, 0.0):
            return None
        with registry.lock:
            tracker = registry.trackers.get(key)
            if tracker is None:
                try:
                    tracker = Tracker(adapter, config)
                    tracker.listener.start()
                except Exception as e:  # noqa: BLE001
                    registry.retry_at[key] = clock() + START_RETRY
                    if not registry.start_warned:
                        logger.warning("Client tracking cannot start, reads go to the server: %s", e)
                        registry.start_warned = True
                    return None
                # Replace, never change in place: trackers() iterates the dict without the lock.
                registry.trackers = {**registry.trackers, key: tracker}
    return tracker


def find_tracker(key: Hashable) -> Tracker | None:
    return _current().trackers.get(key)


def trackers() -> tuple[Tracker, ...]:
    return tuple(_current().trackers.values())
