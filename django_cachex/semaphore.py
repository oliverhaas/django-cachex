"""Weighted semaphores for django-cachex.

Two implementations share a common API:

- ``Semaphore`` / ``AsyncSemaphore`` (in-process, used by ``LocMemCache``):
  FIFO deque + ``threading.Lock`` for state.
- ``RedisSemaphore`` / ``RedisAsyncSemaphore`` (added in later tasks):
  Lua-script-via-EVAL on any RESP adapter.

The public top-level names ``Semaphore`` / ``AsyncSemaphore`` resolve to the
in-process classes; Redis-backed instances are constructed by the cache
factory methods (``cache.semaphore(...)`` / ``cache.asemaphore(...)``) and
are not exposed by name.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field


class SemaphoreError(Exception):
    """Raised when a semaphore operation fails."""


class SemaphoreTimeoutError(SemaphoreError):
    """Raised when ``timeout`` elapses before the caller could acquire."""


@dataclass
class _LocalState:
    """Shared state for one (cache-instance, name) pair."""

    capacity: int
    used: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)
    waiters: deque = field(default_factory=deque)  # FIFO of (weight, event_or_future)


# Registry keyed by (owner_id, name). Future tasks pass the cache instance's
# id() as owner_id so each cache scopes its semaphores like Django's
# LocMemCache scopes its OrderedDict per LOCATION.
_local_registry: dict[tuple[int, str], _LocalState] = {}
_registry_lock = threading.Lock()


def _get_state(owner_id: int, name: str, capacity: int) -> _LocalState:
    key = (owner_id, name)
    with _registry_lock:
        state = _local_registry.get(key)
        if state is None:
            state = _LocalState(capacity=capacity)
            _local_registry[key] = state
        return state


class Semaphore:
    """Local in-process weighted semaphore (counting variant in Task 3)."""

    def __init__(
        self,
        name: str,
        capacity: int,
        *,
        weight: int = 1,
        lease: float | None = None,  # accepted but ignored on local backend
        timeout: float | None = None,
        _owner_id: int = 0,
    ) -> None:
        if capacity <= 0:
            msg = "capacity must be positive"
            raise ValueError(msg)
        if weight <= 0:
            msg = "weight must be positive"
            raise ValueError(msg)
        if weight > capacity:
            msg = f"weight ({weight}) exceeds capacity ({capacity})"
            raise ValueError(msg)
        self.name = name
        self.weight = weight
        self.timeout = timeout
        self._state = _get_state(_owner_id, name, capacity)
        self._held = False

    def acquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:
        with self._state.lock:
            if self._state.used + self.weight <= self._state.capacity:
                self._state.used += self.weight
                self._held = True
                return True
        if not blocking:
            return False
        msg = "blocking acquire not yet implemented"
        raise NotImplementedError(msg)

    def release(self) -> None:
        if not self._held:
            msg = "Cannot release a semaphore not held by this instance"
            raise SemaphoreError(msg)
        with self._state.lock:
            self._state.used -= self.weight
            self._held = False


class AsyncSemaphore:
    """Local in-process async weighted semaphore. Implementation lands in Task 7."""


__all__ = ["AsyncSemaphore", "Semaphore", "SemaphoreError", "SemaphoreTimeoutError"]
