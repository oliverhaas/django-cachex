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
import time
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
        if timeout is None:
            timeout = self.timeout
        state = self._state
        event: threading.Event | None = None
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            with state.lock:
                head_ok = not state.waiters or state.waiters[0][1] is event
                if head_ok and state.used + self.weight <= state.capacity:
                    if event is not None:
                        # We were at the head of the queue; pop ourselves before admitting.
                        state.waiters.popleft()
                    state.used += self.weight
                    self._held = True
                    return True
                if not blocking:
                    return False
                if event is None:
                    event = threading.Event()
                    state.waiters.append((self.weight, event))

            # Wait outside the state lock.
            remaining: float | None
            if deadline is None:
                remaining = None
            else:
                remaining = max(0.0, deadline - time.monotonic())
                if remaining == 0.0:
                    self._remove_self_and_notify(event)
                    msg = f"semaphore {self.name!r} acquire timed out"
                    raise SemaphoreTimeoutError(msg)
            event.wait(timeout=remaining)
            event.clear()

    def release(self) -> None:
        if not self._held:
            msg = "Cannot release a semaphore not held by this instance"
            raise SemaphoreError(msg)
        state = self._state
        with state.lock:
            state.used -= self.weight
            self._held = False
            self._notify_next(state)

    def _remove_self_and_notify(self, event: threading.Event) -> None:
        state = self._state
        with state.lock:
            state.waiters = deque((w, ev) for (w, ev) in state.waiters if ev is not event)
            self._notify_next(state)

    @staticmethod
    def _notify_next(state: _LocalState) -> None:
        """Wake any head-of-queue waiter whose weight now fits."""
        if not state.waiters:
            return
        weight, event = state.waiters[0]
        if state.used + weight <= state.capacity:
            # Trigger the event; the woken waiter pops itself once it re-acquires
            # the state lock.
            event.set()


class AsyncSemaphore:
    """Local in-process async weighted semaphore. Implementation lands in Task 7."""


__all__ = ["AsyncSemaphore", "Semaphore", "SemaphoreError", "SemaphoreTimeoutError"]
