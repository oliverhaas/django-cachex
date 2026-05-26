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

import asyncio
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
    waiters: deque = field(default_factory=deque)  # FIFO of (weight, _Waiter)


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


def _notify_next(state: _LocalState) -> None:
    """Wake the head waiter if its weight now fits."""
    if not state.waiters:
        return
    weight, waiter = state.waiters[0]
    if state.used + weight <= state.capacity:
        waiter.wake()


class _Waiter:
    """Cross-context wake target. Either a sync ``threading.Event`` or an
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

    def wake(self) -> None:
        if self.event is not None:
            self.event.set()
            return
        assert self.future is not None  # noqa: S101
        assert self.loop is not None  # noqa: S101
        self.loop.call_soon_threadsafe(self._set_future_result)

    def _set_future_result(self) -> None:
        assert self.future is not None  # noqa: S101
        if not self.future.done():
            self.future.set_result(None)


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
        waiter: _Waiter | None = None
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            with state.lock:
                head_ok = not state.waiters or state.waiters[0][1] is waiter
                if head_ok and state.used + self.weight <= state.capacity:
                    if waiter is not None:
                        # We were at the head of the queue; pop ourselves before admitting.
                        state.waiters.popleft()
                    state.used += self.weight
                    self._held = True
                    return True
                if not blocking:
                    return False
                if waiter is None:
                    waiter = _Waiter(async_=False)
                    state.waiters.append((self.weight, waiter))

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
            waiter.wait_sync(remaining)
            waiter.clear_sync()

    def release(self) -> None:
        if not self._held:
            msg = "Cannot release a semaphore not held by this instance"
            raise SemaphoreError(msg)
        state = self._state
        with state.lock:
            state.used -= self.weight
            self._held = False
            _notify_next(state)

    def _remove_waiter_and_notify(self, waiter: _Waiter) -> None:
        state = self._state
        with state.lock:
            state.waiters = deque((w, ev) for (w, ev) in state.waiters if ev is not waiter)
            _notify_next(state)


class AsyncSemaphore:
    """Local in-process async weighted semaphore.

    Shares ``_LocalState`` with the sync ``Semaphore`` of the same
    (owner_id, name), so sync and async callers can contend for the same
    budget without coordination.
    """

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

    async def acquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:  # noqa: C901
        if timeout is None:
            timeout = self.timeout
        state = self._state
        loop = asyncio.get_running_loop()
        waiter: _Waiter | None = None
        deadline = None if timeout is None else loop.time() + timeout

        while True:
            with state.lock:
                head_ok = not state.waiters or state.waiters[0][1] is waiter
                if head_ok and state.used + self.weight <= state.capacity:
                    if waiter is not None:
                        state.waiters.popleft()
                    state.used += self.weight
                    self._held = True
                    return True
                if not blocking:
                    return False
                if waiter is None:
                    waiter = _Waiter(async_=True)
                    state.waiters.append((self.weight, waiter))

            if deadline is None:
                remaining = None
            else:
                remaining = max(0.0, deadline - loop.time())
                if remaining == 0.0:
                    self._remove_waiter_and_notify(waiter)
                    msg = f"semaphore {self.name!r} acquire timed out"
                    raise SemaphoreTimeoutError(msg)
            ok = await waiter.wait_async(remaining)
            if not ok:
                self._remove_waiter_and_notify(waiter)
                msg = f"semaphore {self.name!r} acquire timed out"
                raise SemaphoreTimeoutError(msg)
            # The future is one-shot, so after a successful wake we replace
            # the waiter with a fresh one. We hold the wait-queue spot until
            # we successfully admit (head_ok + budget) on the next iteration.
            new_waiter = _Waiter(async_=True)
            with state.lock:
                for i, (w, ev) in enumerate(state.waiters):
                    if ev is waiter:
                        state.waiters[i] = (w, new_waiter)
                        break
            waiter = new_waiter

    async def release(self) -> None:
        if not self._held:
            msg = "Cannot release a semaphore not held by this instance"
            raise SemaphoreError(msg)
        state = self._state
        with state.lock:
            state.used -= self.weight
            self._held = False
            _notify_next(state)

    def _remove_waiter_and_notify(self, waiter: _Waiter) -> None:
        state = self._state
        with state.lock:
            state.waiters = deque((w, ev) for (w, ev) in state.waiters if ev is not waiter)
            _notify_next(state)


__all__ = ["AsyncSemaphore", "Semaphore", "SemaphoreError", "SemaphoreTimeoutError"]
