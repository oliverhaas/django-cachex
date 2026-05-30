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
import contextlib
import secrets
import threading
import time
import warnings
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from types import TracebackType

    from django_cachex.adapters.protocols import RespAdapterProtocol


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
    waiters: deque[tuple[int, _Waiter]] = field(default_factory=deque)  # FIFO of (weight, _Waiter)


# Registry keyed by (owner_id, name). Future tasks pass the cache instance's
# id() as owner_id so each cache scopes its semaphores like Django's
# LocMemCache scopes its OrderedDict per LOCATION.
_local_registry: dict[tuple[int, str], _LocalState] = {}
_registry_lock = threading.Lock()


def _get_state(owner_id: int, name: str, capacity: int) -> _LocalState:
    key = (owner_id, name)
    old_capacity: int | None = None
    with _registry_lock:
        state = _local_registry.get(key)
        if state is None:
            state = _LocalState(capacity=capacity)
            _local_registry[key] = state
            return state
        if state.capacity != capacity:
            old_capacity = state.capacity
            with state.lock:
                state.capacity = capacity
    if old_capacity is not None:
        # Warn AFTER releasing the registry lock: a user warning handler that
        # re-enters _get_state would otherwise deadlock on _registry_lock.
        warnings.warn(
            (
                f"semaphore {name!r}: capacity changed from {old_capacity} "
                f"to {capacity}; new value takes effect on next acquire. "
                f"In-flight claims are not retroactively rejected."
            ),
            RuntimeWarning,
            stacklevel=3,
        )
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
            # No try/except around event.wait() here: threads aren't
            # cooperatively cancelled like coroutines, so there is no
            # CancelledError analogue that could leave a phantom waiter
            # behind. The async path needs that guard; the sync path doesn't.
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

    async def acquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:  # noqa: C901, PLR0912
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
            try:
                ok = await waiter.wait_async(remaining)
            except BaseException:
                # Cancellation (CancelledError derives from BaseException in
                # 3.8+) or any other propagating exception must not leave a
                # phantom reservation at the head of the queue, or smaller
                # waiters behind it will deadlock.
                self._remove_waiter_and_notify(waiter)
                raise
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

    async def __aenter__(self) -> Self:
        if not await self.acquire():
            msg = f"could not acquire semaphore {self.name!r}"
            raise SemaphoreError(msg)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.release()


class RedisSemaphore:
    """RESP-backed weighted semaphore using Lua scripts via the adapter's eval().

    Constructed by ``cache.semaphore(...)`` on a RESP-backed cache. Tokens
    are random hex strings minted per call. The acquire/release Lua scripts
    handle budget accounting and queue ordering atomically. Cluster mode is
    supported: the three Redis keys per semaphore name share a ``{name}``
    hash tag so they colocate on one slot, as Redis Cluster requires for
    atomic multi-key Lua.
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
        if capacity <= 0:
            msg = "capacity must be positive"
            raise ValueError(msg)
        if weight <= 0:
            msg = "weight must be positive"
            raise ValueError(msg)
        if weight > capacity:
            msg = f"weight ({weight}) exceeds capacity ({capacity})"
            raise ValueError(msg)
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

    def _keys(self) -> tuple[str, str, str]:
        """All three keys share a ``{name}`` hash-tag so they land on one slot."""
        prefix = "{" + self.name + "}"
        return (f"{prefix}:state", f"{prefix}:claims", f"{prefix}:queue")

    @staticmethod
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

    def acquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:
        from django_cachex.cache._semaphore_lua import ACQUIRE_LUA, DEQUEUE_LUA

        if timeout is None:
            timeout = self.timeout
        token = self._token or secrets.token_hex(16)
        self._token = token
        state_key, claims_key, queue_key = self._keys()
        lease_ms = max(1, int(self.lease * 1000))
        deadline = None if timeout is None else time.monotonic() + timeout
        backoff_ms = 10
        max_backoff_ms = 500

        while True:
            now_ms = int(time.time() * 1000)
            result = self._adapter.eval(
                ACQUIRE_LUA,
                3,
                state_key,
                claims_key,
                queue_key,
                token,
                str(self.weight),
                str(self.capacity),
                str(lease_ms),
                str(now_ms),
            )
            status = self._decode_status(result)
            if status == "acquired":
                return True
            if not blocking:
                self._adapter.eval(DEQUEUE_LUA, 1, queue_key, token)
                self._token = None
                return False
            if deadline is not None and time.monotonic() >= deadline:
                self._adapter.eval(DEQUEUE_LUA, 1, queue_key, token)
                self._token = None
                msg = f"semaphore {self.name!r} acquire timed out"
                raise SemaphoreTimeoutError(msg)
            # Jittered exponential backoff.
            jitter_ms = secrets.randbelow(max(1, backoff_ms // 2 + 1))
            sleep_s = (backoff_ms + jitter_ms) / 1000.0
            if deadline is not None:
                sleep_s = min(sleep_s, max(0.0, deadline - time.monotonic()))
            if sleep_s > 0:
                time.sleep(sleep_s)
            backoff_ms = min(max_backoff_ms, int(backoff_ms * 1.5))

    def release(self) -> None:
        from django_cachex.cache._semaphore_lua import RELEASE_LUA

        if self._token is None:
            msg = "Cannot release a semaphore not held by this instance"
            raise SemaphoreError(msg)
        state_key, claims_key, queue_key = self._keys()
        self._adapter.eval(RELEASE_LUA, 3, state_key, claims_key, queue_key, self._token)
        self._token = None

    def extend(self, additional_seconds: float) -> bool:
        """Bump the lease TTL of the held claim by ``additional_seconds``.

        Returns True if extended, False if the claim isn't ours (already
        released or reaped).
        """
        from django_cachex.cache._semaphore_lua import EXTEND_LUA

        if self._token is None:
            msg = "Cannot extend a semaphore not held by this instance"
            raise SemaphoreError(msg)
        state_key, claims_key, _ = self._keys()
        additional_ms = max(1, int(additional_seconds * 1000))
        result = self._adapter.eval(
            EXTEND_LUA,
            2,
            state_key,
            claims_key,
            self._token,
            str(additional_ms),
        )
        # Lua returns 0 / 1; coerce to bool.
        return bool(int(result)) if isinstance(result, (int, bytes, str)) else bool(result)

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


class RedisAsyncSemaphore:
    """Async mirror of :class:`RedisSemaphore`.

    Uses the same Lua scripts; awaits the adapter's ``aeval()``. Constructed
    by ``cache.asemaphore(...)`` on a RESP-backed cache.
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
        if capacity <= 0:
            msg = "capacity must be positive"
            raise ValueError(msg)
        if weight <= 0:
            msg = "weight must be positive"
            raise ValueError(msg)
        if weight > capacity:
            msg = f"weight ({weight}) exceeds capacity ({capacity})"
            raise ValueError(msg)
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

    def _keys(self) -> tuple[str, str, str]:
        """All three keys share a ``{name}`` hash-tag so they land on one slot."""
        prefix = "{" + self.name + "}"
        return (f"{prefix}:state", f"{prefix}:claims", f"{prefix}:queue")

    async def acquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:  # noqa: C901
        from django_cachex.cache._semaphore_lua import ACQUIRE_LUA, DEQUEUE_LUA

        if timeout is None:
            timeout = self.timeout
        token = self._token or secrets.token_hex(16)
        self._token = token
        state_key, claims_key, queue_key = self._keys()
        lease_ms = max(1, int(self.lease * 1000))
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        backoff_ms = 10
        max_backoff_ms = 500

        async def _dequeue_token() -> None:
            # Best-effort cleanup of our queue entry on any non-success exit
            # (timeout raise, cancellation, other failure). Suppress because
            # we may already be unwinding for a different reason.
            with contextlib.suppress(Exception):
                await self._adapter.aeval(DEQUEUE_LUA, 1, queue_key, token)

        while True:
            now_ms = int(time.time() * 1000)
            try:
                result = await self._adapter.aeval(
                    ACQUIRE_LUA,
                    3,
                    state_key,
                    claims_key,
                    queue_key,
                    token,
                    str(self.weight),
                    str(self.capacity),
                    str(lease_ms),
                    str(now_ms),
                )
            except BaseException:
                # Cancellation mid-eval can leave a queue entry behind that
                # would silently block subsequent acquires under this name.
                await _dequeue_token()
                self._token = None
                raise
            status = RedisSemaphore._decode_status(result)
            if status == "acquired":
                return True
            if not blocking:
                await _dequeue_token()
                self._token = None
                return False
            if deadline is not None and loop.time() >= deadline:
                await _dequeue_token()
                self._token = None
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
                    # Cancellation during backoff: drop our queue entry
                    # so smaller waiters behind us don't deadlock.
                    await _dequeue_token()
                    self._token = None
                    raise
            backoff_ms = min(max_backoff_ms, int(backoff_ms * 1.5))

    async def release(self) -> None:
        from django_cachex.cache._semaphore_lua import RELEASE_LUA

        if self._token is None:
            msg = "Cannot release a semaphore not held by this instance"
            raise SemaphoreError(msg)
        state_key, claims_key, queue_key = self._keys()
        await self._adapter.aeval(RELEASE_LUA, 3, state_key, claims_key, queue_key, self._token)
        self._token = None

    async def extend(self, additional_seconds: float) -> bool:
        """Bump the lease TTL of the held claim by ``additional_seconds``.

        Returns True if extended, False if the claim isn't ours (already
        released or reaped).
        """
        from django_cachex.cache._semaphore_lua import EXTEND_LUA

        if self._token is None:
            msg = "Cannot extend a semaphore not held by this instance"
            raise SemaphoreError(msg)
        state_key, claims_key, _ = self._keys()
        additional_ms = max(1, int(additional_seconds * 1000))
        result = await self._adapter.aeval(
            EXTEND_LUA,
            2,
            state_key,
            claims_key,
            self._token,
            str(additional_ms),
        )
        # Lua returns 0 / 1; coerce to bool.
        return bool(int(result)) if isinstance(result, (int, bytes, str)) else bool(result)

    async def __aenter__(self) -> Self:
        if not await self.acquire():
            msg = f"could not acquire semaphore {self.name!r}"
            raise SemaphoreError(msg)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.release()


__all__ = [
    "AsyncSemaphore",
    "RedisAsyncSemaphore",
    "RedisSemaphore",
    "Semaphore",
    "SemaphoreError",
    "SemaphoreTimeoutError",
]
