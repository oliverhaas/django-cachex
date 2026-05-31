"""Weighted semaphores for django-cachex.

One class per backend, each exposing paired sync/async methods to match the
rest of the project's convention (``foo``/``afoo``):

- :class:`Semaphore`: in-process, used by ``LocMemCache``. State lives in the
  cache's own ``_SemaphoreRegistry``; standalone instances share a
  process-wide registry.
- :class:`RespSemaphore`: backed by Lua scripts dispatched through any
  ``RespAdapterProtocol`` (redis-py, redis-rs, valkey-py, valkey-glide).
  Constructed by ``cache.semaphore(...)`` / ``cache.asemaphore(...)`` and not
  exposed at the package root.

Acquisition is non-reentrant: one instance holds at most one claim at a time.
Re-acquiring before releasing raises :class:`SemaphoreError`. Create a new
instance per acquire/release lifecycle, the same way :class:`Lock` is used.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets
import threading
import time
import warnings
from collections import OrderedDict
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
    """Shared state for one (registry, name) pair.

    ``waiters`` is an ``OrderedDict`` of ``_Waiter -> weight`` so head lookup,
    append, and remove are all O(1). Insertion order is preserved, giving FIFO.
    """

    capacity: int
    used: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)
    waiters: OrderedDict[_Waiter, int] = field(default_factory=OrderedDict)


class _SemaphoreRegistry:
    """Per-cache (or process-wide) registry of :class:`_LocalState` by name.

    Replaces the old ``id(cache)``-keyed module-level dict, which could alias
    between a GC'd cache instance and a newly-created one if Python reused
    the address. Each :class:`~django_cachex.cache.LocMemCache` owns one
    registry; standalone :class:`Semaphore` instances share the module-level
    ``_DEFAULT_REGISTRY``.
    """

    def __init__(self) -> None:
        self._states: dict[str, _LocalState] = {}
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
                stacklevel=4,
            )
        return state


_DEFAULT_REGISTRY = _SemaphoreRegistry()


def _notify_next(state: _LocalState) -> None:
    """Wake the head waiter if its weight now fits."""
    if not state.waiters:
        return
    head_waiter = next(iter(state.waiters))
    head_weight = state.waiters[head_waiter]
    if state.used + head_weight <= state.capacity:
        head_waiter.wake()


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

    def acquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:
        if self._held:
            msg = f"semaphore {self.name!r} already held by this instance"
            raise SemaphoreError(msg)
        if timeout is None:
            timeout = self.timeout
        state = self._state
        waiter: _Waiter | None = None
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            with state.lock:
                head_ok = not state.waiters or next(iter(state.waiters)) is waiter
                if head_ok and state.used + self.weight <= state.capacity:
                    if waiter is not None:
                        # We were at the head of the queue; pop ourselves before admitting.
                        state.waiters.pop(waiter)
                    state.used += self.weight
                    self._held = True
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
        if not self._held:
            msg = "Cannot release a semaphore not held by this instance"
            raise SemaphoreError(msg)
        state = self._state
        with state.lock:
            state.used -= self.weight
            self._held = False
            _notify_next(state)

    # ----------------------------------------------------------------- async

    async def aacquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:  # noqa: C901, PLR0912
        if self._held:
            msg = f"semaphore {self.name!r} already held by this instance"
            raise SemaphoreError(msg)
        if timeout is None:
            timeout = self.timeout
        state = self._state
        loop = asyncio.get_running_loop()
        waiter: _Waiter | None = None
        deadline = None if timeout is None else loop.time() + timeout

        while True:
            with state.lock:
                head_ok = not state.waiters or next(iter(state.waiters)) is waiter
                if head_ok and state.used + self.weight <= state.capacity:
                    if waiter is not None:
                        state.waiters.pop(waiter)
                    state.used += self.weight
                    self._held = True
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
                if waiter in state.waiters:
                    weight = state.waiters.pop(waiter)
                    state.waiters[new_waiter] = weight
                    state.waiters.move_to_end(new_waiter, last=False)
                    # Restore head position by moving every other waiter back
                    # to the end in their original order. Since OrderedDict
                    # preserves insertion order, popping us and reinserting
                    # at the front via move_to_end(last=False) keeps FIFO.
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
    (10 ms initial, 500 ms cap). Worst-case admission latency after a release
    on another process is therefore up to ~500 ms even when budget is free;
    in-process the local :class:`Semaphore` wakes immediately. Non-blocking
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
        prefix = "{" + name + "}"
        self._state_key = f"{prefix}:state"
        self._claims_key = f"{prefix}:claims"
        self._queue_key = f"{prefix}:queue"

    # ------------------------------------------------------------------ sync

    def acquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:
        from django_cachex.cache._semaphore_lua import ACQUIRE_LUA, DEQUEUE_LUA

        if self._token is not None:
            msg = f"semaphore {self.name!r} already held by this instance"
            raise SemaphoreError(msg)
        if timeout is None:
            timeout = self.timeout
        token = secrets.token_hex(16)
        self._token = token
        lease_ms = max(1, int(self.lease * 1000))
        deadline = None if timeout is None else time.monotonic() + timeout
        backoff_ms = 10
        max_backoff_ms = 500

        while True:
            now_ms = int(time.time() * 1000)
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
                str(now_ms),
            )
            status = _decode_status(result)
            if status == "acquired":
                return True
            if not blocking:
                self._adapter.eval(DEQUEUE_LUA, 1, self._queue_key, token)
                self._token = None
                return False
            if deadline is not None and time.monotonic() >= deadline:
                self._adapter.eval(DEQUEUE_LUA, 1, self._queue_key, token)
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
        self._adapter.eval(
            RELEASE_LUA,
            3,
            self._state_key,
            self._claims_key,
            self._queue_key,
            self._token,
        )
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
        additional_ms = max(1, int(additional_seconds * 1000))
        result = self._adapter.eval(
            EXTEND_LUA,
            2,
            self._state_key,
            self._claims_key,
            self._token,
            str(additional_ms),
        )
        return bool(result)

    # ----------------------------------------------------------------- async

    async def aacquire(self, *, blocking: bool = True, timeout: float | None = None) -> bool:  # noqa: C901
        from django_cachex.cache._semaphore_lua import ACQUIRE_LUA, DEQUEUE_LUA

        if self._token is not None:
            msg = f"semaphore {self.name!r} already held by this instance"
            raise SemaphoreError(msg)
        if timeout is None:
            timeout = self.timeout
        token = secrets.token_hex(16)
        self._token = token
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
                await self._adapter.aeval(DEQUEUE_LUA, 1, self._queue_key, token)

        while True:
            now_ms = int(time.time() * 1000)
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
                    str(now_ms),
                )
            except BaseException:
                await _dequeue_token()
                self._token = None
                raise
            status = _decode_status(result)
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
                    await _dequeue_token()
                    self._token = None
                    raise
            backoff_ms = min(max_backoff_ms, int(backoff_ms * 1.5))

    async def arelease(self) -> None:
        from django_cachex.cache._semaphore_lua import RELEASE_LUA

        if self._token is None:
            msg = "Cannot release a semaphore not held by this instance"
            raise SemaphoreError(msg)
        await self._adapter.aeval(
            RELEASE_LUA,
            3,
            self._state_key,
            self._claims_key,
            self._queue_key,
            self._token,
        )
        self._token = None

    async def aextend(self, additional_seconds: float) -> bool:
        """Async mirror of :meth:`extend`."""
        from django_cachex.cache._semaphore_lua import EXTEND_LUA

        if self._token is None:
            msg = "Cannot extend a semaphore not held by this instance"
            raise SemaphoreError(msg)
        additional_ms = max(1, int(additional_seconds * 1000))
        result = await self._adapter.aeval(
            EXTEND_LUA,
            2,
            self._state_key,
            self._claims_key,
            self._token,
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
