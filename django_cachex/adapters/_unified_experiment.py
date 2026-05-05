"""Experimental: one ``redis.asyncio`` client per process, sync calls hop to it.

Spike for the question "what if redis-py / valkey-py had a multiplexed-style
single-transport model like redis-rs / valkey-glide?".

Architecture:

- One process-wide daemon thread runs an ``asyncio`` event loop ("worker").
- One ``valkey.asyncio.Valkey`` client per (URL, options), held module-wide.
- Sync callers route coroutines via ``asyncio.run_coroutine_threadsafe`` and
  block on ``.result()``.
- Async callers route the same way but ``await asyncio.wrap_future(...)``.

This sits on top of the standard :class:`~django_cachex.adapters.valkey_py.ValkeyPyAdapter`
— all 100+ command methods inherited unchanged — by overriding only
``get_client`` / ``get_async_client`` to return facades.

Caveats:

- Pipeline objects returned by the underlying client are not wrapped; pipeline
  methods would still try to drive their connection on the calling loop.
  Out of scope for the spike.
- Cross-thread future overhead is real (~5-50μs per op on Linux); meant to be
  measured by the benchmarks, not assumed.
"""

import asyncio
import inspect
import threading
from typing import Any, Self

from django_cachex.adapters.valkey_py import (
    _VALKEY_AVAILABLE,
    ValkeyPyAdapter,
    _options_key,
)

if _VALKEY_AVAILABLE:
    from valkey.asyncio import Valkey as ValkeyAsyncClient
else:
    ValkeyAsyncClient = None  # type: ignore[assignment,misc]


# =============================================================================
# Process-wide daemon-thread event loop
# =============================================================================

_WORKER_LOOP: asyncio.AbstractEventLoop | None = None
_WORKER_LOCK = threading.Lock()


def _worker_loop() -> asyncio.AbstractEventLoop:
    """Lazy-init a single daemon-thread event loop for the process."""
    global _WORKER_LOOP  # noqa: PLW0603 — module-level lazy singleton
    if _WORKER_LOOP is not None:
        return _WORKER_LOOP
    with _WORKER_LOCK:
        if _WORKER_LOOP is not None:
            return _WORKER_LOOP
        loop = asyncio.new_event_loop()
        thread = threading.Thread(
            target=loop.run_forever,
            daemon=True,
            name="cachex-unified-worker",
        )
        thread.start()
        _WORKER_LOOP = loop
    return _WORKER_LOOP


# =============================================================================
# Process-wide unified client cache (one async Valkey per config)
# =============================================================================

_UNIFIED_CLIENTS: dict[tuple[Any, ...], Any] = {}
_UNIFIED_CLIENTS_LOCK = threading.Lock()

# Options that the upstream sync client takes but the async client doesn't —
# filter before constructing the unified client.
_SYNC_ONLY_OPTIONS = frozenset({"parser_class", "pool_class"})


def _unified_client(url: str, options: dict[str, Any]) -> Any:
    """Get-or-create the process-wide async client for ``(url, options)``."""
    if ValkeyAsyncClient is None:
        msg = "valkey-py is not installed; UnifiedValkeyPyAdapter requires it"
        raise ImportError(msg)
    async_options = {k: v for k, v in options.items() if k not in _SYNC_ONLY_OPTIONS}
    key = (url, _options_key(async_options))
    if key in _UNIFIED_CLIENTS:
        return _UNIFIED_CLIENTS[key]
    with _UNIFIED_CLIENTS_LOCK:
        if key in _UNIFIED_CLIENTS:
            return _UNIFIED_CLIENTS[key]
        client = ValkeyAsyncClient.from_url(url, **async_options)
        _UNIFIED_CLIENTS[key] = client
    return client


# =============================================================================
# Facades
# =============================================================================


class _UnifiedSyncFacade:
    """Sync-shaped wrapper around a ``valkey.asyncio.Valkey`` client.

    Each callable attribute returns a sync function that schedules the
    underlying coroutine on the worker loop and blocks on the result.
    Non-callable attributes pass through. The ``pipeline()`` method is
    special-cased to wrap the returned ``Pipeline`` in a sync facade so
    chained ops route through the worker loop too.
    """

    __slots__ = ("_c", "_loop")

    def __init__(self, async_client: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._c = async_client
        self._loop = loop

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._c, name)
        if not callable(attr):
            return attr
        loop = self._loop

        if name == "pipeline":

            def pipe_call(*args: Any, **kwargs: Any) -> _UnifiedSyncPipelineFacade:
                return _UnifiedSyncPipelineFacade(attr(*args, **kwargs), loop)

            return pipe_call

        def call(*args: Any, **kwargs: Any) -> Any:
            r = attr(*args, **kwargs)
            if asyncio.iscoroutine(r):
                fut = asyncio.run_coroutine_threadsafe(r, loop)
                return fut.result()
            return r

        return call


class _UnifiedAsyncPipelineFacade:
    """Wrap an async ``Pipeline``. Chaining methods (``mset``/``expire``/...)
    return the underlying pipeline synchronously — we re-wrap to ourselves
    so the chain stays inside the facade. Async methods (``execute``,
    ``discard``, ...) route their coroutines via the worker loop."""

    __slots__ = ("_loop", "_p")

    def __init__(self, pipe: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._p = pipe
        self._loop = loop

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._p, name)
        if not callable(attr):
            return attr
        loop = self._loop

        if inspect.iscoroutinefunction(attr):

            async def async_call(*args: Any, **kwargs: Any) -> Any:
                r = attr(*args, **kwargs)
                if asyncio.iscoroutine(r):
                    fut = asyncio.run_coroutine_threadsafe(r, loop)
                    return await asyncio.wrap_future(fut)
                return r

            return async_call

        underlying = self._p

        def chain_call(*args: Any, **kwargs: Any) -> Any:
            r = attr(*args, **kwargs)
            if r is underlying:
                return self
            return r

        return chain_call

    async def __aenter__(self) -> Self:
        fut = asyncio.run_coroutine_threadsafe(self._p.__aenter__(), self._loop)
        await asyncio.wrap_future(fut)
        return self

    async def __aexit__(self, *exc: object) -> Any:
        fut = asyncio.run_coroutine_threadsafe(self._p.__aexit__(*exc), self._loop)
        return await asyncio.wrap_future(fut)


class _UnifiedSyncPipelineFacade:
    """Sync variant — same idea, but ``execute()`` etc block on ``.result()``."""

    __slots__ = ("_loop", "_p")

    def __init__(self, pipe: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._p = pipe
        self._loop = loop

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._p, name)
        if not callable(attr):
            return attr
        loop = self._loop

        if inspect.iscoroutinefunction(attr):

            def async_call(*args: Any, **kwargs: Any) -> Any:
                r = attr(*args, **kwargs)
                if asyncio.iscoroutine(r):
                    fut = asyncio.run_coroutine_threadsafe(r, loop)
                    return fut.result()
                return r

            return async_call

        underlying = self._p

        def chain_call(*args: Any, **kwargs: Any) -> Any:
            r = attr(*args, **kwargs)
            if r is underlying:
                return self
            return r

        return chain_call


class _UnifiedAsyncIterator:
    """Drive each ``__anext__`` of an async generator on the worker loop.

    Async generators returned by methods like ``scan_iter`` perform I/O
    inside their bodies (``await self.scan(...)``). Iterating them on the
    calling loop would await on the wrong loop; routing each step via the
    worker loop keeps the generator's I/O on the same loop as the client.
    """

    __slots__ = ("_g", "_loop")

    def __init__(self, agen: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._g = agen
        self._loop = loop

    def __aiter__(self) -> _UnifiedAsyncIterator:
        return self

    async def __anext__(self) -> Any:
        fut = asyncio.run_coroutine_threadsafe(self._g.__anext__(), self._loop)
        return await asyncio.wrap_future(fut)

    async def aclose(self) -> None:
        if hasattr(self._g, "aclose"):
            fut = asyncio.run_coroutine_threadsafe(self._g.aclose(), self._loop)
            await asyncio.wrap_future(fut)


class _UnifiedAsyncFacade:
    """Async-shaped wrapper around a ``valkey.asyncio.Valkey`` client.

    Each callable attribute is wrapped in an ``async def`` that, when the
    underlying call returns a coroutine, schedules it on the worker loop
    and awaits the wrapped future. Async-generator methods like
    ``scan_iter`` are special-cased: the wrapper returns the generator
    immediately (no extra ``await``) but each ``__anext__`` is routed to
    the worker loop via :class:`_UnifiedAsyncIterator`.
    """

    __slots__ = ("_c", "_loop")

    def __init__(self, async_client: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._c = async_client
        self._loop = loop

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._c, name)
        if not callable(attr):
            return attr
        loop = self._loop

        if inspect.isasyncgenfunction(attr):
            # Adapter callers do ``async for x in client.method(...)`` —
            # no ``await`` — so the wrapper must return an async iterator
            # synchronously, not via ``async def``.
            def gen_call(*args: Any, **kwargs: Any) -> _UnifiedAsyncIterator:
                return _UnifiedAsyncIterator(attr(*args, **kwargs), loop)

            return gen_call

        if name == "pipeline":
            # ``pipeline()`` is sync on ``redis.asyncio.Redis``; wrap the
            # returned Pipeline so its chain stays inside our facade.
            def pipe_call(*args: Any, **kwargs: Any) -> _UnifiedAsyncPipelineFacade:
                return _UnifiedAsyncPipelineFacade(attr(*args, **kwargs), loop)

            return pipe_call

        # Default: ``async def`` wrapper that detects coroutine returns at
        # call time. We can't gate on ``inspect.iscoroutinefunction(attr)``
        # because ``valkey.asyncio`` defines ``set`` / ``get`` etc. as
        # plain ``def`` methods that *return* coroutines (commands are
        # generated by metaclass), so ``iscoroutinefunction`` is False
        # for them but they still need cross-loop routing.

        async def call(*args: Any, **kwargs: Any) -> Any:
            r = attr(*args, **kwargs)
            if asyncio.iscoroutine(r):
                fut = asyncio.run_coroutine_threadsafe(r, loop)
                return await asyncio.wrap_future(fut)
            return r

        return call


# =============================================================================
# Adapter
# =============================================================================


class UnifiedValkeyPyAdapter(ValkeyPyAdapter):
    """One async client per process; sync + async paths both route through it.

    Inherits the full command surface from :class:`ValkeyPyAdapter` — only
    the client-resolution methods change.
    """

    def get_client(self, key: str | None = None, *, write: bool = False) -> Any:
        del key, write
        async_client = _unified_client(self._servers[0], self._pool_options)
        return _UnifiedSyncFacade(async_client, _worker_loop())

    def get_async_client(self, key: str | None = None, *, write: bool = False) -> Any:
        del key, write
        async_client = _unified_client(self._servers[0], self._pool_options)
        return _UnifiedAsyncFacade(async_client, _worker_loop())


__all__ = ["UnifiedValkeyPyAdapter"]
