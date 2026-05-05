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
import threading
from typing import Any

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
    Non-callable attributes pass through.
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

        def call(*args: Any, **kwargs: Any) -> Any:
            r = attr(*args, **kwargs)
            if asyncio.iscoroutine(r):
                fut = asyncio.run_coroutine_threadsafe(r, loop)
                return fut.result()
            return r

        return call


class _UnifiedAsyncFacade:
    """Async-shaped wrapper around a ``valkey.asyncio.Valkey`` client.

    Each callable attribute is wrapped in an ``async def`` that, when the
    underlying call returns a coroutine, schedules it on the worker loop
    and awaits the wrapped future. Lets adapter methods written against
    ``redis.asyncio`` semantics work unchanged.
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
