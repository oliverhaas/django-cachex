"""Concrete adapters for the ``valkey-py`` driver.

Three topologies live here:

- :class:`ValkeyAdapter` — single-node / replicated (extends
  :class:`~django_cachex.adapters.default.BaseKeyValueAdapter`).
- :class:`ValkeySentinelAdapter` — Sentinel-discovered primary/replicas
  (extends :class:`~django_cachex.adapters.sentinel.BaseKeyValueSentinelAdapter`).
- :class:`ValkeyClusterAdapter` — Cluster mode
  (extends :class:`~django_cachex.adapters.cluster.BaseKeyValueClusterAdapter`).

Each class fills in the lib-specific ``_lib`` / ``_*_class`` slots that the
base classes consume; behaviour is otherwise inherited verbatim. See
:mod:`django_cachex.adapters.redis_py` for the parallel set built on
``redis-py`` (a thin subclass layer — ``redis-py`` and ``valkey-py`` share an
API).

If ``valkey-py`` isn't installed, ``__init__`` raises a clean
:class:`ImportError` instead of failing later with an obscure attribute
error from the parent classes.
"""

from __future__ import annotations

from typing import Any

from django_cachex.adapters.cluster import BaseKeyValueClusterAdapter
from django_cachex.adapters.default import BaseKeyValueAdapter
from django_cachex.adapters.sentinel import BaseKeyValueSentinelAdapter

_VALKEY_AVAILABLE = False
try:
    import valkey
    from valkey.asyncio import ConnectionPool as ValkeyAsyncConnectionPool
    from valkey.asyncio import Valkey as ValkeyAsyncClient
    from valkey.asyncio.cluster import ValkeyCluster as AsyncValkeyCluster
    from valkey.asyncio.sentinel import Sentinel as AsyncValkeySentinel
    from valkey.asyncio.sentinel import SentinelConnectionPool as AsyncValkeySentinelConnectionPool
    from valkey.cluster import ValkeyCluster
    from valkey.cluster import key_slot as valkey_key_slot
    from valkey.sentinel import Sentinel as ValkeySentinel
    from valkey.sentinel import SentinelConnectionPool as ValkeySentinelConnectionPool

    _VALKEY_AVAILABLE = True
except ImportError:
    valkey = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]


def _missing_valkey() -> ImportError:
    return ImportError(
        "valkey-py is required for ValkeyAdapter (and friends). Install it with: pip install django-cachex[valkey-py]",
    )


class ValkeyAdapter(BaseKeyValueAdapter):
    """Single-node / replicated cache adapter using ``valkey-py``."""

    # Annotations widen the inferred narrow types so the redis-py thin layer
    # in ``redis_py.py`` can override with ``type[redis.Redis]`` etc. without
    # tripping mypy's ``incompatible-override`` check. (The base class
    # already declares them as ``type[Any] | None``; we re-annotate to keep
    # mypy from narrowing on the redis-py subclasses.)
    _lib: Any
    _client_class: type[Any]
    _pool_class: type[Any]
    _async_client_class: type[Any]
    _async_pool_class: type[Any]

    if _VALKEY_AVAILABLE:
        _lib = valkey
        _client_class = valkey.Valkey
        _pool_class = valkey.ConnectionPool
        _async_client_class = ValkeyAsyncClient
        _async_pool_class = ValkeyAsyncConnectionPool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if not _VALKEY_AVAILABLE:
            raise _missing_valkey()
        super().__init__(*args, **kwargs)


class ValkeySentinelAdapter(BaseKeyValueSentinelAdapter):
    """Sentinel-managed cache adapter using ``valkey-py``."""

    _lib: Any
    _client_class: type[Any]
    _pool_class: type[Any]
    _sentinel_class: type[Any]
    _sentinel_pool_class: type[Any]
    _async_client_class: type[Any]
    _async_sentinel_class: type[Any]
    _async_sentinel_pool_class: type[Any]

    if _VALKEY_AVAILABLE:
        _lib = valkey
        _client_class = valkey.Valkey
        _pool_class = ValkeySentinelConnectionPool
        _sentinel_class = ValkeySentinel
        _sentinel_pool_class = ValkeySentinelConnectionPool
        _async_client_class = ValkeyAsyncClient
        _async_sentinel_class = AsyncValkeySentinel
        _async_sentinel_pool_class = AsyncValkeySentinelConnectionPool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if not _VALKEY_AVAILABLE:
            raise _missing_valkey()
        super().__init__(*args, **kwargs)


class ValkeyClusterAdapter(BaseKeyValueClusterAdapter):
    """Cluster cache adapter using ``valkey-py``."""

    _lib: Any
    _client_class: type[Any]
    _pool_class: type[Any]
    _cluster_class: type[Any]
    _async_cluster_class: type[Any]
    _key_slot_func: Any

    if _VALKEY_AVAILABLE:
        _lib = valkey
        # Required by base, unused for cluster.
        _client_class = valkey.Valkey
        _pool_class = valkey.ConnectionPool
        _cluster_class = ValkeyCluster
        _async_cluster_class = AsyncValkeyCluster
        _key_slot_func = staticmethod(valkey_key_slot)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if not _VALKEY_AVAILABLE:
            raise _missing_valkey()
        super().__init__(*args, **kwargs)


__all__ = [
    "_VALKEY_AVAILABLE",
    "ValkeyAdapter",
    "ValkeyClusterAdapter",
    "ValkeySentinelAdapter",
]
