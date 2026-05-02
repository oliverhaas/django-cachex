"""Concrete adapters for the ``redis-py`` driver.

``redis-py`` and ``valkey-py`` share an API surface (``valkey-py`` is a fork);
each class here is a thin subclass of its
:mod:`django_cachex.adapter.valkey_py` counterpart that swaps in the
``redis-py`` lib + client/pool/sentinel/cluster classes. All behaviour comes
from the base classes in
:mod:`~django_cachex.adapter.default`,
:mod:`~django_cachex.adapter.sentinel`, and
:mod:`~django_cachex.adapter.cluster`.

If ``redis-py`` isn't installed, ``__init__`` raises a clean
:class:`ImportError`. ``valkey-py`` doesn't need to be installed for these to
work — :func:`__init__` jumps straight to ``BaseKeyValueAdapter.__init__``,
skipping the ``valkey-py`` availability check on the parent class.
"""

from __future__ import annotations

from typing import Any

from django_cachex.adapter.cluster import BaseKeyValueClusterAdapter
from django_cachex.adapter.default import BaseKeyValueAdapter
from django_cachex.adapter.sentinel import BaseKeyValueSentinelAdapter
from django_cachex.adapter.valkey_py import (
    ValkeyAdapter,
    ValkeyClusterAdapter,
    ValkeySentinelAdapter,
)

_REDIS_AVAILABLE = False
try:
    import redis
    from redis.asyncio import ConnectionPool as RedisAsyncConnectionPool
    from redis.asyncio import Redis as RedisAsyncClient
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.asyncio.sentinel import Sentinel as AsyncRedisSentinel
    from redis.asyncio.sentinel import SentinelConnectionPool as AsyncRedisSentinelConnectionPool
    from redis.cluster import RedisCluster
    from redis.cluster import key_slot as redis_key_slot
    from redis.sentinel import Sentinel as RedisSentinel
    from redis.sentinel import SentinelConnectionPool as RedisSentinelConnectionPool

    _REDIS_AVAILABLE = True
except ImportError:
    redis = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]


def _missing_redis() -> ImportError:
    return ImportError(
        "redis-py is required for RedisAdapter (and friends). Install it with: pip install django-cachex[redis-py]",
    )


class RedisAdapter(ValkeyAdapter):
    """Single-node / replicated cache adapter using ``redis-py``."""

    if _REDIS_AVAILABLE:
        _lib = redis
        _client_class = redis.Redis
        _pool_class = redis.ConnectionPool
        _async_client_class = RedisAsyncClient
        _async_pool_class = RedisAsyncConnectionPool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if not _REDIS_AVAILABLE:
            raise _missing_redis()
        # Skip ValkeyAdapter.__init__ (which would re-check valkey-py).
        BaseKeyValueAdapter.__init__(self, *args, **kwargs)


class RedisSentinelAdapter(ValkeySentinelAdapter):
    """Sentinel-managed cache adapter using ``redis-py``."""

    if _REDIS_AVAILABLE:
        _lib = redis
        _client_class = redis.Redis
        _pool_class = RedisSentinelConnectionPool
        _sentinel_class = RedisSentinel
        _sentinel_pool_class = RedisSentinelConnectionPool
        _async_client_class = RedisAsyncClient
        _async_sentinel_class = AsyncRedisSentinel
        _async_sentinel_pool_class = AsyncRedisSentinelConnectionPool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if not _REDIS_AVAILABLE:
            raise _missing_redis()
        BaseKeyValueSentinelAdapter.__init__(self, *args, **kwargs)


class RedisClusterAdapter(ValkeyClusterAdapter):
    """Cluster cache adapter using ``redis-py``."""

    if _REDIS_AVAILABLE:
        _lib = redis
        _client_class = redis.Redis
        _pool_class = redis.ConnectionPool
        _cluster_class = RedisCluster
        _async_cluster_class = AsyncRedisCluster
        _key_slot_func = staticmethod(redis_key_slot)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if not _REDIS_AVAILABLE:
            raise _missing_redis()
        BaseKeyValueClusterAdapter.__init__(self, *args, **kwargs)


__all__ = [
    "_REDIS_AVAILABLE",
    "RedisAdapter",
    "RedisClusterAdapter",
    "RedisSentinelAdapter",
]
