"""Concrete adapters for the ``redis-py`` driver.

``redis-py`` and ``valkey-py`` share an API surface (``valkey-py`` is a fork);
the redis-py adapters here are thin subclasses that swap in the redis-py
classes via class slots and redirect the lib-availability check from
valkey-py to redis-py via :class:`_RedisPyMixin`. All command logic is
inherited from :class:`~django_cachex.adapters.valkey_py.ValkeyPyAdapter`
and friends.
"""

import weakref

from django_cachex.adapters.valkey_py import (
    AsyncPoolsRegistry,
    ValkeyPyAdapter,
    ValkeyPyAsyncPipelineAdapter,
    ValkeyPyClusterAdapter,
    ValkeyPyPipelineAdapter,
    ValkeyPySentinelAdapter,
)

# Process-wide async pool registry for the redis-py driver. Distinct from
# the valkey-py one in :mod:`~django_cachex.adapters.valkey_py` so each
# driver owns its own state — the two are never used together in practice,
# but keeping them isolated makes ownership obvious. Shared across all
# three redis-py topologies (single, sentinel, cluster).
_REDIS_ASYNC_POOLS: AsyncPoolsRegistry = weakref.WeakKeyDictionary()

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
        "redis-py is required for RedisPyAdapter (and friends). Install it with: pip install django-cachex[redis-py]",
    )


class _RedisPyMixin:
    """Redirects ValkeyPyAdapter's lib-availability check from valkey-py to redis-py.

    Mixed in (first) to every redis-py concrete class — ``RedisPyAdapter``,
    ``RedisPySentinelAdapter``, ``RedisPyClusterAdapter`` — so they raise
    an ImportError naming redis-py rather than valkey-py when their
    dependency is missing. Also points ``_async_pools`` at the redis-py
    registry so redis-py and valkey-py don't share pool state.
    """

    _LIB_AVAILABLE: bool = _REDIS_AVAILABLE
    _async_pools = _REDIS_ASYNC_POOLS

    @staticmethod
    def _missing_lib_error() -> ImportError:
        return _missing_redis()


class RedisPyPipelineAdapter(ValkeyPyPipelineAdapter):
    """Pipeline adapter for the redis-py driver.

    Empty subclass — redis-py and valkey-py share a pipeline API surface,
    so all the queueing logic is inherited from
    :class:`~django_cachex.adapters.valkey_py.ValkeyPyPipelineAdapter`.
    Exists for symmetry with :class:`RedisPyAdapter` so the class
    hierarchy mirrors the adapter hierarchy.
    """


class RedisPyAsyncPipelineAdapter(ValkeyPyAsyncPipelineAdapter):
    """Async pipeline adapter for the redis-py driver.

    Empty subclass — same rationale as :class:`RedisPyPipelineAdapter`.
    """


class RedisPyAdapter(_RedisPyMixin, ValkeyPyAdapter):
    """Single-node / replicated cache adapter using ``redis-py``."""

    if _REDIS_AVAILABLE:
        _lib = redis
        _client_class = redis.Redis
        _pool_class = redis.ConnectionPool
        _async_client_class = RedisAsyncClient
        _async_pool_class = RedisAsyncConnectionPool

    def pipeline(self, *, transaction: bool = True) -> RedisPyPipelineAdapter:
        client = self.get_client(write=True)
        return RedisPyPipelineAdapter(client.pipeline(transaction=transaction))

    def apipeline(self, *, transaction: bool = True) -> RedisPyAsyncPipelineAdapter:
        client = self._build_async_client_sync(write=True)
        return RedisPyAsyncPipelineAdapter(client.pipeline(transaction=transaction))


class RedisPySentinelAdapter(_RedisPyMixin, ValkeyPySentinelAdapter):
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

    def pipeline(self, *, transaction: bool = True) -> RedisPyPipelineAdapter:
        client = self.get_client(write=True)
        return RedisPyPipelineAdapter(client.pipeline(transaction=transaction))

    def apipeline(self, *, transaction: bool = True) -> RedisPyAsyncPipelineAdapter:
        client = self._build_async_client_sync(write=True)
        return RedisPyAsyncPipelineAdapter(client.pipeline(transaction=transaction))


class RedisPyClusterAdapter(_RedisPyMixin, ValkeyPyClusterAdapter):
    """Cluster cache adapter using ``redis-py``."""

    if _REDIS_AVAILABLE:
        _lib = redis
        _client_class = redis.Redis
        _pool_class = redis.ConnectionPool
        _cluster_class = RedisCluster
        _async_cluster_class = AsyncRedisCluster
        _key_slot_func = staticmethod(redis_key_slot)

    def pipeline(self, *, transaction: bool = True) -> RedisPyPipelineAdapter:
        client = self.get_client(write=True)
        return RedisPyPipelineAdapter(client.pipeline(transaction=False))

    def apipeline(self, *, transaction: bool = True) -> RedisPyAsyncPipelineAdapter:
        client = self._build_async_client_sync(write=True)
        return RedisPyAsyncPipelineAdapter(client.pipeline(transaction=False))


__all__ = [
    "_REDIS_AVAILABLE",
    "RedisPyAdapter",
    "RedisPyAsyncPipelineAdapter",
    "RedisPyClusterAdapter",
    "RedisPyPipelineAdapter",
    "RedisPySentinelAdapter",
]
