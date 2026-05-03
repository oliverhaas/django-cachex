"""Concrete cache backends for the ``redis-py`` driver.

Three topologies live here, each a one-line subclass that swaps in the
matching ``redis-py`` adapter:

- :class:`RedisCache` — single-node / replicated.
- :class:`RedisSentinelCache` — Sentinel-managed primary/replicas.
- :class:`RedisClusterCache` — Cluster mode.

See :mod:`django_cachex.cache.valkey_py` for the parallel set built on
``valkey-py`` (the two libraries share an API surface).
"""

from django_cachex.adapters.redis_py import (
    RedisPyAdapter,
    RedisPyClusterAdapter,
    RedisPySentinelAdapter,
)
from django_cachex.cache.resp import RespCache, RespClusterCache, RespSentinelCache


class RedisCache(RespCache):
    """Django cache backend using the ``redis-py`` library."""

    _adapter_class = RedisPyAdapter


class RedisSentinelCache(RespSentinelCache):
    """Django cache backend for Redis Sentinel high availability (redis-py).

    Failover and service discovery happen through Redis Sentinel; the
    ``LOCATION`` hostname is the Sentinel service name. Raises
    :class:`ImportError` on instantiation if ``redis-py`` isn't installed.
    """

    _adapter_class = RedisPySentinelAdapter


class RedisClusterCache(RespClusterCache):
    """Django cache backend for Redis Cluster mode (redis-py).

    Keys are sharded across nodes by hash slot. Raises :class:`ImportError`
    on instantiation if ``redis-py`` isn't installed.
    """

    _adapter_class = RedisPyClusterAdapter


__all__ = [
    "RedisCache",
    "RedisClusterCache",
    "RedisSentinelCache",
]
