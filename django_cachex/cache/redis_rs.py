"""Django cache backends backed by the Rust adapter.

Each subclass differs from the corresponding pure-Python backend only in its
``_adapter_class`` attribute; every high-level cache method is inherited unchanged.
Users opt in via ``CACHES["default"]["BACKEND"]``.
"""

from django_cachex.adapters.redis_rs import (
    RedisRsAdapter,
    RedisRsClusterAdapter,
    RedisRsSentinelAdapter,
)
from django_cachex.cache.resp import RespCache, RespClusterCache, RespSentinelCache


class RedisRsCache(RespCache):
    """Django cache backend using the Rust adapter against a single node."""

    _cachex_support = "cachex"
    _adapter_class = RedisRsAdapter


class RedisRsClusterCache(RespClusterCache):
    """Rust adapter for Valkey/Redis cluster mode."""

    _cachex_support = "cachex"
    _adapter_class = RedisRsClusterAdapter


class RedisRsSentinelCache(RespSentinelCache):
    """Rust adapter for sentinel-managed Valkey/Redis topologies."""

    _cachex_support = "cachex"
    _adapter_class = RedisRsSentinelAdapter


__all__ = [
    "RedisRsCache",
    "RedisRsClusterCache",
    "RedisRsSentinelCache",
]
