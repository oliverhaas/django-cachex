"""Django cache backends backed by the Rust ``RedisRsDriver``.

Each subclass differs from the corresponding pure-Python backend only in its
``_adapter_class`` attribute; every high-level cache method is inherited unchanged.
Users opt in via ``CACHES["default"]["BACKEND"]``.
"""

from django_cachex.adapters.redis_rs import (
    RedisRsAdapter,
    RedisRsClusterAdapter,
    RedisRsSentinelAdapter,
)
from django_cachex.cache.resp import RespCache


class RedisRsCache(RespCache):
    """Django cache backend using the Rust driver against a single node."""

    _cachex_support = "cachex"
    _adapter_class = RedisRsAdapter


class RedisRsClusterCache(RespCache):
    """Rust-driven Valkey/Redis cluster backend."""

    _cachex_support = "cachex"
    _adapter_class = RedisRsClusterAdapter


class RedisRsSentinelCache(RespCache):
    """Rust-driven sentinel-managed Valkey/Redis backend."""

    _cachex_support = "cachex"
    _adapter_class = RedisRsSentinelAdapter


__all__ = [
    "RedisRsCache",
    "RedisRsClusterCache",
    "RedisRsSentinelCache",
]
