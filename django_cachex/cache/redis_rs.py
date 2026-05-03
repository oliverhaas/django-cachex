"""Django cache backends backed by the Rust ``RedisRsDriver``.

Each subclass differs from the corresponding pure-Python backend only in its
``_adapter_class`` attribute; every high-level cache method is inherited unchanged.
Users opt in via ``CACHES["default"]["BACKEND"]``.
"""

from django_cachex.adapters.redis_rs import (
    RedisRsValkeyAdapter,
    RedisRsValkeyClusterAdapter,
    RedisRsValkeySentinelAdapter,
)
from django_cachex.cache.key_value import KeyValueCache


class RedisRsValkeyCache(KeyValueCache):
    """Django cache backend using the Rust driver against a single node."""

    _cachex_support = "cachex"
    _adapter_class = RedisRsValkeyAdapter


class RedisRsValkeyClusterCache(KeyValueCache):
    """Rust-driven Valkey/Redis cluster backend."""

    _cachex_support = "cachex"
    _adapter_class = RedisRsValkeyClusterAdapter


class RedisRsValkeySentinelCache(KeyValueCache):
    """Rust-driven sentinel-managed Valkey/Redis backend."""

    _cachex_support = "cachex"
    _adapter_class = RedisRsValkeySentinelAdapter


__all__ = [
    "RedisRsValkeyCache",
    "RedisRsValkeyClusterCache",
    "RedisRsValkeySentinelCache",
]
