"""Django cache backends backed by the Rust ``RustValkeyDriver``.

Each subclass differs from the corresponding pure-Python backend only in its
``_class`` attribute; every high-level cache method is inherited unchanged.
Users opt in via ``CACHES["default"]["BACKEND"]``.
"""

from __future__ import annotations

from django_cachex.cache.default import KeyValueCache
from django_cachex.client.rust import (
    RustKeyValueCacheClient,
    RustValkeyClusterCacheClient,
    RustValkeySentinelCacheClient,
)


class RustValkeyCache(KeyValueCache):
    """Django cache backend using the Rust driver against a single node."""

    _cachex_support = "cachex"
    _class = RustKeyValueCacheClient


class RustRedisCache(RustValkeyCache):
    """Alias for parity with redis-py-backed naming. Driver is the same."""


class RustValkeyClusterCache(KeyValueCache):
    """Rust-driven Valkey/Redis cluster backend."""

    _cachex_support = "cachex"
    _class = RustValkeyClusterCacheClient


class RustRedisClusterCache(RustValkeyClusterCache):
    """Alias for parity with redis-py-backed naming."""


class RustValkeySentinelCache(KeyValueCache):
    """Rust-driven sentinel-managed Valkey/Redis backend."""

    _cachex_support = "cachex"
    _class = RustValkeySentinelCacheClient


class RustRedisSentinelCache(RustValkeySentinelCache):
    """Alias for parity with redis-py-backed naming."""


__all__ = [
    "RustRedisCache",
    "RustRedisClusterCache",
    "RustRedisSentinelCache",
    "RustValkeyCache",
    "RustValkeyClusterCache",
    "RustValkeySentinelCache",
]
