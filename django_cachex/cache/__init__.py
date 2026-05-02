"""Cache module - provides cache backend classes.

These are the classes to use as BACKEND in Django's CACHES setting.
"""

from django_cachex.cache.cluster import (
    KeyValueClusterCache,
    RedisClusterCache,
    ValkeyClusterCache,
)
from django_cachex.cache.database import DatabaseCache
from django_cachex.cache.default import (
    KeyValueCache,
    RedisCache,
    ValkeyCache,
)
from django_cachex.cache.glide import ValkeyGlideCache
from django_cachex.cache.locmem import LocMemCache
from django_cachex.cache.mixin import CachexMixin
from django_cachex.cache.rust import (
    RustRedisCache,
    RustRedisClusterCache,
    RustRedisSentinelCache,
    RustValkeyCache,
    RustValkeyClusterCache,
    RustValkeySentinelCache,
)
from django_cachex.cache.sentinel import (
    KeyValueSentinelCache,
    RedisSentinelCache,
    ValkeySentinelCache,
)
from django_cachex.cache.stream import StreamCache
from django_cachex.cache.tiered import TieredCache

__all__ = [
    "CachexMixin",
    "DatabaseCache",
    "KeyValueCache",
    "KeyValueClusterCache",
    "KeyValueSentinelCache",
    "LocMemCache",
    "RedisCache",
    "RedisClusterCache",
    "RedisSentinelCache",
    "RustRedisCache",
    "RustRedisClusterCache",
    "RustRedisSentinelCache",
    "RustValkeyCache",
    "RustValkeyClusterCache",
    "RustValkeySentinelCache",
    "StreamCache",
    "TieredCache",
    "ValkeyCache",
    "ValkeyClusterCache",
    "ValkeyGlideCache",
    "ValkeySentinelCache",
]
