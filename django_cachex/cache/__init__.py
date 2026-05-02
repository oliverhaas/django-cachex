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
from django_cachex.cache.mixin import CachexCompat
from django_cachex.cache.redis_rs import (
    RedisRsValkeyCache,
    RedisRsValkeyClusterCache,
    RedisRsValkeySentinelCache,
)
from django_cachex.cache.sentinel import (
    KeyValueSentinelCache,
    RedisSentinelCache,
    ValkeySentinelCache,
)
from django_cachex.cache.stream import StreamCache
from django_cachex.cache.tiered import TieredCache

__all__ = [
    "CachexCompat",
    "DatabaseCache",
    "KeyValueCache",
    "KeyValueClusterCache",
    "KeyValueSentinelCache",
    "LocMemCache",
    "RedisCache",
    "RedisClusterCache",
    "RedisRsValkeyCache",
    "RedisRsValkeyClusterCache",
    "RedisRsValkeySentinelCache",
    "RedisSentinelCache",
    "StreamCache",
    "TieredCache",
    "ValkeyCache",
    "ValkeyClusterCache",
    "ValkeyGlideCache",
    "ValkeySentinelCache",
]
