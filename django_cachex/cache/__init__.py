"""Cache module - provides cache backend classes.

These are the classes to use as BACKEND in Django's CACHES setting.
"""

from django_cachex.cache.compat import CachexCompat
from django_cachex.cache.database import DatabaseCache
from django_cachex.cache.locmem import LocMemCache
from django_cachex.cache.redis_py import (
    RedisCache,
    RedisClusterCache,
    RedisSentinelCache,
)
from django_cachex.cache.redis_rs import (
    RedisRsCache,
    RedisRsClusterCache,
    RedisRsSentinelCache,
)
from django_cachex.cache.resp import RespCache, RespClusterCache, RespSentinelCache
from django_cachex.cache.stream import StreamCache
from django_cachex.cache.tiered import TieredCache
from django_cachex.cache.valkey_glide import ValkeyGlideCache
from django_cachex.cache.valkey_py import (
    ValkeyCache,
    ValkeyClusterCache,
    ValkeySentinelCache,
)

__all__ = [
    "CachexCompat",
    "DatabaseCache",
    "LocMemCache",
    "RedisCache",
    "RedisClusterCache",
    "RedisRsCache",
    "RedisRsClusterCache",
    "RedisRsSentinelCache",
    "RedisSentinelCache",
    "RespCache",
    "RespClusterCache",
    "RespSentinelCache",
    "StreamCache",
    "TieredCache",
    "ValkeyCache",
    "ValkeyClusterCache",
    "ValkeyGlideCache",
    "ValkeySentinelCache",
]
