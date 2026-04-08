"""Cache module - provides cache backend classes.

These are the classes to use as BACKEND in Django's CACHES setting.
"""

from django_cachex.cache.cluster import (
    KeyValueClusterCache,
    RedisClusterCache,
    ValkeyClusterCache,
)
from django_cachex.cache.default import (
    KeyValueCache,
    RedisCache,
    ValkeyCache,
)
from django_cachex.cache.postgres import PostgreSQLCache
from django_cachex.cache.sentinel import (
    KeyValueSentinelCache,
    RedisSentinelCache,
    ValkeySentinelCache,
)
from django_cachex.cache.sync import SyncCache
from django_cachex.cache.tiered import TieredCache

__all__ = [
    "KeyValueCache",
    "KeyValueClusterCache",
    "KeyValueSentinelCache",
    "PostgreSQLCache",
    "RedisCache",
    "RedisClusterCache",
    "RedisSentinelCache",
    "SyncCache",
    "TieredCache",
    "ValkeyCache",
    "ValkeyClusterCache",
    "ValkeySentinelCache",
]
