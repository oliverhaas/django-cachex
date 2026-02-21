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
from django_cachex.cache.sentinel import (
    KeyValueSentinelCache,
    RedisSentinelCache,
    ValkeySentinelCache,
)
from django_cachex.cache.tiered import (
    RedisClusterTieredCache,
    RedisSentinelTieredCache,
    RedisTieredCache,
    TieredCacheMixin,
    ValkeyClusterTieredCache,
    ValkeySentinelTieredCache,
    ValkeyTieredCache,
)

__all__ = [
    "KeyValueCache",
    "KeyValueClusterCache",
    "KeyValueSentinelCache",
    "RedisCache",
    "RedisClusterCache",
    "RedisClusterTieredCache",
    "RedisSentinelCache",
    "RedisSentinelTieredCache",
    "RedisTieredCache",
    "TieredCacheMixin",
    "ValkeyCache",
    "ValkeyClusterCache",
    "ValkeyClusterTieredCache",
    "ValkeySentinelCache",
    "ValkeySentinelTieredCache",
    "ValkeyTieredCache",
]
