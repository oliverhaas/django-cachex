"""Cache module - provides cache backend classes.

These are the classes to use as BACKEND in Django's CACHES setting.
"""

from django_cachex.client.cache import (
    KeyValueCache,
    RedisCache,
    ValkeyCache,
)
from django_cachex.client.cluster import (
    RedisClusterCache,
    ValkeyClusterCache,
)
from django_cachex.client.sentinel import (
    RedisSentinelCache,
    ValkeySentinelCache,
)

__all__ = [
    "KeyValueCache",
    "RedisCache",
    "RedisClusterCache",
    "RedisSentinelCache",
    "ValkeyCache",
    "ValkeyClusterCache",
    "ValkeySentinelCache",
]
