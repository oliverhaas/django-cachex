"""Cache module - provides cache backend classes.

These are the classes to use as BACKEND in Django's CACHES setting.
"""

from django_cachex.client.cache import (
    KeyValueCache,
    RedisCache,
    ValkeyCache,
)

__all__ = [
    "KeyValueCache",
    "RedisCache",
    "ValkeyCache",
]
