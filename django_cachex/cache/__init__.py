"""Cache backend classes for Django's ``CACHES`` setting."""

# Names resolve lazily (PEP 562) so a LocMem or Database BACKEND imports no driver.

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from django_cachex.cache.database import DatabaseCache
    from django_cachex.cache.locmem import LocMemCache
    from django_cachex.cache.redis_py import (
        RedisCache,
        RedisClusterCache,
        RedisSentinelCache,
    )
    from django_cachex.cache.resp import RespCache, RespClusterCache, RespSentinelCache
    from django_cachex.cache.stream import StreamCache
    from django_cachex.cache.tiered import TieredCache
    from django_cachex.cache.valkey_glide import ValkeyGlideCache, ValkeyGlideClusterCache
    from django_cachex.cache.valkey_py import (
        ValkeyCache,
        ValkeyClusterCache,
        ValkeySentinelCache,
    )

# Exported name -> defining submodule.
_LAZY_EXPORTS = {
    "DatabaseCache": "django_cachex.cache.database",
    "LocMemCache": "django_cachex.cache.locmem",
    "RedisCache": "django_cachex.cache.redis_py",
    "RedisClusterCache": "django_cachex.cache.redis_py",
    "RedisSentinelCache": "django_cachex.cache.redis_py",
    "RespCache": "django_cachex.cache.resp",
    "RespClusterCache": "django_cachex.cache.resp",
    "RespSentinelCache": "django_cachex.cache.resp",
    "StreamCache": "django_cachex.cache.stream",
    "TieredCache": "django_cachex.cache.tiered",
    "ValkeyCache": "django_cachex.cache.valkey_py",
    "ValkeyClusterCache": "django_cachex.cache.valkey_py",
    "ValkeyGlideCache": "django_cachex.cache.valkey_glide",
    "ValkeyGlideClusterCache": "django_cachex.cache.valkey_glide",
    "ValkeySentinelCache": "django_cachex.cache.valkey_py",
}


def __getattr__(name: str) -> Any:
    """Resolve a backend class on first access (PEP 562)."""
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)


__all__ = [
    "DatabaseCache",
    "LocMemCache",
    "RedisCache",
    "RedisClusterCache",
    "RedisSentinelCache",
    "RespCache",
    "RespClusterCache",
    "RespSentinelCache",
    "StreamCache",
    "TieredCache",
    "ValkeyCache",
    "ValkeyClusterCache",
    "ValkeyGlideCache",
    "ValkeyGlideClusterCache",
    "ValkeySentinelCache",
]
