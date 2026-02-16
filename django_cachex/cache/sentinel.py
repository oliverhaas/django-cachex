"""Sentinel cache backends for Redis-compatible backends."""

from __future__ import annotations

from typing import Any

from django_cachex.cache.default import KeyValueCache, ValkeyCache
from django_cachex.client.sentinel import _REDIS_AVAILABLE, _VALKEY_AVAILABLE, KeyValueSentinelCacheClient


class KeyValueSentinelCache(KeyValueCache):
    """Sentinel cache backend base class.

    Extends KeyValueCache for sentinel-specific behavior.
    Subclasses set `_class` class attribute to their specific SentinelCacheClient.
    """

    _class: type[KeyValueSentinelCacheClient] = KeyValueSentinelCacheClient


# Redis Sentinel
if _REDIS_AVAILABLE:
    from django_cachex.client.sentinel import RedisSentinelCacheClient

    class RedisSentinelCache(KeyValueSentinelCache):
        """Django cache backend for Redis Sentinel high availability.

        Provides automatic failover and service discovery via Redis Sentinel.
        LOCATION should use the Sentinel service name as the hostname.
        """

        _class = RedisSentinelCacheClient

else:

    class RedisSentinelCache(KeyValueCache):  # type: ignore[no-redef]
        """Redis Sentinel cache backend (requires redis-py to be installed)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "RedisSentinelCache requires redis-py to be installed. Install it with: pip install redis",
            )


# Valkey Sentinel
if _VALKEY_AVAILABLE:
    from django_cachex.client.sentinel import ValkeySentinelCacheClient

    class ValkeySentinelCache(KeyValueSentinelCache):
        """Django cache backend for Valkey Sentinel high availability.

        Provides automatic failover and service discovery via Valkey Sentinel.
        LOCATION should use the Sentinel service name as the hostname.
        """

        _class = ValkeySentinelCacheClient

else:

    class ValkeySentinelCache(ValkeyCache):  # type: ignore[no-redef]
        """Valkey Sentinel cache backend (requires valkey-py to be installed)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "ValkeySentinelCache requires valkey-py to be installed. Install it with: pip install valkey",
            )


__all__ = [
    "KeyValueSentinelCache",
    "RedisSentinelCache",
    "ValkeySentinelCache",
]
