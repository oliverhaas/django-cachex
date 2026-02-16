"""Cluster cache backends for Redis-compatible backends."""

from __future__ import annotations

from typing import Any

from django_cachex.cache.default import KeyValueCache
from django_cachex.client.cluster import KeyValueClusterCacheClient


class KeyValueClusterCache(KeyValueCache):
    """Cluster cache backend base class.

    Extends KeyValueCache for cluster-specific behavior.
    Subclasses set `_class` class attribute to their specific ClusterCacheClient.
    """

    _class: type[KeyValueClusterCacheClient] = KeyValueClusterCacheClient


# Try to import Redis Cluster
try:
    from django_cachex.client.cluster import RedisClusterCacheClient

    class RedisClusterCache(KeyValueClusterCache):
        """Django cache backend for Redis Cluster mode.

        Provides automatic sharding across multiple Redis nodes using hash slots.
        """

        _class = RedisClusterCacheClient

except ImportError:

    class RedisClusterCache(KeyValueCache):  # type: ignore[no-redef]
        """Redis Cluster cache backend (requires redis-py to be installed)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "RedisClusterCache requires redis-py to be installed. Install it with: pip install redis",
            )


# Try to import Valkey Cluster
try:
    from django_cachex.client.cluster import ValkeyClusterCacheClient

    class ValkeyClusterCache(KeyValueClusterCache):
        """Django cache backend for Valkey Cluster mode.

        Provides automatic sharding across multiple Valkey nodes using hash slots.
        """

        _class = ValkeyClusterCacheClient

except ImportError:

    class ValkeyClusterCache(KeyValueCache):  # type: ignore[no-redef]
        """Valkey Cluster cache backend (requires valkey-py with cluster support)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "ValkeyClusterCache requires valkey-py with cluster support. Install it with: pip install valkey",
            )


__all__ = [
    "KeyValueClusterCache",
    "RedisClusterCache",
    "ValkeyClusterCache",
]
