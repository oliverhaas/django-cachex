"""Cluster cache backends for Redis-compatible backends."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django_cachex.adapter.cluster import BaseKeyValueClusterAdapter
from django_cachex.cache.default import KeyValueCache

if TYPE_CHECKING:
    from django_cachex.adapter.pipeline import Pipeline


class KeyValueClusterCache(KeyValueCache):
    """Cluster cache backend base class.

    Extends KeyValueCache for cluster-specific behavior.
    Subclasses set `_adapter_class` class attribute to their specific ClusterCacheClient.
    """

    _adapter_class: type[BaseKeyValueClusterAdapter] = BaseKeyValueClusterAdapter

    def pipeline(
        self,
        *,
        transaction: bool = True,
        version: int | None = None,
    ) -> Pipeline:
        """Create a pipeline. Cluster pipelines never use transactions."""
        return super().pipeline(transaction=False, version=version)


# Try to import Redis Cluster
try:
    from django_cachex.adapter.cluster import RedisClusterAdapter

    class RedisClusterCache(KeyValueClusterCache):
        """Django cache backend for Redis Cluster mode.

        Keys are sharded across nodes by hash slot.
        """

        _adapter_class = RedisClusterAdapter

except ImportError:

    class RedisClusterCache(KeyValueCache):  # type: ignore[no-redef]
        """Redis Cluster cache backend (requires redis-py to be installed)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "RedisClusterCache requires redis-py to be installed. Install it with: pip install redis",
            )


# Try to import Valkey Cluster
try:
    from django_cachex.adapter.cluster import ValkeyClusterAdapter

    class ValkeyClusterCache(KeyValueClusterCache):
        """Django cache backend for Valkey Cluster mode.

        Keys are sharded across nodes by hash slot.
        """

        _adapter_class = ValkeyClusterAdapter

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
