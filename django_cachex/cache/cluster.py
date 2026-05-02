"""Cluster cache backends for Redis-compatible backends."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django_cachex.adapters.cluster import BaseKeyValueClusterAdapter
from django_cachex.adapters.redis_py import RedisClusterAdapter
from django_cachex.adapters.valkey_py import ValkeyClusterAdapter
from django_cachex.cache.default import KeyValueCache

if TYPE_CHECKING:
    from django_cachex.adapters.pipeline import Pipeline


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


class RedisClusterCache(KeyValueClusterCache):
    """Django cache backend for Redis Cluster mode (redis-py).

    Keys are sharded across nodes by hash slot. Raises :class:`ImportError`
    on instantiation if ``redis-py`` isn't installed.
    """

    _adapter_class = RedisClusterAdapter


class ValkeyClusterCache(KeyValueClusterCache):
    """Django cache backend for Valkey Cluster mode (valkey-py).

    Keys are sharded across nodes by hash slot. Raises :class:`ImportError`
    on instantiation if ``valkey-py`` isn't installed.
    """

    _adapter_class = ValkeyClusterAdapter


__all__ = [
    "KeyValueClusterCache",
    "RedisClusterCache",
    "ValkeyClusterCache",
]
