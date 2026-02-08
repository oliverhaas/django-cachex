"""Cluster cache backend and client for Redis-compatible backends.

This module provides cache backends for Redis Cluster mode, handling
server-side sharding and slot-aware operations.

Architecture (matching Django's RedisCache structure):
- KeyValueClusterCacheClient(KeyValueCacheClient): Base class with cluster handling
- RedisClusterCacheClient: Sets class attributes for redis-py cluster
- ValkeyClusterCacheClient: Sets class attributes for valkey-py cluster
- KeyValueClusterCache(KeyValueCache): Base cache backend
- RedisClusterCache: Sets _class = RedisClusterCacheClient
- ValkeyClusterCache: Sets _class = ValkeyClusterCacheClient
"""

from __future__ import annotations

import asyncio
import weakref
from typing import TYPE_CHECKING, Any, cast, override

from django_cachex.client.cache import KeyValueCache
from django_cachex.client.default import KeyValueCacheClient

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence

    from django_cachex.client.pipeline import Pipeline
    from django_cachex.types import KeyT

# Known options that shouldn't be passed to the cluster client
_KNOWN_OPTIONS = frozenset(
    {
        "sentinels",
        "sentinel_kwargs",
        "compressor",
        "serializer",
        "ignore_exceptions",
        "log_ignored_exceptions",
    },
)


# =============================================================================
# CacheClient Classes (actual Redis operations)
# =============================================================================


class KeyValueClusterCacheClient(KeyValueCacheClient):
    """Cluster cache client base class.

    Extends KeyValueCacheClient with cluster-specific handling for
    server-side sharding and slot-aware operations.

    Subclasses must set:
    - _lib: The library module (redis or valkey)
    - _client_class: Not used for cluster (cluster manages connections)
    - _pool_class: Not used for cluster
    - _cluster_class: The cluster class (RedisCluster or ValkeyCluster)
    - _async_cluster_class: The async cluster class (async RedisCluster or ValkeyCluster)
    - _key_slot_func: Function to calculate key slot
    """

    # Subclasses must set these
    _cluster_class: type[Any] | None = None
    _async_cluster_class: type[Any] | None = None
    _key_slot_func: Any = None  # Function to calculate key slot

    @override
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Per-instance cluster (cluster manages its own connection pool)
        self._cluster_instance: Any | None = None
        # Per-instance async clusters: WeakKeyDictionary keyed by event loop
        self._async_cluster_instances: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop,
            Any,
        ] = weakref.WeakKeyDictionary()

    @property
    def _cluster(self) -> type[Any]:
        """Get the cluster class, asserting it's configured."""
        assert self._cluster_class is not None, "Subclasses must set _cluster_class"  # noqa: S101
        return self._cluster_class

    @property
    def _async_cluster(self) -> type[Any]:
        """Get the async cluster class, asserting it's configured."""
        assert self._async_cluster_class is not None, "Subclasses must set _async_cluster_class"  # noqa: S101
        return self._async_cluster_class

    @override
    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the Cluster client.

        Cluster topology discovery happens lazily on first access.
        """
        if self._cluster_instance is not None:
            return self._cluster_instance

        from urllib.parse import urlparse

        url = self._servers[0]
        parsed_url = urlparse(url)
        # Pass through options
        cluster_options = {key_opt: value for key_opt, value in self._options.items() if key_opt not in _KNOWN_OPTIONS}

        if parsed_url.hostname:
            cluster_options["host"] = parsed_url.hostname
        if parsed_url.port:
            cluster_options["port"] = parsed_url.port

        self._cluster_instance = self._cluster(**cluster_options)
        return self._cluster_instance

    @override
    def get_async_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the async Cluster client for the current event loop.

        Cluster topology discovery happens lazily on first access.
        """
        loop = asyncio.get_running_loop()

        # Check if we already have an async cluster for this loop
        if loop in self._async_cluster_instances:
            return self._async_cluster_instances[loop]

        from urllib.parse import urlparse

        url = self._servers[0]
        parsed_url = urlparse(url)
        # Pass through options
        cluster_options = {key_opt: value for key_opt, value in self._options.items() if key_opt not in _KNOWN_OPTIONS}

        if parsed_url.hostname:
            cluster_options["host"] = parsed_url.hostname
        if parsed_url.port:
            cluster_options["port"] = parsed_url.port

        cluster = self._async_cluster(**cluster_options)
        self._async_cluster_instances[loop] = cluster

        return cluster

    def _group_keys_by_slot(self, keys: Iterable[KeyT]) -> dict[int, list[KeyT]]:
        """Group keys by their cluster slot."""
        from collections import defaultdict

        slots: dict[int, list[KeyT]] = defaultdict(list)
        for key in keys:
            key_bytes = key.encode() if isinstance(key, str) else key
            slot = self._key_slot_func(key_bytes)
            slots[slot].append(key)
        return dict(slots)

    # Override methods that need cluster-specific handling

    @override
    def get_many(self, keys: Iterable[KeyT]) -> dict[KeyT, Any]:
        """Retrieve many keys, handling cross-slot keys."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_client(write=False)
        # mget_nonatomic handles slot splitting
        results = cast(
            "list[bytes | None]",
            client.mget_nonatomic(keys),
        )

        recovered_data = {}
        for key, value in zip(keys, results, strict=True):
            if value is not None:
                recovered_data[key] = self.decode(value)

        return recovered_data

    @override
    def set_many(self, data: Mapping[KeyT, Any], timeout: float | None = None) -> list[KeyT]:
        """Set multiple values, handling cross-slot keys."""
        if not data:
            return []

        client = self.get_client(write=True)

        # Prepare data with encoded values
        prepared_data = {k: self.encode(v) for k, v in data.items()}

        # mset_nonatomic handles slot splitting
        client.mset_nonatomic(prepared_data)

        # Set expiry if needed
        if timeout is not None:
            timeout_ms = int(timeout * 1000)
            if timeout_ms > 0:
                pipe = client.pipeline()
                for key in prepared_data:
                    pipe.pexpire(key, timeout_ms)
                pipe.execute()
        return []

    @override
    def delete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys, grouping by slot."""
        if not keys:
            return 0

        client = self.get_client(write=True)

        # Group keys by slot
        slots = self._group_keys_by_slot(keys)

        total_deleted = 0
        for slot_keys in slots.values():
            total_deleted += cast("int", client.delete(*slot_keys))
        return total_deleted

    @override
    def clear(self) -> bool:
        """Flush all primary nodes in the cluster."""
        client = self.get_client(write=True)

        # Use PRIMARIES constant from the cluster class
        client.flushdb(target_nodes=self._cluster.PRIMARIES)
        return True

    @override
    def keys(self, pattern: str) -> list[str]:
        """Execute KEYS command across all primary nodes (pattern is already prefixed)."""
        client = self.get_client(write=False)

        keys_result = cast(
            "list[bytes]",
            client.keys(pattern, target_nodes=self._cluster.PRIMARIES),
        )
        return [k.decode() for k in keys_result]

    @override
    def iter_keys(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> Iterator[str]:
        """Iterate keys matching pattern across all primary nodes (pattern is already prefixed)."""
        client = self.get_client(write=False)

        if itersize is None:
            itersize = self._default_scan_itersize

        for item in client.scan_iter(
            match=pattern,
            count=itersize,
            target_nodes=self._cluster.PRIMARIES,
        ):
            yield item.decode()

    @override
    def delete_pattern(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> int:
        """Remove all keys matching pattern across all primary nodes (pattern is already prefixed)."""
        client = self.get_client(write=True)

        if itersize is None:
            itersize = self._default_scan_itersize

        # Collect all matching keys from all primaries
        keys_list = list(
            client.scan_iter(
                match=pattern,
                count=itersize,
                target_nodes=self._cluster.PRIMARIES,
            ),
        )

        if not keys_list:
            return 0

        # Group keys by slot for efficient deletion
        slots = self._group_keys_by_slot(keys_list)

        total_deleted = 0
        for slot_keys in slots.values():
            total_deleted += cast("int", client.delete(*slot_keys))
        return total_deleted

    @override
    def close(self, **kwargs: Any) -> None:
        """No-op. Cluster lives for the instance's lifetime (matches Django's BaseCache)."""

    # =========================================================================
    # Async Override Methods
    # =========================================================================

    @override
    async def aget_many(self, keys: Iterable[KeyT]) -> dict[KeyT, Any]:
        """Retrieve many keys asynchronously, handling cross-slot keys."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_async_client(write=False)

        # mget_nonatomic handles slot splitting
        results = cast(
            "list[bytes | None]",
            await client.mget_nonatomic(keys),
        )

        recovered_data = {}
        for key, value in zip(keys, results, strict=True):
            if value is not None:
                recovered_data[key] = self.decode(value)

        return recovered_data

    @override
    async def aset_many(self, data: Mapping[KeyT, Any], timeout: float | None = None) -> list[KeyT]:
        """Set multiple values asynchronously, handling cross-slot keys."""
        if not data:
            return []

        client = self.get_async_client(write=True)

        # Prepare data with encoded values
        prepared_data = {k: self.encode(v) for k, v in data.items()}

        # mset_nonatomic handles slot splitting
        await client.mset_nonatomic(prepared_data)

        # Set expiry if needed
        if timeout is not None:
            timeout_ms = int(timeout * 1000)
            if timeout_ms > 0:
                pipe = client.pipeline()
                for key in prepared_data:
                    pipe.pexpire(key, timeout_ms)
                await pipe.execute()
        return []

    @override
    async def adelete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys asynchronously, grouping by slot."""
        if not keys:
            return 0

        client = self.get_async_client(write=True)

        # Group keys by slot
        slots = self._group_keys_by_slot(keys)

        total_deleted = 0
        for slot_keys in slots.values():
            total_deleted += cast("int", await client.delete(*slot_keys))
        return total_deleted

    @override
    async def aclear(self) -> bool:
        """Flush all primary nodes in the cluster asynchronously."""
        client = self.get_async_client(write=True)

        # Use PRIMARIES constant from the cluster class
        await client.flushdb(target_nodes=self._async_cluster.PRIMARIES)
        return True

    @override
    async def akeys(self, pattern: str) -> list[str]:
        """Execute KEYS command asynchronously across all primary nodes."""
        client = self.get_async_client(write=False)

        keys_result = cast(
            "list[bytes]",
            await client.keys(pattern, target_nodes=self._async_cluster.PRIMARIES),
        )
        return [k.decode() for k in keys_result]

    @override
    async def aiter_keys(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> AsyncIterator[str]:
        """Iterate keys matching pattern asynchronously across all primary nodes."""
        client = self.get_async_client(write=False)

        if itersize is None:
            itersize = self._default_scan_itersize

        async for item in client.scan_iter(
            match=pattern,
            count=itersize,
            target_nodes=self._async_cluster.PRIMARIES,
        ):
            yield item.decode()

    @override
    async def adelete_pattern(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> int:
        """Remove all keys matching pattern asynchronously across all primary nodes."""
        client = self.get_async_client(write=True)

        if itersize is None:
            itersize = self._default_scan_itersize

        # Collect all matching keys from all primaries
        keys_list = [
            key
            async for key in client.scan_iter(
                match=pattern,
                count=itersize,
                target_nodes=self._async_cluster.PRIMARIES,
            )
        ]

        if not keys_list:
            return 0

        # Group keys by slot for efficient deletion
        slots = self._group_keys_by_slot(keys_list)

        total_deleted = 0
        for slot_keys in slots.values():
            total_deleted += cast("int", await client.delete(*slot_keys))
        return total_deleted

    @override
    async def aclose(self, **kwargs: Any) -> None:
        """No-op. Cluster lives for the instance's lifetime (matches Django's BaseCache)."""

    @override
    def pipeline(
        self,
        *,
        transaction: bool = True,
        version: int | None = None,
    ) -> Pipeline:
        """Create a pipeline for batched operations.

        Note: Cluster mode doesn't support transactions, so transaction
        parameter is ignored and always set to False.
        """
        from django_cachex.client.pipeline import Pipeline

        client = self.get_client(write=True)
        # Cluster doesn't support transactions
        raw_pipeline = client.pipeline(transaction=False)
        return Pipeline(cache_client=self, pipeline=raw_pipeline, version=version)


# =============================================================================
# Cache Classes (extend BaseCache, delegate to CacheClient)
# =============================================================================


class KeyValueClusterCache(KeyValueCache):
    """Cluster cache backend base class.

    Extends KeyValueCache for cluster-specific behavior.
    Subclasses set `_class` class attribute to their specific ClusterCacheClient.
    """

    _class: type[KeyValueClusterCacheClient] = KeyValueClusterCacheClient


# =============================================================================
# Concrete Implementations
# =============================================================================

# Try to import Redis Cluster
try:
    import redis
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.cluster import RedisCluster
    from redis.cluster import key_slot as redis_key_slot

    class RedisClusterCacheClient(KeyValueClusterCacheClient):
        """Redis Cluster cache client.

        Extends KeyValueClusterCacheClient with Redis-specific classes.
        Handles server-side sharding and slot-aware operations.
        """

        _lib = redis
        _client_class = redis.Redis  # Not used for cluster but required by base
        _pool_class = redis.ConnectionPool  # Not used for cluster but required by base
        _cluster_class = RedisCluster
        _async_cluster_class = AsyncRedisCluster
        _key_slot_func = staticmethod(redis_key_slot)

    class RedisClusterCache(KeyValueClusterCache):
        """Django cache backend for Redis Cluster mode.

        Redis Cluster provides automatic sharding across multiple Redis nodes,
        enabling horizontal scaling. Data is automatically distributed across
        nodes using hash slots.

        Requirements:
            Requires redis-py to be installed::

                pip install redis

        Key Differences from Standard Redis:
            - Data is sharded across multiple nodes (16384 hash slots)
            - Multi-key operations only work when all keys are on the same slot
            - Transactions (MULTI/EXEC) are limited to single-slot operations
            - The backend automatically handles cluster topology discovery

        Example:
            Configure with a single cluster node (topology is auto-discovered)::

                CACHES = {
                    "default": {
                        "BACKEND": "django_cachex.client.RedisClusterCache",
                        "LOCATION": "redis://cluster-node-1:6379",
                    }
                }

            With options::

                CACHES = {
                    "default": {
                        "BACKEND": "django_cachex.client.RedisClusterCache",
                        "LOCATION": "redis://cluster-node-1:6379",
                        "OPTIONS": {
                            "skip_full_coverage_check": True,
                            "serializer": "django_cachex.serializers.json.JSONSerializer",
                        }
                    }
                }

        Note:
            Multi-key operations like ``get_many()`` and ``set_many()`` work across
            slots but are not atomic - they execute as separate operations per slot.

        See Also:
            - ``RedisCache``: For standalone Redis servers
            - ``RedisSentinelCache``: For Redis Sentinel high availability
        """

        _class = RedisClusterCacheClient

except ImportError:

    class RedisClusterCacheClient(KeyValueCacheClient):  # type: ignore[no-redef]
        """Redis Cluster cache client (requires redis-py to be installed)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "RedisClusterCacheClient requires redis-py to be installed. Install it with: pip install redis",
            )

    class RedisClusterCache(KeyValueCache):  # type: ignore[no-redef]
        """Redis Cluster cache backend (requires redis-py to be installed)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "RedisClusterCache requires redis-py to be installed. Install it with: pip install redis",
            )


# Try to import Valkey Cluster
try:
    import valkey
    from valkey.asyncio.cluster import ValkeyCluster as AsyncValkeyCluster
    from valkey.cluster import ValkeyCluster
    from valkey.cluster import key_slot as valkey_key_slot

    class ValkeyClusterCacheClient(KeyValueClusterCacheClient):
        """Valkey Cluster cache client.

        Extends KeyValueClusterCacheClient with Valkey-specific classes.
        Handles server-side sharding and slot-aware operations.
        """

        _lib = valkey
        _client_class = valkey.Valkey  # Not used for cluster but required by base
        _pool_class = valkey.ConnectionPool  # Not used for cluster but required by base
        _cluster_class = ValkeyCluster
        _async_cluster_class = AsyncValkeyCluster
        _key_slot_func = staticmethod(valkey_key_slot)

    class ValkeyClusterCache(KeyValueClusterCache):
        """Django cache backend for Valkey Cluster mode.

        Valkey Cluster provides automatic sharding across multiple Valkey nodes,
        enabling horizontal scaling. Data is automatically distributed across
        nodes using hash slots.

        Requirements:
            Requires valkey-py to be installed::

                pip install valkey

        Key Differences from Standard Valkey:
            - Data is sharded across multiple nodes (16384 hash slots)
            - Multi-key operations only work when all keys are on the same slot
            - Transactions (MULTI/EXEC) are limited to single-slot operations
            - The backend automatically handles cluster topology discovery

        Example:
            Configure with a single cluster node (topology is auto-discovered)::

                CACHES = {
                    "default": {
                        "BACKEND": "django_cachex.client.ValkeyClusterCache",
                        "LOCATION": "valkey://cluster-node-1:6379",
                    }
                }

        Note:
            Valkey is wire-protocol compatible with Redis, so you can also use
            ``RedisClusterCache`` with redis-py to connect to Valkey clusters.

        See Also:
            - ``ValkeyCache``: For standalone Valkey servers
            - ``RedisClusterCache``: Alternative using redis-py library
        """

        _class = ValkeyClusterCacheClient

except ImportError:

    class ValkeyClusterCacheClient(KeyValueCacheClient):  # type: ignore[no-redef]
        """Valkey Cluster cache client (requires valkey-py with cluster support)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "ValkeyClusterCacheClient requires valkey-py with cluster support. Install it with: pip install valkey",
            )

    class ValkeyClusterCache(KeyValueCache):  # type: ignore[no-redef]
        """Valkey Cluster cache backend (requires valkey-py with cluster support)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "ValkeyClusterCache requires valkey-py with cluster support. Install it with: pip install valkey",
            )


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "KeyValueClusterCache",
    "KeyValueClusterCacheClient",
    "RedisClusterCache",
    "RedisClusterCacheClient",
    "ValkeyClusterCache",
    "ValkeyClusterCacheClient",
]
