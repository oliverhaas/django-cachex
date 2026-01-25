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
from typing import TYPE_CHECKING, Any, ClassVar, cast

from django.conf import settings

from django_cachex.client.cache import KeyValueCache
from django_cachex.client.default import (
    KeyValueCacheClient,
    _main_exceptions,
)
from django_cachex.exceptions import ConnectionInterruptedError

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
        "close_connection",
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

    # Cluster-level cache (cluster manages its own connection pool)
    _clusters: ClassVar[dict[str, Any]] = {}

    # Async cluster cache: WeakKeyDictionary keyed by event loop
    # Each loop gets its own dict of URL -> async cluster
    _async_clusters: ClassVar[weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, Any]]] = (
        weakref.WeakKeyDictionary()
    )

    # Subclasses must set these
    _cluster_class: type[Any] | None = None
    _async_cluster_class: type[Any] | None = None
    _key_slot_func: Any = None  # Function to calculate key slot

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

    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the Cluster client."""
        url = self._servers[0]
        if url in self._clusters:
            return self._clusters[url]

        from urllib.parse import urlparse

        parsed_url = urlparse(url)
        # Pass through options
        cluster_options = {key_opt: value for key_opt, value in self._options.items() if key_opt not in _KNOWN_OPTIONS}

        if parsed_url.hostname:
            cluster_options["host"] = parsed_url.hostname
        if parsed_url.port:
            cluster_options["port"] = parsed_url.port

        cluster = self._cluster(**cluster_options)
        self._clusters[url] = cluster
        return cluster

    def get_async_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the async Cluster client for the current event loop."""
        loop = asyncio.get_running_loop()
        url = self._servers[0]

        # Check if we already have an async cluster for this loop
        if loop in self._async_clusters and url in self._async_clusters[loop]:
            return self._async_clusters[loop][url]

        from urllib.parse import urlparse

        parsed_url = urlparse(url)
        # Pass through options
        cluster_options = {key_opt: value for key_opt, value in self._options.items() if key_opt not in _KNOWN_OPTIONS}

        if parsed_url.hostname:
            cluster_options["host"] = parsed_url.hostname
        if parsed_url.port:
            cluster_options["port"] = parsed_url.port

        cluster = self._async_cluster(**cluster_options)

        # Cache the cluster for this event loop
        if loop not in self._async_clusters:
            self._async_clusters[loop] = {}
        self._async_clusters[loop][url] = cluster

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

    def get_many(self, keys: Iterable[KeyT]) -> dict[KeyT, Any]:
        """Retrieve many keys, handling cross-slot keys."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_client(write=False)

        try:
            # mget_nonatomic handles slot splitting
            results = cast(
                "list[bytes | None]",
                client.mget_nonatomic(keys),
            )

            recovered_data = {}
            for key, value in zip(keys, results, strict=True):
                if value is not None:
                    recovered_data[key] = self.decode(value)

        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        return recovered_data

    def set_many(self, data: Mapping[KeyT, Any], timeout: float | None = None) -> list[KeyT]:
        """Set multiple values, handling cross-slot keys."""
        if not data:
            return []

        client = self.get_client(write=True)

        # Prepare data with encoded values
        prepared_data = {k: self.encode(v) for k, v in data.items()}

        try:
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
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        return []

    def delete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys, grouping by slot."""
        if not keys:
            return 0

        client = self.get_client(write=True)

        # Group keys by slot
        slots = self._group_keys_by_slot(keys)

        try:
            total_deleted = 0
            for slot_keys in slots.values():
                total_deleted += cast("int", client.delete(*slot_keys))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return total_deleted

    def clear(self) -> bool:
        """Flush all primary nodes in the cluster."""
        client = self.get_client(write=True)

        try:
            # Use PRIMARIES constant from the cluster class
            client.flushdb(target_nodes=self._cluster.PRIMARIES)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        return True

    def keys(self, pattern: str) -> list[str]:
        """Execute KEYS command across all primary nodes (pattern is already prefixed)."""
        client = self.get_client(write=False)

        try:
            keys_result = cast(
                "list[bytes]",
                client.keys(pattern, target_nodes=self._cluster.PRIMARIES),
            )
            return [k.decode() for k in keys_result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

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

    def delete_pattern(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> int:
        """Remove all keys matching pattern across all primary nodes (pattern is already prefixed)."""
        client = self.get_client(write=True)

        if itersize is None:
            itersize = self._default_scan_itersize

        try:
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
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        return total_deleted

    def close(self, **kwargs: Any) -> None:
        """Close the cluster connection if configured to do so."""
        close_flag = self._options.get(
            "close_connection",
            getattr(settings, "DJANGO_REDIS_CLOSE_CONNECTION", False),
        )
        if close_flag:
            url = self._servers[0]
            if url in self._clusters:
                self._clusters[url].close()
                del self._clusters[url]

    # =========================================================================
    # Async Override Methods
    # =========================================================================

    async def aget_many(self, keys: Iterable[KeyT]) -> dict[KeyT, Any]:
        """Retrieve many keys asynchronously, handling cross-slot keys."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_async_client(write=False)

        try:
            # mget_nonatomic handles slot splitting
            results = cast(
                "list[bytes | None]",
                await client.mget_nonatomic(keys),
            )

            recovered_data = {}
            for key, value in zip(keys, results, strict=True):
                if value is not None:
                    recovered_data[key] = self.decode(value)

        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        return recovered_data

    async def aset_many(self, data: Mapping[KeyT, Any], timeout: float | None = None) -> list[KeyT]:
        """Set multiple values asynchronously, handling cross-slot keys."""
        if not data:
            return []

        client = self.get_async_client(write=True)

        # Prepare data with encoded values
        prepared_data = {k: self.encode(v) for k, v in data.items()}

        try:
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
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        return []

    async def adelete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys asynchronously, grouping by slot."""
        if not keys:
            return 0

        client = self.get_async_client(write=True)

        # Group keys by slot
        slots = self._group_keys_by_slot(keys)

        try:
            total_deleted = 0
            for slot_keys in slots.values():
                total_deleted += cast("int", await client.delete(*slot_keys))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return total_deleted

    async def aclear(self) -> bool:
        """Flush all primary nodes in the cluster asynchronously."""
        client = self.get_async_client(write=True)

        try:
            # Use PRIMARIES constant from the cluster class
            await client.flushdb(target_nodes=self._async_cluster.PRIMARIES)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        return True

    async def akeys(self, pattern: str) -> list[str]:
        """Execute KEYS command asynchronously across all primary nodes."""
        client = self.get_async_client(write=False)

        try:
            keys_result = cast(
                "list[bytes]",
                await client.keys(pattern, target_nodes=self._async_cluster.PRIMARIES),
            )
            return [k.decode() for k in keys_result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

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

    async def adelete_pattern(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> int:
        """Remove all keys matching pattern asynchronously across all primary nodes."""
        client = self.get_async_client(write=True)

        if itersize is None:
            itersize = self._default_scan_itersize

        try:
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
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        return total_deleted

    async def aclose(self, **kwargs: Any) -> None:
        """Close the async cluster connection if configured to do so."""
        close_flag = self._options.get(
            "close_connection",
            getattr(settings, "DJANGO_REDIS_CLOSE_CONNECTION", False),
        )
        if close_flag:
            loop = asyncio.get_running_loop()
            url = self._servers[0]
            if loop in self._async_clusters and url in self._async_clusters[loop]:
                await self._async_clusters[loop][url].aclose()
                del self._async_clusters[loop][url]

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
_REDIS_CLUSTER_AVAILABLE = False
try:
    import redis
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.cluster import RedisCluster
    from redis.cluster import key_slot as redis_key_slot

    _REDIS_CLUSTER_AVAILABLE = True

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
        """Redis Cluster cache backend.

        Extends KeyValueClusterCache for Redis Cluster support.
        Use as: BACKEND = "django_cachex.client.RedisClusterCache"
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
_VALKEY_CLUSTER_AVAILABLE = False
try:
    import valkey
    from valkey.asyncio.cluster import ValkeyCluster as AsyncValkeyCluster
    from valkey.cluster import ValkeyCluster
    from valkey.cluster import key_slot as valkey_key_slot

    _VALKEY_CLUSTER_AVAILABLE = True

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
        """Valkey Cluster cache backend.

        Extends KeyValueClusterCache for Valkey Cluster support.
        Use as: BACKEND = "django_cachex.client.ValkeyClusterCache"
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
