"""Cluster-aware adapter base class.

Cluster mode handles server-side sharding and slot-aware operations on top
of the single-node base. Per-driver concrete subclasses live in
:mod:`django_cachex.adapters.valkey_py` (``valkey-py``) and
:mod:`django_cachex.adapters.redis_py` (``redis-py``).
"""

from __future__ import annotations

import asyncio
import weakref
from collections import defaultdict
from itertools import batched
from typing import TYPE_CHECKING, Any, cast, override
from urllib.parse import urlparse

from django_cachex.adapters.default import BaseKeyValueAdapter
from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence

    from django_cachex.adapters.pipeline import BaseKeyValuePipelineAdapter
    from django_cachex.types import KeyT

# =============================================================================
# CacheClient Classes (actual Redis operations)
# =============================================================================


class BaseKeyValueClusterAdapter(BaseKeyValueAdapter):
    """Cluster cache client base class.

    Extends BaseKeyValueAdapter with cluster-specific handling for
    server-side sharding and slot-aware operations.
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
        # Keyed by event loop because async clusters are bound to the loop they're
        # created on. The same client instance can see multiple loops (e.g., WSGI
        # thread that calls asyncio.run() — ContextVar copies mean the same cache
        # instance is shared with the async context). WeakKeyDictionary ensures
        # automatic cleanup when a loop is GC'd.
        self._async_cluster_instances: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop,
            Any,
        ] = weakref.WeakKeyDictionary()

    @property
    def _cluster(self) -> type[Any]:
        """Get the cluster class, asserting it's configured."""
        if self._cluster_class is None:
            msg = "Subclasses must set _cluster_class"
            raise RuntimeError(msg)
        return self._cluster_class

    @property
    def _async_cluster(self) -> type[Any]:
        """Get the async cluster class, asserting it's configured."""
        if self._async_cluster_class is None:
            msg = "Subclasses must set _async_cluster_class"
            raise RuntimeError(msg)
        return self._async_cluster_class

    @override
    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the Cluster client."""
        if self._cluster_instance is not None:
            return self._cluster_instance

        url = self._servers[0]
        parsed_url = urlparse(url)
        # Pass through options
        cluster_options = {
            key_opt: value for key_opt, value in self._options.items() if key_opt not in self._CLIENT_ONLY_OPTIONS
        }

        if parsed_url.hostname:
            cluster_options["host"] = parsed_url.hostname
        if parsed_url.port:
            cluster_options["port"] = parsed_url.port

        self._cluster_instance = self._cluster(**cluster_options)
        return self._cluster_instance

    @override
    def get_async_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the async Cluster client for the current event loop."""
        loop = asyncio.get_running_loop()

        # Check if we already have an async cluster for this loop
        if loop in self._async_cluster_instances:
            return self._async_cluster_instances[loop]

        url = self._servers[0]
        parsed_url = urlparse(url)
        # Pass through options
        cluster_options = {
            key_opt: value for key_opt, value in self._options.items() if key_opt not in self._CLIENT_ONLY_OPTIONS
        }

        if parsed_url.hostname:
            cluster_options["host"] = parsed_url.hostname
        if parsed_url.port:
            cluster_options["port"] = parsed_url.port

        cluster = self._async_cluster(**cluster_options)
        self._async_cluster_instances[loop] = cluster

        return cluster

    def _group_keys_by_slot(self, keys: Iterable[KeyT]) -> dict[int, list[KeyT]]:
        """Group keys by their cluster slot."""
        slots: dict[int, list[KeyT]] = defaultdict(list)
        for key in keys:
            key_bytes = key.encode() if isinstance(key, str) else key
            slot = self._key_slot_func(key_bytes)
            slots[slot].append(key)
        return dict(slots)

    # Override methods that need cluster-specific handling

    @override
    def get_many(self, keys: Iterable[KeyT], *, stampede_prevention: bool | dict | None = None) -> dict[KeyT, Any]:
        """Retrieve many keys, handling cross-slot keys."""
        from django_cachex.stampede import should_recompute

        keys = list(keys)
        if not keys:
            return {}

        client = self.get_client(write=False)
        # mget_nonatomic handles slot splitting
        results = cast(
            "list[bytes | None]",
            client.mget_nonatomic(keys),
        )

        # Collect non-None results
        found = {k: v for k, v in zip(keys, results, strict=False) if v is not None}

        # Stampede filtering: pipeline TTL for all found keys
        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            pipe = client.pipeline()
            found_keys = list(found.keys())
            for k in found_keys:
                pipe.ttl(k)
            ttls = pipe.execute()
            for k, ttl in zip(found_keys, ttls, strict=False):
                if isinstance(ttl, int) and ttl > 0 and should_recompute(ttl, config):
                    del found[k]

        return dict(found.items())

    @override
    def set_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        """Set multiple values, handling cross-slot keys."""
        if not data:
            return []

        client = self.get_client(write=True)

        prepared_data = dict(data.items())
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            # timeout=0 means "delete immediately" (matches base client behavior)
            for slot_keys in self._group_keys_by_slot(prepared_data.keys()).values():
                client.delete(*slot_keys)
        elif actual_timeout is None:
            # No expiry
            client.mset_nonatomic(prepared_data)
        else:
            # Use SET with PX per key in a pipeline so each key is set
            # atomically with its TTL (no window where keys exist without expiry)
            timeout_ms = int(actual_timeout * 1000)
            pipe = client.pipeline()
            for key, value in prepared_data.items():
                pipe.set(key, value, px=timeout_ms)
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

        total_deleted = 0
        for batch in batched(
            client.scan_iter(
                match=pattern,
                count=itersize,
                target_nodes=self._cluster.PRIMARIES,
            ),
            itersize,
            strict=False,
        ):
            for slot_keys in self._group_keys_by_slot(batch).values():
                total_deleted += cast("int", client.delete(*slot_keys))
        return total_deleted

    @override
    def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        """SCAN is not supported in cluster mode (per-node cursors can't be combined). Use iter_keys() instead."""
        raise NotSupportedError("scan", "cluster")

    @override
    def close(self, **kwargs: Any) -> None:
        """No-op. Cluster lives for the instance's lifetime (matches Django's BaseCache)."""

    # =========================================================================
    # Async Override Methods
    # =========================================================================

    @override
    async def aget_many(
        self,
        keys: Iterable[KeyT],
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> dict[KeyT, Any]:
        """Retrieve many keys asynchronously, handling cross-slot keys."""
        from django_cachex.stampede import should_recompute

        keys = list(keys)
        if not keys:
            return {}

        client = self.get_async_client(write=False)

        # mget_nonatomic handles slot splitting
        results = cast(
            "list[bytes | None]",
            await client.mget_nonatomic(keys),
        )

        # Collect non-None results
        found = {k: v for k, v in zip(keys, results, strict=False) if v is not None}

        # Stampede filtering: pipeline TTL for all found keys
        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            pipe = client.pipeline()
            found_keys = list(found.keys())
            for k in found_keys:
                pipe.ttl(k)
            ttls = await pipe.execute()
            for k, ttl in zip(found_keys, ttls, strict=False):
                if isinstance(ttl, int) and ttl > 0 and should_recompute(ttl, config):
                    del found[k]

        return dict(found.items())

    @override
    async def aset_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        """Set multiple values asynchronously, handling cross-slot keys."""
        if not data:
            return []

        client = self.get_async_client(write=True)

        prepared_data = dict(data.items())
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            # timeout=0 means "delete immediately" (matches base client behavior)
            for slot_keys in self._group_keys_by_slot(prepared_data.keys()).values():
                await client.delete(*slot_keys)
        elif actual_timeout is None:
            # No expiry
            await client.mset_nonatomic(prepared_data)
        else:
            # Use SET with PX per key in a pipeline so each key is set
            # atomically with its TTL (no window where keys exist without expiry)
            timeout_ms = int(actual_timeout * 1000)
            pipe = client.pipeline()
            for key, value in prepared_data.items():
                pipe.set(key, value, px=timeout_ms)
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

        total_deleted = 0
        batch: list[Any] = []
        async for key in client.scan_iter(
            match=pattern,
            count=itersize,
            target_nodes=self._async_cluster.PRIMARIES,
        ):
            batch.append(key)
            if len(batch) >= itersize:
                for slot_keys in self._group_keys_by_slot(batch).values():
                    total_deleted += cast("int", await client.delete(*slot_keys))
                batch.clear()
        if batch:
            for slot_keys in self._group_keys_by_slot(batch).values():
                total_deleted += cast("int", await client.delete(*slot_keys))
        return total_deleted

    @override
    async def ascan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        """SCAN is not supported in cluster mode (per-node cursors can't be combined). Use aiter_keys() instead."""
        raise NotSupportedError("scan", "cluster")

    @override
    async def aclose(self, **kwargs: Any) -> None:
        """No-op. Cluster lives for the instance's lifetime (matches Django's BaseCache)."""

    @override
    def pipeline(self, *, transaction: bool = True) -> BaseKeyValuePipelineAdapter:
        """Construct a cluster pipeline adapter. Transactions are ignored in cluster mode."""
        from django_cachex.adapters.pipeline import RedisPipelineAdapter

        client = self.get_client(write=True)
        return RedisPipelineAdapter(client.pipeline(transaction=False))


__all__ = ["BaseKeyValueClusterAdapter"]
