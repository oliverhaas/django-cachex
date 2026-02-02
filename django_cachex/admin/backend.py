"""
Cache panel abstraction layer.

Provides a unified interface for inspecting and manipulating different cache backends.
Adapted from dj-cache-panel.

.. deprecated::
    This module is deprecated. Use :mod:`django_cachex.admin.service` instead.
"""

from __future__ import annotations

import fnmatch
import time
from typing import TYPE_CHECKING, Any, ClassVar, cast

from django.conf import settings
from django.core.cache import caches
from django.db import connection

if TYPE_CHECKING:
    from django.core.cache.backends.base import BaseCache

CACHEX_PANEL_SETTINGS = getattr(settings, "CACHEX_PANEL", {})


# Default Mapping of backend class paths to panel classes
BACKEND_PANEL_MAP_DEFAULT = {
    # Local memory cache
    "django.core.cache.backends.locmem.LocMemCache": "LocalMemoryCachePanel",
    # django-cachex backends - use optimized DjangoCachexPanel with native methods
    "django_cachex.cache.ValkeyCache": "DjangoCachexPanel",
    "django_cachex.cache.RedisCache": "DjangoCachexPanel",
    "django_cachex.cache.KeyValueCache": "DjangoCachexPanel",
    # django-cachex backends (full module path)
    "django_cachex.client.cache.ValkeyCache": "DjangoCachexPanel",
    "django_cachex.client.cache.RedisCache": "DjangoCachexPanel",
    "django_cachex.client.cache.KeyValueCache": "DjangoCachexPanel",
    # Memcached backends
    "django.core.cache.backends.memcached.PyMemcacheCache": "MemcachedCachePanel",
    "django.core.cache.backends.memcached.PyLibMCCache": "MemcachedCachePanel",
    "django.core.cache.backends.memcached.MemcachedCache": "MemcachedCachePanel",
    # Database cache
    "django.core.cache.backends.db.DatabaseCache": "DatabaseCachePanel",
    # File-based cache
    "django.core.cache.backends.filebased.FileBasedCache": "FileBasedCachePanel",
    # Dummy cache
    "django.core.cache.backends.dummy.DummyCache": "DummyCachePanel",
}

BACKEND_PANEL_MAP = CACHEX_PANEL_SETTINGS.get(
    "BACKEND_PANEL_MAP",
    BACKEND_PANEL_MAP_DEFAULT,
)
BACKEND_PANEL_EXTENSIONS = CACHEX_PANEL_SETTINGS.get("BACKEND_PANEL_EXTENSIONS", {})
BACKEND_PANEL_MAP.update(BACKEND_PANEL_EXTENSIONS)

CACHES_SETTINGS = CACHEX_PANEL_SETTINGS.get("CACHES", {})


def _import_panel_class(panel_class_name: str) -> type[CachePanel] | None:
    """Import a panel class from a dotted module path."""
    from importlib import import_module

    module_path, class_name = panel_class_name.rsplit(".", 1)
    try:
        module = import_module(module_path)
        panel_class = getattr(module, class_name, None)
        if panel_class is None:
            msg = f"Class '{class_name}' not found in module '{module_path}'"
            raise ImportError(msg)  # noqa: TRY301
        return panel_class
    except (ImportError, AttributeError, ValueError) as e:
        msg = f"Failed to import panel class '{panel_class_name}': {e!s}"
        raise ImportError(msg) from e


def get_cache_panel(cache_name: str) -> CachePanel:
    """
    Returns the proper CachePanel subclass for the given cache name.

    .. deprecated::
        Use :func:`django_cachex.admin.service.get_cache_service` instead.

    The panel class can be specified as:
    - A simple class name (e.g., "RedisCachePanel") - looked up in this module
    - A full module path (e.g., "myapp.panels.CustomCachePanel") - imported dynamically
    """
    cache_config = settings.CACHES.get(cache_name)
    if not cache_config:
        msg = f"Cache '{cache_name}' is not configured in CACHES setting."
        raise ValueError(msg)

    backend = cache_config.get("BACKEND", "")

    panel_class_name = BACKEND_PANEL_MAP.get(backend)
    if panel_class_name:
        if "." in panel_class_name:
            # Dynamic import from module path
            panel_class = _import_panel_class(panel_class_name)
            if panel_class:
                return panel_class(cache_name)
        else:
            # Simple class name - get from this module's globals
            panel_class = globals().get(panel_class_name)
            if panel_class:
                return panel_class(cache_name)

    # Fallback to a generic panel for unknown backends
    return GenericCachePanel(cache_name)


class CachePanel:
    """
    Base class defining the interface for a cache panel.

    Subclasses implement cache-specific functionality for different backends.
    """

    ABILITIES: ClassVar[dict[str, bool]] = {
        "query": False,
        "get_key": False,
        "delete_key": False,
        "edit_key": False,
        "add_key": False,
        "flush_cache": False,
        "get_ttl": False,
        "set_ttl": False,
        "get_type": False,
        "get_type_data": False,
        "get_size": False,
        "info": False,
        "slowlog": False,
        "string_ops": False,
        "list_ops": False,
        "set_ops": False,
        "hash_ops": False,
        "zset_ops": False,
        "stream_ops": False,
    }

    def __init__(self, cache_name: str) -> None:
        self.cache_name = cache_name
        self.cache_settings = CACHES_SETTINGS.get(self.cache_name, {})

    @property
    def cache(self) -> BaseCache:
        """Returns the cache instance for the cache panel."""
        return caches[self.cache_name]

    @property
    def abilities(self) -> dict[str, bool]:
        """Returns the abilities of the cache panel."""
        override_abilities = self.cache_settings.get("abilities", {})
        computed_abilities = {}
        for feature, value in self.ABILITIES.items():
            computed_abilities[feature] = value
            if feature in override_abilities:
                computed_abilities[feature] = override_abilities[feature]
        return computed_abilities

    def is_feature_supported(self, feature: str) -> bool:
        """Returns True if the cache panel supports the given feature."""
        return self.abilities.get(feature, False)

    def get_cache_metadata(self) -> dict[str, Any]:
        """Returns metadata about the cache configuration."""
        cache = self.cache
        return {
            "key_prefix": getattr(cache, "key_prefix", "") or "",
            "version": getattr(cache, "version", 1),
            "location": self._get_location(),
        }

    def info(self) -> dict[str, Any]:
        """Return information about the cache instance.

        Returns:
            Dict with cache metadata and server information:
            - backend: Backend class path
            - location: Server location(s)
            - key_prefix: Key prefix
            - version: Cache version
            - server: Server-specific info (Redis/Valkey specific)
            - keyspace: Keyspace statistics (Redis/Valkey specific)
        """
        if not self.is_feature_supported("info"):
            msg = "Info is not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._info()

    def _info(self) -> dict[str, Any]:
        """Default implementation returns basic metadata."""
        return {
            "backend": self.cache_settings.get("BACKEND", "Unknown"),
            **self.get_cache_metadata(),
            "server": None,
            "keyspace": None,
        }

    def slowlog(self, count: int = 25) -> dict[str, Any]:
        """Get the slow query log entries.

        Args:
            count: Number of entries to retrieve (default: 25).

        Returns:
            Dict with slowlog entries and metadata.
        """
        if not self.is_feature_supported("slowlog"):
            msg = "Slow log is not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._slowlog(count)

    def _slowlog(self, count: int = 25) -> dict[str, Any]:
        """Default implementation returns empty slowlog."""
        return {
            "entries": [],
            "length": 0,
            "error": "Slow log not available for this backend.",
        }

    def _get_location(self) -> str:
        """Get the cache server location(s)."""
        cache = self.cache
        # Try different attributes where location might be stored
        servers = getattr(cache, "_servers", None)
        if servers:
            return ", ".join(servers) if isinstance(servers, list) else str(servers)
        server = getattr(cache, "_server", None)
        if server:
            return str(server)
        # Fall back to settings
        location = self.cache_settings.get("LOCATION", "")
        if isinstance(location, list):
            return ", ".join(location)
        return str(location) if location else ""

    def make_key(self, key: str) -> str:
        """Get the full Redis key including prefix and version."""
        cache = cast("Any", self.cache)
        if hasattr(cache, "make_key"):
            return cache.make_key(key)
        return key

    def _query(
        self,
        pattern: str = "*",
        cursor: int = 0,
        count: int = 100,
    ) -> dict[str, Any]:
        """Scan the cache for keys using cursor-based pagination.

        For backends without native SCAN support, this simulates cursor
        pagination using offset-based slicing.
        """
        keys = cast("Any", self.cache).keys(pattern)
        keys.sort()
        total_count = len(keys)

        # Simulate cursor as offset for non-SCAN backends
        start_idx = cursor
        end_idx = start_idx + count
        paginated_keys = keys[start_idx:end_idx]

        # Next cursor is 0 if we've reached the end
        next_cursor = end_idx if end_idx < total_count else 0

        return {
            "keys": paginated_keys,
            "next_cursor": next_cursor,
            "total_count": total_count,
        }

    def query(
        self,
        instance_alias: str,
        pattern: str = "*",
        cursor: int = 0,
        count: int = 100,
    ) -> dict[str, Any]:
        """Query the cache for keys using cursor-based pagination.

        Args:
            instance_alias: Cache instance name
            pattern: Key pattern to match
            cursor: Cursor position (0 to start)
            count: Number of keys to return per call

        Returns:
            Dict with keys, next_cursor (0 if done), and optionally total_count.
        """
        if not self.is_feature_supported("query"):
            msg = "Querying is not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._query(pattern, cursor, count)

    def _get_key(self, key: str) -> dict[str, Any]:
        """Get a key value from the cache."""
        sentinel = object()
        value = self.cache.get(key, sentinel)
        exists = value is not sentinel

        if not exists:
            value = None

        return {
            "key": key,
            "value": value,
            "exists": exists,
            "type": type(value).__name__ if value is not None else None,
            "expiry": None,
        }

    def get_key(self, key: str) -> dict[str, Any]:
        """Get a key value from the cache."""
        if not self.is_feature_supported("get_key"):
            msg = "Getting keys is not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._get_key(key)

    def _delete_key(self, key: str) -> dict[str, Any]:
        """Delete a key from the cache."""
        self.cache.delete(key)
        return {
            "success": True,
            "error": None,
            "message": f"Key {key} deleted successfully.",
        }

    def delete_key(self, key: str) -> dict[str, Any]:
        """Delete a key from the cache."""
        if not self.is_feature_supported("delete_key"):
            msg = "Deleting keys is not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._delete_key(key)

    def _edit_key(
        self,
        key: str,
        value: Any,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Update the value of a cache key."""
        self.cache.set(key, value, timeout=timeout)
        return {
            "success": True,
            "error": None,
            "message": f"Key {key} updated successfully.",
        }

    def edit_key(
        self,
        key: str,
        value: Any,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Update the value of a cache key."""
        if not self.is_feature_supported("edit_key"):
            msg = "Editing keys is not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._edit_key(key, value, timeout=timeout)

    def _flush_cache(self) -> dict[str, Any]:
        """Clear all entries from the cache."""
        self.cache.clear()
        return {
            "success": True,
            "error": None,
            "message": "Cache flushed successfully.",
        }

    def flush_cache(self) -> dict[str, Any]:
        """Clear all entries from the cache."""
        if not self.is_feature_supported("flush_cache"):
            msg = "Flushing the cache is not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._flush_cache()

    def _get_key_ttl(self, key: str) -> int | None:
        """Get the TTL of a key in seconds. Returns None if not supported."""
        return None

    def get_key_ttl(self, key: str) -> int | None:
        """Get the TTL of a key in seconds."""
        if not self.is_feature_supported("get_ttl"):
            return None
        return self._get_key_ttl(key)

    def _get_key_type(self, key: str) -> str | None:
        """Get the Redis data type of a key. Returns None if not supported."""
        return None

    def get_key_type(self, key: str) -> str | None:
        """Get the data type of a key (string, list, set, hash, zset)."""
        if not self.is_feature_supported("get_type"):
            return None
        return self._get_key_type(key)

    def get_keys_info(self, keys: list[str]) -> list[dict[str, Any]]:
        """Get info (TTL, type) for multiple keys. Used by key_search view."""
        result = []
        for key in keys:
            info: dict[str, Any] = {"key": key}
            if self.is_feature_supported("get_ttl"):
                info["ttl"] = self._get_key_ttl(key)
            if self.is_feature_supported("get_type"):
                info["type"] = self._get_key_type(key)
            result.append(info)
        return result

    def get_type_data(self, key: str, key_type: str | None = None) -> dict[str, Any]:
        """Get type-specific data for a key. Override in subclasses for full support."""
        return {}

    def _get_key_size(self, key: str, key_type: str | None = None) -> int | None:
        """Get the size/length of a key. Returns None if not supported."""
        return None

    def get_key_size(self, key: str, key_type: str | None = None) -> int | None:
        """Get the size/length of a key.

        Returns:
        - For strings: length in bytes
        - For lists: number of elements
        - For sets: number of members
        - For hashes: number of fields
        - For sorted sets: number of members
        """
        if not self.is_feature_supported("get_size"):
            return None
        return self._get_key_size(key, key_type)

    def _set_key_ttl(self, key: str, ttl: int) -> dict[str, Any]:
        """Set the TTL of a key in seconds. Returns result dict."""
        return {"success": False, "message": "Setting TTL is not supported."}

    def set_key_ttl(self, key: str, ttl: int) -> dict[str, Any]:
        """Set the TTL of a key in seconds.

        Args:
            key: The cache key
            ttl: TTL in seconds. Use 0 or negative to remove expiry (persist).
        """
        if not self.is_feature_supported("set_ttl"):
            msg = "Setting TTL is not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._set_key_ttl(key, ttl)

    def _persist_key(self, key: str) -> dict[str, Any]:
        """Remove the TTL from a key. Returns result dict."""
        return {"success": False, "message": "Persist is not supported."}

    def persist_key(self, key: str) -> dict[str, Any]:
        """Remove the TTL from a key, making it permanent."""
        if not self.is_feature_supported("set_ttl"):
            msg = "Persist is not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._persist_key(key)

    # List operations
    def list_lpop(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop element(s) from the left of a list.

        Args:
            key: The list key.
            count: Number of elements to pop (default: 1).
        """
        if not self.is_feature_supported("list_ops"):
            msg = "List operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._list_lpop(key, count)

    def _list_lpop(self, key: str, count: int = 1) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def list_rpop(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop element(s) from the right of a list.

        Args:
            key: The list key.
            count: Number of elements to pop (default: 1).
        """
        if not self.is_feature_supported("list_ops"):
            msg = "List operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._list_rpop(key, count)

    def _list_rpop(self, key: str, count: int = 1) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def list_lpush(self, key: str, value: str) -> dict[str, Any]:
        """Push an element to the left of a list."""
        if not self.is_feature_supported("list_ops"):
            msg = "List operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._list_lpush(key, value)

    def _list_lpush(self, key: str, value: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def list_rpush(self, key: str, value: str) -> dict[str, Any]:
        """Push an element to the right of a list."""
        if not self.is_feature_supported("list_ops"):
            msg = "List operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._list_rpush(key, value)

    def _list_rpush(self, key: str, value: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def list_lrem(self, key: str, value: str, count: int = 0) -> dict[str, Any]:
        """Remove elements from a list by value.

        Args:
            key: The list key.
            value: The value to remove.
            count: Number of occurrences to remove.
                   0 = all occurrences, >0 = first N from head, <0 = first N from tail.
        """
        if not self.is_feature_supported("list_ops"):
            msg = "List operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._list_lrem(key, value, count)

    def _list_lrem(self, key: str, value: str, count: int = 0) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def list_ltrim(self, key: str, start: int, stop: int) -> dict[str, Any]:
        """Trim a list to the specified range.

        Args:
            key: The list key.
            start: Start index (0-based, inclusive).
            stop: Stop index (0-based, inclusive). Use -1 for last element.
        """
        if not self.is_feature_supported("list_ops"):
            msg = "List operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._list_ltrim(key, start, stop)

    def _list_ltrim(self, key: str, start: int, stop: int) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    # Set operations
    def set_sadd(self, key: str, member: str) -> dict[str, Any]:
        """Add a member to a set."""
        if not self.is_feature_supported("set_ops"):
            msg = "Set operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._set_sadd(key, member)

    def _set_sadd(self, key: str, member: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def set_srem(self, key: str, member: str) -> dict[str, Any]:
        """Remove a member from a set."""
        if not self.is_feature_supported("set_ops"):
            msg = "Set operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._set_srem(key, member)

    def _set_srem(self, key: str, member: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    # Hash operations
    def hash_hset(self, key: str, field: str, value: str) -> dict[str, Any]:
        """Set a field in a hash."""
        if not self.is_feature_supported("hash_ops"):
            msg = "Hash operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._hash_hset(key, field, value)

    def _hash_hset(self, key: str, field: str, value: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def hash_hdel(self, key: str, field: str) -> dict[str, Any]:
        """Delete a field from a hash."""
        if not self.is_feature_supported("hash_ops"):
            msg = "Hash operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._hash_hdel(key, field)

    def _hash_hdel(self, key: str, field: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    # Sorted set operations
    def zset_zadd(
        self,
        key: str,
        member: str,
        score: float,
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> dict[str, Any]:
        """Add a member to a sorted set.

        Args:
            key: The sorted set key.
            member: The member to add.
            score: The score for the member.
            nx: Only add new elements (don't update existing).
            xx: Only update existing elements (don't add new).
            gt: Only update when new score > existing score.
            lt: Only update when new score < existing score.
        """
        if not self.is_feature_supported("zset_ops"):
            msg = "Sorted set operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._zset_zadd(key, member, score, nx=nx, xx=xx, gt=gt, lt=lt)

    def _zset_zadd(
        self,
        key: str,
        member: str,
        score: float,
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def zset_zrem(self, key: str, member: str) -> dict[str, Any]:
        """Remove a member from a sorted set."""
        if not self.is_feature_supported("zset_ops"):
            msg = "Sorted set operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._zset_zrem(key, member)

    def _zset_zrem(self, key: str, member: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    # String operations
    def string_incr(self, key: str, delta: int = 1) -> dict[str, Any]:
        """Increment a string value by delta."""
        if not self.is_feature_supported("string_ops"):
            msg = "String operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._string_incr(key, delta)

    def _string_incr(self, key: str, delta: int = 1) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def string_decr(self, key: str, delta: int = 1) -> dict[str, Any]:
        """Decrement a string value by delta."""
        if not self.is_feature_supported("string_ops"):
            msg = "String operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._string_decr(key, delta)

    def _string_decr(self, key: str, delta: int = 1) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def string_append(self, key: str, value: str) -> dict[str, Any]:
        """Append a string to an existing string value."""
        if not self.is_feature_supported("string_ops"):
            msg = "String operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._string_append(key, value)

    def _string_append(self, key: str, value: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    # Set pop operation
    def set_spop(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop random member(s) from a set.

        Args:
            key: The set key.
            count: Number of members to pop (default: 1).
        """
        if not self.is_feature_supported("set_ops"):
            msg = "Set operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._set_spop(key, count)

    def _set_spop(self, key: str, count: int = 1) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    # Sorted set pop operations
    def zset_zpopmin(self, key: str) -> dict[str, Any]:
        """Pop the member with lowest score from a sorted set."""
        if not self.is_feature_supported("zset_ops"):
            msg = "Sorted set operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._zset_zpopmin(key)

    def _zset_zpopmin(self, key: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def zset_zpopmax(self, key: str) -> dict[str, Any]:
        """Pop the member with highest score from a sorted set."""
        if not self.is_feature_supported("zset_ops"):
            msg = "Sorted set operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._zset_zpopmax(key)

    def _zset_zpopmax(self, key: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    # Stream operations
    def stream_xadd(self, key: str, fields: dict[str, str]) -> dict[str, Any]:
        """Add an entry to a stream."""
        if not self.is_feature_supported("stream_ops"):
            msg = "Stream operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._stream_xadd(key, fields)

    def _stream_xadd(self, key: str, fields: dict[str, str]) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def stream_xdel(self, key: str, entry_id: str) -> dict[str, Any]:
        """Delete an entry from a stream."""
        if not self.is_feature_supported("stream_ops"):
            msg = "Stream operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._stream_xdel(key, entry_id)

    def _stream_xdel(self, key: str, entry_id: str) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}

    def stream_xtrim(self, key: str, maxlen: int) -> dict[str, Any]:
        """Trim a stream to a maximum length."""
        if not self.is_feature_supported("stream_ops"):
            msg = "Stream operations are not supported for this cache backend."
            raise NotImplementedError(msg)
        return self._stream_xtrim(key, maxlen)

    def _stream_xtrim(self, key: str, maxlen: int) -> dict[str, Any]:
        return {"success": False, "message": "Not supported."}


class LocalMemoryCachePanel(CachePanel):
    """Cache panel for the local memory cache backend."""

    ABILITIES: ClassVar[dict[str, bool]] = {
        "query": True,
        "get_key": True,
        "delete_key": True,
        "edit_key": True,
        "add_key": True,
        "flush_cache": True,
    }

    def _get_internal_cache(self) -> dict[str, Any]:
        """Access the internal cache dictionary of LocMemCache."""
        return getattr(self.cache, "_cache", {})

    def _query(
        self,
        pattern: str = "*",
        cursor: int = 0,
        count: int = 100,
    ) -> dict[str, Any]:
        """Scan the local memory cache for keys matching the pattern."""
        internal_cache = self._get_internal_cache()
        key_prefix = getattr(self.cache, "key_prefix", "")

        all_keys = []
        for internal_key in internal_cache:
            # Internal keys are formatted as ":{version}:{key_prefix}{key}"
            parts = internal_key.split(":", 2)
            if len(parts) >= 3:
                user_key = parts[2]
                if key_prefix and user_key.startswith(key_prefix):
                    user_key = user_key[len(key_prefix) :]
                all_keys.append(user_key)
            else:
                all_keys.append(internal_key)

        # Filter by pattern using fnmatch
        if pattern and pattern != "*":
            matching_keys = [k for k in all_keys if fnmatch.fnmatch(k, pattern)]
        else:
            matching_keys = all_keys

        matching_keys.sort()
        total_count = len(matching_keys)

        # Simulate cursor as offset
        start_idx = cursor
        end_idx = start_idx + count
        paginated_keys = matching_keys[start_idx:end_idx]

        # Next cursor is 0 if we've reached the end
        next_cursor = end_idx if end_idx < total_count else 0

        return {
            "keys": paginated_keys,
            "next_cursor": next_cursor,
            "total_count": total_count,
        }


class DatabaseCachePanel(CachePanel):
    """Cache panel for the database cache backend."""

    ABILITIES: ClassVar[dict[str, bool]] = {
        "query": True,
        "get_key": True,
        "delete_key": True,
        "edit_key": True,
        "add_key": True,
        "flush_cache": True,
    }

    def _get_table_name(self) -> str:
        """Get the database table name for this cache."""
        return cast("Any", self.cache)._table

    def _query(
        self,
        pattern: str = "*",
        cursor: int = 0,
        count: int = 100,
    ) -> dict[str, Any]:
        """Scan the database cache for keys matching the pattern."""
        table_name = self._get_table_name()
        quoted_table_name = connection.ops.quote_name(table_name)

        if pattern and pattern != "*":
            transformed_pattern = self.cache.make_key(pattern)
            sql_pattern = transformed_pattern.replace("*", "%").replace("?", "_")
        else:
            sql_pattern = "%"

        current_time = time.time()

        with connection.cursor() as db_cursor:
            if connection.vendor in ("postgresql", "oracle"):
                expires_condition = "expires > to_timestamp(%s)"
            elif connection.vendor == "mysql":
                expires_condition = "expires > FROM_UNIXTIME(%s)"
            else:
                expires_condition = "expires > %s"

            # Table name is safely quoted via connection.ops.quote_name()
            count_sql = f"""
                SELECT COUNT(*)
                FROM {quoted_table_name}
                WHERE cache_key LIKE %s AND {expires_condition}
            """  # noqa: S608
            db_cursor.execute(count_sql, [sql_pattern, current_time])
            total_count = db_cursor.fetchone()[0]

            # Use cursor as offset
            keys_sql = f"""
                SELECT cache_key
                FROM {quoted_table_name}
                WHERE cache_key LIKE %s AND {expires_condition}
                ORDER BY cache_key
                LIMIT %s OFFSET %s
            """  # noqa: S608
            db_cursor.execute(keys_sql, [sql_pattern, current_time, count, cursor])
            raw_keys = [row[0] for row in db_cursor.fetchall()]

            keys = []
            key_prefix = self.cache.key_prefix
            for cache_key in raw_keys:
                if cache_key.startswith(key_prefix):
                    key_without_prefix = cache_key[len(key_prefix) :]
                    parts = key_without_prefix.split(":", 2)
                    if len(parts) >= 3:
                        original_key = parts[2]
                    else:
                        original_key = key_without_prefix
                    keys.append(original_key)
                else:
                    keys.append(cache_key)

        # Next cursor is 0 if we've reached the end
        next_cursor = cursor + count if cursor + count < total_count else 0

        return {
            "keys": keys,
            "next_cursor": next_cursor,
            "total_count": total_count,
        }


class FileBasedCachePanel(CachePanel):
    """Cache panel for the file based cache backend."""

    ABILITIES: ClassVar[dict[str, bool]] = {
        "query": False,  # Cannot list keys - stored as hashed filenames
        "get_key": True,
        "delete_key": True,
        "edit_key": True,
        "add_key": True,
        "flush_cache": True,
    }


class DummyCachePanel(CachePanel):
    """Cache panel for the dummy cache backend."""

    ABILITIES: ClassVar[dict[str, bool]] = {
        "query": False,
        "get_key": False,
        "delete_key": False,
        "edit_key": False,
        "add_key": False,
        "flush_cache": False,
    }


class GenericCachePanel(CachePanel):
    """Generic cache panel for unknown/unsupported cache backends."""

    ABILITIES: ClassVar[dict[str, bool]] = {
        "query": False,
        "get_key": True,
        "delete_key": True,
        "edit_key": False,
        "add_key": False,
        "flush_cache": False,
    }


class MemcachedCachePanel(CachePanel):
    """Cache panel for the memcached cache backend."""

    ABILITIES: ClassVar[dict[str, bool]] = {
        "query": False,
        "get_key": True,
        "delete_key": True,
        "edit_key": True,
        "add_key": True,
        "flush_cache": True,
    }


class DjangoCachexPanel(CachePanel):
    """Cache panel for django-cachex backends using native cache methods.

    This panel is optimized for django-cachex backends (ValkeyCache, RedisCache)
    and uses the cache's native methods directly, avoiding raw client access
    where possible. This provides:
    - Proper key prefix handling built-in
    - Consistent with the cache's serialization
    - Simpler, cleaner code
    """

    ABILITIES: ClassVar[dict[str, bool]] = {
        "query": True,
        "get_key": True,
        "delete_key": True,
        "edit_key": True,
        "add_key": True,
        "flush_cache": True,
        "get_ttl": True,
        "set_ttl": True,
        "get_type": True,
        "get_type_data": True,
        "get_size": True,
        "info": True,
        "slowlog": True,
        "string_ops": True,
        "list_ops": True,
        "set_ops": True,
        "hash_ops": True,
        "zset_ops": True,
        "stream_ops": True,
    }

    def _get_key(self, key: str) -> dict[str, Any]:
        """Get a key value, handling non-string Redis types.

        For non-string types (list, set, hash, zset), we can't use cache.get()
        because Redis will return WRONGTYPE error. Instead, we detect the type
        and return a placeholder - the actual data is fetched via get_type_data().
        """
        # First check the Redis type
        key_type = self._get_key_type(key)

        if key_type is None:
            # Key doesn't exist
            return {
                "key": key,
                "value": None,
                "exists": False,
                "type": None,
                "expiry": None,
            }

        # For non-string types, don't try to get() - it will fail
        if key_type in ("list", "set", "hash", "zset", "stream"):
            return {
                "key": key,
                "value": None,  # Value comes from get_type_data() instead
                "exists": True,
                "type": key_type,
                "expiry": None,
            }

        # For string type, use the normal get method
        sentinel = object()
        value = self.cache.get(key, sentinel)
        exists = value is not sentinel

        if not exists:
            value = None

        return {
            "key": key,
            "value": value,
            "exists": exists,
            "type": type(value).__name__ if value is not None else None,
            "expiry": None,
        }

    def _get_key_ttl(self, key: str) -> int | None:
        """Get the TTL of a key in seconds using cache.ttl()."""
        try:
            cache = cast("Any", self.cache)
            ttl = cache.ttl(key)
            # Redis returns -1 for no expiry, -2 for key doesn't exist
            if ttl is not None and ttl >= 0:
                return ttl
            if ttl == -1:
                return None  # No expiry
            return None
        except Exception:  # noqa: BLE001
            return None

    def _get_key_type(self, key: str) -> str | None:
        """Get the Redis data type using cache.type()."""
        try:
            cache = cast("Any", self.cache)
            return cache.type(key)
        except Exception:  # noqa: BLE001
            return None

    def get_type_data(self, key: str, key_type: str | None = None) -> dict[str, Any]:  # noqa: PLR0911
        """Get type-specific data using cache's native methods."""
        if not self.is_feature_supported("get_type_data"):
            return {}

        if key_type is None:
            key_type = self.get_key_type(key)

        if not key_type or key_type == "string":
            return {}

        try:
            cache = cast("Any", self.cache)

            if key_type == "list":
                items = cache.lrange(key, 0, -1)
                # Items are already decoded by the cache
                items = [str(i) for i in items]
                return {"items": items, "length": len(items)}

            if key_type == "hash":
                fields = cache.hgetall(key)
                # Fields are already decoded
                decoded = {str(k): str(v) for k, v in fields.items()}
                return {"fields": decoded, "length": len(decoded)}

            if key_type == "set":
                members = cache.smembers(key)
                # Members are already decoded
                members = sorted(str(m) for m in members)
                return {"members": members, "length": len(members)}

            if key_type == "zset":
                members = cache.zrange(key, 0, -1, withscores=True)
                # Members are already decoded
                zset_members = [(str(m), s) for m, s in members]
                return {"members": zset_members, "length": len(zset_members)}

            if key_type == "stream" and hasattr(cache, "_cache") and hasattr(cache._cache, "xrange"):
                # Access stream methods via internal cache client
                entries = cache._cache.xrange(key, count=100)  # Limit to 100 entries
                length = cache._cache.xlen(key)
                # Entries are tuples of (entry_id, dict of fields)
                return {"entries": entries, "length": length}

            return {}
        except Exception:  # noqa: BLE001
            return {}

    def _get_key_size(self, key: str, key_type: str | None = None) -> int | None:
        """Get the size/length of a key using cache's native methods."""
        if key_type is None:
            key_type = self.get_key_type(key)

        if not key_type:
            return None

        try:
            cache = cast("Any", self.cache)

            # Use cache's native size methods
            size_methods: dict[str, Any] = {
                "list": lambda: cache.llen(key),
                "set": lambda: cache.scard(key),
                "hash": lambda: cache.hlen(key),
                "zset": lambda: cache.zcard(key),
            }

            # Stream uses xlen via internal cache client
            if hasattr(cache, "_cache") and hasattr(cache._cache, "xlen"):
                size_methods["stream"] = lambda: cache._cache.xlen(key)

            method = size_methods.get(key_type)
            if method:
                return method()

            # For strings, we need raw client access for STRLEN
            if key_type == "string":
                client = cache.get_client(write=False)
                full_key = cache.make_key(key)
                return client.strlen(full_key)

            return None
        except Exception:  # noqa: BLE001
            return None

    def _set_key_ttl(self, key: str, ttl: int) -> dict[str, Any]:
        """Set the TTL of a key using cache.expire()."""
        try:
            cache = cast("Any", self.cache)
            result = cache.expire(key, ttl)
            if result:
                return {"success": True, "message": f"TTL set to {ttl} seconds."}
            return {"success": False, "message": "Key does not exist or TTL could not be set."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _persist_key(self, key: str) -> dict[str, Any]:
        """Remove the TTL from a key using cache.persist()."""
        try:
            cache = cast("Any", self.cache)
            result = cache.persist(key)
            if result:
                return {"success": True, "message": "TTL removed. Key will not expire."}
            return {"success": False, "message": "Key does not exist or has no TTL."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # List operations using cache's native methods
    def _list_lpop(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop element(s) from the left of a list using cache.lpop()."""
        try:
            cache = cast("Any", self.cache)
            if count == 1:
                value = cache.lpop(key)
                if value is not None:
                    return {"success": True, "value": value, "message": f"Popped: {value}"}
                return {"success": False, "message": "List is empty or key does not exist."}
            # Pop multiple elements
            values = cache.lpop(key, count=count)
            if values:
                return {
                    "success": True,
                    "values": values,
                    "message": f"Popped {len(values)} item(s): {values}",
                }
            return {"success": False, "message": "List is empty or key does not exist."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _list_rpop(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop element(s) from the right of a list using cache.rpop()."""
        try:
            cache = cast("Any", self.cache)
            if count == 1:
                value = cache.rpop(key)
                if value is not None:
                    return {"success": True, "value": value, "message": f"Popped: {value}"}
                return {"success": False, "message": "List is empty or key does not exist."}
            # Pop multiple elements
            values = cache.rpop(key, count=count)
            if values:
                return {
                    "success": True,
                    "values": values,
                    "message": f"Popped {len(values)} item(s): {values}",
                }
            return {"success": False, "message": "List is empty or key does not exist."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _list_lpush(self, key: str, value: str) -> dict[str, Any]:
        """Push an element to the left of a list using cache.lpush()."""
        try:
            cache = cast("Any", self.cache)
            new_len = cache.lpush(key, value)
            return {"success": True, "length": new_len, "message": f"Pushed to left. Length: {new_len}"}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _list_rpush(self, key: str, value: str) -> dict[str, Any]:
        """Push an element to the right of a list using cache.rpush()."""
        try:
            cache = cast("Any", self.cache)
            new_len = cache.rpush(key, value)
            return {"success": True, "length": new_len, "message": f"Pushed to right. Length: {new_len}"}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _list_lrem(self, key: str, value: str, count: int = 0) -> dict[str, Any]:
        """Remove elements from a list by value using cache.lrem()."""
        try:
            cache = cast("Any", self.cache)
            removed = cache.lrem(key, count, value)
            if removed > 0:
                return {
                    "success": True,
                    "removed": removed,
                    "message": f"Removed {removed} occurrence(s) of '{value}'.",
                }
            return {"success": False, "message": f"'{value}' not found in list."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _list_ltrim(self, key: str, start: int, stop: int) -> dict[str, Any]:
        """Trim a list to the specified range using cache.ltrim()."""
        try:
            cache = cast("Any", self.cache)
            cache.ltrim(key, start, stop)
            return {
                "success": True,
                "message": f"List trimmed to range [{start}:{stop}].",
            }
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # Set operations using cache's native methods
    def _set_sadd(self, key: str, member: str) -> dict[str, Any]:
        """Add a member to a set using cache.sadd()."""
        try:
            cache = cast("Any", self.cache)
            added = cache.sadd(key, member)
            if added:
                return {"success": True, "message": f"Added '{member}' to set."}
            return {"success": True, "message": f"'{member}' already exists in set."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _set_srem(self, key: str, member: str) -> dict[str, Any]:
        """Remove a member from a set using cache.srem()."""
        try:
            cache = cast("Any", self.cache)
            removed = cache.srem(key, member)
            if removed:
                return {"success": True, "message": f"Removed '{member}' from set."}
            return {"success": False, "message": f"'{member}' not found in set."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # Hash operations using cache's native methods
    def _hash_hset(self, key: str, field: str, value: str) -> dict[str, Any]:
        """Set a field in a hash using cache.hset()."""
        try:
            cache = cast("Any", self.cache)
            cache.hset(key, field, value)
            return {"success": True, "message": f"Set field '{field}'."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _hash_hdel(self, key: str, field: str) -> dict[str, Any]:
        """Delete a field from a hash using cache.hdel()."""
        try:
            cache = cast("Any", self.cache)
            removed = cache.hdel(key, field)
            if removed:
                return {"success": True, "message": f"Deleted field '{field}'."}
            return {"success": False, "message": f"Field '{field}' not found."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # Sorted set operations using cache's native methods
    def _zset_zadd(
        self,
        key: str,
        member: str,
        score: float,
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> dict[str, Any]:
        """Add a member to a sorted set using cache.zadd()."""
        try:
            cache = cast("Any", self.cache)
            added = cache.zadd(key, {member: score}, nx=nx, xx=xx, gt=gt, lt=lt)
            # Build appropriate message based on flags
            if nx and not added:
                return {"success": True, "message": f"'{member}' already exists (NX flag)."}
            if xx and not added:
                return {"success": True, "message": f"'{member}' does not exist (XX flag)."}
            if (gt or lt) and not added:
                return {"success": True, "message": "Score not updated (GT/LT condition not met)."}
            if added:
                return {"success": True, "message": f"Added '{member}' with score {score}."}
            return {"success": True, "message": f"Updated '{member}' score to {score}."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _zset_zrem(self, key: str, member: str) -> dict[str, Any]:
        """Remove a member from a sorted set using cache.zrem()."""
        try:
            cache = cast("Any", self.cache)
            removed = cache.zrem(key, member)
            if removed:
                return {"success": True, "message": f"Removed '{member}' from sorted set."}
            return {"success": False, "message": f"'{member}' not found in sorted set."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # String operations using cache's incr method
    def _string_incr(self, key: str, delta: int = 1) -> dict[str, Any]:
        """Increment a string value using cache.incr()."""
        try:
            cache = cast("Any", self.cache)
            new_value = cache.incr(key, delta)
            return {"success": True, "value": new_value, "message": f"Value incremented to {new_value}."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _string_decr(self, key: str, delta: int = 1) -> dict[str, Any]:
        """Decrement a string value using cache.incr() with negative delta."""
        try:
            cache = cast("Any", self.cache)
            new_value = cache.incr(key, -delta)
            return {"success": True, "value": new_value, "message": f"Value decremented to {new_value}."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _string_append(self, key: str, value: str) -> dict[str, Any]:
        """Append a string to an existing value.

        Note: This is a get-append-set operation, not atomic.
        Uses Python string concatenation to preserve serialization.
        """
        try:
            cache = cast("Any", self.cache)
            current = cache.get(key)
            if current is None:
                return {"success": False, "message": "Key does not exist."}
            if not isinstance(current, str):
                return {"success": False, "message": "Value is not a string."}
            new_value = current + value
            cache.set(key, new_value, timeout=None)
            return {
                "success": True,
                "length": len(new_value),
                "message": f"Appended '{value}'. New length: {len(new_value)}.",
            }
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # Set pop operation using cache's spop method
    def _set_spop(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop random member(s) from a set using cache.spop()."""
        try:
            cache = cast("Any", self.cache)
            if count == 1:
                value = cache.spop(key)
                if value is not None:
                    return {"success": True, "value": value, "message": f"Popped: {value}"}
                return {"success": False, "message": "Set is empty or key does not exist."}
            # Pop multiple members
            values = cache.spop(key, count=count)
            if values:
                return {
                    "success": True,
                    "values": list(values) if hasattr(values, "__iter__") else [values],
                    "message": f"Popped {len(values) if hasattr(values, '__len__') else 1} member(s): {values}",
                }
            return {"success": False, "message": "Set is empty or key does not exist."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # Sorted set pop operations using cache's zpopmin/zpopmax methods
    def _zset_zpopmin(self, key: str) -> dict[str, Any]:
        """Pop the member with lowest score using cache.zpopmin()."""
        try:
            cache = cast("Any", self.cache)
            result = cache.zpopmin(key, count=1)
            if result:
                member, score = result[0]
                return {
                    "success": True,
                    "member": member,
                    "score": score,
                    "message": f"Popped: {member} (score: {score})",
                }
            return {"success": False, "message": "Sorted set is empty or key does not exist."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _zset_zpopmax(self, key: str) -> dict[str, Any]:
        """Pop the member with highest score using cache.zpopmax()."""
        try:
            cache = cast("Any", self.cache)
            result = cache.zpopmax(key, count=1)
            if result:
                member, score = result[0]
                return {
                    "success": True,
                    "member": member,
                    "score": score,
                    "message": f"Popped: {member} (score: {score})",
                }
            return {"success": False, "message": "Sorted set is empty or key does not exist."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # Stream operations using internal cache client
    def _stream_xadd(self, key: str, fields: dict[str, str]) -> dict[str, Any]:
        """Add an entry to a stream using cache._cache.xadd()."""
        try:
            cache = cast("Any", self.cache)
            if not hasattr(cache, "_cache") or not hasattr(cache._cache, "xadd"):
                return {"success": False, "message": "Stream operations not available."}
            entry_id = cache._cache.xadd(key, fields)
            return {"success": True, "entry_id": entry_id, "message": f"Added entry {entry_id}."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _stream_xdel(self, key: str, entry_id: str) -> dict[str, Any]:
        """Delete an entry from a stream using cache._cache.xdel()."""
        try:
            cache = cast("Any", self.cache)
            if not hasattr(cache, "_cache") or not hasattr(cache._cache, "xdel"):
                return {"success": False, "message": "Stream operations not available."}
            deleted = cache._cache.xdel(key, entry_id)
            if deleted:
                return {"success": True, "message": f"Deleted entry {entry_id}."}
            return {"success": False, "message": f"Entry {entry_id} not found."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _stream_xtrim(self, key: str, maxlen: int) -> dict[str, Any]:
        """Trim a stream using cache._cache.xtrim()."""
        try:
            cache = cast("Any", self.cache)
            if not hasattr(cache, "_cache") or not hasattr(cache._cache, "xtrim"):
                return {"success": False, "message": "Stream operations not available."}
            trimmed = cache._cache.xtrim(key, maxlen=maxlen)
            return {"success": True, "message": f"Trimmed {trimmed} entries."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def _info(self) -> dict[str, Any]:
        """Get detailed cache information using Redis INFO command."""
        base_info = {
            "backend": self.cache_settings.get("BACKEND", "Unknown"),
            **self.get_cache_metadata(),
            "server": None,
            "keyspace": None,
            "memory": None,
            "clients": None,
            "stats": None,
        }

        try:
            cache = cast("Any", self.cache)
            # Try to get raw Redis/Valkey client
            if hasattr(cache, "_cache") and hasattr(cache._cache, "info"):
                raw_info = cache._cache.info()

                # Extract server info
                base_info["server"] = {
                    "redis_version": raw_info.get("redis_version"),
                    "os": raw_info.get("os"),
                    "arch_bits": raw_info.get("arch_bits"),
                    "uptime_in_seconds": raw_info.get("uptime_in_seconds"),
                    "uptime_in_days": raw_info.get("uptime_in_days"),
                    "tcp_port": raw_info.get("tcp_port"),
                    "process_id": raw_info.get("process_id"),
                    "run_id": raw_info.get("run_id"),
                }

                # Extract memory info
                base_info["memory"] = {
                    "used_memory": raw_info.get("used_memory"),
                    "used_memory_human": raw_info.get("used_memory_human"),
                    "used_memory_peak": raw_info.get("used_memory_peak"),
                    "used_memory_peak_human": raw_info.get("used_memory_peak_human"),
                    "maxmemory": raw_info.get("maxmemory"),
                    "maxmemory_human": raw_info.get("maxmemory_human"),
                    "maxmemory_policy": raw_info.get("maxmemory_policy"),
                }

                # Extract client info
                base_info["clients"] = {
                    "connected_clients": raw_info.get("connected_clients"),
                    "blocked_clients": raw_info.get("blocked_clients"),
                    "tracking_clients": raw_info.get("tracking_clients"),
                }

                # Extract stats
                base_info["stats"] = {
                    "total_connections_received": raw_info.get("total_connections_received"),
                    "total_commands_processed": raw_info.get("total_commands_processed"),
                    "instantaneous_ops_per_sec": raw_info.get("instantaneous_ops_per_sec"),
                    "keyspace_hits": raw_info.get("keyspace_hits"),
                    "keyspace_misses": raw_info.get("keyspace_misses"),
                    "expired_keys": raw_info.get("expired_keys"),
                    "evicted_keys": raw_info.get("evicted_keys"),
                }

                # Extract keyspace info (db0, db1, etc.)
                keyspace = {k: v for k, v in raw_info.items() if k.startswith("db") and isinstance(v, dict)}
                if keyspace:
                    base_info["keyspace"] = keyspace

        except Exception:  # noqa: BLE001, S110
            # If we can't get info, just return basic metadata
            pass

        return base_info

    def _slowlog(self, count: int = 25) -> dict[str, Any]:
        """Get slow log entries using Redis SLOWLOG command."""
        result: dict[str, Any] = {
            "entries": [],
            "length": 0,
            "error": None,
        }

        try:
            cache = cast("Any", self.cache)
            if hasattr(cache, "_cache") and hasattr(cache._cache, "slowlog_get"):
                # Get total length first
                result["length"] = cache._cache.slowlog_len()

                # Get the entries
                raw_entries = cache._cache.slowlog_get(count)
                entries = []
                for entry in raw_entries:
                    # Each entry is a dict with: id, start_time, duration, command, client_address, client_name
                    if isinstance(entry, dict):
                        entries.append(
                            {
                                "id": entry.get("id"),
                                "timestamp": entry.get("start_time"),
                                "duration_us": entry.get("duration"),
                                "command": entry.get("command", []),
                                "client": entry.get("client_address"),
                                "client_name": entry.get("client_name"),
                            },
                        )
                    elif isinstance(entry, (list, tuple)) and len(entry) >= 4:
                        # Older format: [id, timestamp, duration, [command...]]
                        entries.append(
                            {
                                "id": entry[0],
                                "timestamp": entry[1],
                                "duration_us": entry[2],
                                "command": entry[3] if len(entry) > 3 else [],
                                "client": entry[4] if len(entry) > 4 else None,
                                "client_name": entry[5] if len(entry) > 5 else None,
                            },
                        )
                result["entries"] = entries
            else:
                result["error"] = "Slow log not available for this backend."

        except Exception as e:  # noqa: BLE001
            result["error"] = str(e)

        return result

    def _query(
        self,
        pattern: str = "*",
        cursor: int = 0,
        count: int = 100,
    ) -> dict[str, Any]:
        """Query keys using SCAN for cursor-based pagination."""
        try:
            cache = cast("Any", self.cache)
            next_cursor, keys = cache.scan(cursor=cursor, pattern=pattern, count=count)

            # Sort keys for consistent ordering within each batch
            keys.sort()

            return {
                "keys": keys,
                "next_cursor": next_cursor,
                "total_count": None,  # SCAN doesn't provide total count
            }
        except Exception as e:  # noqa: BLE001
            return {
                "keys": [],
                "next_cursor": 0,
                "total_count": None,
                "error": str(e),
            }
