"""
CacheService - unified service for cache admin operations.

This module provides the CacheService class which is the main interface
for cache admin views. It expects a cachex-like interface and uses
wrappers to adapt Django builtin caches.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from django.conf import settings
from django.core.cache import caches

from django_cachex.exceptions import NotSupportedError

from .wrappers import BaseCacheWrapper, get_wrapper

# Admin settings
CACHEX_ADMIN_SETTINGS = getattr(settings, "CACHEX_ADMIN", {})

CACHES_SETTINGS = CACHEX_ADMIN_SETTINGS.get("CACHES", {})


def _is_native_backend(backend: str) -> bool:
    """Check if the backend is a native django-cachex backend."""
    return backend.startswith("django_cachex.")


class CacheService:
    """Unified service for cache admin operations.

    This class provides a consistent interface for all cache admin operations.
    For native django-cachex backends, it uses the cache directly.
    For Django builtin backends, it wraps them to provide a cachex-like interface.

    The service uses try/except NotSupportedError pattern - views should catch
    NotSupportedError to handle unsupported operations gracefully.
    """

    def __init__(self, cache_name: str, cache: Any) -> None:
        """Initialize the CacheService.

        Args:
            cache_name: The name of the cache in CACHES setting.
            cache: The cache object (native or wrapped).
        """
        self.cache_name = cache_name
        self._cache = cache
        self._cache_config = settings.CACHES.get(cache_name, {})
        self._cache_settings = CACHES_SETTINGS.get(cache_name, {})
        self._is_native = _is_native_backend(str(self._cache_config.get("BACKEND", "")))

    @property
    def backend(self) -> str:
        """Get the backend class path."""
        return str(self._cache_config.get("BACKEND", ""))

    @property
    def is_native(self) -> bool:
        """Check if this is a native django-cachex backend."""
        return self._is_native

    @property
    def support_level(self) -> str:
        """Get the support level for this backend.

        Returns:
            'native' for django-cachex backends, 'wrapped' for others.
        """
        return "native" if self._is_native else "wrapped"

    def supports(self, operation: str) -> bool:
        """Check if an operation is supported by this cache.

        This is a hint for templates to conditionally render UI elements.
        The actual operation may still fail - views should catch NotSupportedError.

        For native django-cachex backends, all operations are supported.
        For wrapped backends, we check if the method exists and doesn't
        immediately raise NotSupportedError.

        Args:
            operation: The operation to check (e.g., 'keys', 'ttl', 'query').

        Returns:
            True if the operation appears to be supported, False otherwise.
        """
        # Native backends support all operations
        if self._is_native:
            return True

        # Map operation names to method names on the wrapper
        method_map = {
            # View-friendly aliases
            "query": "keys",
            "get_key": "get",
            "delete_key": "delete",
            "edit_key": "set",
            "add_key": "set",
            "flush_cache": "clear",
            "get_ttl": "ttl",
            "set_ttl": "expire",
            "get_type": "type",
            # Composite operations - check representative method
            "list_ops": "lrange",
            "set_ops": "smembers",
            "hash_ops": "hgetall",
            "zset_ops": "zrange",
            "stream_ops": "xrange",
            "get_type_data": "type",
            "get_size": "memory_usage",
        }

        method_name = method_map.get(operation, operation)
        method = getattr(self._cache, method_name, None)

        if method is None:
            return False

        # For methods that require arguments, try calling with test data
        # to see if they raise NotSupportedError immediately
        try:
            if method_name == "keys" or method_name in ("ttl", "type", "expire", "persist", "memory_usage"):
                method("__test_support__")
            # For other methods, assume supported if they exist
            return True
        except NotSupportedError:
            return False
        except Exception:  # noqa: BLE001
            # Other errors mean the operation exists but failed for other reasons
            return True

    # Metadata methods

    def get_cache_metadata(self) -> dict[str, Any]:
        """Get metadata about the cache configuration."""
        cache = self._cache
        if isinstance(cache, BaseCacheWrapper):
            cache = cache._cache

        return {
            "key_prefix": getattr(cache, "key_prefix", "") or "",
            "version": getattr(cache, "version", 1),
            "location": self._get_location(),
        }

    def _get_location(self) -> str:
        """Get the cache server location(s)."""
        cache = self._cache
        if isinstance(cache, BaseCacheWrapper):
            cache = cache._cache

        # Try different attributes where location might be stored
        servers = getattr(cache, "_servers", None)
        if servers:
            return ", ".join(servers) if isinstance(servers, list) else str(servers)
        server = getattr(cache, "_server", None)
        if server:
            return str(server)
        # Fall back to settings
        location = self._cache_config.get("LOCATION", "")
        if isinstance(location, list):
            return ", ".join(location)
        return str(location) if location else ""

    def make_key(self, key: str) -> str:
        """Get the full cache key including prefix and version."""
        cache = self._cache
        if isinstance(cache, BaseCacheWrapper):
            return cache.make_key(key)
        if hasattr(cache, "make_key"):
            return cache.make_key(key)
        return key

    # Core operations

    def get(self, key: str) -> dict[str, Any]:
        """Get a key value from the cache.

        Returns:
            Dict with key, value, exists, type, expiry fields.
        """
        if self._is_native:
            # For native backends, check type first to handle non-string types
            key_type = self.type(key)
            if key_type is None:
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

        # For string type or wrapped backends, use normal get
        sentinel = object()
        value = self._cache.get(key, sentinel)
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

    def set(self, key: str, value: Any, timeout: float | None = None) -> dict[str, Any]:
        """Set a key value in the cache.

        Returns:
            Dict with success, error, message fields.
        """
        try:
            self._cache.set(key, value, timeout=timeout)
            return {
                "success": True,
                "error": None,
                "message": f"Key {key} updated successfully.",
            }
        except Exception as e:  # noqa: BLE001
            return {
                "success": False,
                "error": str(e),
                "message": f"Error setting key: {e}",
            }

    def delete(self, key: str) -> dict[str, Any]:
        """Delete a key from the cache.

        Returns:
            Dict with success, error, message fields.
        """
        try:
            self._cache.delete(key)
            return {
                "success": True,
                "error": None,
                "message": f"Key {key} deleted successfully.",
            }
        except Exception as e:  # noqa: BLE001
            return {
                "success": False,
                "error": str(e),
                "message": f"Error deleting key: {e}",
            }

    def clear(self) -> dict[str, Any]:
        """Clear all keys from the cache.

        Returns:
            Dict with success, error, message fields.
        """
        try:
            self._cache.clear()
            return {
                "success": True,
                "error": None,
                "message": "Cache flushed successfully.",
            }
        except Exception as e:  # noqa: BLE001
            return {
                "success": False,
                "error": str(e),
                "message": f"Error flushing cache: {e}",
            }

    # Key listing and search

    def keys(
        self,
        pattern: str = "*",
        cursor: int = 0,
        count: int = 100,
    ) -> dict[str, Any]:
        """List keys matching the pattern with cursor-based pagination.

        Args:
            pattern: Glob pattern to match keys.
            cursor: Cursor position (0 to start).
            count: Number of keys to return per call.

        Returns:
            Dict with keys, next_cursor, and optionally total_count fields.

        Raises:
            NotSupportedError: If key listing is not supported.
        """
        try:
            # Use SCAN if available (Redis/Valkey)
            if hasattr(self._cache, "scan"):
                next_cursor, keys = self._cache.scan(cursor=cursor, pattern=pattern, count=count)
                keys = sorted(keys)  # Sort for consistent ordering
                return {
                    "keys": keys,
                    "next_cursor": next_cursor,
                    "total_count": None,  # SCAN doesn't provide total count
                }

            # Fall back to keys() with simulated cursor (offset-based)
            all_keys = self._cache.keys(pattern)
            if hasattr(all_keys, "sort"):
                all_keys.sort()
            else:
                all_keys = sorted(all_keys)

            total_count = len(all_keys)
            start_idx = cursor
            end_idx = start_idx + count
            paginated_keys = all_keys[start_idx:end_idx]

            # Next cursor is 0 if we've reached the end
            next_cursor = end_idx if end_idx < total_count else 0

            return {
                "keys": paginated_keys,
                "next_cursor": next_cursor,
                "total_count": total_count,
            }
        except NotSupportedError:
            raise
        except Exception as e:  # noqa: BLE001
            return {
                "keys": [],
                "next_cursor": 0,
                "total_count": None,
                "error": str(e),
            }

    # TTL operations

    def ttl(self, key: str) -> int | None:
        """Get the TTL of a key in seconds.

        Returns:
            TTL in seconds, None for no expiry, None for key not found.

        Raises:
            NotSupportedError: If TTL is not supported.
        """
        try:
            result = self._cache.ttl(key)
            # Redis returns -1 for no expiry, -2 for key doesn't exist
            if result is not None and result >= 0:
                return result
            if result == -1:
                return None  # No expiry
            return None
        except NotSupportedError:
            raise
        except Exception:  # noqa: BLE001
            return None

    def expire(self, key: str, timeout: int) -> dict[str, Any]:
        """Set the TTL of a key.

        Returns:
            Dict with success, message fields.

        Raises:
            NotSupportedError: If expire is not supported.
        """
        try:
            result = self._cache.expire(key, timeout)
            if result:
                return {"success": True, "message": f"TTL set to {timeout} seconds."}
            return {"success": False, "message": "Key does not exist or TTL could not be set."}
        except NotSupportedError:
            raise
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def persist(self, key: str) -> dict[str, Any]:
        """Remove the TTL from a key.

        Returns:
            Dict with success, message fields.

        Raises:
            NotSupportedError: If persist is not supported.
        """
        try:
            result = self._cache.persist(key)
            if result:
                return {"success": True, "message": "TTL removed. Key will not expire."}
            return {"success": False, "message": "Key does not exist or has no TTL."}
        except NotSupportedError:
            raise
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # Type operations

    def type(self, key: str) -> str | None:
        """Get the data type of a key.

        Returns:
            Type string (string, list, set, hash, zset, stream) or None.

        Raises:
            NotSupportedError: If type detection is not supported.
        """
        try:
            return self._cache.type(key)
        except NotSupportedError:
            raise
        except Exception:  # noqa: BLE001
            return None

    def get_type_data(self, key: str, key_type: str | None = None) -> dict[str, Any]:  # noqa: PLR0911
        """Get type-specific data for a key.

        Args:
            key: The cache key.
            key_type: Optional type hint (avoids extra type() call).

        Returns:
            Dict with type-specific data (items for lists, members for sets, etc.)
            Empty dict if not supported.
        """
        if not self._is_native:
            return {}

        if key_type is None:
            key_type = self.type(key)

        if not key_type or key_type == "string":
            return {}

        try:
            cache = cast("Any", self._cache)

            if key_type == "list":
                items = cache.lrange(key, 0, -1)
                items = [str(i) for i in items]
                return {"items": items, "length": len(items)}

            if key_type == "hash":
                fields = cache.hgetall(key)
                decoded = {str(k): str(v) for k, v in fields.items()}
                return {"fields": decoded, "length": len(decoded)}

            if key_type == "set":
                members = cache.smembers(key)
                members = sorted(str(m) for m in members)
                return {"members": members, "length": len(members)}

            if key_type == "zset":
                members = cache.zrange(key, 0, -1, withscores=True)
                zset_members = [(str(m), s) for m, s in members]
                return {"members": zset_members, "length": len(zset_members)}

            if key_type == "stream" and hasattr(cache, "_cache") and hasattr(cache._cache, "xrange"):
                entries = cache._cache.xrange(key, count=100)
                length = cache._cache.xlen(key)
                return {"entries": entries, "length": length}

            return {}
        except Exception:  # noqa: BLE001
            return {}

    def size(self, key: str, key_type: str | None = None) -> int | None:
        """Get the size/length of a key.

        Args:
            key: The cache key.
            key_type: Optional type hint.

        Returns:
            Size/length or None if not supported.
        """
        if not self._is_native:
            return None

        if key_type is None:
            key_type = self.type(key)

        if not key_type:
            return None

        try:
            cache = cast("Any", self._cache)

            size_methods: dict[str, Any] = {
                "list": lambda: cache.llen(key),
                "set": lambda: cache.scard(key),
                "hash": lambda: cache.hlen(key),
                "zset": lambda: cache.zcard(key),
            }

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

    # Server info operations

    def info(self) -> dict[str, Any]:
        """Get raw cache server information.

        Returns the raw output from the cache backend's info() method,
        which is a JSON-serializable dict with up to 2 levels of nesting.

        Returns:
            Dict with raw server info, or empty dict if not supported.
        """
        cache = cast("Any", self._cache)

        # First try the cache's own info() method
        if hasattr(cache, "info"):
            try:
                return dict(cache.info())
            except Exception:  # noqa: BLE001, S110
                pass

        # Fall back to accessing the internal cache client directly
        if hasattr(cache, "_cache") and hasattr(cache._cache, "info"):
            try:
                return dict(cache._cache.info())
            except Exception:  # noqa: BLE001, S110
                pass

        return {}

    def metadata(self) -> dict[str, Any]:
        """Get cache metadata with parsed server information.

        Returns configuration metadata plus nicely parsed server info
        for display in the admin panel.

        Returns:
            Dict with backend, location, key_prefix, version, and
            parsed server/memory/clients/stats/keyspace sections.

        Raises:
            NotSupportedError: If metadata is not supported.
        """
        base_info = {
            "backend": self.backend,
            **self.get_cache_metadata(),
            "server": None,
            "keyspace": None,
            "memory": None,
            "clients": None,
            "stats": None,
        }

        if not self._is_native:
            try:
                wrapper_info = self._cache.info()
                base_info.update(wrapper_info)
                return base_info
            except NotSupportedError:
                raise
            except Exception:  # noqa: BLE001
                raise NotSupportedError("metadata", self.__class__.__name__) from None

        # Native backend - get detailed Redis/Valkey info
        try:
            raw_info = self.info()
            if raw_info:
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

                base_info["memory"] = {
                    "used_memory": raw_info.get("used_memory"),
                    "used_memory_human": raw_info.get("used_memory_human"),
                    "used_memory_peak": raw_info.get("used_memory_peak"),
                    "used_memory_peak_human": raw_info.get("used_memory_peak_human"),
                    "maxmemory": raw_info.get("maxmemory"),
                    "maxmemory_human": raw_info.get("maxmemory_human"),
                    "maxmemory_policy": raw_info.get("maxmemory_policy"),
                }

                base_info["clients"] = {
                    "connected_clients": raw_info.get("connected_clients"),
                    "blocked_clients": raw_info.get("blocked_clients"),
                    "tracking_clients": raw_info.get("tracking_clients"),
                }

                base_info["stats"] = {
                    "total_connections_received": raw_info.get("total_connections_received"),
                    "total_commands_processed": raw_info.get("total_commands_processed"),
                    "instantaneous_ops_per_sec": raw_info.get("instantaneous_ops_per_sec"),
                    "keyspace_hits": raw_info.get("keyspace_hits"),
                    "keyspace_misses": raw_info.get("keyspace_misses"),
                    "expired_keys": raw_info.get("expired_keys"),
                    "evicted_keys": raw_info.get("evicted_keys"),
                }

                keyspace = {k: v for k, v in raw_info.items() if k.startswith("db") and isinstance(v, dict)}
                if keyspace:
                    base_info["keyspace"] = keyspace

        except Exception:  # noqa: BLE001, S110
            pass

        return base_info

    def _parse_slowlog_entry(self, entry: Any) -> dict[str, Any]:
        """Parse a raw slowlog entry into structured format."""
        if isinstance(entry, dict):
            ts = entry.get("start_time")
            return {
                "id": entry.get("id"),
                "timestamp": datetime.fromtimestamp(ts, tz=UTC) if ts else None,
                "duration_us": entry.get("duration"),
                "command": entry.get("command", []),
                "client": entry.get("client_address"),
                "client_name": entry.get("client_name"),
            }
        if isinstance(entry, (list, tuple)) and len(entry) >= 4:
            ts = entry[1]
            return {
                "id": entry[0],
                "timestamp": datetime.fromtimestamp(ts, tz=UTC) if ts else None,
                "duration_us": entry[2],
                "command": entry[3] if len(entry) > 3 else [],
                "client": entry[4] if len(entry) > 4 else None,
                "client_name": entry[5] if len(entry) > 5 else None,
            }
        return {}

    def slowlog_get(self, count: int = 25) -> dict[str, Any]:
        """Get slow query log entries.

        Args:
            count: Number of entries to retrieve.

        Returns:
            Dict with entries, length, error fields.

        Raises:
            NotSupportedError: If slowlog is not supported.
        """
        result: dict[str, Any] = {
            "entries": [],
            "length": 0,
            "error": None,
        }

        if not self._is_native:
            try:
                return self._cache.slowlog_get(count)
            except NotSupportedError:
                raise
            except Exception:  # noqa: BLE001
                raise NotSupportedError("slowlog", self.__class__.__name__) from None

        cache = cast("Any", self._cache)

        # Try cache's own slowlog methods first (KeyValueCache exposes these)
        if hasattr(cache, "slowlog_get") and hasattr(cache, "slowlog_len"):
            try:
                result["length"] = cache.slowlog_len()
                raw_entries = cache.slowlog_get(count)
            except Exception as e:  # noqa: BLE001
                result["error"] = str(e)
                return result
        # Fall back to internal cache client
        elif hasattr(cache, "_cache") and hasattr(cache._cache, "slowlog_get"):
            try:
                result["length"] = cache._cache.slowlog_len()
                raw_entries = cache._cache.slowlog_get(count)
            except Exception as e:  # noqa: BLE001
                result["error"] = str(e)
                return result
        else:
            result["error"] = "Slow log not available for this backend."
            return result

        # Parse raw entries into structured format
        result["entries"] = [self._parse_slowlog_entry(entry) for entry in raw_entries]

        return result

    # List operations

    def lpop(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop from the left of a list."""
        if not self._is_native:
            raise NotSupportedError("lpop", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            if count == 1:
                value = cache.lpop(key)
                if value is not None:
                    return {"success": True, "value": value, "message": f"Popped: {value}"}
                return {"success": False, "message": "List is empty or key does not exist."}
            values = cache.lpop(key, count=count)
            if values:
                return {"success": True, "values": values, "message": f"Popped {len(values)} item(s): {values}"}
            return {"success": False, "message": "List is empty or key does not exist."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def rpop(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop from the right of a list."""
        if not self._is_native:
            raise NotSupportedError("rpop", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            if count == 1:
                value = cache.rpop(key)
                if value is not None:
                    return {"success": True, "value": value, "message": f"Popped: {value}"}
                return {"success": False, "message": "List is empty or key does not exist."}
            values = cache.rpop(key, count=count)
            if values:
                return {"success": True, "values": values, "message": f"Popped {len(values)} item(s): {values}"}
            return {"success": False, "message": "List is empty or key does not exist."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def lpush(self, key: str, value: str) -> dict[str, Any]:
        """Push to the left of a list."""
        if not self._is_native:
            raise NotSupportedError("lpush", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            new_len = cache.lpush(key, value)
            return {"success": True, "length": new_len, "message": f"Pushed to left. Length: {new_len}"}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def rpush(self, key: str, value: str) -> dict[str, Any]:
        """Push to the right of a list."""
        if not self._is_native:
            raise NotSupportedError("rpush", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            new_len = cache.rpush(key, value)
            return {"success": True, "length": new_len, "message": f"Pushed to right. Length: {new_len}"}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def lrem(self, key: str, value: str, count: int = 0) -> dict[str, Any]:
        """Remove elements from a list."""
        if not self._is_native:
            raise NotSupportedError("lrem", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
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

    def ltrim(self, key: str, start: int, stop: int) -> dict[str, Any]:
        """Trim a list to the specified range."""
        if not self._is_native:
            raise NotSupportedError("ltrim", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            cache.ltrim(key, start, stop)
            return {"success": True, "message": f"List trimmed to range [{start}:{stop}]."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # Set operations

    def sadd(self, key: str, member: str) -> dict[str, Any]:
        """Add a member to a set."""
        if not self._is_native:
            raise NotSupportedError("sadd", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            added = cache.sadd(key, member)
            if added:
                return {"success": True, "message": f"Added '{member}' to set."}
            return {"success": True, "message": f"'{member}' already exists in set."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def srem(self, key: str, member: str) -> dict[str, Any]:
        """Remove a member from a set."""
        if not self._is_native:
            raise NotSupportedError("srem", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            removed = cache.srem(key, member)
            if removed:
                return {"success": True, "message": f"Removed '{member}' from set."}
            return {"success": False, "message": f"'{member}' not found in set."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def spop(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop random members from a set."""
        if not self._is_native:
            raise NotSupportedError("spop", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            if count == 1:
                value = cache.spop(key)
                if value is not None:
                    return {"success": True, "value": value, "message": f"Popped: {value}"}
                return {"success": False, "message": "Set is empty or key does not exist."}
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

    # Hash operations

    def hset(self, key: str, field: str, value: str) -> dict[str, Any]:
        """Set a field in a hash."""
        if not self._is_native:
            raise NotSupportedError("hset", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            cache.hset(key, field, value)
            return {"success": True, "message": f"Set field '{field}'."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def hdel(self, key: str, field: str) -> dict[str, Any]:
        """Delete a field from a hash."""
        if not self._is_native:
            raise NotSupportedError("hdel", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            removed = cache.hdel(key, field)
            if removed:
                return {"success": True, "message": f"Deleted field '{field}'."}
            return {"success": False, "message": f"Field '{field}' not found."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # Sorted set operations

    def zadd(
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
        """Add a member to a sorted set."""
        if not self._is_native:
            raise NotSupportedError("zadd", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            added = cache.zadd(key, {member: score}, nx=nx, xx=xx, gt=gt, lt=lt)
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

    def zrem(self, key: str, member: str) -> dict[str, Any]:
        """Remove a member from a sorted set."""
        if not self._is_native:
            raise NotSupportedError("zrem", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            removed = cache.zrem(key, member)
            if removed:
                return {"success": True, "message": f"Removed '{member}' from sorted set."}
            return {"success": False, "message": f"'{member}' not found in sorted set."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def zpopmin(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop the member(s) with lowest score."""
        if not self._is_native:
            raise NotSupportedError("zpopmin", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            result = cache.zpopmin(key, count=count)
            if result:
                if count == 1:
                    member, score = result[0]
                    return {
                        "success": True,
                        "member": member,
                        "score": score,
                        "message": f"Popped: {member} (score: {score})",
                    }
                members = [f"{m} ({s})" for m, s in result]
                return {
                    "success": True,
                    "count": len(result),
                    "message": f"Popped {len(result)} member(s): {', '.join(members)}",
                }
            return {"success": False, "message": "Sorted set is empty or key does not exist."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def zpopmax(self, key: str, count: int = 1) -> dict[str, Any]:
        """Pop the member(s) with highest score."""
        if not self._is_native:
            raise NotSupportedError("zpopmax", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            result = cache.zpopmax(key, count=count)
            if result:
                if count == 1:
                    member, score = result[0]
                    return {
                        "success": True,
                        "member": member,
                        "score": score,
                        "message": f"Popped: {member} (score: {score})",
                    }
                members = [f"{m} ({s})" for m, s in result]
                return {
                    "success": True,
                    "count": len(result),
                    "message": f"Popped {len(result)} member(s): {', '.join(members)}",
                }
            return {"success": False, "message": "Sorted set is empty or key does not exist."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # Stream operations

    def xadd(self, key: str, fields: dict[str, str]) -> dict[str, Any]:
        """Add an entry to a stream."""
        if not self._is_native:
            raise NotSupportedError("xadd", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            if not hasattr(cache, "_cache") or not hasattr(cache._cache, "xadd"):
                return {"success": False, "message": "Stream operations not available."}
            entry_id = cache._cache.xadd(key, fields)
            return {"success": True, "entry_id": entry_id, "message": f"Added entry {entry_id}."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def xdel(self, key: str, entry_id: str) -> dict[str, Any]:
        """Delete an entry from a stream."""
        if not self._is_native:
            raise NotSupportedError("xdel", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            if not hasattr(cache, "_cache") or not hasattr(cache._cache, "xdel"):
                return {"success": False, "message": "Stream operations not available."}
            deleted = cache._cache.xdel(key, entry_id)
            if deleted:
                return {"success": True, "message": f"Deleted entry {entry_id}."}
            return {"success": False, "message": f"Entry {entry_id} not found."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def xtrim(self, key: str, maxlen: int) -> dict[str, Any]:
        """Trim a stream to a maximum length."""
        if not self._is_native:
            raise NotSupportedError("xtrim", "wrapped backend")

        try:
            cache = cast("Any", self._cache)
            if not hasattr(cache, "_cache") or not hasattr(cache._cache, "xtrim"):
                return {"success": False, "message": "Stream operations not available."}
            trimmed = cache._cache.xtrim(key, maxlen=maxlen)
            return {"success": True, "message": f"Trimmed {trimmed} entries."}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    # ===========================================================================
    # CachePanel-compatible aliases
    # These methods provide backwards compatibility with the CachePanel interface
    # used in views. They delegate to the main CacheService methods.
    # ===========================================================================

    def is_feature_supported(self, feature: str) -> bool:
        """Check if a feature is supported (CachePanel compatibility)."""
        return self.supports(feature)

    @property
    def abilities(self) -> dict[str, bool]:
        """Get abilities dict (CachePanel compatibility)."""
        # Build abilities dict based on what's supported
        features = [
            "query",
            "get_key",
            "delete_key",
            "edit_key",
            "add_key",
            "flush_cache",
            "get_ttl",
            "set_ttl",
            "get_type",
            "get_type_data",
            "get_size",
            "info",
            "slowlog",
            "list_ops",
            "set_ops",
            "hash_ops",
            "zset_ops",
            "stream_ops",
        ]
        return {f: self.supports(f) for f in features}

    def get_key(self, key: str) -> dict[str, Any]:
        """Get a key (CachePanel compatibility)."""
        return self.get(key)

    def delete_key(self, key: str) -> dict[str, Any]:
        """Delete a key (CachePanel compatibility)."""
        return self.delete(key)

    def edit_key(self, key: str, value: Any, timeout: float | None = None) -> dict[str, Any]:
        """Edit a key (CachePanel compatibility)."""
        return self.set(key, value, timeout)

    def flush_cache(self) -> dict[str, Any]:
        """Flush cache (CachePanel compatibility)."""
        return self.clear()

    def query(
        self,
        instance_alias: str,  # Ignored, for compatibility
        pattern: str = "*",
        cursor: int = 0,
        count: int = 100,
    ) -> dict[str, Any]:
        """Query keys (CachePanel compatibility)."""
        return self.keys(pattern, cursor, count)

    def get_key_ttl(self, key: str) -> int | None:
        """Get TTL (CachePanel compatibility)."""
        try:
            return self.ttl(key)
        except NotSupportedError:
            return None

    def set_key_ttl(self, key: str, ttl: int) -> dict[str, Any]:
        """Set TTL (CachePanel compatibility)."""
        try:
            return self.expire(key, ttl)
        except NotSupportedError:
            return {"success": False, "message": "Setting TTL is not supported."}

    def persist_key(self, key: str) -> dict[str, Any]:
        """Persist key (CachePanel compatibility)."""
        try:
            return self.persist(key)
        except NotSupportedError:
            return {"success": False, "message": "Persist is not supported."}

    def get_key_type(self, key: str) -> str | None:
        """Get type (CachePanel compatibility)."""
        try:
            return self.type(key)
        except NotSupportedError:
            return None

    def get_key_size(self, key: str, key_type: str | None = None) -> int | None:
        """Get size (CachePanel compatibility)."""
        return self.size(key, key_type)

    def slowlog(self, count: int = 25) -> dict[str, Any]:
        """Get slowlog (CachePanel compatibility)."""
        return self.slowlog_get(count)

    # List operation aliases
    def list_lpop(self, key: str, count: int = 1) -> dict[str, Any]:
        """List lpop (CachePanel compatibility)."""
        try:
            return self.lpop(key, count)
        except NotSupportedError:
            return {"success": False, "message": "List operations not supported."}

    def list_rpop(self, key: str, count: int = 1) -> dict[str, Any]:
        """List rpop (CachePanel compatibility)."""
        try:
            return self.rpop(key, count)
        except NotSupportedError:
            return {"success": False, "message": "List operations not supported."}

    def list_lpush(self, key: str, value: str) -> dict[str, Any]:
        """List lpush (CachePanel compatibility)."""
        try:
            return self.lpush(key, value)
        except NotSupportedError:
            return {"success": False, "message": "List operations not supported."}

    def list_rpush(self, key: str, value: str) -> dict[str, Any]:
        """List rpush (CachePanel compatibility)."""
        try:
            return self.rpush(key, value)
        except NotSupportedError:
            return {"success": False, "message": "List operations not supported."}

    def list_lrem(self, key: str, value: str, count: int = 0) -> dict[str, Any]:
        """List lrem (CachePanel compatibility)."""
        try:
            return self.lrem(key, value, count)
        except NotSupportedError:
            return {"success": False, "message": "List operations not supported."}

    def list_ltrim(self, key: str, start: int, stop: int) -> dict[str, Any]:
        """List ltrim (CachePanel compatibility)."""
        try:
            return self.ltrim(key, start, stop)
        except NotSupportedError:
            return {"success": False, "message": "List operations not supported."}

    # Set operation aliases
    def set_sadd(self, key: str, member: str) -> dict[str, Any]:
        """Set sadd (CachePanel compatibility)."""
        try:
            return self.sadd(key, member)
        except NotSupportedError:
            return {"success": False, "message": "Set operations not supported."}

    def set_srem(self, key: str, member: str) -> dict[str, Any]:
        """Set srem (CachePanel compatibility)."""
        try:
            return self.srem(key, member)
        except NotSupportedError:
            return {"success": False, "message": "Set operations not supported."}

    def set_spop(self, key: str, count: int = 1) -> dict[str, Any]:
        """Set spop (CachePanel compatibility)."""
        try:
            return self.spop(key, count)
        except NotSupportedError:
            return {"success": False, "message": "Set operations not supported."}

    # Hash operation aliases
    def hash_hset(self, key: str, field: str, value: str) -> dict[str, Any]:
        """Hash hset (CachePanel compatibility)."""
        try:
            return self.hset(key, field, value)
        except NotSupportedError:
            return {"success": False, "message": "Hash operations not supported."}

    def hash_hdel(self, key: str, field: str) -> dict[str, Any]:
        """Hash hdel (CachePanel compatibility)."""
        try:
            return self.hdel(key, field)
        except NotSupportedError:
            return {"success": False, "message": "Hash operations not supported."}

    # Sorted set operation aliases
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
        """Sorted set zadd (CachePanel compatibility)."""
        try:
            return self.zadd(key, member, score, nx=nx, xx=xx, gt=gt, lt=lt)
        except NotSupportedError:
            return {"success": False, "message": "Sorted set operations not supported."}

    def zset_zrem(self, key: str, member: str) -> dict[str, Any]:
        """Sorted set zrem (CachePanel compatibility)."""
        try:
            return self.zrem(key, member)
        except NotSupportedError:
            return {"success": False, "message": "Sorted set operations not supported."}

    def zset_zpopmin(self, key: str, count: int = 1) -> dict[str, Any]:
        """Sorted set zpopmin (CachePanel compatibility)."""
        try:
            return self.zpopmin(key, count=count)
        except NotSupportedError:
            return {"success": False, "message": "Sorted set operations not supported."}

    def zset_zpopmax(self, key: str, count: int = 1) -> dict[str, Any]:
        """Sorted set zpopmax (CachePanel compatibility)."""
        try:
            return self.zpopmax(key, count=count)
        except NotSupportedError:
            return {"success": False, "message": "Sorted set operations not supported."}

    # Stream operation aliases
    def stream_xadd(self, key: str, fields: dict[str, str]) -> dict[str, Any]:
        """Stream xadd (CachePanel compatibility)."""
        try:
            return self.xadd(key, fields)
        except NotSupportedError:
            return {"success": False, "message": "Stream operations not supported."}

    def stream_xdel(self, key: str, entry_id: str) -> dict[str, Any]:
        """Stream xdel (CachePanel compatibility)."""
        try:
            return self.xdel(key, entry_id)
        except NotSupportedError:
            return {"success": False, "message": "Stream operations not supported."}

    def stream_xtrim(self, key: str, maxlen: int) -> dict[str, Any]:
        """Stream xtrim (CachePanel compatibility)."""
        try:
            return self.xtrim(key, maxlen)
        except NotSupportedError:
            return {"success": False, "message": "Stream operations not supported."}


def get_cache_service(cache_name: str) -> CacheService:
    """Get a CacheService for the given cache name.

    For django-cachex backends, uses the cache directly.
    For Django builtin backends, wraps them first.

    Args:
        cache_name: The name of the cache in CACHES setting.

    Returns:
        A CacheService instance.

    Raises:
        ValueError: If the cache is not configured.
    """
    cache_config = settings.CACHES.get(cache_name)
    if not cache_config:
        msg = f"Cache '{cache_name}' is not configured in CACHES setting."
        raise ValueError(msg)

    backend = str(cache_config.get("BACKEND", ""))
    cache = caches[cache_name]

    # Native django-cachex - use directly
    if _is_native_backend(backend):
        return CacheService(cache_name, cache)

    # Django builtin - wrap first
    wrapped = get_wrapper(cache, cache_name)
    return CacheService(cache_name, wrapped)
