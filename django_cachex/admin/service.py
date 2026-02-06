"""
CacheService - unified service for cache admin operations.

This module provides the CacheService class which is the main interface
for cache admin views. It expects a cachex-like interface and uses
wrappers to adapt Django builtin caches.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Mapping

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

    def set(self, key: str, value: Any, timeout: float | None = None) -> None:
        """Set a key value in the cache."""
        self._cache.set(key, value, timeout=timeout)

    def delete(self, key: str) -> bool:
        """Delete a key from the cache. Returns True if key existed."""
        return self._cache.delete(key)

    def clear(self) -> None:
        """Clear all keys from the cache."""
        self._cache.clear()

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
        try:
            if key_type is None:
                key_type = self.type(key)
        except NotSupportedError:
            return {}

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
        try:
            if key_type is None:
                key_type = self.type(key)
        except NotSupportedError:
            return None

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
        for display in the cache admin.

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

        try:
            raw_info = self.info()
        except NotSupportedError:
            raise
        except Exception:  # noqa: BLE001
            raise NotSupportedError("metadata", self.__class__.__name__) from None

        if not raw_info:
            return base_info

        # Check if info() returned already-structured data (from wrappers)
        if isinstance(raw_info.get("server"), dict):
            base_info.update(raw_info)
            return base_info

        # Parse flat Redis/Valkey INFO into structured sections
        try:
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

        cache = cast("Any", self._cache)

        # Try cache's slowlog_get first - wrappers return structured result
        if hasattr(cache, "slowlog_get"):
            try:
                slowlog_result = cache.slowlog_get(count)
                # Wrappers return structured dict with "entries" key
                if isinstance(slowlog_result, dict) and "entries" in slowlog_result:
                    return slowlog_result
                # Native backends return raw entries list - need length too
                if hasattr(cache, "slowlog_len"):
                    result["length"] = cache.slowlog_len()
                result["entries"] = [self._parse_slowlog_entry(entry) for entry in slowlog_result]
                return result
            except NotSupportedError:
                raise
            except Exception as e:  # noqa: BLE001
                result["error"] = str(e)
                return result

        # Fall back to internal cache client for native backends
        if hasattr(cache, "_cache") and hasattr(cache._cache, "slowlog_get"):
            try:
                result["length"] = cache._cache.slowlog_len()
                raw_entries = cache._cache.slowlog_get(count)
                result["entries"] = [self._parse_slowlog_entry(entry) for entry in raw_entries]
                return result
            except Exception as e:  # noqa: BLE001
                result["error"] = str(e)
                return result

        result["error"] = "Slow log not available for this backend."
        return result

    # List operations

    def lpop(self, key: str, count: int = 1) -> Any:
        """Pop from the left of a list. Returns value(s) or None if empty."""
        cache = cast("Any", self._cache)
        if count == 1:
            return cache.lpop(key)
        return cache.lpop(key, count=count)

    def rpop(self, key: str, count: int = 1) -> Any:
        """Pop from the right of a list. Returns value(s) or None if empty."""
        cache = cast("Any", self._cache)
        if count == 1:
            return cache.rpop(key)
        return cache.rpop(key, count=count)

    def lpush(self, key: str, value: str) -> int:
        """Push to the left of a list. Returns new length."""
        cache = cast("Any", self._cache)
        return cache.lpush(key, value)

    def rpush(self, key: str, value: str) -> int:
        """Push to the right of a list. Returns new length."""
        cache = cast("Any", self._cache)
        return cache.rpush(key, value)

    def lrem(self, key: str, value: str, count: int = 0) -> int:
        """Remove elements from a list. Returns count removed."""
        cache = cast("Any", self._cache)
        return cache.lrem(key, count, value)

    def ltrim(self, key: str, start: int, stop: int) -> None:
        """Trim a list to the specified range."""
        cache = cast("Any", self._cache)
        cache.ltrim(key, start, stop)

    # Set operations

    def sadd(self, key: str, member: str) -> bool:
        """Add a member to a set. Returns True if member was new."""
        cache = cast("Any", self._cache)
        return bool(cache.sadd(key, member))

    def srem(self, key: str, member: str) -> bool:
        """Remove a member from a set. Returns True if member existed."""
        cache = cast("Any", self._cache)
        return bool(cache.srem(key, member))

    def spop(self, key: str, count: int = 1) -> Any:
        """Pop random members from a set. Returns value(s) or None if empty."""
        cache = cast("Any", self._cache)
        if count == 1:
            return cache.spop(key)
        return cache.spop(key, count=count)

    # Hash operations

    def hset(self, key: str, field: str, value: str) -> None:
        """Set a field in a hash."""
        cache = cast("Any", self._cache)
        cache.hset(key, field, value)

    def hdel(self, key: str, field: str) -> bool:
        """Delete a field from a hash. Returns True if field existed."""
        cache = cast("Any", self._cache)
        return bool(cache.hdel(key, field))

    # Sorted set operations

    def zadd(
        self,
        key: str,
        mapping: Mapping[str, float],
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        """Add members to a sorted set. Returns number of elements added."""
        cache = cast("Any", self._cache)
        return cache.zadd(key, mapping, nx=nx, xx=xx, gt=gt, lt=lt)

    def zrem(self, key: str, member: str) -> bool:
        """Remove a member from a sorted set. Returns True if member existed."""
        cache = cast("Any", self._cache)
        return bool(cache.zrem(key, member))

    def zpopmin(self, key: str, count: int = 1) -> list[tuple[str, float]]:
        """Pop the member(s) with lowest score. Returns list of (member, score) tuples."""
        cache = cast("Any", self._cache)
        return cache.zpopmin(key, count=count)

    def zpopmax(self, key: str, count: int = 1) -> list[tuple[str, float]]:
        """Pop the member(s) with highest score. Returns list of (member, score) tuples."""
        cache = cast("Any", self._cache)
        return cache.zpopmax(key, count=count)

    # Stream operations

    def xadd(self, key: str, fields: dict[str, str]) -> str:
        """Add an entry to a stream. Returns the entry ID."""
        cache = cast("Any", self._cache)
        return cache._cache.xadd(key, fields)

    def xdel(self, key: str, entry_id: str) -> bool:
        """Delete an entry from a stream. Returns True if entry existed."""
        cache = cast("Any", self._cache)
        return bool(cache._cache.xdel(key, entry_id))

    def xtrim(self, key: str, maxlen: int) -> int:
        """Trim a stream to a maximum length. Returns number of entries removed."""
        cache = cast("Any", self._cache)
        return cache._cache.xtrim(key, maxlen=maxlen)


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
