"""
Cache wrappers that adapt Django builtin caches to a cachex-like interface.

These wrappers provide a consistent API for the CacheService to use,
raising NotSupportedError for operations that the underlying cache
doesn't support.
"""

from __future__ import annotations

import contextlib
import fnmatch
import time
from typing import TYPE_CHECKING, Any, cast

from django.conf import settings
from django.db import connection

from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet
    from pathlib import Path

    from django.core.cache.backends.base import BaseCache


class BaseCacheWrapper:
    """Base wrapper that adapts Django's BaseCache to a cachex-like interface.

    Provides core operations that work with any Django cache backend.
    Extended operations raise NotSupportedError by default.
    """

    def __init__(self, cache: BaseCache, cache_name: str) -> None:
        self._cache = cache
        self._cache_name = cache_name
        self._cache_config = settings.CACHES.get(cache_name, {})

    @property
    def key_prefix(self) -> str:
        """Get the cache key prefix."""
        return getattr(self._cache, "key_prefix", "") or ""

    @property
    def version(self) -> int:
        """Get the cache version."""
        return getattr(self._cache, "version", 1)

    def make_key(self, key: str) -> str:
        """Get the full cache key including prefix and version."""
        if hasattr(self._cache, "make_key"):
            return self._cache.make_key(key)
        return key

    # Core operations - always available via Django's BaseCache

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the cache."""
        return self._cache.get(key, default)

    def set(self, key: str, value: Any, timeout: float | None = None) -> bool:
        """Set a value in the cache."""
        self._cache.set(key, value, timeout=timeout)
        return True

    def delete(self, key: str) -> bool:
        """Delete a key from the cache."""
        self._cache.delete(key)
        return True

    def clear(self) -> bool:
        """Clear all keys from the cache."""
        self._cache.clear()
        return True

    # Extended operations - raise NotSupportedError by default

    def keys(self, pattern: str = "*") -> list[str]:
        """List keys matching the pattern."""
        raise NotSupportedError("keys", self.__class__.__name__)

    def ttl(self, key: str) -> int:
        """Get the TTL of a key in seconds.

        Returns:
            TTL in seconds, -1 for no expiry, -2 for key not found.
        """
        raise NotSupportedError("ttl", self.__class__.__name__)

    def expire(self, key: str, timeout: int) -> bool:
        """Set the TTL of a key."""
        raise NotSupportedError("expire", self.__class__.__name__)

    def persist(self, key: str) -> bool:
        """Remove the TTL from a key."""
        raise NotSupportedError("persist", self.__class__.__name__)

    def type(self, key: str) -> str | None:
        """Get the data type of a key."""
        raise NotSupportedError("type", self.__class__.__name__)

    def info(self) -> dict[str, Any]:
        """Get cache server information.

        Returns basic info for wrapped caches since they don't have
        Redis-like INFO commands.
        """
        return {}

    def slowlog_get(self, count: int = 25) -> dict[str, Any]:
        """Get slow query log entries.

        Returns empty result for wrapped caches since they don't have
        Redis-like SLOWLOG commands.
        """
        return {
            "entries": [],
            "length": 0,
            "error": "Slow log is not available for this backend.",
        }

    # String operations

    def incr(self, key: str, delta: int = 1) -> int:
        """Increment a numeric value."""
        raise NotSupportedError("incr", self.__class__.__name__)

    # List operations

    def lrange(self, key: str, start: int, stop: int) -> list[Any]:
        """Get a range of elements from a list."""
        raise NotSupportedError("lrange", self.__class__.__name__)

    def llen(self, key: str) -> int:
        """Get the length of a list."""
        raise NotSupportedError("llen", self.__class__.__name__)

    def lpush(self, key: str, *values: Any) -> int:
        """Push values to the left of a list."""
        raise NotSupportedError("lpush", self.__class__.__name__)

    def rpush(self, key: str, *values: Any) -> int:
        """Push values to the right of a list."""
        raise NotSupportedError("rpush", self.__class__.__name__)

    def lpop(self, key: str, count: int | None = None) -> Any:
        """Pop from the left of a list."""
        raise NotSupportedError("lpop", self.__class__.__name__)

    def rpop(self, key: str, count: int | None = None) -> Any:
        """Pop from the right of a list."""
        raise NotSupportedError("rpop", self.__class__.__name__)

    def lrem(self, key: str, count: int, value: Any) -> int:
        """Remove elements from a list."""
        raise NotSupportedError("lrem", self.__class__.__name__)

    def ltrim(self, key: str, start: int, stop: int) -> bool:
        """Trim a list to the specified range."""
        raise NotSupportedError("ltrim", self.__class__.__name__)

    # Set operations

    def smembers(self, key: str) -> AbstractSet[Any]:
        """Get all members of a set."""
        raise NotSupportedError("smembers", self.__class__.__name__)

    def scard(self, key: str) -> int:
        """Get the number of members in a set."""
        raise NotSupportedError("scard", self.__class__.__name__)

    def sadd(self, key: str, *members: Any) -> int:
        """Add members to a set."""
        raise NotSupportedError("sadd", self.__class__.__name__)

    def srem(self, key: str, *members: Any) -> int:
        """Remove members from a set."""
        raise NotSupportedError("srem", self.__class__.__name__)

    def spop(self, key: str, count: int | None = None) -> Any:
        """Pop random members from a set."""
        raise NotSupportedError("spop", self.__class__.__name__)

    # Hash operations

    def hgetall(self, key: str) -> dict[str, Any]:
        """Get all fields and values of a hash."""
        raise NotSupportedError("hgetall", self.__class__.__name__)

    def hlen(self, key: str) -> int:
        """Get the number of fields in a hash."""
        raise NotSupportedError("hlen", self.__class__.__name__)

    def hset(self, key: str, field: str, value: Any) -> int:
        """Set a field in a hash."""
        raise NotSupportedError("hset", self.__class__.__name__)

    def hdel(self, key: str, *fields: str) -> int:
        """Delete fields from a hash."""
        raise NotSupportedError("hdel", self.__class__.__name__)

    # Sorted set operations

    def zrange(
        self,
        key: str,
        start: int,
        stop: int,
        withscores: bool = False,
    ) -> list[Any]:
        """Get a range of members from a sorted set."""
        raise NotSupportedError("zrange", self.__class__.__name__)

    def zcard(self, key: str) -> int:
        """Get the number of members in a sorted set."""
        raise NotSupportedError("zcard", self.__class__.__name__)

    def zadd(
        self,
        key: str,
        mapping: dict[str, float],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int:
        """Add members to a sorted set."""
        raise NotSupportedError("zadd", self.__class__.__name__)

    def zrem(self, key: str, *members: Any) -> int:
        """Remove members from a sorted set."""
        raise NotSupportedError("zrem", self.__class__.__name__)

    def zpopmin(self, key: str, count: int = 1) -> list[tuple[Any, float]]:
        """Pop members with lowest scores from a sorted set."""
        raise NotSupportedError("zpopmin", self.__class__.__name__)

    def zpopmax(self, key: str, count: int = 1) -> list[tuple[Any, float]]:
        """Pop members with highest scores from a sorted set."""
        raise NotSupportedError("zpopmax", self.__class__.__name__)


def _format_bytes(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    size: float = float(size_bytes)
    for unit in ("B", "K", "M", "G", "T"):
        if abs(size) < 1024:
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size = size / 1024
    return f"{size:.1f}P"


def _deep_getsizeof(obj: Any, seen: set[int] | None = None) -> int:
    """Recursively calculate the deep size of an object in bytes.

    Handles nested dicts, lists, tuples, sets, and other common types.
    Uses object ids to avoid counting the same object twice.
    """
    import sys

    if seen is None:
        seen = set()

    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)

    size = sys.getsizeof(obj)

    if isinstance(obj, dict):
        size += sum(_deep_getsizeof(k, seen) + _deep_getsizeof(v, seen) for k, v in obj.items())
    elif isinstance(obj, (list, tuple, set, frozenset)):
        size += sum(_deep_getsizeof(item, seen) for item in obj)
    elif hasattr(obj, "__dict__"):
        size += _deep_getsizeof(obj.__dict__, seen)

    return size


class LocMemCacheWrapper(BaseCacheWrapper):
    """Wrapper for Django's LocMemCache.

    Enables keys() by accessing the internal _cache dict.
    """

    def _get_internal_cache(self) -> dict[str, Any]:
        """Access the internal cache dictionary of LocMemCache."""
        return getattr(self._cache, "_cache", {})

    def _get_expire_info(self) -> dict[str, float]:
        """Access the internal expiry info dictionary."""
        return getattr(self._cache, "_expire_info", {})

    def info(self) -> dict[str, Any]:
        """Get LocMemCache info in structured format matching Redis INFO."""
        internal_cache = self._get_internal_cache()
        expire_info = self._get_expire_info()
        key_count = len(internal_cache)

        # Count keys with TTL set
        import time

        current_time = time.time()
        keys_with_expiry = sum(1 for exp in expire_info.values() if exp and exp > current_time)

        # Estimate memory usage with deep size calculation
        try:
            # Calculate deep size of all cached values (includes nested objects)
            total_size = sum(_deep_getsizeof(v) for v in internal_cache.values())
            # Also include key strings and the cache dict overhead
            total_size += sum(_deep_getsizeof(k) for k in internal_cache)
        except Exception:  # noqa: BLE001
            total_size = 0

        max_entries = getattr(self._cache, "_max_entries", 300)
        cull_frequency = getattr(self._cache, "_cull_frequency", 3)

        # Return structured data matching template expectations
        return {
            "backend": "LocMemCache",
            "server": {
                "redis_version": "LocMemCache (in-process)",
                "process_id": None,
            },
            "memory": {
                "used_memory": total_size,
                "used_memory_human": _format_bytes(total_size),
                "maxmemory": max_entries,
                "maxmemory_human": f"{max_entries} entries",
                "maxmemory_policy": f"cull 1/{cull_frequency} when full",
            },
            "keyspace": {
                "db0": {
                    "keys": key_count,
                    "expires": keys_with_expiry,
                },
            },
        }

    def keys(self, pattern: str = "*") -> list[str]:
        """List keys matching the pattern by scanning internal cache dict."""
        internal_cache = self._get_internal_cache()
        key_prefix = self.key_prefix

        all_keys = []
        for internal_key in internal_cache:
            # Internal keys are formatted as "{key_prefix}:{version}:{key}"
            # e.g., ":1:mykey" (no prefix) or "myprefix:1:mykey" (with prefix)
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
        return matching_keys


class DatabaseCacheWrapper(BaseCacheWrapper):
    """Wrapper for Django's DatabaseCache.

    Enables keys() by querying the database table.
    Enables ttl() by reading the expires column.
    """

    def _get_table_name(self) -> str:
        """Get the database table name for this cache."""
        return cast("Any", self._cache)._table

    def keys(self, pattern: str = "*") -> list[str]:
        """List keys matching the pattern by querying the database."""
        table_name = self._get_table_name()
        quoted_table_name = connection.ops.quote_name(table_name)

        if pattern and pattern != "*":
            transformed_pattern = self._cache.make_key(pattern)
            sql_pattern = transformed_pattern.replace("*", "%").replace("?", "_")
        else:
            sql_pattern = "%"

        current_time = time.time()

        with connection.cursor() as cursor:
            if connection.vendor in ("postgresql", "oracle"):
                expires_condition = "expires > to_timestamp(%s)"
            elif connection.vendor == "mysql":
                expires_condition = "expires > FROM_UNIXTIME(%s)"
            else:
                expires_condition = "expires > %s"

            # Table name is safely quoted via connection.ops.quote_name()
            keys_sql = f"""
                SELECT cache_key
                FROM {quoted_table_name}
                WHERE cache_key LIKE %s AND {expires_condition}
                ORDER BY cache_key
            """  # noqa: S608
            cursor.execute(keys_sql, [sql_pattern, current_time])
            raw_keys = [row[0] for row in cursor.fetchall()]

            keys = []
            key_prefix = self.key_prefix
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

        return keys

    def ttl(self, key: str) -> int:
        """Get TTL by reading the expires column from database.

        Returns:
            TTL in seconds, -1 for no expiry (not applicable for DB cache),
            -2 for key not found.
        """
        table_name = self._get_table_name()
        quoted_table_name = connection.ops.quote_name(table_name)
        cache_key = self._cache.make_key(key)
        current_time = time.time()

        with connection.cursor() as cursor:
            if connection.vendor in ("postgresql", "oracle"):
                sql = f"""
                    SELECT EXTRACT(EPOCH FROM expires) - %s
                    FROM {quoted_table_name}
                    WHERE cache_key = %s
                """  # noqa: S608
            elif connection.vendor == "mysql":
                sql = f"""
                    SELECT UNIX_TIMESTAMP(expires) - %s
                    FROM {quoted_table_name}
                    WHERE cache_key = %s
                """  # noqa: S608
            else:
                sql = f"""
                    SELECT expires - %s
                    FROM {quoted_table_name}
                    WHERE cache_key = %s
                """  # noqa: S608

            cursor.execute(sql, [current_time, cache_key])
            row = cursor.fetchone()
            if row is None:
                return -2  # Key not found
            ttl = row[0]
            if ttl <= 0:
                return -2  # Key expired
            return int(ttl)

    def info(self) -> dict[str, Any]:
        """Get DatabaseCache info in structured format matching Redis INFO."""
        table_name = self._get_table_name()
        quoted_table_name = connection.ops.quote_name(table_name)

        total_count: int | str = 0
        active_count: int | str = 0
        expired_count: int | str = 0

        try:
            with connection.cursor() as cursor:
                # Get total row count
                cursor.execute(f"SELECT COUNT(*) FROM {quoted_table_name}")  # noqa: S608
                total_count = cursor.fetchone()[0]

                # Get non-expired row count
                current_time = time.time()
                if connection.vendor in ("postgresql", "oracle"):
                    expires_condition = "expires > to_timestamp(%s)"
                elif connection.vendor == "mysql":
                    expires_condition = "expires > FROM_UNIXTIME(%s)"
                else:
                    expires_condition = "expires > %s"

                cursor.execute(
                    f"SELECT COUNT(*) FROM {quoted_table_name} WHERE {expires_condition}",  # noqa: S608
                    [current_time],
                )
                active_count = cursor.fetchone()[0]
                expired_count = (
                    total_count - active_count if isinstance(total_count, int) and isinstance(active_count, int) else 0
                )
        except Exception:  # noqa: BLE001
            total_count = "error"
            active_count = "error"

        # Return structured data matching template expectations
        return {
            "backend": "DatabaseCache",
            "server": {
                "redis_version": f"DatabaseCache ({connection.vendor})",
                "os": f"table: {table_name}",
            },
            "keyspace": {
                "db0": {
                    "keys": active_count if active_count != "error" else 0,
                    "expires": active_count if active_count != "error" else 0,  # All DB cache keys have expiry
                },
            },
            "stats": {
                "expired_keys": expired_count,
            },
        }


class FileCacheWrapper(BaseCacheWrapper):
    """Wrapper for Django's FileBasedCache.

    Lists keys as MD5 hash filenames (original keys cannot be recovered).
    Individual key viewing is not supported since MD5 is one-way.
    """

    def _get_cache_dir(self) -> Path | None:
        """Get the cache directory path."""
        from pathlib import Path

        cache_dir = getattr(self._cache, "_dir", None)
        return Path(cache_dir) if cache_dir else None

    def keys(self, pattern: str = "*") -> list[str]:
        """List cache file hashes as keys.

        Note: These are MD5 hashes of the original keys. The original key
        names cannot be recovered from the hashes.
        """
        cache_path = self._get_cache_dir()
        if not cache_path or not cache_path.exists():
            return []

        try:
            # FileBasedCache stores files in subdirectories like: cache_dir/ab/cd/abcd1234...
            # The filename (without path) is the MD5 hash
            hashes = [fp.name for fp in cache_path.rglob("*") if fp.is_file()]

            # Filter by pattern if not "*"
            if pattern and pattern != "*":
                hashes = [h for h in hashes if fnmatch.fnmatch(h, pattern)]

            hashes.sort()
            return hashes
        except Exception:  # noqa: BLE001
            return []

    def info(self) -> dict[str, Any]:
        """Get FileBasedCache info in structured format matching Redis INFO."""
        cache_path = self._get_cache_dir()

        if not cache_path:
            return {
                "backend": "FileBasedCache",
                "server": {"redis_version": "FileBasedCache", "os": "unknown directory"},
            }

        file_count: int = 0
        total_size: int = 0

        with contextlib.suppress(Exception):
            if cache_path.exists():
                files = list(cache_path.rglob("*"))
                file_count = sum(1 for f in files if f.is_file())
                total_size = sum(f.stat().st_size for f in files if f.is_file())

        # Return structured data matching template expectations
        return {
            "backend": "FileBasedCache",
            "server": {
                "redis_version": "FileBasedCache",
                "os": str(cache_path),
            },
            "memory": {
                "used_memory": total_size,
                "used_memory_human": _format_bytes(total_size),
            },
            "keyspace": {
                "db0": {
                    "keys": file_count,
                },
            },
        }


class MemcachedCacheWrapper(BaseCacheWrapper):
    """Wrapper for Memcached backends.

    Cannot list keys, but can get stats via info().
    """

    def info(self) -> dict[str, Any]:
        """Get memcached stats in structured format matching Redis INFO."""
        cache = cast("Any", self._cache)

        # Try to get memcached stats
        mc_stats: dict[str, Any] = {}
        if hasattr(cache, "_cache") and hasattr(cache._cache, "stats"):
            with contextlib.suppress(Exception):
                mc_stats = cache._cache.stats() or {}

        # Extract relevant stats (memcached stats format varies by library)
        # Common fields: bytes, curr_items, total_connections, cmd_get, cmd_set
        bytes_used = 0
        curr_items = 0
        for server_stats in mc_stats.values() if mc_stats else []:
            if isinstance(server_stats, dict):
                bytes_used += int(server_stats.get("bytes", 0))
                curr_items += int(server_stats.get("curr_items", 0))

        return {
            "backend": "Memcached",
            "server": {
                "redis_version": "Memcached",
            },
            "memory": {
                "used_memory": bytes_used,
                "used_memory_human": _format_bytes(bytes_used) if bytes_used else None,
            },
            "keyspace": {
                "db0": {
                    "keys": curr_items,
                },
            }
            if curr_items
            else None,
        }


class DjangoRedisCacheWrapper(BaseCacheWrapper):
    """Wrapper for Django's builtin Redis cache backend.

    Provides basic support for django.core.cache.backends.redis.RedisCache.
    For full Redis/Valkey functionality (keys listing, TTL inspection, data type
    operations), use django-cachex's ValkeyCache or RedisCache backends instead.
    """

    # Note: Django's Redis backend doesn't expose SCAN/KEYS by default,
    # so we keep functionality minimal. Users should migrate to cachex for
    # full Redis features.

    def info(self) -> dict[str, Any]:
        """Get Django Redis cache info in structured format."""
        return {
            "backend": "Django RedisCache",
            "server": {
                "redis_version": "Django RedisCache (limited support)",
                "os": "Use django-cachex ValkeyCache/RedisCache for full features",
            },
        }


class DummyCacheWrapper(BaseCacheWrapper):
    """Wrapper for DummyCache.

    All operations raise NotSupportedError since DummyCache discards everything.
    """

    def get(self, key: str, default: Any = None) -> Any:
        """Get always returns default for DummyCache."""
        return default

    def set(self, key: str, value: Any, timeout: float | None = None) -> bool:
        """Set is a no-op for DummyCache."""
        return True

    def delete(self, key: str) -> bool:
        """Delete is a no-op for DummyCache."""
        return True

    def clear(self) -> bool:
        """Clear is a no-op for DummyCache."""
        return True

    def keys(self, pattern: str = "*") -> list[str]:
        """DummyCache has no keys."""
        return []

    def info(self) -> dict[str, Any]:
        """Get DummyCache info in structured format."""
        return {
            "backend": "DummyCache",
            "server": {
                "redis_version": "DummyCache (no storage)",
                "os": "All data is discarded - used for development/testing",
            },
            "keyspace": {
                "db0": {
                    "keys": 0,
                },
            },
        }


# Mapping of backend class paths to wrapper classes
WRAPPER_MAP: dict[str, type[BaseCacheWrapper]] = {
    # Local memory cache
    "django.core.cache.backends.locmem.LocMemCache": LocMemCacheWrapper,
    # Database cache
    "django.core.cache.backends.db.DatabaseCache": DatabaseCacheWrapper,
    # File-based cache
    "django.core.cache.backends.filebased.FileBasedCache": FileCacheWrapper,
    # Dummy cache
    "django.core.cache.backends.dummy.DummyCache": DummyCacheWrapper,
    # Memcached backends
    "django.core.cache.backends.memcached.PyMemcacheCache": MemcachedCacheWrapper,
    "django.core.cache.backends.memcached.PyLibMCCache": MemcachedCacheWrapper,
    "django.core.cache.backends.memcached.MemcachedCache": MemcachedCacheWrapper,
    # Django's builtin Redis cache (Django 4.0+)
    "django.core.cache.backends.redis.RedisCache": DjangoRedisCacheWrapper,
}


def get_wrapper(cache: BaseCache, cache_name: str) -> BaseCacheWrapper:
    """Get the appropriate wrapper for a cache backend.

    Args:
        cache: The Django cache instance.
        cache_name: The name of the cache in CACHES setting.

    Returns:
        A wrapper instance for the cache.
    """
    cache_config = settings.CACHES.get(cache_name, {})
    backend = str(cache_config.get("BACKEND", ""))

    wrapper_class = WRAPPER_MAP.get(backend, BaseCacheWrapper)
    return wrapper_class(cache, cache_name)
