"""Helper functions for cache admin views."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.core.cache import caches

from django_cachex.exceptions import NotSupportedError
from django_cachex.types import KeyType

from .wrappers import _deep_getsizeof, wrap_cache

if TYPE_CHECKING:
    from collections.abc import Mapping


def get_cache(cache_name: str) -> Any:
    """Get a cache backend, wrapping if needed for admin compatibility."""
    cache_config = settings.CACHES.get(cache_name)
    if not cache_config:
        msg = f"Cache '{cache_name}' is not configured in CACHES setting."
        raise ValueError(msg)

    backend = str(cache_config.get("BACKEND", ""))
    cache = caches[cache_name]

    # Native django-cachex - use directly
    if backend.startswith("django_cachex."):
        return cache

    # Django builtin or unknown - wrap first
    return wrap_cache(cache)


def get_metadata(cache: Any, cache_config: Mapping[str, Any]) -> dict[str, Any]:
    """Get cache metadata with parsed server information."""
    # Get location from config
    location = cache_config.get("LOCATION", "")
    if isinstance(location, list):
        location = ", ".join(location)
    else:
        location = str(location) if location else ""

    base_info: dict[str, Any] = {
        "backend": str(cache_config.get("BACKEND", "")),
        "key_prefix": cache.key_prefix,
        "version": cache.version,
        "location": location,
        "server": None,
        "keyspace": None,
        "memory": None,
        "clients": None,
        "stats": None,
    }

    try:
        raw_info = cache.info()
    except NotSupportedError:
        raise
    except Exception:  # noqa: BLE001
        raise NotSupportedError("info", cache.__class__.__name__) from None

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


def get_type_data(cache: Any, key: str, key_type: str | None = None) -> dict[str, Any]:
    """Get type-specific data for a key."""
    try:
        if key_type is None:
            key_type = cache.type(key)
    except NotSupportedError:
        key_type = None

    if not key_type or key_type == KeyType.STRING:
        return {}

    result: dict[str, Any] = {}
    try:
        match key_type:
            case KeyType.LIST:
                items = [str(i) for i in cache.lrange(key, 0, -1)]
                result = {"items": items, "length": len(items)}
            case KeyType.HASH:
                fields = {str(k): str(v) for k, v in cache.hgetall(key).items()}
                result = {"fields": fields, "length": len(fields)}
            case KeyType.SET:
                members = sorted(str(m) for m in cache.smembers(key))
                result = {"members": members, "length": len(members)}
            case KeyType.ZSET:
                zset_members = [(str(m), s) for m, s in cache.zrange(key, 0, -1, withscores=True)]
                result = {"members": zset_members, "length": len(zset_members)}
            case KeyType.STREAM if hasattr(cache, "_cache") and hasattr(cache._cache, "xrange"):
                entries = cache._cache.xrange(key, count=100)
                result = {"entries": entries, "length": cache._cache.xlen(key)}
    except Exception:  # noqa: BLE001, S110
        pass
    return result


def get_size(cache: Any, key: str, key_type: str | None = None) -> int | None:
    """Get the size/length of a key."""
    try:
        if key_type is None:
            key_type = cache.type(key)
    except NotSupportedError:
        return None

    if not key_type:
        return None

    def _string_size() -> int | None:
        # Use STRLEN via raw client when available
        try:
            client = cache.get_client(write=False)
            full_key = cache.make_key(key)
            return client.strlen(full_key)
        except (NotSupportedError, AttributeError):
            pass
        # Fallback: compute Python object size (e.g. LocMemCache)
        value = cache.get(key)
        return _deep_getsizeof(value) if value is not None else None

    try:
        size_methods: dict[str, Any] = {
            KeyType.STRING: _string_size,
            KeyType.LIST: lambda: cache.llen(key),
            KeyType.SET: lambda: cache.scard(key),
            KeyType.HASH: lambda: cache.hlen(key),
            KeyType.ZSET: lambda: cache.zcard(key),
            KeyType.STREAM: lambda: cache.xlen(key),
        }

        method = size_methods.get(key_type)
        return method() if method else None
    except Exception:  # noqa: BLE001
        return None


def _parse_slowlog_entry(entry: Any) -> dict[str, Any]:
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


def get_slowlog(cache: Any, count: int = 25) -> dict[str, Any]:
    """Get slow query log entries."""
    result: dict[str, Any] = {
        "entries": [],
        "length": 0,
        "error": None,
    }

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
            result["entries"] = [_parse_slowlog_entry(entry) for entry in slowlog_result]
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
            result["entries"] = [_parse_slowlog_entry(entry) for entry in raw_entries]
            return result
        except Exception as e:  # noqa: BLE001
            result["error"] = str(e)
            return result

    result["error"] = "Slow log not available for this backend."
    return result
