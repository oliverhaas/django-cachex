"""Helper functions for cache admin views."""

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.core.cache import caches
from django.utils.translation import gettext_lazy as _

from django_cachex.exceptions import NotSupportedError
from django_cachex.types import KeyType
from django_cachex.utils import _deep_getsizeof

if TYPE_CHECKING:
    from collections.abc import Mapping


def _row(label: Any, value: Any) -> dict[str, Any] | None:
    """Build one display-row dict, or None if there's nothing to show."""
    if value is None or value == "":
        return None
    return {"label": label, "value": value}


def _server_rows(server: Mapping[str, Any]) -> list[dict[str, Any]]:
    uptime_days = server.get("uptime_in_days")
    uptime_seconds = server.get("uptime_in_seconds")
    uptime = (
        f"{uptime_days} days ({uptime_seconds} seconds)"
        if uptime_days is not None and uptime_seconds is not None
        else None
    )
    arch = server.get("arch_bits")
    candidates = [
        _row(_("Redis/Valkey Version"), server.get("redis_version")),
        _row(_("Operating System"), server.get("os")),
        _row(_("Architecture"), f"{arch}-bit" if arch else None),
        _row(_("TCP Port"), server.get("tcp_port")),
        _row(_("Uptime"), uptime),
        _row(_("Process ID"), server.get("process_id")),
    ]
    return [r for r in candidates if r is not None]


def _memory_rows(memory: Mapping[str, Any]) -> list[dict[str, Any]]:
    used_human = memory.get("used_memory_human")
    used = memory.get("used_memory")
    peak_human = memory.get("used_memory_peak_human")
    peak = memory.get("used_memory_peak")
    candidates = [
        _row(
            _("Used Memory"),
            f"{used_human} ({used} bytes)" if used_human and used is not None else None,
        ),
        _row(
            _("Peak Memory"),
            f"{peak_human} ({peak} bytes)" if peak_human and peak is not None else None,
        ),
        _row(_("Max Memory"), memory.get("maxmemory_human") or memory.get("maxmemory")),
        _row(_("Eviction Policy"), memory.get("maxmemory_policy")),
    ]
    return [r for r in candidates if r is not None]


def _clients_rows(clients: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates = [
        _row(_("Connected Clients"), clients.get("connected_clients")),
        _row(_("Blocked Clients"), clients.get("blocked_clients")),
    ]
    return [r for r in candidates if r is not None]


def _stats_rows(stats: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates = [
        _row(_("Total Connections"), stats.get("total_connections_received")),
        _row(_("Total Commands"), stats.get("total_commands_processed")),
        _row(_("Ops/sec"), stats.get("instantaneous_ops_per_sec")),
        _row(_("Keyspace Hits"), stats.get("keyspace_hits")),
        _row(_("Keyspace Misses"), stats.get("keyspace_misses")),
        _row(_("Expired Keys"), stats.get("expired_keys")),
        _row(_("Evicted Keys"), stats.get("evicted_keys")),
    ]
    return [r for r in candidates if r is not None]


def get_cache(cache_name: str) -> Any:
    """Get a cache backend for admin use."""
    cache_config = settings.CACHES.get(cache_name)
    if not cache_config:
        msg = f"Cache '{cache_name}' is not configured in CACHES setting."
        raise ValueError(msg)
    return caches[cache_name]


def parse_metadata(
    cache: Any,
    cache_config: Mapping[str, Any],
    raw_info: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build structured cache metadata from a pre-fetched ``cache.info()`` result.

    Pure parsing — does not call ``cache.info()`` itself, so the caller can fetch
    once and reuse the same payload for both metadata display and raw JSON dump.
    """
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
        "server_rows": [],
        "keyspace": None,
        "memory_rows": [],
        "clients_rows": [],
        "stats_rows": [],
    }

    if not raw_info:
        return base_info

    # Wrappers (LocMem/Database/Stream) return already-structured sections
    # under the "server"/"memory"/"clients"/"stats" keys; pre-structured
    # backends short-circuit the flat-INFO parse.
    def _section(name: str) -> Mapping[str, Any]:
        sub = raw_info.get(name)
        return sub if isinstance(sub, dict) else raw_info

    try:
        base_info["server_rows"] = _server_rows(_section("server"))
        base_info["memory_rows"] = _memory_rows(_section("memory"))
        base_info["clients_rows"] = _clients_rows(_section("clients"))
        base_info["stats_rows"] = _stats_rows(_section("stats"))

        # Keyspace stays nested (per-db cards in the template).
        if isinstance(raw_info.get("keyspace"), dict):
            base_info["keyspace"] = raw_info["keyspace"]
        else:
            ks = {k: v for k, v in raw_info.items() if k.startswith("db") and isinstance(v, dict)}
            if ks:
                base_info["keyspace"] = ks

    except Exception:  # noqa: BLE001, S110
        pass

    return base_info


PAGE_SIZE = 100


def _paginate(total: int, page: int) -> dict[str, Any]:
    """Compute pagination metadata."""
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * PAGE_SIZE
    end = min(start + PAGE_SIZE, total)
    return {
        "page": page,
        "page_size": PAGE_SIZE,
        "total": total,
        "total_pages": total_pages,
        "has_previous": page > 1,
        "has_next": page < total_pages,
        "previous_page": page - 1 if page > 1 else None,
        "next_page": page + 1 if page < total_pages else None,
        "start_index": start,
        "end_index": end,
    }


def get_type_data(
    cache: Any,
    key: str,
    key_type: str | None = None,
    *,
    page: int = 1,
) -> dict[str, Any]:
    """Get type-specific data for a key."""
    try:
        if key_type is None:
            key_type = cache.type(key)
    except NotSupportedError:
        key_type = None

    if not key_type or key_type == KeyType.STRING:
        return {}

    result = _fetch_type_data(cache, key, key_type, page=page)

    # Add SHA1 fingerprints for CAS (compare-and-swap) protection.
    if result and hasattr(cache, "eval_script"):
        _add_cas_fingerprints(cache, key, key_type, result)

    return result


def _format_value(value: Any) -> str:
    """Format a cache value for display — JSON for round-trippable values, repr() otherwise.

    Uses the same approach as format_value_for_display() for string keys:
    JSON-serializes anything that round-trips cleanly, so types are distinguishable
    (e.g. 42 vs "42", null vs "null").
    """
    if value is None:
        return "null"
    try:
        serialized = json.dumps(value, indent=2, ensure_ascii=False)
        if json.loads(serialized) == value:
            return serialized
    except TypeError, ValueError, OverflowError:
        pass
    return repr(value)


def _fetch_type_data(cache: Any, key: str, key_type: str, *, page: int = 1) -> dict[str, Any]:
    """Fetch type-specific data from cache, paginated."""
    try:
        match key_type:
            case KeyType.LIST:
                length = cache.llen(key)
                pagination = _paginate(length, page)
                start = pagination["start_index"]
                stop = pagination["end_index"] - 1  # LRANGE stop is inclusive
                items = [_format_value(i) for i in cache.lrange(key, start, stop)]
                # Pre-build item_entries with offset-aware indices (CAS overwrites with SHA1s)
                item_entries = [(start + i, item, "") for i, item in enumerate(items)]
                return {"items": items, "length": length, "pagination": pagination, "item_entries": item_entries}
            case KeyType.HASH:
                fields = {str(k): _format_value(v) for k, v in cache.hgetall(key).items()}
                length = len(fields)
                pagination = _paginate(length, page)
                s, e = pagination["start_index"], pagination["end_index"]
                sliced = dict(list(fields.items())[s:e])
                return {"fields": sliced, "length": length, "pagination": pagination}
            case KeyType.SET:
                members = sorted(_format_value(m) for m in cache.smembers(key))
                length = len(members)
                pagination = _paginate(length, page)
                s, e = pagination["start_index"], pagination["end_index"]
                return {"members": members[s:e], "length": length, "pagination": pagination}
            case KeyType.ZSET:
                length = cache.zcard(key)
                pagination = _paginate(length, page)
                start = pagination["start_index"]
                stop = pagination["end_index"] - 1  # ZRANGE stop is inclusive
                zset_members = [(_format_value(m), s) for m, s in cache.zrange(key, start, stop, withscores=True)]
                return {"members": zset_members, "length": length, "pagination": pagination}
            case KeyType.STREAM if hasattr(cache, "xrange"):
                length = cache.xlen(key)
                pagination = _paginate(length, page)
                # Fetch up to page*PAGE_SIZE entries and slice to the last page
                entries = cache.xrange(key, count=pagination["end_index"])
                sliced = entries[pagination["start_index"] :]
                return {"entries": sliced, "length": length, "pagination": pagination}
    except Exception:  # noqa: BLE001, S110
        pass
    return {}


def _add_cas_fingerprints(cache: Any, key: str, key_type: str | None, result: dict[str, Any]) -> None:
    """Add SHA1 fingerprints to type data for CAS protection.

    Produces combined list structures usable in Django templates
    (since templates can't do variable-key dict lookups).
    """
    try:
        pagination = result.get("pagination")
        match key_type:
            case KeyType.LIST:
                if pagination:
                    from django_cachex.admin.cas import get_list_sha1s_range

                    start = pagination["start_index"]
                    stop = pagination["end_index"] - 1  # inclusive for LRANGE
                    list_sha1s = get_list_sha1s_range(cache, key, start, stop)
                else:
                    from django_cachex.admin.cas import get_list_sha1s

                    list_sha1s = get_list_sha1s(cache, key)
                items = result.get("items", [])
                offset = pagination["start_index"] if pagination else 0
                result["item_entries"] = [
                    (offset + i, item, list_sha1s[i] if i < len(list_sha1s) else "") for i, item in enumerate(items)
                ]
            case KeyType.HASH:
                fields = result.get("fields", {})
                if pagination and fields:
                    from django_cachex.admin.cas import get_hash_field_sha1s_for

                    hash_sha1s = get_hash_field_sha1s_for(cache, key, list(fields.keys()))
                else:
                    from django_cachex.admin.cas import get_hash_field_sha1s

                    hash_sha1s = get_hash_field_sha1s(cache, key)
                result["field_entries"] = [(field, value, hash_sha1s.get(field, "")) for field, value in fields.items()]
    except Exception:  # noqa: BLE001, S110
        pass


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
        except NotSupportedError, AttributeError:
            pass
        # Fallback: compute Python object size (e.g. LocMemCache). Decode
        # failures for stale data must not break the size column — return None
        # so the row still renders and the user can delete the broken key.
        from django_cachex.exceptions import CompressorError, SerializerError

        try:
            value = cache.get(key)
        except CompressorError, SerializerError:
            return None
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
        dur = entry.get("duration", 0) or 0
        return {
            "id": entry.get("id"),
            "timestamp": datetime.fromtimestamp(ts, tz=UTC) if ts else None,
            "duration_us": dur,
            "duration_ms": dur / 1000,
            "duration_s": dur / 1_000_000,
            "command": entry.get("command", []),
            "client": entry.get("client_address"),
            "client_name": entry.get("client_name"),
        }
    if isinstance(entry, (list, tuple)) and len(entry) >= 4:
        ts = entry[1]
        dur = entry[2] or 0
        return {
            "id": entry[0],
            "timestamp": datetime.fromtimestamp(ts, tz=UTC) if ts else None,
            "duration_us": dur,
            "duration_ms": dur / 1000,
            "duration_s": dur / 1_000_000,
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
    if hasattr(cache, "_cache") and hasattr(cache.adapter, "slowlog_get"):
        try:
            result["length"] = cache.adapter.slowlog_len()
            raw_entries = cache.adapter.slowlog_get(count)
            result["entries"] = [_parse_slowlog_entry(entry) for entry in raw_entries]
            return result
        except Exception as e:  # noqa: BLE001
            result["error"] = str(e)
            return result

    result["error"] = "Slow log not available for this backend."
    return result
