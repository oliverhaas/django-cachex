"""Helper functions for cache admin views."""

import contextlib
import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.core.cache import caches
from django.utils.translation import gettext_lazy as _

from django_cachex.admin.cas import get_hash_field_sha1s_for, get_list_sha1s_range, supports_cas
from django_cachex.exceptions import CompressorError, NotSupportedError, SerializerError
from django_cachex.types import KeyType
from django_cachex.utils import _deep_getsizeof

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Mapping


class CacheUnavailableError(Exception):
    """The alias is missing from ``CACHES`` or its backend cannot be built."""


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
    """Get a cache backend for admin use.

    Raises:
        CacheUnavailableError: the alias is missing from ``CACHES`` or its
            backend cannot be built. Callers turn this into a message.
    """
    cache_config = settings.CACHES.get(cache_name)
    if not cache_config:
        msg = f"Cache '{cache_name}' is not configured in CACHES setting."
        raise CacheUnavailableError(msg)
    try:
        return caches[cache_name]
    except Exception as exc:
        msg = f"Cache '{cache_name}' could not be loaded: {exc}"
        raise CacheUnavailableError(msg) from exc


def parse_metadata(
    cache: Any,
    cache_config: Mapping[str, Any],
    raw_info: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build structured cache metadata from a pre-fetched ``cache.info()`` result.

    Pure parsing; does not call ``cache.info()`` itself, so the caller can fetch
    once and reuse the same payload for both metadata display and raw JSON dump.
    """
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

    except Exception:
        logger.exception("parse_metadata: failed to extract section data")

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


# Types with a browsable structure. Strings are rendered by the value editor
# and anything else, ``KeyType.UNKNOWN`` included, is opaque to the admin.
CONTAINER_TYPES = frozenset(
    {KeyType.LIST, KeyType.SET, KeyType.HASH, KeyType.ZSET, KeyType.STREAM},
)

# Types the key detail page knows how to render. A key of any other type, and a
# key an adapter reports as ``KeyType.UNKNOWN``, is shown read-only.
RENDERABLE_TYPES = CONTAINER_TYPES | {KeyType.STRING}

# Types the admin can create. ``KeyType.UNKNOWN`` describes a key the package
# does not model, so it is never something a user asks the admin to write.
CREATABLE_TYPES = tuple(t for t in KeyType if t is not KeyType.UNKNOWN)


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

    if key_type not in CONTAINER_TYPES:
        return {}

    result = _fetch_type_data(cache, key, key_type, page=page)

    # CAS fingerprints are hashed server-side, so backends without scripting
    # (stock Django, LocMem, Database) get no conflict detection.
    if result and supports_cas(cache):
        _add_cas_fingerprints(cache, key, key_type, result)

    return result


def is_json_serializable(value: Any) -> bool:
    """Check if a value can be safely round-tripped through JSON without loss."""
    try:
        serialized = json.dumps(value)
        deserialized = json.loads(serialized)
        return deserialized == value
    except TypeError, ValueError, OverflowError:
        return False


def format_value_for_display(value: Any) -> tuple[str, bool]:
    """Format a value for display in the admin UI, returning (display_string, is_editable)."""
    if value is None:
        return "null", True

    if is_json_serializable(value):
        return json.dumps(value, indent=2, ensure_ascii=False), True
    return repr(value), False


def _format_entry(value: Any) -> tuple[str, bool]:
    """Format one container entry as (display_string, editable).

    Shares ``format_value_for_display`` with the string editor so containers and
    strings agree on what is round-trippable. Entries that fall back to repr()
    are marked non-editable: submitting the repr back would store the repr text.
    """
    return format_value_for_display(value)


def parse_json_or_str(value: str) -> Any:
    """Try to interpret a string as JSON, falling back to the raw string."""
    with contextlib.suppress(json.JSONDecodeError, ValueError):
        return json.loads(value)
    return value


def is_hashable(value: Any) -> bool:
    """Report whether ``value`` can be used as a dict key."""
    try:
        hash(value)
    except TypeError:
        return False
    return True


def _zset_rows(entries: Any) -> list[tuple[str, float, bool]]:
    """Format ``(member, score)`` pairs as display rows."""
    rows = []
    for raw, score in entries:
        member, editable = _format_entry(raw)
        # ZADD takes members as dict keys, so a member that parses back as an
        # array or object can be shown but never written.
        rows.append((member, score, editable and is_hashable(parse_json_or_str(member))))
    return rows


def _fetch_stream_data(cache: Any, key: str, *, page: int) -> dict[str, Any]:
    """Fetch one page of stream entries."""
    if not hasattr(cache, "xrange"):
        # Falling through would return {} and render "Stream is empty",
        # which is a different claim entirely.
        return {"error": "Stream browsing is not supported by this cache backend."}
    length = cache.xlen(key)
    pagination = _paginate(length, page)
    if not length:
        # XRANGE rejects COUNT 0, which is what an empty stream asks for once
        # its last entry has been deleted.
        return {"entries": [], "length": 0, "pagination": pagination}
    # Fetch up to page*PAGE_SIZE entries and slice to the last page
    entries = cache.xrange(key, count=pagination["end_index"])
    return {"entries": entries[pagination["start_index"] :], "length": length, "pagination": pagination}


def _fetch_type_data(cache: Any, key: str, key_type: str, *, page: int = 1) -> dict[str, Any]:  # noqa: PLR0911
    """Fetch type-specific data from cache, paginated."""
    try:
        match key_type:
            case KeyType.LIST:
                length = cache.llen(key)
                pagination = _paginate(length, page)
                start = pagination["start_index"]
                stop = pagination["end_index"] - 1  # LRANGE stop is inclusive
                # Every entry carries an empty SHA1 slot that CAS fills in later, so
                # the template sees the same tuple shape on backends without scripting.
                item_entries = []
                for i, raw in enumerate(cache.lrange(key, start, stop)):
                    item, editable = _format_entry(raw)
                    item_entries.append((start + i, item, "", editable))
                return {"length": length, "pagination": pagination, "item_entries": item_entries}
            case KeyType.HASH:
                fields = {str(k): v for k, v in cache.hgetall(key).items()}
                length = len(fields)
                pagination = _paginate(length, page)
                s, e = pagination["start_index"], pagination["end_index"]
                field_entries = []
                for field, raw in list(fields.items())[s:e]:
                    value, editable = _format_entry(raw)
                    field_entries.append((field, value, "", editable))
                return {"length": length, "pagination": pagination, "field_entries": field_entries}
            case KeyType.SET:
                members = sorted(_format_entry(m) for m in cache.smembers(key))
                length = len(members)
                pagination = _paginate(length, page)
                s, e = pagination["start_index"], pagination["end_index"]
                return {"members": members[s:e], "length": length, "pagination": pagination}
            case KeyType.ZSET:
                length = cache.zcard(key)
                pagination = _paginate(length, page)
                start = pagination["start_index"]
                stop = pagination["end_index"] - 1  # ZRANGE stop is inclusive
                zset_members = _zset_rows(cache.zrange(key, start, stop, withscores=True))
                return {"members": zset_members, "length": length, "pagination": pagination}
            case KeyType.STREAM:
                return _fetch_stream_data(cache, key, page=page)
    except Exception as e:
        logger.exception("Failed to fetch type-specific admin data for key %r", key)
        return {"error": str(e)}
    return {}


def _add_cas_fingerprints(cache: Any, key: str, key_type: str | None, result: dict[str, Any]) -> None:
    """Add SHA1 fingerprints to type data for CAS protection.

    Produces combined list structures usable in Django templates
    (since templates can't do variable-key dict lookups).
    """
    try:
        # ``_fetch_type_data`` always paginates lists and hashes, so the range
        # readers are the only ones needed.
        pagination = result["pagination"]
        match key_type:
            case KeyType.LIST:
                start = pagination["start_index"]
                stop = pagination["end_index"] - 1  # inclusive for LRANGE
                list_sha1s = get_list_sha1s_range(cache, key, start, stop)
                result["item_entries"] = [
                    (index, item, list_sha1s[i] if i < len(list_sha1s) else "", editable)
                    for i, (index, item, _, editable) in enumerate(result.get("item_entries", []))
                ]
            case KeyType.HASH:
                entries = result.get("field_entries", [])
                hash_sha1s = get_hash_field_sha1s_for(cache, key, [field for field, *_ in entries])
                result["field_entries"] = [
                    (field, value, hash_sha1s.get(field, ""), editable) for field, value, _, editable in entries
                ]
    except Exception:
        # CAS protection is best-effort. Mirror the warning emitted by
        # ``_key_detail_view`` (key_detail.py) so the operator knows the
        # next update will skip conflict detection.
        logger.warning(
            "CAS fingerprint collection failed for key %r (type=%s); edits will skip conflict checks",
            key,
            key_type,
            exc_info=True,
        )


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
        # STRLEN over the raw client measures the stored bytes. Stock Django
        # backends have no get_client; BaseCachex declares it and raises.
        if hasattr(cache, "get_client"):
            try:
                client = cache.get_client(write=False)
                return client.strlen(cache.make_key(key))
            except NotSupportedError:
                pass
        # Fallback: compute Python object size (e.g. LocMemCache). Decode
        # failures for stale data must not break the size column, return None
        # so the row still renders and the user can delete the broken key.
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
    except Exception:
        logger.exception("get_size: size lookup failed for key %r (type=%s)", key, key_type)
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
            "command": entry[3],
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

    result["error"] = "Slow log not available for this backend."
    return result
