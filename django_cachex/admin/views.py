"""
Views for the django-cachex cache admin.

Provides cache inspection and management functionality.
These views can be configured with different template prefixes and URL builders
to support both the standard Django admin and alternative admin themes like Unfold.
"""

from __future__ import annotations

import contextlib
import json
import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.safestring import mark_safe

from .models import Cache, Key
from .service import get_cache_service

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


# =============================================================================
# URL Helpers
# =============================================================================


def cache_list_url() -> str:
    """URL for the cache list page."""
    return reverse("admin:django_cachex_cache_changelist")


def key_list_url(cache_name: str) -> str:
    """URL for the key list page."""
    return reverse("admin:django_cachex_key_changelist") + f"?cache={cache_name}"


def key_detail_url(cache_name: str, key: str) -> str:
    """URL for the key detail page."""
    pk = Key.make_pk(cache_name, key)
    return reverse("admin:django_cachex_key_change", args=[pk])


def key_add_url(cache_name: str) -> str:
    """URL for the key add page."""
    return reverse("admin:django_cachex_key_add") + f"?cache={cache_name}"


# =============================================================================
# View Configuration
# =============================================================================


class ViewConfig:
    """Configuration for cache admin views.

    Holds template prefix configuration to support both Django admin
    and alternative admin themes (like Unfold).

    Args:
        template_prefix: Base path for templates (e.g., "admin/django_cachex")
        template_overrides: Dict mapping canonical names to actual template paths.
            Use this when template names differ between themes.
    """

    def __init__(
        self,
        template_prefix: str = "admin/django_cachex",
        template_overrides: dict[str, str] | None = None,
    ):
        self.template_prefix = template_prefix.rstrip("/")
        self.template_overrides = template_overrides or {}

    def template(self, name: str) -> str:
        """Get the full template path for a template name.

        Checks template_overrides first, then falls back to prefix + name.
        """
        if name in self.template_overrides:
            return f"{self.template_prefix}/{self.template_overrides[name]}"
        return f"{self.template_prefix}/{name}"


# Default configuration for standard Django admin
ADMIN_CONFIG = ViewConfig(template_prefix="admin/django_cachex")


logger = logging.getLogger(__name__)


def _is_json_serializable(value: Any) -> bool:
    """Check if a value can be safely serialized to JSON and back without loss.

    This performs a round-trip check to ensure the value can be serialized
    to JSON and deserialized back to an equivalent Python object.

    Returns True for: None, bool, int, float, str, and dicts/lists containing only
    these types. Returns False for: bytes, datetime, custom objects, or any value
    where the round-trip changes the data.

    Args:
        value: The Python value to check.

    Returns:
        True if the value can round-trip through JSON without information loss.
    """
    try:
        serialized = json.dumps(value)
        deserialized = json.loads(serialized)
        return deserialized == value
    except (TypeError, ValueError, OverflowError):
        return False


def _format_value_for_display(value: Any) -> tuple[str, bool]:
    """Format a value for display in the admin UI.

    Args:
        value: The Python value to format.

    Returns:
        A tuple of (display_string, is_editable).
        - JSON-serializable values are displayed as formatted JSON and are editable.
        - Non-JSON-serializable values are displayed using repr() and are read-only.
    """
    if value is None:
        return "null", True

    if _is_json_serializable(value):
        return json.dumps(value, indent=2, ensure_ascii=False), True
    return repr(value), False


# Help messages for each view (HTML content wrapped with mark_safe)
HELP_MESSAGES = {
    "index": mark_safe(
        "<strong>Cache Instances</strong><br>"
        "View all cache backends configured in your Django settings.<br><br>"
        "<strong>Support Levels</strong><br>"
        "<table style='margin: 4px 0 12px 0; border-collapse: collapse;'>"
        "<tr><td style='padding: 2px 8px;'><strong>cachex</strong></td>"
        "<td style='padding: 2px 8px;'>Full support - All features including key browsing, "
        "type detection, TTL management, and data operations</td></tr>"
        "<tr><td style='padding: 2px 8px;'><strong>wrapped</strong></td>"
        "<td style='padding: 2px 8px;'>Wrapped support - Django built-in backends "
        "(LocMem, Database, File) with some features</td></tr>"
        "<tr><td style='padding: 2px 8px;'><strong>limited</strong></td>"
        "<td style='padding: 2px 8px;'>Limited support - Custom/unknown backends, "
        "basic get/set only</td></tr>"
        "</table>"
        "<strong>Actions</strong><br>"
        "- Click a cache name to browse its keys<br>"
        "- Select caches and use 'Flush selected caches' to clear them<br>"
        "- Use the filter sidebar to filter by support level",
    ),
    "key_search": mark_safe(
        "<strong>Key Browser</strong><br>"
        "Search and manage cache keys for this backend.<br><br>"
        "<strong>Search Patterns</strong><br>"
        "Search combines Django-style convenience with Redis/Valkey glob patterns.<br>"
        "- <code>session</code> - Keys containing 'session' (auto-wrapped as <code>*session*</code>)<br>"
        "- <code>prefix:*</code> - Keys starting with 'prefix:'<br>"
        "- <code>*:suffix</code> - Keys ending with ':suffix'<br>"
        "- <code>*</code> - List all keys (default when empty)<br><br>"
        "<strong>Table Columns</strong><br>"
        "- <strong>Key</strong> - Click to view/edit the key<br>"
        "- <strong>Type</strong> - Data type (string, list, set, hash, zset, stream)<br>"
        "- <strong>TTL</strong> - Seconds until expiration<br>"
        "- <strong>Size</strong> - Length for collections, bytes for strings<br><br>"
        "<strong>Actions</strong><br>"
        "- Use 'Add key' to create new entries<br>"
        "- Select keys and use 'Delete selected' to remove them",
    ),
    "key_detail_string": mark_safe(
        "<strong>String Key</strong><br>"
        "View and edit this string value stored in cache.<br><br>"
        "<strong>Value Format</strong><br>"
        "Values are displayed and edited as JSON. Strings appear quoted, "
        "objects as <code>{...}</code>, arrays as <code>[...]</code>.<br><br>"
        "<strong>Operations</strong><br>"
        "- Edit the value in the textarea and click <strong>Update</strong><br>"
        "- Set TTL to control expiration (empty = no expiry)<br>"
        "- Use <strong>Delete</strong> to remove this key",
    ),
    "key_detail_list": mark_safe(
        "<strong>List Key</strong><br>"
        "View and modify this Redis list (ordered collection).<br><br>"
        "<strong>Operations</strong><br>"
        "- <strong>Push Left/Right</strong> - Add items to the head or tail<br>"
        "- <strong>Pop Left/Right</strong> - Remove and return items from head or tail<br>"
        "- <strong>Trim</strong> - Keep only items in the specified index range<br>"
        "- <strong>Remove</strong> - Delete specific items from the list<br><br>"
        "<strong>Index</strong><br>"
        "Items are shown with their 0-based index. Index 0 is the head (left).",
    ),
    "key_detail_set": mark_safe(
        "<strong>Set Key</strong><br>"
        "View and modify this Redis set (unordered unique members).<br><br>"
        "<strong>Operations</strong><br>"
        "- <strong>Add</strong> - Add a new member to the set<br>"
        "- <strong>Pop</strong> - Remove and return random member(s)<br>"
        "- <strong>Remove</strong> - Delete a specific member<br><br>"
        "<strong>Note</strong><br>"
        "Sets do not allow duplicate members. Adding an existing member has no effect.",
    ),
    "key_detail_hash": mark_safe(
        "<strong>Hash Key</strong><br>"
        "View and modify this Redis hash (field-value mapping).<br><br>"
        "<strong>Operations</strong><br>"
        "- <strong>Set Field</strong> - Add a new field or update existing<br>"
        "- <strong>Update</strong> - Modify a field's value inline<br>"
        "- <strong>Delete</strong> - Remove a field from the hash<br><br>"
        "<strong>Note</strong><br>"
        "Field names must be unique. Setting an existing field overwrites its value.",
    ),
    "key_detail_zset": mark_safe(
        "<strong>Sorted Set Key</strong><br>"
        "View and modify this Redis sorted set (members ordered by score).<br><br>"
        "<strong>Operations</strong><br>"
        "- <strong>Add</strong> - Add member with score. Flags: "
        "NX (only if new), XX (only if exists), GT (if score greater), LT (if score less)<br>"
        "- <strong>Pop Min/Max</strong> - Remove member(s) with lowest/highest score<br>"
        "- <strong>Remove</strong> - Delete a specific member<br><br>"
        "<strong>Ordering</strong><br>"
        "Members are displayed by score (lowest first). Rank is the 0-based position.",
    ),
    "key_detail_stream": mark_safe(
        "<strong>Stream Key</strong><br>"
        "View and modify this Redis stream (append-only log).<br><br>"
        "<strong>Operations</strong><br>"
        "- <strong>Add Entry</strong> - Append a new entry with field-value data<br>"
        "- <strong>Trim</strong> - Limit stream to a maximum number of entries<br>"
        "- <strong>Delete</strong> - Remove a specific entry by ID<br><br>"
        "<strong>Entry IDs</strong><br>"
        "Each entry has a unique ID in the format <code>timestamp-sequence</code>. "
        "IDs are auto-generated when adding entries.",
    ),
    "key_detail": mark_safe(
        "<strong>Key Details</strong><br>"
        "View and modify this cache key.<br><br>"
        "<strong>TTL</strong><br>"
        "Time-to-live in seconds. Leave empty for no expiry.",
    ),
    "key_add": mark_safe(
        "<strong>Add Key</strong><br>"
        "Create a new cache entry with a key name, value, and optional timeout.<br><br>"
        "<strong>Key Name</strong><br>"
        "Enter a unique identifier. Common patterns: <code>user:123</code>, "
        "<code>session:abc</code>, <code>cache:page:home</code><br><br>"
        "<strong>Value Format</strong><br>"
        "- Plain text - Stored as a string<br>"
        '- <code>{"key": "value"}</code> - JSON objects are parsed and stored<br>'
        "- <code>[1, 2, 3]</code> - JSON arrays are parsed and stored<br><br>"
        "<strong>Timeout</strong><br>"
        "- Leave empty - Uses the cache's default timeout<br>"
        "- Enter seconds - Key expires after this duration<br>"
        "- Enter 0 - Key never expires",
    ),
    "cache_info": mark_safe(
        "<strong>Cache Server Info</strong><br>"
        "Detailed information about the Redis/Valkey server.<br><br>"
        "<strong>Sections</strong><br>"
        "- <strong>Configuration</strong> - Backend, location, key prefix, version<br>"
        "- <strong>Server</strong> - Redis/Valkey version, OS, uptime, process ID<br>"
        "- <strong>Memory</strong> - Used/peak/max memory, eviction policy<br>"
        "- <strong>Clients</strong> - Connected and blocked client counts<br>"
        "- <strong>Statistics</strong> - Ops/sec, cache hits/misses, expired/evicted keys<br>"
        "- <strong>Keyspace</strong> - Key counts and TTL stats per database",
    ),
    "cache_slowlog": mark_safe(
        "<strong>Slow Query Log</strong><br>"
        "Commands that exceeded the server's slowlog threshold.<br><br>"
        "<strong>Columns</strong><br>"
        "- <strong>ID</strong> - Unique log entry identifier<br>"
        "- <strong>Time</strong> - When the command was executed<br>"
        "- <strong>Duration</strong> - Execution time in microseconds<br>"
        "- <strong>Command</strong> - The slow command with arguments<br>"
        "- <strong>Client</strong> - Client address and name<br><br>"
        "<strong>Configuration</strong><br>"
        "The threshold is set via Redis <code>slowlog-log-slower-than</code> config. "
        "Use the dropdown to show 10-100 entries.",
    ),
}


def _show_help(request: HttpRequest, view_name: str) -> bool:
    """Check if help was requested and show message if so. Returns True if help shown."""
    if request.GET.get("help"):
        help_text = HELP_MESSAGES.get(view_name, "")
        if help_text:
            messages.info(request, help_text)
        return True
    return False


# Django core builtin backends that are wrapped for nearly-full support
_DJANGO_BUILTINS = {
    "django.core.cache.backends.locmem.LocMemCache",
    "django.core.cache.backends.db.DatabaseCache",
    "django.core.cache.backends.filebased.FileBasedCache",
    "django.core.cache.backends.dummy.DummyCache",
    "django.core.cache.backends.memcached.PyMemcacheCache",
    "django.core.cache.backends.memcached.PyLibMCCache",
    "django.core.cache.backends.memcached.MemcachedCache",
}


def _get_support_level(backend: str) -> str:
    """Determine the support level for a cache backend.

    Returns:
        - "cachex": Full support (django-cachex backends)
        - "wrapped": Django core builtin backends (wrapped for almost full support)
        - "limited": Custom/unknown backends with limited support
    """
    if backend.startswith("django_cachex."):
        return "cachex"
    if backend in _DJANGO_BUILTINS:
        return "wrapped"
    return "limited"


def _parse_timeout(timeout_str: str) -> tuple[float | None, str | None]:
    """Parse timeout string and return (timeout, error_message)."""
    timeout_str = timeout_str.strip()
    if not timeout_str:
        return None, None
    try:
        timeout = float(timeout_str)
        if timeout < 0:
            return None, "Timeout must be non-negative"
        return timeout, None
    except ValueError:
        return None, f"Invalid timeout value: {timeout_str}"


def _get_page_range(
    current_page: int,
    total_pages: int,
    window: int = 2,
) -> list[int | str]:
    """Generate a page range for pagination display."""
    if total_pages <= 7:
        return list(range(1, total_pages + 1))

    pages: list[int | str] = []
    pages.append(1)

    start = max(2, current_page - window)
    end = min(total_pages - 1, current_page + window)

    if start > 2:
        pages.append("...")

    pages.extend(range(start, end + 1))

    if end < total_pages - 1:
        pages.append("...")

    if total_pages > 1:
        pages.append(total_pages)

    return pages


def _index_view(request: HttpRequest, config: ViewConfig) -> HttpResponse:
    """Display all configured cache instances with their capabilities.

    This is the internal implementation; use index() for the decorated admin view.
    """
    # Show help message if requested
    help_active = _show_help(request, "index")

    # Handle POST requests (flush cache action)
    if request.method == "POST":
        action = request.POST.get("action")
        selected_caches = request.POST.getlist("_selected_action")

        if action == "flush_selected" and selected_caches:
            flushed_count = 0
            for cache_name in selected_caches:
                try:
                    service = get_cache_service(cache_name)
                    service.clear()
                    flushed_count += 1
                except Exception as e:  # noqa: BLE001
                    messages.error(request, f"Error flushing '{cache_name}': {e!s}")
            if flushed_count > 0:
                messages.success(
                    request,
                    f"Successfully flushed {flushed_count} cache(s).",
                )
            return redirect(cache_list_url())

    # Get filter parameters
    support_filter = request.GET.get("support", "").strip()
    search_query = request.GET.get("q", "").strip().lower()

    caches_info: list[dict[str, Any]] = []
    any_flush_supported = False
    for cache_name, cache_config in settings.CACHES.items():
        cache_obj = Cache.get_by_name(cache_name)
        backend = str(cache_config.get("BACKEND", "Unknown"))
        support_level = cache_obj.support_level if cache_obj else "limited"
        any_flush_supported = True
        try:
            get_cache_service(cache_name)  # Verify cache is accessible
            cache_info = {
                "name": cache_name,
                "config": cache_config,
                "backend": backend,
                "backend_short": backend.rsplit(".", 1)[-1] if "." in backend else backend,
                "location": cache_config.get("LOCATION", ""),
                "support_level": support_level,
            }
            caches_info.append(cache_info)
        except Exception as e:  # noqa: BLE001
            cache_info = {
                "name": cache_name,
                "config": cache_config,
                "backend": backend,
                "backend_short": backend.rsplit(".", 1)[-1] if "." in backend else backend,
                "location": cache_config.get("LOCATION", ""),
                "support_level": support_level,
                "error": str(e),
            }
            caches_info.append(cache_info)

    # Apply support filter
    if support_filter:
        caches_info = [c for c in caches_info if c["support_level"] == support_filter]

    # Apply search filter
    if search_query:
        caches_info = [
            c
            for c in caches_info
            if search_query in c["name"].lower()
            or search_query in c["backend"].lower()
            or search_query in str(c.get("location", "")).lower()
        ]

    context = admin.site.each_context(request)
    context.update(
        {
            "caches_info": caches_info,
            "has_caches_configured": bool(settings.CACHES),
            "title": "Cache Admin - Instances",
            "support_filter": support_filter,
            "search_query": search_query,
            "any_flush_supported": any_flush_supported,
            "help_active": help_active,
        },
    )
    return render(request, config.template("cache/index.html"), context)


@staff_member_required
def index(request: HttpRequest) -> HttpResponse:
    """Display all configured cache instances with their capabilities."""
    return _index_view(request, ADMIN_CONFIG)


def _cache_detail_view(
    request: HttpRequest,
    cache_name: str,
    config: ViewConfig,
) -> HttpResponse:
    """Display cache details (info + slowlog combined).

    This is the internal implementation; use cache_detail() for the decorated admin view.
    """
    cache_obj = Cache.get_by_name(cache_name)

    if cache_obj is None:
        messages.error(request, f"Cache '{cache_name}' not found.")
        return redirect(cache_list_url())

    # Show help message if requested
    help_active = _show_help(request, "cache_info")

    service = get_cache_service(cache_name)

    # Get cache metadata and info
    info_data = None
    raw_info = None
    try:
        info_data = service.metadata()
        raw_info = service.info()
    except Exception as e:  # noqa: BLE001
        messages.error(request, f"Error retrieving cache info: {e!s}")

    # Get slowlog count from query param (default 10)
    slowlog_count = int(request.GET.get("count", 10))

    # Get slowlog entries
    slowlog_data = None
    try:
        slowlog_data = service.slowlog_get(slowlog_count)
    except Exception as e:  # noqa: BLE001
        messages.error(request, f"Error retrieving slow log: {e!s}")

    # Convert raw_info to pretty-printed JSON for display
    raw_info_json = None
    if raw_info:
        raw_info_json = json.dumps(raw_info, indent=2, default=str)

    context = admin.site.each_context(request)
    context.update(
        {
            "title": f"Cache: {cache_name}",
            "cache_name": cache_name,
            "cache_obj": cache_obj,
            "info_data": info_data,
            "raw_info_json": raw_info_json,
            "slowlog_data": slowlog_data,
            "slowlog_count": slowlog_count,
            "help_active": help_active,
        },
    )
    return render(request, config.template("cache/change_form.html"), context)


@staff_member_required
def cache_detail(request: HttpRequest, cache_name: str) -> HttpResponse:
    """Display cache details (info + slowlog combined)."""
    return _cache_detail_view(request, cache_name, ADMIN_CONFIG)


def _help_view(request: HttpRequest, config: ViewConfig) -> HttpResponse:
    """Display help information about the cache admin.

    This is the internal implementation; use help_view() for the decorated admin view.
    """
    context = admin.site.each_context(request)
    context.update(
        {
            "title": "Cache Admin - Help",
        },
    )
    return render(request, config.template("cache/help.html"), context)


@staff_member_required
def help_view(request: HttpRequest) -> HttpResponse:
    """Display help information about the cache admin."""
    return _help_view(request, ADMIN_CONFIG)


def _key_search_view(  # noqa: C901, PLR0912, PLR0915
    request: HttpRequest,
    cache_name: str,
    config: ViewConfig,
) -> HttpResponse:
    """View for searching/browsing cache keys.

    This is the internal implementation; use key_search() for the decorated admin view.
    """
    # Show help message if requested
    help_active = _show_help(request, "key_search")

    service = get_cache_service(cache_name)
    cache_config = settings.CACHES.get(cache_name, {})

    # Handle POST requests (bulk delete)
    if request.method == "POST":
        action = request.POST.get("action")

        if action == "delete_selected":
            selected_keys = request.POST.getlist("_selected_action")
            if selected_keys:
                deleted_count = 0
                for key in selected_keys:
                    with contextlib.suppress(Exception):
                        service.delete(key)
                        deleted_count += 1
                if deleted_count > 0:
                    messages.success(
                        request,
                        f"Successfully deleted {deleted_count} key(s).",
                    )
            return redirect(key_list_url(cache_name))

    context = admin.site.each_context(request)
    context.update(
        {
            "title": f"Keys in '{cache_name}'",
            "cache_name": cache_name,
            "cache_config": cache_config,
            "help_active": help_active,
        },
    )

    search_query = request.GET.get("q", "").strip()
    count = int(request.GET.get("count", 100))
    cursor = int(request.GET.get("cursor", 0))

    context["search_query"] = search_query
    context["count"] = count
    context["cursor"] = cursor

    # Handle pattern search (auto-wrap in wildcards for Django-style contains search)
    if search_query:
        # Auto-wrap in wildcards if none present (Django-style contains search)
        if "*" not in search_query and "?" not in search_query:
            pattern = f"*{search_query}*"
        else:
            pattern = search_query
    else:
        pattern = "*"
    try:
        query_result = service.keys(
            pattern=pattern,
            cursor=cursor,
            count=count,
        )

        keys = query_result["keys"]
        next_cursor = query_result["next_cursor"]
        total_count = query_result.get("total_count")  # May be None for SCAN
        error = query_result.get("error")

        if error:
            context["error"] = error

        keys_data = []
        for key_item in keys:
            if isinstance(key_item, dict):
                user_key = key_item["key"]
                redis_key = key_item.get("redis_key")
            else:
                user_key = key_item
                redis_key = None

            key_entry: dict[str, Any] = {
                "key": user_key,
                "pk": Key.make_pk(cache_name, user_key),
            }
            if redis_key:
                key_entry["redis_key"] = redis_key
            keys_data.append(key_entry)

        # Fetch TTL, type, and size for displayed keys
        for key_entry in keys_data:
            user_key = key_entry["key"]
            key_type = None
            with contextlib.suppress(Exception):
                ttl = service.ttl(user_key)
                key_entry["ttl"] = ttl
                if ttl is not None and ttl >= 0:
                    key_entry["ttl_expires_at"] = timezone.now() + timedelta(seconds=ttl)
            with contextlib.suppress(Exception):
                key_type = service.type(user_key)
                key_entry["type"] = key_type
            with contextlib.suppress(Exception):
                key_entry["size"] = service.size(user_key, key_type)

        context["keys_data"] = keys_data
        context["total_keys"] = total_count  # May be None
        context["keys_count"] = len(keys_data)

        # Cursor-based pagination
        context["next_cursor"] = next_cursor
        context["has_next"] = next_cursor != 0
        context["has_previous"] = cursor > 0

    except Exception:
        logger.exception("Error querying cache '%s'", cache_name)
        context["error_message"] = "An error occurred while querying the cache."

    return render(request, config.template("cache/key_search.html"), context)


@staff_member_required
def key_search(request: HttpRequest, cache_name: str) -> HttpResponse:
    """View for searching/browsing cache keys."""
    return _key_search_view(request, cache_name, ADMIN_CONFIG)


def _key_detail_view(  # noqa: C901, PLR0911, PLR0912, PLR0915
    request: HttpRequest,
    cache_name: str,
    key: str,
    config: ViewConfig,
) -> HttpResponse:
    """View for displaying the details of a specific cache key.

    This is the internal implementation; use key_detail() for the decorated admin view.
    """
    service = get_cache_service(cache_name)

    # Handle POST requests (update or delete)
    if request.method == "POST":
        action = request.POST.get("action")

        if action == "delete":
            try:
                service.delete(key)
                messages.success(request, "Key deleted successfully.")
                return redirect(key_list_url(cache_name))
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error deleting key: {e!s}")

        elif action == "update":
            try:
                new_value: Any = request.POST.get("value", "")
                with contextlib.suppress(json.JSONDecodeError, ValueError):
                    new_value = json.loads(new_value)

                # Update value only (TTL is handled separately via set_ttl action)
                service.set(key, new_value, timeout=None)
                messages.success(request, "Key updated successfully.")
                return redirect(key_detail_url(cache_name, key))
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))

        elif action == "set_ttl":
            try:
                ttl_str = request.POST.get("ttl_value", "").strip()
                if not ttl_str or ttl_str == "0":
                    # Empty or 0 TTL = persist (no expiry)
                    if service.persist(key):
                        messages.success(request, "TTL removed. Key will not expire.")
                    else:
                        messages.error(request, "Key does not exist or has no TTL.")
                else:
                    ttl_int = int(ttl_str)
                    if ttl_int < 0:
                        messages.error(request, "TTL must be non-negative.")
                    elif service.expire(key, ttl_int):
                        messages.success(request, f"TTL set to {ttl_int} seconds.")
                    else:
                        messages.error(request, "Key does not exist or TTL could not be set.")
                return redirect(key_detail_url(cache_name, key))
            except ValueError:
                messages.error(request, "Invalid TTL value. Must be a number.")
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error setting TTL: {e!s}")

        elif action == "persist":
            try:
                if service.persist(key):
                    messages.success(request, "TTL removed. Key will not expire.")
                else:
                    messages.error(request, "Key does not exist or has no TTL.")
                return redirect(key_detail_url(cache_name, key))
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error removing TTL: {e!s}")

        # List operations
        elif action == "lpop":
            count = 1
            count_str = request.POST.get("pop_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = max(1, int(count_str))
            try:
                result = service.lpop(key, count=count)
                if not result:
                    messages.error(request, "List is empty or key does not exist.")
                elif len(result) == 1:
                    messages.success(request, f"Popped: {result[0]}")
                else:
                    messages.success(request, f"Popped {len(result)} item(s): {result}")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        elif action == "rpop":
            count = 1
            count_str = request.POST.get("pop_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = max(1, int(count_str))
            try:
                result = service.rpop(key, count=count)
                if not result:
                    messages.error(request, "List is empty or key does not exist.")
                elif len(result) == 1:
                    messages.success(request, f"Popped: {result[0]}")
                else:
                    messages.success(request, f"Popped {len(result)} item(s): {result}")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        elif action == "lpush":
            value = request.POST.get("push_value", "").strip()
            if value:
                try:
                    new_len = service.lpush(key, value)
                    messages.success(request, f"Pushed to left. Length: {new_len}")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Value is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "rpush":
            value = request.POST.get("push_value", "").strip()
            if value:
                try:
                    new_len = service.rpush(key, value)
                    messages.success(request, f"Pushed to right. Length: {new_len}")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Value is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "lrem":
            value = request.POST.get("item_value", "").strip()
            count = 0  # Default: remove all occurrences
            count_str = request.POST.get("lrem_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = int(count_str)
            if value:
                try:
                    removed = service.lrem(key, value, count=count)
                    if removed > 0:
                        messages.success(request, f"Removed {removed} occurrence(s) of '{value}'.")
                    else:
                        messages.warning(request, f"'{value}' not found in list.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Value is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "ltrim":
            try:
                start = int(request.POST.get("trim_start", "0"))
                stop = int(request.POST.get("trim_stop", "-1"))
                service.ltrim(key, start, stop)
                messages.success(request, f"List trimmed to range [{start}:{stop}].")
            except ValueError:
                messages.error(request, "Start and stop must be integers.")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Set operations
        elif action == "sadd":
            member = request.POST.get("member_value", "").strip()
            if member:
                try:
                    added = service.sadd(key, member)
                    if added:
                        messages.success(request, f"Added '{member}' to set.")
                    else:
                        messages.info(request, f"'{member}' already exists in set.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Member is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "srem":
            member = request.POST.get("member", "").strip()
            if member:
                try:
                    removed = service.srem(key, member)
                    if removed:
                        messages.success(request, f"Removed '{member}' from set.")
                    else:
                        messages.warning(request, f"'{member}' not found in set.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Hash operations
        elif action == "hset":
            field = request.POST.get("field_name", "").strip()
            value = request.POST.get("field_value", "").strip()
            if field:
                try:
                    service.hset(key, field, value)
                    messages.success(request, f"Set field '{field}'.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Field name is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "hdel":
            field = request.POST.get("field", "").strip()
            if field:
                try:
                    removed = service.hdel(key, field)
                    if removed:
                        messages.success(request, f"Deleted field '{field}'.")
                    else:
                        messages.warning(request, f"Field '{field}' not found.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Sorted set operations
        elif action == "zadd":
            member = request.POST.get("member_value", "").strip()
            score_str = request.POST.get("score_value", "").strip()
            # Get ZADD flags
            nx = request.POST.get("zadd_nx") == "on"
            xx = request.POST.get("zadd_xx") == "on"
            gt = request.POST.get("zadd_gt") == "on"
            lt = request.POST.get("zadd_lt") == "on"
            if member and score_str:
                try:
                    score = float(score_str)
                    added = service.zadd(key, {member: score}, nx=nx, xx=xx, gt=gt, lt=lt)
                    if added:
                        messages.success(request, f"Added '{member}' with score {score}.")
                    else:
                        messages.success(request, f"Updated '{member}' score to {score}.")
                except ValueError:
                    messages.error(request, "Score must be a number.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Member and score are required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "zrem":
            member = request.POST.get("member", "").strip()
            if member:
                try:
                    removed = service.zrem(key, member)
                    if removed:
                        messages.success(request, f"Removed '{member}' from sorted set.")
                    else:
                        messages.warning(request, f"'{member}' not found in sorted set.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Set pop operation
        elif action == "spop":
            count = 1
            count_str = request.POST.get("pop_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = max(1, int(count_str))
            try:
                result = service.spop(key, count=count)
                if not result:
                    messages.error(request, "Set is empty or key does not exist.")
                elif len(result) == 1:
                    messages.success(request, f"Popped: {result[0]}")
                else:
                    messages.success(request, f"Popped {len(result)} member(s): {result}")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Sorted set pop operations
        elif action == "zpopmin":
            pop_count = int(request.POST.get("pop_count", 1) or 1)
            try:
                result = service.zpopmin(key, count=pop_count)
                if not result:
                    messages.error(request, "Sorted set is empty or key does not exist.")
                elif len(result) == 1:
                    member, score = result[0]
                    messages.success(request, f"Popped: {member} (score: {score})")
                else:
                    members = [f"{m} ({s})" for m, s in result]
                    messages.success(request, f"Popped {len(result)} member(s): {', '.join(members)}")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        elif action == "zpopmax":
            pop_count = int(request.POST.get("pop_count", 1) or 1)
            try:
                result = service.zpopmax(key, count=pop_count)
                if not result:
                    messages.error(request, "Sorted set is empty or key does not exist.")
                elif len(result) == 1:
                    member, score = result[0]
                    messages.success(request, f"Popped: {member} (score: {score})")
                else:
                    members = [f"{m} ({s})" for m, s in result]
                    messages.success(request, f"Popped {len(result)} member(s): {', '.join(members)}")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Stream operations
        elif action == "xadd":
            field_name = request.POST.get("field_name", "").strip()
            field_value = request.POST.get("field_value", "").strip()
            if field_name and field_value:
                try:
                    entry_id = service.xadd(key, {field_name: field_value})
                    messages.success(request, f"Added entry {entry_id}.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Field name and value are required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "xdel":
            entry_id = request.POST.get("entry_id", "").strip()
            if entry_id:
                try:
                    deleted = service.xdel(key, entry_id)
                    if deleted:
                        messages.success(request, f"Deleted entry {entry_id}.")
                    else:
                        messages.warning(request, f"Entry {entry_id} not found.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Entry ID is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "xtrim":
            maxlen_str = request.POST.get("maxlen", "").strip()
            if maxlen_str:
                try:
                    maxlen = int(maxlen_str)
                    trimmed = service.xtrim(key, maxlen)
                    messages.success(request, f"Trimmed {trimmed} entries.")
                except ValueError:
                    messages.error(request, "Max length must be a number.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Max length is required.")
            return redirect(key_detail_url(cache_name, key))

    # GET request - display the key
    key_result = service.get(key)

    raw_value = key_result.get("value")
    value_is_editable = True

    if raw_value is not None:
        # Format value for display - JSON-serializable values are editable
        value_display, value_is_editable = _format_value_for_display(raw_value)
    else:
        value_display = "null"

    cache_config = settings.CACHES.get(cache_name, {})
    key_exists = key_result.get("exists", False)

    # Check for create mode (type param provided for non-existing key)
    create_mode = False
    create_type = request.GET.get("type", "").strip()
    if not key_exists:
        if create_type:
            # Create mode: key doesn't exist but type is specified
            create_mode = True
        else:
            messages.error(request, f"Key '{key}' does not exist in cache '{cache_name}'.")
            return redirect(key_list_url(cache_name))

    # Get TTL and type for the key
    key_type = None
    ttl = None
    ttl_expires_at = None
    type_data: dict[str, Any] = {}
    if key_exists:
        with contextlib.suppress(Exception):
            key_type = service.type(key)
        with contextlib.suppress(Exception):
            ttl = service.ttl(key)
            if ttl is not None and ttl >= 0:
                ttl_expires_at = timezone.now() + timedelta(seconds=ttl)
        # Get type-specific data for non-string types
        if key_type and key_type != "string":
            type_data = service.get_type_data(key, key_type)
    elif create_mode:
        # In create mode, use the type from query param
        key_type = create_type

    # Show type-specific help message if requested
    help_key = (
        f"key_detail_{key_type}" if key_type in ("string", "list", "set", "hash", "zset", "stream") else "key_detail"
    )
    help_active = _show_help(request, help_key)

    # Get cache metadata for displaying the raw key info
    cache_metadata = service.get_cache_metadata()
    raw_key = service.make_key(key)

    # In create mode, enable ops based on feature support (not key existence)
    can_operate = key_exists or create_mode

    context = admin.site.each_context(request)
    context.update(
        {
            "title": f"Add Key: {key}" if create_mode else f"Key: {key}",
            "cache_name": cache_name,
            "cache_config": cache_config,
            "key": key,
            "raw_key": raw_key,
            "cache_metadata": cache_metadata,
            "key_value": key_result,
            "key_exists": key_exists,
            "create_mode": create_mode,
            "value_display": value_display,
            "value_is_editable": value_is_editable,
            "key_type": key_type,
            "ttl": ttl,
            "ttl_expires_at": ttl_expires_at,
            "type_data": type_data,
            "delete_supported": key_exists,
            "backend_edit_supported": can_operate,
            "edit_supported": can_operate and value_is_editable,
            "set_ttl_supported": key_exists,
            "list_ops_supported": can_operate,
            "set_ops_supported": can_operate,
            "hash_ops_supported": can_operate,
            "zset_ops_supported": can_operate,
            "stream_ops_supported": can_operate,
            "help_active": help_active,
        },
    )
    return render(request, config.template("key/change_form.html"), context)


@staff_member_required
def key_detail(request: HttpRequest, cache_name: str, key: str) -> HttpResponse:
    """View for displaying the details of a specific cache key."""
    return _key_detail_view(request, cache_name, key, ADMIN_CONFIG)


def _key_add_view(
    request: HttpRequest,
    cache_name: str,
    config: ViewConfig,
) -> HttpResponse:
    """View for adding a new cache key - collects key name and type, then redirects to key_detail.

    This is the internal implementation; use key_add() for the decorated admin view.
    """
    help_active = _show_help(request, "key_add")
    service = get_cache_service(cache_name)
    cache_config = settings.CACHES.get(cache_name, {})

    if request.method == "POST":
        key_name = request.POST.get("key", "").strip()
        key_type = request.POST.get("type", "string").strip()

        if not key_name:
            messages.error(request, "Key name is required.")
        else:
            # Check if key already exists
            existing = service.get(key_name)
            if existing.get("exists", False):
                messages.warning(request, f"Key '{key_name}' already exists.")
                return redirect(key_detail_url(cache_name, key_name))
            # Redirect to key_detail in create mode
            from urllib.parse import urlencode

            base_url = key_detail_url(cache_name, key_name)
            params = urlencode({"type": key_type})
            # Use ? or & depending on whether URL already has query params
            separator = "&" if "?" in base_url else "?"
            return redirect(f"{base_url}{separator}{params}")

    # Pre-fill from query params (for Back button)
    prefill_key = request.GET.get("key", "")
    prefill_type = request.GET.get("type", "string")

    context = admin.site.each_context(request)
    context.update(
        {
            "title": f"Add key to '{cache_name}'",
            "cache_name": cache_name,
            "cache_config": cache_config,
            "prefill_key": prefill_key,
            "prefill_type": prefill_type,
            "help_active": help_active,
        },
    )
    return render(request, config.template("cache/key_add.html"), context)


@staff_member_required
def key_add(request: HttpRequest, cache_name: str) -> HttpResponse:
    """View for adding a new cache key - collects key name and type, then redirects to key_detail."""
    return _key_add_view(request, cache_name, ADMIN_CONFIG)
