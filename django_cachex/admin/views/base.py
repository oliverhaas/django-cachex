"""
Base utilities and configuration for cache admin views.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from django.contrib import messages
from django.urls import reverse
from django.utils.safestring import mark_safe

from django_cachex.admin.models import Key

if TYPE_CHECKING:
    from django.http import HttpRequest


logger = logging.getLogger(__name__)


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


# =============================================================================
# Help Messages
# =============================================================================


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


# =============================================================================
# Utility Functions
# =============================================================================


def show_help(request: HttpRequest, view_name: str) -> bool:
    """Check if help was requested and show message if so. Returns True if help shown."""
    if request.GET.get("help"):
        help_text = HELP_MESSAGES.get(view_name, "")
        if help_text:
            messages.info(request, help_text)
        return True
    return False


def is_json_serializable(value: Any) -> bool:
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


def format_value_for_display(value: Any) -> tuple[str, bool]:
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

    if is_json_serializable(value):
        return json.dumps(value, indent=2, ensure_ascii=False), True
    return repr(value), False


def parse_timeout(timeout_str: str) -> tuple[float | None, str | None]:
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


def get_page_range(
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
