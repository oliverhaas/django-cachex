"""
Django admin classes for cache management.

This module provides ModelAdmin classes for managing cache instances
and cache keys through Django's admin interface.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, ClassVar

from django.contrib import admin, messages
from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.urls import path, reverse
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from .helpers import get_cache, get_metadata, get_slowlog
from .models import Cache, Key
from .views.base import (
    ViewConfig,
    show_help,
)

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse

    _CacheBase = admin.ModelAdmin[Cache]
    _KeyBase = admin.ModelAdmin[Key]
else:
    _CacheBase = admin.ModelAdmin
    _KeyBase = admin.ModelAdmin


@admin.register(Cache)
class CacheAdmin(_CacheBase):
    """
    Admin for cache instances.

    - changelist_view: Lists all cache instances (from settings.CACHES)
    - change_view: Shows cache details (info + slowlog combined)
    """

    _cachex_help_messages: ClassVar[dict[str, str]] = {
        "cache_list": mark_safe(
            "<strong>Cache Instances</strong><br>"
            "View all cache backends configured in your Django settings.<br><br>"
            "<strong>Support Levels</strong><br>"
            "<table style='margin: 4px 0 12px 0; border-collapse: collapse;'>"
            "<tr><td style='padding: 2px 8px;'><strong>cachex</strong></td>"
            "<td style='padding: 2px 8px;'>Full support — All features including key browsing, "
            "type detection, TTL management, and data operations</td></tr>"
            "<tr><td style='padding: 2px 8px;'><strong>wrapped</strong></td>"
            "<td style='padding: 2px 8px;'>Wrapped support — Django built-in backends "
            "(LocMem, Database, File) with some features</td></tr>"
            "<tr><td style='padding: 2px 8px;'><strong>limited</strong></td>"
            "<td style='padding: 2px 8px;'>Limited support — Custom/unknown backends, "
            "basic get/set only</td></tr>"
            "</table>"
            "<strong>Actions</strong><br>"
            "• Click a cache name to view its details (info, stats, slowlog)<br>"
            "• Click 'List Keys' to browse keys in that cache<br>"
            "• Select caches and use 'Flush selected caches' to clear them<br>"
            "• Use the filter sidebar to filter by support level",
        ),
        "cache_detail": mark_safe(
            "<strong>Cache Details</strong><br>"
            "View server information, memory stats, and slow query log for this cache.<br><br>"
            "<strong>Sections</strong><br>"
            "• <strong>Configuration</strong> — Backend, location, key prefix<br>"
            "• <strong>Server</strong> — Redis/Valkey version, uptime, port<br>"
            "• <strong>Memory</strong> — Used memory, peak, eviction policy<br>"
            "• <strong>Clients</strong> — Connected and blocked clients<br>"
            "• <strong>Statistics</strong> — Commands processed, hits/misses<br>"
            "• <strong>Keyspace</strong> — Keys per database<br>"
            "• <strong>Slow Log</strong> — Slow queries for performance analysis",
        ),
        "cache_slowlog": mark_safe(
            "<strong>Slow Query Log</strong><br>"
            "Commands that exceeded the server's slowlog threshold.<br><br>"
            "<strong>Columns</strong><br>"
            "• <strong>ID</strong> — Unique log entry identifier<br>"
            "• <strong>Time</strong> — When the command was executed<br>"
            "• <strong>Duration</strong> — Execution time in microseconds<br>"
            "• <strong>Command</strong> — The slow command with arguments<br>"
            "• <strong>Client</strong> — Client address and name<br><br>"
            "<strong>Configuration</strong><br>"
            "The threshold is set via Redis <code>slowlog-log-slower-than</code> config. "
            "Use the dropdown to show 10-100 entries.",
        ),
    }

    # Disable standard CRUD operations
    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_delete_permission(
        self,
        request: HttpRequest,
        obj: Cache | None = None,
    ) -> bool:
        return False

    def has_change_permission(
        self,
        request: HttpRequest,
        obj: Cache | None = None,
    ) -> bool:
        return bool(getattr(request.user, "is_staff", False))

    def has_view_permission(
        self,
        request: HttpRequest,
        obj: Cache | None = None,
    ) -> bool:
        return bool(getattr(request.user, "is_staff", False))

    def has_module_permission(self, request: HttpRequest) -> bool:
        return bool(getattr(request.user, "is_staff", False))

    def get_urls(self) -> list:
        """Add custom URL patterns."""
        urls = super().get_urls()
        custom_urls = [
            path(
                "<path:object_id>/change/",
                self.admin_site.admin_view(self.change_view),
                name="django_cachex_cache_change",
            ),
        ]
        return custom_urls + urls

    def changelist_view(
        self,
        request: HttpRequest,
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """List all configured cache instances."""
        # Show help message if requested
        help_active = show_help(request, "cache_list", self._cachex_help_messages)

        # Handle POST requests (flush cache action)
        if request.method == "POST":
            action = request.POST.get("action")
            selected_caches = request.POST.getlist("_selected_action")

            if action == "flush_selected" and selected_caches:
                flushed_count = 0
                for cache_name in selected_caches:
                    try:
                        cache = get_cache(cache_name)
                        cache.clear()
                        flushed_count += 1
                    except Exception as e:  # noqa: BLE001
                        messages.error(request, f"Error flushing '{cache_name}': {e!s}")
                if flushed_count > 0:
                    messages.success(
                        request,
                        f"Successfully flushed {flushed_count} cache(s).",
                    )
                return HttpResponseRedirect(
                    reverse("admin:django_cachex_cache_changelist"),
                )

        # Get filter parameters
        support_filter = request.GET.get("support", "").strip()
        search_query = request.GET.get("q", "").strip().lower()

        # Build cache info list
        caches_info: list[dict[str, Any]] = []
        any_flush_supported = False

        for cache in Cache.get_all():
            any_flush_supported = True
            try:
                get_cache(cache.name)  # Verify cache is accessible
                cache_info = {
                    "name": cache.name,
                    "config": cache.config,
                    "backend": cache.backend,
                    "backend_short": cache.backend_short,
                    "location": cache.location,
                    "support_level": cache.support_level,
                }
                caches_info.append(cache_info)
            except Exception as e:  # noqa: BLE001
                cache_info = {
                    "name": cache.name,
                    "config": cache.config,
                    "backend": cache.backend,
                    "backend_short": cache.backend_short,
                    "location": cache.location,
                    "support_level": cache.support_level,
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

        context = {
            **self.admin_site.each_context(request),
            "caches_info": caches_info,
            "has_caches_configured": bool(caches_info) or bool(Cache.get_all()),
            "title": _("Cache Admin - Instances"),
            "support_filter": support_filter,
            "search_query": search_query,
            "any_flush_supported": any_flush_supported,
            "help_active": help_active,
            "opts": self.model._meta,
            "cl": None,  # No changelist object
        }
        return render(request, "admin/django_cachex/cache/change_list.html", context)

    def change_view(
        self,
        request: HttpRequest,
        object_id: str,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Display cache details (info + slowlog combined)."""
        cache_name = object_id
        cache_obj = Cache.get_by_name(cache_name)

        if cache_obj is None:
            messages.error(request, f"Cache '{cache_name}' not found.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        # Show help message if requested
        help_active = show_help(request, "cache_detail", self._cachex_help_messages)

        from django.conf import settings

        cache = get_cache(cache_name)
        cache_config = settings.CACHES.get(cache_name, {})

        # Get cache metadata and info
        info_data = None
        raw_info = None
        try:
            info_data = get_metadata(cache, cache_config)
            raw_info = cache.info()
        except Exception as e:  # noqa: BLE001
            messages.error(request, f"Error retrieving cache info: {e!s}")

        # Get slowlog (last 10 entries)
        slowlog_data = None
        try:
            slowlog_data = get_slowlog(cache, 10)
        except Exception as e:  # noqa: BLE001
            messages.error(request, f"Error retrieving slow log: {e!s}")

        # Convert raw_info to pretty-printed JSON for display
        raw_info_json = None
        if raw_info:
            raw_info_json = json.dumps(raw_info, indent=2, default=str)

        context = {
            **self.admin_site.each_context(request),
            "title": f"Cache: {cache_name}",
            "cache_name": cache_name,
            "cache_obj": cache_obj,
            "info_data": info_data,
            "raw_info_json": raw_info_json,
            "slowlog_data": slowlog_data,
            "help_active": help_active,
            "opts": self.model._meta,
        }
        return render(request, "admin/django_cachex/cache/change_form.html", context)


@admin.register(Key)
class KeyAdmin(_KeyBase):
    """
    Admin for cache keys.

    - changelist_view: Browse keys for a cache (requires ?cache=<name> parameter)
    - change_view: View/edit a specific key
    - add_view: Add a new key to a cache

    This admin is accessed via CacheAdmin and is hidden from the sidebar.
    """

    _cachex_help_messages: ClassVar[dict[str, str]] = {
        "key_list": mark_safe(
            "<strong>Key Browser</strong><br>"
            "Search and manage cache keys for this backend.<br><br>"
            "<strong>Search Patterns</strong><br>"
            "Search combines Django-style convenience with Redis/Valkey glob patterns.<br>"
            "• <code>session</code> — Keys containing 'session' (auto-wrapped as <code>*session*</code>)<br>"
            "• <code>prefix:*</code> — Keys starting with 'prefix:'<br>"
            "• <code>*:suffix</code> — Keys ending with ':suffix'<br>"
            "• <code>*</code> — List all keys (default when empty)<br><br>"
            "<strong>Table Columns</strong><br>"
            "• <strong>Key</strong> — Click to view/edit the key<br>"
            "• <strong>Type</strong> — Data type (string, list, set, hash, zset, stream)<br>"
            "• <strong>TTL</strong> — Time until expiration<br>"
            "• <strong>Size</strong> — Length for collections, bytes for strings<br><br>"
            "<strong>Actions</strong><br>"
            "• Use 'Add key' to create new entries<br>"
            "• Select keys and use 'Delete selected' to remove them",
        ),
        "key_detail_string": mark_safe(
            "<strong>String Key</strong><br>"
            "View and edit this string value stored in cache.<br><br>"
            "<strong>Value Format</strong><br>"
            "Values are displayed and edited as JSON. Strings appear quoted, "
            "objects as <code>{...}</code>, arrays as <code>[...]</code>.<br><br>"
            "<strong>Operations</strong><br>"
            "• Edit the value in the textarea and click <strong>Update</strong><br>"
            "• Set TTL to control expiration (empty = no expiry)<br>"
            "• Use <strong>Delete</strong> to remove this key",
        ),
        "key_detail_list": mark_safe(
            "<strong>List Key</strong><br>"
            "View and modify this Redis list (ordered collection).<br><br>"
            "<strong>Operations</strong><br>"
            "• <strong>Push Left/Right</strong> — Add items to the head or tail<br>"
            "• <strong>Pop Left/Right</strong> — Remove and return items from head or tail<br>"
            "• <strong>Trim</strong> — Keep only items in the specified index range<br>"
            "• <strong>Remove</strong> — Delete specific items from the list<br><br>"
            "<strong>Index</strong><br>"
            "Items are shown with their 0-based index. Index 0 is the head (left).",
        ),
        "key_detail_set": mark_safe(
            "<strong>Set Key</strong><br>"
            "View and modify this Redis set (unordered unique members).<br><br>"
            "<strong>Operations</strong><br>"
            "• <strong>Add</strong> — Add a new member to the set<br>"
            "• <strong>Pop</strong> — Remove and return random member(s)<br>"
            "• <strong>Remove</strong> — Delete a specific member<br><br>"
            "<strong>Note</strong><br>"
            "Sets do not allow duplicate members. Adding an existing member has no effect.",
        ),
        "key_detail_hash": mark_safe(
            "<strong>Hash Key</strong><br>"
            "View and modify this Redis hash (field-value mapping).<br><br>"
            "<strong>Operations</strong><br>"
            "• <strong>Set Field</strong> — Add a new field or update existing<br>"
            "• <strong>Update</strong> — Modify a field's value inline<br>"
            "• <strong>Delete</strong> — Remove a field from the hash<br><br>"
            "<strong>Note</strong><br>"
            "Field names must be unique. Setting an existing field overwrites its value.",
        ),
        "key_detail_zset": mark_safe(
            "<strong>Sorted Set Key</strong><br>"
            "View and modify this Redis sorted set (members ordered by score).<br><br>"
            "<strong>Operations</strong><br>"
            "• <strong>Add</strong> — Add member with score. Flags: "
            "NX (only if new), XX (only if exists), GT (if score greater), LT (if score less)<br>"
            "• <strong>Pop Min/Max</strong> — Remove member(s) with lowest/highest score<br>"
            "• <strong>Remove</strong> — Delete a specific member<br><br>"
            "<strong>Ordering</strong><br>"
            "Members are displayed by score (lowest first). Rank is the 0-based position.",
        ),
        "key_detail_stream": mark_safe(
            "<strong>Stream Key</strong><br>"
            "View and modify this Redis stream (append-only log).<br><br>"
            "<strong>Operations</strong><br>"
            "• <strong>Add Entry</strong> — Append a new entry with field-value data<br>"
            "• <strong>Trim</strong> — Limit stream to a maximum number of entries<br>"
            "• <strong>Delete</strong> — Remove a specific entry by ID<br><br>"
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
            "• Plain text — Stored as a string<br>"
            '• <code>{"key": "value"}</code> — JSON objects are parsed and stored<br>'
            "• <code>[1, 2, 3]</code> — JSON arrays are parsed and stored<br><br>"
            "<strong>Timeout</strong><br>"
            "• Leave empty — Uses the cache's default timeout<br>"
            "• Enter seconds — Key expires after this duration<br>"
            "• Enter 0 — Key never expires",
        ),
    }

    # Hide from sidebar - accessed via Cache
    def has_module_permission(self, request: HttpRequest) -> bool:
        return False

    def has_add_permission(self, request: HttpRequest) -> bool:
        return bool(getattr(request.user, "is_staff", False))

    def has_delete_permission(
        self,
        request: HttpRequest,
        obj: Key | None = None,
    ) -> bool:
        return bool(getattr(request.user, "is_staff", False))

    def has_change_permission(
        self,
        request: HttpRequest,
        obj: Key | None = None,
    ) -> bool:
        return bool(getattr(request.user, "is_staff", False))

    def has_view_permission(
        self,
        request: HttpRequest,
        obj: Key | None = None,
    ) -> bool:
        return bool(getattr(request.user, "is_staff", False))

    def get_urls(self) -> list:
        """Add custom URL patterns for key operations."""
        urls = super().get_urls()
        custom_urls = [
            path(
                "<path:object_id>/change/",
                self.admin_site.admin_view(self.change_view),
                name="django_cachex_key_change",
            ),
        ]
        return custom_urls + urls

    def _get_config(self) -> ViewConfig:
        return ViewConfig(help_messages=self._cachex_help_messages)

    def changelist_view(
        self,
        request: HttpRequest,
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Browse keys for a specific cache."""
        from .views.key_list import _key_list_view

        cache_name = request.GET.get("cache", "default")

        # Verify cache exists
        if Cache.get_by_name(cache_name) is None:
            messages.error(request, f"Cache '{cache_name}' not found.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        return _key_list_view(request, cache_name, self._get_config())

    def change_view(
        self,
        request: HttpRequest,
        object_id: str,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """View/edit a specific key."""
        from .views.key_detail import _key_detail_view

        cache_name, key_name = Key.parse_pk(object_id)

        if not cache_name:
            messages.error(request, "Invalid key identifier.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        return _key_detail_view(request, cache_name, key_name, self._get_config())

    def add_view(
        self,
        request: HttpRequest,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Add a new key to a cache."""
        from .views.key_add import _key_add_view

        cache_name = request.GET.get("cache", "default")

        if Cache.get_by_name(cache_name) is None:
            messages.error(request, f"Cache '{cache_name}' not found.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        return _key_add_view(request, cache_name, self._get_config())
