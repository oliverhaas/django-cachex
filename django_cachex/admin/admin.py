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

from .models import Cache, Key
from .service import get_cache_service

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

    # Help messages for each view (HTML content wrapped with mark_safe)
    _cachex_help_messages: ClassVar[dict[str, str]] = {
        "cache_changelist": mark_safe(
            "<strong>Cache Instances</strong><br>"
            "View all cache backends configured in your Django settings.<br><br>"
            "<strong>Support Levels</strong><br>"
            "<table style='margin: 4px 0 12px 0; border-collapse: collapse;'>"
            "<tr><td style='padding: 2px 8px;'><strong>cachex</strong></td>"
            "<td style='padding: 2px 8px;'>Full support — All features including key browsing, "
            "type detection, TTL management, and data operations</td></tr>"
            "<tr><td style='padding: 2px 8px;'><strong>wrapped</strong></td>"
            "<td style='padding: 2px 8px;'>Wrapped support — Django built-in backends "
            "(Redis, LocMem, Database, File) with some features</td></tr>"
            "<tr><td style='padding: 2px 8px;'><strong>limited</strong></td>"
            "<td style='padding: 2px 8px;'>Limited support — Custom/unknown backends, "
            "basic get/set only</td></tr>"
            "</table>"
            "<strong>Actions</strong><br>"
            "• Click a cache name to view its details (info, stats, slowlog)<br>"
            "• Click 'List Keys' to browse keys in that cache<br>"
            "• Select caches with checkboxes and use 'Flush selected caches' to clear them<br>"
            "• Use the filter sidebar to show only specific support levels",
        ),
        "cache_change": mark_safe(
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
    }

    # Django builtin backends that get "wrapped" support level
    _cachex_django_builtins: ClassVar[set[str]] = {
        "django.core.cache.backends.locmem.LocMemCache",
        "django.core.cache.backends.db.DatabaseCache",
        "django.core.cache.backends.filebased.FileBasedCache",
        "django.core.cache.backends.dummy.DummyCache",
        "django.core.cache.backends.memcached.PyMemcacheCache",
        "django.core.cache.backends.memcached.PyLibMCCache",
        "django.core.cache.backends.memcached.MemcachedCache",
    }

    def _cachex_show_help(self, request: HttpRequest, view_name: str) -> bool:
        """Check if help was requested and show message if so. Returns True if help shown."""
        if request.GET.get("help"):
            help_text = self._cachex_help_messages.get(view_name, "")
            if help_text:
                messages.info(request, help_text)
            return True
        return False

    def _cachex_get_support_level(self, backend: str) -> str:
        """Determine the support level for a cache backend.

        Returns:
            - "cachex": Full support (django-cachex backends)
            - "wrapped": Django core builtin backends (wrapped for almost full support)
            - "limited": Custom/unknown backends with limited support
        """
        # django-cachex backends - full support
        if backend.startswith("django_cachex."):
            return "cachex"

        # Django core builtin backends - wrapped support
        if backend in self._cachex_django_builtins:
            return "wrapped"

        # Unknown/custom backends
        return "limited"

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
        help_active = self._cachex_show_help(request, "cache_changelist")

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
            backend = cache.backend
            support_level = self._cachex_get_support_level(backend)
            any_flush_supported = True
            try:
                get_cache_service(cache.name)  # Verify cache is accessible
                cache_info = {
                    "name": cache.name,
                    "config": cache.config,
                    "backend": backend,
                    "backend_short": cache.backend_short,
                    "location": cache.location,
                    "support_level": support_level,
                }
                caches_info.append(cache_info)
            except Exception as e:  # noqa: BLE001
                cache_info = {
                    "name": cache.name,
                    "config": cache.config,
                    "backend": backend,
                    "backend_short": cache.backend_short,
                    "location": cache.location,
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
        help_active = self._cachex_show_help(request, "cache_change")

        service = get_cache_service(cache_name)

        # Get cache metadata and info
        info_data = None
        raw_info = None
        try:
            info_data = service.metadata()
            raw_info = service.info()
        except Exception as e:  # noqa: BLE001
            messages.error(request, f"Error retrieving cache info: {e!s}")

        # Get slowlog (last 10 entries)
        slowlog_data = None
        try:
            slowlog_data = service.slowlog_get(10)
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

    def changelist_view(
        self,
        request: HttpRequest,
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Browse keys for a specific cache."""
        # Import here to avoid circular imports and keep views logic accessible
        from . import views

        cache_name = request.GET.get("cache", "default")

        # Verify cache exists
        if Cache.get_by_name(cache_name) is None:
            messages.error(request, f"Cache '{cache_name}' not found.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        # Delegate to views.key_list with adapted URL handling
        # For now, we use a redirect to maintain compatibility
        # This will be replaced with inline logic in a future iteration
        return views.key_list(request, cache_name)

    def change_view(
        self,
        request: HttpRequest,
        object_id: str,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """View/edit a specific key."""
        from . import views

        cache_name, key_name = Key.parse_pk(object_id)

        if not cache_name:
            messages.error(request, "Invalid key identifier.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        return views.key_detail(request, cache_name, key_name)

    def add_view(
        self,
        request: HttpRequest,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Add a new key to a cache."""
        from . import views

        cache_name = request.GET.get("cache", "default")

        if Cache.get_by_name(cache_name) is None:
            messages.error(request, f"Cache '{cache_name}' not found.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        return views.key_add(request, cache_name)
