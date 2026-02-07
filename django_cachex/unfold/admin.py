"""
Django admin classes for cache management with Unfold theme.

This module provides ModelAdmin classes that use the Unfold admin theme
while sharing view logic with the standard admin module via ViewConfig.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from django.contrib import admin
from django.http import HttpResponseRedirect
from django.urls import path, reverse

from django_cachex.admin.admin import CacheAdmin as StandardCacheAdmin
from django_cachex.admin.admin import KeyAdmin as StandardKeyAdmin
from django_cachex.admin.models import Cache, Key
from django_cachex.admin.views import (
    ViewConfig,
    _cache_detail_view,
    _index_view,
    _key_add_view,
    _key_detail_view,
    _key_list_view,
)

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse

    _CacheBase = admin.ModelAdmin[Cache]
    _KeyBase = admin.ModelAdmin[Key]
else:
    try:
        from unfold.admin import ModelAdmin as UnfoldModelAdmin

        _CacheBase = UnfoldModelAdmin
        _KeyBase = UnfoldModelAdmin
    except ImportError:
        _CacheBase = admin.ModelAdmin
        _KeyBase = admin.ModelAdmin


# Configuration for unfold-themed views
# Uses admin URLs but with unfold template prefix
UNFOLD_CACHE_CONFIG = ViewConfig(
    template_prefix="unfold/django_cachex",
    help_messages=StandardCacheAdmin._cachex_help_messages,
)
UNFOLD_KEY_CONFIG = ViewConfig(
    template_prefix="unfold/django_cachex",
    help_messages=StandardKeyAdmin._cachex_help_messages,
)


# Unregister models if already registered (e.g., by django_cachex.admin)
# This allows unfold to take over even if both apps are in INSTALLED_APPS
with contextlib.suppress(admin.sites.NotRegistered):  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    admin.site.unregister(Cache)

with contextlib.suppress(admin.sites.NotRegistered):  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    admin.site.unregister(Key)


@admin.register(Cache)
class CacheAdmin(_CacheBase):
    """
    Unfold-themed admin for caches.

    Uses shared view logic with unfold templates.
    """

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
        """List all configured caches using unfold templates."""
        return _index_view(request, UNFOLD_CACHE_CONFIG)

    def change_view(
        self,
        request: HttpRequest,
        object_id: str,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Display cache details using unfold templates."""
        return _cache_detail_view(request, object_id, UNFOLD_CACHE_CONFIG)


@admin.register(Key)
class KeyAdmin(_KeyBase):
    """
    Unfold-themed admin for cache keys.

    Uses shared view logic with unfold templates.
    """

    def has_module_permission(self, request: HttpRequest) -> bool:
        # Hide from sidebar - accessed via Cache
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
        """Browse keys for a specific cache using unfold templates."""
        from django.contrib import messages

        cache_name = request.GET.get("cache", "default")

        if Cache.get_by_name(cache_name) is None:
            messages.error(request, f"Cache '{cache_name}' not found.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        return _key_list_view(request, cache_name, UNFOLD_KEY_CONFIG)

    def change_view(
        self,
        request: HttpRequest,
        object_id: str,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """View/edit a specific key using unfold templates."""
        from django.contrib import messages

        cache_name, key_name = Key.parse_pk(object_id)

        if not cache_name:
            messages.error(request, "Invalid key identifier.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        return _key_detail_view(request, cache_name, key_name, UNFOLD_KEY_CONFIG)

    def add_view(
        self,
        request: HttpRequest,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Add a new key to a cache using unfold templates."""
        from django.contrib import messages

        cache_name = request.GET.get("cache", "default")

        if Cache.get_by_name(cache_name) is None:
            messages.error(request, f"Cache '{cache_name}' not found.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        return _key_add_view(request, cache_name, UNFOLD_KEY_CONFIG)
