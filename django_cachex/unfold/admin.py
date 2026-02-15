"""Django admin classes for cache management with Unfold theme."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin.utils import unquote
from django.core.exceptions import PermissionDenied
from django.http import HttpResponseRedirect
from django.urls import path, reverse

from django_cachex.admin.admin import CacheAdmin as StandardCacheAdmin
from django_cachex.admin.admin import KeyAdmin as StandardKeyAdmin
from django_cachex.admin.models import Cache, Key
from django_cachex.admin.queryset import CacheAdminMixin, KeyAdminMixin
from django_cachex.admin.views import (
    ViewConfig,
    _cache_detail_view,
    _key_add_view,
    _key_detail_view,
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


# Configuration for unfold-themed key detail/add views
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
class CacheAdmin(CacheAdminMixin, _CacheBase):  # type: ignore[misc]
    """Unfold-themed admin for caches.

    Uses CacheAdminMixin for list_display, filtering, search, and flush action.
    """

    _cachex_help_messages = StandardCacheAdmin._cachex_help_messages

    # Caches are defined in settings — add/delete don't apply
    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_delete_permission(
        self,
        request: HttpRequest,
        obj: Cache | None = None,
    ) -> bool:
        return False

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

    def change_view(
        self,
        request: HttpRequest,
        object_id: str,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Display cache details using unfold templates."""
        if not self.has_view_or_change_permission(request):
            raise PermissionDenied
        unfold_config = ViewConfig(
            template_prefix="unfold/django_cachex",
            help_messages=self._cachex_help_messages,
        )
        return _cache_detail_view(request, object_id, unfold_config)


@admin.register(Key)
class KeyAdmin(KeyAdminMixin, _KeyBase):  # type: ignore[misc]
    """Unfold-themed admin for cache keys.

    Uses KeyAdminMixin for list_display, filtering, search, and delete action.
    """

    _cachex_help_messages = StandardKeyAdmin._cachex_help_messages

    def has_module_permission(self, request: HttpRequest) -> bool:
        return True

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

    def change_view(
        self,
        request: HttpRequest,
        object_id: str,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """View/edit a specific key using unfold templates."""
        if not self.has_view_or_change_permission(request):
            raise PermissionDenied

        cache_name, key_name = Key.parse_pk(unquote(object_id))

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
        if not self.has_add_permission(request):
            raise PermissionDenied

        cache_name = request.GET.get("cache") or next(iter(settings.CACHES))

        if Cache.get_by_name(cache_name) is None:
            messages.error(request, f"Cache '{cache_name}' not found.")
            return HttpResponseRedirect(
                reverse("admin:django_cachex_cache_changelist"),
            )

        return _key_add_view(request, cache_name, UNFOLD_KEY_CONFIG)
