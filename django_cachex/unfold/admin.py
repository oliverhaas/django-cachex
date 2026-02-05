from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django.contrib import admin
from django.http import HttpResponseRedirect
from django.urls import reverse

from .models import Cache

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse

    _BaseAdmin = admin.ModelAdmin[Cache]
else:
    try:
        from unfold.admin import ModelAdmin as _BaseAdmin
    except ImportError:
        _BaseAdmin = admin.ModelAdmin


@admin.register(Cache)
class CacheAdmin(_BaseAdmin):
    """
    Admin class for the unfold-styled cache admin.

    The actual views are served via the cache admin URL include.
    This class provides the sidebar entry and permissions.
    """

    def changelist_view(
        self,
        request: HttpRequest,
        extra_context: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Redirect to the cache admin index view."""
        return HttpResponseRedirect(reverse("django_cachex:index"))

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(
        self,
        request: HttpRequest,
        obj: Cache | None = None,
    ) -> bool:
        return bool(getattr(request.user, "is_staff", False))

    def has_delete_permission(
        self,
        request: HttpRequest,
        obj: Cache | None = None,
    ) -> bool:
        return False

    def has_view_permission(
        self,
        request: HttpRequest,
        obj: Cache | None = None,
    ) -> bool:
        return bool(getattr(request.user, "is_staff", False))

    def has_module_permission(self, request: HttpRequest) -> bool:
        return bool(getattr(request.user, "is_staff", False))
