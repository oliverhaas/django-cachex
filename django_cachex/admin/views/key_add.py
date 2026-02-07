"""
Key add view for the django-cachex admin.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlencode

from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import redirect, render

from django_cachex.admin.service import get_cache_service
from django_cachex.admin.views.base import (
    ADMIN_CONFIG,
    ViewConfig,
    key_detail_url,
    show_help,
)

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


def _key_add_view(
    request: HttpRequest,
    cache_name: str,
    config: ViewConfig,
) -> HttpResponse:
    """View for adding a new cache key - collects key name and type, then redirects to key_detail.

    This is the internal implementation; use key_add() for the decorated admin view.
    """
    help_active = show_help(request, "key_add")
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
    return render(request, config.template("key/add_form.html"), context)


@staff_member_required
def key_add(request: HttpRequest, cache_name: str) -> HttpResponse:
    """View for adding a new cache key - collects key name and type, then redirects to key_detail."""
    return _key_add_view(request, cache_name, ADMIN_CONFIG)
