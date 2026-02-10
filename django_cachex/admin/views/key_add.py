"""
Key add view for the django-cachex admin.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlencode

from django.contrib import admin, messages
from django.core.exceptions import PermissionDenied
from django.shortcuts import redirect, render

from django_cachex.admin.helpers import get_cache
from django_cachex.admin.views.base import (
    ViewConfig,
    key_detail_url,
    show_help,
)
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


def _key_add_view(
    request: HttpRequest,
    cache_name: str,
    config: ViewConfig,
) -> HttpResponse:
    """View for adding a new cache key - collects key name and type, then redirects to key_detail."""
    help_active = show_help(request, "key_add", config.help_messages)
    cache = get_cache(cache_name)

    if request.method == "POST":
        if not request.user.has_perm("django_cachex.add_key"):
            raise PermissionDenied
        key_name = request.POST.get("key", "").strip()
        key_type = request.POST.get("type", KeyType.STRING).strip()

        if not key_name:
            messages.error(request, "Key name is required.")
        else:
            # Check if key already exists
            if cache.has_key(key_name):
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
    prefill_type = request.GET.get("type", KeyType.STRING)

    context = admin.site.each_context(request)
    context.update(
        {
            "title": f"Add key to '{cache_name}'",
            "cache_name": cache_name,
            "prefill_key": prefill_key,
            "prefill_type": prefill_type,
            "help_active": help_active,
        },
    )
    return render(request, config.template("key/add_form.html"), context)
