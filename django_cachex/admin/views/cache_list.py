"""
Cache list view for the django-cachex admin.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.contrib import admin, messages
from django.shortcuts import redirect, render

from django_cachex.admin.helpers import get_cache
from django_cachex.admin.models import Cache
from django_cachex.admin.views.base import (
    ViewConfig,
    cache_list_url,
    show_help,
)

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


def _index_view(request: HttpRequest, config: ViewConfig) -> HttpResponse:
    """Display all configured caches with their capabilities."""
    # Show help message if requested
    help_active = show_help(request, "cache_list", config.help_messages)

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
            get_cache(cache_name)  # Verify cache is accessible
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
            "title": "Cache Admin - Caches",
            "support_filter": support_filter,
            "search_query": search_query,
            "any_flush_supported": any_flush_supported,
            "help_active": help_active,
        },
    )
    return render(request, config.template("cache/change_list.html"), context)
