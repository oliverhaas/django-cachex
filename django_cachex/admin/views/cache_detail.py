"""
Cache detail view for the django-cachex admin.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import redirect, render

from django_cachex.admin.models import Cache
from django_cachex.admin.service import get_cache_service
from django_cachex.admin.views.base import (
    ADMIN_CONFIG,
    ViewConfig,
    cache_list_url,
    show_help,
)

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


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
    help_active = show_help(request, "cache_info")

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
