"""
Key list view for the django-cachex admin.
"""

from __future__ import annotations

import contextlib
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import redirect, render
from django.utils import timezone

from django_cachex.admin.models import Key
from django_cachex.admin.service import get_cache_service
from django_cachex.admin.views.base import (
    ADMIN_CONFIG,
    ViewConfig,
    key_list_url,
    logger,
    show_help,
)

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


def _key_list_view(  # noqa: C901, PLR0912, PLR0915
    request: HttpRequest,
    cache_name: str,
    config: ViewConfig,
) -> HttpResponse:
    """View for searching/browsing cache keys.

    This is the internal implementation; use key_list() for the decorated admin view.
    """
    # Show help message if requested
    help_active = show_help(request, "key_list")

    service = get_cache_service(cache_name)
    cache_config = settings.CACHES.get(cache_name, {})

    # Handle POST requests (bulk delete)
    if request.method == "POST":
        action = request.POST.get("action")

        if action == "delete_selected":
            selected_keys = request.POST.getlist("_selected_action")
            if selected_keys:
                deleted_count = 0
                for key in selected_keys:
                    with contextlib.suppress(Exception):
                        service.delete(key)
                        deleted_count += 1
                if deleted_count > 0:
                    messages.success(
                        request,
                        f"Successfully deleted {deleted_count} key(s).",
                    )
            return redirect(key_list_url(cache_name))

    context = admin.site.each_context(request)
    context.update(
        {
            "title": f"Keys in '{cache_name}'",
            "cache_name": cache_name,
            "cache_config": cache_config,
            "help_active": help_active,
        },
    )

    search_query = request.GET.get("q", "").strip()
    count = int(request.GET.get("count", 100))
    cursor = int(request.GET.get("cursor", 0))

    context["search_query"] = search_query
    context["count"] = count
    context["cursor"] = cursor

    # Handle pattern search (auto-wrap in wildcards for Django-style contains search)
    if search_query:
        # Auto-wrap in wildcards if none present (Django-style contains search)
        if "*" not in search_query and "?" not in search_query:
            pattern = f"*{search_query}*"
        else:
            pattern = search_query
    else:
        pattern = "*"
    try:
        query_result = service.keys(
            pattern=pattern,
            cursor=cursor,
            count=count,
        )

        keys = query_result["keys"]
        next_cursor = query_result["next_cursor"]
        total_count = query_result.get("total_count")  # May be None for SCAN
        error = query_result.get("error")

        if error:
            context["error"] = error

        keys_data = []
        for key_item in keys:
            if isinstance(key_item, dict):
                user_key = key_item["key"]
                redis_key = key_item.get("redis_key")
            else:
                user_key = key_item
                redis_key = None

            key_entry: dict[str, Any] = {
                "key": user_key,
                "pk": Key.make_pk(cache_name, user_key),
            }
            if redis_key:
                key_entry["redis_key"] = redis_key
            keys_data.append(key_entry)

        # Fetch TTL, type, and size for displayed keys
        for key_entry in keys_data:
            user_key = key_entry["key"]
            key_type = None
            with contextlib.suppress(Exception):
                ttl = service.ttl(user_key)
                key_entry["ttl"] = ttl
                if ttl >= 0:
                    key_entry["ttl_expires_at"] = timezone.now() + timedelta(seconds=ttl)
            with contextlib.suppress(Exception):
                key_type = service.type(user_key)
                key_entry["type"] = key_type
            with contextlib.suppress(Exception):
                key_entry["size"] = service.size(user_key, key_type)

        context["keys_data"] = keys_data
        context["total_keys"] = total_count  # May be None
        context["keys_count"] = len(keys_data)

        # Cursor-based pagination
        context["next_cursor"] = next_cursor
        context["has_next"] = next_cursor != 0
        context["has_previous"] = cursor > 0

    except Exception:  # noqa: BLE001
        logger.exception("Error querying cache '%s'", cache_name)
        context["error_message"] = "An error occurred while querying the cache."

    return render(request, config.template("key/change_list.html"), context)


@staff_member_required
def key_list(request: HttpRequest, cache_name: str) -> HttpResponse:
    """View for searching/browsing cache keys."""
    return _key_list_view(request, cache_name, ADMIN_CONFIG)
