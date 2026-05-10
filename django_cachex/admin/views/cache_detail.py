"""Cache detail view for the django-cachex admin."""

import json
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.contrib import admin, messages
from django.shortcuts import redirect, render

from django_cachex.admin.helpers import get_cache, get_slowlog, parse_metadata
from django_cachex.admin.models import Cache
from django_cachex.admin.views.base import (
    ViewConfig,
    cache_list_url,
    show_help,
)
from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


def _handle_danger_zone_post(
    request: HttpRequest,
    cache_name: str,
) -> HttpResponse | None:
    """Handle danger zone POST actions. Returns a redirect or None."""
    from django.core.exceptions import PermissionDenied

    if request.method != "POST":
        return None

    if not request.user.has_perm("django_cachex.change_cache"):  # ty: ignore[unresolved-attribute]
        raise PermissionDenied

    action = request.POST.get("action")
    if action not in ("clear_all_versions", "flush_db"):
        return None

    # The danger-zone buttons are only rendered for cachex backends; a
    # direct POST against a wrapped/limited backend would otherwise hit
    # AttributeError on ``clear_all_versions`` / ``flush_db``. Reject up
    # front with a clean message instead.
    cache_obj = Cache.get_by_name(cache_name)
    if cache_obj is None or cache_obj.support_level != "cachex":
        messages.error(request, "Destructive operations are only available on django-cachex backends.")
        return redirect(request.get_full_path())

    cache = get_cache(cache_name)

    if action == "clear_all_versions":
        try:
            deleted = cache.clear_all_versions()
            messages.success(request, f"Deleted {deleted} key(s) across all versions of '{cache_name}'.")
        except Exception as exc:  # noqa: BLE001
            messages.error(request, f"Error: {exc}")
        return redirect(request.get_full_path())

    # The action == "flush_db" branch.
    try:
        cache.flush_db()
        messages.success(request, f"Database flushed for '{cache_name}'.")
    except Exception as exc:  # noqa: BLE001
        messages.error(request, f"Error: {exc}")
    return redirect(request.get_full_path())


def _cache_detail_view(
    request: HttpRequest,
    cache_name: str,
    config: ViewConfig,
) -> HttpResponse:
    """Display cache details (info + slowlog combined)."""
    cache_obj = Cache.get_by_name(cache_name)

    if cache_obj is None:
        messages.error(request, f"Cache '{cache_name}' not found.")
        return redirect(cache_list_url())

    response = _handle_danger_zone_post(request, cache_name)
    if response is not None:
        return response

    help_active = show_help(request, "cache_detail", config.help_messages)

    cache = get_cache(cache_name)
    cache_config = settings.CACHES.get(cache_name, {})

    # ``AttributeError`` and ``NotSupportedError`` mean "this backend
    # doesn't expose info()": render the Configuration section from
    # ``settings.CACHES`` and skip the dynamic server/memory/stats sections.
    # Other exceptions (connection drop, etc.) get surfaced as a message.
    raw_info: dict[str, Any] | None = None
    try:
        raw_info = cache.info()
    except AttributeError, NotSupportedError:
        raw_info = None
    except Exception as e:  # noqa: BLE001
        messages.error(request, f"Error retrieving cache info: {e!s}")

    info_data = parse_metadata(cache, cache_config, raw_info)

    try:
        slowlog_count = max(1, int(request.GET.get("count", 10)))
    except ValueError, TypeError:
        slowlog_count = 10

    slowlog_data = None
    try:
        slowlog_data = get_slowlog(cache, slowlog_count)
    except AttributeError, NotSupportedError:
        slowlog_data = None
    except Exception as e:  # noqa: BLE001
        messages.error(request, f"Error retrieving slow log: {e!s}")

    raw_info_json = None
    if raw_info:
        raw_info_json = json.dumps(raw_info, indent=2, default=str)

    is_cachex = cache_obj.support_level == "cachex"
    can_change = request.user.has_perm("django_cachex.change_cache")  # ty: ignore[unresolved-attribute]

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
            "show_danger_zone": is_cachex and can_change,
        },
    )
    return render(request, config.template("cache/change_form.html"), context)
