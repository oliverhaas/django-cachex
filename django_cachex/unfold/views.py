"""
Views for the django-cachex unfold-styled cache admin.

This module provides the same functionality as django_cachex.admin.views
but renders with unfold-optimized templates.
"""

from __future__ import annotations

import contextlib
import json
import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone

from django_cachex.admin.service import get_cache_service
from django_cachex.admin.views import (
    _format_value_for_display,
    _get_support_level,
    _parse_timeout,
    _show_help,
)

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse

logger = logging.getLogger(__name__)


def _template(name: str) -> str:
    """Get the unfold template path."""
    return f"unfold/django_cachex/cache/{name}"


@staff_member_required
def index(request: HttpRequest) -> HttpResponse:  # noqa: C901
    """Display all configured cache instances with their panel abilities."""
    # Show help message if requested
    help_active = _show_help(request, "index")

    # Handle POST requests (flush cache action)
    if request.method == "POST":
        action = request.POST.get("action")
        selected_caches = request.POST.getlist("_selected_action")

        if action == "flush_selected" and selected_caches:
            flushed_count = 0
            for cache_name in selected_caches:
                try:
                    service = get_cache_service(cache_name)
                    if service.is_feature_supported("flush_cache"):
                        service.flush_cache()
                        flushed_count += 1
                except Exception as e:  # noqa: BLE001
                    messages.error(request, f"Error flushing '{cache_name}': {e!s}")
            if flushed_count > 0:
                messages.success(
                    request,
                    f"Successfully flushed {flushed_count} cache(s).",
                )
            return redirect(reverse("django_cachex:index"))

    # Get filter parameters
    support_filter = request.GET.get("support", "").strip()
    search_query = request.GET.get("q", "").strip().lower()

    caches_info: list[dict[str, Any]] = []
    any_flush_supported = False
    for cache_name, cache_config in settings.CACHES.items():
        backend = str(cache_config.get("BACKEND", "Unknown"))
        support_level = _get_support_level(backend)
        try:
            service = get_cache_service(cache_name)
            flush_supported = service.is_feature_supported("flush_cache")
            if flush_supported:
                any_flush_supported = True
            cache_info = {
                "name": cache_name,
                "config": cache_config,
                "backend": backend,
                "backend_short": backend.rsplit(".", 1)[-1] if "." in backend else backend,
                "location": cache_config.get("LOCATION", ""),
                "abilities": service.abilities,
                "support_level": support_level,
                "flush_supported": flush_supported,
            }
            caches_info.append(cache_info)
        except Exception as e:  # noqa: BLE001
            cache_info = {
                "name": cache_name,
                "config": cache_config,
                "backend": backend,
                "backend_short": backend.rsplit(".", 1)[-1] if "." in backend else backend,
                "location": cache_config.get("LOCATION", ""),
                "abilities": {},
                "support_level": support_level,
                "flush_supported": False,
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
            "title": "Cache Admin - Instances",
            "support_filter": support_filter,
            "search_query": search_query,
            "any_flush_supported": any_flush_supported,
            "help_active": help_active,
        },
    )
    return render(request, _template("index.html"), context)


@staff_member_required
def help_view(request: HttpRequest) -> HttpResponse:
    """Display help information about the cache panel."""
    context = admin.site.each_context(request)
    context.update(
        {
            "title": "Cache Admin - Help",
        },
    )
    return render(request, _template("help.html"), context)


@staff_member_required
def key_search(request: HttpRequest, cache_name: str) -> HttpResponse:  # noqa: C901, PLR0912, PLR0915
    """View for searching/browsing cache keys."""
    # Show help message if requested
    help_active = _show_help(request, "key_search")

    service = get_cache_service(cache_name)
    cache_config = settings.CACHES.get(cache_name, {})

    # Handle POST requests (bulk delete)
    if request.method == "POST":
        action = request.POST.get("action")

        if action == "delete_selected" and service.is_feature_supported("delete_key"):
            selected_keys = request.POST.getlist("_selected_action")
            if selected_keys:
                deleted_count = 0
                for key in selected_keys:
                    with contextlib.suppress(Exception):
                        service.delete_key(key)
                        deleted_count += 1
                if deleted_count > 0:
                    messages.success(
                        request,
                        f"Successfully deleted {deleted_count} key(s).",
                    )
            return redirect(
                reverse("django_cachex:key_search", args=[cache_name]),
            )

    context = admin.site.each_context(request)
    context.update(
        {
            "title": f"Keys in '{cache_name}'",
            "cache_name": cache_name,
            "cache_config": cache_config,
            "query_supported": service.is_feature_supported("query"),
            "get_key_supported": service.is_feature_supported("get_key"),
            "abilities": service.abilities,
            "help_active": help_active,
        },
    )

    search_query = request.GET.get("q", "").strip()
    cursor = int(request.GET.get("cursor", 0))
    count = int(request.GET.get("count", 25))

    context["search_query"] = search_query
    context["cursor"] = cursor
    context["count"] = count

    # Handle pattern search (auto-wrap in wildcards for Django-style contains search)
    if service.is_feature_supported("query"):
        if search_query:
            # Auto-wrap in wildcards if none present (Django-style contains search)
            if "*" not in search_query and "?" not in search_query:
                pattern = f"*{search_query}*"
            else:
                pattern = search_query
        else:
            pattern = "*"
        try:
            query_result = service.query(
                instance_alias=cache_name,
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

                key_entry: dict[str, Any] = {"key": user_key}
                if redis_key:
                    key_entry["redis_key"] = redis_key
                keys_data.append(key_entry)

            # Fetch TTL, type, and size for displayed keys
            if keys_data:
                for key_entry in keys_data:
                    user_key = key_entry["key"]
                    key_type = None
                    if service.is_feature_supported("get_ttl"):
                        ttl = service.get_key_ttl(user_key)
                        key_entry["ttl"] = ttl
                        if ttl is not None and ttl >= 0:
                            key_entry["ttl_expires_at"] = timezone.now() + timedelta(seconds=ttl)
                    if service.is_feature_supported("get_type"):
                        key_type = service.get_key_type(user_key)
                        key_entry["type"] = key_type
                    if service.is_feature_supported("get_size"):
                        key_entry["size"] = service.get_key_size(user_key, key_type)

            context["keys_data"] = keys_data
            context["show_ttl"] = service.is_feature_supported("get_ttl")
            context["show_type"] = service.is_feature_supported("get_type")
            context["show_size"] = service.is_feature_supported("get_size")
            context["total_keys"] = total_count
            context["next_cursor"] = next_cursor
            context["has_more"] = next_cursor != 0

        except Exception:
            logger.exception("Error querying cache '%s'", cache_name)
            context["error_message"] = "An error occurred while querying the cache."

    return render(request, _template("key_search.html"), context)


@staff_member_required
def key_detail(request: HttpRequest, cache_name: str, key: str) -> HttpResponse:  # noqa: C901, PLR0911, PLR0912, PLR0915
    """View for displaying the details of a specific cache key."""
    # Show help message if requested
    help_active = _show_help(request, "key_detail")

    service = get_cache_service(cache_name)

    # Handle POST requests (update or delete)
    if request.method == "POST":
        action = request.POST.get("action")

        if action == "delete" and service.is_feature_supported("delete_key"):
            try:
                service.delete_key(key)
                messages.success(request, "Key deleted successfully.")
                return redirect(
                    reverse("django_cachex:key_search", args=[cache_name]),
                )
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error deleting key: {e!s}")

        elif action == "update" and service.is_feature_supported("edit_key"):
            try:
                new_value: Any = request.POST.get("value", "")
                with contextlib.suppress(json.JSONDecodeError, ValueError):
                    new_value = json.loads(new_value)

                timeout, timeout_error = _parse_timeout(request.POST.get("timeout", ""))
                if timeout_error:
                    messages.error(request, timeout_error)
                else:
                    result = service.edit_key(key, new_value, timeout=timeout)
                    messages.success(request, result["message"])
                    return redirect(
                        reverse("django_cachex:key_detail", args=[cache_name, key]),
                    )
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error updating key: {e!s}")

        elif action == "set_ttl" and service.is_feature_supported("set_ttl"):
            try:
                ttl_value = request.POST.get("ttl_value", "").strip()
                if not ttl_value:
                    messages.error(request, "TTL value is required.")
                else:
                    ttl_int = int(ttl_value)
                    if ttl_int <= 0:
                        messages.error(request, "TTL must be a positive number.")
                    else:
                        result = service.set_key_ttl(key, ttl_int)
                        if result["success"]:
                            messages.success(request, result["message"])
                        else:
                            messages.error(request, result["message"])
                        return redirect(
                            reverse("django_cachex:key_detail", args=[cache_name, key]),
                        )
            except ValueError:
                messages.error(request, "Invalid TTL value. Must be a number.")
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error setting TTL: {e!s}")

        elif action == "persist" and service.is_feature_supported("set_ttl"):
            try:
                result = service.persist_key(key)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
                return redirect(
                    reverse("django_cachex:key_detail", args=[cache_name, key]),
                )
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error removing TTL: {e!s}")

        # List operations
        elif action == "list_lpop" and service.is_feature_supported("list_ops"):
            count = 1
            count_str = request.POST.get("pop_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = max(1, int(count_str))
            result = service.list_lpop(key, count=count)
            if result["success"]:
                messages.success(request, result["message"])
            else:
                messages.error(request, result["message"])
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "list_rpop" and service.is_feature_supported("list_ops"):
            count = 1
            count_str = request.POST.get("pop_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = max(1, int(count_str))
            result = service.list_rpop(key, count=count)
            if result["success"]:
                messages.success(request, result["message"])
            else:
                messages.error(request, result["message"])
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "list_lpush" and service.is_feature_supported("list_ops"):
            value = request.POST.get("push_value", "").strip()
            if value:
                result = service.list_lpush(key, value)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            else:
                messages.error(request, "Value is required.")
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "list_rpush" and service.is_feature_supported("list_ops"):
            value = request.POST.get("push_value", "").strip()
            if value:
                result = service.list_rpush(key, value)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            else:
                messages.error(request, "Value is required.")
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "list_lrem" and service.is_feature_supported("list_ops"):
            value = request.POST.get("item_value", "").strip()
            count = 0  # Default: remove all occurrences
            count_str = request.POST.get("lrem_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = int(count_str)
            if value:
                result = service.list_lrem(key, value, count=count)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            else:
                messages.error(request, "Value is required.")
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "list_ltrim" and service.is_feature_supported("list_ops"):
            try:
                start = int(request.POST.get("trim_start", "0"))
                stop = int(request.POST.get("trim_stop", "-1"))
                result = service.list_ltrim(key, start, stop)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            except ValueError:
                messages.error(request, "Start and stop must be integers.")
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        # Set operations
        elif action == "set_sadd" and service.is_feature_supported("set_ops"):
            member = request.POST.get("member_value", "").strip()
            if member:
                result = service.set_sadd(key, member)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            else:
                messages.error(request, "Member is required.")
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "set_srem" and service.is_feature_supported("set_ops"):
            member = request.POST.get("member", "").strip()
            if member:
                result = service.set_srem(key, member)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        # Hash operations
        elif action == "hash_hset" and service.is_feature_supported("hash_ops"):
            field = request.POST.get("field_name", "").strip()
            value = request.POST.get("field_value", "").strip()
            if field:
                result = service.hash_hset(key, field, value)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            else:
                messages.error(request, "Field name is required.")
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "hash_hdel" and service.is_feature_supported("hash_ops"):
            field = request.POST.get("field", "").strip()
            if field:
                result = service.hash_hdel(key, field)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        # Sorted set operations
        elif action == "zset_zadd" and service.is_feature_supported("zset_ops"):
            member = request.POST.get("member_value", "").strip()
            score_str = request.POST.get("score_value", "").strip()
            # Get ZADD flags
            nx = request.POST.get("zadd_nx") == "on"
            xx = request.POST.get("zadd_xx") == "on"
            gt = request.POST.get("zadd_gt") == "on"
            lt = request.POST.get("zadd_lt") == "on"
            if member and score_str:
                try:
                    score = float(score_str)
                    result = service.zset_zadd(key, member, score, nx=nx, xx=xx, gt=gt, lt=lt)
                    if result["success"]:
                        messages.success(request, result["message"])
                    else:
                        messages.error(request, result["message"])
                except ValueError:
                    messages.error(request, "Score must be a number.")
            else:
                messages.error(request, "Member and score are required.")
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "zset_zrem" and service.is_feature_supported("zset_ops"):
            member = request.POST.get("member", "").strip()
            if member:
                result = service.zset_zrem(key, member)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        # Set pop operation
        elif action == "set_spop" and service.is_feature_supported("set_ops"):
            count = 1
            count_str = request.POST.get("pop_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = max(1, int(count_str))
            result = service.set_spop(key, count=count)
            if result["success"]:
                messages.success(request, result["message"])
            else:
                messages.error(request, result["message"])
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        # Sorted set pop operations
        elif action == "zset_zpopmin" and service.is_feature_supported("zset_ops"):
            result = service.zset_zpopmin(key)
            if result["success"]:
                messages.success(request, result["message"])
            else:
                messages.error(request, result["message"])
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "zset_zpopmax" and service.is_feature_supported("zset_ops"):
            result = service.zset_zpopmax(key)
            if result["success"]:
                messages.success(request, result["message"])
            else:
                messages.error(request, result["message"])
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        # Stream operations
        elif action == "stream_xadd" and service.is_feature_supported("stream_ops"):
            field_name = request.POST.get("field_name", "").strip()
            field_value = request.POST.get("field_value", "").strip()
            if field_name and field_value:
                result = service.stream_xadd(key, {field_name: field_value})
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            else:
                messages.error(request, "Field name and value are required.")
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "stream_xdel" and service.is_feature_supported("stream_ops"):
            entry_id = request.POST.get("entry_id", "").strip()
            if entry_id:
                result = service.stream_xdel(key, entry_id)
                if result["success"]:
                    messages.success(request, result["message"])
                else:
                    messages.error(request, result["message"])
            else:
                messages.error(request, "Entry ID is required.")
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

        elif action == "stream_xtrim" and service.is_feature_supported("stream_ops"):
            maxlen_str = request.POST.get("maxlen", "").strip()
            if maxlen_str:
                try:
                    maxlen = int(maxlen_str)
                    result = service.stream_xtrim(key, maxlen)
                    if result["success"]:
                        messages.success(request, result["message"])
                    else:
                        messages.error(request, result["message"])
                except ValueError:
                    messages.error(request, "Max length must be a number.")
            else:
                messages.error(request, "Max length is required.")
            return redirect(reverse("django_cachex:key_detail", args=[cache_name, key]))

    # GET request - display the key
    key_result = service.get_key(key)

    raw_value = key_result.get("value")
    value_is_editable = True

    if raw_value is not None:
        # Format value for display - JSON-serializable values are editable
        value_display, value_is_editable = _format_value_for_display(raw_value)
    else:
        value_display = "null"

    cache_config = settings.CACHES.get(cache_name, {})
    key_exists = key_result.get("exists", False)

    # Check for create mode (type param provided for non-existing key)
    create_mode = False
    create_type = request.GET.get("type", "").strip()
    if not key_exists and create_type:
        create_mode = True

    # Get TTL and type for the key
    redis_type = None
    ttl = None
    ttl_expires_at = None
    type_data: dict[str, Any] = {}
    if key_exists:
        if service.is_feature_supported("get_type"):
            redis_type = service.get_key_type(key)
        if service.is_feature_supported("get_ttl"):
            ttl = service.get_key_ttl(key)
            if ttl is not None and ttl >= 0:
                ttl_expires_at = timezone.now() + timedelta(seconds=ttl)
        # Get type-specific data for non-string types
        if redis_type and redis_type != "string":
            type_data = service.get_type_data(key, redis_type)
    elif create_mode:
        # In create mode, use the type from query param
        redis_type = create_type

    # Get cache metadata for displaying the raw key info
    cache_metadata = service.get_cache_metadata()
    raw_key = service.make_key(key)

    # In create mode, enable ops based on feature support (not key existence)
    can_operate = key_exists or create_mode

    context = admin.site.each_context(request)
    context.update(
        {
            "title": f"Add Key: {key}" if create_mode else f"Key: {key}",
            "cache_name": cache_name,
            "cache_config": cache_config,
            "key": key,
            "raw_key": raw_key,
            "cache_metadata": cache_metadata,
            "key_value": key_result,
            "key_exists": key_exists,
            "create_mode": create_mode,
            "value_display": value_display,
            "value_is_editable": value_is_editable,
            "redis_type": redis_type,
            "ttl": ttl,
            "ttl_expires_at": ttl_expires_at,
            "type_data": type_data,
            "query_supported": service.is_feature_supported("query"),
            "get_key_supported": service.is_feature_supported("get_key"),
            "delete_supported": service.is_feature_supported("delete_key") and key_exists,
            "backend_edit_supported": service.is_feature_supported("edit_key") and can_operate,
            "edit_supported": service.is_feature_supported("edit_key") and can_operate and value_is_editable,
            "set_ttl_supported": service.is_feature_supported("set_ttl") and key_exists,
            "list_ops_supported": service.is_feature_supported("list_ops") and can_operate,
            "set_ops_supported": service.is_feature_supported("set_ops") and can_operate,
            "hash_ops_supported": service.is_feature_supported("hash_ops") and can_operate,
            "zset_ops_supported": service.is_feature_supported("zset_ops") and can_operate,
            "stream_ops_supported": service.is_feature_supported("stream_ops") and can_operate,
            "help_active": help_active,
        },
    )
    return render(request, _template("key_detail.html"), context)


@staff_member_required
def key_add(request: HttpRequest, cache_name: str) -> HttpResponse:
    """View for adding a new cache key - collects key name and type, then redirects to key_detail."""
    help_active = _show_help(request, "key_add")
    service = get_cache_service(cache_name)
    cache_config = settings.CACHES.get(cache_name, {})

    if request.method == "POST":
        key_name = request.POST.get("key", "").strip()
        key_type = request.POST.get("type", "string").strip()

        if not key_name:
            messages.error(request, "Key name is required.")
        else:
            # Check if key already exists
            existing = service.get_key(key_name)
            if existing.get("exists", False):
                messages.warning(request, f"Key '{key_name}' already exists.")
                return redirect(reverse("django_cachex:key_detail", args=[cache_name, key_name]))
            # Redirect to key_detail in create mode
            from urllib.parse import urlencode

            base_url = reverse("django_cachex:key_detail", args=[cache_name, key_name])
            params = urlencode({"type": key_type})
            return redirect(f"{base_url}?{params}")

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
            "edit_supported": service.is_feature_supported("edit_key"),
            "list_ops_supported": service.is_feature_supported("list_ops"),
            "set_ops_supported": service.is_feature_supported("set_ops"),
            "hash_ops_supported": service.is_feature_supported("hash_ops"),
            "zset_ops_supported": service.is_feature_supported("zset_ops"),
            "help_active": help_active,
        },
    )
    return render(request, _template("key_add.html"), context)


@staff_member_required
def cache_info(request: HttpRequest, cache_name: str) -> HttpResponse:
    """Display detailed information about a cache instance."""
    import json

    from .models import Cache

    service = get_cache_service(cache_name)
    if service is None:
        messages.error(request, f"Cache '{cache_name}' not found.")
        return redirect(reverse("django_cachex:index"))

    # Get the cache object for support level
    cache_obj = Cache.get_by_name(cache_name)

    # Check if info is supported
    info_supported = service.is_feature_supported("info")
    info_data = None
    raw_info = None

    if info_supported:
        try:
            info_data = service.metadata()
            raw_info = service.info()
        except NotImplementedError:
            info_supported = False
        except Exception as e:  # noqa: BLE001
            messages.error(request, f"Error retrieving cache info: {e!s}")

    # Get slowlog (last 10 entries)
    slowlog_supported = service.is_feature_supported("slowlog")
    slowlog_data = None
    if slowlog_supported:
        try:
            slowlog_data = service.slowlog(10)
        except NotImplementedError:
            slowlog_supported = False
        except Exception as e:  # noqa: BLE001
            messages.error(request, f"Error retrieving slow log: {e!s}")

    # Convert raw_info to pretty-printed JSON for display
    raw_info_json = None
    if raw_info:
        raw_info_json = json.dumps(raw_info, indent=2, default=str)

    # Show help if requested
    help_active = _show_help(request, "cache_info")

    context = {
        **admin.site.each_context(request),
        "title": f"Cache Info: {cache_name}",
        "cache_name": cache_name,
        "cache_obj": cache_obj,
        "info_supported": info_supported,
        "info_data": info_data,
        "raw_info_json": raw_info_json,
        "slowlog_supported": slowlog_supported,
        "slowlog_data": slowlog_data,
        "help_active": help_active,
    }

    return render(request, _template("cache_info.html"), context)


@staff_member_required
def cache_slowlog(request: HttpRequest, cache_name: str) -> HttpResponse:
    """Display the slow query log for a cache instance."""
    service = get_cache_service(cache_name)
    if service is None:
        messages.error(request, f"Cache '{cache_name}' not found.")
        return redirect(reverse("django_cachex:index"))

    # Check if slowlog is supported
    slowlog_supported = service.is_feature_supported("slowlog")
    slowlog_data = None

    # Get count parameter
    count = 25
    count_str = request.GET.get("count", "25")
    with contextlib.suppress(ValueError):
        count = max(1, min(100, int(count_str)))

    if slowlog_supported:
        try:
            slowlog_data = service.slowlog(count)
        except NotImplementedError:
            slowlog_supported = False
        except Exception as e:  # noqa: BLE001
            messages.error(request, f"Error retrieving slow log: {e!s}")

    # Show help if requested
    help_active = _show_help(request, "cache_slowlog")

    context = {
        **admin.site.each_context(request),
        "title": f"Slow Log: {cache_name}",
        "cache_name": cache_name,
        "slowlog_supported": slowlog_supported,
        "slowlog_data": slowlog_data,
        "count": count,
        "help_active": help_active,
    }

    return render(request, _template("cache_slowlog.html"), context)
