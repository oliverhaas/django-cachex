"""
Key-related views for the django-cachex admin.
"""

from __future__ import annotations

import contextlib
import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

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
    format_value_for_display,
    key_detail_url,
    key_list_url,
    logger,
    show_help,
)

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


def _key_search_view(  # noqa: C901, PLR0912, PLR0915
    request: HttpRequest,
    cache_name: str,
    config: ViewConfig,
) -> HttpResponse:
    """View for searching/browsing cache keys.

    This is the internal implementation; use key_search() for the decorated admin view.
    """
    # Show help message if requested
    help_active = show_help(request, "key_search")

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
                if ttl is not None and ttl >= 0:
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

    return render(request, config.template("cache/key_search.html"), context)


@staff_member_required
def key_search(request: HttpRequest, cache_name: str) -> HttpResponse:
    """View for searching/browsing cache keys."""
    return _key_search_view(request, cache_name, ADMIN_CONFIG)


def _key_detail_view(  # noqa: C901, PLR0911, PLR0912, PLR0915
    request: HttpRequest,
    cache_name: str,
    key: str,
    config: ViewConfig,
) -> HttpResponse:
    """View for displaying the details of a specific cache key.

    This is the internal implementation; use key_detail() for the decorated admin view.
    """
    service = get_cache_service(cache_name)

    # Handle POST requests (update or delete)
    if request.method == "POST":
        action = request.POST.get("action")

        if action == "delete":
            try:
                service.delete(key)
                messages.success(request, "Key deleted successfully.")
                return redirect(key_list_url(cache_name))
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error deleting key: {e!s}")

        elif action == "update":
            try:
                new_value: Any = request.POST.get("value", "")
                with contextlib.suppress(json.JSONDecodeError, ValueError):
                    new_value = json.loads(new_value)

                # Update value only (TTL is handled separately via set_ttl action)
                service.set(key, new_value, timeout=None)
                messages.success(request, "Key updated successfully.")
                return redirect(key_detail_url(cache_name, key))
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))

        elif action == "set_ttl":
            try:
                ttl_str = request.POST.get("ttl_value", "").strip()
                if not ttl_str or ttl_str == "0":
                    # Empty or 0 TTL = persist (no expiry)
                    if service.persist(key):
                        messages.success(request, "TTL removed. Key will not expire.")
                    else:
                        messages.error(request, "Key does not exist or has no TTL.")
                else:
                    ttl_int = int(ttl_str)
                    if ttl_int < 0:
                        messages.error(request, "TTL must be non-negative.")
                    elif service.expire(key, ttl_int):
                        messages.success(request, f"TTL set to {ttl_int} seconds.")
                    else:
                        messages.error(request, "Key does not exist or TTL could not be set.")
                return redirect(key_detail_url(cache_name, key))
            except ValueError:
                messages.error(request, "Invalid TTL value. Must be a number.")
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error setting TTL: {e!s}")

        elif action == "persist":
            try:
                if service.persist(key):
                    messages.success(request, "TTL removed. Key will not expire.")
                else:
                    messages.error(request, "Key does not exist or has no TTL.")
                return redirect(key_detail_url(cache_name, key))
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error removing TTL: {e!s}")

        # List operations
        elif action == "lpop":
            count = 1
            count_str = request.POST.get("pop_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = max(1, int(count_str))
            try:
                result = service.lpop(key, count=count)
                if not result:
                    messages.error(request, "List is empty or key does not exist.")
                elif len(result) == 1:
                    messages.success(request, f"Popped: {result[0]}")
                else:
                    messages.success(request, f"Popped {len(result)} item(s): {result}")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        elif action == "rpop":
            count = 1
            count_str = request.POST.get("pop_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = max(1, int(count_str))
            try:
                result = service.rpop(key, count=count)
                if not result:
                    messages.error(request, "List is empty or key does not exist.")
                elif len(result) == 1:
                    messages.success(request, f"Popped: {result[0]}")
                else:
                    messages.success(request, f"Popped {len(result)} item(s): {result}")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        elif action == "lpush":
            value = request.POST.get("push_value", "").strip()
            if value:
                try:
                    new_len = service.lpush(key, value)
                    messages.success(request, f"Pushed to left. Length: {new_len}")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Value is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "rpush":
            value = request.POST.get("push_value", "").strip()
            if value:
                try:
                    new_len = service.rpush(key, value)
                    messages.success(request, f"Pushed to right. Length: {new_len}")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Value is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "lrem":
            value = request.POST.get("item_value", "").strip()
            count = 0  # Default: remove all occurrences
            count_str = request.POST.get("lrem_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = int(count_str)
            if value:
                try:
                    removed = service.lrem(key, value, count=count)
                    if removed > 0:
                        messages.success(request, f"Removed {removed} occurrence(s) of '{value}'.")
                    else:
                        messages.warning(request, f"'{value}' not found in list.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Value is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "ltrim":
            try:
                start = int(request.POST.get("trim_start", "0"))
                stop = int(request.POST.get("trim_stop", "-1"))
                service.ltrim(key, start, stop)
                messages.success(request, f"List trimmed to range [{start}:{stop}].")
            except ValueError:
                messages.error(request, "Start and stop must be integers.")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Set operations
        elif action == "sadd":
            member = request.POST.get("member_value", "").strip()
            if member:
                try:
                    added = service.sadd(key, member)
                    if added:
                        messages.success(request, f"Added '{member}' to set.")
                    else:
                        messages.info(request, f"'{member}' already exists in set.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Member is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "srem":
            member = request.POST.get("member", "").strip()
            if member:
                try:
                    removed = service.srem(key, member)
                    if removed:
                        messages.success(request, f"Removed '{member}' from set.")
                    else:
                        messages.warning(request, f"'{member}' not found in set.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Hash operations
        elif action == "hset":
            field = request.POST.get("field_name", "").strip()
            value = request.POST.get("field_value", "").strip()
            if field:
                try:
                    service.hset(key, field, value)
                    messages.success(request, f"Set field '{field}'.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Field name is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "hdel":
            field = request.POST.get("field", "").strip()
            if field:
                try:
                    removed = service.hdel(key, field)
                    if removed:
                        messages.success(request, f"Deleted field '{field}'.")
                    else:
                        messages.warning(request, f"Field '{field}' not found.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Sorted set operations
        elif action == "zadd":
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
                    added = service.zadd(key, {member: score}, nx=nx, xx=xx, gt=gt, lt=lt)
                    if added:
                        messages.success(request, f"Added '{member}' with score {score}.")
                    else:
                        messages.success(request, f"Updated '{member}' score to {score}.")
                except ValueError:
                    messages.error(request, "Score must be a number.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Member and score are required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "zrem":
            member = request.POST.get("member", "").strip()
            if member:
                try:
                    removed = service.zrem(key, member)
                    if removed:
                        messages.success(request, f"Removed '{member}' from sorted set.")
                    else:
                        messages.warning(request, f"'{member}' not found in sorted set.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Set pop operation
        elif action == "spop":
            count = 1
            count_str = request.POST.get("pop_count", "").strip()
            if count_str:
                with contextlib.suppress(ValueError):
                    count = max(1, int(count_str))
            try:
                result = service.spop(key, count=count)
                if not result:
                    messages.error(request, "Set is empty or key does not exist.")
                elif len(result) == 1:
                    messages.success(request, f"Popped: {result[0]}")
                else:
                    messages.success(request, f"Popped {len(result)} member(s): {result}")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Sorted set pop operations
        elif action == "zpopmin":
            pop_count = int(request.POST.get("pop_count", 1) or 1)
            try:
                result = service.zpopmin(key, count=pop_count)
                if not result:
                    messages.error(request, "Sorted set is empty or key does not exist.")
                elif len(result) == 1:
                    member, score = result[0]
                    messages.success(request, f"Popped: {member} (score: {score})")
                else:
                    members = [f"{m} ({s})" for m, s in result]
                    messages.success(request, f"Popped {len(result)} member(s): {', '.join(members)}")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        elif action == "zpopmax":
            pop_count = int(request.POST.get("pop_count", 1) or 1)
            try:
                result = service.zpopmax(key, count=pop_count)
                if not result:
                    messages.error(request, "Sorted set is empty or key does not exist.")
                elif len(result) == 1:
                    member, score = result[0]
                    messages.success(request, f"Popped: {member} (score: {score})")
                else:
                    members = [f"{m} ({s})" for m, s in result]
                    messages.success(request, f"Popped {len(result)} member(s): {', '.join(members)}")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Stream operations
        elif action == "xadd":
            field_name = request.POST.get("field_name", "").strip()
            field_value = request.POST.get("field_value", "").strip()
            if field_name and field_value:
                try:
                    entry_id = service.xadd(key, {field_name: field_value})
                    messages.success(request, f"Added entry {entry_id}.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Field name and value are required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "xdel":
            entry_id = request.POST.get("entry_id", "").strip()
            if entry_id:
                try:
                    deleted = service.xdel(key, entry_id)
                    if deleted:
                        messages.success(request, f"Deleted entry {entry_id}.")
                    else:
                        messages.warning(request, f"Entry {entry_id} not found.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Entry ID is required.")
            return redirect(key_detail_url(cache_name, key))

        elif action == "xtrim":
            maxlen_str = request.POST.get("maxlen", "").strip()
            if maxlen_str:
                try:
                    maxlen = int(maxlen_str)
                    trimmed = service.xtrim(key, maxlen)
                    messages.success(request, f"Trimmed {trimmed} entries.")
                except ValueError:
                    messages.error(request, "Max length must be a number.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Max length is required.")
            return redirect(key_detail_url(cache_name, key))

    # GET request - display the key
    key_result = service.get(key)

    raw_value = key_result.get("value")
    value_is_editable = True

    if raw_value is not None:
        # Format value for display - JSON-serializable values are editable
        value_display, value_is_editable = format_value_for_display(raw_value)
    else:
        value_display = "null"

    cache_config = settings.CACHES.get(cache_name, {})
    key_exists = key_result.get("exists", False)

    # Check for create mode (type param provided for non-existing key)
    create_mode = False
    create_type = request.GET.get("type", "").strip()
    if not key_exists:
        if create_type:
            # Create mode: key doesn't exist but type is specified
            create_mode = True
        else:
            messages.error(request, f"Key '{key}' does not exist in cache '{cache_name}'.")
            return redirect(key_list_url(cache_name))

    # Get TTL and type for the key
    key_type = None
    ttl = None
    ttl_expires_at = None
    type_data: dict[str, Any] = {}
    if key_exists:
        with contextlib.suppress(Exception):
            key_type = service.type(key)
        with contextlib.suppress(Exception):
            ttl = service.ttl(key)
            if ttl is not None and ttl >= 0:
                ttl_expires_at = timezone.now() + timedelta(seconds=ttl)
        # Get type-specific data for non-string types
        if key_type and key_type != "string":
            type_data = service.get_type_data(key, key_type)
    elif create_mode:
        # In create mode, use the type from query param
        key_type = create_type

    # Show type-specific help message if requested
    help_key = (
        f"key_detail_{key_type}" if key_type in ("string", "list", "set", "hash", "zset", "stream") else "key_detail"
    )
    help_active = show_help(request, help_key)

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
            "key_type": key_type,
            "ttl": ttl,
            "ttl_expires_at": ttl_expires_at,
            "type_data": type_data,
            "delete_supported": key_exists,
            "backend_edit_supported": can_operate,
            "edit_supported": can_operate and value_is_editable,
            "set_ttl_supported": key_exists,
            "list_ops_supported": can_operate,
            "set_ops_supported": can_operate,
            "hash_ops_supported": can_operate,
            "zset_ops_supported": can_operate,
            "stream_ops_supported": can_operate,
            "help_active": help_active,
        },
    )
    return render(request, config.template("key/change_form.html"), context)


@staff_member_required
def key_detail(request: HttpRequest, cache_name: str, key: str) -> HttpResponse:
    """View for displaying the details of a specific cache key."""
    return _key_detail_view(request, cache_name, key, ADMIN_CONFIG)


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
    return render(request, config.template("cache/key_add.html"), context)


@staff_member_required
def key_add(request: HttpRequest, cache_name: str) -> HttpResponse:
    """View for adding a new cache key - collects key name and type, then redirects to key_detail."""
    return _key_add_view(request, cache_name, ADMIN_CONFIG)
