"""
Key detail view for the django-cachex admin.
"""

from __future__ import annotations

import contextlib
import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from django.contrib import admin, messages
from django.core.exceptions import PermissionDenied
from django.shortcuts import redirect, render
from django.utils import timezone

from django_cachex.admin.helpers import get_cache, get_type_data
from django_cachex.admin.views.base import (
    ViewConfig,
    format_value_for_display,
    key_detail_url,
    key_list_url,
    show_help,
)
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


def _key_detail_view(  # noqa: C901, PLR0911, PLR0912, PLR0915
    request: HttpRequest,
    cache_name: str,
    key: str,
    config: ViewConfig,
) -> HttpResponse:
    """Display details of a specific cache key and handle mutations."""
    cache = get_cache(cache_name)

    # Handle POST requests (update or delete)
    if request.method == "POST":
        action = request.POST.get("action")

        # Permission gates: delete needs delete_key, all other mutations need change_key
        if action == "delete" and not request.user.has_perm("django_cachex.delete_key"):
            raise PermissionDenied
        if action and action != "delete" and not request.user.has_perm("django_cachex.change_key"):
            raise PermissionDenied

        if action == "delete":
            try:
                cache.delete(key)
                messages.success(request, "Key deleted successfully.")
                return redirect(key_list_url(cache_name))
            except Exception as e:  # noqa: BLE001
                messages.error(request, f"Error deleting key: {e!s}")

        elif action == "update":
            try:
                new_value: Any = request.POST.get("value", "")
                with contextlib.suppress(json.JSONDecodeError, ValueError):
                    new_value = json.loads(new_value)

                original_sha1 = request.POST.get("original_sha1", "").strip()
                if original_sha1 and hasattr(cache, "eval_script"):
                    from django_cachex.admin.cas import cas_update_string

                    cas_result = cas_update_string(cache, key, original_sha1, new_value)
                    if cas_result == 1:
                        messages.success(request, "Key updated successfully.")
                    elif cas_result == 0:
                        messages.warning(
                            request,
                            "Conflict: this value was modified since you loaded the page. Please refresh and try again.",
                        )
                    else:
                        messages.error(request, "Key no longer exists.")
                else:
                    cache.set(key, new_value, timeout=None)
                    messages.success(request, "Key updated successfully.")
                return redirect(key_detail_url(cache_name, key))
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))

        elif action == "set_ttl":
            try:
                ttl_str = request.POST.get("ttl_value", "").strip()
                if not ttl_str or ttl_str == "0":
                    # Empty or 0 TTL = persist (no expiry)
                    if cache.persist(key):
                        messages.success(request, "TTL removed. Key will not expire.")
                    else:
                        messages.error(request, "Key does not exist or has no TTL.")
                else:
                    ttl_int = int(ttl_str)
                    if ttl_int < 0:
                        messages.error(request, "TTL must be non-negative.")
                    elif cache.expire(key, ttl_int):
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
                if cache.persist(key):
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
                result = cache.lpop(key, count=count)
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
                result = cache.rpop(key, count=count)
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
                    new_len = cache.lpush(key, value)
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
                    new_len = cache.rpush(key, value)
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
                    removed = cache.lrem(key, count, value)
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
                cache.ltrim(key, start, stop)
                messages.success(request, f"List trimmed to range [{start}:{stop}].")
            except ValueError:
                messages.error(request, "Start and stop must be integers.")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        elif action == "lset":
            try:
                index = int(request.POST.get("index", "0"))
                value = request.POST.get("item_value", "").strip()
                original_sha1 = request.POST.get("original_sha1", "").strip()
                if original_sha1 and hasattr(cache, "eval_script"):
                    from django_cachex.admin.cas import cas_update_list_element

                    cas_result = cas_update_list_element(cache, key, index, original_sha1, value)
                    if cas_result == 1:
                        messages.success(request, f"Updated element at index {index}.")
                    elif cas_result == 0:
                        messages.warning(
                            request,
                            f"Conflict: element at index {index} was modified since you loaded the page. "
                            "Please refresh and try again.",
                        )
                    else:
                        messages.error(request, f"Index {index} no longer exists.")
                else:
                    cache.lset(key, index, value)
                    messages.success(request, f"Updated element at index {index}.")
            except ValueError:
                messages.error(request, "Index must be an integer.")
            except Exception as e:  # noqa: BLE001
                messages.error(request, str(e))
            return redirect(key_detail_url(cache_name, key))

        # Set operations
        elif action == "sadd":
            member = request.POST.get("member_value", "").strip()
            if member:
                try:
                    added = cache.sadd(key, member)
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
                    removed = cache.srem(key, member)
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
                    original_sha1 = request.POST.get("original_sha1", "").strip()
                    if original_sha1 and hasattr(cache, "eval_script"):
                        from django_cachex.admin.cas import cas_update_hash_field

                        cas_result = cas_update_hash_field(cache, key, field, original_sha1, value)
                        if cas_result == 1:
                            messages.success(request, f"Updated field '{field}'.")
                        elif cas_result == 0:
                            messages.warning(
                                request,
                                f"Conflict: field '{field}' was modified since you loaded the page. "
                                "Please refresh and try again.",
                            )
                        else:
                            messages.error(request, f"Field '{field}' no longer exists.")
                    else:
                        cache.hset(key, field, value)
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
                    removed = cache.hdel(key, field)
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
            if member and score_str:
                try:
                    score = float(score_str)
                    original_score = request.POST.get("original_score", "").strip()
                    if original_score and hasattr(cache, "eval_script"):
                        from django_cachex.admin.cas import cas_update_zset_score

                        cas_result = cas_update_zset_score(cache, key, member, original_score, score)
                        if cas_result == 1:
                            messages.success(request, f"Updated '{member}' score to {score}.")
                        elif cas_result == 0:
                            messages.warning(
                                request,
                                f"Conflict: score of '{member}' was modified since you loaded the page. "
                                "Please refresh and try again.",
                            )
                        else:
                            messages.error(request, f"Member '{member}' no longer exists.")
                    else:
                        # New member add (no original_score) or non-CAS backend
                        nx = request.POST.get("zadd_nx") == "on"
                        xx = request.POST.get("zadd_xx") == "on"
                        gt = request.POST.get("zadd_gt") == "on"
                        lt = request.POST.get("zadd_lt") == "on"
                        added = cache.zadd(key, {member: score}, nx=nx, xx=xx, gt=gt, lt=lt)
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
                    removed = cache.zrem(key, member)
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
                result = cache.spop(key, count=count)
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
                result = cache.zpopmin(key, count=pop_count)
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
                result = cache.zpopmax(key, count=pop_count)
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
                    entry_id = cache._cache.xadd(key, {field_name: field_value})
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
                    deleted = cache._cache.xdel(key, entry_id)
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
                    trimmed = cache._cache.xtrim(key, maxlen=maxlen)
                    messages.success(request, f"Trimmed {trimmed} entries.")
                except ValueError:
                    messages.error(request, "Max length must be a number.")
                except Exception as e:  # noqa: BLE001
                    messages.error(request, str(e))
            else:
                messages.error(request, "Max length is required.")
            return redirect(key_detail_url(cache_name, key))

    # GET request - display the key
    key_exists = cache.has_key(key)

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
            key_type = cache.type(key)
        with contextlib.suppress(Exception):
            ttl = cache.ttl(key)
            if ttl >= 0:
                ttl_expires_at = timezone.now() + timedelta(seconds=ttl)
        # Get type-specific data for non-string types
        if key_type and key_type != KeyType.STRING:
            type_data = get_type_data(cache, key, key_type)
    elif create_mode:
        # In create mode, use the type from query param
        key_type = create_type

    # Get value for string keys (cache.get() only works for strings)
    raw_value = None
    value_is_editable = True
    string_sha1 = None
    if key_exists and (not key_type or key_type == KeyType.STRING):
        raw_value = cache.get(key)
        if hasattr(cache, "eval_script"):
            with contextlib.suppress(Exception):
                from django_cachex.admin.cas import get_string_sha1

                string_sha1 = get_string_sha1(cache, key)

    if raw_value is not None:
        # Format value for display - JSON-serializable values are editable
        value_display, value_is_editable = format_value_for_display(raw_value)
    else:
        value_display = "null"

    # Show type-specific help message if requested
    help_key = f"key_detail_{key_type}" if isinstance(key_type, KeyType) else "key_detail"
    help_active = show_help(request, help_key, config.help_messages)

    # Get cache metadata for displaying the raw key info
    cache_metadata = {
        "key_prefix": cache.key_prefix,
        "version": cache.version,
    }
    raw_key = cache.make_key(key)

    # In create mode, enable ops based on feature support (not key existence)
    can_operate = key_exists or create_mode

    context = admin.site.each_context(request)
    context.update(
        {
            "title": f"Add Key: {key}" if create_mode else f"Key: {key}",
            "cache_name": cache_name,
            "key": key,
            "raw_key": raw_key,
            "cache_metadata": cache_metadata,
            "key_value": {"value": raw_value, "exists": key_exists},
            "key_exists": key_exists,
            "create_mode": create_mode,
            "value_display": value_display,
            "value_is_editable": value_is_editable,
            "string_sha1": string_sha1,
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
