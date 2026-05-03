"""
Key detail view for the django-cachex admin.
"""

import contextlib
import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from django.contrib import admin, messages
from django.core.exceptions import PermissionDenied
from django.shortcuts import redirect, render
from django.utils import timezone

from django_cachex.admin.cas import (
    cas_update_hash_field,
    cas_update_list_element,
    cas_update_string,
    cas_update_zset_score,
    get_string_sha1,
)
from django_cachex.admin.helpers import get_cache, get_type_data
from django_cachex.admin.views.base import (
    ViewConfig,
    format_value_for_display,
    key_detail_url,
    key_list_url,
    show_help,
)
from django_cachex.exceptions import CompressorError, SerializerError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Callable

    from django.http import HttpRequest, HttpResponse


# -- Small helpers --------------------------------------------------------------


def _redirect_to_key(cache_name: str, key: str, page: int) -> HttpResponse:
    """Return a redirect to the key detail page, preserving pagination."""
    url = key_detail_url(cache_name, key)
    if page > 1:
        url += f"?page={page}"
    return redirect(url)


def _parse_count(request: HttpRequest, field: str, *, default: int = 1, min_value: int = 1) -> int:
    """Parse an integer count from a POST field, defaulting on invalid input."""
    raw = request.POST.get(field, "").strip()
    if not raw:
        return default
    try:
        return max(min_value, int(raw))
    except ValueError:
        return default


def _parse_json_or_str(value: str) -> Any:
    """Try to interpret a string as JSON, falling back to the raw string."""
    with contextlib.suppress(json.JSONDecodeError, ValueError):
        return json.loads(value)
    return value


def _apply_cas_result(
    request: HttpRequest,
    cas_result: int,
    *,
    success: str,
    conflict: str,
    missing: str,
) -> None:
    """Translate a CAS script result (1=ok, 0=conflict, -1=missing) into a message."""
    if cas_result == 1:
        messages.success(request, success)
    elif cas_result == 0:
        messages.warning(request, conflict)
    else:
        messages.error(request, missing)


def _report_pop(request: HttpRequest, result: Any, *, on_empty: str, kind: str) -> None:
    """Render a success/error message for a pop-style operation."""
    if not result:
        messages.error(request, on_empty)
    elif len(result) == 1:
        messages.success(request, f"Popped: {result[0]}")
    else:
        messages.success(request, f"Popped {len(result)} {kind}: {result}")


# -- POST action handlers -------------------------------------------------------
#
# Each handler returns ``HttpResponse`` to redirect, or ``None`` to fall through
# to the GET-style render (used when a mutation fails and we want the user to
# see the current state of the page with the error message).


def _handle_delete(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse | None:
    del page
    try:
        cache.delete(key)
        messages.success(request, "Key deleted successfully.")
        return redirect(key_list_url(cache_name))
    except Exception as e:  # noqa: BLE001
        messages.error(request, f"Error deleting key: {e!s}")
        return None


def _handle_update(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse | None:
    try:
        new_value = _parse_json_or_str(request.POST.get("value", ""))
        original_sha1 = request.POST.get("original_sha1", "").strip()
        if original_sha1 and hasattr(cache, "eval_script"):
            cas_result = cas_update_string(cache, key, original_sha1, new_value)
            _apply_cas_result(
                request,
                cas_result,
                success="Key updated successfully.",
                conflict="Conflict: this value was modified since you loaded the page. Please refresh and try again.",
                missing="Key no longer exists.",
            )
        else:
            existing_ttl = cache.ttl(key)
            timeout = existing_ttl if existing_ttl and existing_ttl > 0 else None
            cache.set(key, new_value, timeout=timeout)
            messages.success(request, "Key updated successfully.")
        return _redirect_to_key(cache_name, key, page)
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
        return None


def _handle_set_ttl(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse | None:
    try:
        ttl_str = request.POST.get("ttl_value", "").strip()
        if not ttl_str or ttl_str == "0":
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
        return _redirect_to_key(cache_name, key, page)
    except ValueError:
        messages.error(request, "Invalid TTL value. Must be a number.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, f"Error setting TTL: {e!s}")
    return None


def _handle_persist(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse | None:
    try:
        if cache.persist(key):
            messages.success(request, "TTL removed. Key will not expire.")
        else:
            messages.error(request, "Key does not exist or has no TTL.")
        return _redirect_to_key(cache_name, key, page)
    except Exception as e:  # noqa: BLE001
        messages.error(request, f"Error removing TTL: {e!s}")
        return None


def _handle_lpop(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    count = _parse_count(request, "pop_count")
    try:
        result = cache.lpop(key, count=count)
        _report_pop(request, result, on_empty="List is empty or key does not exist.", kind="item(s)")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_rpop(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    count = _parse_count(request, "pop_count")
    try:
        result = cache.rpop(key, count=count)
        _report_pop(request, result, on_empty="List is empty or key does not exist.", kind="item(s)")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_lpush(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    raw = request.POST.get("value", "").strip()
    if not raw:
        messages.error(request, "Value is required.")
        return _redirect_to_key(cache_name, key, page)
    try:
        new_len = cache.lpush(key, _parse_json_or_str(raw))
        messages.success(request, f"Pushed to left. Length: {new_len}")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_rpush(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    raw = request.POST.get("value", "").strip()
    if not raw:
        messages.error(request, "Value is required.")
        return _redirect_to_key(cache_name, key, page)
    try:
        new_len = cache.rpush(key, _parse_json_or_str(raw))
        messages.success(request, f"Pushed to right. Length: {new_len}")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_lrem(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    raw = request.POST.get("value", "").strip()
    value = _parse_json_or_str(raw)
    count = 0  # 0 = remove all occurrences
    count_str = request.POST.get("lrem_count", "").strip()
    if count_str:
        with contextlib.suppress(ValueError):
            count = int(count_str)
    if not value:
        messages.error(request, "Value is required.")
        return _redirect_to_key(cache_name, key, page)
    try:
        removed = cache.lrem(key, count, value)
        if removed > 0:
            messages.success(request, f"Removed {removed} occurrence(s) of '{value}'.")
        else:
            messages.warning(request, f"'{value}' not found in list.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_ltrim(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    try:
        start = int(request.POST.get("trim_start", "0"))
        stop = int(request.POST.get("trim_stop", "-1"))
        cache.ltrim(key, start, stop)
        messages.success(request, f"List trimmed to range [{start}:{stop}].")
    except ValueError:
        messages.error(request, "Start and stop must be integers.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_lset(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    try:
        index = int(request.POST.get("index", "0"))
        value = _parse_json_or_str(request.POST.get("value", "").strip())
        original_sha1 = request.POST.get("original_sha1", "").strip()
        if original_sha1 and hasattr(cache, "eval_script"):
            cas_result = cas_update_list_element(cache, key, index, original_sha1, value)
            _apply_cas_result(
                request,
                cas_result,
                success=f"Updated element at index {index}.",
                conflict=(
                    f"Conflict: element at index {index} was modified since you loaded the page. "
                    "Please refresh and try again."
                ),
                missing=f"Index {index} no longer exists.",
            )
        else:
            cache.lset(key, index, value)
            messages.success(request, f"Updated element at index {index}.")
    except ValueError:
        messages.error(request, "Index must be an integer.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_sadd(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    raw = request.POST.get("member", "").strip()
    if not raw:
        messages.error(request, "Member is required.")
        return _redirect_to_key(cache_name, key, page)
    try:
        member = _parse_json_or_str(raw)
        if cache.sadd(key, member):
            messages.success(request, f"Added '{member}' to set.")
        else:
            messages.info(request, f"'{member}' already exists in set.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_srem(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    raw = request.POST.get("member", "").strip()
    if not raw:
        return _redirect_to_key(cache_name, key, page)
    try:
        member = _parse_json_or_str(raw)
        if cache.srem(key, member):
            messages.success(request, f"Removed '{member}' from set.")
        else:
            messages.warning(request, f"'{member}' not found in set.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_spop(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    count = _parse_count(request, "pop_count")
    try:
        result = cache.spop(key, count=count)
        _report_pop(request, result, on_empty="Set is empty or key does not exist.", kind="member(s)")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_hset(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    field = request.POST.get("field", "").strip()
    value = _parse_json_or_str(request.POST.get("field_value", "").strip())
    if not field:
        messages.error(request, "Field name is required.")
        return _redirect_to_key(cache_name, key, page)
    try:
        original_sha1 = request.POST.get("original_sha1", "").strip()
        if original_sha1 and hasattr(cache, "eval_script"):
            cas_result = cas_update_hash_field(cache, key, field, original_sha1, value)
            _apply_cas_result(
                request,
                cas_result,
                success=f"Updated field '{field}'.",
                conflict=(
                    f"Conflict: field '{field}' was modified since you loaded the page. Please refresh and try again."
                ),
                missing=f"Field '{field}' no longer exists.",
            )
        else:
            cache.hset(key, field, value)
            messages.success(request, f"Set field '{field}'.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_hdel(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    field = request.POST.get("field", "").strip()
    if not field:
        return _redirect_to_key(cache_name, key, page)
    try:
        if cache.hdel(key, field):
            messages.success(request, f"Deleted field '{field}'.")
        else:
            messages.warning(request, f"Field '{field}' not found.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_zadd(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    raw_member = request.POST.get("member", "").strip()
    score_str = request.POST.get("score_value", "").strip()
    if not (raw_member and score_str):
        messages.error(request, "Member and score are required.")
        return _redirect_to_key(cache_name, key, page)
    try:
        member = _parse_json_or_str(raw_member)
        score = float(score_str)
        original_score = request.POST.get("original_score", "").strip()
        if original_score and hasattr(cache, "eval_script"):
            cas_result = cas_update_zset_score(cache, key, member, original_score, score)
            _apply_cas_result(
                request,
                cas_result,
                success=f"Updated '{member}' score to {score}.",
                conflict=(
                    f"Conflict: score of '{member}' was modified since you loaded the page. "
                    "Please refresh and try again."
                ),
                missing=f"Member '{member}' no longer exists.",
            )
        else:
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
    return _redirect_to_key(cache_name, key, page)


def _handle_zrem(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    raw = request.POST.get("member", "").strip()
    if not raw:
        return _redirect_to_key(cache_name, key, page)
    try:
        member = _parse_json_or_str(raw)
        if cache.zrem(key, member):
            messages.success(request, f"Removed '{member}' from sorted set.")
        else:
            messages.warning(request, f"'{member}' not found in sorted set.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_zpop(
    request: HttpRequest,
    cache: Any,
    cache_name: str,
    key: str,
    page: int,
    *,
    op: str,
) -> HttpResponse:
    count = _parse_count(request, "pop_count")
    try:
        result = getattr(cache, op)(key, count=count)
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
    return _redirect_to_key(cache_name, key, page)


def _handle_zpopmin(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    return _handle_zpop(request, cache, cache_name, key, page, op="zpopmin")


def _handle_zpopmax(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    return _handle_zpop(request, cache, cache_name, key, page, op="zpopmax")


def _handle_xadd(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    field = request.POST.get("field", "").strip()
    field_value = request.POST.get("field_value", "").strip()
    if not (field and field_value):
        messages.error(request, "Field name and value are required.")
        return _redirect_to_key(cache_name, key, page)
    try:
        entry_id = cache.xadd(key, {field: field_value})
        messages.success(request, f"Added entry {entry_id}.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_xdel(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    entry_id = request.POST.get("entry_id", "").strip()
    if not entry_id:
        messages.error(request, "Entry ID is required.")
        return _redirect_to_key(cache_name, key, page)
    try:
        if cache.xdel(key, entry_id):
            messages.success(request, f"Deleted entry {entry_id}.")
        else:
            messages.warning(request, f"Entry {entry_id} not found.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


def _handle_xtrim(request: HttpRequest, cache: Any, cache_name: str, key: str, page: int) -> HttpResponse:
    maxlen_str = request.POST.get("maxlen", "").strip()
    if not maxlen_str:
        messages.error(request, "Max length is required.")
        return _redirect_to_key(cache_name, key, page)
    try:
        maxlen = int(maxlen_str)
        trimmed = cache.xtrim(key, maxlen=maxlen)
        messages.success(request, f"Trimmed {trimmed} entries.")
    except ValueError:
        messages.error(request, "Max length must be a number.")
    except Exception as e:  # noqa: BLE001
        messages.error(request, str(e))
    return _redirect_to_key(cache_name, key, page)


# Action → handler dispatch table. Adding a new POST action means adding a
# handler above and a single entry here.
_POST_HANDLERS: dict[str, Callable[[HttpRequest, Any, str, str, int], HttpResponse | None]] = {
    "delete": _handle_delete,
    "update": _handle_update,
    "set_ttl": _handle_set_ttl,
    "persist": _handle_persist,
    # List
    "lpop": _handle_lpop,
    "rpop": _handle_rpop,
    "lpush": _handle_lpush,
    "rpush": _handle_rpush,
    "lrem": _handle_lrem,
    "ltrim": _handle_ltrim,
    "lset": _handle_lset,
    # Set
    "sadd": _handle_sadd,
    "srem": _handle_srem,
    "spop": _handle_spop,
    # Hash
    "hset": _handle_hset,
    "hdel": _handle_hdel,
    # Sorted set
    "zadd": _handle_zadd,
    "zrem": _handle_zrem,
    "zpopmin": _handle_zpopmin,
    "zpopmax": _handle_zpopmax,
    # Stream
    "xadd": _handle_xadd,
    "xdel": _handle_xdel,
    "xtrim": _handle_xtrim,
}


def _check_post_permission(request: HttpRequest, action: str | None) -> None:
    """Raise PermissionDenied if the user lacks the right to perform ``action``."""
    if action == "delete" and not request.user.has_perm("django_cachex.delete_key"):  # ty: ignore[unresolved-attribute]
        raise PermissionDenied
    if action and action != "delete" and not request.user.has_perm("django_cachex.change_key"):  # ty: ignore[unresolved-attribute]
        raise PermissionDenied


def _key_detail_view(  # noqa: C901, PLR0912, PLR0915
    request: HttpRequest,
    cache_name: str,
    key: str,
    config: ViewConfig,
) -> HttpResponse:
    """Display details of a specific cache key and handle mutations."""
    cache = get_cache(cache_name)

    # Read pagination state (query string is preserved on POST to current URL)
    try:
        page = max(1, int(request.GET.get("page", 1)))
    except ValueError, TypeError:
        page = 1

    if request.method == "POST":
        action = request.POST.get("action")
        _check_post_permission(request, action)
        handler = _POST_HANDLERS.get(action) if action else None
        if handler is not None:
            response = handler(request, cache, cache_name, key, page)
            if response is not None:
                return response

    # GET request - display the key
    key_exists = cache.has_key(key)

    # Check for create mode (type param provided for non-existing key)
    create_mode = False
    create_type = request.GET.get("type", "").strip()
    if not key_exists:
        if create_type:
            # Create mode: key doesn't exist but type is specified
            create_mode = True
            messages.warning(
                request,
                "This key does not exist yet. Use the operations below to add your first item and create the key.",
            )
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
            if ttl is not None and ttl >= 0:
                ttl_expires_at = timezone.now() + timedelta(seconds=ttl)
        # Get type-specific data for non-string types
        if key_type and key_type != KeyType.STRING:
            type_data = get_type_data(cache, key, key_type, page=page)
    elif create_mode:
        # In create mode, use the type from query param
        key_type = create_type

    # Get value for string keys (cache.get() only works for strings).
    # Decode failures (compressor / serializer mismatch on stale data) must NOT
    # crash the page — the user still needs to be able to delete the broken key.
    raw_value = None
    value_is_editable = True
    value_decode_error: str | None = None
    string_sha1 = None
    if key_exists and (not key_type or key_type == KeyType.STRING):
        try:
            raw_value = cache.get(key)
        except (CompressorError, SerializerError) as exc:
            value_decode_error = str(exc) or exc.__class__.__name__
            value_is_editable = False
            messages.warning(
                request,
                f"Value cannot be decoded ({exc.__class__.__name__}: {exc}). "
                "The key was likely written with a different compressor or serializer. "
                "You can still delete the key.",
            )
        else:
            if hasattr(cache, "eval_script"):
                with contextlib.suppress(Exception):
                    string_sha1 = get_string_sha1(cache, key)

    if value_decode_error is not None:
        value_display = f"<value cannot be decoded: {value_decode_error}>"
    elif raw_value is not None:
        # Format value for display - JSON-serializable values are editable
        value_display, value_is_editable = format_value_for_display(raw_value)
    else:
        value_display = "null"

    # Show type-specific help message if requested. ``key_type`` is either a
    # ``KeyType`` enum (existing key, set by ``cache.type()``) or the raw
    # string from the ``?type=`` query param (create mode); both interpolate
    # the same way since ``KeyType`` is a ``StrEnum``.
    help_key = f"key_detail_{key_type}" if key_type else "key_detail"
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
