"""Key add view for the django-cachex admin."""

from typing import TYPE_CHECKING
from urllib.parse import urlencode

from django.contrib import admin, messages
from django.shortcuts import redirect, render

from django_cachex.admin.helpers import CREATABLE_TYPES, CacheUnavailableError, get_cache
from django_cachex.admin.views.base import (
    ViewConfig,
    cache_list_url,
    key_detail_url,
    key_list_url,
    show_help,
)
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


_TYPE_LABELS = {
    KeyType.STRING: "String",
    KeyType.LIST: "List",
    KeyType.SET: "Set",
    KeyType.HASH: "Hash",
    KeyType.ZSET: "Sorted Set",
    KeyType.STREAM: "Stream",
}


def _key_add_view(
    request: HttpRequest,
    cache_name: str,
    config: ViewConfig,
) -> HttpResponse:
    """Collect a new key's name and type, then redirect to key_detail."""
    help_active = show_help(request, "key_add", config.help_messages)
    try:
        cache = get_cache(cache_name)
    except CacheUnavailableError as exc:
        messages.error(request, str(exc))
        return redirect(cache_list_url())

    if request.method == "POST":
        key_name = request.POST.get("key", "").strip()
        key_type = request.POST.get("type", KeyType.STRING).strip()

        if not key_name:
            messages.error(request, "Key name is required.")
        else:
            if cache.has_key(key_name):
                messages.warning(request, f"Key '{key_name}' already exists.")
                return redirect(key_detail_url(cache_name, key_name))
            base_url = key_detail_url(cache_name, key_name)
            params = urlencode({"type": key_type})
            separator = "&" if "?" in base_url else "?"
            return redirect(f"{base_url}{separator}{params}")

    prefill_key = request.GET.get("key", "")
    prefill_type = request.GET.get("type", KeyType.STRING)

    context = admin.site.each_context(request)
    context.update(
        {
            "title": f"Add key to '{cache_name}'",
            "cache_name": cache_name,
            "key_list_href": key_list_url(cache_name),
            "prefill_key": prefill_key,
            "prefill_type": prefill_type,
            "type_choices": [(t.value, _TYPE_LABELS[t]) for t in CREATABLE_TYPES],
            "help_active": help_active,
        },
    )
    return render(request, config.template("key/add_form.html"), context)
