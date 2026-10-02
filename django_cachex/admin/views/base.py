"""Base utilities and configuration for cache admin views."""

from typing import TYPE_CHECKING

from django.contrib import messages
from django.contrib.admin.utils import quote
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.http import urlencode

from django_cachex.admin.helpers import can_view_caches, can_view_keys
from django_cachex.admin.models import Key

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


# =============================================================================
# URL Helpers
# =============================================================================


def cache_list_url() -> str:
    return reverse("admin:django_cachex_cache_changelist")


def key_list_url(cache_name: str) -> str:
    return reverse("admin:django_cachex_key_changelist") + "?" + urlencode({"cache": cache_name})


def key_detail_url(cache_name: str, key: str) -> str:
    # The admin's ``quote``/``unquote`` pair round-trips '/', ':', '%', and
    # '_XX' sequences losslessly, matching ``ChangeList.url_for_result``.
    pk = Key.make_pk(cache_name, key)
    return reverse("admin:django_cachex_key_change", args=[quote(pk)])


def list_hrefs(request: HttpRequest, cache_name: str) -> dict[str, str]:
    """Return the cache list and key list links for ``request``.

    A list the user cannot open gets "", so its breadcrumb is plain text and
    the page has no Back link to it, as in Django's admin.
    """
    return {
        "cache_list_href": cache_list_url() if can_view_caches(request) else "",
        "key_list_href": key_list_url(cache_name) if can_view_keys(request) else "",
    }


def cache_list_redirect(request: HttpRequest) -> HttpResponse:
    return redirect(cache_list_url() if can_view_caches(request) else reverse("admin:index"))


def key_list_redirect(request: HttpRequest, cache_name: str) -> HttpResponse:
    return redirect(key_list_url(cache_name) if can_view_keys(request) else reverse("admin:index"))


# =============================================================================
# View Configuration
# =============================================================================


_TEMPLATE_PREFIX = "admin/django_cachex"


class ViewConfig:
    """Per-admin context passed into cache admin views (help messages today)."""

    def __init__(self, help_messages: dict[str, str] | None = None) -> None:
        self.help_messages = help_messages or {}

    @staticmethod
    def template(name: str) -> str:
        """Resolve a template path under the cachex admin namespace."""
        return f"{_TEMPLATE_PREFIX}/{name}"


# =============================================================================
# Utility Functions
# =============================================================================


def show_help(
    request: HttpRequest,
    view_name: str,
    help_messages: dict[str, str],
) -> bool:
    """Show the help message when requested. Returns True if help was shown."""
    if request.GET.get("help"):
        help_text = help_messages.get(view_name, "")
        if help_text:
            messages.info(request, help_text)
        return True
    return False
