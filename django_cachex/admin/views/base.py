"""Base utilities and configuration for cache admin views."""

import json
import logging
from typing import TYPE_CHECKING, Any

from django.contrib import messages
from django.urls import reverse

from django_cachex.admin.models import Key

if TYPE_CHECKING:
    from django.http import HttpRequest


logger = logging.getLogger(__name__)


# =============================================================================
# URL Helpers
# =============================================================================


def cache_list_url() -> str:
    return reverse("admin:django_cachex_cache_changelist")


def key_list_url(cache_name: str) -> str:
    return reverse("admin:django_cachex_key_changelist") + f"?cache={cache_name}"


def key_detail_url(cache_name: str, key: str) -> str:
    pk = Key.make_pk(cache_name, key)
    return reverse("admin:django_cachex_key_change", args=[pk])


def key_add_url(cache_name: str) -> str:
    return reverse("admin:django_cachex_key_add") + f"?cache={cache_name}"


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


def is_json_serializable(value: Any) -> bool:
    """Check if a value can be safely round-tripped through JSON without loss."""
    try:
        serialized = json.dumps(value)
        deserialized = json.loads(serialized)
        return deserialized == value
    except TypeError, ValueError, OverflowError:
        return False


def format_value_for_display(value: Any) -> tuple[str, bool]:
    """Format a value for display in the admin UI, returning (display_string, is_editable)."""
    if value is None:
        return "null", True

    if is_json_serializable(value):
        return json.dumps(value, indent=2, ensure_ascii=False), True
    return repr(value), False
