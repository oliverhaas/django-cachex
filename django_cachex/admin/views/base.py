"""
Base utilities and configuration for cache admin views.
"""

from __future__ import annotations

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
    """URL for the cache list page."""
    return reverse("admin:django_cachex_cache_changelist")


def key_list_url(cache_name: str) -> str:
    """URL for the key list page."""
    return reverse("admin:django_cachex_key_changelist") + f"?cache={cache_name}"


def key_detail_url(cache_name: str, key: str) -> str:
    """URL for the key detail page."""
    pk = Key.make_pk(cache_name, key)
    return reverse("admin:django_cachex_key_change", args=[pk])


def key_add_url(cache_name: str) -> str:
    """URL for the key add page."""
    return reverse("admin:django_cachex_key_add") + f"?cache={cache_name}"


# =============================================================================
# View Configuration
# =============================================================================


class ViewConfig:
    """Configuration for cache admin views.

    Holds template prefix and help messages for views. Supports both Django admin
    and alternative admin themes (like Unfold).

    Args:
        template_prefix: Base path for templates (e.g., "admin/django_cachex")
        template_overrides: Dict mapping canonical names to actual template paths.
            Use this when template names differ between themes.
        help_messages: Dict mapping view names to HTML help content.
    """

    def __init__(
        self,
        template_prefix: str = "admin/django_cachex",
        template_overrides: dict[str, str] | None = None,
        help_messages: dict[str, str] | None = None,
    ):
        self.template_prefix = template_prefix.rstrip("/")
        self.template_overrides = template_overrides or {}
        self.help_messages = help_messages or {}

    def template(self, name: str) -> str:
        """Get the full template path for a template name.

        Checks template_overrides first, then falls back to prefix + name.
        """
        if name in self.template_overrides:
            return f"{self.template_prefix}/{self.template_overrides[name]}"
        return f"{self.template_prefix}/{name}"


# Default configuration for standard Django admin
ADMIN_CONFIG = ViewConfig(template_prefix="admin/django_cachex")


# =============================================================================
# Utility Functions
# =============================================================================


def show_help(
    request: HttpRequest,
    view_name: str,
    help_messages: dict[str, str],
) -> bool:
    """Check if help was requested and show message if so. Returns True if help shown."""
    if request.GET.get("help"):
        help_text = help_messages.get(view_name, "")
        if help_text:
            messages.info(request, help_text)
        return True
    return False


def is_json_serializable(value: Any) -> bool:
    """Check if a value can be safely serialized to JSON and back without loss.

    This performs a round-trip check to ensure the value can be serialized
    to JSON and deserialized back to an equivalent Python object.

    Returns True for: None, bool, int, float, str, and dicts/lists containing only
    these types. Returns False for: bytes, datetime, custom objects, or any value
    where the round-trip changes the data.

    Args:
        value: The Python value to check.

    Returns:
        True if the value can round-trip through JSON without information loss.
    """
    try:
        serialized = json.dumps(value)
        deserialized = json.loads(serialized)
        return deserialized == value
    except (TypeError, ValueError, OverflowError):
        return False


def format_value_for_display(value: Any) -> tuple[str, bool]:
    """Format a value for display in the admin UI.

    Args:
        value: The Python value to format.

    Returns:
        A tuple of (display_string, is_editable).
        - JSON-serializable values are displayed as formatted JSON and are editable.
        - Non-JSON-serializable values are displayed using repr() and are read-only.
    """
    if value is None:
        return "null", True

    if is_json_serializable(value):
        return json.dumps(value, indent=2, ensure_ascii=False), True
    return repr(value), False
