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
    """Configuration for cache admin views, supporting Django admin and alternative themes."""

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
        """Get the full template path for a template name."""
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
    """Check if a value can be safely round-tripped through JSON without loss."""
    try:
        serialized = json.dumps(value)
        deserialized = json.loads(serialized)
        return deserialized == value
    except (TypeError, ValueError, OverflowError):
        return False


def format_value_for_display(value: Any) -> tuple[str, bool]:
    """Format a value for display in the admin UI, returning (display_string, is_editable)."""
    if value is None:
        return "null", True

    if is_json_serializable(value):
        return json.dumps(value, indent=2, ensure_ascii=False), True
    return repr(value), False
