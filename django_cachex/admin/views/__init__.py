"""
Views for the django-cachex cache admin.

Provides cache inspection and management functionality.
These views can be configured with different template prefixes and URL builders
to support both the standard Django admin and alternative admin themes like Unfold.
"""

from .base import (
    ADMIN_CONFIG,
    HELP_MESSAGES,
    ViewConfig,
    cache_list_url,
    format_value_for_display,
    get_page_range,
    is_json_serializable,
    key_add_url,
    key_detail_url,
    key_list_url,
    parse_timeout,
    show_help,
)
from .cache import (
    _cache_detail_view,
    _help_view,
    _index_view,
    cache_detail,
    help_view,
    index,
)
from .key import (
    _key_add_view,
    _key_detail_view,
    _key_list_view,
    key_add,
    key_detail,
    key_list,
)

__all__ = [
    "ADMIN_CONFIG",
    "HELP_MESSAGES",
    "ViewConfig",
    "_cache_detail_view",
    "_help_view",
    "_index_view",
    "_key_add_view",
    "_key_detail_view",
    "_key_list_view",
    "cache_detail",
    "cache_list_url",
    "format_value_for_display",
    "get_page_range",
    "help_view",
    "index",
    "is_json_serializable",
    "key_add",
    "key_add_url",
    "key_detail",
    "key_detail_url",
    "key_list",
    "key_list_url",
    "parse_timeout",
    "show_help",
]
