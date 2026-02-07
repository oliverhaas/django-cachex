"""
Views for the django-cachex cache admin.

Provides cache inspection and management functionality.
These views can be configured with different template prefixes and URL builders
to support both the standard Django admin and alternative admin themes like Unfold.
"""

from .base import (
    ADMIN_CONFIG,
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
from .cache_detail import (
    _cache_detail_view,
    cache_detail,
)
from .cache_list import (
    _index_view,
    index,
)
from .key_add import (
    _key_add_view,
    key_add,
)
from .key_detail import (
    _key_detail_view,
    key_detail,
)
from .key_list import (
    _key_list_view,
    key_list,
)

__all__ = [
    "ADMIN_CONFIG",
    "ViewConfig",
    "_cache_detail_view",
    "_index_view",
    "_key_add_view",
    "_key_detail_view",
    "_key_list_view",
    "cache_detail",
    "cache_list_url",
    "format_value_for_display",
    "get_page_range",
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
