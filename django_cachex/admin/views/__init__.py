"""Views for the django-cachex cache admin."""

from .base import ViewConfig
from .cache_detail import cache_detail_view
from .key_add import key_add_view
from .key_detail import key_detail_view

__all__ = [
    "ViewConfig",
    "cache_detail_view",
    "key_add_view",
    "key_detail_view",
]
