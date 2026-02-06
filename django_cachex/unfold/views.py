"""
Views for the django-cachex unfold-styled cache admin.

This module provides the same functionality as django_cachex.admin.views
but renders with unfold-optimized templates. All view logic is shared with
the admin module; only the template prefix and URL builders differ.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.contrib.admin.views.decorators import staff_member_required

from django_cachex.admin.views import (
    StandaloneUrlBuilder,
    ViewConfig,
    _help_view,
    _index_view,
    _key_add_view,
    _key_detail_view,
    _key_search_view,
)

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


# Configuration for unfold-themed views
# Template overrides map admin template names to unfold equivalents
UNFOLD_CONFIG = ViewConfig(
    template_prefix="unfold/django_cachex",
    url_builder=StandaloneUrlBuilder(namespace="django_cachex"),
    template_overrides={
        # Unfold uses cache/ for all templates, admin uses key/ for key details
        "key/change_form.html": "cache/key_detail.html",
    },
)


@staff_member_required
def index(request: HttpRequest) -> HttpResponse:
    """Display all configured cache instances with their capabilities."""
    return _index_view(request, UNFOLD_CONFIG)


@staff_member_required
def help_view(request: HttpRequest) -> HttpResponse:
    """Display help information about the cache admin."""
    return _help_view(request, UNFOLD_CONFIG)


@staff_member_required
def key_search(request: HttpRequest, cache_name: str) -> HttpResponse:
    """View for searching/browsing cache keys."""
    return _key_search_view(request, cache_name, UNFOLD_CONFIG)


@staff_member_required
def key_detail(request: HttpRequest, cache_name: str, key: str) -> HttpResponse:
    """View for displaying the details of a specific cache key."""
    return _key_detail_view(request, cache_name, key, UNFOLD_CONFIG)


@staff_member_required
def key_add(request: HttpRequest, cache_name: str) -> HttpResponse:
    """View for adding a new cache key."""
    return _key_add_view(request, cache_name, UNFOLD_CONFIG)
