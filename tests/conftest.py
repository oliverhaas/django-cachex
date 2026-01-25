"""Pytest configuration for django-cachex tests."""

import sys
from pathlib import Path

import pytest

from tests.fixtures import (
    cache,
    client_class,
    cluster_container,
    cluster_container_factory,
    compressors,
    native_parser,
    redis_container,
    redis_container_factory,
    redis_images,
    sentinel_container,
    sentinel_container_factory,
    sentinel_mode,
    serializers,
    settings,
)

# Re-export fixtures so pytest can discover them
__all__ = [
    "cache",
    "client_class",
    "cluster_container",
    "cluster_container_factory",
    "compressors",
    "native_parser",
    "redis_container",
    "redis_container_factory",
    "redis_images",
    "sentinel_container",
    "sentinel_container_factory",
    "sentinel_mode",
    "serializers",
    "settings",
    "skip_if_no_async_support",
]


def pytest_configure(config):
    """Add tests directory to Python path."""
    sys.path.insert(0, str(Path(__file__).absolute().parent))


@pytest.fixture(autouse=True)
def skip_if_no_async_support(request, cache):
    """Skip async tests if the cache client doesn't support async operations.

    This fixture auto-skips tests marked with @pytest.mark.asyncio when the
    cache client doesn't have async support (i.e., _async_pool_class is None).
    """
    # Only check for async tests
    if request.node.get_closest_marker("asyncio") is not None:
        # Check if the cache client supports async
        if hasattr(cache, "_cache") and cache._cache._async_pool_class is None:
            pytest.skip("Async not supported for this client type")
