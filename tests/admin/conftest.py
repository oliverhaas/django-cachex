"""Pytest configuration for admin view tests.

This conftest provides simple, non-parametrized fixtures for admin view testing.
We override the parametrized fixtures from the parent conftest to avoid
running admin tests with every cache backend combination.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, cast

import pytest
from django.contrib.auth.models import User
from django.test import Client, override_settings

if TYPE_CHECKING:
    from django_cachex.cache import KeyValueCache

# Get Redis host/port from environment or use defaults
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


def get_cache_config() -> dict:
    """Build a simple CACHES config for tests."""
    return {
        "default": {
            "BACKEND": "django_cachex.cache.ValkeyCache",
            "LOCATION": f"redis://{REDIS_HOST}:{REDIS_PORT}?db=15",
        },
    }


# Override parametrized fixtures from parent conftest with simple versions
@pytest.fixture
def client_class():
    """Non-parametrized client_class fixture."""
    return "default"


@pytest.fixture
def sentinel_mode():
    """Non-parametrized sentinel_mode fixture."""
    return False


@pytest.fixture
def admin_user(db):
    """Create a superuser for admin access."""
    return User.objects.create_superuser(
        username="admin",
        email="admin@example.com",
        password="password",  # noqa: S106
    )


@pytest.fixture
def admin_client(admin_user) -> Client:
    """Create a logged-in admin client."""
    client = Client()
    client.force_login(admin_user)
    return client


@pytest.fixture
def test_cache(db):
    """Provide a cache instance for testing admin views.

    Uses override_settings to configure the cache for the duration of the test.
    """
    from django.core.cache import caches

    cache_config = get_cache_config()

    with override_settings(CACHES=cache_config):
        # Close all caches to force recreation with new settings
        caches.close_all()

        cache = cast("KeyValueCache", caches["default"])
        cache.clear()
        yield cache
        cache.clear()
