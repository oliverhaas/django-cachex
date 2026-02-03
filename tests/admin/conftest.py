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


def get_cache_config(host: str, port: int) -> dict:
    """Build a simple CACHES config for tests."""
    return {
        "default": {
            "BACKEND": "django_cachex.cache.ValkeyCache",
            "LOCATION": f"redis://{host}:{port}?db=15",
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
def test_cache(db, redis_container):
    """Provide a cache instance for testing admin views.

    Uses override_settings to configure the cache for the duration of the test.
    Depends on redis_container to ensure a Redis server is available.
    """
    from django.core.cache import caches

    # redis_container sets REDIS_HOST and REDIS_PORT env vars
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    cache_config = get_cache_config(host, port)

    with override_settings(CACHES=cache_config):
        # Close all caches to force recreation with new settings
        caches.close_all()

        cache = cast("KeyValueCache", caches["default"])
        cache.clear()
        yield cache
        cache.clear()
