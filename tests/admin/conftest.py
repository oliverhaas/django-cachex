"""Pytest configuration for admin view tests."""

from typing import TYPE_CHECKING, cast

import pytest
from django.contrib.auth.models import User
from django.test import Client, override_settings

if TYPE_CHECKING:
    from django_cachex.cache import RespCache
    from tests.fixtures.containers import RedisContainerInfo


def get_cache_config(host: str, port: int) -> dict:
    """Build a simple CACHES config for tests."""
    return {
        "default": {
            "BACKEND": "django_cachex.cache.ValkeyCache",
            "LOCATION": f"redis://{host}:{port}?db=15",
        },
        "local": {
            "BACKEND": "django_cachex.cache.LocMemCache",
            "LOCATION": "admin-test-local",
        },
    }


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
def stream_alias(test_cache):
    """Add a StreamCache alias broadcasting over the default cache.

    StreamCache has its own TTL surface (``pttl`` returns -1 for no expiry, no
    ``pexpire`` at all), which the admin views have to handle.
    """
    import uuid

    from django.conf import settings
    from django.core.cache import caches
    from django.core.cache.backends.locmem import _caches, _expire_info, _locks

    location = f"admin-test-stream-{uuid.uuid4().hex[:8]}"
    config = {
        **settings.CACHES,
        "stream": {
            "BACKEND": "django_cachex.cache.StreamCache",
            "LOCATION": location,
            "OPTIONS": {
                "transport": "default",
                "stream_key": f"admin:sync:{uuid.uuid4().hex[:8]}",
                "block_timeout": 100,
            },
        },
    }
    with override_settings(CACHES=config):
        caches.close_all()
        yield "stream"
        caches["stream"].shutdown()
        for registry in (_caches, _expire_info, _locks):
            registry.pop(location, None)
    caches.close_all()


@pytest.fixture
def test_cache(db, redis_container: RedisContainerInfo):
    """Provide a cache backend for testing admin views."""
    from django.core.cache import caches

    cache_config = get_cache_config(redis_container.host, redis_container.port)

    with override_settings(CACHES=cache_config):
        caches.close_all()

        cache = cast("RespCache", caches["default"])
        cache.flush_db()
        yield cache
        cache.flush_db()
