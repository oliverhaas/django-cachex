"""Tests for connection string/URL handling."""

import pytest
from django.core.cache import caches
from django.test import override_settings

# Test that various URL formats work with the cache backend
# These tests verify that URLs are properly handled when creating connections


@pytest.mark.parametrize(
    "url_path",
    [
        "/0",
        "/1",
        "/2",
    ],
)
def test_db_in_url(redis_container, url_path):
    """Test that db number in URL path works."""
    host = redis_container.host
    port = redis_container.port

    caches_config = {
        "default": {
            "BACKEND": "django_cachex.cache.RedisCache",
            "LOCATION": f"redis://{host}:{port}{url_path}",
        },
    }

    with override_settings(CACHES=caches_config):
        cache = caches["default"]
        cache.set("test_db_url", "value")
        assert cache.get("test_db_url") == "value"
        cache.delete("test_db_url")


def test_db_in_query_string(redis_container):
    """Test that db number in query string works."""
    host = redis_container.host
    port = redis_container.port

    caches_config = {
        "default": {
            "BACKEND": "django_cachex.cache.RedisCache",
            "LOCATION": f"redis://{host}:{port}?db=3",
        },
    }

    with override_settings(CACHES=caches_config):
        cache = caches["default"]
        cache.set("test_db_query", "value")
        assert cache.get("test_db_query") == "value"
        cache.delete("test_db_query")


def test_db_in_options(redis_container):
    """Test that db number in OPTIONS works (Django-style)."""
    host = redis_container.host
    port = redis_container.port

    caches_config = {
        "default": {
            "BACKEND": "django_cachex.cache.RedisCache",
            "LOCATION": f"redis://{host}:{port}",  # No db in URL
            "OPTIONS": {
                "db": 4,
            },
        },
    }

    with override_settings(CACHES=caches_config):
        cache = caches["default"]
        cache.set("test_db_options", "value")
        assert cache.get("test_db_options") == "value"
        cache.delete("test_db_options")
