"""Tests for connection string/URL handling."""

import pytest
from django.core.cache import caches
from django.test import override_settings


def _pool_kwargs(cache):
    return cache.adapter._get_connection_pool(write=True).connection_kwargs


@pytest.mark.parametrize("db", [0, 1, 2])
def test_db_in_url_path(redis_container, db):
    location = f"redis://{redis_container.host}:{redis_container.port}/{db}"
    caches_config = {"default": {"BACKEND": "django_cachex.cache.RedisCache", "LOCATION": location}}

    with override_settings(CACHES=caches_config):
        cache = caches["default"]

        assert _pool_kwargs(cache)["db"] == db
        cache.set("test_db_url", "value")
        assert cache.get("test_db_url") == "value"
        cache.delete("test_db_url")


def test_db_in_query_string(redis_container):
    location = f"redis://{redis_container.host}:{redis_container.port}?db=3"
    caches_config = {"default": {"BACKEND": "django_cachex.cache.RedisCache", "LOCATION": location}}

    with override_settings(CACHES=caches_config):
        cache = caches["default"]

        assert _pool_kwargs(cache)["db"] == 3
        cache.set("test_db_query", "value")
        assert cache.get("test_db_query") == "value"
        cache.delete("test_db_query")


def test_url_credentials_reach_the_pool(redis_container):
    """The URL userinfo has to survive into the pool, even against a server without auth."""
    location = f"redis://alice:s3cret@{redis_container.host}:{redis_container.port}/1"
    caches_config = {"default": {"BACKEND": "django_cachex.cache.RedisCache", "LOCATION": location}}

    with override_settings(CACHES=caches_config):
        kwargs = _pool_kwargs(caches["default"])

        assert kwargs["username"] == "alice"
        assert kwargs["password"] == "s3cret"
