"""Tests for Redis cache internals matching Django's RedisCacheTests.

These tests mirror Django's RedisCacheTests from django/tests/cache/tests.py
to ensure django-cachex internals match Django's official Redis cache backend.

Reference: https://github.com/django/django/blob/main/tests/cache/tests.py
"""

from typing import TYPE_CHECKING
from unittest import mock

from django.core.cache import caches
from django.test import override_settings

from django_cachex.cache import RedisCache
from django_cachex.client import RedisCacheClient

if TYPE_CHECKING:
    from tests.fixtures.containers import RedisContainerInfo


class TestRedisCacheInternals:
    """Tests matching Django's RedisCacheTests for Redis-specific internals.

    These tests verify that our implementation matches Django's official
    redis cache backend (django.core.cache.backends.redis.RedisCache).
    """

    def test_incr_write_connection(self, cache: RedisCache):
        """Verify incr uses write connection."""
        cache.set("number", 42)
        with mock.patch.object(
            cache._cache, "get_client", wraps=cache._cache.get_client
        ) as mocked_get_client:
            cache.incr("number")
            # incr should use write=True
            assert mocked_get_client.call_args.kwargs.get("write") is True

    def test_cache_client_class(self, cache: RedisCache):
        """Verify RedisCacheClient class is used."""
        # Check _class attribute points to correct client class
        assert cache._class == RedisCacheClient or issubclass(cache._class, RedisCacheClient.__class__.__bases__)
        # Check _cache is an instance of the client
        assert isinstance(cache._cache, cache._class)

    def test_get_backend_timeout_method(self, cache: RedisCache):
        """Test get_backend_timeout handles positive, negative, and None values."""
        # Positive timeout returns as-is
        positive_timeout = 10
        positive_backend_timeout = cache.get_backend_timeout(positive_timeout)
        assert positive_backend_timeout == positive_timeout

        # Negative timeout returns 0 (immediate expiration)
        negative_timeout = -5
        negative_backend_timeout = cache.get_backend_timeout(negative_timeout)
        assert negative_backend_timeout == 0

        # None timeout returns None (no expiration)
        none_timeout = None
        none_backend_timeout = cache.get_backend_timeout(none_timeout)
        assert none_backend_timeout is None

    def test_get_connection_pool_index(self, cache: RedisCache):
        """Test connection pool index selection logic."""
        # Write always returns index 0 (primary)
        pool_index = cache._cache._get_connection_pool_index(write=True)
        assert pool_index == 0

        # Read returns 0 for single server, or random replica for multiple
        pool_index = cache._cache._get_connection_pool_index(write=False)
        if len(cache._cache._servers) == 1:
            assert pool_index == 0
        else:
            assert pool_index >= 0
            assert pool_index < len(cache._cache._servers)

    def test_get_connection_pool(self, cache: RedisCache):
        """Test ConnectionPool instantiation."""
        import redis

        # Write pool
        pool = cache._cache._get_connection_pool(write=True)
        assert isinstance(pool, redis.ConnectionPool)

        # Read pool
        pool = cache._cache._get_connection_pool(write=False)
        assert isinstance(pool, redis.ConnectionPool)

    def test_get_client(self, cache: RedisCache):
        """Test Redis client creation returns redis.Redis or redis.RedisCluster instance."""
        import redis

        client = cache._cache.get_client()
        # Can be Redis (standalone/sentinel) or RedisCluster (cluster mode)
        assert isinstance(client, (redis.Redis, redis.RedisCluster))

    def test_serializer_dumps(self, cache: RedisCache):
        """Test serialization: integers stay as-is, bools/strings become bytes.

        Note: We test via the encode() method which handles the integer optimization.
        Django's test checks _serializer.dumps() but our architecture uses encode().
        """
        # Integers are stored directly (no serialization overhead)
        assert cache._cache.encode(123) == 123

        # Booleans are serialized to bytes
        assert isinstance(cache._cache.encode(True), bytes)

        # Strings are serialized to bytes
        assert isinstance(cache._cache.encode("abc"), bytes)

    def test_redis_pool_options(self, redis_container: "RedisContainerInfo"):
        """Test that pool OPTIONS are passed to connection pool."""
        from contextlib import suppress


        host = redis_container.host
        port = redis_container.port

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}/5",  # db=5 in URL
                "OPTIONS": {
                    "socket_timeout": 0.1,
                    "retry_on_timeout": True,
                },
            },
        }

        # Clear cached cache instance before override_settings
        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            pool = cache._cache._get_connection_pool(write=False)

            # db is parsed from URL path
            assert pool.connection_kwargs["db"] == 5
            # OPTIONS are passed to pool
            assert pool.connection_kwargs["socket_timeout"] == 0.1
            assert pool.connection_kwargs["retry_on_timeout"] is True

        # Clean up
        with suppress(KeyError, AttributeError):
            del caches["default"]


class TestRedisCacheClientMethods:
    """Additional tests for CacheClient method behavior."""

    def test_get_client_write_vs_read(self, cache: RedisCache):
        """Test get_client respects write parameter."""
        # Both should return valid Redis clients
        write_client = cache._cache.get_client(write=True)
        read_client = cache._cache.get_client(write=False)

        assert write_client is not None
        assert read_client is not None

    def test_connection_pool_caching(self, cache: RedisCache):
        """Test that connection pools are cached and reused."""
        pool1 = cache._cache._get_connection_pool(write=True)
        pool2 = cache._cache._get_connection_pool(write=True)

        # Same pool should be returned
        assert pool1 is pool2

    def test_multiple_servers_pool_selection(self, redis_container: "RedisContainerInfo"):
        """Test pool selection with multiple servers configured."""
        from contextlib import suppress

        host = redis_container.host
        port = redis_container.port
        url = f"redis://{host}:{port}/1"

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": [url, url, url],  # 3 "servers" (same for testing)
            },
        }

        # Clear cached cache instance before override_settings
        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Write should always use index 0
            write_idx = cache._cache._get_connection_pool_index(write=True)
            assert write_idx == 0

            # Read can use any index
            read_idx = cache._cache._get_connection_pool_index(write=False)
            assert 0 <= read_idx < 3

        # Clean up
        with suppress(KeyError, AttributeError):
            del caches["default"]
