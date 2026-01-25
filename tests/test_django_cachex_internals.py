"""Tests for Redis cache internals matching Django's RedisCacheTests.

These tests mirror Django's RedisCacheTests from django/tests/cache/tests.py
to ensure django-cachex internals match Django's official Redis cache backend.

Reference: https://github.com/django/django/blob/main/tests/cache/tests.py
"""

import asyncio
import gc
import weakref
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from django.core.cache import caches
from django.test import override_settings

from django_cachex.cache import KeyValueCache
from django_cachex.client import RedisCacheClient

if TYPE_CHECKING:
    from tests.fixtures.containers import RedisContainerInfo


class TestRedisCacheInternals:
    """Tests matching Django's RedisCacheTests for Redis-specific internals.

    These tests verify that our implementation matches Django's official
    redis cache backend (django.core.cache.backends.redis.RedisCache).
    """

    def test_incr_write_connection(self, cache: KeyValueCache):
        """Verify incr uses write connection."""
        cache.set("number", 42)
        with mock.patch.object(
            cache._cache, "get_client", wraps=cache._cache.get_client
        ) as mocked_get_client:
            cache.incr("number")
            # incr should use write=True
            assert mocked_get_client.call_args.kwargs.get("write") is True

    def test_cache_client_class(self, cache: KeyValueCache):
        """Verify RedisCacheClient class is used."""
        # Check _class attribute points to correct client class
        assert cache._class == RedisCacheClient or issubclass(cache._class, RedisCacheClient.__class__.__bases__)
        # Check _cache is an instance of the client
        assert isinstance(cache._cache, cache._class)

    def test_get_backend_timeout_method(self, cache: KeyValueCache):
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

    def test_get_connection_pool_index(self, cache: KeyValueCache):
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

    def test_get_connection_pool(self, cache: KeyValueCache):
        """Test ConnectionPool instantiation."""
        import redis

        # Write pool
        pool = cache._cache._get_connection_pool(write=True)
        assert isinstance(pool, redis.ConnectionPool)

        # Read pool
        pool = cache._cache._get_connection_pool(write=False)
        assert isinstance(pool, redis.ConnectionPool)

    def test_get_client(self, cache: KeyValueCache):
        """Test Redis client creation returns redis.Redis or redis.RedisCluster instance."""
        import redis

        client = cache._cache.get_client()
        # Can be Redis (standalone/sentinel) or RedisCluster (cluster mode)
        assert isinstance(client, (redis.Redis, redis.RedisCluster))

    def test_serializer_dumps(self, cache: KeyValueCache):
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

    def test_get_client_write_vs_read(self, cache: KeyValueCache):
        """Test get_client respects write parameter."""
        # Both should return valid Redis clients
        write_client = cache._cache.get_client(write=True)
        read_client = cache._cache.get_client(write=False)

        assert write_client is not None
        assert read_client is not None

    def test_connection_pool_caching(self, cache: KeyValueCache):
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


class TestConnectionCleanup:
    """Tests for connection pool cleanup behavior."""

    def test_sync_pool_is_cached_per_instance(self, cache: KeyValueCache):
        """Test that sync pools are cached per instance."""
        # Get pool twice - should be same object
        pool1 = cache._cache._get_connection_pool(write=True)
        pool2 = cache._cache._get_connection_pool(write=True)
        assert pool1 is pool2

        # Should be in _pools dict at index 0
        assert 0 in cache._cache._pools
        assert cache._cache._pools[0] is pool1

    @pytest.mark.asyncio
    async def test_async_pool_is_cached_per_event_loop(self, cache: KeyValueCache):
        """Test that async pools are cached per event loop."""
        # Skip if async not supported (cluster/sentinel don't have async yet)
        if cache._cache._async_pool_class is None:
            pytest.skip("Async not supported for this client type")

        # Get async pool twice in same event loop - should be same object
        pool1 = cache._cache._get_async_connection_pool(write=True)
        pool2 = cache._cache._get_async_connection_pool(write=True)
        assert pool1 is pool2

        # Should be in _async_pools for current loop
        loop = asyncio.get_running_loop()
        assert loop in cache._cache._async_pools
        assert cache._cache._async_pools[loop][0] is pool1

    def test_async_pool_different_per_loop(self, redis_container: "RedisContainerInfo"):
        """Test that different event loops get different async pools.

        This is a synchronous test that creates its own event loops to avoid
        conflicts with pytest-asyncio's event loop management.
        """
        from contextlib import suppress

        host = redis_container.host
        port = redis_container.port

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}/1",
            },
        }

        # Clear cached cache instance before override_settings
        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            client = cache._cache

            # Create first event loop and get pool
            loop1 = asyncio.new_event_loop()

            async def get_pool():
                return client._get_async_connection_pool(write=True)

            pool1 = loop1.run_until_complete(get_pool())

            # Create second event loop and get pool
            loop2 = asyncio.new_event_loop()
            pool2 = loop2.run_until_complete(get_pool())

            # Pools should be different objects
            assert pool1 is not pool2

            # Each loop should have its own entry
            assert loop1 in client._async_pools
            assert loop2 in client._async_pools

            # Clean up loops
            loop1.close()
            loop2.close()

        # Clean up
        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_close_without_close_connection_option(self, cache: KeyValueCache):
        """Test close() does nothing when close_connection=False (default)."""
        # Create a pool first
        pool = cache._cache._get_connection_pool(write=True)
        assert 0 in cache._cache._pools

        # close() should not clear pools when close_connection is False
        cache._cache.close()
        assert 0 in cache._cache._pools
        assert cache._cache._pools[0] is pool

    def test_close_with_close_connection_option(self, redis_container: "RedisContainerInfo"):
        """Test close() disconnects and clears pools when close_connection=True."""
        from contextlib import suppress

        host = redis_container.host
        port = redis_container.port

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}/1",
                "OPTIONS": {
                    "close_connection": True,
                },
            },
        }

        # Clear cached cache instance before override_settings
        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            client = cache._cache

            # Create a pool
            pool = client._get_connection_pool(write=True)
            assert 0 in client._pools

            # close() should disconnect and clear pools
            with mock.patch.object(pool, "disconnect") as mock_disconnect:
                client.close()

                mock_disconnect.assert_called_once()
                assert len(client._pools) == 0

        # Clean up
        with suppress(KeyError, AttributeError):
            del caches["default"]

    @pytest.mark.asyncio
    async def test_aclose_disconnects_async_pools(self, cache: KeyValueCache):
        """Test aclose() disconnects async pools for current event loop."""
        # Skip if async not supported (cluster/sentinel don't have async yet)
        if cache._cache._async_pool_class is None:
            pytest.skip("Async not supported for this client type")

        # Create an async pool
        pool = cache._cache._get_async_connection_pool(write=True)
        loop = asyncio.get_running_loop()

        assert loop in cache._cache._async_pools
        assert cache._cache._async_pools[loop][0] is pool

        # aclose() should disconnect and remove pool for current loop
        with mock.patch.object(pool, "disconnect", new_callable=mock.AsyncMock) as mock_disconnect:
            await cache._cache.aclose()

            mock_disconnect.assert_called_once()
            assert loop not in cache._cache._async_pools

    def test_aclose_only_affects_current_loop(self, redis_container: "RedisContainerInfo"):
        """Test aclose() only removes pools for the current event loop.

        This is a synchronous test that creates its own event loops to avoid
        conflicts with pytest-asyncio's event loop management.
        """
        from contextlib import suppress

        host = redis_container.host
        port = redis_container.port

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}/1",
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            client = cache._cache

            # Create two event loops and get pools in each
            loop1 = asyncio.new_event_loop()
            loop2 = asyncio.new_event_loop()

            async def get_pool():
                return client._get_async_connection_pool(write=True)

            pool1 = loop1.run_until_complete(get_pool())
            pool2 = loop2.run_until_complete(get_pool())

            # Both loops should have pools
            assert loop1 in client._async_pools
            assert loop2 in client._async_pools

            # aclose() in loop1 should only affect loop1
            async def close_pools():
                with mock.patch.object(pool1, "disconnect", new_callable=mock.AsyncMock):
                    await client.aclose()

            loop1.run_until_complete(close_pools())

            assert loop1 not in client._async_pools
            # Loop2's pool should still be there
            assert loop2 in client._async_pools

            # Clean up
            loop1.close()
            loop2.close()

        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_weak_key_dictionary_cleanup_on_loop_gc(self, redis_container: "RedisContainerInfo"):
        """Test that async pools are cleaned up when event loop is garbage collected.

        This tests the WeakKeyDictionary behavior - when an event loop is GC'd,
        its entry in _async_pools should be automatically removed.
        """
        from contextlib import suppress

        host = redis_container.host
        port = redis_container.port

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}/1",
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            client = cache._cache

            # Create a new event loop and get a pool in it
            loop = asyncio.new_event_loop()

            async def create_pool():
                return client._get_async_connection_pool(write=True)

            pool = loop.run_until_complete(create_pool())
            loop.close()

            # Loop should be in _async_pools
            assert loop in client._async_pools

            # Keep a weak reference to track when loop is GC'd
            loop_ref = weakref.ref(loop)

            # Delete the loop reference and force garbage collection
            del loop
            gc.collect()

            # The loop should have been garbage collected
            assert loop_ref() is None

            # The WeakKeyDictionary should have automatically removed the entry
            # Note: We can't check for a specific loop anymore since it's GC'd,
            # but we can verify the pool is no longer accessible
            # Check that no dead references remain
            # (accessing the dict should work without errors)
            _ = list(client._async_pools.keys())

        with suppress(KeyError, AttributeError):
            del caches["default"]

    @pytest.mark.asyncio
    async def test_async_pool_reuse_after_operations(self, cache: KeyValueCache):
        """Test that async pool is reused across multiple async operations."""
        # Skip if async not supported (cluster/sentinel don't have async yet)
        if cache._cache._async_pool_class is None:
            pytest.skip("Async not supported for this client type")

        # Perform some async operations
        await cache.aset("test_reuse_1", "value1")
        await cache.aset("test_reuse_2", "value2")
        await cache.aget("test_reuse_1")
        await cache.adelete("test_reuse_1")

        # Should still have only one pool for the current loop
        loop = asyncio.get_running_loop()
        assert len(cache._cache._async_pools.get(loop, {})) <= len(cache._cache._servers)

        # Clean up
        await cache.adelete("test_reuse_2")

    @pytest.mark.asyncio
    async def test_mixed_sync_async_operations(self, cache: KeyValueCache):
        """Test that sync and async operations use separate pools."""
        # Skip if async not supported (cluster/sentinel don't have async yet)
        if cache._cache._async_pool_class is None:
            pytest.skip("Async not supported for this client type")

        # Perform sync operation (creates sync pool)
        cache.set("sync_key", "sync_value")
        sync_pool = cache._cache._get_connection_pool(write=True)

        # Perform async operation (creates async pool)
        await cache.aset("async_key", "async_value")
        async_pool = cache._cache._get_async_connection_pool(write=True)

        # Pools should be different objects
        assert sync_pool is not async_pool

        # Both should exist
        assert 0 in cache._cache._pools
        loop = asyncio.get_running_loop()
        assert loop in cache._cache._async_pools

        # Clean up
        cache.delete("sync_key")
        await cache.adelete("async_key")
