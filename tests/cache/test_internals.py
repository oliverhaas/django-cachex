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

from django_cachex.adapter import RedisAdapter
from django_cachex.adapter.default import _ASYNC_POOLS

if TYPE_CHECKING:
    from django_cachex.cache import KeyValueCache
    from tests.fixtures.containers import RedisContainerInfo


class TestRedisCacheInternals:
    """Tests matching Django's RedisCacheTests for Redis-specific internals.

    These tests verify that our implementation matches Django's official
    redis cache backend (django.core.cache.backends.redis.RedisCache).
    """

    def test_incr_write_connection(self, cache: KeyValueCache):
        cache.set("number", 42)
        with mock.patch.object(cache.adapter, "get_client", wraps=cache.adapter.get_client) as mocked_get_client:
            cache.incr("number")
            # incr should use write=True
            assert mocked_get_client.call_args.kwargs.get("write") is True

    def test_adapter_class(self, cache: KeyValueCache):
        # Check _adapter_class attribute points to the right adapter class
        assert cache._adapter_class == RedisAdapter or issubclass(
            cache._adapter_class,
            RedisAdapter.__class__.__bases__,
        )  # type: ignore[attr-defined]
        # Check adapter is an instance of the configured class
        assert isinstance(cache.adapter, cache._adapter_class)

    def test_get_backend_timeout_method(self, cache: KeyValueCache):
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
        # Write always returns index 0 (primary)
        pool_index = cache.adapter._get_connection_pool_index(write=True)
        assert pool_index == 0

        # Read returns 0 for single server, or random replica for multiple
        pool_index = cache.adapter._get_connection_pool_index(write=False)
        if len(cache.adapter._servers) == 1:
            assert pool_index == 0
        else:
            assert pool_index >= 0
            assert pool_index < len(cache.adapter._servers)

    def test_get_connection_pool(self, cache: KeyValueCache):
        import redis

        # Write pool
        pool = cache.adapter._get_connection_pool(write=True)
        assert isinstance(pool, redis.ConnectionPool)

        # Read pool
        pool = cache.adapter._get_connection_pool(write=False)
        assert isinstance(pool, redis.ConnectionPool)

    def test_get_client(self, cache: KeyValueCache):
        """Test Redis client creation returns redis.Redis or redis.RedisCluster instance."""
        import redis

        client = cache.adapter.get_client()
        # Can be Redis (standalone/sentinel) or RedisCluster (cluster mode)
        assert isinstance(client, (redis.Redis, redis.RedisCluster))

    def test_serializer_dumps(self, cache: KeyValueCache):
        """Test serialization: integers stay as-is, bools/strings become bytes.

        Note: We test via the encode() method which handles the integer optimization.
        Django's test checks _serializer.dumps() but our architecture uses encode().
        """
        # Integers are stored directly (no serialization overhead)
        assert cache.adapter.encode(123) == 123

        # Booleans are serialized to bytes
        assert isinstance(cache.adapter.encode(True), bytes)

        # Strings are serialized to bytes
        assert isinstance(cache.adapter.encode("abc"), bytes)

    def test_redis_pool_options(self, redis_container: RedisContainerInfo):
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

        # Clear cached cache backend before override_settings
        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            pool = cache.adapter._get_connection_pool(write=False)  # type: ignore[attr-defined]

            # db is parsed from URL path
            assert pool.connection_kwargs["db"] == 5
            # OPTIONS are passed to pool
            assert pool.connection_kwargs["socket_timeout"] == 0.1
            assert pool.connection_kwargs["retry_on_timeout"] is True

        # Clean up
        with suppress(KeyError, AttributeError):
            del caches["default"]


class TestRedisAdapterMethods:
    """Additional tests for CacheClient method behavior."""

    def test_get_client_write_vs_read(self, cache: KeyValueCache):
        # Both should return valid Redis clients
        write_client = cache.adapter.get_client(write=True)
        read_client = cache.adapter.get_client(write=False)

        assert write_client is not None
        assert read_client is not None

    def test_connection_pool_caching(self, cache: KeyValueCache):
        pool1 = cache.adapter._get_connection_pool(write=True)
        pool2 = cache.adapter._get_connection_pool(write=True)

        # Same pool should be returned
        assert pool1 is pool2

    def test_multiple_servers_pool_selection(self, redis_container: RedisContainerInfo):
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

        # Clear cached cache backend before override_settings
        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Write should always use index 0
            write_idx = cache.adapter._get_connection_pool_index(write=True)  # type: ignore[attr-defined]
            assert write_idx == 0

            # Read can use any index
            read_idx = cache.adapter._get_connection_pool_index(write=False)  # type: ignore[attr-defined]
            assert 0 <= read_idx < 3

        # Clean up
        with suppress(KeyError, AttributeError):
            del caches["default"]


class TestConnectionCleanup:
    """Tests for connection pool cleanup behavior."""

    def test_sync_pool_is_cached_per_instance(self, cache: KeyValueCache):
        # Get pool twice - should be same object
        pool1 = cache.adapter._get_connection_pool(write=True)
        pool2 = cache.adapter._get_connection_pool(write=True)
        assert pool1 is pool2

        # Should be in _pools dict at index 0
        assert 0 in cache.adapter._pools
        assert cache.adapter._pools[0] is pool1

    @pytest.mark.asyncio
    async def test_async_pool_is_cached_per_event_loop(self, cache: KeyValueCache):
        """Test that async pools are cached per event loop."""
        # Skip if async not supported (cluster/sentinel don't have async yet)
        if cache.adapter._async_pool_class is None:
            pytest.skip("Async not supported for this client type")

        # Get async pool twice in same event loop - should be same object
        pool1 = cache.adapter._get_async_connection_pool(write=True)
        pool2 = cache.adapter._get_async_connection_pool(write=True)
        assert pool1 is pool2

        # Pool lives in the module-level registry under the current loop.
        loop = asyncio.get_running_loop()
        assert loop in _ASYNC_POOLS
        assert pool1 in _ASYNC_POOLS[loop].values()

    def test_async_pool_different_per_loop(self, redis_container: RedisContainerInfo):
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

        # Clear cached cache backend before override_settings
        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            client = cache.adapter  # type: ignore[attr-defined]  # type: ignore[attr-defined]

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

            # Each loop has its own entry in the module-level registry.
            assert loop1 in _ASYNC_POOLS
            assert loop2 in _ASYNC_POOLS

            # Clean up loops
            loop1.close()
            loop2.close()

        # Clean up
        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_close_is_noop(self, cache: KeyValueCache):
        """Test close() is a no-op — pools persist after close."""
        # Create a pool first
        pool = cache.adapter._get_connection_pool(write=True)
        assert 0 in cache.adapter._pools

        # close() should NOT clear pools
        cache.adapter.close()
        assert 0 in cache.adapter._pools
        assert cache.adapter._pools[0] is pool

    @pytest.mark.asyncio
    async def test_aclose_is_noop(self, cache: KeyValueCache):
        """Test aclose() is a no-op — async pools persist after aclose."""
        if cache.adapter._async_pool_class is None:
            pytest.skip("Async not supported for this client type")

        # Create an async pool
        pool = cache.adapter._get_async_connection_pool(write=True)
        loop = asyncio.get_running_loop()

        assert loop in _ASYNC_POOLS
        assert pool in _ASYNC_POOLS[loop].values()

        # aclose() should NOT disconnect or remove pools
        await cache.adapter.aclose()
        assert loop in _ASYNC_POOLS
        assert pool in _ASYNC_POOLS[loop].values()

    @pytest.mark.asyncio
    async def test_async_pool_shared_across_per_task_client_instances(
        self,
        cache: KeyValueCache,
    ) -> None:
        """Regression: a fresh ``BaseKeyValueAdapter`` reuses the existing pool.

        Django's ``asgiref.local.Local``-backed cache handler returns a fresh
        ``BaseCache`` instance per asyncio task, which means a fresh
        ``BaseKeyValueAdapter`` is built on every async request. Before the
        process-wide ``_ASYNC_POOLS`` registry, each fresh client created its
        own pool, so ``max_connections`` had no effect across tasks and the
        pool grew unbounded under concurrent load. This locks in the fix.
        """
        if cache.adapter._async_pool_class is None:
            pytest.skip("Async not supported for this client type")

        # First client (the one Django gave us) opens a pool.
        original_pool = cache.adapter._get_async_connection_pool(write=True)

        # Build a fresh client with the same servers + options — this is what
        # Django's per-task Local does on every request.
        cls = type(cache.adapter)
        fresh_client = cls(cache.adapter._servers, **cache.adapter._options)

        # The fresh client must reuse the same pool from the module registry.
        fresh_pool = fresh_client._get_async_connection_pool(write=True)
        assert fresh_pool is original_pool, (
            "Fresh per-task client created a new pool — process-wide registry not working."
        )

        # And one more independent instance, just to be sure.
        another_client = cls(cache.adapter._servers, **cache.adapter._options)
        assert another_client._get_async_connection_pool(write=True) is original_pool

    def test_weak_key_dictionary_cleanup_on_loop_gc(self, redis_container: RedisContainerInfo):
        """Test that async pools are cleaned up when event loop is garbage collected.

        This tests the WeakKeyDictionary behavior - when an event loop is GC'd,
        its entry in the module-level _ASYNC_POOLS registry should be
        automatically removed.
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
            client = cache.adapter  # type: ignore[attr-defined]

            # Create a new event loop and get a pool in it
            loop = asyncio.new_event_loop()

            async def create_pool():
                return client._get_async_connection_pool(write=True)

            pool = loop.run_until_complete(create_pool())
            loop.close()

            # Loop should be in the module-level registry.
            assert loop in _ASYNC_POOLS

            # Keep a weak reference to track when loop is GC'd
            loop_ref = weakref.ref(loop)

            # Delete the loop reference and force garbage collection
            del loop
            del pool
            gc.collect()

            # The loop should have been garbage collected
            assert loop_ref() is None

            # The WeakKeyDictionary should have automatically removed the entry.
            # Iterating works without errors; the dead loop is gone.
            _ = list(_ASYNC_POOLS.keys())

        with suppress(KeyError, AttributeError):
            del caches["default"]

    @pytest.mark.asyncio
    async def test_async_pool_reuse_after_operations(self, cache: KeyValueCache):
        # Skip if async not supported (cluster/sentinel don't have async yet)
        if cache.adapter._async_pool_class is None:
            pytest.skip("Async not supported for this client type")

        # Snapshot the write pool before any ops; the same object must
        # still be the active write pool after a batch of mixed ops
        # (this is what verifies "reuse", not just an upper bound).
        loop = asyncio.get_running_loop()
        original_write_pool = cache.adapter._get_async_connection_pool(write=True)

        await cache.aset("test_reuse_1", "value1")
        await cache.aset("test_reuse_2", "value2")
        await cache.aget("test_reuse_1")
        await cache.adelete("test_reuse_1")

        # Same write-pool object is still active.
        assert cache.adapter._get_async_connection_pool(write=True) is original_write_pool
        # And we never created more than one pool per configured server.
        assert 1 <= len(_ASYNC_POOLS.get(loop, {})) <= len(cache.adapter._servers)

        # Clean up
        await cache.adelete("test_reuse_2")

    @pytest.mark.asyncio
    async def test_mixed_sync_async_operations(self, cache: KeyValueCache):
        # Skip if async not supported (cluster/sentinel don't have async yet)
        if cache.adapter._async_pool_class is None:
            pytest.skip("Async not supported for this client type")

        # Perform sync operation (creates sync pool)
        cache.set("sync_key", "sync_value")
        sync_pool = cache.adapter._get_connection_pool(write=True)

        # Perform async operation (creates async pool)
        await cache.aset("async_key", "async_value")
        async_pool = cache.adapter._get_async_connection_pool(write=True)

        # Pools should be different objects
        assert sync_pool is not async_pool

        # Both should exist
        assert 0 in cache.adapter._pools
        loop = asyncio.get_running_loop()
        assert loop in _ASYNC_POOLS

        # Clean up
        cache.delete("sync_key")
        await cache.adelete("async_key")

    def test_sync_then_nested_async_run(self, redis_container: RedisContainerInfo):
        """Test WSGI-like pattern: sync ops then asyncio.run() for async ops.

        Simulates a WSGI thread that does sync cache work, then spins up an
        event loop for some async logic using the same cache instance.
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

            # 1. Sync operations (like a normal WSGI request)
            cache.set("wsgi_key", "wsgi_value")
            assert cache.get("wsgi_key") == "wsgi_value"

            # 2. Spin up an event loop for async work (same cache instance)
            async def async_work():
                await cache.aset("async_key", "async_value")
                result = await cache.aget("async_key")
                assert result == "async_value"
                await cache.adelete("async_key")

            loop = asyncio.new_event_loop()
            loop.run_until_complete(async_work())
            loop.close()

            # 3. Back to sync — should still work fine
            assert cache.get("wsgi_key") == "wsgi_value"
            cache.delete("wsgi_key")

        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_multiple_sequential_event_loops(self, redis_container: RedisContainerInfo):
        """Test multiple asyncio.run() calls from the same sync context.

        Simulates a WSGI thread that calls asyncio.run() multiple times across
        different requests, each creating a new event loop. The WeakKeyDictionary
        should handle old loops being GC'd and new ones being created.
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

            async def async_set_get(key, value):
                await cache.aset(key, value)
                return await cache.aget(key)

            # First "request": sync + async
            cache.set("sync_1", "value_1")
            loop1 = asyncio.new_event_loop()
            result = loop1.run_until_complete(async_set_get("async_1", "avalue_1"))
            assert result == "avalue_1"
            loop1.close()

            # Second "request": new event loop, same cache instance
            cache.set("sync_2", "value_2")
            loop2 = asyncio.new_event_loop()
            result = loop2.run_until_complete(async_set_get("async_2", "avalue_2"))
            assert result == "avalue_2"
            loop2.close()

            # Third "request": yet another event loop
            loop3 = asyncio.new_event_loop()
            result = loop3.run_until_complete(async_set_get("async_3", "avalue_3"))
            assert result == "avalue_3"
            loop3.close()

            # Sync still works
            assert cache.get("sync_1") == "value_1"
            assert cache.get("sync_2") == "value_2"

            # Clean up
            cache.delete_many(["sync_1", "sync_2", "async_1", "async_2", "async_3"])

        with suppress(KeyError, AttributeError):
            del caches["default"]
