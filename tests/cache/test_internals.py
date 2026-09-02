"""Tests for Redis cache internals matching Django's RedisCacheTests.

These tests mirror Django's RedisCacheTests from django/tests/cache/tests.py
to ensure django-cachex internals match Django's official Redis cache backend.

Reference: https://github.com/django/django/blob/main/tests/cache/tests.py
"""

import asyncio
import enum
import gc
import weakref
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest
from asgiref.sync import async_to_sync
from django.core.cache import caches
from django.test import override_settings

from django_cachex.adapters import RedisPyAdapter
from django_cachex.exceptions import WrongTypeError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django_cachex.cache import RespCache
    from tests.fixtures.containers import RedisContainerInfo


class _Priority(enum.IntEnum):
    # Module level so pickle can resolve it by qualname.
    LOW = 1
    HIGH = 3


@contextmanager
def redis_cache(location: str | list[str], **options: Any) -> Iterator[RespCache]:
    """Yield a RedisCache built from ``location``, outside the adapter matrix.

    ``override_settings(CACHES=...)`` rebuilds Django's cache handler on entry
    and on exit, so the caller needs no teardown of its own.
    """
    config: dict[str, Any] = {"BACKEND": "django_cachex.cache.RedisCache", "LOCATION": location}
    if options:
        config["OPTIONS"] = options
    with override_settings(CACHES={"default": config}):
        yield caches["default"]


def skip_without_generic_async_pool(cache: RespCache) -> None:
    """Skip adapters that manage their own async pools instead of ``_async_pool_class``.

    Cluster and Sentinel both have async clients; they just reach them
    through the cluster registry and the Sentinel pool class respectively.
    """
    if cache.adapter._async_pool_class is None:
        pytest.skip("Cluster and Sentinel adapters manage their own async pools")


class TestRedisCacheInternals:
    """Tests matching Django's RedisCacheTests for Redis-specific internals.

    These tests verify that our implementation matches Django's official
    redis cache backend (django.core.cache.backends.redis.RedisCache).
    """

    def test_incr_write_connection(self, cache: RespCache):
        cache.set("number", 42)
        with mock.patch.object(cache.adapter, "get_client", wraps=cache.adapter.get_client) as mocked_get_client:
            cache.incr("number")
            assert mocked_get_client.call_args.kwargs.get("write") is True

    def test_adapter_class(self, cache: RespCache):
        from django_cachex.adapters import RedisPyClusterAdapter, RedisPySentinelAdapter
        from django_cachex.adapters.redis_py import _RedisPyMixin

        assert issubclass(  # type: ignore[attr-defined]
            cache._adapter_class,
            (RedisPyAdapter, RedisPyClusterAdapter, RedisPySentinelAdapter),
        ) or issubclass(cache._adapter_class, _RedisPyMixin)  # type: ignore[attr-defined]
        assert isinstance(cache.adapter, cache._adapter_class)

    def test_get_backend_timeout_method(self, cache: RespCache):
        assert cache.get_backend_timeout(10) == 10
        # A negative timeout means expire immediately, not "no expiry".
        assert cache.get_backend_timeout(-5) == 0
        assert cache.get_backend_timeout(None) is None

    def test_get_connection_pool_index(self, cache: RespCache):
        assert cache.adapter._get_connection_pool_index(write=True) == 0

        pool_index = cache.adapter._get_connection_pool_index(write=False)
        if len(cache.adapter._servers) == 1:
            assert pool_index == 0
        else:
            assert pool_index >= 0
            assert pool_index < len(cache.adapter._servers)

    def test_get_connection_pool(self, cache: RespCache):
        import redis

        assert isinstance(cache.adapter._get_connection_pool(write=True), redis.ConnectionPool)
        assert isinstance(cache.adapter._get_connection_pool(write=False), redis.ConnectionPool)

    def test_get_client(self, cache: RespCache):
        """Test Redis client creation returns redis.Redis or redis.RedisCluster instance."""
        import redis

        client = cache.adapter.get_client()
        assert isinstance(client, (redis.Redis, redis.RedisCluster))

    def test_serializer_dumps(self, cache: RespCache):
        """Test serialization: integers stay as-is, bools/strings become bytes.

        We test via the encode() method which handles the integer optimization.
        Django's test checks _serializer.dumps() but our architecture uses encode().
        """
        assert cache.encode(123) == 123
        assert isinstance(cache.encode(True), bytes)
        assert isinstance(cache.encode("abc"), bytes)

    def test_encode_serializes_int_subclasses(self, cache: RespCache):
        """Only exact int passes through; IntEnum is serialized so its type survives.

        Regression: isinstance-based dispatch stored IntEnum members as bare
        ints, so they came back as plain int.
        """
        encoded = cache.encode(_Priority.HIGH)
        assert isinstance(encoded, bytes)
        assert cache.decode(encoded) is _Priority.HIGH

    def test_bool_roundtrip(self, cache: RespCache):
        cache.set("internals_bool_true", True)
        assert cache.get("internals_bool_true") is True
        cache.set("internals_bool_false", False)
        assert cache.get("internals_bool_false") is False

    def test_int_enum_roundtrip(self, cache: RespCache):
        cache.set("internals_enum", _Priority.HIGH)
        result = cache.get("internals_enum")
        assert result is _Priority.HIGH
        assert type(result) is _Priority

    def test_plain_int_roundtrip(self, cache: RespCache):
        cache.set("internals_int", 123)
        result = cache.get("internals_int")
        assert result == 123
        assert type(result) is int

    def test_redis_pool_options(self, redis_container: RedisContainerInfo):
        location = f"redis://{redis_container.host}:{redis_container.port}/5"

        with redis_cache(location, socket_timeout=0.1, retry_on_timeout=True) as cache:
            pool = cache.adapter._get_connection_pool(write=False)

            assert pool.connection_kwargs["db"] == 5
            assert pool.connection_kwargs["socket_timeout"] == 0.1
            assert pool.connection_kwargs["retry_on_timeout"] is True


class TestRedisAdapterMethods:
    def test_get_client_write_vs_read(self, cache: RespCache):
        write_client = cache.adapter.get_client(write=True)
        read_client = cache.adapter.get_client(write=False)

        assert write_client is not None
        assert read_client is not None

    def test_connection_pool_caching(self, cache: RespCache):
        pool1 = cache.adapter._get_connection_pool(write=True)
        pool2 = cache.adapter._get_connection_pool(write=True)

        assert pool1 is pool2

    def test_client_is_cached_per_pool(self, cache: RespCache):
        # Regression: a brand-new client per command left one uncollectable
        # cyclic object (the WRONGTYPE patch) behind on every cache call.
        assert cache.adapter.get_client(write=True) is cache.adapter.get_client(write=True)

    @pytest.mark.asyncio
    async def test_async_client_is_cached_per_pool(self, cache: RespCache):
        skip_without_generic_async_pool(cache)

        assert await cache.adapter.get_async_client(write=True) is await cache.adapter.get_async_client(write=True)

    def test_count_form_pop_reports_a_missing_key_as_none(self, cache: RespCache):
        # Regression: the driver's nil reply collapsed to [], so a miss looked
        # like an empty pop; LocMemCache returns None either way.
        cache.delete("missing_list")

        assert cache.lpop("missing_list", count=2) is None
        assert cache.rpop("missing_list", count=2) is None

    def test_pipeline_translates_wrongtype(self, cache: RespCache):
        # Regression: the client-instance patch never reached the driver's
        # pipeline, so batched type errors escaped as raw ResponseErrors.
        cache.set("wrongtype_pipeline", 1)
        try:
            pipe = cache.pipeline()
            pipe.lpush("wrongtype_pipeline", "value")
            with pytest.raises(WrongTypeError):
                pipe.execute()
        finally:
            cache.delete("wrongtype_pipeline")

    @pytest.mark.asyncio
    async def test_async_pipeline_translates_wrongtype(self, cache: RespCache):
        await cache.aset("wrongtype_apipeline", 1)
        try:
            pipe = await cache.apipeline()
            pipe.lpush("wrongtype_apipeline", "value")
            with pytest.raises(WrongTypeError):
                await pipe.execute()
        finally:
            await cache.adelete("wrongtype_apipeline")

    def test_xpending_filters_require_count(self, cache: RespCache):
        # Regression: without count the range/consumer filters were dropped and
        # the summary dict came back instead of the per-message list.
        with pytest.raises(ValueError, match="xpending\\(\\) requires count"):
            cache.xpending("stream", "group", start="-", end="+")

    def test_multiple_servers_pool_selection(self, redis_container: RedisContainerInfo):
        # The same URL three times: the index, not the endpoint, is what matters here.
        url = f"redis://{redis_container.host}:{redis_container.port}/1"

        with redis_cache([url, url, url]) as cache:
            assert cache.adapter._get_connection_pool_index(write=True) == 0
            assert 0 <= cache.adapter._get_connection_pool_index(write=False) < 3


class TestConnectionCleanup:
    """Tests for connection pool cleanup behavior."""

    def test_sync_pool_is_cached_per_instance(self, cache: RespCache):
        pool1 = cache.adapter._get_connection_pool(write=True)
        pool2 = cache.adapter._get_connection_pool(write=True)
        assert pool1 is pool2

        assert 0 in cache.adapter._pools
        assert cache.adapter._pools[0] is pool1

    @pytest.mark.asyncio
    async def test_async_pool_is_cached_per_event_loop(self, cache: RespCache):
        skip_without_generic_async_pool(cache)

        pool1 = cache.adapter._get_async_connection_pool(write=True)
        pool2 = cache.adapter._get_async_connection_pool(write=True)
        assert pool1 is pool2

        loop = asyncio.get_running_loop()
        async_pools = cache.adapter._async_pools
        assert loop in async_pools
        assert pool1 in async_pools[loop].values()

    def test_async_pool_different_per_loop(self, redis_container: RedisContainerInfo):
        """Each event loop gets its own pool and its own registry entry.

        Stays synchronous and drives its own loops, so pytest-asyncio's loop
        management does not interfere.
        """
        location = f"redis://{redis_container.host}:{redis_container.port}/1"

        with redis_cache(location) as cache:
            adapter = cache.adapter

            async def get_pool():
                return adapter._get_async_connection_pool(write=True)

            loop1 = asyncio.new_event_loop()
            loop2 = asyncio.new_event_loop()
            try:
                pool1 = loop1.run_until_complete(get_pool())
                pool2 = loop2.run_until_complete(get_pool())

                assert pool1 is not pool2
                assert loop1 in adapter._async_pools
                assert loop2 in adapter._async_pools
            finally:
                loop1.close()
                loop2.close()

    def test_close_keeps_sync_pools(self, cache: RespCache):
        """Django fires close() on every request_finished, so the sync pool has to survive it."""
        pool = cache.adapter._get_connection_pool(write=True)

        cache.adapter.close()

        assert cache.adapter._pools[0] is pool

    @pytest.mark.asyncio
    async def test_aclose_disconnects_the_running_loops_pools(self, cache: RespCache):
        skip_without_generic_async_pool(cache)
        pool = cache.adapter._get_async_connection_pool(write=True)
        loop = asyncio.get_running_loop()
        assert pool in cache.adapter._async_pools[loop].values()

        await cache.adapter.aclose()

        assert loop not in cache.adapter._async_pools
        assert cache.adapter._get_async_connection_pool(write=True) is not pool

    @pytest.mark.asyncio
    async def test_async_pool_shared_across_per_task_client_instances(
        self,
        cache: RespCache,
    ) -> None:
        """Regression: a fresh adapter reuses the existing pool.

        Django's ``asgiref.local.Local``-backed cache handler returns a fresh
        ``BaseCache`` instance per asyncio task, which means a fresh adapter
        is built on every async request. Before the process-wide
        ``_async_pools`` registry, each fresh client created its own pool, so
        every async cache call opened a new TCP connection instead of reusing
        the one from the prior call. This locks in the fix.
        """
        skip_without_generic_async_pool(cache)

        original_pool = cache.adapter._get_async_connection_pool(write=True)

        # What Django's per-task Local does on every request.
        cls = type(cache.adapter)
        fresh_client = cls(cache.adapter._servers, **cache.adapter._options)

        fresh_pool = fresh_client._get_async_connection_pool(write=True)
        assert fresh_pool is original_pool, (
            "Fresh per-task client created a new pool; process-wide registry not working."
        )

        another_client = cls(cache.adapter._servers, **cache.adapter._options)
        assert another_client._get_async_connection_pool(write=True) is original_pool

    def test_weak_key_dictionary_cleanup_on_loop_gc(self, redis_container: RedisContainerInfo):
        """A collected event loop takes its registry entry with it.

        Holds only for a pool that never opened a connection: nothing then
        points back at the loop, so the weak key can expire on its own.
        """
        location = f"redis://{redis_container.host}:{redis_container.port}/1"

        with redis_cache(location) as cache:
            adapter = cache.adapter
            async_pools = adapter._async_pools

            async def create_pool():
                return adapter._get_async_connection_pool(write=True)

            loop = asyncio.new_event_loop()
            pool = loop.run_until_complete(create_pool())
            loop.close()
            assert loop in async_pools

            loop_ref = weakref.ref(loop)
            del loop, pool
            gc.collect()

            assert loop_ref() is None
            assert [entry for entry in async_pools if entry.is_closed()] == []

    def test_pools_of_closed_loops_are_evicted(
        self,
        redis_container: RedisContainerInfo,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Regression: one pool and one connection leaked per event loop.

        ``async_to_sync`` from a plain sync thread runs ``asyncio.run()`` per
        call. Each pool pins its own loop through the transport of every
        connection it opened, so weak keys alone never freed the entries and
        the registry grew without bound.
        """
        monkeypatch.setattr(RedisPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
        location = f"redis://{redis_container.host}:{redis_container.port}/1"

        with redis_cache(location) as cache:
            registry = cache.adapter._async_pools

            async_to_sync(cache.aset)("loop_churn", "value")
            first_slot = next(iter(registry.values()))
            pool_ref = weakref.ref(next(iter(first_slot.values())))
            del first_slot

            for _ in range(5):
                assert async_to_sync(cache.aget)("loop_churn") == "value"

            # One entry: the loop of the last call, swept by the call after it.
            assert len(registry) == 1
            gc.collect()
            assert pool_ref() is None, "a pool stayed reachable after its event loop closed"

            cache.delete("loop_churn")

    @pytest.mark.asyncio
    async def test_async_pool_reuse_after_operations(self, cache: RespCache):
        skip_without_generic_async_pool(cache)

        loop = asyncio.get_running_loop()
        original_write_pool = cache.adapter._get_async_connection_pool(write=True)

        await cache.aset("test_reuse_1", "value1")
        await cache.aset("test_reuse_2", "value2")
        await cache.aget("test_reuse_1")
        await cache.adelete("test_reuse_1")

        assert cache.adapter._get_async_connection_pool(write=True) is original_write_pool
        assert 1 <= len(cache.adapter._async_pools.get(loop, {})) <= len(cache.adapter._servers)

        await cache.adelete("test_reuse_2")

    @pytest.mark.asyncio
    async def test_mixed_sync_async_operations(self, cache: RespCache):
        skip_without_generic_async_pool(cache)

        cache.set("sync_key", "sync_value")
        sync_pool = cache.adapter._get_connection_pool(write=True)

        await cache.aset("async_key", "async_value")
        async_pool = cache.adapter._get_async_connection_pool(write=True)

        assert sync_pool is not async_pool
        assert 0 in cache.adapter._pools
        assert asyncio.get_running_loop() in cache.adapter._async_pools

        cache.delete("sync_key")
        await cache.adelete("async_key")

    def test_sync_then_nested_async_run(self, redis_container: RedisContainerInfo):
        """A WSGI thread does sync work, then drives a loop of its own on the same cache."""
        location = f"redis://{redis_container.host}:{redis_container.port}/1"

        with redis_cache(location) as cache:
            cache.set("wsgi_key", "wsgi_value")
            assert cache.get("wsgi_key") == "wsgi_value"

            async def async_work():
                await cache.aset("async_key", "async_value")
                assert await cache.aget("async_key") == "async_value"
                await cache.adelete("async_key")

            loop = asyncio.new_event_loop()
            loop.run_until_complete(async_work())
            loop.close()

            assert cache.get("wsgi_key") == "wsgi_value"
            cache.delete("wsgi_key")

    def test_multiple_sequential_event_loops(
        self,
        redis_container: RedisContainerInfo,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """A WSGI thread driving one loop per request keeps working, and keeps one entry.

        Each new loop sweeps out the pools of the loops that closed before it,
        so only the loop of the most recent request is still registered.
        """
        monkeypatch.setattr(RedisPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
        location = f"redis://{redis_container.host}:{redis_container.port}/1"

        with redis_cache(location) as cache:

            async def async_set_get(key, value):
                await cache.aset(key, value)
                return await cache.aget(key)

            for index in (1, 2, 3):
                cache.set(f"sync_{index}", f"value_{index}")
                loop = asyncio.new_event_loop()
                assert loop.run_until_complete(async_set_get(f"async_{index}", f"avalue_{index}")) == f"avalue_{index}"
                loop.close()

            assert len(cache.adapter._async_pools) == 1
            assert cache.get("sync_1") == "value_1"
            assert cache.get("sync_3") == "value_3"

            cache.delete_many([f"sync_{i}" for i in (1, 2, 3)] + [f"async_{i}" for i in (1, 2, 3)])
