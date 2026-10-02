"""Tests for Redis cache internals matching Django's RedisCacheTests.

These tests mirror Django's RedisCacheTests from django/tests/cache/tests.py
to ensure django-cachex internals match Django's official Redis cache backend.

Reference: https://github.com/django/django/blob/main/tests/cache/tests.py
"""

import asyncio
import enum
import gc
import importlib
import weakref
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import pytest
from asgiref.sync import async_to_sync
from django.core.cache import caches
from django.test import override_settings

from django_cachex.adapters import RedisPyAdapter
from django_cachex.exceptions import WrongTypeError
from tests.fixtures.cache import ADAPTER_IMAGES

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django_cachex.cache import RespCache
    from tests.fixtures.containers import RedisContainerInfo


class _Priority(enum.IntEnum):
    # Module level so pickle can resolve it by qualname.
    LOW = 1
    HIGH = 3


@contextmanager
def redis_cache(
    location: str | list[str],
    *,
    backend: str = "django_cachex.cache.RedisCache",
    **options: Any,
) -> Iterator[RespCache]:
    """Yield a cache (a RedisCache unless ``backend`` names another) built from ``location``, outside the adapter matrix.

    ``override_settings(CACHES=...)`` rebuilds Django's cache handler on entry
    and on exit, so the caller needs no teardown of its own.
    """
    config: dict[str, Any] = {"BACKEND": backend, "LOCATION": location}
    if options:
        config["OPTIONS"] = options
    with override_settings(CACHES={"default": config}):
        yield caches["default"]


def skip_without_generic_async_pool(cache: RespCache) -> None:
    """Skip adapters that manage their own async pools instead of ``_async_pool_class``.

    Cluster and Sentinel both have async clients; they just reach them
    through the cluster registry and the Sentinel pool class respectively.
    """
    if not hasattr(cache.adapter, "_async_pool_class"):
        pytest.skip("valkey-glide has no async pools, only one client per event loop")
    if cache.adapter._async_pool_class is None:
        pytest.skip("Cluster and Sentinel adapters manage their own async pools")


def test_incr_write_connection(cache: RespCache, resp_adapter: str, mocker):
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide routes writes to the primary itself, not through get_client(write=True)")
    cache.set("number", 42)
    mocked_get_client = mocker.patch.object(cache.adapter, "get_client", wraps=cache.adapter.get_client)
    cache.incr("number")
    assert mocked_get_client.call_args.kwargs.get("write") is True


def test_adapter_class(cache: RespCache, resp_adapter: str):
    assert cache._adapter_class.__module__ == f"django_cachex.adapters.{resp_adapter.replace('-', '_')}"
    assert isinstance(cache.adapter, cache._adapter_class)


def test_get_backend_timeout_method(cache: RespCache):
    assert cache.get_backend_timeout(10) == 10
    # A negative timeout means expire immediately, not "no expiry".
    assert cache.get_backend_timeout(-5) == 0
    assert cache.get_backend_timeout(None) is None


def test_get_connection_pool_index(cache: RespCache, resp_adapter: str):
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide has no pool per server; its client routes reads to replicas itself")
    assert cache.adapter._get_connection_pool_index(write=True) == 0

    pool_index = cache.adapter._get_connection_pool_index(write=False)
    if len(cache.adapter._servers) == 1:
        assert pool_index == 0
    else:
        assert 1 <= pool_index < len(cache.adapter._servers)


def test_get_connection_pool(cache: RespCache, resp_adapter: str):
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide has no connection pools, only one multiplexed client per config")
    driver = importlib.import_module(ADAPTER_IMAGES[resp_adapter][1])

    assert isinstance(cache.adapter._get_connection_pool(write=True), driver.ConnectionPool)
    assert isinstance(cache.adapter._get_connection_pool(write=False), driver.ConnectionPool)


def test_get_client(cache: RespCache, resp_adapter: str):
    """Test client creation returns the driver's Redis or RedisCluster instance."""
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide's get_client() returns an error-translating proxy over its client")
    driver = importlib.import_module(ADAPTER_IMAGES[resp_adapter][1])

    client = cache.adapter.get_client()
    assert isinstance(client, (driver.Redis, driver.RedisCluster))


def test_serializer_dumps(cache: RespCache):
    """Test serialization: integers stay as-is, bools/strings become bytes.

    We test via the encode() method which handles the integer optimization.
    Django's test checks _serializer.dumps() but our architecture uses encode().
    """
    assert cache.encode(123) == 123
    assert isinstance(cache.encode(True), bytes)
    assert isinstance(cache.encode("abc"), bytes)


def test_encode_serializes_int_subclasses(cache: RespCache):
    """Only exact int passes through; IntEnum is serialized so its type survives.

    Regression: isinstance-based dispatch stored IntEnum members as bare
    ints, so they came back as plain int.
    """
    encoded = cache.encode(_Priority.HIGH)
    assert isinstance(encoded, bytes)
    assert cache.decode(encoded) is _Priority.HIGH


def test_bool_roundtrip(cache: RespCache):
    cache.set("internals_bool_true", True)
    assert cache.get("internals_bool_true") is True
    cache.set("internals_bool_false", False)
    assert cache.get("internals_bool_false") is False


def test_int_enum_roundtrip(cache: RespCache):
    cache.set("internals_enum", _Priority.HIGH)
    result = cache.get("internals_enum")
    assert result is _Priority.HIGH
    assert type(result) is _Priority


def test_plain_int_roundtrip(cache: RespCache):
    cache.set("internals_int", 123)
    result = cache.get("internals_int")
    assert result == 123
    assert type(result) is int


def test_get_client_write_vs_read_bind_their_own_pools(cache: RespCache, client_class: str, resp_adapter: str):
    write_client = cache.adapter.get_client(write=True)
    read_client = cache.adapter.get_client(write=False)

    if client_class == "cluster" or resp_adapter == "valkey-glide":
        assert write_client is read_client
        return
    assert write_client.connection_pool is cache.adapter._pools[0]
    assert read_client.connection_pool in cache.adapter._pools.values()
    if len(cache.adapter._servers) > 1:
        assert read_client.connection_pool is not write_client.connection_pool


def test_connection_pool_caching(cache: RespCache, resp_adapter: str):
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide has no connection pools, only one multiplexed client per config")
    pool1 = cache.adapter._get_connection_pool(write=True)
    pool2 = cache.adapter._get_connection_pool(write=True)

    assert pool1 is pool2


def test_client_is_cached_per_pool(cache: RespCache):
    # Regression: a brand-new client per command left one uncollectable
    # cyclic object (the WRONGTYPE patch) behind on every cache call.
    assert cache.adapter.get_client(write=True) is cache.adapter.get_client(write=True)


@pytest.mark.asyncio
async def test_async_client_is_cached_per_pool(cache: RespCache):
    skip_without_generic_async_pool(cache)

    assert await cache.adapter.get_async_client(write=True) is await cache.adapter.get_async_client(write=True)


def test_count_form_pop_reports_a_missing_key_as_none(cache: RespCache):
    # Regression: the driver's nil reply collapsed to [], so a miss looked
    # like an empty pop; LocMemCache returns None either way.
    cache.delete("missing_list")

    assert cache.lpop("missing_list", count=2) is None
    assert cache.rpop("missing_list", count=2) is None


def test_pipeline_translates_wrongtype(cache: RespCache):
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
async def test_async_pipeline_translates_wrongtype(cache: RespCache):
    await cache.aset("wrongtype_apipeline", 1)
    try:
        pipe = await cache.apipeline()
        pipe.lpush("wrongtype_apipeline", "value")
        with pytest.raises(WrongTypeError):
            await pipe.execute()
    finally:
        await cache.adelete("wrongtype_apipeline")


def test_xpending_filters_require_count(cache: RespCache):
    # Regression: without count the range/consumer filters were dropped and
    # the summary dict came back instead of the per-message list.
    with pytest.raises(ValueError, match="xpending\\(\\) requires count"):
        cache.xpending("stream", "group", start="-", end="+")


def test_multiple_servers_pool_selection(redis_container: RedisContainerInfo, mocker):
    # The same URL three times: the index, not the endpoint, is what matters here.
    url = f"redis://{redis_container.host}:{redis_container.port}/1"

    with redis_cache([url, url, url]) as cache:
        assert cache.adapter._get_connection_pool_index(write=True) == 0
        randint = mocker.patch("django_cachex.adapters.valkey_py.random.randint", return_value=2)
        assert cache.adapter._get_connection_pool_index(write=False) == 2
        randint.assert_called_once_with(1, 2)


def test_sync_pools_shared_across_per_thread_cache_instances(cache: RespCache):
    """Django builds an instance per thread and per task; a pool per instance reconnected for each."""
    write_client = cache.adapter.get_client(write=True)
    read_client = cache.adapter.get_client(write=False)

    fresh = caches.create_connection("default")

    assert fresh.adapter.get_client(write=True) is write_client
    assert fresh.adapter.get_client(write=False) is read_client


DRIVER_BACKENDS = ["django_cachex.cache.ValkeyCache", "django_cachex.cache.RedisCache"]


@pytest.mark.parametrize("backend", DRIVER_BACKENDS)
def test_retry_on_error_list_keeps_one_sync_pool_across_instances(redis_container: RedisContainerInfo, backend: str):
    retry_on_error: list[type[Exception]] = [ConnectionError]
    location = f"redis://{redis_container.host}:{redis_container.port}/1"

    with redis_cache(location, backend=backend, retry_on_timeout=True, retry_on_error=retry_on_error) as cache:
        cache.set("retry_list_sync", 1)
        fresh = caches.create_connection("default")

        assert fresh.adapter.get_client(write=True) is cache.adapter.get_client(write=True)
        cache.delete("retry_list_sync")

    assert retry_on_error == [ConnectionError]


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", DRIVER_BACKENDS)
async def test_retry_on_error_list_keeps_one_async_pool_across_instances(
    redis_container: RedisContainerInfo,
    backend: str,
):
    retry_on_error: list[type[Exception]] = [ConnectionError]
    location = f"redis://{redis_container.host}:{redis_container.port}/1"

    with redis_cache(location, backend=backend, retry_on_timeout=True, retry_on_error=retry_on_error) as cache:
        fresh = None
        try:
            await cache.aset("retry_list_async", 1)
            fresh = caches.create_connection("default")

            assert await fresh.adapter.get_async_client(write=True) is await cache.adapter.get_async_client(write=True)
            await cache.adelete("retry_list_async")
        finally:
            await cache.aclose()
            if fresh is not None:
                await fresh.aclose()

    assert retry_on_error == [ConnectionError]


@pytest.mark.asyncio
async def test_async_pool_is_cached_per_event_loop(cache: RespCache):
    skip_without_generic_async_pool(cache)

    pool1 = cache.adapter._get_async_connection_pool(write=True)
    pool2 = cache.adapter._get_async_connection_pool(write=True)
    assert pool1 is pool2

    loop = asyncio.get_running_loop()
    async_pools = cache.adapter._async_pools
    assert loop in async_pools
    assert pool1 in async_pools[loop].values()


def test_async_pool_different_per_loop(redis_container: RedisContainerInfo):
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


def test_close_keeps_sync_pools(cache: RespCache, resp_adapter: str):
    """Django fires close() on every request_finished, so the sync pool has to survive it."""
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide has no connection pools, only one multiplexed client per config")
    pool = cache.adapter._get_connection_pool(write=True)

    cache.adapter.close()

    assert cache.adapter._pools[0] is pool


def test_close_leaves_other_instances_connected(cache: RespCache, client_class: str, resp_adapter: str):
    """One thread's request_finished closes its instance while other threads keep using theirs."""
    if client_class == "cluster" and resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide sends CLIENT ID to a random cluster node")
    other = caches.create_connection("default")
    assert other.adapter.get_client(write=True) is cache.adapter.get_client(write=True)
    connection_id = other.adapter.get_client(write=True).client_id()

    cache.close()

    assert other.adapter.get_client(write=True).client_id() == connection_id


@pytest.mark.asyncio
async def test_aclose_disconnects_the_running_loops_pools(cache: RespCache):
    skip_without_generic_async_pool(cache)
    pool = cache.adapter._get_async_connection_pool(write=True)
    loop = asyncio.get_running_loop()
    assert pool in cache.adapter._async_pools[loop].values()

    await cache.adapter.aclose()

    # Only this adapter's own entries go; another alias on the loop keeps
    # its pool, so the loop slot itself survives.
    assert pool not in cache.adapter._async_pools.get(loop, {}).values()
    assert cache.adapter._get_async_connection_pool(write=True) is not pool


@pytest.mark.asyncio
async def test_async_pool_shared_across_per_task_client_instances(
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
    assert fresh_pool is original_pool, "Fresh per-task client created a new pool; process-wide registry not working."

    another_client = cls(cache.adapter._servers, **cache.adapter._options)
    assert another_client._get_async_connection_pool(write=True) is original_pool


def test_weak_key_dictionary_cleanup_on_loop_gc(redis_container: RedisContainerInfo):
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
        try:
            pool = loop.run_until_complete(create_pool())
        finally:
            loop.close()
        assert loop in async_pools

        loop_ref = weakref.ref(loop)
        del loop, pool
        gc.collect()

        assert loop_ref() is None
        assert [entry for entry in async_pools if entry.is_closed()] == []


def test_pools_of_closed_loops_are_evicted(
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
async def test_async_pool_reuse_after_operations(cache: RespCache):
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
async def test_mixed_sync_async_operations(cache: RespCache):
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


def test_sync_then_nested_async_run(redis_container: RedisContainerInfo):
    """A WSGI thread does sync work, then drives a loop of its own on the same cache."""
    location = f"redis://{redis_container.host}:{redis_container.port}/1"

    with redis_cache(location) as cache:
        cache.set("wsgi_key", "wsgi_value")
        assert cache.get("wsgi_key") == "wsgi_value"

        async def async_work():
            try:
                await cache.aset("async_key", "async_value")
                assert await cache.aget("async_key") == "async_value"
                await cache.adelete("async_key")
            finally:
                # The pools belong to this loop; closing it without
                # disconnecting them leaves their sockets to the collector.
                await cache.aclose()

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(async_work())
        finally:
            loop.close()

        assert cache.get("wsgi_key") == "wsgi_value"
        cache.delete("wsgi_key")


def test_multiple_sequential_event_loops(
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
            try:
                await cache.aset(key, value)
                return await cache.aget(key)
            finally:
                await cache.aclose()

        for index in (1, 2, 3):
            cache.set(f"sync_{index}", f"value_{index}")
            loop = asyncio.new_event_loop()
            try:
                assert loop.run_until_complete(async_set_get(f"async_{index}", f"avalue_{index}")) == f"avalue_{index}"
            finally:
                loop.close()

        assert len(cache.adapter._async_pools) == 1
        assert cache.get("sync_1") == "value_1"
        assert cache.get("sync_3") == "value_3"

        cache.delete_many([f"sync_{i}" for i in (1, 2, 3)] + [f"async_{i}" for i in (1, 2, 3)])
