"""Tests for miscellaneous cache operations: scan, decr_version, clear_all_versions, flush_db."""

import asyncio
import time
from typing import TYPE_CHECKING

import pytest

from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django_cachex.cache import RespCache


def _is_py_cluster(client_class: str, sentinel_mode: str | bool, resp_adapter: str) -> bool:
    """redis-py / valkey-py cluster can't combine per-node cursors, so SCAN raises.

    valkey-glide scans all nodes itself and returns combined keys.
    """
    return client_class == "cluster" and not sentinel_mode and resp_adapter in {"redis-py", "valkey-py"}


def test_scan_returns_keys(
    cache: RespCache,
    client_class: str,
    sentinel_mode: str | bool,
    resp_adapter: str,
):
    if _is_py_cluster(client_class, sentinel_mode, resp_adapter):
        with pytest.raises(NotSupportedError):
            cache.scan(pattern="scantest_*")
        return

    cache.set("scantest_a", 1)
    cache.set("scantest_b", 2)

    cursor, keys = cache.scan(pattern="scantest_*")
    assert isinstance(keys, list)
    all_keys = set(keys)
    while cursor != 0:
        cursor, keys = cache.scan(cursor=cursor, pattern="scantest_*")
        all_keys.update(keys)
    assert all_keys == {"scantest_a", "scantest_b"}


def test_scan_empty(
    cache: RespCache,
    client_class: str,
    sentinel_mode: str | bool,
    resp_adapter: str,
):
    if _is_py_cluster(client_class, sentinel_mode, resp_adapter):
        with pytest.raises(NotSupportedError):
            cache.scan(pattern="nonexistent_pattern_xyz_*")
        return

    _cursor, keys = cache.scan(pattern="nonexistent_pattern_xyz_*")
    assert keys == []


def test_scan_returns_an_undecodable_name_that_reads_and_deletes_its_own_key(
    cache: RespCache,
    client_class: str,
    sentinel_mode: str | bool,
    resp_adapter: str,
):
    if _is_py_cluster(client_class, sentinel_mode, resp_adapter):
        pytest.skip("SCAN raises on a redis-py or valkey-py cluster")
    cache.set("scanbad_\\xff", "escaped spelling")
    cache.get_client(write=True).set(cache.make_key("scanbad_").encode() + b"\xff", cache.encode("raw bytes"))

    cursor, keys = cache.scan(pattern="scanbad_*")
    all_keys = set(keys)
    while cursor != 0:
        cursor, keys = cache.scan(cursor=cursor, pattern="scanbad_*")
        all_keys.update(keys)

    assert all_keys == {"scanbad_\\xff", "scanbad_\udcff"}
    assert cache.get("scanbad_\udcff") == "raw bytes"
    assert cache.delete("scanbad_\udcff") is True
    assert cache.get("scanbad_\udcff") is None
    assert cache.get("scanbad_\\xff") == "escaped spelling"


def test_decr_version(cache: RespCache):
    # Use hash tag so versioned keys stay in same cluster slot
    cache.set("{dv}:key", "hello", version=2)
    new_version = cache.decr_version("{dv}:key", version=2)

    assert new_version == 1
    assert cache.get("{dv}:key", version=2) is None
    assert cache.get("{dv}:key", version=1) == "hello"


def test_decr_version_default(cache: RespCache):
    # Set at default version (1), decrement to version 0
    cache.set("{dv2}:key", "value")
    new_version = cache.decr_version("{dv2}:key")

    assert new_version == 0
    assert cache.get("{dv2}:key") is None
    assert cache.get("{dv2}:key", version=0) == "value"


def test_clear_all_versions(cache: RespCache):
    cache.set("cav_key1", "v1", version=1)
    cache.set("cav_key2", "v2", version=2)

    count = cache.clear_all_versions()
    assert count == 2

    assert cache.get("cav_key1", version=1) is None
    assert cache.get("cav_key2", version=2) is None


def test_flush_db(cache: RespCache):
    cache.set("flush_key", "value")
    assert cache.get("flush_key") == "value"

    result = cache.flush_db()
    assert result is True
    assert cache.get("flush_key") is None


@pytest.fixture
def _lazy_user_flush(
    cache: RespCache,
    client_class: str,
    sentinel_mode: str | bool,
    resp_adapter: str,
) -> Iterator[None]:
    """Turn on ``lazyfree-lazy-user-flush`` for one test, then restore it."""
    if client_class == "cluster" and not sentinel_mode:
        pytest.skip("CONFIG SET and INFO are per node on cluster")
    name = "lazyfree-lazy-user-flush"
    client = cache.get_client(write=True)
    if resp_adapter == "valkey-glide":
        old = client.config_get([name])[name.encode()]
        client.config_set({name: "yes"})
        yield
        client.config_set({name: old})
    else:
        old = client.config_get(name)[name]
        client.config_set(name, "yes")
        yield
        client.config_set(name, old)


def _lazyfreed_objects(cache: RespCache, at_least: int) -> int:
    """``lazyfreed_objects`` from INFO, polled for up to 5 s until it reaches ``at_least``.

    A lazy flush frees the keys in a background thread, after the reply.
    """
    deadline = time.monotonic() + 5
    while (freed := cache.info("memory")["lazyfreed_objects"]) < at_least and time.monotonic() < deadline:
        time.sleep(0.01)
    return freed


@pytest.mark.usefixtures("_lazy_user_flush")
def test_flush_db_follows_lazyfree_lazy_user_flush(cache: RespCache):
    before = cache.info("memory")["lazyfreed_objects"]
    cache.set_many({"lazy_a": 1, "lazy_b": 2, "lazy_c": 3})

    assert cache.flush_db() is True
    assert cache.get_many(["lazy_a", "lazy_b", "lazy_c"]) == {}
    assert _lazyfreed_objects(cache, at_least=before + 3) >= before + 3


@pytest.mark.asyncio
async def test_ascan_returns_keys(
    cache: RespCache,
    client_class: str,
    sentinel_mode: str | bool,
    resp_adapter: str,
):
    if _is_py_cluster(client_class, sentinel_mode, resp_adapter):
        with pytest.raises(NotSupportedError):
            await cache.ascan(pattern="ascantest_*")
        return

    cache.set("ascantest_a", 1)
    cache.set("ascantest_b", 2)

    cursor, keys = await cache.ascan(pattern="ascantest_*")
    assert isinstance(keys, list)
    all_keys = set(keys)
    while cursor != 0:
        cursor, keys = await cache.ascan(cursor=cursor, pattern="ascantest_*")
        all_keys.update(keys)
    assert all_keys == {"ascantest_a", "ascantest_b"}


@pytest.mark.asyncio
async def test_ascan_empty(
    cache: RespCache,
    client_class: str,
    sentinel_mode: str | bool,
    resp_adapter: str,
):
    if _is_py_cluster(client_class, sentinel_mode, resp_adapter):
        with pytest.raises(NotSupportedError):
            await cache.ascan(pattern="nonexistent_pattern_xyz_*")
        return

    _cursor, keys = await cache.ascan(pattern="nonexistent_pattern_xyz_*")
    assert keys == []


@pytest.mark.asyncio
async def test_ascan_returns_an_undecodable_name_that_reads_and_deletes_its_own_key(
    cache: RespCache,
    client_class: str,
    sentinel_mode: str | bool,
    resp_adapter: str,
):
    if _is_py_cluster(client_class, sentinel_mode, resp_adapter):
        pytest.skip("SCAN raises on a redis-py or valkey-py cluster")
    cache.set("ascanbad_\\xff", "escaped spelling")
    cache.get_client(write=True).set(cache.make_key("ascanbad_").encode() + b"\xff", cache.encode("raw bytes"))

    cursor, keys = await cache.ascan(pattern="ascanbad_*")
    all_keys = set(keys)
    while cursor != 0:
        cursor, keys = await cache.ascan(cursor=cursor, pattern="ascanbad_*")
        all_keys.update(keys)

    assert all_keys == {"ascanbad_\\xff", "ascanbad_\udcff"}
    assert await cache.aget("ascanbad_\udcff") == "raw bytes"
    assert await cache.adelete("ascanbad_\udcff") is True
    assert await cache.aget("ascanbad_\udcff") is None
    assert await cache.aget("ascanbad_\\xff") == "escaped spelling"


@pytest.fixture
def _skip_cluster(client_class: str, sentinel_mode: str | bool):
    """``RespClusterCache.alock`` raises NotSupportedError; see test_locks.py."""
    if client_class == "cluster" and not sentinel_mode:
        pytest.skip("alock is rejected on cluster")


@pytest.mark.usefixtures("_skip_cluster")
@pytest.mark.asyncio
async def test_alock_acquire_and_release(cache: RespCache):
    lock = await cache.alock("alock_resource")
    acquired = await lock.acquire(blocking=False)
    assert acquired is True
    assert cache.has_key("alock_resource") is True

    await lock.release()
    assert cache.has_key("alock_resource") is False


@pytest.mark.usefixtures("_skip_cluster")
@pytest.mark.asyncio
async def test_alock_prevents_double_acquire(cache: RespCache):
    lock1 = await cache.alock("alock_resource2")
    assert await lock1.acquire(blocking=False) is True

    lock2 = await cache.alock("alock_resource2")
    assert await lock2.acquire(blocking=False) is False

    await lock1.release()


@pytest.mark.usefixtures("_skip_cluster")
@pytest.mark.asyncio
async def test_alock_context_manager(cache: RespCache):
    async with await cache.alock("alock_ctx"):
        assert cache.has_key("alock_ctx") is True
    assert cache.has_key("alock_ctx") is False


@pytest.mark.asyncio
async def test_adecr_version(cache: RespCache):
    cache.set("{adv}:key", "hello", version=2)
    new_version = await cache.adecr_version("{adv}:key", version=2)

    assert new_version == 1
    assert cache.get("{adv}:key", version=2) is None
    assert cache.get("{adv}:key", version=1) == "hello"


@pytest.mark.asyncio
async def test_adecr_version_default(cache: RespCache):
    cache.set("{adv2}:key", "value")
    new_version = await cache.adecr_version("{adv2}:key")

    assert new_version == 0
    assert cache.get("{adv2}:key") is None
    assert cache.get("{adv2}:key", version=0) == "value"


@pytest.mark.asyncio
async def test_aclear_all_versions(cache: RespCache):
    cache.set("acav_key1", "v1", version=1)
    cache.set("acav_key2", "v2", version=2)

    count = await cache.aclear_all_versions()
    assert count == 2

    assert cache.get("acav_key1", version=1) is None
    assert cache.get("acav_key2", version=2) is None


@pytest.mark.asyncio
async def test_aget_or_set_missing_key(cache: RespCache):
    result = await cache.aget_or_set("agos_key", "default_value")
    assert result == "default_value"
    assert cache.get("agos_key") == "default_value"


@pytest.mark.asyncio
async def test_aget_or_set_existing_key(cache: RespCache):
    cache.set("agos_key2", "existing")
    result = await cache.aget_or_set("agos_key2", "default_value")
    assert result == "existing"


@pytest.mark.asyncio
async def test_aget_or_set_with_callable(cache: RespCache):
    result = await cache.aget_or_set("agos_key3", lambda: "computed")
    assert result == "computed"
    assert cache.get("agos_key3") == "computed"


@pytest.mark.asyncio
async def test_aget_or_set_awaits_an_async_default(cache: RespCache):
    async def compute() -> str:
        await asyncio.sleep(0)
        return "awaited"

    class AsyncCallable:
        async def __call__(self) -> str:
            return "awaited via __call__"

    assert await cache.aget_or_set("agos_async", compute) == "awaited"
    assert cache.get("agos_async") == "awaited"
    assert await cache.aget_or_set("agos_async_call", AsyncCallable()) == "awaited via __call__"
    assert cache.get("agos_async_call") == "awaited via __call__"


@pytest.mark.asyncio
async def test_aget_or_set_does_not_call_the_default_on_a_hit(cache: RespCache):
    cache.set("agos_hit", "existing")

    async def compute() -> str:
        raise AssertionError("default computed on a hit")

    assert await cache.aget_or_set("agos_hit", compute) == "existing"


@pytest.mark.asyncio
async def test_aflush_db(cache: RespCache):
    cache.set("aflush_key", "value")
    assert cache.get("aflush_key") == "value"

    result = await cache.aflush_db()
    assert result is True
    assert cache.get("aflush_key") is None


@pytest.mark.usefixtures("_lazy_user_flush")
@pytest.mark.asyncio
async def test_aflush_db_follows_lazyfree_lazy_user_flush(cache: RespCache):
    before = cache.info("memory")["lazyfreed_objects"]
    cache.set_many({"alazy_a": 1, "alazy_b": 2, "alazy_c": 3})

    assert await cache.aflush_db() is True
    assert cache.get_many(["alazy_a", "alazy_b", "alazy_c"]) == {}
    assert _lazyfreed_objects(cache, at_least=before + 3) >= before + 3
