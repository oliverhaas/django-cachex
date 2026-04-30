"""Tests for the Rust-driver-backed cache backends.

These hit the same surface as the existing redis-py-backed tests but use
``RustValkeyCache`` so the Rust connection layer + PyO3 driver get
exercised end-to-end behind Django's cache API.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from django.test import override_settings

from django_cachex._rust_clients import _reset_for_tests
from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django_cachex.cache import KeyValueCache
    from tests.fixtures.containers import RedisContainerInfo


@pytest.fixture(autouse=True)
def _clear_rust_registry() -> Iterator[None]:
    """Driver registry leaks between tests if not cleared (per-test flushdb)."""
    _reset_for_tests()
    yield
    _reset_for_tests()


@pytest.fixture
def rust_cache(redis_container: RedisContainerInfo) -> Iterator[KeyValueCache]:
    location = f"redis://{redis_container.host}:{redis_container.port}/0"
    caches = {
        "default": {
            "BACKEND": "django_cachex.cache.RustValkeyCache",
            "LOCATION": location,
            "OPTIONS": {},
        },
    }
    with override_settings(CACHES=caches):
        from django.core.cache import cache

        cache.flush_db()
        yield cache  # type: ignore[misc]
        cache.flush_db()


# ----------------------------------------------------------------------- core


def test_set_get(rust_cache):
    rust_cache.set("k", "v", timeout=None)
    assert rust_cache.get("k") == "v"


def test_get_missing(rust_cache):
    assert rust_cache.get("missing") is None
    assert rust_cache.get("missing", default="fallback") == "fallback"


def test_add_only_when_absent(rust_cache):
    assert rust_cache.add("k", "v1", timeout=None) is True
    assert rust_cache.add("k", "v2", timeout=None) is False
    assert rust_cache.get("k") == "v1"


def test_delete(rust_cache):
    rust_cache.set("k", "v", timeout=None)
    assert rust_cache.delete("k") is True
    assert rust_cache.delete("k") is False


def test_delete_many(rust_cache):
    rust_cache.set_many({"a": 1, "b": 2, "c": 3}, timeout=None)
    rust_cache.delete_many(["a", "b", "missing"])
    assert rust_cache.get("a") is None
    assert rust_cache.get("c") == 3


def test_set_many_get_many(rust_cache):
    rust_cache.set_many({"a": 1, "b": "two", "c": [1, 2, 3]}, timeout=None)
    result = rust_cache.get_many(["a", "b", "c", "missing"])
    assert result == {"a": 1, "b": "two", "c": [1, 2, 3]}


def test_has_key(rust_cache):
    assert rust_cache.has_key("k") is False
    rust_cache.set("k", 1, timeout=None)
    assert rust_cache.has_key("k") is True


def test_clear(rust_cache):
    rust_cache.set_many({"a": 1, "b": 2}, timeout=None)
    rust_cache.clear()
    assert rust_cache.get("a") is None
    assert rust_cache.get("b") is None


def test_incr(rust_cache):
    rust_cache.set("counter", 10, timeout=None)
    assert rust_cache.incr("counter") == 11
    assert rust_cache.incr("counter", delta=5) == 16


def test_touch(rust_cache):
    rust_cache.set("k", "v", timeout=None)
    assert rust_cache.touch("k", timeout=100) is True
    ttl = rust_cache.ttl("k")
    assert ttl is not None
    assert 0 < ttl <= 100


# ----------------------------------------------------------------- TTL ops


def test_ttl_no_expiry(rust_cache):
    rust_cache.set("k", "v", timeout=None)
    assert rust_cache.ttl("k") is None


def test_ttl_missing_returns_minus_two(rust_cache):
    assert rust_cache.ttl("missing") == -2


def test_expire_persist_roundtrip(rust_cache):
    rust_cache.set("k", "v", timeout=None)
    assert rust_cache.expire("k", 50) is True
    ttl = rust_cache.ttl("k")
    assert ttl is not None and 0 < ttl <= 50
    assert rust_cache.persist("k") is True
    assert rust_cache.ttl("k") is None


def test_expireat_via_eval(rust_cache):
    import time

    rust_cache.set("k", "v", timeout=None)
    when = int(time.time()) + 60
    assert rust_cache.expire_at("k", when) is True
    ttl = rust_cache.ttl("k")
    assert ttl is not None and 0 < ttl <= 60


# ------------------------------------------------------------- pattern / scan


def test_keys_pattern(rust_cache):
    rust_cache.set("u:1", 1, timeout=None)
    rust_cache.set("u:2", 2, timeout=None)
    rust_cache.set("other", 3, timeout=None)
    found = sorted(rust_cache.keys("u:*"))
    assert found == ["u:1", "u:2"]


def test_delete_pattern(rust_cache):
    for i in range(5):
        rust_cache.set(f"u:{i}", i, timeout=None)
    rust_cache.set("keep", "me", timeout=None)
    deleted = rust_cache.delete_pattern("u:*")
    assert deleted == 5
    assert rust_cache.get("keep") == "me"


def test_iter_keys(rust_cache):
    for i in range(5):
        rust_cache.set(f"u:{i}", i, timeout=None)
    found = sorted(rust_cache.iter_keys("u:*"))
    assert found == ["u:0", "u:1", "u:2", "u:3", "u:4"]


# -------------------------------------------------------------------- type


def test_type(rust_cache):
    from django_cachex.types import KeyType

    rust_cache.set("s", "value", timeout=None)
    rust_cache.lpush("l", "a")
    assert rust_cache.type("s") == KeyType.STRING
    assert rust_cache.type("l") == KeyType.LIST
    assert rust_cache.type("missing") is None


# ------------------------------------------------------------------- hashes


def test_hash_basic(rust_cache):
    rust_cache.hset("h", "f1", "v1")
    rust_cache.hset("h", "f2", 42)
    assert rust_cache.hget("h", "f1") == "v1"
    assert rust_cache.hget("h", "f2") == 42
    assert rust_cache.hlen("h") == 2
    assert sorted(rust_cache.hkeys("h")) == ["f1", "f2"]
    assert rust_cache.hexists("h", "f1") is True
    assert rust_cache.hexists("h", "missing") is False


def test_hgetall(rust_cache):
    rust_cache.hset("h", mapping={"a": 1, "b": "two"})
    assert rust_cache.hgetall("h") == {"a": 1, "b": "two"}


def test_hdel(rust_cache):
    rust_cache.hset("h", mapping={"a": 1, "b": 2, "c": 3})
    assert rust_cache.hdel("h", "a", "missing") == 1
    assert sorted(rust_cache.hkeys("h")) == ["b", "c"]


def test_hincrby(rust_cache):
    rust_cache.hset("h", "n", 10)
    assert rust_cache.hincrby("h", "n", 5) == 15
    # Float increments go through Lua eval — check it doesn't blow up.
    assert rust_cache.hincrbyfloat("h", "f", 1.5) == 1.5


def test_hsetnx(rust_cache):
    assert rust_cache.hsetnx("h", "f", "v1") is True
    assert rust_cache.hsetnx("h", "f", "v2") is False
    assert rust_cache.hget("h", "f") == "v1"


# -------------------------------------------------------------------- lists


def test_list_basic(rust_cache):
    rust_cache.rpush("l", "a", "b", "c")
    rust_cache.lpush("l", "z")
    assert rust_cache.lrange("l", 0, -1) == ["z", "a", "b", "c"]
    assert rust_cache.llen("l") == 4
    assert rust_cache.lindex("l", 0) == "z"
    assert rust_cache.lpop("l") == "z"
    assert rust_cache.rpop("l") == "c"


def test_list_lset_lrem_ltrim(rust_cache):
    rust_cache.rpush("l", "a", "x", "a", "x", "a")
    assert rust_cache.lrem("l", 2, "a") == 2
    rust_cache.lset("l", 0, "X")
    assert rust_cache.lrange("l", 0, -1) == ["X", "x", "a"]
    rust_cache.ltrim("l", 0, 1)
    assert rust_cache.lrange("l", 0, -1) == ["X", "x"]


def test_list_linsert(rust_cache):
    rust_cache.rpush("l", "a", "c")
    assert rust_cache.linsert("l", "BEFORE", "c", "b") == 3
    assert rust_cache.lrange("l", 0, -1) == ["a", "b", "c"]


# --------------------------------------------------------------------- sets


def test_set_ops(rust_cache):
    assert rust_cache.sadd("s", "a", "b", "c") == 3
    assert sorted(rust_cache.smembers("s")) == ["a", "b", "c"]
    assert rust_cache.sismember("s", "a") is True
    assert rust_cache.sismember("s", "z") is False
    assert rust_cache.scard("s") == 3
    assert rust_cache.srem("s", "a") == 1


def test_sinter_sunion_sdiff(rust_cache):
    rust_cache.sadd("s1", "a", "b", "c")
    rust_cache.sadd("s2", "b", "c", "d")
    assert sorted(rust_cache.sinter(["s1", "s2"])) == ["b", "c"]
    assert sorted(rust_cache.sunion(["s1", "s2"])) == ["a", "b", "c", "d"]
    assert sorted(rust_cache.sdiff(["s1", "s2"])) == ["a"]


# ------------------------------------------------------------------- zsets


def test_zset_basic(rust_cache):
    rust_cache.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert rust_cache.zrange("z", 0, -1) == ["a", "b", "c"]
    assert rust_cache.zrange("z", 0, -1, withscores=True) == [
        ("a", 1.0),
        ("b", 2.0),
        ("c", 3.0),
    ]
    assert rust_cache.zcard("z") == 3
    assert rust_cache.zscore("z", "a") == 1.0
    assert rust_cache.zrank("z", "b") == 1
    assert rust_cache.zrem("z", "a") == 1


def test_zrangebyscore(rust_cache):
    rust_cache.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert rust_cache.zrangebyscore("z", 2.0, 3.0) == ["b", "c"]


def test_zpopmin_zpopmax(rust_cache):
    rust_cache.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})
    popped_min = rust_cache.zpopmin("z", count=1)
    assert popped_min == [("a", 1.0)]
    popped_max = rust_cache.zpopmax("z", count=1)
    assert popped_max == [("c", 3.0)]


# ------------------------------------------------------------------- locks


def test_lock_acquire_release(rust_cache):
    lock = rust_cache.lock("mylock", timeout=5)
    assert lock.acquire() is True
    assert lock.locked() is True
    lock.release()
    assert lock.locked() is False


def test_lock_context_manager(rust_cache):
    with rust_cache.lock("mylock", timeout=5):
        # holds the lock
        pass


def test_lock_extends(rust_cache):
    lock = rust_cache.lock("mylock", timeout=2)
    lock.acquire()
    try:
        assert lock.extend(5) is True
    finally:
        lock.release()


def test_lock_blocks_other_holders(rust_cache):
    lock1 = rust_cache.lock("mylock", timeout=10)
    lock2 = rust_cache.lock("mylock", timeout=10)
    lock1.acquire()
    try:
        assert lock2.acquire(blocking=False) is False
    finally:
        lock1.release()


# --------------------------------------------------------------- raw client


def test_get_raw_client_returns_driver(rust_cache):
    from django_cachex._driver import RustValkeyDriver  # ty: ignore[unresolved-import]

    raw = rust_cache._cache.get_raw_client()
    assert isinstance(raw, RustValkeyDriver)
    raw.set_sync("rawkey", b"rawval")
    assert raw.get_sync("rawkey") == b"rawval"


# ------------------------------------------------------------------ scripts


def test_eval(rust_cache):
    result = rust_cache._cache.eval(
        "return tonumber(ARGV[1]) + tonumber(ARGV[2])",
        0,
        7,
        5,
    )
    assert result == 12


# ----------------------------------------------------------------- pipeline


def test_pipeline_raises(rust_cache):
    with pytest.raises(NotSupportedError):
        rust_cache.pipeline()


# --------------------------------------------------------- async smoke tests


@pytest.mark.asyncio
async def test_aset_aget(rust_cache):
    await rust_cache.aset("k", "v", timeout=None)
    assert await rust_cache.aget("k") == "v"


@pytest.mark.asyncio
async def test_aset_many_aget_many(rust_cache):
    await rust_cache.aset_many({"a": 1, "b": 2}, timeout=None)
    result = await rust_cache.aget_many(["a", "b", "missing"])
    assert result == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_atype(rust_cache):
    from django_cachex.types import KeyType

    await rust_cache.aset("s", "v", timeout=None)
    assert await rust_cache.atype("s") == KeyType.STRING


@pytest.mark.asyncio
async def test_aexpire(rust_cache):
    await rust_cache.aset("k", "v", timeout=None)
    assert await rust_cache.aexpire("k", 50) is True
    ttl = await rust_cache.attl("k")
    assert ttl is not None and 0 < ttl <= 50


@pytest.mark.asyncio
async def test_async_hash(rust_cache):
    await rust_cache.ahset("h", "f", "v")
    assert await rust_cache.ahget("h", "f") == "v"
    assert await rust_cache.ahlen("h") == 1


@pytest.mark.asyncio
async def test_async_list(rust_cache):
    await rust_cache.arpush("l", "a", "b", "c")
    assert await rust_cache.alrange("l", 0, -1) == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_async_set(rust_cache):
    await rust_cache.asadd("s", "a", "b", "c")
    assert sorted(await rust_cache.asmembers("s")) == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_async_zset(rust_cache):
    await rust_cache.azadd("z", {"a": 1.0, "b": 2.0})
    assert await rust_cache.azrange("z", 0, -1) == ["a", "b"]


@pytest.mark.asyncio
async def test_alock(rust_cache):
    lock = rust_cache.alock("mylock", timeout=5)
    assert await lock.aacquire() is True
    await lock.arelease()


# ------------------------------------------------------- lazy connection


def test_unreachable_server_does_not_raise_at_construction(redis_container):
    """Driver must connect lazily so IGNORE_EXCEPTIONS-style wrappers can catch errors."""
    caches = {
        "default": {
            "BACKEND": "django_cachex.cache.RustValkeyCache",
            "LOCATION": "redis://127.0.0.1:1/0",
            "OPTIONS": {},
        },
    }
    with override_settings(CACHES=caches):
        from django.core.cache import cache

        # Constructing the cache and reaching for ``_cache`` must not blow
        # up; only an actual I/O call should attempt to connect.
        client = cache._cache
        assert client is not None
        with pytest.raises((ConnectionError, Exception)):
            cache.get("k")
