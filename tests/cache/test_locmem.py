"""Tests for ``django_cachex.cache.locmem.LocMemCache``.

LocMemCache implements the cachex extension surface natively rather than
against a RESP server, so these tests cover the same contract the
parametrized RESP tests do, without a container.
"""

import copy
import inspect
import pickle
import time
from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.test import override_settings

from django_cachex.cache.locmem import LocMemCache, _ZSet
from django_cachex.exceptions import NotSupportedError, WrongTypeError
from django_cachex.utils import _deep_getsizeof, _glob_to_regex

if TYPE_CHECKING:
    from collections.abc import Iterator


LOCMEM_CACHES = {
    "locmem": {
        "BACKEND": "django_cachex.cache.LocMemCache",
        "LOCATION": "test-locmem",
    },
}


@pytest.fixture
def locmem_cache() -> Iterator[LocMemCache]:
    with override_settings(CACHES=LOCMEM_CACHES):
        cache = caches["locmem"]
        cache.clear()
        yield cache  # type: ignore[misc]


# =============================================================================
# Backend
# =============================================================================


def test_locmem_is_cachex(locmem_cache: LocMemCache):
    assert isinstance(locmem_cache, LocMemCache)
    assert locmem_cache._cachex_support == "cachex"


def test_extension_methods_are_implemented_not_inherited_stubs(locmem_cache: LocMemCache):
    # ``BaseCachex`` defines a raising stub for every extension, so
    # ``hasattr`` proves nothing; answering for a missing key does.
    assert locmem_cache.llen("missing") == 0
    assert locmem_cache.scard("missing") == 0
    assert locmem_cache.hlen("missing") == 0
    assert locmem_cache.zcard("missing") == 0


# Instances with the same LOCATION share all state, since Django creates one backend instance per thread.
def test_collections_shared_between_instances():
    # Regression: ``_collections`` was per-instance, so a second handle
    # on the same LOCATION saw an empty keyspace for collection types.
    a = LocMemCache("shared-collections", {})
    b = LocMemCache("shared-collections", {})
    a.clear()
    a.rpush("l", 1, 2)
    a.sadd("s", "x")
    a.hset("h", "f", "v")
    a.zadd("z", {"m": 1.5})
    assert b.lrange("l", 0, -1) == [1, 2]
    assert b.smembers("s") == {"x"}
    assert b.hget("h", "f") == "v"
    assert b.zscore("z", "m") == 1.5
    b.srem("s", "x")
    assert a.has_key("s") is False


def test_semaphore_registry_shared_between_instances():
    # Regression: each instance built its own registry, splitting the
    # semaphore budget across handles on the same LOCATION.
    a = LocMemCache("shared-semaphores", {})
    b = LocMemCache("shared-semaphores", {})
    assert a._semaphore_registry is b._semaphore_registry


def test_collection_writes_respect_max_entries():
    # Regression: collection keys were never culled.
    cache = LocMemCache("cull-collections", {"OPTIONS": {"MAX_ENTRIES": 5, "CULL_FREQUENCY": 2}})
    cache.clear()
    for i in range(20):
        cache.sadd(f"k{i}", "x")
    assert len(cache._cache) + len(cache._collections) <= 5


def test_mixed_writes_respect_max_entries():
    cache = LocMemCache("cull-mixed", {"OPTIONS": {"MAX_ENTRIES": 5, "CULL_FREQUENCY": 2}})
    cache.clear()
    for i in range(10):
        cache.set(f"s{i}", i)
        cache.rpush(f"l{i}", i)
    assert len(cache._cache) + len(cache._collections) <= 5
    # Culled keys must not leak TTL entries.
    assert len(cache._expire_info) == len(cache._cache) + len(cache._collections)


def test_collection_rewrite_at_capacity_keeps_key_and_ttl():
    # Regression: a rewrite at capacity culled the key being written, then
    # re-added it without its TTL and evicted a sibling.
    cache = LocMemCache("cull-rewrite", {"OPTIONS": {"MAX_ENTRIES": 4, "CULL_FREQUENCY": 2}})
    cache.clear()
    for i in range(4):
        cache.hset(f"h{i}", "f", i)
        cache.expire(f"h{i}", 1000)
    cache.hset("h0", "f", 99)
    assert cache.hget("h0", "f") == 99
    assert cache.ttl("h0") >= 999
    assert sorted(cache.keys()) == ["h0", "h1", "h2", "h3"]


def test_cull_frequency_zero_clears_collections_too():
    cache = LocMemCache("cull-zero", {"OPTIONS": {"MAX_ENTRIES": 3, "CULL_FREQUENCY": 0}})
    cache.clear()
    for i in range(4):
        cache.sadd(f"k{i}", "x")
    # The write at capacity clears everything, then inserts one key.
    assert len(cache._cache) + len(cache._collections) == 1


# LocMemCache set() nx/xx/get flag semantics (parity with RespCache).
def test_nx_new_key_writes(locmem_cache: LocMemCache):
    assert locmem_cache.set("k", 1, nx=True) is True
    assert locmem_cache.get("k") == 1


def test_nx_existing_key_no_write(locmem_cache: LocMemCache):
    locmem_cache.set("k", "old")
    assert locmem_cache.set("k", "new", nx=True) is False
    assert locmem_cache.get("k") == "old"


def test_xx_missing_key_no_write(locmem_cache: LocMemCache):
    assert locmem_cache.set("k", 1, xx=True) is False
    assert locmem_cache.get("k") is None


def test_xx_existing_key_writes(locmem_cache: LocMemCache):
    locmem_cache.set("k", "old")
    assert locmem_cache.set("k", "new", xx=True) is True
    assert locmem_cache.get("k") == "new"


def test_get_missing_returns_none_and_writes(locmem_cache: LocMemCache):
    assert locmem_cache.set("k", "first", get=True) is None
    assert locmem_cache.get("k") == "first"


def test_get_existing_returns_prior_and_writes(locmem_cache: LocMemCache):
    locmem_cache.set("k", "old")
    assert locmem_cache.set("k", "new", get=True) == "old"
    assert locmem_cache.get("k") == "new"


def test_nx_and_get_combined(locmem_cache: LocMemCache):
    locmem_cache.set("k", "old")
    # nx blocks the write, get still returns prior
    assert locmem_cache.set("k", "new", nx=True, get=True) == "old"
    assert locmem_cache.get("k") == "old"


def test_nx_xx_mutually_exclusive(locmem_cache: LocMemCache):
    with pytest.raises(ValueError, match="mutually exclusive"):
        locmem_cache.set("k", 1, nx=True, xx=True)


def test_get_on_collection_raises_wrongtype(locmem_cache: LocMemCache):
    locmem_cache.lpush("k", 1, 2, 3)
    with pytest.raises(WrongTypeError):
        locmem_cache.set("k", "scalar", get=True)


@pytest.mark.asyncio
async def test_aset_nx(locmem_cache: LocMemCache):
    assert await locmem_cache.aset("k", 1, nx=True) is True
    assert await locmem_cache.aset("k", 2, nx=True) is False
    assert locmem_cache.get("k") == 1


@pytest.mark.asyncio
async def test_aset_get(locmem_cache: LocMemCache):
    locmem_cache.set("k", "old")
    assert await locmem_cache.aset("k", "new", get=True) == "old"
    assert locmem_cache.get("k") == "new"


# Collections live in ``_collections`` as Python objects, not pickled bytes: writes mutate in place, reads copy.
def test_lrange_returns_independent_copy(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c")
    snapshot = locmem_cache.lrange("k", 0, -1)
    snapshot.append("MUTATED")
    # The cache must be insulated from the caller's mutation.
    assert locmem_cache.lrange("k", 0, -1) == ["a", "b", "c"]


def test_smembers_returns_independent_copy(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b")
    snapshot = locmem_cache.smembers("k")
    snapshot.add("MUTATED")
    assert locmem_cache.smembers("k") == {"a", "b"}


def test_hgetall_returns_independent_copy(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1, "b": 2})
    snapshot = locmem_cache.hgetall("k")
    snapshot["c"] = 3
    assert locmem_cache.hgetall("k") == {"a": 1, "b": 2}


def test_set_after_collection_overwrites(locmem_cache: LocMemCache):
    # RESP ``SET`` overwrites any prior collection at the key, regardless
    # of type (mirrors Redis behavior).
    locmem_cache.lpush("k", "x", "y")
    assert locmem_cache.type("k") == "list"
    locmem_cache.set("k", "abc")
    assert locmem_cache.type("k") == "string"
    assert locmem_cache.get("k") == "abc"


def test_has_key_finds_collections(locmem_cache: LocMemCache):
    locmem_cache.lpush("l", "x")
    locmem_cache.sadd("s", "x")
    locmem_cache.hset("h", "f", "v")
    locmem_cache.zadd("z", {"m": 1.0})
    for k in ("l", "s", "h", "z"):
        assert locmem_cache.has_key(k) is True
    assert locmem_cache.has_key("missing") is False


def test_keys_lists_collections(locmem_cache: LocMemCache):
    locmem_cache.set("opaque", "v")
    locmem_cache.lpush("alist", "x")
    locmem_cache.zadd("azset", {"m": 1.0})
    all_keys = locmem_cache.keys()
    assert "opaque" in all_keys
    assert "alist" in all_keys
    assert "azset" in all_keys


def test_clear_drops_collections(locmem_cache: LocMemCache):
    locmem_cache.lpush("l", "x")
    locmem_cache.zadd("z", {"m": 1.0})
    locmem_cache.clear()
    assert locmem_cache.type("l") is None
    assert locmem_cache.type("z") is None


def test_delete_drops_collection(locmem_cache: LocMemCache):
    locmem_cache.lpush("k", "x")
    assert locmem_cache.delete("k") is True
    assert locmem_cache.type("k") is None
    # Re-creating after delete works (key-type "resets", like Redis).
    locmem_cache.hset("k", "f", "v")
    assert locmem_cache.type("k") == "hash"


# =============================================================================
# Keys, TTL, expire, persist, type, info, delete_pattern, iter_keys
# =============================================================================


def test_set_and_get(locmem_cache: LocMemCache):
    locmem_cache.set("key1", "value1")
    assert locmem_cache.get("key1") == "value1"


def test_get_missing_returns_default(locmem_cache: LocMemCache):
    assert locmem_cache.get("missing") is None
    assert locmem_cache.get("missing", "default") == "default"


def test_delete(locmem_cache: LocMemCache):
    locmem_cache.set("key1", "value1")
    assert locmem_cache.delete("key1") is True
    assert locmem_cache.get("key1") is None


def test_clear(locmem_cache: LocMemCache):
    locmem_cache.set("key1", "value1")
    locmem_cache.set("key2", "value2")
    locmem_cache.clear()
    assert locmem_cache.get("key1") is None


def test_keys_returns_all(locmem_cache: LocMemCache):
    locmem_cache.set("alpha", 1)
    locmem_cache.set("beta", 2)
    keys = locmem_cache.keys()
    assert "alpha" in keys
    assert "beta" in keys


def test_keys_does_not_double_strip_prefix():
    """User keys starting with KEY_PREFIX must not be re-stripped.

    Django's key format is ``KEY_PREFIX:VERSION:user_key``. After
    ``split(':', 2)`` ``parts[2]`` is already the user key. A second
    ``startswith(key_prefix)`` strip would mangle a key like
    ``myapp:bar`` into ``:bar`` when ``KEY_PREFIX='myapp'``.
    """
    config = {
        "locmem": {
            "BACKEND": "django_cachex.cache.LocMemCache",
            "LOCATION": "test-prefix-strip",
            "KEY_PREFIX": "myapp",
        },
    }
    with override_settings(CACHES=config):
        cache = caches["locmem"]
        cache.clear()
        cache.set("myapp:bar", "v")
        keys = cache.keys("*")
        assert "myapp:bar" in keys
        assert ":bar" not in keys


def test_keys_with_colon_in_key_prefix():
    # Regression: keys() assumed a colon-free prefix and silently
    # dropped every key when KEY_PREFIX itself contained a colon.
    config = {
        "locmem": {
            "BACKEND": "django_cachex.cache.LocMemCache",
            "LOCATION": "test-colon-prefix",
            "KEY_PREFIX": "app:v2",
        },
    }
    with override_settings(CACHES=config):
        cache = caches["locmem"]
        cache.clear()
        cache.set("plain", "v")
        cache.sadd("aset", "x")
        cache.set("other", "v", version=2)
        assert cache.keys() == ["aset", "plain"]
        assert cache.keys(version=2) == ["other"]


def test_keys_key_starting_with_colon(locmem_cache: LocMemCache):
    locmem_cache.set(":leading", "v")
    assert locmem_cache.keys() == [":leading"]
    assert locmem_cache.keys(":lead*") == [":leading"]


def test_keys_excludes_expired_entries(locmem_cache: LocMemCache):
    # Regression: expired-but-not-yet-culled entries were listed.
    locmem_cache.set("gone", "v", timeout=-1)
    locmem_cache.rpush("gone-list", "x")
    locmem_cache.expire("gone-list", -1)
    locmem_cache.set("alive", "v")
    assert locmem_cache.keys() == ["alive"]


def test_keys_with_pattern(locmem_cache: LocMemCache):
    locmem_cache.set("user:1", "alice")
    locmem_cache.set("user:2", "bob")
    locmem_cache.set("session:abc", "data")
    keys = locmem_cache.keys("user:*")
    assert "user:1" in keys
    assert "user:2" in keys
    assert "session:abc" not in keys


def test_keys_with_character_class(locmem_cache: LocMemCache):
    locmem_cache.set("ka", 1)
    locmem_cache.set("kb", 2)
    locmem_cache.set("kc", 3)
    assert locmem_cache.keys("k[ab]") == ["ka", "kb"]


def test_keys_with_negated_character_class(locmem_cache: LocMemCache):
    locmem_cache.set("ka", 1)
    locmem_cache.set("kb", 2)
    assert locmem_cache.keys("k[^a]") == ["kb"]


def test_keys_bang_is_a_class_member_not_a_negation(locmem_cache: LocMemCache):
    # ``fnmatch`` spells negation ``[!a]``; Redis reads ``!`` as a member.
    locmem_cache.set("k!", 1)
    locmem_cache.set("kb", 2)
    assert locmem_cache.keys("k[!a]") == ["k!"]


def test_keys_backslash_escapes_the_next_character(locmem_cache: LocMemCache):
    locmem_cache.set("k*", 1)
    locmem_cache.set("kx", 2)
    assert locmem_cache.keys(r"k\*") == ["k*"]


def test_empty_pattern_matches_only_the_empty_key(locmem_cache: LocMemCache):
    # ``KEYS ""`` on Redis matches the empty key, not everything.
    locmem_cache.set("a", 1)
    assert locmem_cache.keys("") == []
    assert list(locmem_cache.iter_keys("")) == []
    locmem_cache.set("", 2)
    assert locmem_cache.keys("") == [""]
    assert locmem_cache.delete_pattern("") == 1
    assert locmem_cache.keys("*") == ["a"]


def test_keys_with_a_reversed_character_range(locmem_cache: LocMemCache):
    # Redis swaps the bounds of ``[z-a]``; ``re`` would reject the range.
    locmem_cache.set("km", 1)
    locmem_cache.set("k1", 2)
    assert locmem_cache.keys("k[z-a]") == ["km"]
    assert locmem_cache.keys("k[9-0]") == ["k1"]


def test_scan_filters_by_key_type(locmem_cache: LocMemCache):
    locmem_cache.set("plain", 1)
    locmem_cache.rpush("alist", "a")
    locmem_cache.hset("ahash", "f", "v")
    assert locmem_cache.scan(pattern="*", key_type="list") == (0, ["alist"])


@pytest.mark.asyncio
async def test_ascan_mirrors_scan(locmem_cache: LocMemCache):
    locmem_cache.rpush("alist", "a")
    assert await locmem_cache.ascan(pattern="*", key_type="list") == (0, ["alist"])


def test_iter_keys(locmem_cache: LocMemCache):
    locmem_cache.set("a", 1)
    locmem_cache.set("b", 2)
    assert sorted(locmem_cache.iter_keys()) == ["a", "b"]


def test_iter_keys_with_pattern(locmem_cache: LocMemCache):
    locmem_cache.set("user:1", "alice")
    locmem_cache.set("session:1", "x")
    assert list(locmem_cache.iter_keys("user:*")) == ["user:1"]


def test_delete_pattern(locmem_cache: LocMemCache):
    locmem_cache.set("user:1", 1)
    locmem_cache.set("user:2", 2)
    locmem_cache.set("session:abc", "x")
    assert locmem_cache.delete_pattern("user:*") == 2
    assert locmem_cache.get("user:1") is None
    assert locmem_cache.get("session:abc") == "x"


def test_delete_pattern_no_match(locmem_cache: LocMemCache):
    locmem_cache.set("a", 1)
    assert locmem_cache.delete_pattern("missing:*") == 0


def test_ttl_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.ttl("nonexistent") == -2


def test_ttl_persistent_key(locmem_cache: LocMemCache):
    locmem_cache.set("forever", "value", timeout=None)
    assert locmem_cache.ttl("forever") is None


def test_ttl_expiring_key(locmem_cache: LocMemCache):
    locmem_cache.set("temp", "value", timeout=3600)
    assert 3590 <= locmem_cache.ttl("temp") <= 3600


def test_ttl_of_a_fresh_key_reads_the_full_timeout(locmem_cache: LocMemCache):
    # Regression: ``int()`` truncated 299.999 to 299 where Redis rounds
    # to the nearest second and reports 300.
    locmem_cache.set("temp", "value", timeout=300)
    assert locmem_cache.ttl("temp") == 300


@pytest.mark.parametrize(("remaining", "expected"), [(0.6, 1), (0.4, 0), (1.6, 2), (1.4, 1)])
def test_ttl_rounds_to_the_nearest_second(locmem_cache: LocMemCache, remaining, expected):
    locmem_cache.set("temp", "value", timeout=None)
    locmem_cache._expire_info[locmem_cache.make_key("temp")] = time.time() + remaining
    assert locmem_cache.ttl("temp") == expected


def test_expire(locmem_cache: LocMemCache):
    locmem_cache.set("key1", "value1", timeout=None)
    assert locmem_cache.ttl("key1") is None
    locmem_cache.expire("key1", 100)
    assert 90 <= locmem_cache.ttl("key1") <= 100


def test_expire_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.expire("nonexistent", 100) is False


def test_persist(locmem_cache: LocMemCache):
    locmem_cache.set("key1", "value1", timeout=60)
    assert locmem_cache.ttl("key1") > 0
    locmem_cache.persist("key1")
    assert locmem_cache.ttl("key1") is None


def test_persist_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.persist("nonexistent") is False


def test_incr_missing_key_raises_valueerror(locmem_cache: LocMemCache):
    with pytest.raises(ValueError, match="not found"):
        locmem_cache.incr("missing")


def test_incr_on_collection_raises_wrongtype(locmem_cache: LocMemCache):
    # Regression: Django's inherited incr read ``_cache`` directly and
    # raised KeyError for a live key held in ``_collections``.
    locmem_cache.sadd("k", "a")
    with pytest.raises(WrongTypeError):
        locmem_cache.incr("k")


@pytest.mark.asyncio
async def test_aincr_keeps_the_ttl(locmem_cache: LocMemCache):
    # Regression: Django 6.0's BaseCache.aincr is aget then aset with the
    # default timeout, which reset a 3600 s TTL to 300.
    locmem_cache.set("c", 5, timeout=3600)
    assert await locmem_cache.aincr("c") == 6
    assert await locmem_cache.adecr("c", 2) == 4
    assert locmem_cache.ttl("c") > 3000


@pytest.mark.asyncio
async def test_ahas_key_finds_collections(locmem_cache: LocMemCache):
    # Regression: on Django 6.0 ahas_key went through aget and raised
    # WrongTypeError on a list key.
    locmem_cache.rpush("l", "x")
    assert await locmem_cache.ahas_key("l") is True
    assert await locmem_cache.ahas_key("missing") is False


def test_info_returns_dict(locmem_cache: LocMemCache):
    locmem_cache.set("key1", "value1")
    info = locmem_cache.info()
    assert info["backend"] == "LocMemCache"
    assert "server" in info
    assert "memory" in info
    assert info["keyspace"]["db0"]["keys"] >= 1


def test_type_string(locmem_cache: LocMemCache):
    locmem_cache.set("k", "hello")
    assert locmem_cache.type("k") == "string"


def test_type_int_is_string(locmem_cache: LocMemCache):
    locmem_cache.set("k", 42)
    assert locmem_cache.type("k") == "string"


def test_type_list(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", 1, 2, 3)
    assert locmem_cache.type("k") == "list"


def test_type_set(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", 1, 2, 3)
    assert locmem_cache.type("k") == "set"


def test_type_hash(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"name": "alice"})
    assert locmem_cache.type("k") == "hash"


def test_type_zset(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"alice": 100.0, "bob": 85.0})
    assert locmem_cache.type("k") == "zset"


def test_type_opaque_python_list_is_string(locmem_cache: LocMemCache):
    # A plain Python list stored via ``cache.set()`` is opaque to RESP:
    # ``type()`` reports STRING and list ops would WRONGTYPE on it.
    locmem_cache.set("k", [1, 2, 3])
    assert locmem_cache.type("k") == "string"


def test_type_opaque_python_dict_is_string(locmem_cache: LocMemCache):
    locmem_cache.set("k", {"name": "alice"})
    assert locmem_cache.type("k") == "string"


def test_type_opaque_python_set_is_string(locmem_cache: LocMemCache):
    locmem_cache.set("k", {1, 2, 3})
    assert locmem_cache.type("k") == "string"


def test_type_missing(locmem_cache: LocMemCache):
    assert locmem_cache.type("missing") is None


def test_persist_without_a_ttl_returns_false(locmem_cache: LocMemCache):
    locmem_cache.set("plain", 1, timeout=None)
    locmem_cache.rpush("list", "a")
    locmem_cache.set("expiring", 1, timeout=60)
    assert locmem_cache.persist("plain") is False
    assert locmem_cache.persist("list") is False
    assert locmem_cache.persist("expiring") is True
    assert locmem_cache.persist("expiring") is False
    assert locmem_cache.ttl("expiring") is None


def test_scan_returns_remaining_keys_after_earlier_pages_are_deleted(locmem_cache: LocMemCache):
    for i in range(10):
        locmem_cache.set(f"k{i}", i)
    cursor, first = locmem_cache.scan(count=3)
    locmem_cache.delete_many(first)
    seen = []
    while cursor:
        cursor, keys = locmem_cache.scan(cursor, count=3)
        seen.extend(keys)
    assert sorted(seen) == locmem_cache.keys()


# =============================================================================
# Lists
# =============================================================================


def test_lpush_creates_new_list(locmem_cache: LocMemCache):
    assert locmem_cache.lpush("k", "a") == 1
    assert locmem_cache.lrange("k", 0, -1) == ["a"]


def test_lpush_prepends(locmem_cache: LocMemCache):
    locmem_cache.lpush("k", "a")
    locmem_cache.lpush("k", "b")
    assert locmem_cache.lrange("k", 0, -1) == ["b", "a"]


def test_lpush_multiple_values(locmem_cache: LocMemCache):
    assert locmem_cache.lpush("k", "a", "b", "c") == 3
    assert locmem_cache.lrange("k", 0, -1) == ["c", "b", "a"]


def test_lpush_no_values_creates_nothing(locmem_cache: LocMemCache):
    # Regression: an empty ``_List`` was stored, an immortal key no list op
    # can reach or reap. Redis never creates an empty list.
    assert locmem_cache.lpush("empty") == 0
    assert locmem_cache.has_key("empty") is False
    assert locmem_cache.keys() == []


def test_rpush_no_values_creates_nothing(locmem_cache: LocMemCache):
    assert locmem_cache.rpush("empty") == 0
    assert locmem_cache.has_key("empty") is False
    assert locmem_cache.keys() == []


def test_push_no_values_leaves_existing_list_alone(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a")
    locmem_cache.expire("k", 100)
    assert locmem_cache.lpush("k") == 0
    assert locmem_cache.rpush("k") == 0
    assert locmem_cache.lrange("k", 0, -1) == ["a"]
    assert 90 <= locmem_cache.ttl("k") <= 100


def test_lpush_wrongtype_on_string(locmem_cache: LocMemCache):
    locmem_cache.set("k", "string")
    with pytest.raises(TypeError):
        locmem_cache.lpush("k", "x")


def test_lpush_wrongtype_on_opaque_python_list(locmem_cache: LocMemCache):
    # A Python list stored via ``cache.set()`` is opaque (RESP "string").
    locmem_cache.set("k", [1, 2, 3])
    with pytest.raises(TypeError):
        locmem_cache.lpush("k", 4)


def test_rpush_creates_new_list(locmem_cache: LocMemCache):
    assert locmem_cache.rpush("k", "a") == 1
    assert locmem_cache.lrange("k", 0, -1) == ["a"]


def test_rpush_appends(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a")
    locmem_cache.rpush("k", "b")
    assert locmem_cache.lrange("k", 0, -1) == ["a", "b"]


def test_rpush_multiple_values(locmem_cache: LocMemCache):
    assert locmem_cache.rpush("k", "a", "b", "c") == 3
    assert locmem_cache.lrange("k", 0, -1) == ["a", "b", "c"]


def test_rpush_wrongtype_on_string(locmem_cache: LocMemCache):
    locmem_cache.set("k", "string")
    with pytest.raises(TypeError):
        locmem_cache.rpush("k", "x")


def test_lpop_returns_first(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", 1, 2, 3)
    assert locmem_cache.lpop("k") == 1
    assert locmem_cache.lrange("k", 0, -1) == [2, 3]


def test_lpop_with_count(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", 1, 2, 3, 4)
    assert locmem_cache.lpop("k", count=2) == [1, 2]
    assert locmem_cache.lrange("k", 0, -1) == [3, 4]


def test_lpop_empty(locmem_cache: LocMemCache):
    assert locmem_cache.lpop("missing") is None
    assert locmem_cache.lpop("missing", count=2) is None


def test_lpop_deletes_when_empty(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", 1)
    locmem_cache.lpop("k")
    assert locmem_cache.has_key("k") is False


def test_rpop_returns_last(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", 1, 2, 3)
    assert locmem_cache.rpop("k") == 3
    assert locmem_cache.lrange("k", 0, -1) == [1, 2]


def test_rpop_with_count(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", 1, 2, 3, 4)
    assert locmem_cache.rpop("k", count=2) == [4, 3]
    assert locmem_cache.lrange("k", 0, -1) == [1, 2]


def test_rpop_count_zero_pops_nothing(locmem_cache: LocMemCache):
    # Regression: count=0 sliced ``[-0:]`` and popped the whole list.
    locmem_cache.rpush("k", 1, 2, 3)
    assert locmem_cache.rpop("k", count=0) == []
    assert locmem_cache.lrange("k", 0, -1) == [1, 2, 3]


def test_rpop_empty(locmem_cache: LocMemCache):
    assert locmem_cache.rpop("missing") is None
    assert locmem_cache.rpop("missing", count=2) is None


@pytest.mark.parametrize("method", ["lpop", "rpop"])
def test_pop_negative_count_rejected(locmem_cache: LocMemCache, method: str):
    # Regression: a negative count sliced from the opposite end, so
    # ``rpop(key, -2)`` popped from the head. Redis rejects it.
    locmem_cache.rpush("k", 1, 2, 3, 1)
    with pytest.raises(ValueError, match="must be positive"):
        getattr(locmem_cache, method)("k", -2)
    assert locmem_cache.lrange("k", 0, -1) == [1, 2, 3, 1]


@pytest.mark.parametrize("method", ["lpop", "rpop"])
def test_pop_negative_count_rejected_on_missing_key(locmem_cache: LocMemCache, method: str):
    with pytest.raises(ValueError, match="must be positive"):
        getattr(locmem_cache, method)("missing", -1)


def test_rpop_deletes_when_empty(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", 1)
    locmem_cache.rpop("k")
    assert locmem_cache.has_key("k") is False


def test_lrange_full(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c")
    assert locmem_cache.lrange("k", 0, -1) == ["a", "b", "c"]


def test_lrange_partial(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c", "d")
    assert locmem_cache.lrange("k", 1, 2) == ["b", "c"]


def test_lrange_negative_indices(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c", "d", "e")
    assert locmem_cache.lrange("k", -3, -1) == ["c", "d", "e"]


def test_lrange_missing(locmem_cache: LocMemCache):
    assert locmem_cache.lrange("missing", 0, -1) == []


def test_llen(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", 1, 2, 3)
    assert locmem_cache.llen("k") == 3


def test_llen_missing(locmem_cache: LocMemCache):
    assert locmem_cache.llen("missing") == 0


@pytest.mark.parametrize(
    ("count", "expected_removed", "expected_list"),
    [
        (0, 3, ["b", "c"]),
        (2, 2, ["b", "c", "a"]),
        (-1, 1, ["a", "b", "a", "c"]),
    ],
    ids=["all", "head_2", "tail_1"],
)
def test_lrem(locmem_cache: LocMemCache, count: int, expected_removed: int, expected_list: list):
    locmem_cache.rpush("k", "a", "b", "a", "c", "a")
    assert locmem_cache.lrem("k", count, "a") == expected_removed
    assert locmem_cache.lrange("k", 0, -1) == expected_list


def test_lrem_not_found(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b")
    assert locmem_cache.lrem("k", 0, "z") == 0


def test_lrem_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.lrem("missing", 0, "a") == 0


def test_lrem_deletes_when_all_removed(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "a")
    locmem_cache.lrem("k", 0, "a")
    assert locmem_cache.has_key("k") is False


def test_ltrim_basic(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c", "d", "e")
    assert locmem_cache.ltrim("k", 1, 3) is True
    assert locmem_cache.lrange("k", 0, -1) == ["b", "c", "d"]


def test_ltrim_negative_end(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c")
    locmem_cache.ltrim("k", 0, -2)
    assert locmem_cache.lrange("k", 0, -1) == ["a", "b"]


def test_ltrim_out_of_range_deletes(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b")
    locmem_cache.ltrim("k", 5, 10)
    assert locmem_cache.has_key("k") is False


def test_ltrim_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.ltrim("missing", 0, -1) is True


def test_lindex(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c")
    assert locmem_cache.lindex("k", 0) == "a"
    assert locmem_cache.lindex("k", -1) == "c"


def test_lindex_out_of_range(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a")
    assert locmem_cache.lindex("k", 5) is None


def test_lindex_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.lindex("missing", 0) is None


def test_lset(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c")
    assert locmem_cache.lset("k", 1, "B") is True
    assert locmem_cache.lrange("k", 0, -1) == ["a", "B", "c"]


def test_lset_missing_key_raises(locmem_cache: LocMemCache):
    with pytest.raises(ValueError, match="no such key"):
        locmem_cache.lset("missing", 0, "x")


def test_lset_out_of_range_raises(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a")
    with pytest.raises(ValueError, match="index out of range"):
        locmem_cache.lset("k", 5, "x")


def test_linsert_before(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "c")
    assert locmem_cache.linsert("k", "BEFORE", "c", "b") == 3
    assert locmem_cache.lrange("k", 0, -1) == ["a", "b", "c"]


def test_linsert_after(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b")
    assert locmem_cache.linsert("k", "AFTER", "a", "X") == 3
    assert locmem_cache.lrange("k", 0, -1) == ["a", "X", "b"]


def test_linsert_pivot_not_found(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a")
    assert locmem_cache.linsert("k", "BEFORE", "z", "x") == -1


def test_linsert_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.linsert("missing", "BEFORE", "a", "x") == 0


def test_linsert_rejects_an_unknown_position(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "c")
    with pytest.raises(ValueError, match="syntax error"):
        locmem_cache.linsert("k", "SIDEWAYS", "c", "b")
    assert locmem_cache.lrange("k", 0, -1) == ["a", "c"]


def test_lpos_basic(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c", "b")
    assert locmem_cache.lpos("k", "b") == 1


def test_lpos_with_rank(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c", "b", "d", "b")
    assert locmem_cache.lpos("k", "b", rank=2) == 3
    assert locmem_cache.lpos("k", "b", rank=-1) == 5


def test_lpos_with_count(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c", "b", "d", "b")
    assert locmem_cache.lpos("k", "b", count=0) == [1, 3, 5]
    assert locmem_cache.lpos("k", "b", count=2) == [1, 3]


def test_lpos_with_maxlen(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "c", "b")
    assert locmem_cache.lpos("k", "b", maxlen=2) == 1


def test_lpos_negative_rank_scans_the_tail_within_maxlen(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "b", "a", "c", "b")
    assert locmem_cache.lpos("k", "b", rank=-1, maxlen=2) == 3
    assert locmem_cache.lpos("k", "b", rank=-1, count=0) == [3, 0]


def test_lpos_rank_zero_rejected(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a")
    with pytest.raises(ValueError, match="RANK can't be zero"):
        locmem_cache.lpos("k", "a", rank=0)


def test_lpos_negative_count_rejected(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "a", "c", "a")
    with pytest.raises(ValueError, match="COUNT can't be negative"):
        locmem_cache.lpos("k", "a", count=-1)


def test_lpos_negative_maxlen_rejected(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b", "a")
    with pytest.raises(ValueError, match="MAXLEN can't be negative"):
        locmem_cache.lpos("k", "a", maxlen=-1)


def test_lpos_not_found(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a")
    assert locmem_cache.lpos("k", "z") is None
    assert locmem_cache.lpos("k", "z", count=0) == []


def test_lpos_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.lpos("missing", "x") is None
    assert locmem_cache.lpos("missing", "x", count=0) == []


def test_list_ops_preserve_ttl(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", 1, 2)
    locmem_cache.expire("k", 3600)
    locmem_cache.rpush("k", 3)
    assert 3590 <= locmem_cache.ttl("k") <= 3600


def test_list_ops_no_expiry_stays(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", 1, 2)
    locmem_cache.rpush("k", 3)
    assert locmem_cache.ttl("k") is None


# =============================================================================
# Sets
# =============================================================================


def test_sadd_creates_new_set(locmem_cache: LocMemCache):
    assert locmem_cache.sadd("k", "a") == 1
    assert locmem_cache.smembers("k") == {"a"}


def test_sadd_adds_to_existing(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a")
    assert locmem_cache.sadd("k", "b") == 1
    assert locmem_cache.smembers("k") == {"a", "b"}


def test_sadd_duplicate_returns_zero(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a")
    assert locmem_cache.sadd("k", "a") == 0


def test_sadd_multiple_members(locmem_cache: LocMemCache):
    assert locmem_cache.sadd("k", "a", "b", "c") == 3
    assert locmem_cache.smembers("k") == {"a", "b", "c"}


def test_sadd_wrongtype_on_string(locmem_cache: LocMemCache):
    locmem_cache.set("k", "string")
    with pytest.raises(TypeError):
        locmem_cache.sadd("k", "x")


def test_sadd_wrongtype_on_opaque_python_set(locmem_cache: LocMemCache):
    locmem_cache.set("k", {"a", "b"})
    with pytest.raises(TypeError):
        locmem_cache.sadd("k", "c")


def test_sadd_no_members_creates_nothing(locmem_cache: LocMemCache):
    # Regression: an empty write registered a phantom empty set.
    assert locmem_cache.sadd("k") == 0
    assert locmem_cache.has_key("k") is False


def test_srem(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b", "c")
    assert locmem_cache.srem("k", "b") == 1
    assert locmem_cache.smembers("k") == {"a", "c"}


def test_srem_multiple_members(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b", "c", "d")
    assert locmem_cache.srem("k", "a", "c") == 2
    assert locmem_cache.smembers("k") == {"b", "d"}


def test_srem_nonexistent_member(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b")
    assert locmem_cache.srem("k", "z") == 0


def test_srem_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.srem("missing", "a") == 0


def test_srem_deletes_when_empty(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a")
    locmem_cache.srem("k", "a")
    assert locmem_cache.has_key("k") is False


def test_scard(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b", "c")
    assert locmem_cache.scard("k") == 3


def test_scard_missing(locmem_cache: LocMemCache):
    assert locmem_cache.scard("missing") == 0


def test_sismember(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a")
    assert locmem_cache.sismember("k", "a") is True
    assert locmem_cache.sismember("k", "z") is False


def test_sismember_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.sismember("missing", "a") is False


def test_smembers_returns_copy(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b", "c")
    result = locmem_cache.smembers("k")
    assert result == {"a", "b", "c"}
    result.add("d")
    assert locmem_cache.smembers("k") == {"a", "b", "c"}


def test_smembers_missing(locmem_cache: LocMemCache):
    assert locmem_cache.smembers("missing") == set()


def test_smismember(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b", "c")
    assert locmem_cache.smismember("k", "a", "z", "c") == [True, False, True]


def test_smismember_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.smismember("missing", "a", "b") == [False, False]


def test_spop_single(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b", "c")
    member = locmem_cache.spop("k")
    assert member in {"a", "b", "c"}
    assert locmem_cache.scard("k") == 2


def test_spop_with_count(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b", "c")
    popped = locmem_cache.spop("k", count=2)
    assert isinstance(popped, set)
    assert len(popped) == 2
    assert popped.issubset({"a", "b", "c"})


def test_spop_missing_key_single(locmem_cache: LocMemCache):
    assert locmem_cache.spop("missing") is None


def test_spop_missing_key_with_count(locmem_cache: LocMemCache):
    assert locmem_cache.spop("missing", count=2) == set()


def test_spop_deletes_when_empty(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a")
    locmem_cache.spop("k")
    assert locmem_cache.has_key("k") is False


def test_spop_negative_count_rejected(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b")
    with pytest.raises(ValueError, match="must be positive"):
        locmem_cache.spop("k", count=-1)
    assert locmem_cache.scard("k") == 2


def test_srandmember_single(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b", "c")
    assert locmem_cache.srandmember("k") in {"a", "b", "c"}
    assert locmem_cache.scard("k") == 3


def test_srandmember_with_count(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a", "b", "c")
    members = locmem_cache.srandmember("k", count=2)
    assert isinstance(members, list)
    assert len(members) == 2
    assert all(m in {"a", "b", "c"} for m in members)
    assert locmem_cache.scard("k") == 3


def test_srandmember_negative_count_allows_repeats(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a")
    assert locmem_cache.srandmember("k", count=-3) == ["a", "a", "a"]
    assert locmem_cache.scard("k") == 1


def test_srandmember_missing_key_single(locmem_cache: LocMemCache):
    assert locmem_cache.srandmember("missing") is None


def test_srandmember_missing_key_with_count(locmem_cache: LocMemCache):
    assert locmem_cache.srandmember("missing", count=2) == []


def test_sdiff(locmem_cache: LocMemCache):
    locmem_cache.sadd("a", "x", "y", "z")
    locmem_cache.sadd("b", "y")
    assert locmem_cache.sdiff(["a", "b"]) == {"x", "z"}


def test_sdiff_single_string_key(locmem_cache: LocMemCache):
    locmem_cache.sadd("a", "x", "y")
    assert locmem_cache.sdiff("a") == {"x", "y"}


def test_sdiff_missing_keys_yield_empty(locmem_cache: LocMemCache):
    assert locmem_cache.sdiff(["missing1", "missing2"]) == set()


def test_sinter(locmem_cache: LocMemCache):
    locmem_cache.sadd("a", "x", "y", "z")
    locmem_cache.sadd("b", "y", "z", "w")
    assert locmem_cache.sinter(["a", "b"]) == {"y", "z"}


def test_sinter_with_missing_yields_empty(locmem_cache: LocMemCache):
    locmem_cache.sadd("a", "x")
    assert locmem_cache.sinter(["a", "missing"]) == set()


def test_sunion(locmem_cache: LocMemCache):
    locmem_cache.sadd("a", "x", "y")
    locmem_cache.sadd("b", "y", "z")
    assert locmem_cache.sunion(["a", "b"]) == {"x", "y", "z"}


def test_sunion_empty_input(locmem_cache: LocMemCache):
    assert locmem_cache.sunion(["missing1", "missing2"]) == set()


def test_set_ops_preserve_ttl(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a")
    locmem_cache.expire("k", 3600)
    locmem_cache.sadd("k", "b")
    assert 3590 <= locmem_cache.ttl("k") <= 3600


def test_set_ops_no_expiry_stays(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a")
    locmem_cache.sadd("k", "b")
    assert locmem_cache.ttl("k") is None


def test_sadd_with_an_unhashable_member_changes_nothing(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "a")
    with pytest.raises(TypeError):
        locmem_cache.sadd("k", "b", ["unhashable"])
    assert locmem_cache.smembers("k") == {"a"}


# =============================================================================
# Hashes
# =============================================================================


def test_hset_creates_new_hash(locmem_cache: LocMemCache):
    assert locmem_cache.hset("k", "name", "alice") == 1
    assert locmem_cache.hgetall("k") == {"name": "alice"}


def test_hset_adds_field(locmem_cache: LocMemCache):
    locmem_cache.hset("k", "name", "alice")
    assert locmem_cache.hset("k", "age", "30") == 1
    assert locmem_cache.hgetall("k") == {"name": "alice", "age": "30"}


def test_hset_overwrites_returns_zero(locmem_cache: LocMemCache):
    locmem_cache.hset("k", "name", "alice")
    assert locmem_cache.hset("k", "name", "bob") == 0
    assert locmem_cache.hgetall("k") == {"name": "bob"}


def test_hset_wrongtype_on_string(locmem_cache: LocMemCache):
    locmem_cache.set("k", "string")
    with pytest.raises(TypeError):
        locmem_cache.hset("k", "f", "v")


def test_hset_wrongtype_on_opaque_python_dict(locmem_cache: LocMemCache):
    locmem_cache.set("k", {"a": 1})
    with pytest.raises(TypeError):
        locmem_cache.hset("k", "b", 2)


def test_hset_mapping_creates_hash(locmem_cache: LocMemCache):
    assert locmem_cache.hset("k", mapping={"a": 1, "b": 2}) == 2
    assert locmem_cache.hgetall("k") == {"a": 1, "b": 2}


def test_hset_mapping_merges_fields(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1})
    locmem_cache.hset("k", mapping={"b": 2, "c": 3})
    assert locmem_cache.hgetall("k") == {"a": 1, "b": 2, "c": 3}


def test_hset_items_creates_hash(locmem_cache: LocMemCache):
    # items is a flat list of [field, value, field, value, ...]
    assert locmem_cache.hset("k", items=["a", 1, "b", 2]) == 2
    assert locmem_cache.hgetall("k") == {"a": 1, "b": 2}


def test_hset_items_odd_length_raises(locmem_cache: LocMemCache):
    # Same message as the RESP backends, so a project test written
    # against either backend passes on both.
    with pytest.raises(ValueError, match="items must hold field/value pairs"):
        locmem_cache.hset("k", items=["a", 1, "b"])


def test_hset_odd_items_leaves_the_hash_alone(locmem_cache: LocMemCache):
    # Regression: field/mapping writes landed on the live hash before the
    # ``items`` check raised, so the rejected call half-applied.
    locmem_cache.hset("k", "a", 1)
    with pytest.raises(ValueError, match="field/value pairs"):
        locmem_cache.hset("k", "b", 2, mapping={"c": 3}, items=["d"])
    assert locmem_cache.hgetall("k") == {"a": 1}


def test_hset_odd_items_creates_nothing(locmem_cache: LocMemCache):
    with pytest.raises(ValueError, match="field/value pairs"):
        locmem_cache.hset("k", "a", 1, items=["d"])
    assert locmem_cache.has_key("k") is False


def test_hset_no_fields_creates_nothing(locmem_cache: LocMemCache):
    # Regression: an empty write registered a phantom empty hash.
    assert locmem_cache.hset("k", mapping={}) == 0
    assert locmem_cache.has_key("k") is False


def test_hdel_removes_field(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1, "b": 2, "c": 3})
    assert locmem_cache.hdel("k", "b") == 1
    assert locmem_cache.hgetall("k") == {"a": 1, "c": 3}


def test_hdel_multiple_fields(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1, "b": 2, "c": 3})
    assert locmem_cache.hdel("k", "a", "c") == 2
    assert locmem_cache.hgetall("k") == {"b": 2}


def test_hdel_nonexistent_field(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1})
    assert locmem_cache.hdel("k", "z") == 0


def test_hdel_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.hdel("missing", "f") == 0


def test_hdel_deletes_when_empty(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1})
    locmem_cache.hdel("k", "a")
    assert locmem_cache.has_key("k") is False


def test_hget(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"name": "alice"})
    assert locmem_cache.hget("k", "name") == "alice"


def test_hget_missing_field(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"name": "alice"})
    assert locmem_cache.hget("k", "age") is None


def test_hget_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.hget("missing", "f") is None


def test_hgetall_returns_copy(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1, "b": 2})
    result = locmem_cache.hgetall("k")
    assert result == {"a": 1, "b": 2}
    result["c"] = 3
    assert locmem_cache.hgetall("k") == {"a": 1, "b": 2}


def test_hgetall_missing(locmem_cache: LocMemCache):
    assert locmem_cache.hgetall("missing") == {}


def test_hlen(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1, "b": 2, "c": 3})
    assert locmem_cache.hlen("k") == 3


def test_hlen_missing(locmem_cache: LocMemCache):
    assert locmem_cache.hlen("missing") == 0


def test_hkeys(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"x": 1, "y": 2})
    assert sorted(locmem_cache.hkeys("k")) == ["x", "y"]


def test_hkeys_missing(locmem_cache: LocMemCache):
    assert locmem_cache.hkeys("missing") == []


def test_hvals(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"x": 10, "y": 20})
    assert sorted(locmem_cache.hvals("k")) == [10, 20]


def test_hvals_missing(locmem_cache: LocMemCache):
    assert locmem_cache.hvals("missing") == []


def test_hexists(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"name": "alice"})
    assert locmem_cache.hexists("k", "name") is True
    assert locmem_cache.hexists("k", "age") is False


def test_hexists_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.hexists("missing", "f") is False


def test_hmget(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1, "b": 2, "c": 3})
    assert locmem_cache.hmget("k", "a", "c") == [1, 3]


def test_hmget_missing_fields(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1})
    assert locmem_cache.hmget("k", "a", "z") == [1, None]


def test_hmget_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.hmget("missing", "a", "b") == [None, None]


def test_hsetnx_sets_new_field(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1})
    assert locmem_cache.hsetnx("k", "b", 2) is True
    assert locmem_cache.hgetall("k") == {"a": 1, "b": 2}


def test_hsetnx_skips_existing_field(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1})
    assert locmem_cache.hsetnx("k", "a", 99) is False
    assert locmem_cache.hgetall("k") == {"a": 1}


def test_hsetnx_creates_hash(locmem_cache: LocMemCache):
    assert locmem_cache.hsetnx("k", "f", "v") is True
    assert locmem_cache.hgetall("k") == {"f": "v"}


def test_hincrby_new_field(locmem_cache: LocMemCache):
    assert locmem_cache.hincrby("k", "count", 5) == 5


def test_hincrby_existing_field(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"count": 10})
    assert locmem_cache.hincrby("k", "count", 3) == 13


def test_hincrby_creates_hash(locmem_cache: LocMemCache):
    assert locmem_cache.hincrby("k", "x") == 1
    assert locmem_cache.hgetall("k") == {"x": 1}


def test_hincrbyfloat_new_field(locmem_cache: LocMemCache):
    assert locmem_cache.hincrbyfloat("k", "score", 1.5) == pytest.approx(1.5)


def test_hincrbyfloat_existing_field(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"score": 2.5})
    assert locmem_cache.hincrbyfloat("k", "score", 0.5) == pytest.approx(3.0)


def test_hincrbyfloat_int_field(locmem_cache: LocMemCache):
    locmem_cache.hset("k", "f", 2)
    assert locmem_cache.hincrbyfloat("k", "f", 0.5) == pytest.approx(2.5)


def test_hincrby_non_integer_field_raises(locmem_cache: LocMemCache):
    locmem_cache.hset("k", "f", "abc")
    with pytest.raises(ValueError, match="not an integer"):
        locmem_cache.hincrby("k", "f", 1)
    assert locmem_cache.hget("k", "f") == "abc"


def test_hincrby_float_field_raises(locmem_cache: LocMemCache):
    # Redis rejects HINCRBY on a float value; int() would truncate it.
    locmem_cache.hset("k", "f", 3.5)
    with pytest.raises(ValueError, match="not an integer"):
        locmem_cache.hincrby("k", "f", 1)
    assert locmem_cache.hget("k", "f") == 3.5


def test_hincrbyfloat_non_numeric_field_raises(locmem_cache: LocMemCache):
    locmem_cache.hset("k", "f", "abc")
    with pytest.raises(ValueError, match="not a float"):
        locmem_cache.hincrbyfloat("k", "f", 1.0)
    assert locmem_cache.hget("k", "f") == "abc"


def test_hash_ops_preserve_ttl(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1})
    locmem_cache.expire("k", 3600)
    locmem_cache.hset("k", "b", 2)
    assert 3590 <= locmem_cache.ttl("k") <= 3600


def test_hash_ops_no_expiry_stays(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1})
    locmem_cache.hset("k", "b", 2)
    assert locmem_cache.ttl("k") is None


def test_hdel_counts_a_repeated_field_once(locmem_cache: LocMemCache):
    locmem_cache.hset("k", mapping={"a": 1, "b": 2})
    assert locmem_cache.hdel("k", "a", "a") == 1
    assert locmem_cache.hkeys("k") == ["b"]


# =============================================================================
# Sorted sets
# =============================================================================


def test_zadd_basic(locmem_cache: LocMemCache):
    assert locmem_cache.zadd("k", {"a": 1.0, "b": 2.0}) == 2
    assert locmem_cache.zcard("k") == 2


def test_zadd_existing_member_no_count_change(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zadd("k", {"a": 5.0}) == 0
    assert locmem_cache.zscore("k", "a") == 5.0


def test_zadd_with_ch(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    # ``ch=True`` returns the count of changed members (added OR score changed)
    assert locmem_cache.zadd("k", {"a": 2.0, "b": 3.0}, ch=True) == 2


def test_zadd_with_nx(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zadd("k", {"a": 99.0, "b": 2.0}, nx=True) == 1
    assert locmem_cache.zscore("k", "a") == 1.0
    assert locmem_cache.zscore("k", "b") == 2.0


def test_zadd_with_xx(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zadd("k", {"a": 5.0, "b": 2.0}, xx=True) == 0
    assert locmem_cache.zscore("k", "a") == 5.0
    assert locmem_cache.zscore("k", "b") is None


def test_zadd_with_gt(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 5.0})
    locmem_cache.zadd("k", {"a": 3.0}, gt=True)
    assert locmem_cache.zscore("k", "a") == 5.0
    locmem_cache.zadd("k", {"a": 10.0}, gt=True)
    assert locmem_cache.zscore("k", "a") == 10.0


def test_zadd_with_lt(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 5.0})
    locmem_cache.zadd("k", {"a": 10.0}, lt=True)
    assert locmem_cache.zscore("k", "a") == 5.0
    locmem_cache.zadd("k", {"a": 1.0}, lt=True)
    assert locmem_cache.zscore("k", "a") == 1.0


def test_zadd_xx_on_missing_key_creates_nothing(locmem_cache: LocMemCache):
    # Regression: an all-filtered write registered a phantom empty zset.
    assert locmem_cache.zadd("k", {"a": 1.0}, xx=True) == 0
    assert locmem_cache.has_key("k") is False


@pytest.mark.parametrize(
    "flags",
    [{"nx": True, "xx": True}, {"gt": True, "lt": True}, {"nx": True, "gt": True}, {"nx": True, "lt": True}],
    ids=["nx+xx", "gt+lt", "nx+gt", "nx+lt"],
)
def test_zadd_rejects_the_flag_combinations_redis_py_rejects(locmem_cache: LocMemCache, flags):
    locmem_cache.zadd("k", {"a": 1.0})
    with pytest.raises(ValueError, match="ZADD"):
        locmem_cache.zadd("k", {"a": 2.0, "b": 3.0}, **flags)
    assert locmem_cache.zrange("k", 0, -1, withscores=True) == [("a", 1.0)]


def test_zadd_mixed_member_types_with_equal_str(locmem_cache: LocMemCache):
    # Regression: score ties fell back to comparing raw members and
    # raised TypeError for 1 vs "1" (equal str, incomparable types).
    locmem_cache.zadd("k", {1: 1.0, "1": 1.0})
    assert locmem_cache.zcard("k") == 2
    assert set(locmem_cache.zrange("k", 0, -1)) == {1, "1"}
    assert locmem_cache.zrem("k", 1) == 1
    assert locmem_cache.zrange("k", 0, -1) == ["1"]


def test_zadd_coerces_scores_to_float(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": "1"})
    locmem_cache.zadd("k", {"b": 2.0})
    assert locmem_cache.zscore("k", "a") == 1.0
    assert isinstance(locmem_cache.zscore("k", "a"), float)
    assert locmem_cache.zrange("k", 0, -1) == ["a", "b"]


def test_zadd_rejects_a_non_numeric_score(locmem_cache: LocMemCache):
    with pytest.raises(ValueError, match="not a valid float"):
        locmem_cache.zadd("k", {"a": 1.0, "b": "abc"})
    assert locmem_cache.zcard("k") == 0


def test_zcard_missing(locmem_cache: LocMemCache):
    assert locmem_cache.zcard("missing") == 0


def test_zscore_missing_member(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zscore("k", "z") is None


def test_zscore_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zscore("missing", "a") is None


def test_zrank(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zrank("k", "a") == 0
    assert locmem_cache.zrank("k", "c") == 2


def test_zrank_missing_member(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zrank("k", "z") is None


def test_zrank_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zrank("missing", "a") is None


def test_zrevrank(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zrevrank("k", "c") == 0
    assert locmem_cache.zrevrank("k", "a") == 2


def test_zrevrank_missing(locmem_cache: LocMemCache):
    assert locmem_cache.zrevrank("missing", "a") is None


def test_zrange(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zrange("k", 0, -1) == ["a", "b", "c"]
    assert locmem_cache.zrange("k", 0, 1) == ["a", "b"]


def test_zrange_negative_indices(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zrange("k", -2, -1) == ["b", "c"]


def test_zrange_withscores(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
    assert locmem_cache.zrange("k", 0, -1, withscores=True) == [("a", 1.0), ("b", 2.0)]


def test_zrange_out_of_bounds(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zrange("k", 5, 10) == []


def test_zrange_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zrange("missing", 0, -1) == []


def test_zrevrange(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zrevrange("k", 0, -1) == ["c", "b", "a"]


def test_zrevrange_withscores(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
    assert locmem_cache.zrevrange("k", 0, -1, withscores=True) == [("b", 2.0), ("a", 1.0)]


def test_zrevrange_negative(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zrevrange("k", -2, -1) == ["b", "a"]


def test_zrevrange_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zrevrange("missing", 0, -1) == []


def test_zrangebyscore(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert locmem_cache.zrangebyscore("k", 2.0, 3.0) == ["b", "c"]


def test_zrangebyscore_inf(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
    assert locmem_cache.zrangebyscore("k", "-inf", "+inf") == ["a", "b"]


def test_zrangebyscore_withscores(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
    assert locmem_cache.zrangebyscore("k", 1.0, 2.0, withscores=True) == [("a", 1.0), ("b", 2.0)]


def test_zrangebyscore_pagination(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert locmem_cache.zrangebyscore("k", "-inf", "+inf", start=1, num=2) == ["b", "c"]


def test_zrangebyscore_negative_num_reaches_the_end(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert locmem_cache.zrangebyscore("k", "-inf", "+inf", start=0, num=-1) == ["a", "b", "c", "d"]


@pytest.mark.parametrize("method", ["zrangebyscore", "zrevrangebyscore"])
@pytest.mark.parametrize("limit", [{"start": 1}, {"num": 2}], ids=["start-only", "num-only"])
def test_one_sided_limit_is_rejected(locmem_cache: LocMemCache, method: str, limit: dict):
    # Regression: a lone ``start`` or ``num`` silently returned the whole
    # range; redis-py raises for the same call.
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    with pytest.raises(ValueError, match="start and num must both be specified"):
        getattr(locmem_cache, method)("k", "-inf", "+inf", **limit)


def test_zrevrangebyscore(locmem_cache: LocMemCache):
    # Same fixture and expectation as the RESP test in test_sorted_sets.py.
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
    assert locmem_cache.zrevrangebyscore("k", 4.0, 2.0) == ["d", "c", "b"]


def test_zrevrangebyscore_withscores(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zrevrangebyscore("k", "+inf", "-inf", withscores=True) == [
        ("c", 3.0),
        ("b", 2.0),
        ("a", 1.0),
    ]


def test_zrevrangebyscore_limit_applies_after_reversal(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert locmem_cache.zrevrangebyscore("k", "+inf", "-inf", start=1, num=2) == ["c", "b"]
    assert locmem_cache.zrevrangebyscore("k", "+inf", "-inf", start=1, num=-1) == ["c", "b", "a"]


def test_zrevrangebyscore_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zrevrangebyscore("missing", 100.0, 0.0) == []


@pytest.mark.asyncio
async def test_azrevrangebyscore(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert await locmem_cache.azrevrangebyscore("k", 3.0, 2.0) == ["c", "b"]


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("zrangebyscore", ("k", "(1", "+inf")),
        ("zcount", ("k", "(1", "+inf")),
        ("zremrangebyscore", ("k", "-inf", "(3")),
    ],
    ids=["zrangebyscore", "zcount", "zremrangebyscore"],
)
def test_exclusive_bound_raises_not_supported(locmem_cache: LocMemCache, method, args):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    with pytest.raises(NotSupportedError):
        getattr(locmem_cache, method)(*args)


def test_zrangebyscore_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zrangebyscore("missing", 0.0, 100.0) == []


def test_zrem(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zrem("k", "b") == 1
    assert locmem_cache.zcard("k") == 2


def test_zrem_multiple(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zrem("k", "a", "c") == 2


def test_zrem_nonexistent_member(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zrem("k", "z") == 0


def test_zrem_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zrem("missing", "a") == 0


def test_zrem_deletes_when_empty(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    locmem_cache.zrem("k", "a")
    assert locmem_cache.get("k") is None


def test_zincrby_existing_member(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zincrby("k", 2.5, "a") == pytest.approx(3.5)


def test_zincrby_creates_member(locmem_cache: LocMemCache):
    assert locmem_cache.zincrby("k", 5.0, "new") == pytest.approx(5.0)


def test_zincrby_coerces_the_amount(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zincrby("k", "2", "a") == 3.0


def test_zcount(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert locmem_cache.zcount("k", 2.0, 3.0) == 2


def test_zcount_inf(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
    assert locmem_cache.zcount("k", "-inf", "+inf") == 2


def test_zcount_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zcount("missing", 0.0, 10.0) == 0


def test_zpopmin(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zpopmin("k") == [("a", 1.0)]
    assert locmem_cache.zcard("k") == 2


def test_zpopmin_with_count(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zpopmin("k", count=2) == [("a", 1.0), ("b", 2.0)]


def test_zpopmin_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zpopmin("missing") == []


def test_zpopmin_deletes_when_empty(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    locmem_cache.zpopmin("k")
    assert locmem_cache.get("k") is None


def test_zpopmax(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zpopmax("k") == [("c", 3.0)]
    assert locmem_cache.zcard("k") == 2


def test_zpopmax_with_count(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zpopmax("k", count=2) == [("c", 3.0), ("b", 2.0)]


def test_zpopmax_count_zero_pops_nothing(locmem_cache: LocMemCache):
    # Regression: count=0 sliced ``[-0:]`` and popped the whole zset.
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
    assert locmem_cache.zpopmax("k", count=0) == []
    assert locmem_cache.zcard("k") == 2


def test_zpopmax_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zpopmax("missing") == []


def test_zpopmax_deletes_when_empty(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    locmem_cache.zpopmax("k")
    assert locmem_cache.get("k") is None


@pytest.mark.parametrize("method", ["zpopmin", "zpopmax"])
def test_zpop_negative_count_rejected(locmem_cache: LocMemCache, method: str):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    with pytest.raises(ValueError, match="must be positive"):
        getattr(locmem_cache, method)("k", -2)
    assert locmem_cache.zcard("k") == 3


def test_zmscore(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
    assert locmem_cache.zmscore("k", "a", "b", "missing") == [1.0, 2.0, None]


def test_zmscore_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zmscore("missing", "a", "b") == [None, None]


def test_zremrangebyscore(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert locmem_cache.zremrangebyscore("k", 2.0, 3.0) == 2
    assert locmem_cache.zrange("k", 0, -1) == ["a", "d"]


def test_zremrangebyscore_inf(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
    assert locmem_cache.zremrangebyscore("k", "-inf", "+inf") == 2
    assert locmem_cache.get("k") is None


def test_zremrangebyscore_no_match(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zremrangebyscore("k", 5.0, 10.0) == 0


def test_zremrangebyscore_keeps_the_sidecar_consistent(locmem_cache: LocMemCache):
    # The removal walks the sorted sidecar; the survivors must still rank.
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "b2": 2.0, "c": 3.0, "d": 4.0})
    assert locmem_cache.zremrangebyscore("k", 2.0, 3.0) == 3
    assert locmem_cache.zrange("k", 0, -1, withscores=True) == [("a", 1.0), ("d", 4.0)]
    assert locmem_cache.zrank("k", "d") == 1


def test_zremrangebyscore_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zremrangebyscore("missing", 0.0, 10.0) == 0


def test_zremrangebyrank(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
    assert locmem_cache.zremrangebyrank("k", 0, 1) == 2
    assert locmem_cache.zrange("k", 0, -1) == ["c", "d"]


def test_zremrangebyrank_negative(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert locmem_cache.zremrangebyrank("k", -2, -1) == 2
    assert locmem_cache.zrange("k", 0, -1) == ["a"]


def test_zremrangebyrank_out_of_bounds(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0})
    assert locmem_cache.zremrangebyrank("k", 5, 10) == 0


def test_zremrangebyrank_missing_key(locmem_cache: LocMemCache):
    assert locmem_cache.zremrangebyrank("missing", 0, -1) == 0


def test_zremrangebyrank_deletes_when_empty(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
    locmem_cache.zremrangebyrank("k", 0, -1)
    assert locmem_cache.get("k") is None


def test_zrem_counts_a_repeated_member_once(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
    assert locmem_cache.zrem("k", "a", "a") == 1
    assert locmem_cache.zrange("k", 0, -1) == ["b"]


@pytest.mark.parametrize("score", [float("nan"), "nan"])
def test_zadd_rejects_a_nan_score(locmem_cache: LocMemCache, score):
    locmem_cache.zadd("k", {"a": 1.0})
    with pytest.raises(ValueError, match="not a valid float"):
        locmem_cache.zadd("k", {"b": 2.0, "c": score})
    assert locmem_cache.zrange("k", 0, -1, withscores=True) == [("a", 1.0)]


def test_zincrby_rejects_a_nan_increment(locmem_cache: LocMemCache):
    with pytest.raises(ValueError, match="not a valid float"):
        locmem_cache.zincrby("k", float("nan"), "a")
    assert locmem_cache.type("k") is None


def test_zincrby_rejects_a_nan_sum(locmem_cache: LocMemCache):
    locmem_cache.zadd("k", {"a": float("inf")})
    with pytest.raises(ValueError, match="NaN"):
        locmem_cache.zincrby("k", float("-inf"), "a")
    assert locmem_cache.zscore("k", "a") == float("inf")


# =============================================================================
# Version handling
# =============================================================================


def test_versioned_keys_independent(locmem_cache: LocMemCache):
    locmem_cache.set("k", "v1", version=1)
    locmem_cache.set("k", "v2", version=2)
    assert locmem_cache.get("k", version=1) == "v1"
    assert locmem_cache.get("k", version=2) == "v2"


def test_versioned_extension_ops(locmem_cache: LocMemCache):
    locmem_cache.lpush("k", "a", version=1)
    locmem_cache.lpush("k", "b", version=2)
    assert locmem_cache.lrange("k", 0, -1, version=1) == ["a"]
    assert locmem_cache.lrange("k", 0, -1, version=2) == ["b"]


def test_incr_version_moves_string(locmem_cache: LocMemCache):
    locmem_cache.set("k", 5)
    assert locmem_cache.incr_version("k") == 2
    assert locmem_cache.get("k", version=2) == 5
    assert locmem_cache.get("k") is None


def test_incr_version_moves_collection(locmem_cache: LocMemCache):
    # Regression: the inherited get/set/delete round trip raised
    # WrongTypeError on a collection key. RespCache uses RENAME.
    locmem_cache.rpush("k", "a", "b")
    locmem_cache.expire("k", 1000)
    assert locmem_cache.incr_version("k") == 2
    assert locmem_cache.lrange("k", 0, -1, version=2) == ["a", "b"]
    assert locmem_cache.has_key("k") is False
    assert locmem_cache.ttl("k", version=2) >= 999


def test_incr_version_overwrites_destination(locmem_cache: LocMemCache):
    locmem_cache.set("k", "src")
    locmem_cache.set("k", "dst", version=2)
    locmem_cache.incr_version("k")
    assert locmem_cache.get("k", version=2) == "src"


def test_incr_version_missing_key_raises(locmem_cache: LocMemCache):
    with pytest.raises(ValueError, match="not found"):
        locmem_cache.incr_version("nope")


def test_incr_version_zero_delta_keeps_a_string(locmem_cache: LocMemCache):
    # Regression: the destination delete hit the source, then the move
    # raised KeyError on the now-missing entry.
    locmem_cache.set("k", 5, timeout=1000)
    assert locmem_cache.incr_version("k", 0) == 1
    assert locmem_cache.get("k") == 5
    assert locmem_cache.ttl("k") >= 999


def test_incr_version_zero_delta_keeps_a_collection(locmem_cache: LocMemCache):
    locmem_cache.rpush("k", "a", "b")
    assert locmem_cache.incr_version("k", 0) == 1
    assert locmem_cache.lrange("k", 0, -1) == ["a", "b"]


def test_incr_version_zero_delta_on_a_missing_key_raises(locmem_cache: LocMemCache):
    with pytest.raises(ValueError, match="not found"):
        locmem_cache.incr_version("nope", 0)


def test_decr_version_moves_collection(locmem_cache: LocMemCache):
    locmem_cache.sadd("k", "m", version=2)
    assert locmem_cache.decr_version("k", version=2) == 1
    assert locmem_cache.smembers("k") == {"m"}


@pytest.mark.asyncio
async def test_aincr_version_moves_collection(locmem_cache: LocMemCache):
    # Regression: on Django 6.0 aincr_version composed aget/aset/adelete
    # and raised WrongTypeError on a list key.
    locmem_cache.rpush("k", "a", "b")
    assert await locmem_cache.aincr_version("k") == 2
    assert locmem_cache.lrange("k", 0, -1, version=2) == ["a", "b"]
    assert await locmem_cache.adecr_version("k", version=2) == 1
    assert locmem_cache.lrange("k", 0, -1) == ["a", "b"]


# =============================================================================
# RESP-faithful string reads
# =============================================================================


def _ghost_key(cache: LocMemCache, key: str) -> None:
    """Leave behind the state a concurrent collection write creates mid-flight.

    ``_native_write`` sets ``_expire_info[key] = None`` (never expired) and
    stores the value outside ``_cache``, so Django's ``get`` would sail past
    ``_has_expired`` into ``self._cache[key]`` and raise ``KeyError``.
    """
    cache._expire_info[cache.make_key(key)] = None


# get/incr/get_many resolve the collection check and the read under one hold of the non-reentrant ``_lock``.
def test_get_returns_default_when_key_is_not_in_the_pickled_store(locmem_cache: LocMemCache):
    _ghost_key(locmem_cache, "ghost")
    assert locmem_cache.get("ghost") is None
    assert locmem_cache.get("ghost", "fallback") == "fallback"


def test_incr_raises_valueerror_when_key_is_not_in_the_pickled_store(locmem_cache: LocMemCache):
    _ghost_key(locmem_cache, "ghost")
    with pytest.raises(ValueError, match="not found"):
        locmem_cache.incr("ghost")


def test_get_wrongtype_message_uses_the_user_key(locmem_cache: LocMemCache):
    locmem_cache.rpush("lk", 1)
    with pytest.raises(WrongTypeError, match="'lk'") as exc_info:
        locmem_cache.get("lk")
    assert ":1:lk" not in str(exc_info.value)


def test_typed_wrongtype_message_uses_the_user_key(locmem_cache: LocMemCache):
    locmem_cache.rpush("lk", 1)
    with pytest.raises(WrongTypeError, match="'lk'") as exc_info:
        locmem_cache.sadd("lk", "x")
    assert ":1:lk" not in str(exc_info.value)


def test_get_still_moves_the_key_to_the_front(locmem_cache: LocMemCache):
    locmem_cache.set("a", 1)
    locmem_cache.set("b", 2)
    locmem_cache.get("a")
    assert next(iter(locmem_cache._cache)) == locmem_cache.make_key("a")


def test_get_many_skips_collection_keys(locmem_cache: LocMemCache):
    # MGET reports a list/hash/set/zset key as nil, so RespCache omits it;
    # the inherited get()-per-key path aborted the whole batch instead.
    locmem_cache.set("plain", 1)
    locmem_cache.rpush("lst", "a")
    locmem_cache.hset("hsh", "f", "v")
    assert locmem_cache.get_many(["plain", "lst", "hsh", "missing"]) == {"plain": 1}


def test_get_many_honors_version(locmem_cache: LocMemCache):
    locmem_cache.set("k", "v1", version=1)
    locmem_cache.set("k", "v2", version=2)
    assert locmem_cache.get_many(["k"], version=2) == {"k": "v2"}


def test_get_many_skips_expired_keys(locmem_cache: LocMemCache):
    locmem_cache.set("live", 1)
    locmem_cache.set("dead", 2, timeout=-1)
    assert locmem_cache.get_many(["live", "dead"]) == {"live": 1}


@pytest.mark.asyncio
async def test_aget_many_skips_collection_keys(locmem_cache: LocMemCache):
    locmem_cache.set("plain", 1)
    locmem_cache.rpush("lst", "a")
    assert await locmem_cache.aget_many(["plain", "lst"]) == {"plain": 1}


# =============================================================================
# ``_ZSet`` sidecar
# =============================================================================


# Unpickling replays items through ``__setitem__`` before restoring ``__dict__``, so the sidecar is rebuilt.
def test_pickle_round_trip_keeps_one_entry_per_member():
    restored = pickle.loads(pickle.dumps(_ZSet({"a": 1.0, "b": 2.0})))
    assert isinstance(restored, _ZSet)
    assert restored.sorted_members() == [("a", 1.0), ("b", 2.0)]
    assert restored.rank_of("b") == 1


def test_deepcopy_does_not_duplicate_members():
    copied = copy.deepcopy(_ZSet({"a": 1.0, "b": 2.0}))
    assert copied.sorted_members() == [("a", 1.0), ("b", 2.0)]
    assert copied.revrank_of("a") == 1


def test_reported_memory_counts_the_sidecar():
    assert _deep_getsizeof(_ZSet({"a": 1.0})) > _deep_getsizeof({"a": 1.0})


# ``_glob_to_regex`` follows Redis's ``stringmatchlen`` on ranges.
def test_reversed_range_is_swapped():
    assert _glob_to_regex("[z-a]").match("m")
    assert _glob_to_regex("[9-0]").match("5")


def test_escaped_range_end_stays_a_bound():
    # Redis reads the end of a range raw, so ``[a-\z]`` is ``\``..``a``
    # followed by a literal ``z``.
    pattern = _glob_to_regex(r"[a-\z]")
    assert pattern.match("_")
    assert pattern.match("z")
    assert not pattern.match("m")


# =============================================================================
# Async twins
# =============================================================================


def _seed_twin_data(cache: LocMemCache) -> None:
    cache.clear()
    cache.set("s", 5, timeout=300)
    cache.rpush("l", "a", "b", "a")
    cache.sadd("one", "a")
    cache.sadd("two", "a", "b")
    cache.hset("h", mapping={"f": 1, "g": 2.5})
    cache.zadd("z", {"a": 1.0, "b": 2.0, "c": 3.0})


def _twin_state(cache: LocMemCache) -> tuple[dict[str, object], dict[str, int | None]]:
    now = time.time()
    with cache._lock:
        values = {key: pickle.loads(value) for key, value in cache._cache.items()}
        values |= {key: (type(value).__name__, copy.deepcopy(value)) for key, value in cache._collections.items()}
        ttls = {key: None if expiry is None else round(expiry - now) for key, expiry in cache._expire_info.items()}
    return values, ttls


# One-member sets keep ``spop``/``srandmember`` deterministic.
_ASYNC_TWIN_CASES = [
    ("aset", ("s", 7), {"get": True}),
    ("ahas_key", ("l",), {}),
    ("aincr", ("s",), {}),
    ("adecr", ("s", 2), {}),
    ("aget_many", (["s", "l", "missing"],), {}),
    ("aincr_version", ("s",), {}),
    ("adecr_version", ("s",), {}),
    ("attl", ("s",), {}),
    ("atype", ("z",), {}),
    ("apersist", ("s",), {}),
    ("aexpire", ("l", 100), {}),
    ("akeys", ("*",), {}),
    ("ascan", (0,), {"count": 2}),
    ("aiter_keys", ("*",), {}),
    ("adelete_pattern", ("*o*",), {}),
    ("alpush", ("l", "x", "y"), {}),
    ("arpush", ("l", "x"), {}),
    ("alpop", ("l",), {"count": 2}),
    ("arpop", ("l",), {}),
    ("alrange", ("l", 0, -1), {}),
    ("allen", ("l",), {}),
    ("alrem", ("l", 0, "a"), {}),
    ("altrim", ("l", 0, 1), {}),
    ("alindex", ("l", 1), {}),
    ("alset", ("l", 0, "z"), {}),
    ("alinsert", ("l", "BEFORE", "b", "q"), {}),
    ("alpos", ("l", "a"), {"count": 0}),
    ("asadd", ("one", "b", "c"), {}),
    ("asrem", ("two", "a", "zz"), {}),
    ("ascard", ("two",), {}),
    ("asismember", ("one", "a"), {}),
    ("asmembers", ("two",), {}),
    ("aspop", ("one",), {}),
    ("asrandmember", ("one", -3), {}),
    ("asmismember", ("two", "a", "x"), {}),
    ("asdiff", (["two", "one"],), {}),
    ("asinter", (["two", "one"],), {}),
    ("asunion", (["two", "one"],), {}),
    ("ahset", ("h", "n", 1), {"mapping": {"m": 2}}),
    ("ahdel", ("h", "f", "zz"), {}),
    ("ahget", ("h", "f"), {}),
    ("ahgetall", ("h",), {}),
    ("ahlen", ("h",), {}),
    ("ahkeys", ("h",), {}),
    ("ahvals", ("h",), {}),
    ("ahexists", ("h", "g"), {}),
    ("ahmget", ("h", "f", "zz"), {}),
    ("ahsetnx", ("h", "new", 1), {}),
    ("ahincrby", ("h", "f", 2), {}),
    ("ahincrbyfloat", ("h", "g", 0.5), {}),
    ("azadd", ("z", {"d": 4.0, "a": 0.5}), {"ch": True}),
    ("azcard", ("z",), {}),
    ("azscore", ("z", "b"), {}),
    ("azrank", ("z", "c"), {}),
    ("azrevrank", ("z", "c"), {}),
    ("azrange", ("z", 0, 1), {"withscores": True}),
    ("azrevrange", ("z", 0, 1), {}),
    ("azrangebyscore", ("z", 1, 2), {"withscores": True}),
    ("azrevrangebyscore", ("z", 3, 2), {}),
    ("azrem", ("z", "a", "zz"), {}),
    ("azincrby", ("z", 2.5, "a"), {}),
    ("azcount", ("z", 1, 2), {}),
    ("azpopmin", ("z",), {"count": 2}),
    ("azpopmax", ("z",), {}),
    ("azmscore", ("z", "a", "zz"), {}),
    ("azremrangebyrank", ("z", 0, 0), {}),
    ("azremrangebyscore", ("z", 2, 3), {}),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "args", "kwargs"), _ASYNC_TWIN_CASES, ids=[case[0] for case in _ASYNC_TWIN_CASES])
async def test_async_twin_matches_sync(locmem_cache: LocMemCache, name, args, kwargs):
    _seed_twin_data(locmem_cache)
    expected = getattr(locmem_cache, name.removeprefix("a"))(*args, **kwargs)
    if inspect.isgenerator(expected):
        expected = list(expected)
    expected_state = _twin_state(locmem_cache)
    _seed_twin_data(locmem_cache)
    call = getattr(locmem_cache, name)(*args, **kwargs)
    result = [item async for item in call] if inspect.isasyncgen(call) else await call
    assert result == expected
    assert _twin_state(locmem_cache) == expected_state


def test_async_twin_cases_cover_every_async_method():
    defined = {
        name
        for name, member in vars(LocMemCache).items()
        if inspect.iscoroutinefunction(member) or inspect.isasyncgenfunction(member)
    }
    # ``asemaphore`` builds a new Semaphore per call; test_semaphores.py covers it.
    assert defined - {"asemaphore"} == {case[0] for case in _ASYNC_TWIN_CASES}
