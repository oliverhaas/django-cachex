"""Tests for ``django_cachex.cache.locmem.LocMemCache``.

LocMemCache is a cachex-native backend (extends Django's
``LocMemCache`` with the cachex extension surface), so its tests live
here next to the other cachex backends rather than in
``tests/admin/test_wrappers.py``.
"""

from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.test import override_settings

from django_cachex.cache.locmem import LocMemCache

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


class TestBackend:
    def test_locmem_is_cachex(self, locmem_cache: LocMemCache):
        assert isinstance(locmem_cache, LocMemCache)
        assert locmem_cache._cachex_support == "cachex"

    def test_locmem_has_extension_methods(self, locmem_cache: LocMemCache):
        for name in ("lpush", "sadd", "hset", "zadd"):
            assert hasattr(locmem_cache, name)


# =============================================================================
# Keys, TTL, expire, persist, type, info, delete_pattern, iter_keys
# =============================================================================


class TestKeysAndAdmin:
    def test_set_and_get(self, locmem_cache: LocMemCache):
        locmem_cache.set("key1", "value1")
        assert locmem_cache.get("key1") == "value1"

    def test_get_missing_returns_default(self, locmem_cache: LocMemCache):
        assert locmem_cache.get("missing") is None
        assert locmem_cache.get("missing", "default") == "default"

    def test_delete(self, locmem_cache: LocMemCache):
        locmem_cache.set("key1", "value1")
        assert locmem_cache.delete("key1") is True
        assert locmem_cache.get("key1") is None

    def test_clear(self, locmem_cache: LocMemCache):
        locmem_cache.set("key1", "value1")
        locmem_cache.set("key2", "value2")
        locmem_cache.clear()
        assert locmem_cache.get("key1") is None

    def test_keys_returns_all(self, locmem_cache: LocMemCache):
        locmem_cache.set("alpha", 1)
        locmem_cache.set("beta", 2)
        keys = locmem_cache.keys()
        assert "alpha" in keys
        assert "beta" in keys

    def test_keys_with_pattern(self, locmem_cache: LocMemCache):
        locmem_cache.set("user:1", "alice")
        locmem_cache.set("user:2", "bob")
        locmem_cache.set("session:abc", "data")
        keys = locmem_cache.keys("user:*")
        assert "user:1" in keys
        assert "user:2" in keys
        assert "session:abc" not in keys

    def test_iter_keys(self, locmem_cache: LocMemCache):
        locmem_cache.set("a", 1)
        locmem_cache.set("b", 2)
        assert sorted(locmem_cache.iter_keys()) == ["a", "b"]

    def test_iter_keys_with_pattern(self, locmem_cache: LocMemCache):
        locmem_cache.set("user:1", "alice")
        locmem_cache.set("session:1", "x")
        assert list(locmem_cache.iter_keys("user:*")) == ["user:1"]

    def test_delete_pattern(self, locmem_cache: LocMemCache):
        locmem_cache.set("user:1", 1)
        locmem_cache.set("user:2", 2)
        locmem_cache.set("session:abc", "x")
        assert locmem_cache.delete_pattern("user:*") == 2
        assert locmem_cache.get("user:1") is None
        assert locmem_cache.get("session:abc") == "x"

    def test_delete_pattern_no_match(self, locmem_cache: LocMemCache):
        locmem_cache.set("a", 1)
        assert locmem_cache.delete_pattern("missing:*") == 0

    def test_ttl_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.ttl("nonexistent") == -2

    def test_ttl_persistent_key(self, locmem_cache: LocMemCache):
        locmem_cache.set("forever", "value", timeout=None)
        assert locmem_cache.ttl("forever") == -1

    def test_ttl_expiring_key(self, locmem_cache: LocMemCache):
        locmem_cache.set("temp", "value", timeout=3600)
        assert 3590 <= locmem_cache.ttl("temp") <= 3600

    def test_expire(self, locmem_cache: LocMemCache):
        locmem_cache.set("key1", "value1", timeout=None)
        assert locmem_cache.ttl("key1") == -1
        locmem_cache.expire("key1", 100)
        assert 90 <= locmem_cache.ttl("key1") <= 100

    def test_expire_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.expire("nonexistent", 100) is False

    def test_persist(self, locmem_cache: LocMemCache):
        locmem_cache.set("key1", "value1", timeout=60)
        assert locmem_cache.ttl("key1") > 0
        locmem_cache.persist("key1")
        assert locmem_cache.ttl("key1") == -1

    def test_persist_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.persist("nonexistent") is False

    def test_info_returns_dict(self, locmem_cache: LocMemCache):
        locmem_cache.set("key1", "value1")
        info = locmem_cache.info()
        assert info["backend"] == "LocMemCache"
        assert "server" in info
        assert "memory" in info
        assert info["keyspace"]["db0"]["keys"] >= 1

    def test_type_string(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", "hello")
        assert locmem_cache.type("k") == "string"

    def test_type_int_is_string(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", 42)
        assert locmem_cache.type("k") == "string"

    def test_type_list(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", [1, 2, 3])
        assert locmem_cache.type("k") == "list"

    def test_type_set(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {1, 2, 3})
        assert locmem_cache.type("k") == "set"

    def test_type_empty_set(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", set())
        assert locmem_cache.type("k") == "set"

    def test_type_hash_str_keys(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"name": "alice"})
        assert locmem_cache.type("k") == "hash"

    def test_type_empty_dict_is_hash(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {})
        assert locmem_cache.type("k") == "hash"

    def test_type_int_keyed_dict_is_string(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {1: "a", 2: "b"})
        assert locmem_cache.type("k") == "string"

    def test_type_mixed_keyed_dict_is_string(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1, 2: "b"})
        assert locmem_cache.type("k") == "string"

    def test_type_missing(self, locmem_cache: LocMemCache):
        assert locmem_cache.type("missing") is None


# =============================================================================
# Lists
# =============================================================================


class TestListOps:
    def test_lpush_creates_new_list(self, locmem_cache: LocMemCache):
        assert locmem_cache.lpush("k", "a") == 1
        assert locmem_cache.get("k") == ["a"]

    def test_lpush_prepends(self, locmem_cache: LocMemCache):
        locmem_cache.lpush("k", "a")
        locmem_cache.lpush("k", "b")
        assert locmem_cache.get("k") == ["b", "a"]

    def test_lpush_multiple_values(self, locmem_cache: LocMemCache):
        assert locmem_cache.lpush("k", "a", "b", "c") == 3
        assert locmem_cache.get("k") == ["c", "b", "a"]

    def test_lpush_type_error_on_non_list(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", "string")
        with pytest.raises(TypeError):
            locmem_cache.lpush("k", "x")

    def test_rpush_creates_new_list(self, locmem_cache: LocMemCache):
        assert locmem_cache.rpush("k", "a") == 1
        assert locmem_cache.get("k") == ["a"]

    def test_rpush_appends(self, locmem_cache: LocMemCache):
        locmem_cache.rpush("k", "a")
        locmem_cache.rpush("k", "b")
        assert locmem_cache.get("k") == ["a", "b"]

    def test_rpush_multiple_values(self, locmem_cache: LocMemCache):
        assert locmem_cache.rpush("k", "a", "b", "c") == 3
        assert locmem_cache.get("k") == ["a", "b", "c"]

    def test_rpush_type_error_on_non_list(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", "string")
        with pytest.raises(TypeError):
            locmem_cache.rpush("k", "x")

    def test_lpop_returns_first(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", [1, 2, 3])
        assert locmem_cache.lpop("k") == [1]
        assert locmem_cache.get("k") == [2, 3]

    def test_lpop_with_count(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", [1, 2, 3, 4])
        assert locmem_cache.lpop("k", count=2) == [1, 2]
        assert locmem_cache.get("k") == [3, 4]

    def test_lpop_empty(self, locmem_cache: LocMemCache):
        assert locmem_cache.lpop("missing") == []

    def test_lpop_deletes_when_empty(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", [1])
        locmem_cache.lpop("k")
        assert locmem_cache.get("k") is None

    def test_rpop_returns_last(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", [1, 2, 3])
        assert locmem_cache.rpop("k") == [3]
        assert locmem_cache.get("k") == [1, 2]

    def test_rpop_with_count(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", [1, 2, 3, 4])
        assert locmem_cache.rpop("k", count=2) == [4, 3]
        assert locmem_cache.get("k") == [1, 2]

    def test_rpop_empty(self, locmem_cache: LocMemCache):
        assert locmem_cache.rpop("missing") == []

    def test_rpop_deletes_when_empty(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", [1])
        locmem_cache.rpop("k")
        assert locmem_cache.get("k") is None

    def test_lrange_full(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c"])
        assert locmem_cache.lrange("k", 0, -1) == ["a", "b", "c"]

    def test_lrange_partial(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c", "d"])
        assert locmem_cache.lrange("k", 1, 2) == ["b", "c"]

    def test_lrange_negative_indices(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c", "d", "e"])
        assert locmem_cache.lrange("k", -3, -1) == ["c", "d", "e"]

    def test_lrange_missing(self, locmem_cache: LocMemCache):
        assert locmem_cache.lrange("missing", 0, -1) == []

    def test_llen(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", [1, 2, 3])
        assert locmem_cache.llen("k") == 3

    def test_llen_missing(self, locmem_cache: LocMemCache):
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
    def test_lrem(self, locmem_cache: LocMemCache, count: int, expected_removed: int, expected_list: list):
        locmem_cache.set("k", ["a", "b", "a", "c", "a"])
        assert locmem_cache.lrem("k", count, "a") == expected_removed
        assert locmem_cache.get("k") == expected_list

    def test_lrem_not_found(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b"])
        assert locmem_cache.lrem("k", 0, "z") == 0

    def test_lrem_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.lrem("missing", 0, "a") == 0

    def test_lrem_deletes_when_all_removed(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "a"])
        locmem_cache.lrem("k", 0, "a")
        assert locmem_cache.get("k") is None

    def test_ltrim_basic(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c", "d", "e"])
        assert locmem_cache.ltrim("k", 1, 3) is True
        assert locmem_cache.get("k") == ["b", "c", "d"]

    def test_ltrim_negative_end(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c"])
        locmem_cache.ltrim("k", 0, -2)
        assert locmem_cache.get("k") == ["a", "b"]

    def test_ltrim_out_of_range_deletes(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b"])
        locmem_cache.ltrim("k", 5, 10)
        assert locmem_cache.get("k") is None

    def test_ltrim_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.ltrim("missing", 0, -1) is True

    def test_lindex(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c"])
        assert locmem_cache.lindex("k", 0) == "a"
        assert locmem_cache.lindex("k", -1) == "c"

    def test_lindex_out_of_range(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a"])
        assert locmem_cache.lindex("k", 5) is None

    def test_lindex_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.lindex("missing", 0) is None

    def test_lset(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c"])
        assert locmem_cache.lset("k", 1, "B") is True
        assert locmem_cache.get("k") == ["a", "B", "c"]

    def test_lset_missing_key_raises(self, locmem_cache: LocMemCache):
        with pytest.raises(ValueError, match="no such key"):
            locmem_cache.lset("missing", 0, "x")

    def test_lset_out_of_range_raises(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a"])
        with pytest.raises(ValueError, match="index out of range"):
            locmem_cache.lset("k", 5, "x")

    def test_linsert_before(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "c"])
        assert locmem_cache.linsert("k", "BEFORE", "c", "b") == 3
        assert locmem_cache.get("k") == ["a", "b", "c"]

    def test_linsert_after(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b"])
        assert locmem_cache.linsert("k", "AFTER", "a", "X") == 3
        assert locmem_cache.get("k") == ["a", "X", "b"]

    def test_linsert_pivot_not_found(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a"])
        assert locmem_cache.linsert("k", "BEFORE", "z", "x") == -1

    def test_linsert_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.linsert("missing", "BEFORE", "a", "x") == 0

    def test_lpos_basic(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c", "b"])
        assert locmem_cache.lpos("k", "b") == 1

    def test_lpos_with_rank(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c", "b", "d", "b"])
        assert locmem_cache.lpos("k", "b", rank=2) == 3
        assert locmem_cache.lpos("k", "b", rank=-1) == 5

    def test_lpos_with_count(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c", "b", "d", "b"])
        assert locmem_cache.lpos("k", "b", count=0) == [1, 3, 5]
        assert locmem_cache.lpos("k", "b", count=2) == [1, 3]

    def test_lpos_with_maxlen(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a", "b", "c", "b"])
        assert locmem_cache.lpos("k", "b", maxlen=2) == 1

    def test_lpos_not_found(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", ["a"])
        assert locmem_cache.lpos("k", "z") is None
        assert locmem_cache.lpos("k", "z", count=0) == []

    def test_lpos_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.lpos("missing", "x") is None
        assert locmem_cache.lpos("missing", "x", count=0) == []

    def test_list_ops_preserve_ttl(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", [1, 2], timeout=3600)
        locmem_cache.rpush("k", 3)
        assert 3590 <= locmem_cache.ttl("k") <= 3600

    def test_list_ops_no_expiry_stays(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", [1, 2], timeout=None)
        locmem_cache.rpush("k", 3)
        assert locmem_cache.ttl("k") == -1


# =============================================================================
# Sets
# =============================================================================


class TestSetOps:
    def test_sadd_creates_new_set(self, locmem_cache: LocMemCache):
        assert locmem_cache.sadd("k", "a") == 1
        assert locmem_cache.get("k") == {"a"}

    def test_sadd_adds_to_existing(self, locmem_cache: LocMemCache):
        locmem_cache.sadd("k", "a")
        assert locmem_cache.sadd("k", "b") == 1
        assert locmem_cache.get("k") == {"a", "b"}

    def test_sadd_duplicate_returns_zero(self, locmem_cache: LocMemCache):
        locmem_cache.sadd("k", "a")
        assert locmem_cache.sadd("k", "a") == 0

    def test_sadd_multiple_members(self, locmem_cache: LocMemCache):
        assert locmem_cache.sadd("k", "a", "b", "c") == 3
        assert locmem_cache.get("k") == {"a", "b", "c"}

    def test_sadd_type_error_on_non_set(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", "string")
        with pytest.raises(TypeError):
            locmem_cache.sadd("k", "x")

    def test_srem(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a", "b", "c"})
        assert locmem_cache.srem("k", "b") == 1
        assert locmem_cache.get("k") == {"a", "c"}

    def test_srem_multiple_members(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a", "b", "c", "d"})
        assert locmem_cache.srem("k", "a", "c") == 2
        assert locmem_cache.get("k") == {"b", "d"}

    def test_srem_nonexistent_member(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a", "b"})
        assert locmem_cache.srem("k", "z") == 0

    def test_srem_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.srem("missing", "a") == 0

    def test_srem_deletes_when_empty(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a"})
        locmem_cache.srem("k", "a")
        assert locmem_cache.get("k") is None

    def test_scard(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a", "b", "c"})
        assert locmem_cache.scard("k") == 3

    def test_scard_missing(self, locmem_cache: LocMemCache):
        assert locmem_cache.scard("missing") == 0

    def test_sismember(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a"})
        assert locmem_cache.sismember("k", "a") is True
        assert locmem_cache.sismember("k", "z") is False

    def test_sismember_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.sismember("missing", "a") is False

    def test_smembers_returns_copy(self, locmem_cache: LocMemCache):
        original = {"a", "b", "c"}
        locmem_cache.set("k", original)
        result = locmem_cache.smembers("k")
        assert result == original
        result.add("d")
        assert locmem_cache.smembers("k") == original

    def test_smembers_missing(self, locmem_cache: LocMemCache):
        assert locmem_cache.smembers("missing") == set()

    def test_smismember(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a", "b", "c"})
        assert locmem_cache.smismember("k", "a", "z", "c") == [True, False, True]

    def test_smismember_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.smismember("missing", "a", "b") == [False, False]

    def test_spop_single(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a", "b", "c"})
        member = locmem_cache.spop("k")
        assert member in {"a", "b", "c"}
        assert locmem_cache.scard("k") == 2

    def test_spop_with_count(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a", "b", "c"})
        popped = locmem_cache.spop("k", count=2)
        assert isinstance(popped, set)
        assert len(popped) == 2
        assert popped.issubset({"a", "b", "c"})

    def test_spop_missing_key_single(self, locmem_cache: LocMemCache):
        assert locmem_cache.spop("missing") is None

    def test_spop_missing_key_with_count(self, locmem_cache: LocMemCache):
        assert locmem_cache.spop("missing", count=2) == set()

    def test_spop_deletes_when_empty(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a"})
        locmem_cache.spop("k")
        assert locmem_cache.get("k") is None

    def test_srandmember_single(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a", "b", "c"})
        assert locmem_cache.srandmember("k") in {"a", "b", "c"}
        assert locmem_cache.scard("k") == 3

    def test_srandmember_with_count(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a", "b", "c"})
        members = locmem_cache.srandmember("k", count=2)
        assert isinstance(members, list)
        assert len(members) == 2
        assert all(m in {"a", "b", "c"} for m in members)
        assert locmem_cache.scard("k") == 3

    def test_srandmember_missing_key_single(self, locmem_cache: LocMemCache):
        assert locmem_cache.srandmember("missing") is None

    def test_srandmember_missing_key_with_count(self, locmem_cache: LocMemCache):
        assert locmem_cache.srandmember("missing", count=2) == []

    def test_sdiff(self, locmem_cache: LocMemCache):
        locmem_cache.set("a", {"x", "y", "z"})
        locmem_cache.set("b", {"y"})
        assert locmem_cache.sdiff(["a", "b"]) == {"x", "z"}

    def test_sdiff_single_string_key(self, locmem_cache: LocMemCache):
        locmem_cache.set("a", {"x", "y"})
        assert locmem_cache.sdiff("a") == {"x", "y"}

    def test_sdiff_missing_keys_yield_empty(self, locmem_cache: LocMemCache):
        assert locmem_cache.sdiff(["missing1", "missing2"]) == set()

    def test_sinter(self, locmem_cache: LocMemCache):
        locmem_cache.set("a", {"x", "y", "z"})
        locmem_cache.set("b", {"y", "z", "w"})
        assert locmem_cache.sinter(["a", "b"]) == {"y", "z"}

    def test_sinter_with_missing_yields_empty(self, locmem_cache: LocMemCache):
        locmem_cache.set("a", {"x"})
        assert locmem_cache.sinter(["a", "missing"]) == set()

    def test_sunion(self, locmem_cache: LocMemCache):
        locmem_cache.set("a", {"x", "y"})
        locmem_cache.set("b", {"y", "z"})
        assert locmem_cache.sunion(["a", "b"]) == {"x", "y", "z"}

    def test_sunion_empty_input(self, locmem_cache: LocMemCache):
        assert locmem_cache.sunion(["missing1", "missing2"]) == set()

    def test_set_ops_preserve_ttl(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a"}, timeout=3600)
        locmem_cache.sadd("k", "b")
        assert 3590 <= locmem_cache.ttl("k") <= 3600

    def test_set_ops_no_expiry_stays(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a"}, timeout=None)
        locmem_cache.sadd("k", "b")
        assert locmem_cache.ttl("k") == -1


# =============================================================================
# Hashes
# =============================================================================


class TestHashOps:
    def test_hset_creates_new_hash(self, locmem_cache: LocMemCache):
        assert locmem_cache.hset("k", "name", "alice") == 1
        assert locmem_cache.get("k") == {"name": "alice"}

    def test_hset_adds_field(self, locmem_cache: LocMemCache):
        locmem_cache.hset("k", "name", "alice")
        assert locmem_cache.hset("k", "age", "30") == 1
        assert locmem_cache.get("k") == {"name": "alice", "age": "30"}

    def test_hset_overwrites_returns_zero(self, locmem_cache: LocMemCache):
        locmem_cache.hset("k", "name", "alice")
        assert locmem_cache.hset("k", "name", "bob") == 0
        assert locmem_cache.get("k") == {"name": "bob"}

    def test_hset_type_error_on_non_hash(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", "string")
        with pytest.raises(TypeError):
            locmem_cache.hset("k", "f", "v")

    def test_hset_mapping_creates_hash(self, locmem_cache: LocMemCache):
        assert locmem_cache.hset("k", mapping={"a": 1, "b": 2}) == 2
        assert locmem_cache.get("k") == {"a": 1, "b": 2}

    def test_hset_mapping_merges_fields(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1})
        locmem_cache.hset("k", mapping={"b": 2, "c": 3})
        assert locmem_cache.get("k") == {"a": 1, "b": 2, "c": 3}

    def test_hset_items_creates_hash(self, locmem_cache: LocMemCache):
        # items is a flat list of [field, value, field, value, ...]
        assert locmem_cache.hset("k", items=["a", 1, "b", 2]) == 2
        assert locmem_cache.get("k") == {"a": 1, "b": 2}

    def test_hset_items_odd_length_raises(self, locmem_cache: LocMemCache):
        with pytest.raises(ValueError, match="even number"):
            locmem_cache.hset("k", items=["a", 1, "b"])

    def test_hdel_removes_field(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1, "b": 2, "c": 3})
        assert locmem_cache.hdel("k", "b") == 1
        assert locmem_cache.get("k") == {"a": 1, "c": 3}

    def test_hdel_multiple_fields(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1, "b": 2, "c": 3})
        assert locmem_cache.hdel("k", "a", "c") == 2
        assert locmem_cache.get("k") == {"b": 2}

    def test_hdel_nonexistent_field(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1})
        assert locmem_cache.hdel("k", "z") == 0

    def test_hdel_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.hdel("missing", "f") == 0

    def test_hdel_deletes_when_empty(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1})
        locmem_cache.hdel("k", "a")
        assert locmem_cache.get("k") is None

    def test_hget(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"name": "alice"})
        assert locmem_cache.hget("k", "name") == "alice"

    def test_hget_missing_field(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"name": "alice"})
        assert locmem_cache.hget("k", "age") is None

    def test_hget_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.hget("missing", "f") is None

    def test_hgetall_returns_copy(self, locmem_cache: LocMemCache):
        original = {"a": 1, "b": 2}
        locmem_cache.set("k", original)
        result = locmem_cache.hgetall("k")
        assert result == original
        result["c"] = 3
        assert locmem_cache.hgetall("k") == original

    def test_hgetall_missing(self, locmem_cache: LocMemCache):
        assert locmem_cache.hgetall("missing") == {}

    def test_hlen(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1, "b": 2, "c": 3})
        assert locmem_cache.hlen("k") == 3

    def test_hlen_missing(self, locmem_cache: LocMemCache):
        assert locmem_cache.hlen("missing") == 0

    def test_hkeys(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"x": 1, "y": 2})
        assert sorted(locmem_cache.hkeys("k")) == ["x", "y"]

    def test_hkeys_missing(self, locmem_cache: LocMemCache):
        assert locmem_cache.hkeys("missing") == []

    def test_hvals(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"x": 10, "y": 20})
        assert sorted(locmem_cache.hvals("k")) == [10, 20]

    def test_hvals_missing(self, locmem_cache: LocMemCache):
        assert locmem_cache.hvals("missing") == []

    def test_hexists(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"name": "alice"})
        assert locmem_cache.hexists("k", "name") is True
        assert locmem_cache.hexists("k", "age") is False

    def test_hexists_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.hexists("missing", "f") is False

    def test_hmget(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1, "b": 2, "c": 3})
        assert locmem_cache.hmget("k", "a", "c") == [1, 3]

    def test_hmget_missing_fields(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1})
        assert locmem_cache.hmget("k", "a", "z") == [1, None]

    def test_hmget_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.hmget("missing", "a", "b") == [None, None]

    def test_hsetnx_sets_new_field(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1})
        assert locmem_cache.hsetnx("k", "b", 2) is True
        assert locmem_cache.get("k") == {"a": 1, "b": 2}

    def test_hsetnx_skips_existing_field(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1})
        assert locmem_cache.hsetnx("k", "a", 99) is False
        assert locmem_cache.get("k") == {"a": 1}

    def test_hsetnx_creates_hash(self, locmem_cache: LocMemCache):
        assert locmem_cache.hsetnx("k", "f", "v") is True
        assert locmem_cache.get("k") == {"f": "v"}

    def test_hincrby_new_field(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {})
        assert locmem_cache.hincrby("k", "count", 5) == 5

    def test_hincrby_existing_field(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"count": 10})
        assert locmem_cache.hincrby("k", "count", 3) == 13

    def test_hincrby_creates_hash(self, locmem_cache: LocMemCache):
        assert locmem_cache.hincrby("k", "x") == 1
        assert locmem_cache.get("k") == {"x": 1}

    def test_hincrbyfloat_new_field(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {})
        assert locmem_cache.hincrbyfloat("k", "score", 1.5) == pytest.approx(1.5)

    def test_hincrbyfloat_existing_field(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"score": 2.5})
        assert locmem_cache.hincrbyfloat("k", "score", 0.5) == pytest.approx(3.0)

    def test_hash_ops_preserve_ttl(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1}, timeout=3600)
        locmem_cache.hset("k", "b", 2)
        assert 3590 <= locmem_cache.ttl("k") <= 3600

    def test_hash_ops_no_expiry_stays(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", {"a": 1}, timeout=None)
        locmem_cache.hset("k", "b", 2)
        assert locmem_cache.ttl("k") == -1


# =============================================================================
# Sorted sets
# =============================================================================


class TestSortedSetOps:
    def test_zadd_basic(self, locmem_cache: LocMemCache):
        assert locmem_cache.zadd("k", {"a": 1.0, "b": 2.0}) == 2
        assert locmem_cache.zcard("k") == 2

    def test_zadd_existing_member_no_count_change(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        assert locmem_cache.zadd("k", {"a": 5.0}) == 0
        assert locmem_cache.zscore("k", "a") == 5.0

    def test_zadd_with_ch(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        # ``ch=True`` returns the count of changed members (added OR score changed)
        assert locmem_cache.zadd("k", {"a": 2.0, "b": 3.0}, ch=True) == 2

    def test_zadd_with_nx(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        assert locmem_cache.zadd("k", {"a": 99.0, "b": 2.0}, nx=True) == 1
        assert locmem_cache.zscore("k", "a") == 1.0
        assert locmem_cache.zscore("k", "b") == 2.0

    def test_zadd_with_xx(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        assert locmem_cache.zadd("k", {"a": 5.0, "b": 2.0}, xx=True) == 0
        assert locmem_cache.zscore("k", "a") == 5.0
        assert locmem_cache.zscore("k", "b") is None

    def test_zadd_with_gt(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 5.0})
        locmem_cache.zadd("k", {"a": 3.0}, gt=True)
        assert locmem_cache.zscore("k", "a") == 5.0
        locmem_cache.zadd("k", {"a": 10.0}, gt=True)
        assert locmem_cache.zscore("k", "a") == 10.0

    def test_zadd_with_lt(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 5.0})
        locmem_cache.zadd("k", {"a": 10.0}, lt=True)
        assert locmem_cache.zscore("k", "a") == 5.0
        locmem_cache.zadd("k", {"a": 1.0}, lt=True)
        assert locmem_cache.zscore("k", "a") == 1.0

    def test_zcard_missing(self, locmem_cache: LocMemCache):
        assert locmem_cache.zcard("missing") == 0

    def test_zscore_missing_member(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        assert locmem_cache.zscore("k", "z") is None

    def test_zscore_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zscore("missing", "a") is None

    def test_zrank(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zrank("k", "a") == 0
        assert locmem_cache.zrank("k", "c") == 2

    def test_zrank_missing_member(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        assert locmem_cache.zrank("k", "z") is None

    def test_zrank_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zrank("missing", "a") is None

    def test_zrevrank(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zrevrank("k", "c") == 0
        assert locmem_cache.zrevrank("k", "a") == 2

    def test_zrevrank_missing(self, locmem_cache: LocMemCache):
        assert locmem_cache.zrevrank("missing", "a") is None

    def test_zrange(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zrange("k", 0, -1) == ["a", "b", "c"]
        assert locmem_cache.zrange("k", 0, 1) == ["a", "b"]

    def test_zrange_negative_indices(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zrange("k", -2, -1) == ["b", "c"]

    def test_zrange_withscores(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
        assert locmem_cache.zrange("k", 0, -1, withscores=True) == [("a", 1.0), ("b", 2.0)]

    def test_zrange_out_of_bounds(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        assert locmem_cache.zrange("k", 5, 10) == []

    def test_zrange_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zrange("missing", 0, -1) == []

    def test_zrevrange(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zrevrange("k", 0, -1) == ["c", "b", "a"]

    def test_zrevrange_withscores(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
        assert locmem_cache.zrevrange("k", 0, -1, withscores=True) == [("b", 2.0), ("a", 1.0)]

    def test_zrevrange_negative(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zrevrange("k", -2, -1) == ["b", "a"]

    def test_zrevrange_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zrevrange("missing", 0, -1) == []

    def test_zrangebyscore(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        assert locmem_cache.zrangebyscore("k", 2.0, 3.0) == ["b", "c"]

    def test_zrangebyscore_inf(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
        assert locmem_cache.zrangebyscore("k", "-inf", "+inf") == ["a", "b"]

    def test_zrangebyscore_withscores(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
        assert locmem_cache.zrangebyscore("k", 1.0, 2.0, withscores=True) == [("a", 1.0), ("b", 2.0)]

    def test_zrangebyscore_pagination(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        assert locmem_cache.zrangebyscore("k", "-inf", "+inf", start=1, num=2) == ["b", "c"]

    def test_zrangebyscore_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zrangebyscore("missing", 0.0, 100.0) == []

    def test_zrem(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zrem("k", "b") == 1
        assert locmem_cache.zcard("k") == 2

    def test_zrem_multiple(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zrem("k", "a", "c") == 2

    def test_zrem_nonexistent_member(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        assert locmem_cache.zrem("k", "z") == 0

    def test_zrem_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zrem("missing", "a") == 0

    def test_zrem_deletes_when_empty(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        locmem_cache.zrem("k", "a")
        assert locmem_cache.get("k") is None

    def test_zincrby_existing_member(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        assert locmem_cache.zincrby("k", 2.5, "a") == pytest.approx(3.5)

    def test_zincrby_creates_member(self, locmem_cache: LocMemCache):
        assert locmem_cache.zincrby("k", 5.0, "new") == pytest.approx(5.0)

    def test_zcount(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        assert locmem_cache.zcount("k", 2.0, 3.0) == 2

    def test_zcount_inf(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
        assert locmem_cache.zcount("k", "-inf", "+inf") == 2

    def test_zcount_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zcount("missing", 0.0, 10.0) == 0

    def test_zpopmin(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zpopmin("k") == [("a", 1.0)]
        assert locmem_cache.zcard("k") == 2

    def test_zpopmin_with_count(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zpopmin("k", count=2) == [("a", 1.0), ("b", 2.0)]

    def test_zpopmin_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zpopmin("missing") == []

    def test_zpopmin_deletes_when_empty(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        locmem_cache.zpopmin("k")
        assert locmem_cache.get("k") is None

    def test_zpopmax(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zpopmax("k") == [("c", 3.0)]
        assert locmem_cache.zcard("k") == 2

    def test_zpopmax_with_count(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zpopmax("k", count=2) == [("c", 3.0), ("b", 2.0)]

    def test_zpopmax_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zpopmax("missing") == []

    def test_zpopmax_deletes_when_empty(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        locmem_cache.zpopmax("k")
        assert locmem_cache.get("k") is None

    def test_zmscore(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
        assert locmem_cache.zmscore("k", "a", "b", "missing") == [1.0, 2.0, None]

    def test_zmscore_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zmscore("missing", "a", "b") == [None, None]

    def test_zremrangebyscore(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        assert locmem_cache.zremrangebyscore("k", 2.0, 3.0) == 2
        assert locmem_cache.zrange("k", 0, -1) == ["a", "d"]

    def test_zremrangebyscore_inf(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
        assert locmem_cache.zremrangebyscore("k", "-inf", "+inf") == 2
        assert locmem_cache.get("k") is None

    def test_zremrangebyscore_no_match(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        assert locmem_cache.zremrangebyscore("k", 5.0, 10.0) == 0

    def test_zremrangebyscore_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zremrangebyscore("missing", 0.0, 10.0) == 0

    def test_zremrangebyrank(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        assert locmem_cache.zremrangebyrank("k", 0, 1) == 2
        assert locmem_cache.zrange("k", 0, -1) == ["c", "d"]

    def test_zremrangebyrank_negative(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert locmem_cache.zremrangebyrank("k", -2, -1) == 2
        assert locmem_cache.zrange("k", 0, -1) == ["a"]

    def test_zremrangebyrank_out_of_bounds(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0})
        assert locmem_cache.zremrangebyrank("k", 5, 10) == 0

    def test_zremrangebyrank_missing_key(self, locmem_cache: LocMemCache):
        assert locmem_cache.zremrangebyrank("missing", 0, -1) == 0

    def test_zremrangebyrank_deletes_when_empty(self, locmem_cache: LocMemCache):
        locmem_cache.zadd("k", {"a": 1.0, "b": 2.0})
        locmem_cache.zremrangebyrank("k", 0, -1)
        assert locmem_cache.get("k") is None


# =============================================================================
# Version handling
# =============================================================================


class TestVersion:
    def test_versioned_keys_independent(self, locmem_cache: LocMemCache):
        locmem_cache.set("k", "v1", version=1)
        locmem_cache.set("k", "v2", version=2)
        assert locmem_cache.get("k", version=1) == "v1"
        assert locmem_cache.get("k", version=2) == "v2"

    def test_versioned_extension_ops(self, locmem_cache: LocMemCache):
        locmem_cache.lpush("k", "a", version=1)
        locmem_cache.lpush("k", "b", version=2)
        assert locmem_cache.lrange("k", 0, -1, version=1) == ["a"]
        assert locmem_cache.lrange("k", 0, -1, version=2) == ["b"]
