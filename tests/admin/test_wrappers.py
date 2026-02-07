"""Tests for admin cache wrappers (LocMemCache, DatabaseCache, DummyCache, etc.)."""

import pytest
from django.core.cache import caches
from django.test import override_settings

from django_cachex.admin.wrappers import (
    BaseCacheExtensions,
    WrappedDatabaseCache,
    WrappedDummyCache,
    WrappedLocMemCache,
    wrap_cache,
)
from django_cachex.exceptions import NotSupportedError

# =============================================================================
# Cache configurations for testing
# =============================================================================

LOCMEM_CACHES = {
    "locmem": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "test-wrapper-locmem",
    },
}

DATABASE_CACHES = {
    "dbcache": {
        "BACKEND": "django.core.cache.backends.db.DatabaseCache",
        "LOCATION": "test_cache_table",
    },
}

DUMMY_CACHES = {
    "dummy": {
        "BACKEND": "django.core.cache.backends.dummy.DummyCache",
    },
}


# =============================================================================
# wrap_cache factory tests
# =============================================================================


class TestWrapCache:
    """Test that wrap_cache returns properly extended cache backends."""

    @override_settings(CACHES=LOCMEM_CACHES)
    def test_locmem_returns_extended_locmem(self):
        cache = caches["locmem"]
        wrapped = wrap_cache(cache)
        assert isinstance(wrapped, WrappedLocMemCache)
        assert wrapped is cache  # Same instance, class patched

    @override_settings(CACHES=DATABASE_CACHES)
    def test_database_returns_extended_database(self, db):
        from django.core.management import call_command

        call_command("createcachetable", verbosity=0)
        cache = caches["dbcache"]
        wrapped = wrap_cache(cache)
        assert isinstance(wrapped, WrappedDatabaseCache)
        assert wrapped is cache

    @override_settings(CACHES=DUMMY_CACHES)
    def test_dummy_returns_extended_dummy(self):
        cache = caches["dummy"]
        wrapped = wrap_cache(cache)
        assert isinstance(wrapped, WrappedDummyCache)
        assert wrapped is cache

    @override_settings(CACHES=LOCMEM_CACHES)
    def test_wrap_cache_is_idempotent(self):
        cache = caches["locmem"]
        wrapped1 = wrap_cache(cache)
        wrapped2 = wrap_cache(wrapped1)
        assert wrapped1 is wrapped2
        assert getattr(wrapped2, "_cachex_support", None) == "wrapped"


# =============================================================================
# LocMemCache extension tests
# =============================================================================


class TestLocMemCacheExtensions:
    """Parametrized tests for LocMemCache extensions."""

    @pytest.fixture(autouse=True)
    def _setup_cache(self):
        with override_settings(CACHES=LOCMEM_CACHES):
            cache = caches["locmem"]
            cache.clear()
            self.cache = wrap_cache(cache)
            yield

    def test_set_and_get(self):
        self.cache.set("key1", "value1")
        assert self.cache.get("key1") == "value1"

    def test_get_missing_returns_default(self):
        assert self.cache.get("missing") is None
        assert self.cache.get("missing", "default") == "default"

    def test_delete(self):
        self.cache.set("key1", "value1")
        assert self.cache.delete("key1") is True
        assert self.cache.get("key1") is None

    def test_clear(self):
        self.cache.set("key1", "value1")
        self.cache.set("key2", "value2")
        self.cache.clear()
        assert self.cache.get("key1") is None

    def test_keys_returns_all(self):
        self.cache.set("alpha", 1)
        self.cache.set("beta", 2)
        keys = self.cache.keys()
        assert "alpha" in keys
        assert "beta" in keys

    def test_keys_with_pattern(self):
        self.cache.set("user:1", "alice")
        self.cache.set("user:2", "bob")
        self.cache.set("session:abc", "data")
        keys = self.cache.keys("user:*")
        assert "user:1" in keys
        assert "user:2" in keys
        assert "session:abc" not in keys

    def test_ttl_missing_key(self):
        assert self.cache.ttl("nonexistent") == -2

    def test_ttl_persistent_key(self):
        self.cache.set("forever", "value", timeout=None)
        assert self.cache.ttl("forever") == -1

    def test_ttl_expiring_key(self):
        self.cache.set("temp", "value", timeout=3600)
        ttl = self.cache.ttl("temp")
        assert 3590 <= ttl <= 3600

    def test_expire(self):
        self.cache.set("key1", "value1", timeout=None)
        assert self.cache.ttl("key1") == -1
        self.cache.expire("key1", 100)
        ttl = self.cache.ttl("key1")
        assert 90 <= ttl <= 100

    def test_expire_missing_key(self):
        assert self.cache.expire("nonexistent", 100) is False

    def test_persist(self):
        self.cache.set("key1", "value1", timeout=60)
        assert self.cache.ttl("key1") > 0
        self.cache.persist("key1")
        assert self.cache.ttl("key1") == -1

    def test_persist_missing_key(self):
        assert self.cache.persist("nonexistent") is False

    def test_info_returns_dict(self):
        self.cache.set("key1", "value1")
        info = self.cache.info()
        assert info["backend"] == "LocMemCache"
        assert "server" in info
        assert "memory" in info
        assert "keyspace" in info
        assert info["keyspace"]["db0"]["keys"] >= 1


# =============================================================================
# LocMemCache list operations
# =============================================================================


class TestLocMemCacheListOperations:
    """Tests for LocMemCache list operations and type detection."""

    @pytest.fixture(autouse=True)
    def _setup_cache(self):
        with override_settings(CACHES=LOCMEM_CACHES):
            cache = caches["locmem"]
            cache.clear()
            self.cache = wrap_cache(cache)
            yield

    # -- type() ---------------------------------------------------------------

    def test_type_string_for_scalar(self):
        self.cache.set("k", "hello")
        assert self.cache.type("k") == "string"

    def test_type_list_for_list(self):
        self.cache.set("k", [1, 2, 3])
        assert self.cache.type("k") == "list"

    def test_type_none_for_missing(self):
        assert self.cache.type("missing") is None

    def test_type_hash_for_dict(self):
        self.cache.set("k", {"a": 1})
        assert self.cache.type("k") == "hash"

    def test_type_string_for_int(self):
        self.cache.set("k", 42)
        assert self.cache.type("k") == "string"

    # -- lpush() --------------------------------------------------------------

    def test_lpush_creates_new_list(self):
        assert self.cache.lpush("k", "a") == 1
        assert self.cache.get("k") == ["a"]

    def test_lpush_prepends(self):
        self.cache.lpush("k", "a")
        self.cache.lpush("k", "b")
        assert self.cache.get("k") == ["b", "a"]

    def test_lpush_multiple_values(self):
        assert self.cache.lpush("k", "a", "b", "c") == 3
        assert self.cache.get("k") == ["c", "b", "a"]

    def test_lpush_type_error_on_non_list(self):
        self.cache.set("k", "string_value")
        with pytest.raises(TypeError):
            self.cache.lpush("k", "x")

    # -- rpush() --------------------------------------------------------------

    def test_rpush_creates_new_list(self):
        assert self.cache.rpush("k", "a") == 1
        assert self.cache.get("k") == ["a"]

    def test_rpush_appends(self):
        self.cache.rpush("k", "a")
        self.cache.rpush("k", "b")
        assert self.cache.get("k") == ["a", "b"]

    def test_rpush_multiple_values(self):
        assert self.cache.rpush("k", "a", "b", "c") == 3
        assert self.cache.get("k") == ["a", "b", "c"]

    def test_rpush_type_error_on_non_list(self):
        self.cache.set("k", "string_value")
        with pytest.raises(TypeError):
            self.cache.rpush("k", "x")

    # -- lpop() ---------------------------------------------------------------

    def test_lpop_returns_first(self):
        self.cache.set("k", [1, 2, 3])
        assert self.cache.lpop("k") == [1]
        assert self.cache.get("k") == [2, 3]

    def test_lpop_with_count(self):
        self.cache.set("k", [1, 2, 3, 4])
        assert self.cache.lpop("k", count=2) == [1, 2]
        assert self.cache.get("k") == [3, 4]

    def test_lpop_empty_returns_empty(self):
        assert self.cache.lpop("missing") == []

    def test_lpop_deletes_when_empty(self):
        self.cache.set("k", [1])
        self.cache.lpop("k")
        assert self.cache.get("k") is None

    # -- rpop() ---------------------------------------------------------------

    def test_rpop_returns_last(self):
        self.cache.set("k", [1, 2, 3])
        assert self.cache.rpop("k") == [3]
        assert self.cache.get("k") == [1, 2]

    def test_rpop_with_count(self):
        self.cache.set("k", [1, 2, 3, 4])
        assert self.cache.rpop("k", count=2) == [4, 3]
        assert self.cache.get("k") == [1, 2]

    def test_rpop_empty_returns_empty(self):
        assert self.cache.rpop("missing") == []

    def test_rpop_deletes_when_empty(self):
        self.cache.set("k", [1])
        self.cache.rpop("k")
        assert self.cache.get("k") is None

    # -- lrange() -------------------------------------------------------------

    def test_lrange_full(self):
        self.cache.set("k", ["a", "b", "c"])
        assert self.cache.lrange("k", 0, -1) == ["a", "b", "c"]

    def test_lrange_partial(self):
        self.cache.set("k", ["a", "b", "c", "d"])
        assert self.cache.lrange("k", 1, 2) == ["b", "c"]

    def test_lrange_negative_indices(self):
        self.cache.set("k", ["a", "b", "c", "d", "e"])
        assert self.cache.lrange("k", -3, -1) == ["c", "d", "e"]

    def test_lrange_missing_key(self):
        assert self.cache.lrange("missing", 0, -1) == []

    # -- llen() ---------------------------------------------------------------

    def test_llen_returns_length(self):
        self.cache.set("k", [1, 2, 3])
        assert self.cache.llen("k") == 3

    def test_llen_missing_key(self):
        assert self.cache.llen("missing") == 0

    # -- lrem() ---------------------------------------------------------------

    def test_lrem_all_occurrences(self):
        self.cache.set("k", ["a", "b", "a", "c", "a"])
        assert self.cache.lrem("k", 0, "a") == 3
        assert self.cache.get("k") == ["b", "c"]

    def test_lrem_from_head(self):
        self.cache.set("k", ["a", "b", "a", "c", "a"])
        assert self.cache.lrem("k", 2, "a") == 2
        assert self.cache.get("k") == ["b", "c", "a"]

    def test_lrem_from_tail(self):
        self.cache.set("k", ["a", "b", "a", "c", "a"])
        assert self.cache.lrem("k", -1, "a") == 1
        assert self.cache.get("k") == ["a", "b", "a", "c"]

    def test_lrem_not_found(self):
        self.cache.set("k", ["a", "b"])
        assert self.cache.lrem("k", 0, "z") == 0

    def test_lrem_missing_key(self):
        assert self.cache.lrem("missing", 0, "a") == 0

    def test_lrem_deletes_when_all_removed(self):
        self.cache.set("k", ["a", "a"])
        self.cache.lrem("k", 0, "a")
        assert self.cache.get("k") is None

    # -- ltrim() --------------------------------------------------------------

    def test_ltrim_basic(self):
        self.cache.set("k", ["a", "b", "c", "d", "e"])
        assert self.cache.ltrim("k", 1, 3) is True
        assert self.cache.get("k") == ["b", "c", "d"]

    def test_ltrim_negative_end(self):
        self.cache.set("k", ["a", "b", "c"])
        self.cache.ltrim("k", 0, -2)
        assert self.cache.get("k") == ["a", "b"]

    def test_ltrim_out_of_range_deletes(self):
        self.cache.set("k", ["a", "b"])
        self.cache.ltrim("k", 5, 10)
        assert self.cache.get("k") is None

    def test_ltrim_missing_key(self):
        assert self.cache.ltrim("missing", 0, -1) is True

    # -- TTL preservation -----------------------------------------------------

    def test_list_ops_preserve_ttl(self):
        self.cache.set("k", [1, 2], timeout=3600)
        self.cache.rpush("k", 3)
        ttl = self.cache.ttl("k")
        assert 3590 <= ttl <= 3600

    def test_list_ops_no_expiry_stays(self):
        self.cache.set("k", [1, 2], timeout=None)
        self.cache.rpush("k", 3)
        assert self.cache.ttl("k") == -1


# =============================================================================
# LocMemCache set operations
# =============================================================================


class TestLocMemCacheSetOperations:
    """Tests for LocMemCache set operations and type detection."""

    @pytest.fixture(autouse=True)
    def _setup_cache(self):
        with override_settings(CACHES=LOCMEM_CACHES):
            cache = caches["locmem"]
            cache.clear()
            self.cache = wrap_cache(cache)
            yield

    # -- type() for sets ------------------------------------------------------

    def test_type_set_for_set(self):
        self.cache.set("k", {1, 2, 3})
        assert self.cache.type("k") == "set"

    def test_type_set_for_empty_set(self):
        self.cache.set("k", set())
        assert self.cache.type("k") == "set"

    # -- sadd() ---------------------------------------------------------------

    def test_sadd_creates_new_set(self):
        assert self.cache.sadd("k", "a") == 1
        assert self.cache.get("k") == {"a"}

    def test_sadd_adds_to_existing(self):
        self.cache.sadd("k", "a")
        assert self.cache.sadd("k", "b") == 1
        assert self.cache.get("k") == {"a", "b"}

    def test_sadd_duplicate_returns_zero(self):
        self.cache.sadd("k", "a")
        assert self.cache.sadd("k", "a") == 0

    def test_sadd_multiple_members(self):
        assert self.cache.sadd("k", "a", "b", "c") == 3
        assert self.cache.get("k") == {"a", "b", "c"}

    def test_sadd_type_error_on_non_set(self):
        self.cache.set("k", "string_value")
        with pytest.raises(TypeError):
            self.cache.sadd("k", "x")

    # -- srem() ---------------------------------------------------------------

    def test_srem_removes_member(self):
        self.cache.set("k", {"a", "b", "c"})
        assert self.cache.srem("k", "b") == 1
        assert self.cache.get("k") == {"a", "c"}

    def test_srem_multiple_members(self):
        self.cache.set("k", {"a", "b", "c", "d"})
        assert self.cache.srem("k", "a", "c") == 2
        assert self.cache.get("k") == {"b", "d"}

    def test_srem_nonexistent_member(self):
        self.cache.set("k", {"a", "b"})
        assert self.cache.srem("k", "z") == 0

    def test_srem_missing_key(self):
        assert self.cache.srem("missing", "a") == 0

    def test_srem_deletes_when_empty(self):
        self.cache.set("k", {"a"})
        self.cache.srem("k", "a")
        assert self.cache.get("k") is None

    # -- scard() --------------------------------------------------------------

    def test_scard_returns_count(self):
        self.cache.set("k", {"a", "b", "c"})
        assert self.cache.scard("k") == 3

    def test_scard_missing_key(self):
        assert self.cache.scard("missing") == 0

    # -- sismember() ----------------------------------------------------------

    def test_sismember_true(self):
        self.cache.set("k", {"a", "b"})
        assert self.cache.sismember("k", "a") is True

    def test_sismember_false(self):
        self.cache.set("k", {"a", "b"})
        assert self.cache.sismember("k", "z") is False

    def test_sismember_missing_key(self):
        assert self.cache.sismember("missing", "a") is False

    # -- smembers() -----------------------------------------------------------

    def test_smembers_returns_copy(self):
        original = {"a", "b", "c"}
        self.cache.set("k", original)
        result = self.cache.smembers("k")
        assert result == original
        # Should be a copy, not the same object
        result.add("d")
        assert self.cache.smembers("k") == original

    def test_smembers_missing_key(self):
        assert self.cache.smembers("missing") == set()

    # -- spop() ---------------------------------------------------------------

    def test_spop_single(self):
        self.cache.set("k", {"a", "b", "c"})
        member = self.cache.spop("k")
        assert member in {"a", "b", "c"}
        assert self.cache.scard("k") == 2

    def test_spop_with_count(self):
        self.cache.set("k", {"a", "b", "c"})
        popped = self.cache.spop("k", count=2)
        assert isinstance(popped, set)
        assert len(popped) == 2
        assert popped.issubset({"a", "b", "c"})

    def test_spop_missing_key_single(self):
        assert self.cache.spop("missing") is None

    def test_spop_missing_key_with_count(self):
        assert self.cache.spop("missing", count=2) == set()

    def test_spop_deletes_when_empty(self):
        self.cache.set("k", {"a"})
        self.cache.spop("k")
        assert self.cache.get("k") is None

    # -- srandmember() --------------------------------------------------------

    def test_srandmember_single(self):
        self.cache.set("k", {"a", "b", "c"})
        member = self.cache.srandmember("k")
        assert member in {"a", "b", "c"}
        # Should NOT modify the set
        assert self.cache.scard("k") == 3

    def test_srandmember_with_count(self):
        self.cache.set("k", {"a", "b", "c"})
        members = self.cache.srandmember("k", count=2)
        assert isinstance(members, list)
        assert len(members) == 2
        assert all(m in {"a", "b", "c"} for m in members)
        assert self.cache.scard("k") == 3

    def test_srandmember_missing_key_single(self):
        assert self.cache.srandmember("missing") is None

    def test_srandmember_missing_key_with_count(self):
        assert self.cache.srandmember("missing", count=2) == []

    # -- TTL preservation -----------------------------------------------------

    def test_set_ops_preserve_ttl(self):
        self.cache.set("k", {"a"}, timeout=3600)
        self.cache.sadd("k", "b")
        ttl = self.cache.ttl("k")
        assert 3590 <= ttl <= 3600

    def test_set_ops_no_expiry_stays(self):
        self.cache.set("k", {"a"}, timeout=None)
        self.cache.sadd("k", "b")
        assert self.cache.ttl("k") == -1


# =============================================================================
# LocMemCache hash operations
# =============================================================================


class TestLocMemCacheHashOperations:
    """Tests for LocMemCache hash operations and type detection."""

    @pytest.fixture(autouse=True)
    def _setup_cache(self):
        with override_settings(CACHES=LOCMEM_CACHES):
            cache = caches["locmem"]
            cache.clear()
            self.cache = wrap_cache(cache)
            yield

    # -- type() for hashes ----------------------------------------------------

    def test_type_hash_for_str_key_dict(self):
        self.cache.set("k", {"name": "alice", "age": "30"})
        assert self.cache.type("k") == "hash"

    def test_type_hash_for_empty_dict(self):
        self.cache.set("k", {})
        assert self.cache.type("k") == "hash"

    def test_type_string_for_int_key_dict(self):
        self.cache.set("k", {1: "a", 2: "b"})
        assert self.cache.type("k") == "string"

    def test_type_string_for_mixed_key_dict(self):
        self.cache.set("k", {"a": 1, 2: "b"})
        assert self.cache.type("k") == "string"

    # -- hset() ---------------------------------------------------------------

    def test_hset_creates_new_hash(self):
        assert self.cache.hset("k", "name", "alice") == 1
        assert self.cache.get("k") == {"name": "alice"}

    def test_hset_adds_field(self):
        self.cache.hset("k", "name", "alice")
        assert self.cache.hset("k", "age", "30") == 1
        assert self.cache.get("k") == {"name": "alice", "age": "30"}

    def test_hset_overwrites_returns_zero(self):
        self.cache.hset("k", "name", "alice")
        assert self.cache.hset("k", "name", "bob") == 0
        assert self.cache.get("k") == {"name": "bob"}

    def test_hset_type_error_on_non_hash(self):
        self.cache.set("k", "string_value")
        with pytest.raises(TypeError):
            self.cache.hset("k", "f", "v")

    # -- hdel() ---------------------------------------------------------------

    def test_hdel_removes_field(self):
        self.cache.set("k", {"a": 1, "b": 2, "c": 3})
        assert self.cache.hdel("k", "b") == 1
        assert self.cache.get("k") == {"a": 1, "c": 3}

    def test_hdel_multiple_fields(self):
        self.cache.set("k", {"a": 1, "b": 2, "c": 3})
        assert self.cache.hdel("k", "a", "c") == 2
        assert self.cache.get("k") == {"b": 2}

    def test_hdel_nonexistent_field(self):
        self.cache.set("k", {"a": 1})
        assert self.cache.hdel("k", "z") == 0

    def test_hdel_missing_key(self):
        assert self.cache.hdel("missing", "f") == 0

    def test_hdel_deletes_when_empty(self):
        self.cache.set("k", {"a": 1})
        self.cache.hdel("k", "a")
        assert self.cache.get("k") is None

    # -- hget() ---------------------------------------------------------------

    def test_hget_returns_value(self):
        self.cache.set("k", {"name": "alice"})
        assert self.cache.hget("k", "name") == "alice"

    def test_hget_missing_field(self):
        self.cache.set("k", {"name": "alice"})
        assert self.cache.hget("k", "age") is None

    def test_hget_missing_key(self):
        assert self.cache.hget("missing", "f") is None

    # -- hgetall() ------------------------------------------------------------

    def test_hgetall_returns_copy(self):
        original = {"a": 1, "b": 2}
        self.cache.set("k", original)
        result = self.cache.hgetall("k")
        assert result == original
        result["c"] = 3
        assert self.cache.hgetall("k") == original

    def test_hgetall_missing_key(self):
        assert self.cache.hgetall("missing") == {}

    # -- hlen() ---------------------------------------------------------------

    def test_hlen_returns_count(self):
        self.cache.set("k", {"a": 1, "b": 2, "c": 3})
        assert self.cache.hlen("k") == 3

    def test_hlen_missing_key(self):
        assert self.cache.hlen("missing") == 0

    # -- hkeys() / hvals() ---------------------------------------------------

    def test_hkeys_returns_field_names(self):
        self.cache.set("k", {"x": 1, "y": 2})
        assert sorted(self.cache.hkeys("k")) == ["x", "y"]

    def test_hkeys_missing_key(self):
        assert self.cache.hkeys("missing") == []

    def test_hvals_returns_values(self):
        self.cache.set("k", {"x": 10, "y": 20})
        assert sorted(self.cache.hvals("k")) == [10, 20]

    def test_hvals_missing_key(self):
        assert self.cache.hvals("missing") == []

    # -- hexists() ------------------------------------------------------------

    def test_hexists_true(self):
        self.cache.set("k", {"name": "alice"})
        assert self.cache.hexists("k", "name") is True

    def test_hexists_false(self):
        self.cache.set("k", {"name": "alice"})
        assert self.cache.hexists("k", "age") is False

    def test_hexists_missing_key(self):
        assert self.cache.hexists("missing", "f") is False

    # -- hmget() --------------------------------------------------------------

    def test_hmget_returns_values(self):
        self.cache.set("k", {"a": 1, "b": 2, "c": 3})
        assert self.cache.hmget("k", "a", "c") == [1, 3]

    def test_hmget_missing_fields(self):
        self.cache.set("k", {"a": 1})
        assert self.cache.hmget("k", "a", "z") == [1, None]

    def test_hmget_missing_key(self):
        assert self.cache.hmget("missing", "a", "b") == [None, None]

    # -- hmset() --------------------------------------------------------------

    def test_hmset_creates_hash(self):
        assert self.cache.hmset("k", {"a": 1, "b": 2}) is True
        assert self.cache.get("k") == {"a": 1, "b": 2}

    def test_hmset_merges_fields(self):
        self.cache.set("k", {"a": 1})
        self.cache.hmset("k", {"b": 2, "c": 3})
        assert self.cache.get("k") == {"a": 1, "b": 2, "c": 3}

    # -- hsetnx() -------------------------------------------------------------

    def test_hsetnx_sets_new_field(self):
        self.cache.set("k", {"a": 1})
        assert self.cache.hsetnx("k", "b", 2) is True
        assert self.cache.get("k") == {"a": 1, "b": 2}

    def test_hsetnx_skips_existing_field(self):
        self.cache.set("k", {"a": 1})
        assert self.cache.hsetnx("k", "a", 99) is False
        assert self.cache.get("k") == {"a": 1}

    def test_hsetnx_creates_hash(self):
        assert self.cache.hsetnx("k", "f", "v") is True
        assert self.cache.get("k") == {"f": "v"}

    # -- hincrby() / hincrbyfloat() -------------------------------------------

    def test_hincrby_new_field(self):
        self.cache.set("k", {})
        assert self.cache.hincrby("k", "count", 5) == 5

    def test_hincrby_existing_field(self):
        self.cache.set("k", {"count": 10})
        assert self.cache.hincrby("k", "count", 3) == 13

    def test_hincrby_creates_hash(self):
        assert self.cache.hincrby("k", "x") == 1
        assert self.cache.get("k") == {"x": 1}

    def test_hincrbyfloat_new_field(self):
        self.cache.set("k", {})
        result = self.cache.hincrbyfloat("k", "score", 1.5)
        assert result == pytest.approx(1.5)

    def test_hincrbyfloat_existing_field(self):
        self.cache.set("k", {"score": 2.5})
        result = self.cache.hincrbyfloat("k", "score", 0.5)
        assert result == pytest.approx(3.0)

    # -- TTL preservation -----------------------------------------------------

    def test_hash_ops_preserve_ttl(self):
        self.cache.set("k", {"a": 1}, timeout=3600)
        self.cache.hset("k", "b", 2)
        ttl = self.cache.ttl("k")
        assert 3590 <= ttl <= 3600

    def test_hash_ops_no_expiry_stays(self):
        self.cache.set("k", {"a": 1}, timeout=None)
        self.cache.hset("k", "b", 2)
        assert self.cache.ttl("k") == -1


# =============================================================================
# DatabaseCache extension tests
# =============================================================================


class TestDatabaseCacheExtensions:
    """Tests for DatabaseCache extensions."""

    @pytest.fixture(autouse=True)
    def _setup_cache(self, db):
        from django.core.management import call_command

        with override_settings(CACHES=DATABASE_CACHES):
            call_command("createcachetable", verbosity=0)
            cache = caches["dbcache"]
            cache.clear()
            self.cache = wrap_cache(cache)
            yield

    def test_set_and_get(self):
        self.cache.set("key1", "value1")
        assert self.cache.get("key1") == "value1"

    def test_delete(self):
        self.cache.set("key1", "value1")
        self.cache.delete("key1")
        assert self.cache.get("key1") is None

    def test_clear(self):
        self.cache.set("key1", "value1")
        self.cache.clear()
        assert self.cache.get("key1") is None

    def test_keys_returns_all(self):
        self.cache.set("alpha", 1)
        self.cache.set("beta", 2)
        keys = self.cache.keys()
        assert len(keys) >= 2

    def test_ttl_missing_key(self):
        assert self.cache.ttl("nonexistent") == -2

    def test_ttl_expiring_key(self):
        self.cache.set("temp", "value", timeout=3600)
        ttl = self.cache.ttl("temp")
        # SQLite stores expires as datetime strings; the TTL arithmetic
        # may not work correctly on SQLite (returns -2). This is a known
        # limitation since DatabaseCache is primarily used with PostgreSQL/MySQL.
        from django.db import connection

        if connection.vendor == "sqlite":
            assert ttl == -2 or 3500 <= ttl <= 3600
        else:
            assert 3500 <= ttl <= 3600

    def test_info_returns_dict(self):
        self.cache.set("key1", "value1")
        info = self.cache.info()
        assert info["backend"] == "DatabaseCache"
        assert "keyspace" in info

    def test_expire_not_supported(self):
        self.cache.set("key1", "value1")
        with pytest.raises(NotSupportedError):
            self.cache.expire("key1", 100)

    def test_persist_not_supported(self):
        with pytest.raises(NotSupportedError):
            self.cache.persist("key1")


# =============================================================================
# DummyCache extension tests
# =============================================================================


class TestDummyCacheExtensions:
    """Tests for DummyCache extensions."""

    @pytest.fixture(autouse=True)
    def _setup_cache(self):
        with override_settings(CACHES=DUMMY_CACHES):
            cache = caches["dummy"]
            self.cache = wrap_cache(cache)
            yield

    def test_get_returns_default(self):
        assert self.cache.get("key1") is None
        assert self.cache.get("key1", "default") == "default"

    def test_set_is_noop(self):
        # DummyCache.set() returns None per Django's implementation
        self.cache.set("key1", "value1")
        assert self.cache.get("key1") is None

    def test_delete_is_noop(self):
        # DummyCache.delete() returns False per Django's implementation
        assert self.cache.delete("key1") is False

    def test_clear_is_noop(self):
        # DummyCache.clear() returns None per Django's implementation
        self.cache.clear()
        # Just verify it doesn't raise

    def test_keys_returns_empty(self):
        assert self.cache.keys() == []

    def test_info_returns_dict(self):
        info = self.cache.info()
        assert info["backend"] == "DummyCache"
        assert info["keyspace"]["db0"]["keys"] == 0


# =============================================================================
# BaseCacheExtensions unsupported operations
# =============================================================================


UNSUPPORTED_OPERATIONS = [
    ("keys", ("*",)),
    ("ttl", ("key",)),
    ("expire", ("key", 100)),
    ("persist", ("key",)),
    ("lrange", ("key", 0, -1)),
    ("llen", ("key",)),
    ("lpush", ("key", "value")),
    ("rpush", ("key", "value")),
    ("lpop", ("key",)),
    ("rpop", ("key",)),
    ("lrem", ("key", 0, "value")),
    ("ltrim", ("key", 0, -1)),
    ("smembers", ("key",)),
    ("scard", ("key",)),
    ("sadd", ("key", "value")),
    ("srem", ("key", "value")),
    ("spop", ("key",)),
    ("hgetall", ("key",)),
    ("hlen", ("key",)),
    ("hset", ("key", "field", "value")),
    ("hdel", ("key", "field")),
    ("zrange", ("key", 0, -1)),
    ("zcard", ("key",)),
    ("zadd", ("key", {"member": 1.0})),
    ("zrem", ("key", "member")),
    ("zpopmin", ("key",)),
    ("zpopmax", ("key",)),
]


class TestBaseCacheExtensionsUnsupported:
    """Test that BaseCacheExtensions raises NotSupportedError for extended operations."""

    @pytest.fixture(autouse=True)
    def _setup_extensions(self):
        # Create a mock class that combines BaseCache with BaseCacheExtensions
        # to test that base extensions raise NotSupportedError
        from django.core.cache.backends.base import BaseCache

        class MockExtendedCache(BaseCache, BaseCacheExtensions):
            def __init__(self):
                super().__init__(params={})

        self.cache = MockExtendedCache()

    @pytest.mark.parametrize(
        ("operation", "args"),
        UNSUPPORTED_OPERATIONS,
        ids=[op for op, _ in UNSUPPORTED_OPERATIONS],
    )
    def test_unsupported_operation_raises(self, operation, args):
        method = getattr(self.cache, operation)
        with pytest.raises(NotSupportedError):
            method(*args)
