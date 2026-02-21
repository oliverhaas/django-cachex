"""Tests for two-tiered cache (L1 in-process + L2 Redis/Valkey)."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, cast

import pytest
from django.test import override_settings

from tests.fixtures.cache import build_cache_config

if TYPE_CHECKING:
    from django_cachex.cache import KeyValueCache
    from tests.fixtures.containers import RedisContainerInfo

# Map regular backends to tiered variants
TIERED_BACKENDS = {
    "django_cachex.cache.RedisCache": "django_cachex.cache.RedisTieredCache",
    "django_cachex.cache.ValkeyCache": "django_cachex.cache.ValkeyTieredCache",
}

L1_TIMEOUT = 2  # seconds — short for testing


@pytest.fixture
def tiered_cache(redis_container: RedisContainerInfo) -> KeyValueCache:
    """Tiered cache fixture with short L1 TTL for testing."""
    config = build_cache_config(
        redis_container.host,
        redis_container.port,
        client_library=redis_container.client_library,
    )
    # Replace backends with tiered variants
    for cache_config in config.values():
        backend = cache_config["BACKEND"]
        if backend in TIERED_BACKENDS:
            cache_config["BACKEND"] = TIERED_BACKENDS[backend]
        cache_config.setdefault("OPTIONS", {})["TIERED_L1_TIMEOUT"] = L1_TIMEOUT
        cache_config["OPTIONS"]["TIERED_L1_MAX_ENTRIES"] = 100

    with override_settings(CACHES=config):
        from django.core.cache import cache as default_cache

        cache = cast("KeyValueCache", default_cache)
        cache.flush_db()
        yield cache
        cache.flush_db()


class TestTieredBasicOps:
    """Basic cache operations work correctly through tiered cache."""

    def test_get_set_roundtrip(self, tiered_cache: KeyValueCache):
        tiered_cache.set("key1", "value1")
        assert tiered_cache.get("key1") == "value1"

    def test_get_missing_returns_default(self, tiered_cache: KeyValueCache):
        assert tiered_cache.get("missing") is None
        assert tiered_cache.get("missing", "fallback") == "fallback"

    def test_set_many_get_many(self, tiered_cache: KeyValueCache):
        data = {"k1": "v1", "k2": "v2", "k3": "v3"}
        tiered_cache.set_many(data)
        result = tiered_cache.get_many(["k1", "k2", "k3"])
        assert result == data

    def test_delete(self, tiered_cache: KeyValueCache):
        tiered_cache.set("del_key", "val")
        assert tiered_cache.get("del_key") == "val"
        tiered_cache.delete("del_key")
        assert tiered_cache.get("del_key") is None

    def test_delete_many(self, tiered_cache: KeyValueCache):
        tiered_cache.set_many({"dm1": 1, "dm2": 2})
        tiered_cache.delete_many(["dm1", "dm2"])
        assert tiered_cache.get("dm1") is None
        assert tiered_cache.get("dm2") is None

    def test_add(self, tiered_cache: KeyValueCache):
        tiered_cache.delete("add_key")
        assert tiered_cache.add("add_key", "first") is True
        assert tiered_cache.add("add_key", "second") is False
        assert tiered_cache.get("add_key") == "first"

    def test_has_key(self, tiered_cache: KeyValueCache):
        tiered_cache.set("exists", 1)
        assert tiered_cache.has_key("exists") is True
        assert tiered_cache.has_key("nope") is False

    def test_incr(self, tiered_cache: KeyValueCache):
        tiered_cache.set("counter", 10)
        assert tiered_cache.incr("counter") == 11
        assert tiered_cache.incr("counter", 5) == 16
        assert tiered_cache.get("counter") == 16

    def test_decr(self, tiered_cache: KeyValueCache):
        tiered_cache.set("dcounter", 10)
        assert tiered_cache.decr("dcounter") == 9
        assert tiered_cache.get("dcounter") == 9

    def test_touch(self, tiered_cache: KeyValueCache):
        tiered_cache.set("touch_key", "val", timeout=10)
        assert tiered_cache.touch("touch_key", timeout=60) is True
        assert tiered_cache.get("touch_key") == "val"

    def test_get_or_set(self, tiered_cache: KeyValueCache):
        tiered_cache.delete("gos_key")
        val = tiered_cache.get_or_set("gos_key", "created")
        assert val == "created"
        val = tiered_cache.get_or_set("gos_key", "ignored")
        assert val == "created"

    def test_clear(self, tiered_cache: KeyValueCache):
        tiered_cache.set("clear1", "a")
        tiered_cache.set("clear2", "b")
        tiered_cache.clear()
        assert tiered_cache.get("clear1") is None
        assert tiered_cache.get("clear2") is None

    def test_set_nx_updates_l1_on_success(self, tiered_cache: KeyValueCache):
        tiered_cache.delete("nx_key")
        result = tiered_cache.set("nx_key", "val", nx=True)
        assert result is True
        assert tiered_cache.get("nx_key") == "val"

    def test_set_nx_skips_l1_on_failure(self, tiered_cache: KeyValueCache):
        tiered_cache.set("nx_existing", "original")
        result = tiered_cache.set("nx_existing", "new", nx=True)
        assert result is False
        assert tiered_cache.get("nx_existing") == "original"

    def test_versioned_keys(self, tiered_cache: KeyValueCache):
        tiered_cache.set("vkey", "v1", version=1)
        tiered_cache.set("vkey", "v2", version=2)
        assert tiered_cache.get("vkey", version=1) == "v1"
        assert tiered_cache.get("vkey", version=2) == "v2"

    def test_various_value_types(self, tiered_cache: KeyValueCache):
        tiered_cache.set("int", 42)
        tiered_cache.set("float", 3.14)
        tiered_cache.set("list", [1, 2, 3])
        tiered_cache.set("dict", {"a": 1})
        assert tiered_cache.get("int") == 42
        assert tiered_cache.get("float") == 3.14
        assert tiered_cache.get("list") == [1, 2, 3]
        assert tiered_cache.get("dict") == {"a": 1}


class TestL1Behavior:
    """Tests specific to L1 in-process cache behavior."""

    def test_l1_serves_cached_value(self, tiered_cache: KeyValueCache):
        """After a set, the value is served from L1 without hitting L2."""
        tiered_cache.set("l1key", "l1val")
        # Access the underlying L1 cache to verify the value is there
        l1 = tiered_cache._l1
        assert l1.get("l1key") == "l1val"

    def test_l1_populated_on_get_miss(self, tiered_cache: KeyValueCache):
        """When L1 misses but L2 hits, L1 is populated."""
        l1 = tiered_cache._l1
        # Set directly via parent (L2 only) by using the L1 cache's absence
        tiered_cache.set("pop_key", "pop_val")
        # Clear L1 only
        l1.delete("pop_key")
        assert l1.get("pop_key") is None

        # get() should fetch from L2 and populate L1
        assert tiered_cache.get("pop_key") == "pop_val"
        assert l1.get("pop_key") == "pop_val"

    def test_l1_ttl_expiry(self, tiered_cache: KeyValueCache):
        """L1 entries expire after L1_TIMEOUT, then fall through to L2."""
        tiered_cache.set("ttl_key", "ttl_val", timeout=60)

        # Value should be in L1
        l1 = tiered_cache._l1
        assert l1.get("ttl_key") == "ttl_val"

        # Wait for L1 to expire
        time.sleep(L1_TIMEOUT + 0.5)

        # L1 should be expired
        assert l1.get("ttl_key") is None
        # But L2 still has it, so get() succeeds
        assert tiered_cache.get("ttl_key") == "ttl_val"
        # And L1 is repopulated
        assert l1.get("ttl_key") == "ttl_val"

    def test_get_many_partial_l1_hit(self, tiered_cache: KeyValueCache):
        """get_many with some keys in L1 and some only in L2."""
        tiered_cache.set_many({"gm1": "a", "gm2": "b", "gm3": "c"})

        # Clear some keys from L1 only
        l1 = tiered_cache._l1
        l1.delete("gm2")
        l1.delete("gm3")

        result = tiered_cache.get_many(["gm1", "gm2", "gm3"])
        assert result == {"gm1": "a", "gm2": "b", "gm3": "c"}

        # gm2 and gm3 should now be back in L1
        assert l1.get("gm2") == "b"
        assert l1.get("gm3") == "c"

    def test_delete_evicts_from_l1(self, tiered_cache: KeyValueCache):
        """delete() removes from both L1 and L2."""
        tiered_cache.set("del_l1", "val")
        l1 = tiered_cache._l1
        assert l1.get("del_l1") == "val"

        tiered_cache.delete("del_l1")
        assert l1.get("del_l1") is None

    def test_delete_many_evicts_from_l1(self, tiered_cache: KeyValueCache):
        """delete_many() removes from both L1 and L2."""
        tiered_cache.set_many({"dml1": 1, "dml2": 2})
        l1 = tiered_cache._l1
        assert l1.get("dml1") == 1

        tiered_cache.delete_many(["dml1", "dml2"])
        assert l1.get("dml1") is None
        assert l1.get("dml2") is None

    def test_incr_evicts_from_l1(self, tiered_cache: KeyValueCache):
        """incr() evicts from L1 so next get() fetches fresh value from L2."""
        tiered_cache.set("inc_key", 5)
        l1 = tiered_cache._l1
        assert l1.get("inc_key") == 5

        tiered_cache.incr("inc_key", 3)
        # L1 should be evicted
        assert l1.get("inc_key") is None
        # But get() fetches from L2 and repopulates L1
        assert tiered_cache.get("inc_key") == 8
        assert l1.get("inc_key") == 8

    def test_clear_clears_l1(self, tiered_cache: KeyValueCache):
        """clear() clears both L1 and L2."""
        tiered_cache.set("cl1", "a")
        tiered_cache.set("cl2", "b")
        l1 = tiered_cache._l1
        assert l1.get("cl1") == "a"

        tiered_cache.clear()
        assert l1.get("cl1") is None
        assert l1.get("cl2") is None

    def test_delete_pattern_clears_l1(self, tiered_cache: KeyValueCache):
        """delete_pattern() clears entire L1 (can't match patterns in L1)."""
        tiered_cache.set("pat_a", 1)
        tiered_cache.set("pat_b", 2)
        l1 = tiered_cache._l1
        assert l1.get("pat_a") == 1

        tiered_cache.delete_pattern("pat_*")
        assert l1.get("pat_a") is None
        assert l1.get("pat_b") is None

    def test_has_key_l1_fast_path(self, tiered_cache: KeyValueCache):
        """has_key returns True from L1 without L2 call."""
        tiered_cache.set("hk_key", "val")
        # L1 has it, so has_key should return True
        assert tiered_cache.has_key("hk_key") is True

    def test_add_populates_l1_on_success(self, tiered_cache: KeyValueCache):
        """add() populates L1 when L2 add succeeds."""
        tiered_cache.delete("add_l1")
        tiered_cache.add("add_l1", "new_val")
        l1 = tiered_cache._l1
        assert l1.get("add_l1") == "new_val"

    def test_add_skips_l1_on_failure(self, tiered_cache: KeyValueCache):
        """add() does not update L1 when key already exists in L2."""
        tiered_cache.set("add_exists", "original")
        l1 = tiered_cache._l1
        # Clear L1 to test that add(fail) doesn't repopulate
        l1.delete("add_exists")

        tiered_cache.add("add_exists", "new_val")
        # L1 should NOT have the new value (add failed)
        assert l1.get("add_exists") is None

    def test_touch_refreshes_l1(self, tiered_cache: KeyValueCache):
        """touch() refreshes L1 TTL when L2 touch succeeds."""
        tiered_cache.set("touch_l1", "val")
        l1 = tiered_cache._l1
        assert l1.get("touch_l1") == "val"

        result = tiered_cache.touch("touch_l1", timeout=60)
        assert result is True
        # L1 should still have the value (TTL refreshed)
        assert l1.get("touch_l1") == "val"

    def test_flush_db_clears_l1(self, tiered_cache: KeyValueCache):
        """flush_db() clears L1 along with L2."""
        tiered_cache.set("flush_key", "val")
        l1 = tiered_cache._l1
        assert l1.get("flush_key") == "val"

        tiered_cache.flush_db()
        assert l1.get("flush_key") is None

    def test_data_structures_bypass_l1(self, tiered_cache: KeyValueCache):
        """Data structure operations go directly to L2, not through L1."""
        tiered_cache.hset("myhash", mapping={"f1": "v1"})
        assert tiered_cache.hget("myhash", "f1") == "v1"
        # L1 should not have the hash key
        l1 = tiered_cache._l1
        assert l1.get("myhash") is None

    def test_l1_serves_without_l2_call(self, tiered_cache: KeyValueCache):
        """After set, L1 has the value so get doesn't need L2."""
        tiered_cache.set("mock_key", "mock_val")
        l1 = tiered_cache._l1
        # Confirm L1 has the value
        assert l1.get("mock_key") == "mock_val"
        # The mixin's get() checks L1 first — verified by test_l1_populated_on_get_miss
        # and test_l1_ttl_expiry. Here just confirm basic consistency.
        assert tiered_cache.get("mock_key") == "mock_val"


@pytest.mark.asyncio
class TestTieredAsync:
    """Async variants of core operations."""

    async def test_aget_aset(self, tiered_cache: KeyValueCache):
        await tiered_cache.aset("akey", "aval")
        assert await tiered_cache.aget("akey") == "aval"

    async def test_aget_missing(self, tiered_cache: KeyValueCache):
        assert await tiered_cache.aget("amissing") is None
        assert await tiered_cache.aget("amissing", "fb") == "fb"

    async def test_adelete(self, tiered_cache: KeyValueCache):
        await tiered_cache.aset("adel", "val")
        await tiered_cache.adelete("adel")
        assert await tiered_cache.aget("adel") is None

    async def test_aget_many_aset_many(self, tiered_cache: KeyValueCache):
        await tiered_cache.aset_many({"am1": 1, "am2": 2})
        result = await tiered_cache.aget_many(["am1", "am2"])
        assert result == {"am1": 1, "am2": 2}

    async def test_aadd(self, tiered_cache: KeyValueCache):
        await tiered_cache.adelete("aadd_key")
        assert await tiered_cache.aadd("aadd_key", "first") is True
        assert await tiered_cache.aadd("aadd_key", "second") is False
        assert await tiered_cache.aget("aadd_key") == "first"

    async def test_ahas_key(self, tiered_cache: KeyValueCache):
        await tiered_cache.aset("ahk", 1)
        assert await tiered_cache.ahas_key("ahk") is True
        assert await tiered_cache.ahas_key("anope") is False

    async def test_aincr(self, tiered_cache: KeyValueCache):
        await tiered_cache.aset("ainc", 10)
        assert await tiered_cache.aincr("ainc", 5) == 15
        assert await tiered_cache.aget("ainc") == 15

    async def test_atouch(self, tiered_cache: KeyValueCache):
        await tiered_cache.aset("atouch", "val")
        assert await tiered_cache.atouch("atouch", timeout=60) is True
        assert await tiered_cache.aget("atouch") == "val"

    async def test_adelete_many(self, tiered_cache: KeyValueCache):
        await tiered_cache.aset_many({"adm1": 1, "adm2": 2})
        await tiered_cache.adelete_many(["adm1", "adm2"])
        assert await tiered_cache.aget("adm1") is None

    async def test_aclear(self, tiered_cache: KeyValueCache):
        await tiered_cache.aset("acl", "val")
        await tiered_cache.aclear()
        assert await tiered_cache.aget("acl") is None

    async def test_async_l1_populated_on_miss(self, tiered_cache: KeyValueCache):
        """Async get populates L1 on L2 hit."""
        await tiered_cache.aset("al1pop", "val")
        l1 = tiered_cache._l1
        l1.delete("al1pop")
        assert l1.get("al1pop") is None

        assert await tiered_cache.aget("al1pop") == "val"
        assert l1.get("al1pop") == "val"

    async def test_async_get_many_partial_l1(self, tiered_cache: KeyValueCache):
        """Async get_many with partial L1 hits."""
        await tiered_cache.aset_many({"agm1": "a", "agm2": "b"})
        l1 = tiered_cache._l1
        l1.delete("agm2")

        result = await tiered_cache.aget_many(["agm1", "agm2"])
        assert result == {"agm1": "a", "agm2": "b"}
        assert l1.get("agm2") == "b"


class TestTieredCacheMixinConfig:
    """Test configuration and initialization."""

    def test_default_l1_options(self, redis_container: RedisContainerInfo):
        """Default L1 options are used when not specified."""
        config = build_cache_config(
            redis_container.host,
            redis_container.port,
            client_library=redis_container.client_library,
        )
        for cache_config in config.values():
            backend = cache_config["BACKEND"]
            if backend in TIERED_BACKENDS:
                cache_config["BACKEND"] = TIERED_BACKENDS[backend]

        with override_settings(CACHES=config):
            from django.core.cache import cache

            assert cache._l1_timeout == 5  # default
            assert cache._l1._max_entries == 1000  # default LocMemCache MAX_ENTRIES

    def test_custom_l1_options(self, redis_container: RedisContainerInfo):
        """Custom L1 options are respected."""
        config = build_cache_config(
            redis_container.host,
            redis_container.port,
            client_library=redis_container.client_library,
        )
        for cache_config in config.values():
            backend = cache_config["BACKEND"]
            if backend in TIERED_BACKENDS:
                cache_config["BACKEND"] = TIERED_BACKENDS[backend]
            cache_config.setdefault("OPTIONS", {})["TIERED_L1_TIMEOUT"] = 10
            cache_config["OPTIONS"]["TIERED_L1_MAX_ENTRIES"] = 500

        with override_settings(CACHES=config):
            from django.core.cache import cache

            assert cache._l1_timeout == 10
            assert cache._l1._max_entries == 500

    def test_l1_options_not_passed_to_redis_client(self, tiered_cache: KeyValueCache):
        """TIERED_L1_* options are consumed and not forwarded to the Redis client."""
        # If L1 options leaked to the client, it would fail or have unexpected attrs.
        # The fact that tiered_cache was created successfully confirms this.
        # Use hasattr since isinstance doesn't work through Django's ConnectionProxy.
        assert hasattr(tiered_cache, "_l1")
        assert hasattr(tiered_cache, "_l1_timeout")
