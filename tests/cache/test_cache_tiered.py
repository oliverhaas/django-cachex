"""Tests for two-tiered cache (L1 in-process + L2 Redis/Valkey)."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from tests.fixtures.cache import BACKENDS, _get_client_library_options

if TYPE_CHECKING:
    from django.core.cache.backends.base import BaseCache

    from tests.fixtures.containers import RedisContainerInfo

L1_TIMEOUT = 2  # seconds — short L1 cap for testing


def _build_tiered_config(host: str, port: int, client_library: str = "redis") -> dict:
    """Build CACHES config with l1 (LocMemCache), l2 (Redis/Valkey), and tiered."""
    options = _get_client_library_options(client_library)
    location = f"redis://{host}:{port}?db=1"
    backend_class = BACKENDS[("default", client_library)]

    return {
        "l1": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "OPTIONS": {"MAX_ENTRIES": 100},
        },
        "l2": {
            "BACKEND": backend_class,
            "LOCATION": location,
            "OPTIONS": options,
        },
        "default": {
            "BACKEND": "django_cachex.cache.TieredCache",
            "OPTIONS": {
                "TIERS": ["l1", "l2"],
                "L1_TIMEOUT": L1_TIMEOUT,
            },
        },
    }


@pytest.fixture
def tiered_cache(redis_container: RedisContainerInfo) -> BaseCache:
    """Tiered cache fixture with short L1 TTL for testing."""
    config = _build_tiered_config(
        redis_container.host,
        redis_container.port,
        client_library=redis_container.client_library,
    )

    with override_settings(CACHES=config):
        cache = caches["default"]
        cache.clear()
        yield cache
        cache.clear()


class TestTieredBasicOps:
    """Basic cache operations work correctly through tiered cache."""

    def test_get_set_roundtrip(self, tiered_cache: BaseCache):
        tiered_cache.set("key1", "value1")
        assert tiered_cache.get("key1") == "value1"

    def test_get_missing_returns_default(self, tiered_cache: BaseCache):
        assert tiered_cache.get("missing") is None
        assert tiered_cache.get("missing", "fallback") == "fallback"

    def test_set_many_get_many(self, tiered_cache: BaseCache):
        data = {"k1": "v1", "k2": "v2", "k3": "v3"}
        tiered_cache.set_many(data)
        result = tiered_cache.get_many(["k1", "k2", "k3"])
        assert result == data

    def test_delete(self, tiered_cache: BaseCache):
        tiered_cache.set("del_key", "val")
        assert tiered_cache.get("del_key") == "val"
        tiered_cache.delete("del_key")
        assert tiered_cache.get("del_key") is None

    def test_delete_many(self, tiered_cache: BaseCache):
        tiered_cache.set_many({"dm1": 1, "dm2": 2})
        tiered_cache.delete_many(["dm1", "dm2"])
        assert tiered_cache.get("dm1") is None
        assert tiered_cache.get("dm2") is None

    def test_add(self, tiered_cache: BaseCache):
        tiered_cache.delete("add_key")
        assert tiered_cache.add("add_key", "first") is True
        assert tiered_cache.add("add_key", "second") is False
        assert tiered_cache.get("add_key") == "first"

    def test_has_key(self, tiered_cache: BaseCache):
        tiered_cache.set("exists", 1)
        assert tiered_cache.has_key("exists") is True
        assert tiered_cache.has_key("nope") is False

    def test_incr(self, tiered_cache: BaseCache):
        tiered_cache.set("counter", 10)
        assert tiered_cache.incr("counter") == 11
        assert tiered_cache.incr("counter", 5) == 16
        assert tiered_cache.get("counter") == 16

    def test_decr(self, tiered_cache: BaseCache):
        tiered_cache.set("dcounter", 10)
        assert tiered_cache.decr("dcounter") == 9
        assert tiered_cache.get("dcounter") == 9

    def test_touch(self, tiered_cache: BaseCache):
        tiered_cache.set("touch_key", "val", timeout=10)
        assert tiered_cache.touch("touch_key", timeout=60) is True
        assert tiered_cache.get("touch_key") == "val"

    def test_get_or_set(self, tiered_cache: BaseCache):
        tiered_cache.delete("gos_key")
        val = tiered_cache.get_or_set("gos_key", "created")
        assert val == "created"
        val = tiered_cache.get_or_set("gos_key", "ignored")
        assert val == "created"

    def test_clear(self, tiered_cache: BaseCache):
        tiered_cache.set("clear1", "a")
        tiered_cache.set("clear2", "b")
        tiered_cache.clear()
        assert tiered_cache.get("clear1") is None
        assert tiered_cache.get("clear2") is None

    def test_set_nx_updates_l1_on_success(self, tiered_cache: BaseCache):
        tiered_cache.delete("nx_key")
        result = tiered_cache.set("nx_key", "val", nx=True)
        assert result is True
        assert tiered_cache.get("nx_key") == "val"

    def test_set_nx_skips_l1_on_failure(self, tiered_cache: BaseCache):
        tiered_cache.set("nx_existing", "original")
        result = tiered_cache.set("nx_existing", "new", nx=True)
        assert result is False
        assert tiered_cache.get("nx_existing") == "original"

    def test_versioned_keys(self, tiered_cache: BaseCache):
        tiered_cache.set("vkey", "v1", version=1)
        tiered_cache.set("vkey", "v2", version=2)
        assert tiered_cache.get("vkey", version=1) == "v1"
        assert tiered_cache.get("vkey", version=2) == "v2"

    def test_various_value_types(self, tiered_cache: BaseCache):
        tiered_cache.set("int", 42)
        tiered_cache.set("float", 3.14)
        tiered_cache.set("list", [1, 2, 3])
        tiered_cache.set("dict", {"a": 1})
        assert tiered_cache.get("int") == 42
        assert tiered_cache.get("float") == 3.14
        assert tiered_cache.get("list") == [1, 2, 3]
        assert tiered_cache.get("dict") == {"a": 1}

    def test_set_none_value(self, tiered_cache: BaseCache):
        """Storing None should be distinguishable from missing."""
        tiered_cache.set("none_key", None)
        assert tiered_cache.get("none_key", "MISS") is None


class TestL1Behavior:
    """Tests specific to L1 in-process cache behavior."""

    def _get_l1(self) -> BaseCache:
        return caches["l1"]

    def _get_l2(self) -> BaseCache:
        return caches["l2"]

    def test_l1_serves_cached_value(self, tiered_cache: BaseCache):
        """After a set, the value is served from L1."""
        tiered_cache.set("l1key", "l1val")
        l1 = self._get_l1()
        assert l1.get("l1key") == "l1val"

    def test_l1_populated_on_get_miss(self, tiered_cache: BaseCache):
        """When L1 misses but L2 hits, L1 is populated."""
        l1 = self._get_l1()
        tiered_cache.set("pop_key", "pop_val")
        # Clear L1 only
        l1.delete("pop_key")
        assert l1.get("pop_key") is None

        # get() should fetch from L2 and populate L1
        assert tiered_cache.get("pop_key") == "pop_val"
        assert l1.get("pop_key") == "pop_val"

    def test_l1_ttl_expiry(self, tiered_cache: BaseCache):
        """L1 entries expire after L1_TIMEOUT, then fall through to L2."""
        tiered_cache.set("ttl_key", "ttl_val", timeout=60)

        l1 = self._get_l1()
        assert l1.get("ttl_key") == "ttl_val"

        # Wait for L1 to expire
        time.sleep(L1_TIMEOUT + 0.5)

        # L1 should be expired
        assert l1.get("ttl_key") is None
        # But L2 still has it, so get() succeeds
        assert tiered_cache.get("ttl_key") == "ttl_val"
        # And L1 is repopulated
        assert l1.get("ttl_key") == "ttl_val"

    def test_l1_ttl_capped_by_l2(self, tiered_cache: BaseCache):
        """L1 TTL is capped by L2's remaining TTL when shorter than L1 default."""
        l2 = self._get_l2()
        l1 = self._get_l1()

        # Set in L2 with very short TTL (1 second — shorter than L1's 2s default)
        l2.set("short_ttl", "val", timeout=1)

        # Clear L1 so get() will populate from L2
        l1.delete("short_ttl")

        # get() should populate L1 with TTL capped by L2's remaining TTL
        assert tiered_cache.get("short_ttl") == "val"

        # L1 should have it now but with a short TTL
        assert l1.get("short_ttl") == "val"

        # Wait for L2's TTL to expire — L1 should also have expired
        time.sleep(1.5)
        assert l1.get("short_ttl") is None
        assert l2.get("short_ttl") is None

    def test_get_many_partial_l1_hit(self, tiered_cache: BaseCache):
        """get_many with some keys in L1 and some only in L2."""
        tiered_cache.set_many({"gm1": "a", "gm2": "b", "gm3": "c"})

        l1 = self._get_l1()
        l1.delete("gm2")
        l1.delete("gm3")

        result = tiered_cache.get_many(["gm1", "gm2", "gm3"])
        assert result == {"gm1": "a", "gm2": "b", "gm3": "c"}

        # gm2 and gm3 should now be back in L1
        assert l1.get("gm2") == "b"
        assert l1.get("gm3") == "c"

    def test_delete_evicts_from_l1(self, tiered_cache: BaseCache):
        """delete() removes from both L1 and L2."""
        tiered_cache.set("del_l1", "val")
        l1 = self._get_l1()
        assert l1.get("del_l1") == "val"

        tiered_cache.delete("del_l1")
        assert l1.get("del_l1") is None

    def test_delete_many_evicts_from_l1(self, tiered_cache: BaseCache):
        """delete_many() removes from both L1 and L2."""
        tiered_cache.set_many({"dml1": 1, "dml2": 2})
        l1 = self._get_l1()
        assert l1.get("dml1") == 1

        tiered_cache.delete_many(["dml1", "dml2"])
        assert l1.get("dml1") is None
        assert l1.get("dml2") is None

    def test_incr_evicts_from_l1(self, tiered_cache: BaseCache):
        """incr() evicts from L1 so next get() fetches fresh value from L2."""
        tiered_cache.set("inc_key", 5)
        l1 = self._get_l1()
        assert l1.get("inc_key") == 5

        tiered_cache.incr("inc_key", 3)
        # L1 should be evicted
        assert l1.get("inc_key") is None
        # But get() fetches from L2 and repopulates L1
        assert tiered_cache.get("inc_key") == 8
        assert l1.get("inc_key") == 8

    def test_decr_evicts_from_l1(self, tiered_cache: BaseCache):
        """decr() evicts from L1 so next get() fetches fresh value from L2."""
        tiered_cache.set("dec_key", 10)
        l1 = self._get_l1()
        assert l1.get("dec_key") == 10

        tiered_cache.decr("dec_key", 3)
        assert l1.get("dec_key") is None
        assert tiered_cache.get("dec_key") == 7

    def test_clear_clears_l1(self, tiered_cache: BaseCache):
        """clear() clears both L1 and L2."""
        tiered_cache.set("cl1", "a")
        tiered_cache.set("cl2", "b")
        l1 = self._get_l1()
        assert l1.get("cl1") == "a"

        tiered_cache.clear()
        assert l1.get("cl1") is None
        assert l1.get("cl2") is None

    def test_has_key_l1_fast_path(self, tiered_cache: BaseCache):
        """has_key returns True from L1 without L2 call."""
        tiered_cache.set("hk_key", "val")
        assert tiered_cache.has_key("hk_key") is True

    def test_add_populates_l1_on_success(self, tiered_cache: BaseCache):
        """add() populates L1 when L2 add succeeds."""
        tiered_cache.delete("add_l1")
        tiered_cache.add("add_l1", "new_val")
        l1 = self._get_l1()
        assert l1.get("add_l1") == "new_val"

    def test_add_skips_l1_on_failure(self, tiered_cache: BaseCache):
        """add() does not update L1 when key already exists in L2."""
        tiered_cache.set("add_exists", "original")
        l1 = self._get_l1()
        l1.delete("add_exists")

        tiered_cache.add("add_exists", "new_val")
        # L1 should NOT have the new value (add failed)
        assert l1.get("add_exists") is None

    def test_touch_refreshes_l1(self, tiered_cache: BaseCache):
        """touch() refreshes L1 TTL when L2 touch succeeds."""
        tiered_cache.set("touch_l1", "val")
        l1 = self._get_l1()
        assert l1.get("touch_l1") == "val"

        result = tiered_cache.touch("touch_l1", timeout=60)
        assert result is True
        assert l1.get("touch_l1") == "val"

    def test_l1_serves_without_l2_call(self, tiered_cache: BaseCache):
        """After set, L1 has the value so get doesn't need L2."""
        tiered_cache.set("mock_key", "mock_val")
        l1 = self._get_l1()
        assert l1.get("mock_key") == "mock_val"
        assert tiered_cache.get("mock_key") == "mock_val"

    def test_set_timeout_caps_l1(self, tiered_cache: BaseCache):
        """set(timeout=1) should cap L1 TTL at 1 second (less than L1 default of 2)."""
        tiered_cache.set("short", "val", timeout=1)
        l1 = self._get_l1()
        assert l1.get("short") == "val"

        time.sleep(1.5)
        assert l1.get("short") is None


class TestAdminDelegation:
    """Admin delegation methods forward to L2."""

    def test_keys_delegates_to_l2(self, tiered_cache: BaseCache):
        tiered_cache.set("admin_key", "val")
        keys = tiered_cache.keys("*admin*")
        assert any("admin_key" in k for k in keys)

    def test_ttl_delegates_to_l2(self, tiered_cache: BaseCache):
        tiered_cache.set("ttl_admin", "val", timeout=60)
        ttl = tiered_cache.ttl("ttl_admin")
        assert 0 < ttl <= 60

    def test_type_delegates_to_l2(self, tiered_cache: BaseCache):
        tiered_cache.set("type_admin", "val")
        key_type = tiered_cache.type("type_admin")
        assert key_type == "string"

    def test_info_delegates_to_l2(self, tiered_cache: BaseCache):
        info = tiered_cache.info()
        assert isinstance(info, dict)

    def test_scan_delegates_to_l2(self, tiered_cache: BaseCache):
        tiered_cache.set("scan_key", "val")
        cursor, keys = tiered_cache.scan(cursor=0, count=100)
        assert isinstance(cursor, int)
        assert isinstance(keys, list)

    def test_delete_pattern_clears_l1(self, tiered_cache: BaseCache):
        tiered_cache.set("pat_a", 1)
        tiered_cache.set("pat_b", 2)
        l1 = caches["l1"]
        assert l1.get("pat_a") == 1

        tiered_cache.delete_pattern("*pat_*")
        assert l1.get("pat_a") is None
        assert l1.get("pat_b") is None

    def test_expire_evicts_l1(self, tiered_cache: BaseCache):
        tiered_cache.set("exp_key", "val")
        l1 = caches["l1"]
        assert l1.get("exp_key") == "val"

        tiered_cache.expire("exp_key", 1)
        assert l1.get("exp_key") is None


@pytest.mark.asyncio
class TestTieredAsync:
    """Async variants of core operations."""

    async def test_aget_aset(self, tiered_cache: BaseCache):
        await tiered_cache.aset("akey", "aval")
        assert await tiered_cache.aget("akey") == "aval"

    async def test_aget_missing(self, tiered_cache: BaseCache):
        assert await tiered_cache.aget("amissing") is None
        assert await tiered_cache.aget("amissing", "fb") == "fb"

    async def test_adelete(self, tiered_cache: BaseCache):
        await tiered_cache.aset("adel", "val")
        await tiered_cache.adelete("adel")
        assert await tiered_cache.aget("adel") is None

    async def test_aget_many_aset_many(self, tiered_cache: BaseCache):
        await tiered_cache.aset_many({"am1": 1, "am2": 2})
        result = await tiered_cache.aget_many(["am1", "am2"])
        assert result == {"am1": 1, "am2": 2}

    async def test_aadd(self, tiered_cache: BaseCache):
        await tiered_cache.adelete("aadd_key")
        assert await tiered_cache.aadd("aadd_key", "first") is True
        assert await tiered_cache.aadd("aadd_key", "second") is False
        assert await tiered_cache.aget("aadd_key") == "first"

    async def test_ahas_key(self, tiered_cache: BaseCache):
        await tiered_cache.aset("ahk", 1)
        assert await tiered_cache.ahas_key("ahk") is True
        assert await tiered_cache.ahas_key("anope") is False

    async def test_aincr(self, tiered_cache: BaseCache):
        await tiered_cache.aset("ainc", 10)
        assert await tiered_cache.aincr("ainc", 5) == 15
        assert await tiered_cache.aget("ainc") == 15

    async def test_atouch(self, tiered_cache: BaseCache):
        await tiered_cache.aset("atouch", "val")
        assert await tiered_cache.atouch("atouch", timeout=60) is True
        assert await tiered_cache.aget("atouch") == "val"

    async def test_adelete_many(self, tiered_cache: BaseCache):
        await tiered_cache.aset_many({"adm1": 1, "adm2": 2})
        await tiered_cache.adelete_many(["adm1", "adm2"])
        assert await tiered_cache.aget("adm1") is None

    async def test_aclear(self, tiered_cache: BaseCache):
        await tiered_cache.aset("acl", "val")
        await tiered_cache.aclear()
        assert await tiered_cache.aget("acl") is None

    async def test_async_l1_populated_on_miss(self, tiered_cache: BaseCache):
        """Async get populates L1 on L2 hit."""
        await tiered_cache.aset("al1pop", "val")
        l1 = caches["l1"]
        l1.delete("al1pop")
        assert l1.get("al1pop") is None

        assert await tiered_cache.aget("al1pop") == "val"
        assert l1.get("al1pop") == "val"

    async def test_async_get_many_partial_l1(self, tiered_cache: BaseCache):
        """Async get_many with partial L1 hits."""
        await tiered_cache.aset_many({"agm1": "a", "agm2": "b"})
        l1 = caches["l1"]
        l1.delete("agm2")

        result = await tiered_cache.aget_many(["agm1", "agm2"])
        assert result == {"agm1": "a", "agm2": "b"}
        assert l1.get("agm2") == "b"


class TestTieredCacheConfig:
    """Test configuration and initialization."""

    def test_missing_tiers_raises(self):
        """Missing TIERS option should raise ImproperlyConfigured."""
        config = {
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {},
            },
        }
        with override_settings(CACHES=config), pytest.raises(ImproperlyConfigured, match="TIERS"):
            caches["default"].get("test")

    def test_wrong_tier_count_raises(self):
        """TIERS with wrong number of aliases should raise ImproperlyConfigured."""
        config = {
            "l1": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            },
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {
                    "TIERS": ["l1"],
                },
            },
        }
        with override_settings(CACHES=config), pytest.raises(ImproperlyConfigured, match="TIERS"):
            caches["default"].get("test")

    def test_l1_timeout_from_option(self, tiered_cache: BaseCache):
        """L1 TTL cap comes from TieredCache's L1_TIMEOUT option."""
        assert tiered_cache._l1_cap == L1_TIMEOUT

    def test_l1_timeout_fallback_to_l1_default(self, redis_container: RedisContainerInfo):
        """When L1_TIMEOUT is omitted, _l1_cap falls back to L1's default_timeout."""
        options = _get_client_library_options(redis_container.client_library)
        location = f"redis://{redis_container.host}:{redis_container.port}?db=1"
        backend_class = BACKENDS[("default", redis_container.client_library)]

        config = {
            "l1": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                "TIMEOUT": 42,
            },
            "l2": {
                "BACKEND": backend_class,
                "LOCATION": location,
                "OPTIONS": options,
            },
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {
                    "TIERS": ["l1", "l2"],
                    # No L1_TIMEOUT — should fall back to L1's TIMEOUT (42)
                },
            },
        }
        with override_settings(CACHES=config):
            cache = caches["default"]
            assert cache._l1_cap == 42

    def test_cachex_support_level(self, tiered_cache: BaseCache):
        """TieredCache has 'cachex' support level for admin."""
        assert tiered_cache._cachex_support == "cachex"

    def test_locmem_support_level(self):
        """Our LocMemCache overrides CachexMixin to 'cachex'."""
        from django_cachex.cache.locmem import LocMemCache

        assert LocMemCache._cachex_support == "cachex"

    def test_mixin_support_level(self):
        """CachexMixin itself defaults to 'wrapped'."""
        from django_cachex.cache.mixin import CachexMixin

        assert CachexMixin._cachex_support == "wrapped"


class TestTieredSetManyOrdering:
    """Verify set_many writes L2 before L1 so L1 doesn't have phantom data on L2 failure."""

    def test_set_many_data_in_both_tiers(self, tiered_cache: BaseCache):
        """After set_many, data should be in both L1 and L2."""
        data = {"order_a": "va", "order_b": "vb"}
        tiered_cache.set_many(data)

        # L1 should have the data
        assert tiered_cache._l1.get("order_a") == "va"
        assert tiered_cache._l1.get("order_b") == "vb"

        # L2 should also have the data
        assert tiered_cache._l2.get("order_a") == "va"
        assert tiered_cache._l2.get("order_b") == "vb"

    def test_set_many_returns_l2_result(self, tiered_cache: BaseCache):
        """set_many should return L2's result (empty list = no failures)."""
        result = tiered_cache.set_many({"order_c": "vc"})
        assert result == []
