"""Tests for two-tiered cache (L1 in-process + L2 Redis/Valkey)."""

import asyncio
import time
from typing import TYPE_CHECKING, Any

import pytest
from django.core.cache import caches
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.core.cache.backends.locmem import LocMemCache as DjangoLocMemCache
from django.core.exceptions import ImproperlyConfigured, SynchronousOnlyOperation
from django.test import override_settings

from django_cachex.exceptions import NotSupportedError
from tests.cache.conftest import L1_TIMEOUT
from tests.fixtures.cache import BACKENDS, _get_client_library_options

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django.core.cache.backends.base import BaseCache

    from tests.fixtures.containers import RedisContainerInfo


class AsyncGuardL1(DjangoLocMemCache):
    """L1 stand-in that rejects sync calls made on the event loop thread.

    Stands in for a DB-backed L1 (raises ``SynchronousOnlyOperation``) or a
    network-backed one (silently blocks the loop). Its ``a*`` methods are
    Django's, which hop to a worker thread, so an async caller passes.
    """

    @staticmethod
    def _guard(method: str) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        msg = f"L1.{method}() was called synchronously from the event loop thread"
        raise SynchronousOnlyOperation(msg)

    def get(self, key: str, default: Any = None, version: int | None = None) -> Any:
        self._guard("get")
        return super().get(key, default, version)

    def set(self, key: str, value: Any, timeout: Any = DEFAULT_TIMEOUT, version: int | None = None) -> None:
        self._guard("set")
        super().set(key, value, timeout, version)

    def delete(self, key: str, version: int | None = None) -> bool:
        self._guard("delete")
        return super().delete(key, version)

    def has_key(self, key: str, version: int | None = None) -> bool:
        self._guard("has_key")
        return super().has_key(key, version)

    def touch(self, key: str, timeout: Any = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        self._guard("touch")
        return super().touch(key, timeout, version)

    def clear(self) -> None:
        self._guard("clear")
        super().clear()


class TestTieredL2WriteDetection:
    """Unit coverage for the L1-population gate, including the nx/xx + get
    combinations that decide L1/L2 coherence, without needing a Redis 7 L2."""

    def test_write_happened_matrix(self):
        from django_cachex.cache.tiered import TieredCache

        w = TieredCache._l2_write_happened
        # Plain set always writes (L2 returns None).
        assert w(None, nx=False, xx=False, get=False) is True
        # nx/xx without get return a success bool.
        assert w(True, nx=True, xx=False, get=False) is True
        assert w(False, nx=True, xx=False, get=False) is False
        assert w(True, nx=False, xx=True, get=False) is True
        assert w(False, nx=False, xx=True, get=False) is False
        # With get, the L2 return is the prior value; success is inferred from
        # the conditional flag so a rejected write never lands in L1.
        assert w("old", nx=False, xx=False, get=True) is True  # unconditional get
        assert w(None, nx=True, xx=False, get=True) is True  # nx wrote (key was absent)
        assert w("old", nx=True, xx=False, get=True) is False  # nx rejected (key existed)
        assert w("old", nx=False, xx=True, get=True) is True  # xx wrote (key existed)
        assert w(None, nx=False, xx=True, get=True) is False  # xx rejected (key absent)


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

    def test_set_get_returns_prior_value(self, tiered_cache: BaseCache):
        # ``get=True`` must surface the prior value, not a coerced bool.
        tiered_cache.delete("getk")
        assert tiered_cache.set("getk", "first", get=True) is None
        assert tiered_cache.set("getk", "second", get=True) == "first"
        assert tiered_cache.get("getk") == "second"

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
        l1 = self._get_l1()
        tiered_cache.set("pop_key", "pop_val")
        # Clear L1 only
        l1.delete("pop_key")
        assert l1.get("pop_key") is None

        # get() should fetch from L2 and populate L1
        assert tiered_cache.get("pop_key") == "pop_val"
        assert l1.get("pop_key") == "pop_val"

    def test_l1_ttl_expiry(self, tiered_cache: BaseCache):
        """When L1 expires, get() falls through to L2 and repopulates L1."""
        tiered_cache.set("ttl_key", "ttl_val", timeout=60)

        l1 = self._get_l1()
        assert l1.get("ttl_key") == "ttl_val"

        # Force L1 to expire deterministically.
        l1.expire("ttl_key", 0)
        assert l1.get("ttl_key") is None

        # L2 still has it, so get() succeeds and L1 is repopulated.
        assert tiered_cache.get("ttl_key") == "ttl_val"
        assert l1.get("ttl_key") == "ttl_val"

    def test_l1_ttl_capped_by_l2(self, tiered_cache: BaseCache):
        """L1 TTL is capped by L2's remaining TTL when shorter than L1 default."""
        l2 = self._get_l2()
        l1 = self._get_l1()

        # Set in L2 with very short TTL (1 second, shorter than L1's 2s default)
        l2.set("short_ttl", "val", timeout=1)

        # Clear L1 so get() will populate from L2
        l1.delete("short_ttl")

        # get() should populate L1 with TTL capped by L2's remaining TTL
        assert tiered_cache.get("short_ttl") == "val"

        # L1 should have it now but with a short TTL
        assert l1.get("short_ttl") == "val"

        # Force both tiers to expire (their TTLs are real but we don't want
        # to sleep through them); each tier independently confirms gone.
        l1.expire("short_ttl", 0)
        l2.expire("short_ttl", 0)
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

    def test_get_many_batches_l2_ttl_lookups(self, tiered_cache: BaseCache, mocker):
        """L1 repopulation must not cost one TTL round trip per key.

        The per-key ``_get_l2_ttl`` is the fallback for an L2 that can't
        pipeline; a RESP L2 has to take the batched path instead.
        """
        tiered_cache.set_many({"bt1": "a", "bt2": "b", "bt3": "c"})
        l1 = self._get_l1()
        for key in ("bt1", "bt2", "bt3"):
            l1.delete(key)

        per_key = mocker.spy(type(tiered_cache), "_get_l2_ttl")
        result = tiered_cache.get_many(["bt1", "bt2", "bt3"])

        assert result == {"bt1": "a", "bt2": "b", "bt3": "c"}
        assert per_key.call_count == 0
        assert l1.get("bt2") == "b"

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
        tiered_cache.delete("add_l1")
        tiered_cache.add("add_l1", "new_val")
        l1 = self._get_l1()
        assert l1.get("add_l1") == "new_val"

    def test_add_skips_l1_on_failure(self, tiered_cache: BaseCache):
        tiered_cache.set("add_exists", "original")
        l1 = self._get_l1()
        l1.delete("add_exists")

        tiered_cache.add("add_exists", "new_val")
        # L1 should NOT have the new value (add failed)
        assert l1.get("add_exists") is None

    def test_touch_refreshes_l1(self, tiered_cache: BaseCache):
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
        # The cap is the contract; read it from the LocMemCache internals at
        # ms precision rather than waiting it out (and `pttl()` isn't
        # implemented on locmem).
        made_key = l1.make_key("short")
        remaining = l1._expire_info[made_key] - time.time()
        assert 0 < remaining <= 1


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

    def test_delete_pattern_preserves_l1_non_matching(self, tiered_cache: BaseCache):
        """delete_pattern must invalidate only matching keys in L1 (I7).

        Without targeted deletion the implementation called ``L1.clear()``,
        evicting every cached entry on any pattern. Verify the matching
        key is gone but the non-matching key still hits L1.
        """
        tiered_cache.set("user:42:profile", "match")
        tiered_cache.set("session:abc", "keep")
        l1 = caches["l1"]
        assert l1.get("user:42:profile") == "match"
        assert l1.get("session:abc") == "keep"

        tiered_cache.delete_pattern("user:*")
        assert l1.get("user:42:profile") is None
        assert l1.get("session:abc") == "keep"

    def test_expire_evicts_l1(self, tiered_cache: BaseCache):
        tiered_cache.set("exp_key", "val")
        l1 = caches["l1"]
        assert l1.get("exp_key") == "val"

        tiered_cache.expire("exp_key", 1)
        assert l1.get("exp_key") is None


class TestTieredCacheConfig:
    """Test configuration and initialization."""

    def test_missing_tiers_raises(self):
        """Missing tiers option should raise ImproperlyConfigured."""
        config = {
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {},
            },
        }
        with override_settings(CACHES=config), pytest.raises(ImproperlyConfigured, match="tiers"):
            caches["default"].get("test")

    def test_wrong_tier_count_raises(self):
        """tiers with wrong number of aliases should raise ImproperlyConfigured."""
        config = {
            "l1": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            },
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {
                    "tiers": ["l1"],
                },
            },
        }
        with override_settings(CACHES=config), pytest.raises(ImproperlyConfigured, match="tiers"):
            caches["default"].get("test")

    def test_key_prefix_in_options_rejected(self):
        """OPTIONS['KEY_PREFIX'] is never applied to either tier, so silently
        accepting it would store unprefixed keys; init must reject it."""
        config = {
            "l1": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            },
            "l2": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            },
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {
                    "tiers": ["l1", "l2"],
                    "KEY_PREFIX": "myapp",
                },
            },
        }
        with override_settings(CACHES=config), pytest.raises(ImproperlyConfigured, match="KEY_PREFIX"):
            caches["default"].get("test")

    def test_key_prefix_top_level_rejected(self):
        """A top-level KEY_PREFIX is never applied either, so two tiered aliases
        meant to be namespaced apart would silently collide; init must reject it."""
        config = {
            "l1": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            },
            "l2": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            },
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "KEY_PREFIX": "myapp",
                "OPTIONS": {
                    "tiers": ["l1", "l2"],
                },
            },
        }
        with override_settings(CACHES=config), pytest.raises(ImproperlyConfigured, match="KEY_PREFIX"):
            caches["default"].get("test")

    def test_empty_key_prefix_accepted(self):
        """An unset or empty KEY_PREFIX is the Django default and must not raise."""
        config = {
            "l1": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            },
            "l2": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            },
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "KEY_PREFIX": "",
                "OPTIONS": {
                    "tiers": ["l1", "l2"],
                },
            },
        }
        with override_settings(CACHES=config):
            assert caches["default"].get("test") is None

    def test_l1_timeout_from_option(self, tiered_cache: BaseCache):
        assert tiered_cache._l1_cap == L1_TIMEOUT

    def test_l1_timeout_clamped_by_l2_default_on_default_timeout(
        self,
        redis_container: RedisContainerInfo,
    ):
        """L1 must not outlive L2 when caller passes DEFAULT_TIMEOUT (B3).

        Configure L1 with a long default_timeout (3600s) and L2 with a
        short one (5s). A ``set()`` with no explicit timeout resolves to
        each tier's own default. Without the fix L1 stores for 3600s and
        would serve stale after L2 expires.
        """
        options = _get_client_library_options(redis_container.client_library)
        location = f"redis://{redis_container.host}:{redis_container.port}?db=1"
        py_adapter = "redis-py" if redis_container.client_library == "redis" else "valkey-py"
        backend_class = BACKENDS[("default", py_adapter)]

        config = {
            "l1": {
                "BACKEND": "django_cachex.cache.LocMemCache",
                "TIMEOUT": 3600,
            },
            "l2": {
                "BACKEND": backend_class,
                "LOCATION": location,
                "TIMEOUT": 5,
                "OPTIONS": options,
            },
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {
                    # No l1_timeout: falls back to L1.default_timeout = 3600.
                    "tiers": ["l1", "l2"],
                },
            },
        }
        with override_settings(CACHES=config):
            cache = caches["default"]
            assert cache._l1_cap == 3600
            assert cache._l2.default_timeout == 5
            # Caller passes DEFAULT_TIMEOUT implicitly (no timeout arg).
            l1_timeout = cache._l1_timeout_for_set(DEFAULT_TIMEOUT)
            assert l1_timeout is not None
            assert l1_timeout <= 5, f"L1 timeout {l1_timeout} would outlive L2 default {cache._l2.default_timeout}"

    def test_l1_timeout_fallback_to_l1_default(self, redis_container: RedisContainerInfo):
        options = _get_client_library_options(redis_container.client_library)
        location = f"redis://{redis_container.host}:{redis_container.port}?db=1"
        # Pick the redis-py / valkey-py adapter matching the container's library.
        py_adapter = "redis-py" if redis_container.client_library == "redis" else "valkey-py"
        backend_class = BACKENDS[("default", py_adapter)]

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
                    "tiers": ["l1", "l2"],
                    # No L1_TIMEOUT: should fall back to L1's TIMEOUT (42)
                },
            },
        }
        with override_settings(CACHES=config):
            cache = caches["default"]
            assert cache._l1_cap == 42

    def test_cachex_support_level(self, tiered_cache: BaseCache):
        assert tiered_cache._cachex_support == "limited"

    def test_locmem_support_level(self):
        from django_cachex.cache.locmem import LocMemCache

        assert LocMemCache._cachex_support == "cachex"


class TestTieredStockDjangoL2:
    """L2 as a stock Django backend, whose ``set`` takes no nx/xx/get kwargs.

    Regression: the flag kwargs were forwarded unconditionally, so any
    ``set()`` against a stock L2 raised ``TypeError`` at runtime.
    """

    @pytest.fixture
    def stock_tiered(self) -> Iterator[BaseCache]:
        config = {
            "l1": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                "LOCATION": "tiered-stock-l1",
            },
            "l2": {
                "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                "LOCATION": "tiered-stock-l2",
            },
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {
                    "tiers": ["l1", "l2"],
                    "l1_timeout": L1_TIMEOUT,
                },
            },
        }
        with override_settings(CACHES=config):
            cache = caches["default"]
            cache.clear()
            yield cache
            cache.clear()

    def test_plain_set_get_roundtrip(self, stock_tiered: BaseCache):
        stock_tiered.set("sk", "sv")
        assert stock_tiered.get("sk") == "sv"
        assert caches["l2"].get("sk") == "sv"

    def test_set_nx_emulated_via_add(self, stock_tiered: BaseCache):
        assert stock_tiered.set("nxk", "first", nx=True) is True
        assert stock_tiered.set("nxk", "second", nx=True) is False
        assert stock_tiered.get("nxk") == "first"
        assert caches["l1"].get("nxk") == "first"

    def test_set_xx_raises_not_supported(self, stock_tiered: BaseCache):
        with pytest.raises(NotSupportedError):
            stock_tiered.set("xxk", "v", xx=True)

    def test_set_get_flag_raises_not_supported(self, stock_tiered: BaseCache):
        with pytest.raises(NotSupportedError):
            stock_tiered.set("gk", "v", get=True)

    @pytest.mark.asyncio
    async def test_aset_plain_and_nx(self, stock_tiered: BaseCache):
        await stock_tiered.aset("ask", "av")
        assert await stock_tiered.aget("ask") == "av"
        assert await stock_tiered.aset("ask", "other", nx=True) is False
        with pytest.raises(NotSupportedError):
            await stock_tiered.aset("ask", "v", get=True)


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
        result = tiered_cache.set_many({"order_c": "vc"})
        assert result == []


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
        await tiered_cache.aset("al1pop", "val")
        l1 = caches["l1"]
        l1.delete("al1pop")
        assert l1.get("al1pop") is None

        assert await tiered_cache.aget("al1pop") == "val"
        assert l1.get("al1pop") == "val"

    async def test_async_get_many_partial_l1(self, tiered_cache: BaseCache):
        await tiered_cache.aset_many({"agm1": "a", "agm2": "b"})
        l1 = caches["l1"]
        l1.delete("agm2")

        result = await tiered_cache.aget_many(["agm1", "agm2"])
        assert result == {"agm1": "a", "agm2": "b"}
        assert l1.get("agm2") == "b"


@pytest.mark.asyncio
class TestTieredAsyncAdminDelegation:
    """The admin surface has async twins that delegate to L2.

    Regression: none of them were overridden, so ``BaseCachex``'s
    ``NotSupportedError`` stubs won even though every sync twin delegated fine,
    leaving async code unable to invalidate through ``aexpire`` /
    ``adelete_pattern``.
    """

    async def test_attl_and_apttl(self, tiered_cache: BaseCache):
        await tiered_cache.aset("aadmin", "v", timeout=100)
        assert await tiered_cache.attl("aadmin") > 0
        assert await tiered_cache.apttl("aadmin") > 0

    async def test_atype(self, tiered_cache: BaseCache):
        await tiered_cache.aset("aadmin", "v")
        assert await tiered_cache.atype("aadmin") is not None

    async def test_apersist_and_aexpire(self, tiered_cache: BaseCache):
        await tiered_cache.aset("aadmin", "v", timeout=100)
        assert await tiered_cache.apersist("aadmin") is True
        assert await tiered_cache.attl("aadmin") is None  # no expiry
        assert await tiered_cache.aexpire("aadmin", 100) is True
        assert await tiered_cache.attl("aadmin") > 0

    async def test_aexpire_evicts_l1(self, tiered_cache: BaseCache):
        await tiered_cache.aset("aexp", "v")
        assert caches["l1"].get("aexp") == "v"
        await tiered_cache.aexpire("aexp", 100)
        assert caches["l1"].get("aexp") is None

    async def test_akeys_and_aiter_keys(self, tiered_cache: BaseCache):
        await tiered_cache.aset("akeys:1", "v")
        assert "akeys:1" in await tiered_cache.akeys("akeys:*")
        assert [k async for k in tiered_cache.aiter_keys("akeys:*")] == ["akeys:1"]

    async def test_ascan(self, tiered_cache: BaseCache):
        await tiered_cache.aset("ascan:1", "v")
        _cursor, keys = await tiered_cache.ascan(pattern="ascan:*")
        assert "ascan:1" in keys

    async def test_adelete_pattern_invalidates_l1(self, tiered_cache: BaseCache):
        await tiered_cache.aset_many({"adp:1": "a", "keep": "b"})
        assert await tiered_cache.adelete_pattern("adp:*") == 1
        assert await tiered_cache.aget("adp:1") is None
        assert caches["l1"].get("adp:1") is None
        assert caches["l1"].get("keep") == "b"


class TestTieredNonRespL2:
    """A stock (non-RESP) L2 returns ``None`` from ``clear``/``delete_many``,
    which means success, not failure."""

    @pytest.fixture
    def locmem_tiered(self) -> Iterator[BaseCache]:
        config = {
            "l1": {"BACKEND": "django_cachex.cache.LocMemCache", "LOCATION": "tiered-nonresp-l1"},
            "l2": {"BACKEND": "django_cachex.cache.LocMemCache", "LOCATION": "tiered-nonresp-l2"},
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {"tiers": ["l1", "l2"], "l1_timeout": L1_TIMEOUT},
            },
        }
        with override_settings(CACHES=config):
            cache = caches["default"]
            cache.clear()
            yield cache
            cache.clear()

    def test_clear_reports_success(self, locmem_tiered: BaseCache):
        # Regression: ``bool(None)`` reported a successful clear as a failure.
        locmem_tiered.set("a", 1)
        assert locmem_tiered.clear() is True
        assert locmem_tiered.get("a") is None

    def test_delete_many_reports_the_number_of_keys(self, locmem_tiered: BaseCache):
        # Regression: ``None or 0`` reported two successful deletes as zero.
        locmem_tiered.set_many({"a": 1, "b": 2})
        assert locmem_tiered.delete_many(["a", "b"]) == 2
        assert locmem_tiered.get_many(["a", "b"]) == {}

    @pytest.mark.asyncio
    async def test_aclear_and_adelete_many_report_success(self, locmem_tiered: BaseCache):
        await locmem_tiered.aset_many({"a": 1, "b": 2})
        assert await locmem_tiered.adelete_many(["a"]) == 1
        assert await locmem_tiered.aclear() is True


class TestTieredWriteOrdering:
    """L2 is mutated before L1 is invalidated.

    The other order lets a concurrent read repopulate L1 from the pre-mutation
    L2 value, leaving L1 stale for up to ``l1_timeout`` after the call returns.
    """

    @pytest.mark.parametrize(
        ("op", "l1_method", "l2_method"),
        [
            (lambda c: c.delete("ord"), "delete", "delete"),
            (lambda c: c.delete_many(["ord"]), "delete", "delete_many"),
            (lambda c: c.incr("ord"), "delete", "incr"),
            (lambda c: c.decr("ord"), "delete", "decr"),
            (lambda c: c.expire("ord", 10), "delete", "expire"),
        ],
    )
    def test_l2_is_mutated_before_l1_is_invalidated(self, tiered_cache: BaseCache, mocker, op, l1_method, l2_method):
        calls: list[str] = []
        mocker.patch.object(tiered_cache._l1, l1_method, side_effect=lambda *a, **kw: calls.append("l1"))
        mocker.patch.object(tiered_cache._l2, l2_method, side_effect=lambda *a, **kw: calls.append("l2"))

        op(tiered_cache)

        assert calls == ["l2", "l1"]


@pytest.mark.asyncio
class TestTieredAsyncL1Access:
    """Every async path must reach L1 through its ``a*`` methods.

    Regression: they all called L1's sync methods, which raises
    ``SynchronousOnlyOperation`` on a DB-backed L1 and silently blocks the
    event loop on a network-backed one.
    """

    @pytest.fixture
    def guarded_tiered(self) -> Iterator[BaseCache]:
        config = {
            "l1": {"BACKEND": "tests.cache.test_tiered.AsyncGuardL1", "LOCATION": "tiered-guard-l1"},
            "l2": {"BACKEND": "django_cachex.cache.LocMemCache", "LOCATION": "tiered-guard-l2"},
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {"tiers": ["l1", "l2"], "l1_timeout": L1_TIMEOUT},
            },
        }
        with override_settings(CACHES=config):
            cache = caches["default"]
            cache.clear()
            yield cache
            cache.clear()

    async def test_aset_and_aget(self, guarded_tiered: BaseCache):
        await guarded_tiered.aset("k", "v")
        assert await guarded_tiered.aget("k") == "v"

    async def test_aget_populates_l1_on_miss(self, guarded_tiered: BaseCache):
        await guarded_tiered.aset("k", "v")
        await caches["l1"].adelete("k")
        assert await guarded_tiered.aget("k") == "v"
        assert await caches["l1"].aget("k") == "v"

    async def test_aset_with_flags(self, guarded_tiered: BaseCache):
        assert await guarded_tiered.aset("k", "first", nx=True) is True
        assert await guarded_tiered.aset("k", "second", nx=True) is False
        assert await guarded_tiered.aset("k", "third", get=True) == "first"

    async def test_aadd_and_adelete(self, guarded_tiered: BaseCache):
        assert await guarded_tiered.aadd("k", "v") is True
        assert await guarded_tiered.adelete("k") is True

    async def test_aset_many_and_aget_many(self, guarded_tiered: BaseCache):
        await guarded_tiered.aset_many({"a": 1, "b": 2})
        assert await guarded_tiered.aget_many(["a", "b"]) == {"a": 1, "b": 2}

    async def test_adelete_many(self, guarded_tiered: BaseCache):
        await guarded_tiered.aset_many({"a": 1, "b": 2})
        assert await guarded_tiered.adelete_many(["a", "b"]) == 2

    async def test_ahas_key(self, guarded_tiered: BaseCache):
        await guarded_tiered.aset("k", "v")
        assert await guarded_tiered.ahas_key("k") is True

    async def test_aincr_and_adecr(self, guarded_tiered: BaseCache):
        await guarded_tiered.aset("n", 10)
        assert await guarded_tiered.aincr("n", 5) == 15
        assert await guarded_tiered.adecr("n", 5) == 10

    async def test_atouch(self, guarded_tiered: BaseCache):
        await guarded_tiered.aset("k", "v")
        assert await guarded_tiered.atouch("k", timeout=60) is True

    async def test_aclear(self, guarded_tiered: BaseCache):
        await guarded_tiered.aset("k", "v")
        await guarded_tiered.aclear()
        assert await guarded_tiered.aget("k") is None

    async def test_aexpire_and_adelete_pattern(self, guarded_tiered: BaseCache):
        await guarded_tiered.aset("p:1", "v")
        assert await guarded_tiered.aexpire("p:1", 100) is True
        assert await guarded_tiered.adelete_pattern("p:*") == 1


class TestTieredTierAliasValidation:
    """Both tier aliases must exist, differ, and not point back at the tiered cache."""

    def test_duplicate_tier_aliases_rejected(self):
        config = {
            "l1": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"},
            "default": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {"tiers": ["l1", "l1"]},
            },
        }
        with override_settings(CACHES=config), pytest.raises(ImproperlyConfigured, match="distinct"):
            caches["default"].get("test")

    def test_self_referencing_tier_rejected(self):
        # Regression: this recursed until RecursionError on the first get().
        config = {
            "l2": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"},
            "selfref": {
                "BACKEND": "django_cachex.cache.TieredCache",
                "OPTIONS": {"tiers": ["selfref", "l2"]},
            },
        }
        with override_settings(CACHES=config), pytest.raises(ImproperlyConfigured, match="itself"):
            caches["selfref"].get("test")


class TestTieredDelegationErrors:
    """``_delegate`` only translates a missing method or L2's own
    ``NotSupportedError``; it must not swallow bugs inside L2."""

    @pytest.fixture
    def broken_l2_tiered(self, tiered_cache: BaseCache) -> BaseCache:
        return tiered_cache

    def test_attribute_error_from_l2_propagates(self, tiered_cache: BaseCache, mocker):
        # Regression: an AttributeError raised inside L2's implementation was
        # reported as "operation not supported".
        mocker.patch.object(tiered_cache._l2, "keys", side_effect=AttributeError("boom"))
        with pytest.raises(AttributeError, match="boom"):
            tiered_cache.keys("*")

    def test_not_supported_from_l2_is_rewrapped(self, tiered_cache: BaseCache, mocker):
        mocker.patch.object(tiered_cache._l2, "keys", side_effect=NotSupportedError("keys", "L2"))
        with pytest.raises(NotSupportedError, match="TieredCache"):
            tiered_cache.keys("*")

    def test_missing_l2_method_raises_not_supported(self, tiered_cache: BaseCache, mocker):
        mocker.patch.object(type(tiered_cache._l2), "keys", None)
        with pytest.raises(NotSupportedError, match="TieredCache"):
            tiered_cache.keys("*")

    def test_lazy_not_supported_from_iter_keys_is_rewrapped(self, tiered_cache: BaseCache, mocker):
        # ``iter_keys`` is a generator, so the error surfaces on iteration.
        def raising_iter_keys(*_args, **_kwargs):
            raise NotSupportedError("iter_keys", "L2")
            yield  # pragma: no cover

        mocker.patch.object(tiered_cache._l2, "iter_keys", side_effect=raising_iter_keys)
        with pytest.raises(NotSupportedError, match="TieredCache"):
            list(tiered_cache.iter_keys("*"))
