"""Tests for cache stampede prevention via XFetch algorithm (TTL-based)."""

from unittest.mock import patch

import pytest

from django_cachex.cache import KeyValueCache
from django_cachex.stampede import StampedeConfig, should_recompute

# =============================================================================
# Unit tests for stampede module (no Redis needed)
# =============================================================================


class TestShouldRecompute:
    """Tests for the should_recompute() XFetch function."""

    def test_fresh_value_no_recompute(self):
        """With plenty of TTL remaining, should never trigger."""
        config = StampedeConfig(buffer=60, delta=1.0, beta=1.0)
        # TTL = 350 → remaining = 350 - 60 = 290s
        triggers = sum(1 for _ in range(1000) if should_recompute(350, config))
        assert triggers == 0

    def test_expired_always_recomputes(self):
        """When TTL <= buffer, always triggers (logically expired)."""
        config = StampedeConfig(buffer=60, delta=1.0, beta=1.0)
        # TTL = 50 → remaining = 50 - 60 = -10 → always True
        assert should_recompute(50, config) is True
        assert should_recompute(60, config) is True
        assert should_recompute(0, config) is True

    def test_near_expiry_likely_triggers(self):
        """With very little logical time remaining and large delta, triggers often."""
        config = StampedeConfig(buffer=60, delta=10.0, beta=1.0)
        # TTL = 61 → remaining = 1s, delta = 10s → very likely
        triggers = sum(1 for _ in range(100) if should_recompute(61, config))
        assert triggers > 50

    def test_higher_beta_triggers_more(self):
        """Higher beta increases recomputation probability."""
        low_beta = StampedeConfig(buffer=60, delta=2.0, beta=0.5)
        high_beta = StampedeConfig(buffer=60, delta=2.0, beta=5.0)
        # TTL = 65 → remaining = 5s
        low = sum(1 for _ in range(1000) if should_recompute(65, low_beta))
        high = sum(1 for _ in range(1000) if should_recompute(65, high_beta))
        assert high > low

    def test_zero_delta_never_triggers_early(self):
        """With delta=0, only triggers when logically expired."""
        config = StampedeConfig(buffer=60, delta=0.0, beta=1.0)
        # TTL = 65 → remaining = 5s, but delta=0 means no probabilistic trigger
        triggers = sum(1 for _ in range(1000) if should_recompute(65, config))
        assert triggers == 0
        # But when logically expired, still triggers
        assert should_recompute(60, config) is True


class TestStampedeConfig:
    """Tests for StampedeConfig dataclass."""

    def test_default_values(self):
        config = StampedeConfig()
        assert config.buffer == 60
        assert config.beta == 1.0
        assert config.delta == 1.0

    def test_custom_values(self):
        config = StampedeConfig(buffer=30, beta=2.0, delta=0.5)
        assert config.buffer == 30
        assert config.beta == 2.0
        assert config.delta == 0.5


# =============================================================================
# Integration tests (require Redis)
# =============================================================================


class TestStampedeBasicOperations:
    """Basic set/get operations with stampede prevention enabled."""

    def test_set_and_get(self, stampede_cache: KeyValueCache):
        stampede_cache.set("sp_basic", "hello", timeout=300)
        assert stampede_cache.get("sp_basic") == "hello"

    def test_set_and_get_dict(self, stampede_cache: KeyValueCache):
        stampede_cache.set("sp_dict", {"a": 1, "b": [2, 3]}, timeout=300)
        assert stampede_cache.get("sp_dict") == {"a": 1, "b": [2, 3]}

    def test_set_and_get_list(self, stampede_cache: KeyValueCache):
        stampede_cache.set("sp_list", [1, "two", 3.0], timeout=300)
        assert stampede_cache.get("sp_list") == [1, "two", 3.0]

    def test_get_missing_key(self, stampede_cache: KeyValueCache):
        stampede_cache.delete("sp_missing")
        assert stampede_cache.get("sp_missing") is None

    def test_get_with_default(self, stampede_cache: KeyValueCache):
        stampede_cache.delete("sp_default")
        assert stampede_cache.get("sp_default", "fallback") == "fallback"

    def test_delete(self, stampede_cache: KeyValueCache):
        stampede_cache.set("sp_del", "val", timeout=300)
        assert stampede_cache.delete("sp_del") is True
        assert stampede_cache.get("sp_del") is None

    def test_add_new_key(self, stampede_cache: KeyValueCache):
        stampede_cache.delete("sp_add")
        assert stampede_cache.add("sp_add", "first", timeout=300) is True
        assert stampede_cache.get("sp_add") == "first"

    def test_add_existing_key(self, stampede_cache: KeyValueCache):
        stampede_cache.set("sp_add_exists", "original", timeout=300)
        assert stampede_cache.add("sp_add_exists", "new", timeout=300) is False
        assert stampede_cache.get("sp_add_exists") == "original"


class TestStampedeIntegerPassthrough:
    """Integers bypass stampede TTL check (int values parsed by decode, not bytes)."""

    def test_integer_set_get(self, stampede_cache: KeyValueCache):
        stampede_cache.set("sp_int", 42, timeout=300)
        assert stampede_cache.get("sp_int") == 42

    def test_incr(self, stampede_cache: KeyValueCache):
        stampede_cache.set("sp_incr", 10, timeout=300)
        result = stampede_cache.incr("sp_incr")
        assert result == 11

    def test_decr(self, stampede_cache: KeyValueCache):
        stampede_cache.set("sp_decr", 10, timeout=300)
        result = stampede_cache.decr("sp_decr")
        assert result == 9


class TestStampedeExtendedTTL:
    """Redis TTL should be extended by buffer amount."""

    def test_ttl_includes_buffer(self, stampede_cache: KeyValueCache):
        stampede_cache.set("sp_ttl", "val", timeout=300)
        ttl = stampede_cache.ttl("sp_ttl")
        # TTL should be between 300 and 360 (300 + 60 buffer)
        assert ttl is not None
        assert 300 < ttl <= 360

    def test_no_timeout_no_buffer(self, stampede_cache: KeyValueCache):
        """Persistent keys (timeout=None) should not have TTL extended."""
        stampede_cache.set("sp_persist", "val", timeout=None)
        ttl = stampede_cache.ttl("sp_persist")
        assert ttl is None  # No expiry


class TestStampedeGetMany:
    """Batch get operations with stampede prevention."""

    def test_get_many(self, stampede_cache: KeyValueCache):
        stampede_cache.set("sp_m1", "v1", timeout=300)
        stampede_cache.set("sp_m2", "v2", timeout=300)
        stampede_cache.delete("sp_m3")

        result = stampede_cache.get_many(["sp_m1", "sp_m2", "sp_m3"])
        assert "v1" in result.values()
        assert "v2" in result.values()
        assert len(result) == 2

    def test_get_many_filters_expired(self, stampede_cache: KeyValueCache):
        """get_many should filter out logically expired keys."""
        stampede_cache.set("sp_gm_exp", "val", timeout=300)
        # Shrink TTL below buffer → logically expired
        stampede_cache.expire("sp_gm_exp", 50)

        result = stampede_cache.get_many(["sp_gm_exp"])
        assert len(result) == 0


class TestStampedeSetMany:
    """Batch set operations with stampede prevention."""

    def test_set_many(self, stampede_cache: KeyValueCache):
        stampede_cache.set_many({"sp_sm1": "a", "sp_sm2": "b"}, timeout=300)
        assert stampede_cache.get("sp_sm1") == "a"
        assert stampede_cache.get("sp_sm2") == "b"

    def test_set_many_ttl_includes_buffer(self, stampede_cache: KeyValueCache):
        stampede_cache.set_many({"sp_sm_ttl": "val"}, timeout=300)
        ttl = stampede_cache.ttl("sp_sm_ttl")
        assert ttl is not None
        assert 300 < ttl <= 360


class TestStampedeEarlyRecompute:
    """Test that XFetch triggers early recomputation.

    Uses expire() to simulate logical expiry deterministically:
    set with timeout=300 (TTL=360), then expire(key, 50) → TTL=50 < buffer=60 → logically expired.
    """

    def test_returns_none_after_logical_expiry(self, stampede_cache: KeyValueCache):
        """After logical expiry, get() returns None even though key is still in Redis."""
        stampede_cache.set("sp_expire", "val", timeout=300)
        assert stampede_cache.get("sp_expire") == "val"

        # Shrink TTL below buffer → logically expired
        stampede_cache.expire("sp_expire", 50)
        assert stampede_cache.get("sp_expire") is None

    def test_get_or_set_recomputes_after_expiry(self, stampede_cache: KeyValueCache):
        """get_or_set() should trigger recomputation after logical expiry."""
        stampede_cache.set("sp_gos", "old", timeout=300)
        assert stampede_cache.get("sp_gos") == "old"

        # Shrink TTL below buffer → logically expired
        stampede_cache.expire("sp_gos", 50)

        # get_or_set should see None, call the default callable, and set new value
        result = stampede_cache.get_or_set("sp_gos", lambda: "recomputed", timeout=300)
        assert result == "recomputed"

    def test_recompute_stores_with_buffer(self, stampede_cache: KeyValueCache):
        """After recomputation, the new value should have buffered TTL."""
        stampede_cache.set("sp_recomp", "initial", timeout=300)
        stampede_cache.expire("sp_recomp", 50)

        # Recompute
        stampede_cache.set("sp_recomp", "recomputed", timeout=300)

        # Verify value and TTL
        assert stampede_cache.get("sp_recomp") == "recomputed"
        ttl = stampede_cache.ttl("sp_recomp")
        assert ttl is not None
        assert ttl > 300


class TestStampedePipeline:
    """Test pipeline operations with stampede prevention."""

    def test_pipeline_set_get(self, stampede_cache: KeyValueCache):
        with stampede_cache.pipeline() as pipe:
            pipe.set("sp_pipe1", "value1", timeout=300)
            pipe.set("sp_pipe2", "value2", timeout=300)
            pipe.get("sp_pipe1")
            pipe.get("sp_pipe2")
            results = pipe.execute()

        # SET results (True for success), then GET results
        assert results[2] == "value1"
        assert results[3] == "value2"

    def test_pipeline_serves_stale_data(self, stampede_cache: KeyValueCache):
        """Pipeline should serve stale data (not return None) during buffer window."""
        stampede_cache.set("sp_pipe_stale", "stale_val", timeout=300)
        # Shrink TTL below buffer → logically expired, but pipeline should still serve
        stampede_cache.expire("sp_pipe_stale", 50)

        with stampede_cache.pipeline() as pipe:
            pipe.get("sp_pipe_stale")
            results = pipe.execute()

        assert results[0] == "stale_val"


class TestStampedeOverride:
    """Test per-method stampede_prevention=True/False override."""

    def test_false_skips_ttl_check(self, stampede_cache: KeyValueCache):
        """stampede_prevention=False on get() should return value even if logically expired."""
        stampede_cache.set("sp_ovr_get", "val", timeout=300)
        stampede_cache.expire("sp_ovr_get", 50)  # logically expired

        # Default behavior: returns None (logically expired)
        assert stampede_cache.get("sp_ovr_get") is None
        # Override: skip stampede check → returns value
        assert stampede_cache.get("sp_ovr_get", stampede_prevention=False) == "val"

    def test_false_skips_buffer_on_set(self, stampede_cache: KeyValueCache):
        """stampede_prevention=False on set() should not add buffer to TTL."""
        stampede_cache.set("sp_ovr_set", "val", timeout=300, stampede_prevention=False)
        ttl = stampede_cache.ttl("sp_ovr_set")
        assert ttl is not None
        assert ttl <= 300  # No buffer added

    def test_false_on_get_many(self, stampede_cache: KeyValueCache):
        """stampede_prevention=False on get_many() should return logically expired values."""
        stampede_cache.set("sp_ovr_gm", "val", timeout=300)
        stampede_cache.expire("sp_ovr_gm", 50)  # logically expired

        # Default behavior: filtered out (logically expired)
        assert len(stampede_cache.get_many(["sp_ovr_gm"])) == 0
        # With stampede_prevention=False, value is returned despite logical expiry
        result = stampede_cache.get_many(["sp_ovr_gm"], stampede_prevention=False)
        assert result.get("sp_ovr_gm") == "val"

    def test_true_on_non_stampede_cache(self, cache: KeyValueCache):
        """stampede_prevention=True on a cache without global stampede should still add buffer."""
        cache.set("sp_ovr_force", "val", timeout=300, stampede_prevention=True)
        ttl = cache.ttl("sp_ovr_force")
        assert ttl is not None
        assert ttl > 300  # Buffer was added

    def test_dict_override_buffer(self, cache: KeyValueCache):
        """stampede_prevention=dict should override specific config values."""
        # Non-stampede cache with per-call dict override: buffer=120
        cache.set("sp_ovr_dict", "val", timeout=300, stampede_prevention={"buffer": 120})
        ttl = cache.ttl("sp_ovr_dict")
        assert ttl is not None
        assert 300 < ttl <= 420  # 300 + 120 buffer

    def test_dict_override_merges_with_instance(self, stampede_cache: KeyValueCache):
        """Dict override merges with instance config (buffer=60 default)."""
        # Instance has buffer=60, override only changes delta — buffer stays 60
        stampede_cache.set("sp_ovr_merge", "val", timeout=300, stampede_prevention={"delta": 5.0})
        ttl = stampede_cache.ttl("sp_ovr_merge")
        assert ttl is not None
        assert 300 < ttl <= 360  # buffer=60 inherited from instance config


# =============================================================================
# Robustness tests for XFetch algorithm
# =============================================================================


class TestShouldRecomputeEdgeCases:
    """Edge cases for the should_recompute XFetch function."""

    def test_extreme_random_values_do_not_crash(self):
        """should_recompute never raises regardless of RNG output.

        Regression: math.log(0.0) and math.log1p(-1.0) both raise ValueError.
        Using -expovariate(1.0) avoids domain errors entirely.
        """
        config = StampedeConfig(buffer=60, delta=1.0, beta=1.0)
        # Run many iterations — before the fix, one in ~9 quadrillion calls
        # would crash. With expovariate, it never crashes.
        for _ in range(10_000):
            result = should_recompute(350, config)
            assert isinstance(result, bool)

    def test_expovariate_edge_via_mock(self):
        """Mock expovariate to return extreme values and verify no crash."""
        config = StampedeConfig(buffer=60, delta=1.0, beta=1.0)

        # Very large expovariate value → large negative threshold → always triggers
        with patch("django_cachex.stampede.random.expovariate", return_value=1000.0):
            assert should_recompute(65, config) is True

        # Very small expovariate value → threshold near 0 → doesn't trigger on fresh key
        with patch("django_cachex.stampede.random.expovariate", return_value=0.001):
            assert should_recompute(350, config) is False


class TestStampedeGetOrSetRecompute:
    """Test that get_or_set correctly overwrites stale values during stampede."""

    def test_get_or_set_overwrites_stale_key(self, stampede_cache: KeyValueCache):
        """get_or_set must use set() (not add/NX) when stampede triggers, so
        the recomputed value actually replaces the stale one."""
        stampede_cache.set("sp_gos_overwrite", "stale", timeout=300)
        # Shrink TTL below buffer → logically expired
        stampede_cache.expire("sp_gos_overwrite", 50)

        result = stampede_cache.get_or_set(
            "sp_gos_overwrite",
            lambda: "fresh",
            timeout=300,
        )
        assert result == "fresh"
        # Verify the new value is actually stored (not just returned once)
        assert stampede_cache.get("sp_gos_overwrite") == "fresh"

    def test_get_or_set_returns_fresh_not_retrigger(self, stampede_cache: KeyValueCache):
        """After recomputation, get_or_set should return the fresh value, not
        re-trigger stampede on the confirmation get()."""
        stampede_cache.set("sp_gos_retrig", "stale", timeout=300)
        stampede_cache.expire("sp_gos_retrig", 50)

        # With short timeout, stampede could re-trigger on confirmation get
        # if stampede_prevention is passed to the final get(). It shouldn't be.
        result = stampede_cache.get_or_set(
            "sp_gos_retrig",
            lambda: "recomputed",
            timeout=300,
        )
        assert result == "recomputed"

    @pytest.mark.asyncio
    async def test_aget_or_set_overwrites_stale_key(self, stampede_cache: KeyValueCache):
        """Async get_or_set must also overwrite stale keys."""
        stampede_cache.set("sp_agos_overwrite", "stale", timeout=300)
        stampede_cache.expire("sp_agos_overwrite", 50)

        result = await stampede_cache.aget_or_set(
            "sp_agos_overwrite",
            lambda: "fresh_async",
            timeout=300,
        )
        assert result == "fresh_async"
        assert stampede_cache.get("sp_agos_overwrite") == "fresh_async"


class TestStampedeGetManyConsistency:
    """get_many() stampede behavior should match get() for various value types."""

    def test_get_many_filters_logically_expired_consistently(self, stampede_cache: KeyValueCache):
        """get_many() and get() should agree on which keys are logically expired."""
        stampede_cache.set("sp_gmc_str", "hello", timeout=300)
        stampede_cache.set("sp_gmc_int", 42, timeout=300)
        # Shrink TTL below buffer → logically expired
        stampede_cache.expire("sp_gmc_str", 50)
        stampede_cache.expire("sp_gmc_int", 50)

        # Both get() and get_many() should treat them as logically expired
        assert stampede_cache.get("sp_gmc_str") is None
        assert stampede_cache.get("sp_gmc_int") is None
        result = stampede_cache.get_many(["sp_gmc_str", "sp_gmc_int"])
        assert len(result) == 0

    def test_get_many_preserves_fresh_values(self, stampede_cache: KeyValueCache):
        """Fresh values (well above buffer) should never be filtered."""
        stampede_cache.set("sp_gmc_fresh1", "val", timeout=300)
        stampede_cache.set("sp_gmc_fresh2", 99, timeout=300)

        result = stampede_cache.get_many(["sp_gmc_fresh1", "sp_gmc_fresh2"])
        assert result["sp_gmc_fresh1"] == "val"
        assert result["sp_gmc_fresh2"] == 99
