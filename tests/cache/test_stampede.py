"""Tests for cache stampede prevention via XFetch algorithm."""

import time

from django_cachex.cache import KeyValueCache
from django_cachex.stampede import (
    _HEADER_SIZE,
    _HEADER_STRUCT,
    ENVELOPE_MARKER,
    StampedeConfig,
    get_recomputation_delta,
    record_recomputation_start,
    unwrap_envelope,
    wrap_envelope,
)

# =============================================================================
# Unit tests for stampede module (no Redis needed)
# =============================================================================


class TestEnvelopeFormat:
    """Tests for the binary envelope format."""

    def test_wrap_creates_envelope_with_marker(self):
        config = StampedeConfig()
        result = wrap_envelope(b"hello", 300, config)
        assert result.startswith(ENVELOPE_MARKER)

    def test_wrap_has_correct_header_size(self):
        config = StampedeConfig()
        result = wrap_envelope(b"hello", 300, config)
        assert len(result) == _HEADER_SIZE + 5  # 16 + len("hello")

    def test_wrap_preserves_value_bytes(self):
        config = StampedeConfig()
        value = b"test_value_bytes"
        result = wrap_envelope(value, 300, config)
        assert result[_HEADER_SIZE:] == value

    def test_wrap_stores_logical_expiry(self):
        config = StampedeConfig()
        before_ms = int(time.time() * 1000)
        result = wrap_envelope(b"v", 300, config)
        after_ms = int(time.time() * 1000)

        expiry_ms, _delta_ms = _HEADER_STRUCT.unpack(result[4:_HEADER_SIZE])
        expected_min = before_ms + 300_000
        expected_max = after_ms + 300_000
        assert expected_min <= expiry_ms <= expected_max

    def test_wrap_stores_default_delta(self):
        config = StampedeConfig(default_delta=2.5)
        result = wrap_envelope(b"v", 300, config)

        _expiry_ms, delta_ms = _HEADER_STRUCT.unpack(result[4:_HEADER_SIZE])
        assert delta_ms == 2500

    def test_wrap_stores_custom_delta(self):
        config = StampedeConfig()
        result = wrap_envelope(b"v", 300, config, delta_seconds=0.5)

        _expiry_ms, delta_ms = _HEADER_STRUCT.unpack(result[4:_HEADER_SIZE])
        assert delta_ms == 500

    def test_unwrap_non_envelope_returns_unchanged(self):
        config = StampedeConfig()
        raw = b"\x80\x05some_pickle_data"
        value, should_recompute = unwrap_envelope(raw, config)
        assert value == raw
        assert should_recompute is False

    def test_unwrap_fresh_value_not_stale(self):
        config = StampedeConfig()
        envelope = wrap_envelope(b"value", 300, config)

        value, should_recompute = unwrap_envelope(envelope, config)
        assert value == b"value"
        assert should_recompute is False

    def test_unwrap_expired_value_is_stale(self):
        """Value past logical expiry should trigger recompute."""
        config = StampedeConfig()
        # Create an envelope that expired 1 second ago
        now_ms = int(time.time() * 1000)
        expired_expiry_ms = now_ms - 1000
        header = _HEADER_STRUCT.pack(expired_expiry_ms, 1000)
        envelope = ENVELOPE_MARKER + header + b"stale"

        value, should_recompute = unwrap_envelope(envelope, config)
        assert value == b"stale"
        assert should_recompute is True

    def test_unwrap_truncated_envelope_returns_raw(self):
        """Corrupted/truncated envelope is treated as regular value."""
        config = StampedeConfig()
        truncated = ENVELOPE_MARKER + b"\x00"  # Too short
        value, should_recompute = unwrap_envelope(truncated, config)
        assert value == truncated
        assert should_recompute is False


class TestXFetchAlgorithm:
    """Tests for the XFetch probabilistic early expiration."""

    def test_xfetch_never_triggers_for_fresh_values(self):
        """With 5 minutes remaining, XFetch should almost never trigger."""
        config = StampedeConfig(default_delta=1.0, beta=1.0)
        envelope = wrap_envelope(b"v", 300, config, delta_seconds=1.0)

        # Run 1000 trials - with 300s remaining and 1s delta, should never trigger
        triggers = 0
        for _ in range(1000):
            _, should = unwrap_envelope(envelope, config)
            if should:
                triggers += 1
        assert triggers == 0

    def test_xfetch_likely_triggers_near_expiry(self):
        """With very little time remaining and large delta, XFetch should often trigger."""
        config = StampedeConfig(beta=1.0)
        # Create envelope expiring in 100ms with delta of 10 seconds
        now_ms = int(time.time() * 1000)
        near_expiry_ms = now_ms + 100
        header = _HEADER_STRUCT.pack(near_expiry_ms, 10_000)
        envelope = ENVELOPE_MARKER + header + b"v"

        triggers = 0
        for _ in range(100):
            _, should = unwrap_envelope(envelope, config)
            if should:
                triggers += 1
        # With 100ms remaining and 10s delta, should trigger most of the time
        assert triggers > 50

    def test_higher_beta_triggers_earlier(self):
        """Higher beta should increase the probability of early recomputation."""
        now_ms = int(time.time() * 1000)
        # 5 seconds remaining, 2 second delta
        expiry_ms = now_ms + 5000
        header = _HEADER_STRUCT.pack(expiry_ms, 2000)
        envelope = ENVELOPE_MARKER + header + b"v"

        low_beta = StampedeConfig(beta=0.5)
        high_beta = StampedeConfig(beta=5.0)

        low_triggers = sum(1 for _ in range(1000) if unwrap_envelope(envelope, low_beta)[1])
        high_triggers = sum(1 for _ in range(1000) if unwrap_envelope(envelope, high_beta)[1])

        assert high_triggers > low_triggers


class TestDeltaTracking:
    """Tests for recomputation time measurement via ContextVar."""

    def test_record_and_retrieve_delta(self):
        record_recomputation_start("key1")
        time.sleep(0.05)
        delta = get_recomputation_delta("key1")
        assert delta is not None
        assert 0.04 <= delta <= 0.2  # Allow some tolerance

    def test_delta_is_none_for_unrecorded_key(self):
        delta = get_recomputation_delta("never_recorded")
        assert delta is None

    def test_delta_is_consumed_on_retrieval(self):
        record_recomputation_start("key2")
        time.sleep(0.01)
        delta1 = get_recomputation_delta("key2")
        delta2 = get_recomputation_delta("key2")
        assert delta1 is not None
        assert delta2 is None

    def test_multiple_keys_tracked_independently(self):
        record_recomputation_start("key_a")
        time.sleep(0.01)
        record_recomputation_start("key_b")

        delta_a = get_recomputation_delta("key_a")
        delta_b = get_recomputation_delta("key_b")
        assert delta_a is not None
        assert delta_b is not None
        assert delta_a > delta_b  # key_a was recorded earlier


class TestStampedeConfig:
    """Tests for StampedeConfig dataclass."""

    def test_default_values(self):
        config = StampedeConfig()
        assert config.buffer == 60
        assert config.beta == 1.0
        assert config.default_delta == 1.0

    def test_custom_values(self):
        config = StampedeConfig(buffer=30, beta=2.0, default_delta=0.5)
        assert config.buffer == 30
        assert config.beta == 2.0
        assert config.default_delta == 0.5


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
    """Integers should bypass stampede envelope, keeping incr/decr working."""

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

    def test_no_timeout_no_envelope(self, stampede_cache: KeyValueCache):
        """Persistent keys (timeout=None) should not be wrapped."""
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


class TestStampedeBackwardCompat:
    """Test migration: values stored without stampede should be readable with it."""

    def test_non_envelope_value_decoded(self, stampede_cache: KeyValueCache):
        """Directly store a value bypassing stampede, then read with stampede enabled."""
        # Temporarily disable stampede to write a raw (non-envelope) value
        client = stampede_cache._cache
        orig_config = client._stampede_config
        client._stampede_config = None

        stampede_cache.set("sp_compat", "raw_value", timeout=300)

        # Re-enable stampede
        client._stampede_config = orig_config

        # Should still decode correctly (no marker prefix → treated as raw value)
        assert stampede_cache.get("sp_compat") == "raw_value"


class TestStampedeEarlyRecompute:
    """Test that XFetch triggers early recomputation."""

    def test_returns_none_after_logical_expiry(self, stampede_cache: KeyValueCache):
        """After logical expiry, get() returns None even though key is still in Redis."""
        stampede_cache.set("sp_expire", "val", timeout=1)
        # Value is fresh immediately
        assert stampede_cache.get("sp_expire") == "val"

        # Wait for logical expiry (1 second)
        time.sleep(1.1)

        # Key still exists in Redis (TTL was 1 + 60 buffer), but logically expired
        assert stampede_cache.get("sp_expire") is None

    def test_get_or_set_recomputes_after_expiry(self, stampede_cache: KeyValueCache):
        """get_or_set() should trigger recomputation after logical expiry."""
        stampede_cache.set("sp_gos", "old", timeout=1)
        assert stampede_cache.get("sp_gos") == "old"

        time.sleep(1.1)

        # get_or_set should see None, call the default callable, and set new value
        result = stampede_cache.get_or_set("sp_gos", lambda: "recomputed", timeout=300)
        assert result == "recomputed"

    def test_delta_measurement_flow(self, stampede_cache: KeyValueCache):
        """Full flow: get triggers recompute, set measures delta."""
        stampede_cache.set("sp_delta", "initial", timeout=1)
        time.sleep(1.1)

        # get() returns None and records recomputation start
        assert stampede_cache.get("sp_delta") is None

        # Simulate recomputation taking 50ms
        time.sleep(0.05)

        # set() should measure and store the delta
        stampede_cache.set("sp_delta", "recomputed", timeout=300)

        # Verify value is correct
        assert stampede_cache.get("sp_delta") == "recomputed"

        # Verify the TTL is extended
        ttl = stampede_cache.ttl("sp_delta")
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
        stampede_cache.set("sp_pipe_stale", "stale_val", timeout=1)
        time.sleep(1.1)

        # Pipeline should still return the value (stale serving)
        with stampede_cache.pipeline() as pipe:
            pipe.get("sp_pipe_stale")
            results = pipe.execute()

        assert results[0] == "stale_val"
