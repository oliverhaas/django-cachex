"""Tests for timeout and TTL operations."""

import datetime
import time
from datetime import timedelta

import pytest

from django_cachex.cache import KeyValueCache


class TestSetWithTimeout:
    """Tests for set() with timeout parameter."""

    def test_set_nx_with_expiration(self, cache: KeyValueCache):
        cache.delete("expiring_nx")
        result = cache.set("expiring_nx", "temporary", timeout=2, nx=True)
        assert result is True
        time.sleep(3)
        assert cache.get("expiring_nx") is None

    def test_set_nx_does_not_change_existing_ttl(self, cache: KeyValueCache):
        cache.set("existing_ttl", "original")
        cache.set("existing_ttl", "new_value", timeout=2, nx=True)
        # nx=True should fail, so original value remains without short TTL
        time.sleep(3)
        assert cache.get("existing_ttl") == "original"

    def test_key_expires_after_timeout(self, cache: KeyValueCache):
        cache.set("expires_soon", "temp_data", timeout=3)
        time.sleep(4)
        assert cache.get("expires_soon") is None

    def test_zero_timeout_immediate_expiration(self, cache: KeyValueCache):
        cache.set("instant_expire", "gone", timeout=0)
        assert cache.get("instant_expire") is None

    def test_timeout_as_positional_arg(self, cache: KeyValueCache):
        cache.set("pos_timeout", 999, -1)
        assert cache.get("pos_timeout") is None

        cache.set("pos_timeout2", 888, 1)
        assert cache.get("pos_timeout2") == 888
        time.sleep(2)
        assert cache.get("pos_timeout2") is None

    def test_timeout_positional_with_nx(self, cache: KeyValueCache):
        cache.set("nx_pos", "value", None)
        cache.set("nx_pos", "changed", -1, nx=True)
        assert cache.get("nx_pos") == "value"

    def test_negative_timeout_deletes_key(self, cache: KeyValueCache):
        cache.set("neg_timeout", "value", timeout=-1)
        assert cache.get("neg_timeout") is None

    def test_negative_timeout_on_existing_key(self, cache: KeyValueCache):
        cache.set("to_delete", "value", timeout=None)
        cache.set("to_delete", "value", timeout=-1)
        assert cache.get("to_delete") is None

    def test_negative_timeout_with_nx_preserves_existing(self, cache: KeyValueCache):
        cache.set("preserve_me", "original", timeout=None)
        cache.set("preserve_me", "changed", timeout=-1, nx=True)
        assert cache.get("preserve_me") == "original"

    def test_very_small_timeout(self, cache: KeyValueCache):
        cache.set("tiny_ttl", "value", timeout=0.00001)
        # Either already expired or still present (race condition)
        result = cache.get("tiny_ttl")
        assert result in (None, "value")


class TestTTLOperations:
    """Tests for ttl() method."""

    def test_ttl_returns_remaining_seconds(self, cache: KeyValueCache):
        cache.set("ttl_check", "data", 10)
        ttl = cache.ttl("ttl_check")
        assert pytest.approx(ttl) == 10

    def test_ttl_returns_none_for_persistent_key(self, cache: KeyValueCache):
        cache.set("no_expiry", "permanent", timeout=None)
        assert cache.ttl("no_expiry") is None

    def test_ttl_negative_for_deleted_key(self, cache: KeyValueCache):
        # Setting with timeout=-1 immediately deletes the key
        cache.set("already_gone", "value", timeout=-1)
        # Key doesn't exist, so ttl returns -2 (Redis convention)
        ttl = cache.ttl("already_gone")
        assert ttl < 0  # -2 means key doesn't exist

    def test_ttl_negative_for_nonexistent_key(self, cache: KeyValueCache):
        # Missing keys return -2 (Redis convention)
        ttl = cache.ttl("key_does_not_exist")
        assert ttl < 0


class TestPTTLOperations:
    """Tests for pttl() method (millisecond precision)."""

    def test_pttl_returns_milliseconds(self, cache: KeyValueCache):
        cache.set("pttl_key", "data", 10)
        pttl = cache.pttl("pttl_key")
        assert pytest.approx(pttl, 10) == 10000

    def test_pttl_with_fractional_timeout(self, cache: KeyValueCache):
        cache.set("half_sec", "data", 5.5)
        pttl = cache.pttl("half_sec")
        assert pytest.approx(pttl, 10) == 5500

    def test_pttl_returns_none_for_persistent(self, cache: KeyValueCache):
        cache.set("pttl_persist", "data", timeout=None)
        assert cache.pttl("pttl_persist") is None

    def test_pttl_negative_for_deleted_key(self, cache: KeyValueCache):
        # Setting with timeout=-1 immediately deletes the key
        cache.set("pttl_expired", "data", timeout=-1)
        pttl = cache.pttl("pttl_expired")
        assert pttl < 0  # -2 means key doesn't exist

    def test_pttl_negative_for_nonexistent_key(self, cache: KeyValueCache):
        pttl = cache.pttl("pttl_missing")
        assert pttl < 0  # Missing keys return -2


class TestPersistOperation:
    """Tests for persist() method."""

    def test_persist_removes_expiration(self, cache: KeyValueCache):
        cache.set("will_persist", "data", timeout=20)
        assert cache.persist("will_persist") is True
        assert cache.ttl("will_persist") is None

    def test_persist_on_missing_key_returns_false(self, cache: KeyValueCache):
        assert cache.persist("never_existed") is False


class TestExpireOperation:
    """Tests for expire() method."""

    def test_expire_sets_ttl(self, cache: KeyValueCache):
        cache.set("to_expire", "data", timeout=None)
        assert cache.expire("to_expire", 20) is True
        ttl = cache.ttl("to_expire")
        assert pytest.approx(ttl) == 20

    def test_expire_on_missing_key_returns_false(self, cache: KeyValueCache):
        assert cache.expire("ghost_key", 20) is False

    def test_expire_with_cache_default_timeout(self, cache: KeyValueCache):
        # Use the cache's configured default timeout (300 seconds)
        cache.set("default_exp", "data", timeout=None)
        assert cache.expire("default_exp", 300) is True
        assert cache.expire("no_such_key", 300) is False


class TestPExpireOperation:
    """Tests for pexpire() method (millisecond precision)."""

    def test_pexpire_sets_millisecond_ttl(self, cache: KeyValueCache):
        cache.set("pexp_key", "data", timeout=None)
        assert cache.pexpire("pexp_key", 20500) is True
        pttl = cache.pttl("pexp_key")
        assert pytest.approx(pttl, 10) == 20500

    def test_pexpire_on_missing_key_returns_false(self, cache: KeyValueCache):
        assert cache.pexpire("pexp_ghost", 20500) is False

    def test_pexpire_with_millisecond_timeout(self, cache: KeyValueCache):
        # Use milliseconds (300000ms = 300 seconds)
        cache.set("pexp_default", "data", timeout=None)
        assert cache.pexpire("pexp_default", 300000) is True
        assert cache.pexpire("pexp_no_key", 300000) is False


class TestPExpireAtOperation:
    """Tests for pexpire_at() method."""

    def test_pexpire_at_with_datetime(self, cache: KeyValueCache):
        cache.set("pexp_at_dt", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=1)
        assert cache.pexpire_at("pexp_at_dt", future) is True
        pttl = cache.pttl("pexp_at_dt")
        assert pytest.approx(pttl, 10) == timedelta(hours=1).total_seconds()

    def test_pexpire_at_with_timestamp(self, cache: KeyValueCache):
        cache.set("pexp_at_ts", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=2)
        assert cache.pexpire_at("pexp_at_ts", int(future.timestamp() * 1000)) is True
        pttl = cache.pttl("pexp_at_ts")
        assert pytest.approx(pttl, 10) == timedelta(hours=2).total_seconds() * 1000

    def test_pexpire_at_past_time_deletes_key(self, cache: KeyValueCache):
        cache.set("pexp_at_past", "data", timeout=None)
        past = datetime.datetime.now() - timedelta(hours=2)
        assert cache.pexpire_at("pexp_at_past", past) is True
        assert cache.get("pexp_at_past") is None

    def test_pexpire_at_missing_key_returns_false(self, cache: KeyValueCache):
        future = datetime.datetime.now() + timedelta(hours=2)
        assert cache.pexpire_at("pexp_at_ghost", future) is False


class TestExpireAtOperation:
    """Tests for expire_at() method."""

    def test_expire_at_with_datetime(self, cache: KeyValueCache):
        cache.set("exp_at_dt", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=1)
        assert cache.expire_at("exp_at_dt", future) is True
        ttl = cache.ttl("exp_at_dt")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds()

    def test_expire_at_with_timestamp(self, cache: KeyValueCache):
        cache.set("exp_at_ts", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=2)
        assert cache.expire_at("exp_at_ts", int(future.timestamp())) is True
        ttl = cache.ttl("exp_at_ts")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds() * 2

    def test_expire_at_past_time_deletes_key(self, cache: KeyValueCache):
        cache.set("exp_at_past", "data", timeout=None)
        past = datetime.datetime.now() - timedelta(hours=2)
        assert cache.expire_at("exp_at_past", past) is True
        assert cache.get("exp_at_past") is None

    def test_expire_at_missing_key_returns_false(self, cache: KeyValueCache):
        future = datetime.datetime.now() + timedelta(hours=2)
        assert cache.expire_at("exp_at_ghost", future) is False


class TestTouchOperation:
    """Tests for touch() method."""

    def test_touch_with_zero_timeout_deletes(self, cache: KeyValueCache):
        cache.set("touch_zero", "data", timeout=10)
        assert cache.touch("touch_zero", 0) is True
        assert cache.get("touch_zero") is None

    def test_touch_resets_expiration(self, cache: KeyValueCache):
        cache.set("touch_reset", "data", timeout=10)
        assert cache.touch("touch_reset", 2) is True
        assert cache.get("touch_reset") == "data"
        time.sleep(3)
        assert cache.get("touch_reset") is None

    def test_touch_negative_timeout_deletes(self, cache: KeyValueCache):
        cache.set("touch_neg", "data", timeout=10)
        assert cache.touch("touch_neg", -1) is True
        assert cache.get("touch_neg") is None

    def test_touch_missing_key_returns_false(self, cache: KeyValueCache):
        assert cache.touch("touch_ghost", 1) is False

    def test_touch_none_makes_persistent(self, cache: KeyValueCache):
        cache.set("touch_persist", "data", timeout=1)
        assert cache.touch("touch_persist", None) is True
        assert cache.ttl("touch_persist") is None
        time.sleep(2)
        assert cache.get("touch_persist") == "data"

    def test_touch_none_on_missing_key_returns_false(self, cache: KeyValueCache):
        assert cache.touch("touch_persist_ghost", None) is False

    def test_touch_without_timeout_uses_default(self, cache: KeyValueCache):
        cache.set("touch_default", "data", timeout=1)
        assert cache.touch("touch_default") is True
        time.sleep(2)
        assert cache.get("touch_default") == "data"
