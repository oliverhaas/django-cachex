"""Tests for async timeout and TTL operations."""

import asyncio
import datetime
import time
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from django_cachex.cache import KeyValueCache


class TestAsyncSetWithTimeout:
    """Tests for aset() with timeout parameter."""

    @pytest.mark.asyncio
    async def test_aset_nx_with_expiration(self, cache: KeyValueCache):
        await cache.adelete("aexpiring_nx")
        result = await cache.aset("aexpiring_nx", "temporary", timeout=2, nx=True)
        assert result is True
        await asyncio.sleep(3)
        assert await cache.aget("aexpiring_nx") is None

    @pytest.mark.asyncio
    async def test_aset_nx_does_not_change_existing_ttl(self, cache: KeyValueCache):
        await cache.aset("aexisting_ttl", "original")
        await cache.aset("aexisting_ttl", "new_value", timeout=2, nx=True)
        await asyncio.sleep(3)
        assert await cache.aget("aexisting_ttl") == "original"

    @pytest.mark.asyncio
    async def test_akey_expires_after_timeout(self, cache: KeyValueCache):
        await cache.aset("aexpires_soon", "temp_data", timeout=3)
        await asyncio.sleep(4)
        assert await cache.aget("aexpires_soon") is None

    @pytest.mark.asyncio
    async def test_azero_timeout_immediate_expiration(self, cache: KeyValueCache):
        await cache.aset("ainstant_expire", "gone", timeout=0)
        assert await cache.aget("ainstant_expire") is None

    @pytest.mark.asyncio
    async def test_atimeout_as_positional_arg(self, cache: KeyValueCache):
        await cache.aset("apos_timeout", 999, -1)
        assert await cache.aget("apos_timeout") is None

        await cache.aset("apos_timeout2", 888, 1)
        assert await cache.aget("apos_timeout2") == 888
        await asyncio.sleep(2)
        assert await cache.aget("apos_timeout2") is None

    @pytest.mark.asyncio
    async def test_atimeout_positional_with_nx(self, cache: KeyValueCache):
        await cache.aset("anx_pos", "value", None)
        await cache.aset("anx_pos", "changed", -1, nx=True)
        assert await cache.aget("anx_pos") == "value"

    @pytest.mark.asyncio
    async def test_anegative_timeout_deletes_key(self, cache: KeyValueCache):
        await cache.aset("aneg_timeout", "value", timeout=-1)
        assert await cache.aget("aneg_timeout") is None

    @pytest.mark.asyncio
    async def test_anegative_timeout_on_existing_key(self, cache: KeyValueCache):
        await cache.aset("ato_delete", "value", timeout=None)
        await cache.aset("ato_delete", "value", timeout=-1)
        assert await cache.aget("ato_delete") is None

    @pytest.mark.asyncio
    async def test_anegative_timeout_with_nx_preserves_existing(self, cache: KeyValueCache):
        await cache.aset("apreserve_me", "original", timeout=None)
        await cache.aset("apreserve_me", "changed", timeout=-1, nx=True)
        assert await cache.aget("apreserve_me") == "original"

    @pytest.mark.asyncio
    async def test_asubsecond_float_timeout_is_accepted(self, cache: KeyValueCache):
        await cache.aset("atiny_ttl", "value", timeout=0.00001)


class TestAsyncTTLNegative:
    """Tests for the -2 (missing) / -1 (no expiry) TTL conventions on async."""

    @pytest.mark.asyncio
    async def test_attl_negative_for_deleted_key(self, cache: KeyValueCache):
        await cache.aset("aalready_gone", "value", timeout=-1)
        ttl = await cache.attl("aalready_gone")
        assert ttl is not None and ttl < 0


class TestAsyncPTTLEdgeCases:
    """Tests for apttl() edge cases (fractional / deleted)."""

    @pytest.mark.asyncio
    async def test_apttl_with_fractional_timeout(self, cache: KeyValueCache):
        await cache.aset("ahalf_sec", "data", 5.5)
        pttl = await cache.apttl("ahalf_sec")
        assert pytest.approx(pttl, 10) == 5500

    @pytest.mark.asyncio
    async def test_apttl_negative_for_deleted_key(self, cache: KeyValueCache):
        await cache.aset("apttl_expired", "data", timeout=-1)
        pttl = await cache.apttl("apttl_expired")
        assert pttl is not None and pttl < 0


class TestAsyncExpireTimeFlow:
    """Tests for aexpiretime() observing aexpire_at()."""

    @pytest.mark.asyncio
    async def test_aexpiretime_after_expire_at(self, cache: KeyValueCache):
        await cache.aset("aet_at", "data", timeout=None)
        target = int(time.time()) + 7200
        await cache.aexpire_at("aet_at", target)
        result = await cache.aexpiretime("aet_at")
        assert result is not None
        assert abs(result - target) < 2


class TestAsyncExpireDefaults:
    """Tests for aexpire() against the cache's default timeout."""

    @pytest.mark.asyncio
    async def test_aexpire_with_cache_default_timeout(self, cache: KeyValueCache):
        await cache.aset("adefault_exp", "data", timeout=None)
        assert await cache.aexpire("adefault_exp", 300) is True
        assert await cache.aexpire("ano_such_key", 300) is False


class TestAsyncPExpireDefaults:
    """Tests for apexpire() with millisecond input."""

    @pytest.mark.asyncio
    async def test_apexpire_with_millisecond_timeout(self, cache: KeyValueCache):
        await cache.aset("apexp_default", "data", timeout=None)
        assert await cache.apexpire("apexp_default", 300000) is True
        assert await cache.apexpire("apexp_no_key", 300000) is False


class TestAsyncExpireAtPast:
    """Tests for aexpire_at() with past timestamps and timestamp argument."""

    @pytest.mark.asyncio
    async def test_aexpire_at_with_timestamp(self, cache: KeyValueCache):
        await cache.aset("aexp_at_ts", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=2)
        assert await cache.aexpire_at("aexp_at_ts", int(future.timestamp())) is True
        ttl = await cache.attl("aexp_at_ts")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds() * 2

    @pytest.mark.asyncio
    async def test_aexpire_at_past_time_deletes_key(self, cache: KeyValueCache):
        await cache.aset("aexp_at_past", "data", timeout=None)
        past = datetime.datetime.now() - timedelta(hours=2)
        assert await cache.aexpire_at("aexp_at_past", past) is True
        assert await cache.aget("aexp_at_past") is None


class TestAsyncPExpireAtPast:
    """Tests for apexpire_at() with past timestamps."""

    @pytest.mark.asyncio
    async def test_apexpire_at_past_time_deletes_key(self, cache: KeyValueCache):
        await cache.aset("apexp_at_past", "data", timeout=None)
        past = datetime.datetime.now() - timedelta(hours=2)
        assert await cache.apexpire_at("apexp_at_past", past) is True
        assert await cache.aget("apexp_at_past") is None


class TestAsyncTouchOperation:
    """Tests for atouch() with various timeout values."""

    @pytest.mark.asyncio
    async def test_atouch_with_zero_timeout_deletes(self, cache: KeyValueCache):
        await cache.aset("atouch_zero", "data", timeout=10)
        assert await cache.atouch("atouch_zero", 0) is True
        assert await cache.aget("atouch_zero") is None

    @pytest.mark.asyncio
    async def test_atouch_resets_expiration(self, cache: KeyValueCache):
        await cache.aset("atouch_reset", "data", timeout=10)
        assert await cache.atouch("atouch_reset", 2) is True
        assert await cache.aget("atouch_reset") == "data"
        await asyncio.sleep(3)
        assert await cache.aget("atouch_reset") is None

    @pytest.mark.asyncio
    async def test_atouch_negative_timeout_deletes(self, cache: KeyValueCache):
        await cache.aset("atouch_neg", "data", timeout=10)
        assert await cache.atouch("atouch_neg", -1) is True
        assert await cache.aget("atouch_neg") is None

    @pytest.mark.asyncio
    async def test_atouch_missing_key_returns_false(self, cache: KeyValueCache):
        assert await cache.atouch("atouch_ghost", 1) is False

    @pytest.mark.asyncio
    async def test_atouch_none_makes_persistent(self, cache: KeyValueCache):
        await cache.aset("atouch_persist", "data", timeout=1)
        assert await cache.atouch("atouch_persist", None) is True
        assert await cache.attl("atouch_persist") is None
        await asyncio.sleep(2)
        assert await cache.aget("atouch_persist") == "data"

    @pytest.mark.asyncio
    async def test_atouch_none_on_missing_key_returns_false(self, cache: KeyValueCache):
        assert await cache.atouch("atouch_persist_ghost", None) is False

    @pytest.mark.asyncio
    async def test_atouch_without_timeout_uses_default(self, cache: KeyValueCache):
        await cache.aset("atouch_default", "data", timeout=1)
        assert await cache.atouch("atouch_default") is True
        await asyncio.sleep(2)
        assert await cache.aget("atouch_default") == "data"


class TestAsyncTTL:
    """Tests for attl() method."""

    @pytest.mark.asyncio
    async def test_attl_returns_remaining_seconds(self, cache: KeyValueCache):
        cache.set("attl_check", "data", 10)
        ttl = await cache.attl("attl_check")
        assert pytest.approx(ttl) == 10

    @pytest.mark.asyncio
    async def test_attl_returns_none_for_persistent_key(self, cache: KeyValueCache):
        cache.set("attl_persist", "permanent", timeout=None)
        result = await cache.attl("attl_persist")
        assert result is None

    @pytest.mark.asyncio
    async def test_attl_negative_for_nonexistent_key(self, cache: KeyValueCache):
        ttl = await cache.attl("attl_missing")
        assert ttl is not None and ttl < 0


class TestAsyncPTTL:
    """Tests for apttl() method."""

    @pytest.mark.asyncio
    async def test_apttl_returns_milliseconds(self, cache: KeyValueCache):
        cache.set("apttl_key", "data", 10)
        pttl = await cache.apttl("apttl_key")
        assert pytest.approx(pttl, 10) == 10000

    @pytest.mark.asyncio
    async def test_apttl_returns_none_for_persistent(self, cache: KeyValueCache):
        cache.set("apttl_persist", "data", timeout=None)
        result = await cache.apttl("apttl_persist")
        assert result is None

    @pytest.mark.asyncio
    async def test_apttl_negative_for_nonexistent_key(self, cache: KeyValueCache):
        pttl = await cache.apttl("apttl_missing")
        assert pttl is not None and pttl < 0


class TestAsyncExpireTime:
    """Tests for aexpiretime() method (Redis 7.0+)."""

    @pytest.mark.asyncio
    async def test_aexpiretime_returns_unix_timestamp(self, cache: KeyValueCache):
        import time

        cache.set("aet_key", "data", timeout=3600)
        result = await cache.aexpiretime("aet_key")
        assert result is not None
        expected = int(time.time()) + 3600
        assert abs(result - expected) < 5

    @pytest.mark.asyncio
    async def test_aexpiretime_returns_none_for_persistent_key(self, cache: KeyValueCache):
        cache.set("aet_persist", "permanent", timeout=None)
        result = await cache.aexpiretime("aet_persist")
        assert result is None

    @pytest.mark.asyncio
    async def test_aexpiretime_negative_for_nonexistent_key(self, cache: KeyValueCache):
        result = await cache.aexpiretime("aet_missing")
        assert result is not None and result < 0


class TestAsyncExpire:
    """Tests for aexpire() method."""

    @pytest.mark.asyncio
    async def test_aexpire_sets_ttl(self, cache: KeyValueCache):
        cache.set("aexpire_key", "data", timeout=None)
        result = await cache.aexpire("aexpire_key", 20)
        assert result is True
        ttl = cache.ttl("aexpire_key")
        assert pytest.approx(ttl) == 20

    @pytest.mark.asyncio
    async def test_aexpire_on_missing_key_returns_false(self, cache: KeyValueCache):
        result = await cache.aexpire("aexpire_ghost", 20)
        assert result is False


class TestAsyncPExpire:
    """Tests for apexpire() method."""

    @pytest.mark.asyncio
    async def test_apexpire_sets_millisecond_ttl(self, cache: KeyValueCache):
        cache.set("apexpire_key", "data", timeout=None)
        result = await cache.apexpire("apexpire_key", 20500)
        assert result is True
        pttl = cache.pttl("apexpire_key")
        assert pytest.approx(pttl, 10) == 20500

    @pytest.mark.asyncio
    async def test_apexpire_on_missing_key_returns_false(self, cache: KeyValueCache):
        result = await cache.apexpire("apexpire_ghost", 20500)
        assert result is False


class TestAsyncExpireAt:
    """Tests for aexpire_at() method."""

    @pytest.mark.asyncio
    async def test_aexpire_at_with_datetime(self, cache: KeyValueCache):
        cache.set("aexpireat_dt", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=1)
        result = await cache.aexpire_at("aexpireat_dt", future)
        assert result is True
        ttl = cache.ttl("aexpireat_dt")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds()

    @pytest.mark.asyncio
    async def test_aexpire_at_missing_key_returns_false(self, cache: KeyValueCache):
        future = datetime.datetime.now() + timedelta(hours=1)
        result = await cache.aexpire_at("aexpireat_ghost", future)
        assert result is False


class TestAsyncPExpireAt:
    """Tests for apexpire_at() method."""

    @pytest.mark.asyncio
    async def test_apexpire_at_with_datetime(self, cache: KeyValueCache):
        cache.set("apexpireat_dt", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=1)
        result = await cache.apexpire_at("apexpireat_dt", future)
        assert result is True
        pttl = cache.pttl("apexpireat_dt")
        assert pytest.approx(pttl, 10) == timedelta(hours=1).total_seconds() * 1000

    @pytest.mark.asyncio
    async def test_apexpire_at_with_timestamp(self, cache: KeyValueCache):
        cache.set("apexpireat_ts", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=2)
        result = await cache.apexpire_at("apexpireat_ts", int(future.timestamp() * 1000))
        assert result is True
        pttl = cache.pttl("apexpireat_ts")
        assert pytest.approx(pttl, 10) == timedelta(hours=2).total_seconds() * 1000

    @pytest.mark.asyncio
    async def test_apexpire_at_missing_key_returns_false(self, cache: KeyValueCache):
        future = datetime.datetime.now() + timedelta(hours=2)
        result = await cache.apexpire_at("apexpireat_ghost", future)
        assert result is False


class TestAsyncPersist:
    """Tests for apersist() method."""

    @pytest.mark.asyncio
    async def test_apersist_removes_expiration(self, cache: KeyValueCache):
        cache.set("apersist_key", "data", timeout=20)
        result = await cache.apersist("apersist_key")
        assert result is True
        assert cache.ttl("apersist_key") is None

    @pytest.mark.asyncio
    async def test_apersist_on_missing_key_returns_false(self, cache: KeyValueCache):
        result = await cache.apersist("apersist_missing")
        assert result is False


class TestAsyncType:
    """Tests for atype() method."""

    @pytest.mark.asyncio
    async def test_atype_string(self, cache: KeyValueCache):
        cache.set("atype_str", "value")
        result = await cache.atype("atype_str")
        assert result == "string"

    @pytest.mark.asyncio
    async def test_atype_hash(self, cache: KeyValueCache):
        cache.hset("atype_hash", "field", "value")
        result = await cache.atype("atype_hash")
        assert result == "hash"

    @pytest.mark.asyncio
    async def test_atype_list(self, cache: KeyValueCache):
        cache.lpush("atype_list", "value")
        result = await cache.atype("atype_list")
        assert result == "list"

    @pytest.mark.asyncio
    async def test_atype_set(self, cache: KeyValueCache):
        cache.sadd("atype_set", "value")
        result = await cache.atype("atype_set")
        assert result == "set"

    @pytest.mark.asyncio
    async def test_atype_zset(self, cache: KeyValueCache):
        cache.zadd("atype_zset", {"member": 1.0})
        result = await cache.atype("atype_zset")
        assert result == "zset"

    @pytest.mark.asyncio
    async def test_atype_missing_key(self, cache: KeyValueCache):
        result = await cache.atype("atype_missing")
        assert result is None
