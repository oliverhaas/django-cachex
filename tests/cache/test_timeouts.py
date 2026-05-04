"""Tests for timeout and TTL operations."""

import datetime
import time
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


class TestSetWithTimeout:
    """Tests for set() with timeout parameter."""

    def test_set_nx_with_expiration(self, cache: RespCache):
        cache.delete("expiring_nx")
        result = cache.set("expiring_nx", "temporary", timeout=2, nx=True)
        assert result is True
        cache.expire("expiring_nx", 0)
        assert cache.get("expiring_nx") is None

    def test_set_nx_does_not_change_existing_ttl(self, cache: RespCache):
        cache.set("existing_ttl", "original")
        original_ttl = cache.ttl("existing_ttl")
        cache.set("existing_ttl", "new_value", timeout=2, nx=True)
        assert cache.get("existing_ttl") == "original"
        new_ttl = cache.ttl("existing_ttl")
        if original_ttl is None:
            assert new_ttl is None
        else:
            assert new_ttl is not None and new_ttl > 5

    def test_key_expires_after_timeout(self, cache: RespCache):
        cache.set("expires_soon", "temp_data", timeout=3)
        cache.expire("expires_soon", 0)
        assert cache.get("expires_soon") is None

    def test_zero_timeout_immediate_expiration(self, cache: RespCache):
        cache.set("instant_expire", "gone", timeout=0)
        assert cache.get("instant_expire") is None

    def test_timeout_as_positional_arg(self, cache: RespCache):
        cache.set("pos_timeout", 999, -1)
        assert cache.get("pos_timeout") is None

        cache.set("pos_timeout2", 888, 1)
        assert cache.get("pos_timeout2") == 888
        cache.expire("pos_timeout2", 0)
        assert cache.get("pos_timeout2") is None

    def test_timeout_positional_with_nx(self, cache: RespCache):
        cache.set("nx_pos", "value", None)
        cache.set("nx_pos", "changed", -1, nx=True)
        assert cache.get("nx_pos") == "value"

    def test_negative_timeout_deletes_key(self, cache: RespCache):
        cache.set("neg_timeout", "value", timeout=-1)
        assert cache.get("neg_timeout") is None

    def test_negative_timeout_on_existing_key(self, cache: RespCache):
        cache.set("to_delete", "value", timeout=None)
        cache.set("to_delete", "value", timeout=-1)
        assert cache.get("to_delete") is None

    def test_negative_timeout_with_nx_preserves_existing(self, cache: RespCache):
        cache.set("preserve_me", "original", timeout=None)
        cache.set("preserve_me", "changed", timeout=-1, nx=True)
        assert cache.get("preserve_me") == "original"

    def test_subsecond_float_timeout_is_accepted(self, cache: RespCache):
        # Sub-second float timeout must be accepted (not raise). Whether the
        # key has already expired by the time we read is a race we don't
        # assert on; ``test_zero_timeout_immediate_expiration`` covers the
        # rounded-to-zero case.
        cache.set("tiny_ttl", "value", timeout=0.00001)


class TestTTLOperations:
    """Tests for ttl() / pttl() / expiretime() / persist()."""

    def test_ttl_returns_remaining_seconds(self, cache: RespCache):
        cache.set("ttl_check", "data", 10)
        assert pytest.approx(cache.ttl("ttl_check")) == 10

    def test_ttl_returns_none_for_persistent_key(self, cache: RespCache):
        cache.set("no_expiry", "permanent", timeout=None)
        assert cache.ttl("no_expiry") is None

    def test_ttl_negative_for_nonexistent_key(self, cache: RespCache):
        # Missing keys return -2 (Redis convention).
        ttl = cache.ttl("key_does_not_exist")
        assert ttl is not None and ttl < 0

    def test_pttl_returns_milliseconds(self, cache: RespCache):
        cache.set("pttl_key", "data", 10)
        assert pytest.approx(cache.pttl("pttl_key"), 10) == 10000

    def test_pttl_with_fractional_timeout(self, cache: RespCache):
        cache.set("half_sec", "data", 5.5)
        assert pytest.approx(cache.pttl("half_sec"), 10) == 5500

    def test_pttl_returns_none_for_persistent(self, cache: RespCache):
        cache.set("pttl_persist", "data", timeout=None)
        assert cache.pttl("pttl_persist") is None

    def test_pttl_negative_for_nonexistent_key(self, cache: RespCache):
        pttl = cache.pttl("pttl_missing")
        assert pttl is not None and pttl < 0

    def test_expiretime_returns_unix_timestamp(self, cache: RespCache):
        cache.set("et_key", "data", timeout=3600)
        result = cache.expiretime("et_key")
        assert result is not None
        assert abs(result - (int(time.time()) + 3600)) < 5

    def test_expiretime_returns_none_for_persistent_key(self, cache: RespCache):
        cache.set("et_persist", "permanent", timeout=None)
        assert cache.expiretime("et_persist") is None

    def test_expiretime_negative_for_nonexistent_key(self, cache: RespCache):
        result = cache.expiretime("et_missing")
        assert result is not None and result < 0

    def test_expiretime_after_expireat(self, cache: RespCache):
        cache.set("et_at", "data", timeout=None)
        target = int(time.time()) + 7200
        cache.expireat("et_at", target)
        result = cache.expiretime("et_at")
        assert result is not None
        assert abs(result - target) < 2

    def test_persist_removes_expiration(self, cache: RespCache):
        cache.set("will_persist", "data", timeout=20)
        assert cache.persist("will_persist") is True
        assert cache.ttl("will_persist") is None

    def test_persist_on_missing_key_returns_false(self, cache: RespCache):
        assert cache.persist("never_existed") is False


class TestExpireOperations:
    """Tests for expire() / pexpire() / expireat() / pexpireat()."""

    def test_expire_sets_ttl(self, cache: RespCache):
        cache.set("to_expire", "data", timeout=None)
        assert cache.expire("to_expire", 20) is True
        assert pytest.approx(cache.ttl("to_expire")) == 20

    def test_expire_on_missing_key_returns_false(self, cache: RespCache):
        assert cache.expire("ghost_key", 20) is False

    def test_pexpire_sets_millisecond_ttl(self, cache: RespCache):
        cache.set("pexp_key", "data", timeout=None)
        assert cache.pexpire("pexp_key", 20500) is True
        assert pytest.approx(cache.pttl("pexp_key"), 10) == 20500

    def test_pexpire_on_missing_key_returns_false(self, cache: RespCache):
        assert cache.pexpire("pexp_ghost", 20500) is False

    @pytest.mark.parametrize(
        ("kind", "hours"),
        [("datetime", 1), ("timestamp", 2)],
    )
    def test_expireat_sets_ttl(self, cache: RespCache, kind: str, hours: int):
        cache.set("exp_at", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=hours)
        when = future if kind == "datetime" else int(future.timestamp())
        assert cache.expireat("exp_at", when) is True
        assert pytest.approx(cache.ttl("exp_at"), 1) == timedelta(hours=hours).total_seconds()

    def test_expireat_past_time_deletes_key(self, cache: RespCache):
        cache.set("exp_at_past", "data", timeout=None)
        past = datetime.datetime.now() - timedelta(hours=2)
        assert cache.expireat("exp_at_past", past) is True
        assert cache.get("exp_at_past") is None

    def test_expireat_missing_key_returns_false(self, cache: RespCache):
        future = datetime.datetime.now() + timedelta(hours=2)
        assert cache.expireat("exp_at_ghost", future) is False

    @pytest.mark.parametrize(
        ("kind", "hours"),
        [("datetime", 1), ("timestamp", 2)],
    )
    def test_pexpireat_sets_pttl(self, cache: RespCache, kind: str, hours: int):
        cache.set("pexp_at", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=hours)
        when = future if kind == "datetime" else int(future.timestamp() * 1000)
        assert cache.pexpireat("pexp_at", when) is True
        assert pytest.approx(cache.pttl("pexp_at"), 10) == timedelta(hours=hours).total_seconds() * 1000

    def test_pexpireat_past_time_deletes_key(self, cache: RespCache):
        cache.set("pexp_at_past", "data", timeout=None)
        past = datetime.datetime.now() - timedelta(hours=2)
        assert cache.pexpireat("pexp_at_past", past) is True
        assert cache.get("pexp_at_past") is None

    def test_pexpireat_missing_key_returns_false(self, cache: RespCache):
        future = datetime.datetime.now() + timedelta(hours=2)
        assert cache.pexpireat("pexp_at_ghost", future) is False


class TestTouchOperation:
    """Tests for touch()."""

    def test_touch_with_zero_timeout_deletes(self, cache: RespCache):
        cache.set("touch_zero", "data", timeout=10)
        assert cache.touch("touch_zero", 0) is True
        assert cache.get("touch_zero") is None

    def test_touch_resets_expiration(self, cache: RespCache):
        cache.set("touch_reset", "data", timeout=10)
        assert cache.touch("touch_reset", 2) is True
        assert cache.get("touch_reset") == "data"
        cache.expire("touch_reset", 0)
        assert cache.get("touch_reset") is None

    def test_touch_negative_timeout_deletes(self, cache: RespCache):
        cache.set("touch_neg", "data", timeout=10)
        assert cache.touch("touch_neg", -1) is True
        assert cache.get("touch_neg") is None

    def test_touch_missing_key_returns_false(self, cache: RespCache):
        assert cache.touch("touch_ghost", 1) is False

    def test_touch_none_makes_persistent(self, cache: RespCache):
        cache.set("touch_persist", "data", timeout=1)
        assert cache.touch("touch_persist", None) is True
        assert cache.ttl("touch_persist") is None
        assert cache.get("touch_persist") == "data"

    def test_touch_none_on_missing_key_returns_false(self, cache: RespCache):
        assert cache.touch("touch_persist_ghost", None) is False

    def test_touch_without_timeout_uses_default(self, cache: RespCache):
        cache.set("touch_default", "data", timeout=1)
        assert cache.touch("touch_default") is True
        assert cache.get("touch_default") == "data"
        ttl = cache.ttl("touch_default")
        assert ttl is None or ttl > 1


class TestAsyncSetWithTimeout:
    """Tests for aset() with timeout parameter."""

    @pytest.mark.asyncio
    async def test_aset_nx_with_expiration(self, cache: RespCache):
        await cache.adelete("aexpiring_nx")
        result = await cache.aset("aexpiring_nx", "temporary", timeout=2, nx=True)
        assert result is True
        cache.expire("aexpiring_nx", 0)
        assert await cache.aget("aexpiring_nx") is None

    @pytest.mark.asyncio
    async def test_aset_nx_does_not_change_existing_ttl(self, cache: RespCache):
        await cache.aset("aexisting_ttl", "original")
        original_ttl = await cache.attl("aexisting_ttl")
        await cache.aset("aexisting_ttl", "new_value", timeout=2, nx=True)
        assert await cache.aget("aexisting_ttl") == "original"
        new_ttl = await cache.attl("aexisting_ttl")
        if original_ttl is None:
            assert new_ttl is None
        else:
            assert new_ttl is not None and new_ttl > 5

    @pytest.mark.asyncio
    async def test_akey_expires_after_timeout(self, cache: RespCache):
        await cache.aset("aexpires_soon", "temp_data", timeout=3)
        cache.expire("aexpires_soon", 0)
        assert await cache.aget("aexpires_soon") is None

    @pytest.mark.asyncio
    async def test_azero_timeout_immediate_expiration(self, cache: RespCache):
        await cache.aset("ainstant_expire", "gone", timeout=0)
        assert await cache.aget("ainstant_expire") is None

    @pytest.mark.asyncio
    async def test_atimeout_as_positional_arg(self, cache: RespCache):
        await cache.aset("apos_timeout", 999, -1)
        assert await cache.aget("apos_timeout") is None

        await cache.aset("apos_timeout2", 888, 1)
        assert await cache.aget("apos_timeout2") == 888
        cache.expire("apos_timeout2", 0)
        assert await cache.aget("apos_timeout2") is None

    @pytest.mark.asyncio
    async def test_atimeout_positional_with_nx(self, cache: RespCache):
        await cache.aset("anx_pos", "value", None)
        await cache.aset("anx_pos", "changed", -1, nx=True)
        assert await cache.aget("anx_pos") == "value"

    @pytest.mark.asyncio
    async def test_anegative_timeout_deletes_key(self, cache: RespCache):
        await cache.aset("aneg_timeout", "value", timeout=-1)
        assert await cache.aget("aneg_timeout") is None

    @pytest.mark.asyncio
    async def test_anegative_timeout_on_existing_key(self, cache: RespCache):
        await cache.aset("ato_delete", "value", timeout=None)
        await cache.aset("ato_delete", "value", timeout=-1)
        assert await cache.aget("ato_delete") is None

    @pytest.mark.asyncio
    async def test_anegative_timeout_with_nx_preserves_existing(self, cache: RespCache):
        await cache.aset("apreserve_me", "original", timeout=None)
        await cache.aset("apreserve_me", "changed", timeout=-1, nx=True)
        assert await cache.aget("apreserve_me") == "original"

    @pytest.mark.asyncio
    async def test_asubsecond_float_timeout_is_accepted(self, cache: RespCache):
        await cache.aset("atiny_ttl", "value", timeout=0.00001)


class TestAsyncTimeouts:
    """Tests for async TTL / expire / persist / type."""

    @pytest.mark.asyncio
    async def test_attl_returns_remaining_seconds(self, cache: RespCache):
        cache.set("attl_check", "data", 10)
        assert pytest.approx(await cache.attl("attl_check")) == 10

    @pytest.mark.asyncio
    async def test_attl_returns_none_for_persistent_key(self, cache: RespCache):
        cache.set("attl_persist", "permanent", timeout=None)
        assert await cache.attl("attl_persist") is None

    @pytest.mark.asyncio
    async def test_attl_negative_for_nonexistent_key(self, cache: RespCache):
        ttl = await cache.attl("attl_missing")
        assert ttl is not None and ttl < 0

    @pytest.mark.asyncio
    async def test_apttl_returns_milliseconds(self, cache: RespCache):
        cache.set("apttl_key", "data", 10)
        assert pytest.approx(await cache.apttl("apttl_key"), 10) == 10000

    @pytest.mark.asyncio
    async def test_apttl_with_fractional_timeout(self, cache: RespCache):
        await cache.aset("ahalf_sec", "data", 5.5)
        assert pytest.approx(await cache.apttl("ahalf_sec"), 10) == 5500

    @pytest.mark.asyncio
    async def test_apttl_returns_none_for_persistent(self, cache: RespCache):
        cache.set("apttl_persist", "data", timeout=None)
        assert await cache.apttl("apttl_persist") is None

    @pytest.mark.asyncio
    async def test_apttl_negative_for_nonexistent_key(self, cache: RespCache):
        pttl = await cache.apttl("apttl_missing")
        assert pttl is not None and pttl < 0

    @pytest.mark.asyncio
    async def test_aexpiretime_returns_unix_timestamp(self, cache: RespCache):
        cache.set("aet_key", "data", timeout=3600)
        result = await cache.aexpiretime("aet_key")
        assert result is not None
        assert abs(result - (int(time.time()) + 3600)) < 5

    @pytest.mark.asyncio
    async def test_aexpiretime_returns_none_for_persistent_key(self, cache: RespCache):
        cache.set("aet_persist", "permanent", timeout=None)
        assert await cache.aexpiretime("aet_persist") is None

    @pytest.mark.asyncio
    async def test_aexpiretime_negative_for_nonexistent_key(self, cache: RespCache):
        result = await cache.aexpiretime("aet_missing")
        assert result is not None and result < 0

    @pytest.mark.asyncio
    async def test_aexpiretime_after_expireat(self, cache: RespCache):
        await cache.aset("aet_at", "data", timeout=None)
        target = int(time.time()) + 7200
        await cache.aexpireat("aet_at", target)
        result = await cache.aexpiretime("aet_at")
        assert result is not None
        assert abs(result - target) < 2

    @pytest.mark.asyncio
    async def test_aexpire_sets_ttl(self, cache: RespCache):
        cache.set("aexpire_key", "data", timeout=None)
        assert await cache.aexpire("aexpire_key", 20) is True
        assert pytest.approx(cache.ttl("aexpire_key")) == 20

    @pytest.mark.asyncio
    async def test_aexpire_on_missing_key_returns_false(self, cache: RespCache):
        assert await cache.aexpire("aexpire_ghost", 20) is False

    @pytest.mark.asyncio
    async def test_apexpire_sets_millisecond_ttl(self, cache: RespCache):
        cache.set("apexpire_key", "data", timeout=None)
        assert await cache.apexpire("apexpire_key", 20500) is True
        assert pytest.approx(cache.pttl("apexpire_key"), 10) == 20500

    @pytest.mark.asyncio
    async def test_apexpire_on_missing_key_returns_false(self, cache: RespCache):
        assert await cache.apexpire("apexpire_ghost", 20500) is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kind", "hours"),
        [("datetime", 1), ("timestamp", 2)],
    )
    async def test_aexpireat_sets_ttl(self, cache: RespCache, kind: str, hours: int):
        cache.set("aexpireat", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=hours)
        when = future if kind == "datetime" else int(future.timestamp())
        assert await cache.aexpireat("aexpireat", when) is True
        assert pytest.approx(cache.ttl("aexpireat"), 1) == timedelta(hours=hours).total_seconds()

    @pytest.mark.asyncio
    async def test_aexpireat_past_time_deletes_key(self, cache: RespCache):
        await cache.aset("aexp_at_past", "data", timeout=None)
        past = datetime.datetime.now() - timedelta(hours=2)
        assert await cache.aexpireat("aexp_at_past", past) is True
        assert await cache.aget("aexp_at_past") is None

    @pytest.mark.asyncio
    async def test_aexpireat_missing_key_returns_false(self, cache: RespCache):
        future = datetime.datetime.now() + timedelta(hours=1)
        assert await cache.aexpireat("aexpireat_ghost", future) is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kind", "hours"),
        [("datetime", 1), ("timestamp", 2)],
    )
    async def test_apexpireat_sets_pttl(self, cache: RespCache, kind: str, hours: int):
        cache.set("apexpireat", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=hours)
        when = future if kind == "datetime" else int(future.timestamp() * 1000)
        assert await cache.apexpireat("apexpireat", when) is True
        assert pytest.approx(cache.pttl("apexpireat"), 10) == timedelta(hours=hours).total_seconds() * 1000

    @pytest.mark.asyncio
    async def test_apexpireat_past_time_deletes_key(self, cache: RespCache):
        await cache.aset("apexp_at_past", "data", timeout=None)
        past = datetime.datetime.now() - timedelta(hours=2)
        assert await cache.apexpireat("apexp_at_past", past) is True
        assert await cache.aget("apexp_at_past") is None

    @pytest.mark.asyncio
    async def test_apexpireat_missing_key_returns_false(self, cache: RespCache):
        future = datetime.datetime.now() + timedelta(hours=2)
        assert await cache.apexpireat("apexpireat_ghost", future) is False

    @pytest.mark.asyncio
    async def test_apersist_removes_expiration(self, cache: RespCache):
        cache.set("apersist_key", "data", timeout=20)
        assert await cache.apersist("apersist_key") is True
        assert cache.ttl("apersist_key") is None

    @pytest.mark.asyncio
    async def test_apersist_on_missing_key_returns_false(self, cache: RespCache):
        assert await cache.apersist("apersist_missing") is False


class TestAsyncTouchOperation:
    """Tests for atouch()."""

    @pytest.mark.asyncio
    async def test_atouch_with_zero_timeout_deletes(self, cache: RespCache):
        await cache.aset("atouch_zero", "data", timeout=10)
        assert await cache.atouch("atouch_zero", 0) is True
        assert await cache.aget("atouch_zero") is None

    @pytest.mark.asyncio
    async def test_atouch_resets_expiration(self, cache: RespCache):
        await cache.aset("atouch_reset", "data", timeout=10)
        assert await cache.atouch("atouch_reset", 2) is True
        assert await cache.aget("atouch_reset") == "data"
        cache.expire("atouch_reset", 0)
        assert await cache.aget("atouch_reset") is None

    @pytest.mark.asyncio
    async def test_atouch_negative_timeout_deletes(self, cache: RespCache):
        await cache.aset("atouch_neg", "data", timeout=10)
        assert await cache.atouch("atouch_neg", -1) is True
        assert await cache.aget("atouch_neg") is None

    @pytest.mark.asyncio
    async def test_atouch_missing_key_returns_false(self, cache: RespCache):
        assert await cache.atouch("atouch_ghost", 1) is False

    @pytest.mark.asyncio
    async def test_atouch_none_makes_persistent(self, cache: RespCache):
        await cache.aset("atouch_persist", "data", timeout=1)
        assert await cache.atouch("atouch_persist", None) is True
        assert await cache.attl("atouch_persist") is None
        assert await cache.aget("atouch_persist") == "data"

    @pytest.mark.asyncio
    async def test_atouch_none_on_missing_key_returns_false(self, cache: RespCache):
        assert await cache.atouch("atouch_persist_ghost", None) is False

    @pytest.mark.asyncio
    async def test_atouch_without_timeout_uses_default(self, cache: RespCache):
        await cache.aset("atouch_default", "data", timeout=1)
        assert await cache.atouch("atouch_default") is True
        assert await cache.aget("atouch_default") == "data"
        ttl = await cache.attl("atouch_default")
        assert ttl is None or ttl > 1


class TestAsyncType:
    """Tests for atype()."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("setup", "expected"),
        [
            (lambda c: c.set("atype_string", "value"), "string"),
            (lambda c: c.hset("atype_hash", "field", "value"), "hash"),
            (lambda c: c.lpush("atype_list", "value"), "list"),
            (lambda c: c.sadd("atype_set", "value"), "set"),
            (lambda c: c.zadd("atype_zset", {"member": 1.0}), "zset"),
        ],
        ids=["string", "hash", "list", "set", "zset"],
    )
    async def test_atype_returns_kind(self, cache: RespCache, setup, expected: str):
        setup(cache)
        assert await cache.atype(f"atype_{expected}") == expected

    @pytest.mark.asyncio
    async def test_atype_missing_key(self, cache: RespCache):
        assert await cache.atype("atype_missing") is None
