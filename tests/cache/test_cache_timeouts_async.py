"""Tests for async timeout and TTL operations."""

import datetime
from datetime import timedelta

import pytest

from django_cachex.cache import KeyValueCache


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
