"""Tests for async timeout and TTL operations."""

import datetime
from datetime import timedelta

import pytest

from django_cachex.cache import KeyValueCache


@pytest.fixture
def mk(cache: KeyValueCache):
    """Create a prefixed key for direct client async testing."""
    return lambda key, version=None: cache.make_and_validate_key(key, version=version)


class TestAsyncTTL:
    """Tests for attl() method."""

    @pytest.mark.asyncio
    async def test_attl_returns_remaining_seconds(self, cache: KeyValueCache, mk):
        cache.set("attl_check", "data", 10)
        ttl = await cache._cache.attl(mk("attl_check"))
        assert pytest.approx(ttl) == 10

    @pytest.mark.asyncio
    async def test_attl_returns_none_for_persistent_key(self, cache: KeyValueCache, mk):
        cache.set("attl_persist", "permanent", timeout=None)
        result = await cache._cache.attl(mk("attl_persist"))
        assert result is None

    @pytest.mark.asyncio
    async def test_attl_negative_for_nonexistent_key(self, cache: KeyValueCache, mk):
        ttl = await cache._cache.attl(mk("attl_missing"))
        assert ttl is not None and ttl < 0


class TestAsyncPTTL:
    """Tests for apttl() method."""

    @pytest.mark.asyncio
    async def test_apttl_returns_milliseconds(self, cache: KeyValueCache, mk):
        cache.set("apttl_key", "data", 10)
        pttl = await cache._cache.apttl(mk("apttl_key"))
        assert pytest.approx(pttl, 10) == 10000

    @pytest.mark.asyncio
    async def test_apttl_returns_none_for_persistent(self, cache: KeyValueCache, mk):
        cache.set("apttl_persist", "data", timeout=None)
        result = await cache._cache.apttl(mk("apttl_persist"))
        assert result is None

    @pytest.mark.asyncio
    async def test_apttl_negative_for_nonexistent_key(self, cache: KeyValueCache, mk):
        pttl = await cache._cache.apttl(mk("apttl_missing"))
        assert pttl is not None and pttl < 0


class TestAsyncExpire:
    """Tests for aexpire() method."""

    @pytest.mark.asyncio
    async def test_aexpire_sets_ttl(self, cache: KeyValueCache, mk):
        cache.set("aexpire_key", "data", timeout=None)
        result = await cache._cache.aexpire(mk("aexpire_key"), 20)
        assert result is True
        ttl = cache.ttl("aexpire_key")
        assert pytest.approx(ttl) == 20

    @pytest.mark.asyncio
    async def test_aexpire_on_missing_key_returns_false(self, cache: KeyValueCache, mk):
        result = await cache._cache.aexpire(mk("aexpire_ghost"), 20)
        assert result is False


class TestAsyncPExpire:
    """Tests for apexpire() method."""

    @pytest.mark.asyncio
    async def test_apexpire_sets_millisecond_ttl(self, cache: KeyValueCache, mk):
        cache.set("apexpire_key", "data", timeout=None)
        result = await cache._cache.apexpire(mk("apexpire_key"), 20500)
        assert result is True
        pttl = cache.pttl("apexpire_key")
        assert pytest.approx(pttl, 10) == 20500

    @pytest.mark.asyncio
    async def test_apexpire_on_missing_key_returns_false(self, cache: KeyValueCache, mk):
        result = await cache._cache.apexpire(mk("apexpire_ghost"), 20500)
        assert result is False


class TestAsyncExpireAt:
    """Tests for aexpireat() method."""

    @pytest.mark.asyncio
    async def test_aexpireat_with_datetime(self, cache: KeyValueCache, mk):
        cache.set("aexpireat_dt", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=1)
        result = await cache._cache.aexpireat(mk("aexpireat_dt"), future)
        assert result is True
        ttl = cache.ttl("aexpireat_dt")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds()

    @pytest.mark.asyncio
    async def test_aexpireat_missing_key_returns_false(self, cache: KeyValueCache, mk):
        future = datetime.datetime.now() + timedelta(hours=1)
        result = await cache._cache.aexpireat(mk("aexpireat_ghost"), future)
        assert result is False


class TestAsyncPExpireAt:
    """Tests for apexpireat() method."""

    @pytest.mark.asyncio
    async def test_apexpireat_with_datetime(self, cache: KeyValueCache, mk):
        cache.set("apexpireat_dt", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=1)
        result = await cache._cache.apexpireat(mk("apexpireat_dt"), future)
        assert result is True
        pttl = cache.pttl("apexpireat_dt")
        assert pytest.approx(pttl, 10) == timedelta(hours=1).total_seconds() * 1000

    @pytest.mark.asyncio
    async def test_apexpireat_with_timestamp(self, cache: KeyValueCache, mk):
        cache.set("apexpireat_ts", "data", timeout=None)
        future = datetime.datetime.now() + timedelta(hours=2)
        result = await cache._cache.apexpireat(mk("apexpireat_ts"), int(future.timestamp() * 1000))
        assert result is True
        pttl = cache.pttl("apexpireat_ts")
        assert pytest.approx(pttl, 10) == timedelta(hours=2).total_seconds() * 1000

    @pytest.mark.asyncio
    async def test_apexpireat_missing_key_returns_false(self, cache: KeyValueCache, mk):
        future = datetime.datetime.now() + timedelta(hours=2)
        result = await cache._cache.apexpireat(mk("apexpireat_ghost"), future)
        assert result is False


class TestAsyncPersist:
    """Tests for apersist() method."""

    @pytest.mark.asyncio
    async def test_apersist_removes_expiration(self, cache: KeyValueCache, mk):
        cache.set("apersist_key", "data", timeout=20)
        result = await cache._cache.apersist(mk("apersist_key"))
        assert result is True
        assert cache.ttl("apersist_key") is None

    @pytest.mark.asyncio
    async def test_apersist_on_missing_key_returns_false(self, cache: KeyValueCache, mk):
        result = await cache._cache.apersist(mk("apersist_missing"))
        assert result is False


class TestAsyncType:
    """Tests for atype() method."""

    @pytest.mark.asyncio
    async def test_atype_string(self, cache: KeyValueCache, mk):
        cache.set("atype_str", "value")
        result = await cache._cache.atype(mk("atype_str"))
        assert result == "string"

    @pytest.mark.asyncio
    async def test_atype_hash(self, cache: KeyValueCache, mk):
        cache.hset("atype_hash", "field", "value")
        result = await cache._cache.atype(mk("atype_hash"))
        assert result == "hash"

    @pytest.mark.asyncio
    async def test_atype_list(self, cache: KeyValueCache, mk):
        cache.lpush("atype_list", "value")
        result = await cache._cache.atype(mk("atype_list"))
        assert result == "list"

    @pytest.mark.asyncio
    async def test_atype_set(self, cache: KeyValueCache, mk):
        cache.sadd("atype_set", "value")
        result = await cache._cache.atype(mk("atype_set"))
        assert result == "set"

    @pytest.mark.asyncio
    async def test_atype_zset(self, cache: KeyValueCache, mk):
        cache.zadd("atype_zset", {"member": 1.0})
        result = await cache._cache.atype(mk("atype_zset"))
        assert result == "zset"

    @pytest.mark.asyncio
    async def test_atype_missing_key(self, cache: KeyValueCache, mk):
        result = await cache._cache.atype(mk("atype_missing"))
        assert result is None
