"""Tests for async set operations."""

import pytest

from django_cachex.cache import KeyValueCache


class TestAsyncSetBasicOps:
    """Tests for asadd, asrem, asmembers, asismember, ascard."""

    @pytest.mark.asyncio
    async def test_asadd(self, cache: KeyValueCache):
        result = await cache.asadd("afoo", "bar")
        assert result == 1
        assert cache.smembers("afoo") == {"bar"}

    @pytest.mark.asyncio
    async def test_asrem(self, cache: KeyValueCache):
        cache.sadd("afoo_rem", "bar1", "bar2")
        result = await cache.asrem("afoo_rem", "bar1")
        assert result == 1
        result = await cache.asrem("afoo_rem", "bar3")
        assert result == 0

    @pytest.mark.asyncio
    async def test_asmembers(self, cache: KeyValueCache):
        cache.sadd("afoo_members", "bar1", "bar2")
        result = await cache.asmembers("afoo_members")
        assert result == {"bar1", "bar2"}

    @pytest.mark.asyncio
    async def test_asismember(self, cache: KeyValueCache):
        cache.sadd("afoo_ism", "bar")
        assert await cache.asismember("afoo_ism", "bar") is True
        assert await cache.asismember("afoo_ism", "bar2") is False

    @pytest.mark.asyncio
    async def test_ascard(self, cache: KeyValueCache):
        cache.sadd("afoo_card", "bar", "bar2")
        assert await cache.ascard("afoo_card") == 2


class TestAsyncSetPopRandom:
    """Tests for aspop, asrandmember."""

    @pytest.mark.asyncio
    async def test_aspop_default_count(self, cache: KeyValueCache):
        cache.sadd("afoo_pop", "bar1", "bar2")
        result = await cache.aspop("afoo_pop")
        assert result in {"bar1", "bar2"}

    @pytest.mark.asyncio
    async def test_aspop_with_count(self, cache: KeyValueCache):
        cache.sadd("afoo_pop2", "bar1", "bar2")
        result = await cache.aspop("afoo_pop2", 1)
        assert result in [["bar1"], ["bar2"]]

    @pytest.mark.asyncio
    async def test_asrandmember_default_count(self, cache: KeyValueCache):
        cache.sadd("afoo_rand", "bar1", "bar2")
        result = await cache.asrandmember("afoo_rand")
        assert result in {"bar1", "bar2"}

    @pytest.mark.asyncio
    async def test_asrandmember_with_count(self, cache: KeyValueCache):
        cache.sadd("afoo_rand2", "bar1", "bar2")
        result = await cache.asrandmember("afoo_rand2", 1)
        assert result in [["bar1"], ["bar2"]]


class TestAsyncSetMove:
    """Tests for asmove."""

    @pytest.mark.asyncio
    async def test_asmove(self, cache: KeyValueCache):
        cache.sadd("{afoo}1", "bar1", "bar2")
        cache.sadd("{afoo}2", "bar2", "bar3")
        result = await cache.asmove("{afoo}1", "{afoo}2", "bar1")
        assert result is True

        result = await cache.asmove("{afoo}1", "{afoo}2", "bar4")
        assert result is False

        assert cache.smembers("{afoo}1") == {"bar2"}
        assert cache.smembers("{afoo}2") == {"bar1", "bar2", "bar3"}


class TestAsyncSetOperations:
    """Tests for asdiff, asinter, asunion and store variants."""

    @pytest.mark.asyncio
    async def test_asdiff(self, cache: KeyValueCache):
        cache.sadd("{asfoo}1", "bar1", "bar2")
        cache.sadd("{asfoo}2", "bar2", "bar3")
        result = await cache.asdiff(["{asfoo}1", "{asfoo}2"])
        assert result == {"bar1"}

    @pytest.mark.asyncio
    async def test_asdiffstore(self, cache: KeyValueCache):
        cache.sadd("{asfoo}1", "bar1", "bar2")
        cache.sadd("{asfoo}2", "bar2", "bar3")
        result = await cache.asdiffstore("{asfoo}3", ["{asfoo}1", "{asfoo}2"])
        assert result == 1
        assert cache.smembers("{asfoo}3") == {"bar1"}

    @pytest.mark.asyncio
    async def test_asinter(self, cache: KeyValueCache):
        cache.sadd("{asfoo}4", "bar1", "bar2")
        cache.sadd("{asfoo}5", "bar2", "bar3")
        result = await cache.asinter(["{asfoo}4", "{asfoo}5"])
        assert result == {"bar2"}

    @pytest.mark.asyncio
    async def test_asinterstore(self, cache: KeyValueCache):
        cache.sadd("{asfoo}4", "bar1", "bar2")
        cache.sadd("{asfoo}5", "bar2", "bar3")
        result = await cache.asinterstore("{asfoo}6", ["{asfoo}4", "{asfoo}5"])
        assert result == 1
        assert cache.smembers("{asfoo}6") == {"bar2"}

    @pytest.mark.asyncio
    async def test_asunion(self, cache: KeyValueCache):
        cache.sadd("{asfoo}7", "bar1", "bar2")
        cache.sadd("{asfoo}8", "bar2", "bar3")
        result = await cache.asunion(["{asfoo}7", "{asfoo}8"])
        assert result == {"bar1", "bar2", "bar3"}

    @pytest.mark.asyncio
    async def test_asunionstore(self, cache: KeyValueCache):
        cache.sadd("{asfoo}7", "bar1", "bar2")
        cache.sadd("{asfoo}8", "bar2", "bar3")
        result = await cache.asunionstore("{asfoo}9", ["{asfoo}7", "{asfoo}8"])
        assert result == 3
        assert cache.smembers("{asfoo}9") == {"bar1", "bar2", "bar3"}


class TestAsyncSetMultiMember:
    """Tests for asmismember."""

    @pytest.mark.asyncio
    async def test_asmismember(self, cache: KeyValueCache):
        cache.sadd("afoo_mism", "bar1", "bar2", "bar3")
        result = await cache.asmismember("afoo_mism", "bar1", "bar2", "xyz")
        assert result == [True, True, False]


class TestAsyncSetScan:
    """Tests for asscan, asscan_iter."""

    @pytest.mark.asyncio
    async def test_asscan(self, cache: KeyValueCache):
        cache.sadd("afoo_scan", "bar1", "bar2")
        _cursor, items = await cache.asscan("afoo_scan")
        assert items == {"bar1", "bar2"}

    @pytest.mark.asyncio
    async def test_asscan_iter(self, cache: KeyValueCache):
        cache.sadd("afoo_scan_iter", "bar1", "bar2")
        items = set()
        async for item in cache.asscan_iter("afoo_scan_iter"):
            items.add(item)
        assert items == {"bar1", "bar2"}
