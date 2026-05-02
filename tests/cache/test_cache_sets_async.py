"""Tests for async set operations."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
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


class TestAsyncVersionSrcDst:
    """Tests for version_src/version_dst on asmove."""

    @pytest.mark.asyncio
    async def test_asmove_version_src_dst(self, cache: KeyValueCache):
        cache.sadd("{vs}:assrc", "a", "b", version=1)
        cache.sadd("{vs}:asdst", "x", version=2)

        result = await cache.asmove("{vs}:assrc", "{vs}:asdst", "a", version_src=1, version_dst=2)
        assert result is True
        assert cache.smembers("{vs}:assrc", version=1) == {"b"}
        assert cache.smembers("{vs}:asdst", version=2) == {"x", "a"}


class TestAsyncSetStoreOperations:
    """Tests for async sinterstore / sdiffstore."""

    @pytest.mark.asyncio
    async def test_asinterstore(self, cache: KeyValueCache):
        cache.sadd("{afoo}1", "bar1", "bar2")
        cache.sadd("{afoo}2", "bar2", "bar3")
        assert await cache.asinterstore("{afoo}3", ["{afoo}1", "{afoo}2"]) == 1
        assert cache.smembers("{afoo}3") == {"bar2"}

    @pytest.mark.asyncio
    async def test_asdiffstore_with_keys_version(self, cache: KeyValueCache):
        cache.sadd("{afoo}1", "bar1", "bar2", version=2)
        cache.sadd("{afoo}2", "bar2", "bar3", version=2)
        assert await cache.asdiffstore("{afoo}3", ["{afoo}1", "{afoo}2"], version_keys=2) == 1
        assert cache.smembers("{afoo}3") == {"bar1"}

    @pytest.mark.asyncio
    async def test_asdiffstore_with_different_keys_versions_without_initial_set_in_version(
        self,
        cache: KeyValueCache,
    ):
        cache.sadd("{afoo}1", "bar1", "bar2", version=1)
        cache.sadd("{afoo}2", "bar2", "bar3", version=2)
        assert await cache.asdiffstore("{afoo}3", ["{afoo}1", "{afoo}2"], version_keys=2) == 0

    @pytest.mark.asyncio
    async def test_asdiffstore_with_different_keys_versions_with_initial_set_in_version(
        self,
        cache: KeyValueCache,
    ):
        cache.sadd("{afoo}1", "bar1", "bar2", version=2)
        cache.sadd("{afoo}2", "bar2", "bar3", version=1)
        assert await cache.asdiffstore("{afoo}3", ["{afoo}1", "{afoo}2"], version_keys=2) == 2


class TestAsyncRandomMembers:
    """Tests for aspop / asrandmember with explicit count."""

    @pytest.mark.asyncio
    async def test_aspop(self, cache: KeyValueCache):
        cache.sadd("aspop_foo", "bar1", "bar2")
        assert await cache.aspop("aspop_foo", 1) in [["bar1"], ["bar2"]]
        assert cache.smembers("aspop_foo") in [{"bar1"}, {"bar2"}]

    @pytest.mark.asyncio
    async def test_asrandmember(self, cache: KeyValueCache):
        cache.sadd("asrand_foo", "bar1", "bar2")
        assert await cache.asrandmember("asrand_foo", 1) in [["bar1"], ["bar2"]]


class TestAsyncSScanMatch:
    """Coverage for the match-pattern path on asscan / asscan_iter."""

    @pytest.mark.asyncio
    async def test_asscan_with_match(self):
        # SSCAN match operates on raw bytes, not deserialized values.
        # Since values are serialized, match patterns won't work for strings.
        pytest.skip("SSCAN match doesn't work with serialized values")

    @pytest.mark.asyncio
    async def test_asscan_iter_with_match(self):
        pytest.skip("SSCAN match doesn't work with serialized values")
