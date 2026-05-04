"""Tests for set operations."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


class TestSetOperations:
    def test_sadd(self, cache: RespCache):
        assert cache.sadd("foo", "bar") == 1
        assert cache.smembers("foo") == {"bar"}

    def test_scard(self, cache: RespCache):
        cache.sadd("foo", "bar", "bar2")
        assert cache.scard("foo") == 2

    def test_sdiff(self, cache: RespCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sdiff(["{foo}1", "{foo}2"]) == {"bar1"}

    def test_sdiffstore(self, cache: RespCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sdiffstore("{foo}3", ["{foo}1", "{foo}2"]) == 1
        assert cache.smembers("{foo}3") == {"bar1"}

    @pytest.mark.parametrize(
        ("v1", "v2", "version_keys", "expected"),
        [
            (2, 2, 2, 1),
            (1, 2, 2, 0),
            (2, 1, 2, 2),
        ],
    )
    def test_sdiffstore_versions(
        self,
        cache: RespCache,
        v1: int,
        v2: int,
        version_keys: int,
        expected: int,
    ):
        cache.sadd("{foo}1", "bar1", "bar2", version=v1)
        cache.sadd("{foo}2", "bar2", "bar3", version=v2)
        assert cache.sdiffstore("{foo}3", ["{foo}1", "{foo}2"], version_keys=version_keys) == expected

    def test_sinter(self, cache: RespCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sinter(["{foo}1", "{foo}2"]) == {"bar2"}

    def test_sinterstore(self, cache: RespCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sinterstore("{foo}3", ["{foo}1", "{foo}2"]) == 1
        assert cache.smembers("{foo}3") == {"bar2"}

    def test_sismember(self, cache: RespCache):
        cache.sadd("foo", "bar")
        assert cache.sismember("foo", "bar") is True
        assert cache.sismember("foo", "bar2") is False

    def test_smove(self, cache: RespCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.smove("{foo}1", "{foo}2", "bar1") is True
        assert cache.smove("{foo}1", "{foo}2", "bar4") is False
        assert cache.smembers("{foo}1") == {"bar2"}
        assert cache.smembers("{foo}2") == {"bar1", "bar2", "bar3"}

    def test_smove_version_src_dst(self, cache: RespCache):
        cache.sadd("{vs}:ssrc", "a", "b", version=1)
        cache.sadd("{vs}:sdst", "x", version=2)

        result = cache.smove("{vs}:ssrc", "{vs}:sdst", "a", version_src=1, version_dst=2)
        assert result is True
        assert cache.smembers("{vs}:ssrc", version=1) == {"b"}
        assert cache.smembers("{vs}:sdst", version=2) == {"x", "a"}

    def test_spop_default_count(self, cache: RespCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.spop("foo") in {"bar1", "bar2"}
        assert cache.smembers("foo") in [{"bar1"}, {"bar2"}]

    def test_spop_with_count(self, cache: RespCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.spop("foo", 1) in [["bar1"], ["bar2"]]
        assert cache.smembers("foo") in [{"bar1"}, {"bar2"}]

    def test_srandmember_default_count(self, cache: RespCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.srandmember("foo") in {"bar1", "bar2"}

    def test_srandmember_with_count(self, cache: RespCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.srandmember("foo", 1) in [["bar1"], ["bar2"]]

    def test_srem(self, cache: RespCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.srem("foo", "bar1") == 1
        assert cache.srem("foo", "bar3") == 0

    def test_sscan(self, cache: RespCache):
        cache.sadd("foo", "bar1", "bar2")
        _cursor, items = cache.sscan("foo")
        assert items == {"bar1", "bar2"}

    def test_sscan_iter(self, cache: RespCache):
        cache.sadd("foo", "bar1", "bar2")
        items = cache.sscan_iter("foo")
        assert set(items) == {"bar1", "bar2"}

    def test_smismember(self, cache: RespCache):
        cache.sadd("foo", "bar1", "bar2", "bar3")
        assert cache.smismember("foo", "bar1", "bar2", "xyz") == [True, True, False]

    def test_sunion(self, cache: RespCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sunion(["{foo}1", "{foo}2"]) == {"bar1", "bar2", "bar3"}

    def test_sunionstore(self, cache: RespCache):
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sunionstore("{foo}3", ["{foo}1", "{foo}2"]) == 3
        assert cache.smembers("{foo}3") == {"bar1", "bar2", "bar3"}


class TestAsyncSetOperations:
    @pytest.mark.asyncio
    async def test_asadd(self, cache: RespCache):
        result = await cache.asadd("afoo", "bar")
        assert result == 1
        assert cache.smembers("afoo") == {"bar"}

    @pytest.mark.asyncio
    async def test_asrem(self, cache: RespCache):
        cache.sadd("afoo", "bar1", "bar2")
        assert await cache.asrem("afoo", "bar1") == 1
        assert await cache.asrem("afoo", "bar3") == 0

    @pytest.mark.asyncio
    async def test_asmembers(self, cache: RespCache):
        cache.sadd("afoo", "bar1", "bar2")
        assert await cache.asmembers("afoo") == {"bar1", "bar2"}

    @pytest.mark.asyncio
    async def test_asismember(self, cache: RespCache):
        cache.sadd("afoo", "bar")
        assert await cache.asismember("afoo", "bar") is True
        assert await cache.asismember("afoo", "bar2") is False

    @pytest.mark.asyncio
    async def test_ascard(self, cache: RespCache):
        cache.sadd("afoo", "bar", "bar2")
        assert await cache.ascard("afoo") == 2

    @pytest.mark.asyncio
    async def test_asmismember(self, cache: RespCache):
        cache.sadd("afoo", "bar1", "bar2", "bar3")
        assert await cache.asmismember("afoo", "bar1", "bar2", "xyz") == [True, True, False]

    @pytest.mark.asyncio
    async def test_aspop_default_count(self, cache: RespCache):
        cache.sadd("afoo", "bar1", "bar2")
        assert await cache.aspop("afoo") in {"bar1", "bar2"}

    @pytest.mark.asyncio
    async def test_aspop_with_count(self, cache: RespCache):
        cache.sadd("afoo", "bar1", "bar2")
        assert await cache.aspop("afoo", 1) in [["bar1"], ["bar2"]]

    @pytest.mark.asyncio
    async def test_asrandmember_default_count(self, cache: RespCache):
        cache.sadd("afoo", "bar1", "bar2")
        assert await cache.asrandmember("afoo") in {"bar1", "bar2"}

    @pytest.mark.asyncio
    async def test_asrandmember_with_count(self, cache: RespCache):
        cache.sadd("afoo", "bar1", "bar2")
        assert await cache.asrandmember("afoo", 1) in [["bar1"], ["bar2"]]

    @pytest.mark.asyncio
    async def test_asmove(self, cache: RespCache):
        cache.sadd("{afoo}1", "bar1", "bar2")
        cache.sadd("{afoo}2", "bar2", "bar3")
        assert await cache.asmove("{afoo}1", "{afoo}2", "bar1") is True
        assert await cache.asmove("{afoo}1", "{afoo}2", "bar4") is False
        assert cache.smembers("{afoo}1") == {"bar2"}
        assert cache.smembers("{afoo}2") == {"bar1", "bar2", "bar3"}

    @pytest.mark.asyncio
    async def test_asmove_version_src_dst(self, cache: RespCache):
        cache.sadd("{vs}:assrc", "a", "b", version=1)
        cache.sadd("{vs}:asdst", "x", version=2)

        result = await cache.asmove("{vs}:assrc", "{vs}:asdst", "a", version_src=1, version_dst=2)
        assert result is True
        assert cache.smembers("{vs}:assrc", version=1) == {"b"}
        assert cache.smembers("{vs}:asdst", version=2) == {"x", "a"}

    @pytest.mark.asyncio
    async def test_asdiff(self, cache: RespCache):
        cache.sadd("{asfoo}1", "bar1", "bar2")
        cache.sadd("{asfoo}2", "bar2", "bar3")
        assert await cache.asdiff(["{asfoo}1", "{asfoo}2"]) == {"bar1"}

    @pytest.mark.asyncio
    async def test_asdiffstore(self, cache: RespCache):
        cache.sadd("{asfoo}1", "bar1", "bar2")
        cache.sadd("{asfoo}2", "bar2", "bar3")
        assert await cache.asdiffstore("{asfoo}3", ["{asfoo}1", "{asfoo}2"]) == 1
        assert cache.smembers("{asfoo}3") == {"bar1"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("v1", "v2", "version_keys", "expected"),
        [
            (2, 2, 2, 1),
            (1, 2, 2, 0),
            (2, 1, 2, 2),
        ],
    )
    async def test_asdiffstore_versions(
        self,
        cache: RespCache,
        v1: int,
        v2: int,
        version_keys: int,
        expected: int,
    ):
        cache.sadd("{afoo}1", "bar1", "bar2", version=v1)
        cache.sadd("{afoo}2", "bar2", "bar3", version=v2)
        assert await cache.asdiffstore("{afoo}3", ["{afoo}1", "{afoo}2"], version_keys=version_keys) == expected

    @pytest.mark.asyncio
    async def test_asinter(self, cache: RespCache):
        cache.sadd("{asfoo}1", "bar1", "bar2")
        cache.sadd("{asfoo}2", "bar2", "bar3")
        assert await cache.asinter(["{asfoo}1", "{asfoo}2"]) == {"bar2"}

    @pytest.mark.asyncio
    async def test_asinterstore(self, cache: RespCache):
        cache.sadd("{asfoo}1", "bar1", "bar2")
        cache.sadd("{asfoo}2", "bar2", "bar3")
        assert await cache.asinterstore("{asfoo}3", ["{asfoo}1", "{asfoo}2"]) == 1
        assert cache.smembers("{asfoo}3") == {"bar2"}

    @pytest.mark.asyncio
    async def test_asunion(self, cache: RespCache):
        cache.sadd("{asfoo}1", "bar1", "bar2")
        cache.sadd("{asfoo}2", "bar2", "bar3")
        assert await cache.asunion(["{asfoo}1", "{asfoo}2"]) == {"bar1", "bar2", "bar3"}

    @pytest.mark.asyncio
    async def test_asunionstore(self, cache: RespCache):
        cache.sadd("{asfoo}1", "bar1", "bar2")
        cache.sadd("{asfoo}2", "bar2", "bar3")
        assert await cache.asunionstore("{asfoo}3", ["{asfoo}1", "{asfoo}2"]) == 3
        assert cache.smembers("{asfoo}3") == {"bar1", "bar2", "bar3"}

    @pytest.mark.asyncio
    async def test_asscan(self, cache: RespCache):
        cache.sadd("afoo", "bar1", "bar2")
        _cursor, items = await cache.asscan("afoo")
        assert items == {"bar1", "bar2"}

    @pytest.mark.asyncio
    async def test_asscan_iter(self, cache: RespCache):
        cache.sadd("afoo", "bar1", "bar2")
        items = set()
        async for item in cache.asscan_iter("afoo"):
            items.add(item)
        assert items == {"bar1", "bar2"}
