"""Tests for async list operations."""

import pytest

from django_cachex.cache import KeyValueCache


class TestAsyncListPushPop:
    """Tests for alpush, arpush, alpop, arpop."""

    @pytest.mark.asyncio
    async def test_alpush_arpush(self, cache: KeyValueCache):
        await cache.alpush("amylist", "world")
        await cache.alpush("amylist", "hello")
        await cache.arpush("amylist", "!")

        result = cache.lrange("amylist", 0, -1)
        assert result == ["hello", "world", "!"]

    @pytest.mark.asyncio
    async def test_alpush_multiple(self, cache: KeyValueCache):
        count = await cache.alpush("amylist2", "a", "b", "c")
        assert count == 3
        result = cache.lrange("amylist2", 0, -1)
        assert result == ["c", "b", "a"]

    @pytest.mark.asyncio
    async def test_arpush_multiple(self, cache: KeyValueCache):
        count = await cache.arpush("amylist3", "a", "b", "c")
        assert count == 3
        result = cache.lrange("amylist3", 0, -1)
        assert result == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_alpop(self, cache: KeyValueCache):
        cache.rpush("amylist4", "a", "b", "c")

        result = await cache.alpop("amylist4")
        assert result == "a"

        result = await cache.alpop("amylist4", count=2)
        assert result == ["b", "c"]

        result = await cache.alpop("amylist4")
        assert result is None

    @pytest.mark.asyncio
    async def test_arpop(self, cache: KeyValueCache):
        cache.rpush("amylist5", "a", "b", "c")

        result = await cache.arpop("amylist5")
        assert result == "c"

        result = await cache.arpop("amylist5", count=2)
        assert result == ["b", "a"]

        result = await cache.arpop("amylist5")
        assert result is None


class TestAsyncListRange:
    """Tests for alrange, alindex."""

    @pytest.mark.asyncio
    async def test_alrange(self, cache: KeyValueCache):
        cache.rpush("amylist6", "a", "b", "c", "d", "e")

        assert await cache.alrange("amylist6", 0, -1) == ["a", "b", "c", "d", "e"]
        assert await cache.alrange("amylist6", 0, 2) == ["a", "b", "c"]
        assert await cache.alrange("amylist6", -2, -1) == ["d", "e"]
        assert await cache.alrange("anonexistent", 0, -1) == []

    @pytest.mark.asyncio
    async def test_alindex(self, cache: KeyValueCache):
        cache.rpush("amylist7", "a", "b", "c")

        assert await cache.alindex("amylist7", 0) == "a"
        assert await cache.alindex("amylist7", 1) == "b"
        assert await cache.alindex("amylist7", -1) == "c"
        assert await cache.alindex("amylist7", 100) is None
        assert await cache.alindex("anonexistent", 0) is None


class TestAsyncListLength:
    """Tests for allen."""

    @pytest.mark.asyncio
    async def test_allen(self, cache: KeyValueCache):
        assert await cache.allen("amylist8") == 0
        cache.rpush("amylist8", "a", "b", "c")
        assert await cache.allen("amylist8") == 3


class TestAsyncListModify:
    """Tests for alrem, altrim, alset, alinsert."""

    @pytest.mark.asyncio
    async def test_alrem(self, cache: KeyValueCache):
        cache.rpush("amylist9", "a", "b", "a", "c", "a")
        removed = await cache.alrem("amylist9", 2, "a")
        assert removed == 2
        assert cache.lrange("amylist9", 0, -1) == ["b", "c", "a"]

    @pytest.mark.asyncio
    async def test_alrem_from_tail(self, cache: KeyValueCache):
        cache.rpush("amylist10", "a", "b", "a", "c", "a")
        removed = await cache.alrem("amylist10", -2, "a")
        assert removed == 2
        assert cache.lrange("amylist10", 0, -1) == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_alrem_all(self, cache: KeyValueCache):
        cache.rpush("amylist11", "a", "b", "a", "c", "a")
        removed = await cache.alrem("amylist11", 0, "a")
        assert removed == 3
        assert cache.lrange("amylist11", 0, -1) == ["b", "c"]

    @pytest.mark.asyncio
    async def test_altrim(self, cache: KeyValueCache):
        cache.rpush("amylist12", "a", "b", "c", "d", "e")
        result = await cache.altrim("amylist12", 1, 3)
        assert result is True
        assert cache.lrange("amylist12", 0, -1) == ["b", "c", "d"]

    @pytest.mark.asyncio
    async def test_alset(self, cache: KeyValueCache):
        cache.rpush("amylist13", "a", "b", "c")
        result = await cache.alset("amylist13", 1, "B")
        assert result is True
        assert cache.lrange("amylist13", 0, -1) == ["a", "B", "c"]

    @pytest.mark.asyncio
    async def test_alinsert(self, cache: KeyValueCache):
        cache.rpush("amylist14", "a", "c")

        length = await cache.alinsert("amylist14", "BEFORE", "c", "b")
        assert length == 3
        assert cache.lrange("amylist14", 0, -1) == ["a", "b", "c"]

        length = await cache.alinsert("amylist14", "AFTER", "c", "d")
        assert length == 4
        assert cache.lrange("amylist14", 0, -1) == ["a", "b", "c", "d"]

        length = await cache.alinsert("amylist14", "BEFORE", "z", "x")
        assert length == -1


class TestAsyncListPos:
    """Tests for alpos."""

    @pytest.mark.asyncio
    async def test_alpos_basic(self, cache: KeyValueCache):
        cache.rpush("amylist_lpos", "a", "b", "c", "b", "d")

        assert await cache.alpos("amylist_lpos", "b") == 1
        assert await cache.alpos("amylist_lpos", "z") is None

    @pytest.mark.asyncio
    async def test_alpos_with_rank(self, cache: KeyValueCache):
        cache.rpush("amylist_lpos2", "a", "b", "c", "b", "d", "b")

        assert await cache.alpos("amylist_lpos2", "b", rank=2) == 3
        assert await cache.alpos("amylist_lpos2", "b", rank=-1) == 5

    @pytest.mark.asyncio
    async def test_alpos_with_count(self, cache: KeyValueCache):
        cache.rpush("amylist_lpos3", "a", "b", "c", "b", "d", "b")

        result = await cache.alpos("amylist_lpos3", "b", count=0)
        assert result == [1, 3, 5]

        result = await cache.alpos("amylist_lpos3", "b", count=2)
        assert result == [1, 3]


class TestAsyncListMove:
    """Tests for almove."""

    @pytest.mark.asyncio
    async def test_almove_basic(self, cache: KeyValueCache):
        cache.rpush("{alist}src", "a", "b", "c")
        cache.rpush("{alist}dst", "x", "y")

        result = await cache.almove("{alist}src", "{alist}dst", "LEFT", "RIGHT")
        assert result == "a"

        assert cache.lrange("{alist}src", 0, -1) == ["b", "c"]
        assert cache.lrange("{alist}dst", 0, -1) == ["x", "y", "a"]

    @pytest.mark.asyncio
    async def test_almove_empty_source(self, cache: KeyValueCache):
        cache.rpush("{alist}dst3", "x")

        result = await cache.almove("{alist}empty_src", "{alist}dst3", "LEFT", "RIGHT")
        assert result is None


class TestAsyncBlockingOps:
    """Tests for ablpop, abrpop, ablmove."""

    @pytest.mark.asyncio
    async def test_ablpop_immediate(self, cache: KeyValueCache):
        cache.rpush("ablpop_list", "a", "b", "c")

        result = await cache.ablpop("ablpop_list", timeout=1)
        assert result is not None
        rkey, value = result
        assert rkey == "ablpop_list"
        assert value == "a"

    @pytest.mark.asyncio
    async def test_ablpop_timeout(self, cache: KeyValueCache):
        result = await cache.ablpop("ablpop_empty", timeout=0.1)
        assert result is None

    @pytest.mark.asyncio
    async def test_abrpop_immediate(self, cache: KeyValueCache):
        cache.rpush("abrpop_list", "a", "b", "c")

        result = await cache.abrpop("abrpop_list", timeout=1)
        assert result is not None
        rkey, value = result
        assert rkey == "abrpop_list"
        assert value == "c"

    @pytest.mark.asyncio
    async def test_abrpop_timeout(self, cache: KeyValueCache):
        result = await cache.abrpop("abrpop_empty", timeout=0.1)
        assert result is None

    @pytest.mark.asyncio
    async def test_ablmove_immediate(self, cache: KeyValueCache):
        cache.rpush("{ablmove}src", "a", "b", "c")
        cache.rpush("{ablmove}dst", "x")

        result = await cache.ablmove(
            "{ablmove}src",
            "{ablmove}dst",
            timeout=1,
            wherefrom="LEFT",
            whereto="RIGHT",
        )
        assert result == "a"
        assert cache.lrange("{ablmove}src", 0, -1) == ["b", "c"]
        assert cache.lrange("{ablmove}dst", 0, -1) == ["x", "a"]

    @pytest.mark.asyncio
    async def test_ablmove_timeout(self, cache: KeyValueCache):
        cache.rpush("{ablmove_empty}dst", "x")

        result = await cache.ablmove(
            "{ablmove_empty}src",
            "{ablmove_empty}dst",
            timeout=0.1,
        )
        assert result is None
