"""Tests for async list operations."""

import pytest

from django_cachex.cache import KeyValueCache


@pytest.fixture
def mk(cache: KeyValueCache):
    """Create a prefixed key for direct client async testing."""
    return lambda key, version=None: cache.make_and_validate_key(key, version=version)


class TestAsyncListPushPop:
    """Tests for alpush, arpush, alpop, arpop."""

    @pytest.mark.asyncio
    async def test_alpush_arpush(self, cache: KeyValueCache, mk):
        key = mk("amylist")
        await cache._cache.alpush(key, "world")
        await cache._cache.alpush(key, "hello")
        await cache._cache.arpush(key, "!")

        result = cache.lrange("amylist", 0, -1)
        assert result == ["hello", "world", "!"]

    @pytest.mark.asyncio
    async def test_alpush_multiple(self, cache: KeyValueCache, mk):
        key = mk("amylist2")
        count = await cache._cache.alpush(key, "a", "b", "c")
        assert count == 3
        result = cache.lrange("amylist2", 0, -1)
        assert result == ["c", "b", "a"]

    @pytest.mark.asyncio
    async def test_arpush_multiple(self, cache: KeyValueCache, mk):
        key = mk("amylist3")
        count = await cache._cache.arpush(key, "a", "b", "c")
        assert count == 3
        result = cache.lrange("amylist3", 0, -1)
        assert result == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_alpop(self, cache: KeyValueCache, mk):
        cache.rpush("amylist4", "a", "b", "c")
        key = mk("amylist4")

        result = await cache._cache.alpop(key)
        assert result == "a"

        result = await cache._cache.alpop(key, count=2)
        assert result == ["b", "c"]

        result = await cache._cache.alpop(key)
        assert result is None

    @pytest.mark.asyncio
    async def test_arpop(self, cache: KeyValueCache, mk):
        cache.rpush("amylist5", "a", "b", "c")
        key = mk("amylist5")

        result = await cache._cache.arpop(key)
        assert result == "c"

        result = await cache._cache.arpop(key, count=2)
        assert result == ["b", "a"]

        result = await cache._cache.arpop(key)
        assert result is None


class TestAsyncListRange:
    """Tests for alrange, alindex."""

    @pytest.mark.asyncio
    async def test_alrange(self, cache: KeyValueCache, mk):
        cache.rpush("amylist6", "a", "b", "c", "d", "e")
        key = mk("amylist6")

        assert await cache._cache.alrange(key, 0, -1) == ["a", "b", "c", "d", "e"]
        assert await cache._cache.alrange(key, 0, 2) == ["a", "b", "c"]
        assert await cache._cache.alrange(key, -2, -1) == ["d", "e"]
        assert await cache._cache.alrange(mk("anonexistent"), 0, -1) == []

    @pytest.mark.asyncio
    async def test_alindex(self, cache: KeyValueCache, mk):
        cache.rpush("amylist7", "a", "b", "c")
        key = mk("amylist7")

        assert await cache._cache.alindex(key, 0) == "a"
        assert await cache._cache.alindex(key, 1) == "b"
        assert await cache._cache.alindex(key, -1) == "c"
        assert await cache._cache.alindex(key, 100) is None
        assert await cache._cache.alindex(mk("anonexistent"), 0) is None


class TestAsyncListLength:
    """Tests for allen."""

    @pytest.mark.asyncio
    async def test_allen(self, cache: KeyValueCache, mk):
        assert await cache._cache.allen(mk("amylist8")) == 0
        cache.rpush("amylist8", "a", "b", "c")
        assert await cache._cache.allen(mk("amylist8")) == 3


class TestAsyncListModify:
    """Tests for alrem, altrim, alset, alinsert."""

    @pytest.mark.asyncio
    async def test_alrem(self, cache: KeyValueCache, mk):
        cache.rpush("amylist9", "a", "b", "a", "c", "a")
        removed = await cache._cache.alrem(mk("amylist9"), 2, "a")
        assert removed == 2
        assert cache.lrange("amylist9", 0, -1) == ["b", "c", "a"]

    @pytest.mark.asyncio
    async def test_alrem_from_tail(self, cache: KeyValueCache, mk):
        cache.rpush("amylist10", "a", "b", "a", "c", "a")
        removed = await cache._cache.alrem(mk("amylist10"), -2, "a")
        assert removed == 2
        assert cache.lrange("amylist10", 0, -1) == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_alrem_all(self, cache: KeyValueCache, mk):
        cache.rpush("amylist11", "a", "b", "a", "c", "a")
        removed = await cache._cache.alrem(mk("amylist11"), 0, "a")
        assert removed == 3
        assert cache.lrange("amylist11", 0, -1) == ["b", "c"]

    @pytest.mark.asyncio
    async def test_altrim(self, cache: KeyValueCache, mk):
        cache.rpush("amylist12", "a", "b", "c", "d", "e")
        result = await cache._cache.altrim(mk("amylist12"), 1, 3)
        assert result is True
        assert cache.lrange("amylist12", 0, -1) == ["b", "c", "d"]

    @pytest.mark.asyncio
    async def test_alset(self, cache: KeyValueCache, mk):
        cache.rpush("amylist13", "a", "b", "c")
        result = await cache._cache.alset(mk("amylist13"), 1, "B")
        assert result is True
        assert cache.lrange("amylist13", 0, -1) == ["a", "B", "c"]

    @pytest.mark.asyncio
    async def test_alinsert(self, cache: KeyValueCache, mk):
        cache.rpush("amylist14", "a", "c")
        key = mk("amylist14")

        length = await cache._cache.alinsert(key, "BEFORE", "c", "b")
        assert length == 3
        assert cache.lrange("amylist14", 0, -1) == ["a", "b", "c"]

        length = await cache._cache.alinsert(key, "AFTER", "c", "d")
        assert length == 4
        assert cache.lrange("amylist14", 0, -1) == ["a", "b", "c", "d"]

        length = await cache._cache.alinsert(key, "BEFORE", "z", "x")
        assert length == -1


class TestAsyncListPos:
    """Tests for alpos."""

    @pytest.mark.asyncio
    async def test_alpos_basic(self, cache: KeyValueCache, mk):
        cache.rpush("amylist_lpos", "a", "b", "c", "b", "d")
        key = mk("amylist_lpos")

        assert await cache._cache.alpos(key, "b") == 1
        assert await cache._cache.alpos(key, "z") is None

    @pytest.mark.asyncio
    async def test_alpos_with_rank(self, cache: KeyValueCache, mk):
        cache.rpush("amylist_lpos2", "a", "b", "c", "b", "d", "b")
        key = mk("amylist_lpos2")

        assert await cache._cache.alpos(key, "b", rank=2) == 3
        assert await cache._cache.alpos(key, "b", rank=-1) == 5

    @pytest.mark.asyncio
    async def test_alpos_with_count(self, cache: KeyValueCache, mk):
        cache.rpush("amylist_lpos3", "a", "b", "c", "b", "d", "b")
        key = mk("amylist_lpos3")

        result = await cache._cache.alpos(key, "b", count=0)
        assert result == [1, 3, 5]

        result = await cache._cache.alpos(key, "b", count=2)
        assert result == [1, 3]


class TestAsyncListMove:
    """Tests for almove."""

    @pytest.mark.asyncio
    async def test_almove_basic(self, cache: KeyValueCache, mk):
        cache.rpush("{alist}src", "a", "b", "c")
        cache.rpush("{alist}dst", "x", "y")

        result = await cache._cache.almove(mk("{alist}src"), mk("{alist}dst"), "LEFT", "RIGHT")
        assert result == "a"

        assert cache.lrange("{alist}src", 0, -1) == ["b", "c"]
        assert cache.lrange("{alist}dst", 0, -1) == ["x", "y", "a"]

    @pytest.mark.asyncio
    async def test_almove_empty_source(self, cache: KeyValueCache, mk):
        cache.rpush("{alist}dst3", "x")

        result = await cache._cache.almove(mk("{alist}empty_src"), mk("{alist}dst3"), "LEFT", "RIGHT")
        assert result is None


class TestAsyncBlockingOps:
    """Tests for ablpop, abrpop, ablmove."""

    @pytest.mark.asyncio
    async def test_ablpop_immediate(self, cache: KeyValueCache, mk):
        cache.rpush("ablpop_list", "a", "b", "c")
        key = mk("ablpop_list")

        result = await cache._cache.ablpop(key, timeout=1)
        assert result is not None
        rkey, value = result
        assert "ablpop_list" in rkey
        assert value == "a"

    @pytest.mark.asyncio
    async def test_ablpop_timeout(self, cache: KeyValueCache, mk):
        result = await cache._cache.ablpop(mk("ablpop_empty"), timeout=0.1)
        assert result is None

    @pytest.mark.asyncio
    async def test_abrpop_immediate(self, cache: KeyValueCache, mk):
        cache.rpush("abrpop_list", "a", "b", "c")
        key = mk("abrpop_list")

        result = await cache._cache.abrpop(key, timeout=1)
        assert result is not None
        rkey, value = result
        assert "abrpop_list" in rkey
        assert value == "c"

    @pytest.mark.asyncio
    async def test_abrpop_timeout(self, cache: KeyValueCache, mk):
        result = await cache._cache.abrpop(mk("abrpop_empty"), timeout=0.1)
        assert result is None

    @pytest.mark.asyncio
    async def test_ablmove_immediate(self, cache: KeyValueCache, mk):
        cache.rpush("{ablmove}src", "a", "b", "c")
        cache.rpush("{ablmove}dst", "x")

        result = await cache._cache.ablmove(
            mk("{ablmove}src"),
            mk("{ablmove}dst"),
            timeout=1,
            wherefrom="LEFT",
            whereto="RIGHT",
        )
        assert result == "a"
        assert cache.lrange("{ablmove}src", 0, -1) == ["b", "c"]
        assert cache.lrange("{ablmove}dst", 0, -1) == ["x", "a"]

    @pytest.mark.asyncio
    async def test_ablmove_timeout(self, cache: KeyValueCache, mk):
        cache.rpush("{ablmove_empty}dst", "x")

        result = await cache._cache.ablmove(
            mk("{ablmove_empty}src"),
            mk("{ablmove_empty}dst"),
            timeout=0.1,
        )
        assert result is None
