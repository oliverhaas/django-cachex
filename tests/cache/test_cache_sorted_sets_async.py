"""Tests for async sorted set operations."""

import pytest

from django_cachex.cache import KeyValueCache


@pytest.fixture
def mk(cache: KeyValueCache):
    """Create a prefixed key for direct client async testing."""
    return lambda key, version=None: cache.make_and_validate_key(key, version=version)


class TestAsyncSortedSetAdd:
    """Tests for azadd."""

    @pytest.mark.asyncio
    async def test_azadd_basic(self, cache: KeyValueCache, mk):
        result = await cache._cache.azadd(mk("ascores"), {"player1": 100.0, "player2": 200.0})
        assert result == 2
        assert cache.zcard("ascores") == 2

    @pytest.mark.asyncio
    async def test_azadd_with_nx(self, cache: KeyValueCache, mk):
        key = mk("ascores_nx")
        await cache._cache.azadd(key, {"alice": 10.0})
        result = await cache._cache.azadd(key, {"alice": 20.0}, nx=True)
        assert result == 0
        assert cache.zscore("ascores_nx", "alice") == 10.0

    @pytest.mark.asyncio
    async def test_azadd_with_xx(self, cache: KeyValueCache, mk):
        key = mk("ascores_xx")
        await cache._cache.azadd(key, {"bob": 15.0})
        result = await cache._cache.azadd(key, {"bob": 25.0}, xx=True)
        assert result == 0
        assert cache.zscore("ascores_xx", "bob") == 25.0

        result = await cache._cache.azadd(key, {"charlie": 30.0}, xx=True)
        assert result == 0
        assert cache.zscore("ascores_xx", "charlie") is None

    @pytest.mark.asyncio
    async def test_azadd_with_ch(self, cache: KeyValueCache, mk):
        key = mk("ascores_ch")
        await cache._cache.azadd(key, {"player1": 100.0})
        result = await cache._cache.azadd(key, {"player1": 150.0, "player2": 200.0}, ch=True)
        assert result == 2


class TestAsyncSortedSetRemove:
    """Tests for azrem."""

    @pytest.mark.asyncio
    async def test_azrem(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_rem", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache._cache.azrem(mk("ascores_rem"), "b")
        assert result == 1
        assert cache.zcard("ascores_rem") == 2

        result = await cache._cache.azrem(mk("ascores_rem"), "a", "c")
        assert result == 2
        assert cache.zcard("ascores_rem") == 0


class TestAsyncSortedSetScore:
    """Tests for azscore, azmscore."""

    @pytest.mark.asyncio
    async def test_azscore(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_s", {"alice": 42.5, "bob": 100.0})
        assert await cache._cache.azscore(mk("ascores_s"), "alice") == 42.5
        assert await cache._cache.azscore(mk("ascores_s"), "bob") == 100.0
        assert await cache._cache.azscore(mk("ascores_s"), "nonexistent") is None

    @pytest.mark.asyncio
    async def test_azmscore(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_ms", {"a": 1.0, "b": 2.0, "c": 3.0})
        scores = await cache._cache.azmscore(mk("ascores_ms"), "a", "c", "nonexistent")
        assert scores == [1.0, 3.0, None]


class TestAsyncSortedSetRank:
    """Tests for azrank, azrevrank."""

    @pytest.mark.asyncio
    async def test_azrank(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_rank", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        key = mk("ascores_rank")
        assert await cache._cache.azrank(key, "alice") == 0
        assert await cache._cache.azrank(key, "charlie") == 1
        assert await cache._cache.azrank(key, "bob") == 2
        assert await cache._cache.azrank(key, "nonexistent") is None

    @pytest.mark.asyncio
    async def test_azrevrank(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_revrank", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        key = mk("ascores_revrank")
        assert await cache._cache.azrevrank(key, "bob") == 0
        assert await cache._cache.azrevrank(key, "charlie") == 1
        assert await cache._cache.azrevrank(key, "alice") == 2
        assert await cache._cache.azrevrank(key, "nonexistent") is None


class TestAsyncSortedSetCard:
    """Tests for azcard, azcount."""

    @pytest.mark.asyncio
    async def test_azcard(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_card", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert await cache._cache.azcard(mk("ascores_card")) == 3
        assert await cache._cache.azcard(mk("anonexistent")) == 0

    @pytest.mark.asyncio
    async def test_azcount(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_count", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        key = mk("ascores_count")
        assert await cache._cache.azcount(key, 2.0, 4.0) == 3
        assert await cache._cache.azcount(key, "-inf", "+inf") == 5
        assert await cache._cache.azcount(key, 10.0, 20.0) == 0


class TestAsyncSortedSetIncr:
    """Tests for azincrby."""

    @pytest.mark.asyncio
    async def test_azincrby(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_incr", {"player1": 100.0})
        key = mk("ascores_incr")
        new_score = await cache._cache.azincrby(key, 50.0, "player1")
        assert new_score == 150.0

        new_score = await cache._cache.azincrby(key, 25.0, "player2")
        assert new_score == 25.0


class TestAsyncSortedSetRange:
    """Tests for azrange, azrevrange, azrangebyscore, azrevrangebyscore."""

    @pytest.mark.asyncio
    async def test_azrange_basic(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_range", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        key = mk("ascores_range")
        result = await cache._cache.azrange(key, 0, -1)
        assert result == ["alice", "charlie", "bob"]
        result = await cache._cache.azrange(key, 0, 1)
        assert result == ["alice", "charlie"]

    @pytest.mark.asyncio
    async def test_azrange_withscores(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_range_ws", {"alice": 10.5, "bob": 20.0, "charlie": 15.5})
        result = await cache._cache.azrange(mk("ascores_range_ws"), 0, -1, withscores=True)
        assert result == [("alice", 10.5), ("charlie", 15.5), ("bob", 20.0)]

    @pytest.mark.asyncio
    async def test_azrevrange(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_rev", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache._cache.azrevrange(mk("ascores_rev"), 0, -1)
        assert result == ["c", "b", "a"]

    @pytest.mark.asyncio
    async def test_azrevrange_withscores(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_rev_ws", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache._cache.azrevrange(mk("ascores_rev_ws"), 0, -1, withscores=True)
        assert result == [("c", 3.0), ("b", 2.0), ("a", 1.0)]

    @pytest.mark.asyncio
    async def test_azrangebyscore(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_rbs", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        key = mk("ascores_rbs")
        result = await cache._cache.azrangebyscore(key, 2.0, 4.0)
        assert result == ["b", "c", "d"]
        result = await cache._cache.azrangebyscore(key, "-inf", 2.0)
        assert result == ["a", "b"]

    @pytest.mark.asyncio
    async def test_azrangebyscore_withscores(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_rbs_ws", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache._cache.azrangebyscore(mk("ascores_rbs_ws"), 1.0, 2.0, withscores=True)
        assert result == [("a", 1.0), ("b", 2.0)]

    @pytest.mark.asyncio
    async def test_azrangebyscore_pagination(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_rbs_pg", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache._cache.azrangebyscore(mk("ascores_rbs_pg"), "-inf", "+inf", start=1, num=2)
        assert len(result) == 2
        assert result == ["b", "c"]

    @pytest.mark.asyncio
    async def test_azrevrangebyscore(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_rrbs", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache._cache.azrevrangebyscore(mk("ascores_rrbs"), 4.0, 2.0)
        assert result == ["d", "c", "b"]


class TestAsyncSortedSetRemoveRange:
    """Tests for azremrangebyrank, azremrangebyscore."""

    @pytest.mark.asyncio
    async def test_azremrangebyrank(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_rrr", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache._cache.azremrangebyrank(mk("ascores_rrr"), 1, 3)
        assert result == 3
        assert cache.zcard("ascores_rrr") == 2
        assert cache.zrange("ascores_rrr", 0, -1) == ["a", "e"]

    @pytest.mark.asyncio
    async def test_azremrangebyscore(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_rrs", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache._cache.azremrangebyscore(mk("ascores_rrs"), 2.0, 4.0)
        assert result == 3
        assert cache.zcard("ascores_rrs") == 2
        assert cache.zrange("ascores_rrs", 0, -1) == ["a", "e"]


class TestAsyncSortedSetPop:
    """Tests for azpopmin, azpopmax."""

    @pytest.mark.asyncio
    async def test_azpopmin(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_pmin", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache._cache.azpopmin(mk("ascores_pmin"))
        assert result == [("a", 1.0)]
        assert cache.zcard("ascores_pmin") == 2

    @pytest.mark.asyncio
    async def test_azpopmin_with_count(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_pmin2", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache._cache.azpopmin(mk("ascores_pmin2"), count=2)
        assert len(result) == 2
        assert result[0] == ("a", 1.0)
        assert result[1] == ("b", 2.0)

    @pytest.mark.asyncio
    async def test_azpopmin_empty(self, cache: KeyValueCache, mk):
        result = await cache._cache.azpopmin(mk("anonexistent_zset"))
        assert result == []

    @pytest.mark.asyncio
    async def test_azpopmax(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_pmax", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache._cache.azpopmax(mk("ascores_pmax"))
        assert result == [("c", 3.0)]
        assert cache.zcard("ascores_pmax") == 2

    @pytest.mark.asyncio
    async def test_azpopmax_with_count(self, cache: KeyValueCache, mk):
        cache.zadd("ascores_pmax2", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache._cache.azpopmax(mk("ascores_pmax2"), count=2)
        assert len(result) == 2
        assert result[0] == ("c", 3.0)
        assert result[1] == ("b", 2.0)

    @pytest.mark.asyncio
    async def test_azpopmax_empty(self, cache: KeyValueCache, mk):
        result = await cache._cache.azpopmax(mk("anonexistent_zset2"))
        assert result == []
