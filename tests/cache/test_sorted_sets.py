from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


class TestSortedSetOperations:
    """Tests for sorted set (ZSET) operations."""

    def test_zadd_basic(self, cache: RespCache):
        result = cache.zadd("scores", {"player1": 100.0, "player2": 200.0})
        assert result == 2
        assert cache.zcard("scores") == 2

    def test_zadd_with_nx(self, cache: RespCache):
        """Test zadd with nx flag (only add new)."""
        cache.zadd("scores", {"alice": 10.0})
        result = cache.zadd("scores", {"alice": 20.0}, nx=True)
        assert result == 0
        assert cache.zscore("scores", "alice") == 10.0

    def test_zadd_with_xx(self, cache: RespCache):
        """Test zadd with xx flag (only update existing)."""
        cache.zadd("scores", {"bob": 15.0})
        result = cache.zadd("scores", {"bob": 25.0}, xx=True)
        assert result == 0  # No new members added
        assert cache.zscore("scores", "bob") == 25.0
        result = cache.zadd("scores", {"charlie": 30.0}, xx=True)
        assert result == 0
        assert cache.zscore("scores", "charlie") is None

    def test_zadd_with_ch(self, cache: RespCache):
        cache.zadd("scores", {"player1": 100.0})
        result = cache.zadd("scores", {"player1": 150.0, "player2": 200.0}, ch=True)
        assert result == 2  # 1 changed + 1 added

    def test_zcard(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert cache.zcard("scores") == 3
        assert cache.zcard("nonexistent") == 0

    def test_zcount(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        assert cache.zcount("scores", 2.0, 4.0) == 3  # b, c, d
        assert cache.zcount("scores", "-inf", "+inf") == 5
        assert cache.zcount("scores", 10.0, 20.0) == 0

    def test_zincrby(self, cache: RespCache):
        cache.zadd("scores", {"player1": 100.0})
        new_score = cache.zincrby("scores", 50.0, "player1")
        assert new_score == 150.0
        assert cache.zscore("scores", "player1") == 150.0
        new_score = cache.zincrby("scores", 25.0, "player2")
        assert new_score == 25.0

    def test_zpopmax(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = cache.zpopmax("scores")
        assert result == [("c", 3.0)]
        assert cache.zcard("scores") == 2
        cache.zadd("scores", {"d": 4.0, "e": 5.0})
        result = cache.zpopmax("scores", count=2)
        assert len(result) == 2
        assert result[0][0] == "e" and result[0][1] == 5.0
        assert result[1][0] == "d" and result[1][1] == 4.0

    def test_zpopmin(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = cache.zpopmin("scores")
        assert result == [("a", 1.0)]
        assert cache.zcard("scores") == 2
        cache.zadd("scores", {"d": 0.5, "e": 0.1})
        result = cache.zpopmin("scores", count=2)
        assert len(result) == 2
        assert result[0][0] == "e" and result[0][1] == 0.1
        assert result[1][0] == "d" and result[1][1] == 0.5

    def test_zrange_basic(self, cache: RespCache):
        cache.zadd("scores", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        result = cache.zrange("scores", 0, -1)
        assert result == ["alice", "charlie", "bob"]
        result = cache.zrange("scores", 0, 1)
        assert result == ["alice", "charlie"]

    def test_zrange_withscores(self, cache: RespCache):
        cache.zadd("scores", {"alice": 10.5, "bob": 20.0, "charlie": 15.5})
        result = cache.zrange("scores", 0, -1, withscores=True)
        assert result == [("alice", 10.5), ("charlie", 15.5), ("bob", 20.0)]

    def test_zrevrange(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = cache.zrevrange("scores", 0, -1)
        assert result == ["c", "b", "a"]

    def test_zrangebyscore(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = cache.zrangebyscore("scores", 2.0, 4.0)
        assert result == ["b", "c", "d"]
        result = cache.zrangebyscore("scores", "-inf", 2.0)
        assert result == ["a", "b"]

    def test_zrangebyscore_withscores(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = cache.zrangebyscore("scores", 1.0, 2.0, withscores=True)
        assert result == [("a", 1.0), ("b", 2.0)]

    def test_zrangebyscore_pagination(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = cache.zrangebyscore("scores", "-inf", "+inf", start=1, num=2)
        assert len(result) == 2
        assert result == ["b", "c"]

    def test_zrank(self, cache: RespCache):
        cache.zadd("scores", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        assert cache.zrank("scores", "alice") == 0  # Lowest score
        assert cache.zrank("scores", "charlie") == 1
        assert cache.zrank("scores", "bob") == 2
        assert cache.zrank("scores", "nonexistent") is None

    def test_zrem(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = cache.zrem("scores", "b")
        assert result == 1
        assert cache.zcard("scores") == 2
        result = cache.zrem("scores", "a", "c")
        assert result == 2
        assert cache.zcard("scores") == 0

    def test_zremrangebyscore(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = cache.zremrangebyscore("scores", 2.0, 4.0)
        assert result == 3  # b, c, d removed
        assert cache.zcard("scores") == 2
        assert cache.zrange("scores", 0, -1) == ["a", "e"]

    def test_zrevrange_withscores(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = cache.zrevrange("scores", 0, -1, withscores=True)
        assert result == [("c", 3.0), ("b", 2.0), ("a", 1.0)]

    def test_zrevrangebyscore(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = cache.zrevrangebyscore("scores", 4.0, 2.0)
        assert result == ["d", "c", "b"]

    def test_zscore(self, cache: RespCache):
        cache.zadd("scores", {"alice": 42.5, "bob": 100.0})
        assert cache.zscore("scores", "alice") == 42.5
        assert cache.zscore("scores", "bob") == 100.0
        assert cache.zscore("scores", "nonexistent") is None

    def test_sorted_set_serialization(self, cache: RespCache):
        cache.zadd("complex", {("tuple", "key"): 1.0, "string": 2.0})
        result = cache.zrange("complex", 0, -1)
        assert ("tuple", "key") in result or ["tuple", "key"] in result
        assert "string" in result

    def test_sorted_set_version_support(self, cache: RespCache):
        cache.zadd("data", {"v1": 1.0}, version=1)
        cache.zadd("data", {"v2": 2.0}, version=2)

        assert cache.zcard("data", version=1) == 1
        assert cache.zcard("data", version=2) == 1
        assert cache.zrange("data", 0, -1, version=1) == ["v1"]
        assert cache.zrange("data", 0, -1, version=2) == ["v2"]

    def test_sorted_set_float_scores(self, cache: RespCache):
        cache.zadd("precise", {"a": 1.1, "b": 1.2, "c": 1.15})
        result = cache.zrange("precise", 0, -1, withscores=True)
        assert result[0] == ("a", 1.1)
        assert result[1] == ("c", 1.15)
        assert result[2] == ("b", 1.2)

    def test_sorted_set_negative_scores(self, cache: RespCache):
        cache.zadd("temps", {"freezing": -10.0, "cold": 0.0, "warm": 20.0})
        result = cache.zrange("temps", 0, -1)
        assert result == ["freezing", "cold", "warm"]

    def test_zpopmin_empty_set(self, cache: RespCache):
        result = cache.zpopmin("nonexistent")
        assert result == []
        result = cache.zpopmin("nonexistent", count=5)
        assert result == []

    def test_zpopmax_empty_set(self, cache: RespCache):
        result = cache.zpopmax("nonexistent")
        assert result == []
        result = cache.zpopmax("nonexistent", count=5)
        assert result == []

    def test_zrevrank(self, cache: RespCache):
        """Test getting reverse rank (highest score = 0)."""
        cache.zadd("scores", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        assert cache.zrevrank("scores", "bob") == 0  # Highest score
        assert cache.zrevrank("scores", "charlie") == 1
        assert cache.zrevrank("scores", "alice") == 2
        assert cache.zrevrank("scores", "nonexistent") is None

    def test_zmscore(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        scores = cache.zmscore("scores", "a", "c", "nonexistent")
        assert scores == [1.0, 3.0, None]

    def test_zremrangebyrank(self, cache: RespCache):
        cache.zadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        # Remove elements at rank 1-3 (b, c, d)
        result = cache.zremrangebyrank("scores", 1, 3)
        assert result == 3
        assert cache.zcard("scores") == 2
        assert cache.zrange("scores", 0, -1) == ["a", "e"]


class TestAsyncSortedSetAdd:
    """Tests for azadd."""

    @pytest.mark.asyncio
    async def test_azadd_basic(self, cache: RespCache):
        result = await cache.azadd("ascores", {"player1": 100.0, "player2": 200.0})
        assert result == 2
        assert cache.zcard("ascores") == 2

    @pytest.mark.asyncio
    async def test_azadd_with_nx(self, cache: RespCache):
        await cache.azadd("ascores_nx", {"alice": 10.0})
        result = await cache.azadd("ascores_nx", {"alice": 20.0}, nx=True)
        assert result == 0
        assert cache.zscore("ascores_nx", "alice") == 10.0

    @pytest.mark.asyncio
    async def test_azadd_with_xx(self, cache: RespCache):
        await cache.azadd("ascores_xx", {"bob": 15.0})
        result = await cache.azadd("ascores_xx", {"bob": 25.0}, xx=True)
        assert result == 0
        assert cache.zscore("ascores_xx", "bob") == 25.0

        result = await cache.azadd("ascores_xx", {"charlie": 30.0}, xx=True)
        assert result == 0
        assert cache.zscore("ascores_xx", "charlie") is None

    @pytest.mark.asyncio
    async def test_azadd_with_ch(self, cache: RespCache):
        await cache.azadd("ascores_ch", {"player1": 100.0})
        result = await cache.azadd("ascores_ch", {"player1": 150.0, "player2": 200.0}, ch=True)
        assert result == 2


class TestAsyncSortedSetRemove:
    """Tests for azrem."""

    @pytest.mark.asyncio
    async def test_azrem(self, cache: RespCache):
        cache.zadd("ascores_rem", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azrem("ascores_rem", "b")
        assert result == 1
        assert cache.zcard("ascores_rem") == 2

        result = await cache.azrem("ascores_rem", "a", "c")
        assert result == 2
        assert cache.zcard("ascores_rem") == 0


class TestAsyncSortedSetScore:
    """Tests for azscore, azmscore."""

    @pytest.mark.asyncio
    async def test_azscore(self, cache: RespCache):
        cache.zadd("ascores_s", {"alice": 42.5, "bob": 100.0})
        assert await cache.azscore("ascores_s", "alice") == 42.5
        assert await cache.azscore("ascores_s", "bob") == 100.0
        assert await cache.azscore("ascores_s", "nonexistent") is None

    @pytest.mark.asyncio
    async def test_azmscore(self, cache: RespCache):
        cache.zadd("ascores_ms", {"a": 1.0, "b": 2.0, "c": 3.0})
        scores = await cache.azmscore("ascores_ms", "a", "c", "nonexistent")
        assert scores == [1.0, 3.0, None]


class TestAsyncSortedSetRank:
    """Tests for azrank, azrevrank."""

    @pytest.mark.asyncio
    async def test_azrank(self, cache: RespCache):
        cache.zadd("ascores_rank", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        assert await cache.azrank("ascores_rank", "alice") == 0
        assert await cache.azrank("ascores_rank", "charlie") == 1
        assert await cache.azrank("ascores_rank", "bob") == 2
        assert await cache.azrank("ascores_rank", "nonexistent") is None

    @pytest.mark.asyncio
    async def test_azrevrank(self, cache: RespCache):
        cache.zadd("ascores_revrank", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        assert await cache.azrevrank("ascores_revrank", "bob") == 0
        assert await cache.azrevrank("ascores_revrank", "charlie") == 1
        assert await cache.azrevrank("ascores_revrank", "alice") == 2
        assert await cache.azrevrank("ascores_revrank", "nonexistent") is None


class TestAsyncSortedSetCard:
    """Tests for azcard, azcount."""

    @pytest.mark.asyncio
    async def test_azcard(self, cache: RespCache):
        cache.zadd("ascores_card", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert await cache.azcard("ascores_card") == 3
        assert await cache.azcard("anonexistent") == 0

    @pytest.mark.asyncio
    async def test_azcount(self, cache: RespCache):
        cache.zadd("ascores_count", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        assert await cache.azcount("ascores_count", 2.0, 4.0) == 3
        assert await cache.azcount("ascores_count", "-inf", "+inf") == 5
        assert await cache.azcount("ascores_count", 10.0, 20.0) == 0


class TestAsyncSortedSetIncr:
    """Tests for azincrby."""

    @pytest.mark.asyncio
    async def test_azincrby(self, cache: RespCache):
        cache.zadd("ascores_incr", {"player1": 100.0})
        new_score = await cache.azincrby("ascores_incr", 50.0, "player1")
        assert new_score == 150.0

        new_score = await cache.azincrby("ascores_incr", 25.0, "player2")
        assert new_score == 25.0


class TestAsyncSortedSetRange:
    """Tests for azrange, azrevrange, azrangebyscore, azrevrangebyscore."""

    @pytest.mark.asyncio
    async def test_azrange_basic(self, cache: RespCache):
        cache.zadd("ascores_range", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        result = await cache.azrange("ascores_range", 0, -1)
        assert result == ["alice", "charlie", "bob"]
        result = await cache.azrange("ascores_range", 0, 1)
        assert result == ["alice", "charlie"]

    @pytest.mark.asyncio
    async def test_azrange_withscores(self, cache: RespCache):
        cache.zadd("ascores_range_ws", {"alice": 10.5, "bob": 20.0, "charlie": 15.5})
        result = await cache.azrange("ascores_range_ws", 0, -1, withscores=True)
        assert result == [("alice", 10.5), ("charlie", 15.5), ("bob", 20.0)]

    @pytest.mark.asyncio
    async def test_azrevrange(self, cache: RespCache):
        cache.zadd("ascores_rev", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azrevrange("ascores_rev", 0, -1)
        assert result == ["c", "b", "a"]

    @pytest.mark.asyncio
    async def test_azrevrange_withscores(self, cache: RespCache):
        cache.zadd("ascores_rev_ws", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azrevrange("ascores_rev_ws", 0, -1, withscores=True)
        assert result == [("c", 3.0), ("b", 2.0), ("a", 1.0)]

    @pytest.mark.asyncio
    async def test_azrangebyscore(self, cache: RespCache):
        cache.zadd("ascores_rbs", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache.azrangebyscore("ascores_rbs", 2.0, 4.0)
        assert result == ["b", "c", "d"]
        result = await cache.azrangebyscore("ascores_rbs", "-inf", 2.0)
        assert result == ["a", "b"]

    @pytest.mark.asyncio
    async def test_azrangebyscore_withscores(self, cache: RespCache):
        cache.zadd("ascores_rbs_ws", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azrangebyscore("ascores_rbs_ws", 1.0, 2.0, withscores=True)
        assert result == [("a", 1.0), ("b", 2.0)]

    @pytest.mark.asyncio
    async def test_azrangebyscore_pagination(self, cache: RespCache):
        cache.zadd("ascores_rbs_pg", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache.azrangebyscore("ascores_rbs_pg", "-inf", "+inf", start=1, num=2)
        assert len(result) == 2
        assert result == ["b", "c"]

    @pytest.mark.asyncio
    async def test_azrevrangebyscore(self, cache: RespCache):
        cache.zadd("ascores_rrbs", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache.azrevrangebyscore("ascores_rrbs", 4.0, 2.0)
        assert result == ["d", "c", "b"]


class TestAsyncSortedSetRemoveRange:
    """Tests for azremrangebyrank, azremrangebyscore."""

    @pytest.mark.asyncio
    async def test_azremrangebyrank(self, cache: RespCache):
        cache.zadd("ascores_rrr", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache.azremrangebyrank("ascores_rrr", 1, 3)
        assert result == 3
        assert cache.zcard("ascores_rrr") == 2
        assert cache.zrange("ascores_rrr", 0, -1) == ["a", "e"]

    @pytest.mark.asyncio
    async def test_azremrangebyscore(self, cache: RespCache):
        cache.zadd("ascores_rrs", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache.azremrangebyscore("ascores_rrs", 2.0, 4.0)
        assert result == 3
        assert cache.zcard("ascores_rrs") == 2
        assert cache.zrange("ascores_rrs", 0, -1) == ["a", "e"]


class TestAsyncSortedSetPop:
    """Tests for azpopmin, azpopmax."""

    @pytest.mark.asyncio
    async def test_azpopmin(self, cache: RespCache):
        cache.zadd("ascores_pmin", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azpopmin("ascores_pmin")
        assert result == [("a", 1.0)]
        assert cache.zcard("ascores_pmin") == 2

    @pytest.mark.asyncio
    async def test_azpopmin_with_count(self, cache: RespCache):
        cache.zadd("ascores_pmin2", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azpopmin("ascores_pmin2", count=2)
        assert len(result) == 2
        assert result[0] == ("a", 1.0)
        assert result[1] == ("b", 2.0)

    @pytest.mark.asyncio
    async def test_azpopmin_empty(self, cache: RespCache):
        result = await cache.azpopmin("anonexistent_zset")
        assert result == []
        result = await cache.azpopmin("anonexistent_zset", count=5)
        assert result == []

    @pytest.mark.asyncio
    async def test_azpopmax(self, cache: RespCache):
        cache.zadd("ascores_pmax", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azpopmax("ascores_pmax")
        assert result == [("c", 3.0)]
        assert cache.zcard("ascores_pmax") == 2

    @pytest.mark.asyncio
    async def test_azpopmax_with_count(self, cache: RespCache):
        cache.zadd("ascores_pmax2", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azpopmax("ascores_pmax2", count=2)
        assert len(result) == 2
        assert result[0] == ("c", 3.0)
        assert result[1] == ("b", 2.0)

    @pytest.mark.asyncio
    async def test_azpopmax_empty(self, cache: RespCache):
        result = await cache.azpopmax("anonexistent_zset2")
        assert result == []
        result = await cache.azpopmax("anonexistent_zset2", count=5)
        assert result == []


class TestAsyncSortedSetSerialization:
    """Tests for async sorted-set serialization, version, and score-edge cases."""

    @pytest.mark.asyncio
    async def test_asorted_set_serialization(self, cache: RespCache):
        await cache.azadd("acomplex", {("tuple", "key"): 1.0, "string": 2.0})
        result = await cache.azrange("acomplex", 0, -1)
        assert ("tuple", "key") in result or ["tuple", "key"] in result
        assert "string" in result

    @pytest.mark.asyncio
    async def test_asorted_set_version_support(self, cache: RespCache):
        await cache.azadd("adata", {"v1": 1.0}, version=1)
        await cache.azadd("adata", {"v2": 2.0}, version=2)

        assert await cache.azcard("adata", version=1) == 1
        assert await cache.azcard("adata", version=2) == 1
        assert await cache.azrange("adata", 0, -1, version=1) == ["v1"]
        assert await cache.azrange("adata", 0, -1, version=2) == ["v2"]

    @pytest.mark.asyncio
    async def test_asorted_set_float_scores(self, cache: RespCache):
        await cache.azadd("aprecise", {"a": 1.1, "b": 1.2, "c": 1.15})
        result = await cache.azrange("aprecise", 0, -1, withscores=True)
        assert result[0] == ("a", 1.1)
        assert result[1] == ("c", 1.15)
        assert result[2] == ("b", 1.2)

    @pytest.mark.asyncio
    async def test_asorted_set_negative_scores(self, cache: RespCache):
        await cache.azadd("atemps", {"freezing": -10.0, "cold": 0.0, "warm": 20.0})
        result = await cache.azrange("atemps", 0, -1)
        assert result == ["freezing", "cold", "warm"]
