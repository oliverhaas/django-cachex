"""Tests for async hash operations."""

import pytest

from django_cachex.cache import KeyValueCache


class TestAsyncHashSetAndGet:
    """Tests for ahset, ahget, ahmget, ahgetall."""

    @pytest.mark.asyncio
    async def test_ahset_creates_hash_field(self, cache: KeyValueCache):
        await cache.ahset("auser:100", "username", "alice")
        await cache.ahset("auser:100", "email", "alice@example.com")
        assert cache.hlen("auser:100") == 2

    @pytest.mark.asyncio
    async def test_ahget_retrieves_field_value(self, cache: KeyValueCache):
        cache.hset("aproduct:42", "name", "Widget")
        cache.hset("aproduct:42", "metadata", {"color": "blue", "size": "medium"})

        assert await cache.ahget("aproduct:42", "name") == "Widget"
        assert await cache.ahget("aproduct:42", "metadata") == {"color": "blue", "size": "medium"}
        assert await cache.ahget("aproduct:42", "missing_field") is None
        assert await cache.ahget("amissing_hash", "any_field") is None

    @pytest.mark.asyncio
    async def test_ahgetall_returns_all_fields(self, cache: KeyValueCache):
        cache.hset("aconfig:app", "debug", True)
        cache.hset("aconfig:app", "max_connections", 100)
        cache.hset("aconfig:app", "features", ["auth", "logging"])

        result = await cache.ahgetall("aconfig:app")
        assert result == {
            "debug": True,
            "max_connections": 100,
            "features": ["auth", "logging"],
        }
        assert await cache.ahgetall("anonexistent") == {}

    @pytest.mark.asyncio
    async def test_ahmget_retrieves_multiple_fields(self, cache: KeyValueCache):
        cache.hset("asession:abc", "user_id", 123)
        cache.hset("asession:abc", "role", "admin")
        cache.hset("asession:abc", "expires", 3600)

        result = await cache.ahmget("asession:abc", "user_id", "expires", "missing")
        assert result == [123, 3600, None]

    @pytest.mark.asyncio
    async def test_ahset_mapping_creates_multiple_fields(self, cache: KeyValueCache):
        await cache.ahset("aorder:999", mapping={"customer": "Bob", "total": 59.99, "items": 3})

        assert cache.hget("aorder:999", "customer") == "Bob"
        assert cache.hget("aorder:999", "total") == 59.99
        assert cache.hget("aorder:999", "items") == 3
        assert cache.hlen("aorder:999") == 3


class TestAsyncHashDelete:
    """Tests for ahdel."""

    @pytest.mark.asyncio
    async def test_ahdel_removes_field(self, cache: KeyValueCache):
        cache.hset("aprofile:xyz", "name", "Charlie")
        cache.hset("aprofile:xyz", "age", 30)

        deleted = await cache.ahdel("aprofile:xyz", "name")
        assert deleted == 1
        assert cache.hlen("aprofile:xyz") == 1
        assert not cache.hexists("aprofile:xyz", "name")
        assert cache.hexists("aprofile:xyz", "age")


class TestAsyncHashLength:
    """Tests for ahlen."""

    @pytest.mark.asyncio
    async def test_ahlen_counts_fields(self, cache: KeyValueCache):
        assert await cache.ahlen("aempty_hash") == 0
        cache.hset("agrowing_hash", "f1", "v1")
        assert await cache.ahlen("agrowing_hash") == 1
        cache.hset("agrowing_hash", "f2", "v2")
        assert await cache.ahlen("agrowing_hash") == 2


class TestAsyncHashExists:
    """Tests for ahexists."""

    @pytest.mark.asyncio
    async def test_ahexists_checks_field_presence(self, cache: KeyValueCache):
        cache.hset("asettings:user", "theme", "dark")
        assert await cache.ahexists("asettings:user", "theme") is True
        assert await cache.ahexists("asettings:user", "language") is False


class TestAsyncHashKeys:
    """Tests for ahkeys."""

    @pytest.mark.asyncio
    async def test_ahkeys_returns_all_field_names(self, cache: KeyValueCache):
        cache.hset("ainventory:001", "apples", 50)
        cache.hset("ainventory:001", "oranges", 30)
        cache.hset("ainventory:001", "bananas", 25)

        keys = await cache.ahkeys("ainventory:001")
        assert len(keys) == 3
        assert set(keys) == {"apples", "oranges", "bananas"}


class TestAsyncHashValues:
    """Tests for ahvals."""

    @pytest.mark.asyncio
    async def test_ahvals_returns_all_values(self, cache: KeyValueCache):
        cache.hset("anumbers", "one", 1)
        cache.hset("anumbers", "two", 2)
        cache.hset("anumbers", "three", 3)

        values = await cache.ahvals("anumbers")
        assert len(values) == 3
        assert set(values) == {1, 2, 3}

        assert await cache.ahvals("aempty_hash_vals") == []


class TestAsyncHashSetNX:
    """Tests for ahsetnx."""

    @pytest.mark.asyncio
    async def test_ahsetnx_sets_only_if_not_exists(self, cache: KeyValueCache):
        result = await cache.ahsetnx("aunique_hash", "key1", "first")
        assert result is True
        assert cache.hget("aunique_hash", "key1") == "first"

        result = await cache.ahsetnx("aunique_hash", "key1", "second")
        assert result is False
        assert cache.hget("aunique_hash", "key1") == "first"


class TestAsyncHashIncrementOperations:
    """Tests for ahincrby and ahincrbyfloat."""

    @pytest.mark.asyncio
    async def test_ahincrby_increments_integer(self, cache: KeyValueCache):
        cache.hset("acounters", "views", 100)

        result = await cache.ahincrby("acounters", "views", 10)
        assert result == 110

        result = await cache.ahincrby("acounters", "views", -5)
        assert result == 105

    @pytest.mark.asyncio
    async def test_ahincrby_creates_field_if_missing(self, cache: KeyValueCache):
        result = await cache.ahincrby("anew_counters", "clicks", 1)
        assert result == 1

    @pytest.mark.asyncio
    async def test_ahincrbyfloat_increments_float(self, cache: KeyValueCache):
        await cache.ahincrbyfloat("ametrics", "score", 10.5)

        result = await cache.ahincrbyfloat("ametrics", "score", 0.25)
        assert abs(result - 10.75) < 0.001

        result = await cache.ahincrbyfloat("ametrics", "score", -2.0)
        assert abs(result - 8.75) < 0.001

    @pytest.mark.asyncio
    async def test_ahincrbyfloat_creates_field_if_missing(self, cache: KeyValueCache):
        result = await cache.ahincrbyfloat("anew_metrics", "rate", 2.718)
        assert abs(result - 2.718) < 0.001
