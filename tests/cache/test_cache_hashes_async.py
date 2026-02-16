"""Tests for async hash operations."""

import pytest

from django_cachex.cache import KeyValueCache


@pytest.fixture
def mk(cache: KeyValueCache):
    """Create a prefixed key for direct client async testing."""
    return lambda key, version=None: cache.make_and_validate_key(key, version=version)


class TestAsyncHashSetAndGet:
    """Tests for ahset, ahget, ahmget, ahgetall."""

    @pytest.mark.asyncio
    async def test_ahset_creates_hash_field(self, cache: KeyValueCache, mk):
        key = mk("auser:100")
        await cache._cache.ahset(key, "username", "alice")
        await cache._cache.ahset(key, "email", "alice@example.com")
        assert cache.hlen("auser:100") == 2

    @pytest.mark.asyncio
    async def test_ahget_retrieves_field_value(self, cache: KeyValueCache, mk):
        cache.hset("aproduct:42", "name", "Widget")
        cache.hset("aproduct:42", "metadata", {"color": "blue", "size": "medium"})

        key = mk("aproduct:42")
        assert await cache._cache.ahget(key, "name") == "Widget"
        assert await cache._cache.ahget(key, "metadata") == {"color": "blue", "size": "medium"}
        assert await cache._cache.ahget(key, "missing_field") is None
        assert await cache._cache.ahget(mk("amissing_hash"), "any_field") is None

    @pytest.mark.asyncio
    async def test_ahgetall_returns_all_fields(self, cache: KeyValueCache, mk):
        cache.hset("aconfig:app", "debug", True)
        cache.hset("aconfig:app", "max_connections", 100)
        cache.hset("aconfig:app", "features", ["auth", "logging"])

        result = await cache._cache.ahgetall(mk("aconfig:app"))
        assert result == {
            "debug": True,
            "max_connections": 100,
            "features": ["auth", "logging"],
        }
        assert await cache._cache.ahgetall(mk("anonexistent")) == {}

    @pytest.mark.asyncio
    async def test_ahmget_retrieves_multiple_fields(self, cache: KeyValueCache, mk):
        cache.hset("asession:abc", "user_id", 123)
        cache.hset("asession:abc", "role", "admin")
        cache.hset("asession:abc", "expires", 3600)

        result = await cache._cache.ahmget(mk("asession:abc"), "user_id", "expires", "missing")
        assert result == [123, 3600, None]

    @pytest.mark.asyncio
    async def test_ahset_mapping_creates_multiple_fields(self, cache: KeyValueCache, mk):
        key = mk("aorder:999")
        await cache._cache.ahset(key, mapping={"customer": "Bob", "total": 59.99, "items": 3})

        assert cache.hget("aorder:999", "customer") == "Bob"
        assert cache.hget("aorder:999", "total") == 59.99
        assert cache.hget("aorder:999", "items") == 3
        assert cache.hlen("aorder:999") == 3


class TestAsyncHashDelete:
    """Tests for ahdel."""

    @pytest.mark.asyncio
    async def test_ahdel_removes_field(self, cache: KeyValueCache, mk):
        cache.hset("aprofile:xyz", "name", "Charlie")
        cache.hset("aprofile:xyz", "age", 30)

        deleted = await cache._cache.ahdel(mk("aprofile:xyz"), "name")
        assert deleted == 1
        assert cache.hlen("aprofile:xyz") == 1
        assert not cache.hexists("aprofile:xyz", "name")
        assert cache.hexists("aprofile:xyz", "age")


class TestAsyncHashLength:
    """Tests for ahlen."""

    @pytest.mark.asyncio
    async def test_ahlen_counts_fields(self, cache: KeyValueCache, mk):
        assert await cache._cache.ahlen(mk("aempty_hash")) == 0
        cache.hset("agrowing_hash", "f1", "v1")
        assert await cache._cache.ahlen(mk("agrowing_hash")) == 1
        cache.hset("agrowing_hash", "f2", "v2")
        assert await cache._cache.ahlen(mk("agrowing_hash")) == 2


class TestAsyncHashExists:
    """Tests for ahexists."""

    @pytest.mark.asyncio
    async def test_ahexists_checks_field_presence(self, cache: KeyValueCache, mk):
        cache.hset("asettings:user", "theme", "dark")
        assert await cache._cache.ahexists(mk("asettings:user"), "theme") is True
        assert await cache._cache.ahexists(mk("asettings:user"), "language") is False


class TestAsyncHashKeys:
    """Tests for ahkeys."""

    @pytest.mark.asyncio
    async def test_ahkeys_returns_all_field_names(self, cache: KeyValueCache, mk):
        cache.hset("ainventory:001", "apples", 50)
        cache.hset("ainventory:001", "oranges", 30)
        cache.hset("ainventory:001", "bananas", 25)

        keys = await cache._cache.ahkeys(mk("ainventory:001"))
        assert len(keys) == 3
        assert set(keys) == {"apples", "oranges", "bananas"}


class TestAsyncHashValues:
    """Tests for ahvals."""

    @pytest.mark.asyncio
    async def test_ahvals_returns_all_values(self, cache: KeyValueCache, mk):
        cache.hset("anumbers", "one", 1)
        cache.hset("anumbers", "two", 2)
        cache.hset("anumbers", "three", 3)

        values = await cache._cache.ahvals(mk("anumbers"))
        assert len(values) == 3
        assert set(values) == {1, 2, 3}

        assert await cache._cache.ahvals(mk("aempty_hash_vals")) == []


class TestAsyncHashSetNX:
    """Tests for ahsetnx."""

    @pytest.mark.asyncio
    async def test_ahsetnx_sets_only_if_not_exists(self, cache: KeyValueCache, mk):
        key = mk("aunique_hash")
        result = await cache._cache.ahsetnx(key, "key1", "first")
        assert result is True
        assert cache.hget("aunique_hash", "key1") == "first"

        result = await cache._cache.ahsetnx(key, "key1", "second")
        assert result is False
        assert cache.hget("aunique_hash", "key1") == "first"


class TestAsyncHashIncrementOperations:
    """Tests for ahincrby and ahincrbyfloat."""

    @pytest.mark.asyncio
    async def test_ahincrby_increments_integer(self, cache: KeyValueCache, mk):
        cache.hset("acounters", "views", 100)

        result = await cache._cache.ahincrby(mk("acounters"), "views", 10)
        assert result == 110

        result = await cache._cache.ahincrby(mk("acounters"), "views", -5)
        assert result == 105

    @pytest.mark.asyncio
    async def test_ahincrby_creates_field_if_missing(self, cache: KeyValueCache, mk):
        result = await cache._cache.ahincrby(mk("anew_counters"), "clicks", 1)
        assert result == 1

    @pytest.mark.asyncio
    async def test_ahincrbyfloat_increments_float(self, cache: KeyValueCache, mk):
        key = mk("ametrics")
        await cache._cache.ahincrbyfloat(key, "score", 10.5)

        result = await cache._cache.ahincrbyfloat(key, "score", 0.25)
        assert abs(result - 10.75) < 0.001

        result = await cache._cache.ahincrbyfloat(key, "score", -2.0)
        assert abs(result - 8.75) < 0.001

    @pytest.mark.asyncio
    async def test_ahincrbyfloat_creates_field_if_missing(self, cache: KeyValueCache, mk):
        result = await cache._cache.ahincrbyfloat(mk("anew_metrics"), "rate", 2.718)
        assert abs(result - 2.718) < 0.001
