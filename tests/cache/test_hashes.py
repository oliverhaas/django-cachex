"""Tests for hash operations."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


class TestHashSetAndGet:
    """Tests for hset, hget, hmget, hgetall."""

    def test_hset_creates_hash_field(self, cache: RespCache):
        cache.hset("user:100", "username", "alice")
        cache.hset("user:100", "email", "alice@example.com")
        assert cache.hlen("user:100") == 2
        assert cache.hexists("user:100", "username")
        assert cache.hexists("user:100", "email")

    def test_hget_retrieves_field_value(self, cache: RespCache):
        cache.hset("product:42", "name", "Widget")
        cache.hset("product:42", "metadata", {"color": "blue", "size": "medium"})

        assert cache.hget("product:42", "name") == "Widget"
        assert cache.hget("product:42", "metadata") == {"color": "blue", "size": "medium"}
        assert cache.hget("product:42", "missing_field") is None
        assert cache.hget("missing_hash", "any_field") is None

    def test_hgetall_returns_all_fields(self, cache: RespCache):
        cache.hset("config:app", "debug", True)
        cache.hset("config:app", "max_connections", 100)
        cache.hset("config:app", "features", ["auth", "logging"])

        result = cache.hgetall("config:app")
        assert result == {
            "debug": True,
            "max_connections": 100,
            "features": ["auth", "logging"],
        }
        assert cache.hgetall("nonexistent") == {}

    def test_hmget_retrieves_multiple_fields(self, cache: RespCache):
        cache.hset("session:abc", "user_id", 123)
        cache.hset("session:abc", "role", "admin")
        cache.hset("session:abc", "expires", 3600)

        result = cache.hmget("session:abc", "user_id", "expires", "missing")
        assert result == [123, 3600, None]

    def test_hset_mapping_creates_multiple_fields(self, cache: RespCache):
        cache.hset("order:999", mapping={"customer": "Bob", "total": 59.99, "items": 3})

        assert cache.hget("order:999", "customer") == "Bob"
        assert cache.hget("order:999", "total") == 59.99
        assert cache.hget("order:999", "items") == 3
        assert cache.hlen("order:999") == 3


class TestHashDelete:
    """Tests for hdel."""

    def test_hdel_removes_field(self, cache: RespCache):
        cache.hset("profile:xyz", "name", "Charlie")
        cache.hset("profile:xyz", "age", 30)
        assert cache.hlen("profile:xyz") == 2

        deleted = cache.hdel("profile:xyz", "name")
        assert deleted == 1
        assert cache.hlen("profile:xyz") == 1
        assert not cache.hexists("profile:xyz", "name")
        assert cache.hexists("profile:xyz", "age")


class TestHashLength:
    """Tests for hlen."""

    def test_hlen_counts_fields(self, cache: RespCache):
        assert cache.hlen("empty_hash") == 0
        cache.hset("growing_hash", "f1", "v1")
        assert cache.hlen("growing_hash") == 1
        cache.hset("growing_hash", "f2", "v2")
        assert cache.hlen("growing_hash") == 2


class TestHashKeys:
    """Tests for hkeys."""

    def test_hkeys_returns_all_field_names(self, cache: RespCache):
        cache.hset("inventory:001", "apples", 50)
        cache.hset("inventory:001", "oranges", 30)
        cache.hset("inventory:001", "bananas", 25)

        keys = cache.hkeys("inventory:001")
        assert len(keys) == 3
        assert set(keys) == {"apples", "oranges", "bananas"}


class TestHashExists:
    """Tests for hexists."""

    def test_hexists_checks_field_presence(self, cache: RespCache):
        cache.hset("settings:user", "theme", "dark")
        assert cache.hexists("settings:user", "theme") is True
        assert cache.hexists("settings:user", "language") is False


class TestHashVersionSupport:
    """Tests for version parameter on hash operations."""

    def test_different_versions_are_independent(self, cache: RespCache):
        cache.hset("data", "key", "value_v1", version=1)
        cache.hset("data", "other", "other_v1", version=1)
        cache.hset("data", "key", "value_v2", version=2)

        assert cache.hexists("data", "key", version=1)
        assert cache.hexists("data", "other", version=1)
        assert cache.hexists("data", "key", version=2)
        assert not cache.hexists("data", "other", version=2)

        assert cache.hlen("data", version=1) == 2
        assert cache.hlen("data", version=2) == 1

        keys_v1 = cache.hkeys("data", version=1)
        assert set(keys_v1) == {"key", "other"}

        keys_v2 = cache.hkeys("data", version=2)
        assert keys_v2 == ["key"]

        cache.hdel("data", "key", version=1)
        assert not cache.hexists("data", "key", version=1)
        assert cache.hexists("data", "key", version=2)


class TestHashKeyPrefixing:
    """Tests verifying hash keys use prefixes but fields do not."""

    def test_key_prefixed_but_fields_raw(self, cache: RespCache):
        cache.hset("account:500", "balance", 1000.00, version=2)
        cache.hset("account:500", "currency", "USD", version=2)

        prefixed_key = cache.make_key("account:500", version=2)

        # Adapter-level exists / type / hkeys, so this test stays driver-agnostic.
        adapter = cache.adapter
        assert adapter.has_key(prefixed_key)
        key_type = adapter.type(prefixed_key)
        if isinstance(key_type, bytes):
            key_type = key_type.decode()
        assert key_type == "hash"

        raw_fields = {f.decode() if isinstance(f, bytes) else f for f in adapter.hkeys(prefixed_key)}
        assert "balance" in raw_fields
        assert "currency" in raw_fields


class TestHashIncrementOperations:
    """Tests for hincrby and hincrbyfloat."""

    def test_hincrby_increments_integer(self, cache: RespCache):
        cache.hset("counters", "views", 100)

        result = cache.hincrby("counters", "views", 10)
        assert result == 110

        result = cache.hincrby("counters", "views", -5)
        assert result == 105

    def test_hincrby_creates_field_if_missing(self, cache: RespCache):
        result = cache.hincrby("new_counters", "clicks", 1)
        assert result == 1

    def test_hincrbyfloat_increments_float(self, cache: RespCache):
        cache.hincrbyfloat("metrics", "score", 10.5)

        result = cache.hincrbyfloat("metrics", "score", 0.25)
        assert abs(result - 10.75) < 0.001

        result = cache.hincrbyfloat("metrics", "score", -2.0)
        assert abs(result - 8.75) < 0.001

    def test_hincrbyfloat_creates_field_if_missing(self, cache: RespCache):
        result = cache.hincrbyfloat("new_metrics", "rate", 2.718)
        assert abs(result - 2.718) < 0.001


class TestHashSetNX:
    """Tests for hsetnx."""

    def test_hsetnx_sets_only_if_not_exists(self, cache: RespCache):
        result = cache.hsetnx("unique_hash", "key1", "first")
        assert result is True
        assert cache.hget("unique_hash", "key1") == "first"

        result = cache.hsetnx("unique_hash", "key1", "second")
        assert result is False
        assert cache.hget("unique_hash", "key1") == "first"


class TestHashValues:
    """Tests for hvals."""

    def test_hvals_returns_all_values(self, cache: RespCache):
        cache.hset("numbers", "one", 1)
        cache.hset("numbers", "two", 2)
        cache.hset("numbers", "three", 3)

        values = cache.hvals("numbers")
        assert len(values) == 3
        assert set(values) == {1, 2, 3}

        assert cache.hvals("empty_hash") == []


class TestAsyncHashSetAndGet:
    """Tests for ahset, ahget, ahmget, ahgetall."""

    @pytest.mark.asyncio
    async def test_ahset_creates_hash_field(self, cache: RespCache):
        await cache.ahset("auser:100", "username", "alice")
        await cache.ahset("auser:100", "email", "alice@example.com")
        assert cache.hlen("auser:100") == 2

    @pytest.mark.asyncio
    async def test_ahget_retrieves_field_value(self, cache: RespCache):
        cache.hset("aproduct:42", "name", "Widget")
        cache.hset("aproduct:42", "metadata", {"color": "blue", "size": "medium"})

        assert await cache.ahget("aproduct:42", "name") == "Widget"
        assert await cache.ahget("aproduct:42", "metadata") == {"color": "blue", "size": "medium"}
        assert await cache.ahget("aproduct:42", "missing_field") is None
        assert await cache.ahget("amissing_hash", "any_field") is None

    @pytest.mark.asyncio
    async def test_ahgetall_returns_all_fields(self, cache: RespCache):
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
    async def test_ahmget_retrieves_multiple_fields(self, cache: RespCache):
        cache.hset("asession:abc", "user_id", 123)
        cache.hset("asession:abc", "role", "admin")
        cache.hset("asession:abc", "expires", 3600)

        result = await cache.ahmget("asession:abc", "user_id", "expires", "missing")
        assert result == [123, 3600, None]

    @pytest.mark.asyncio
    async def test_ahset_mapping_creates_multiple_fields(self, cache: RespCache):
        await cache.ahset("aorder:999", mapping={"customer": "Bob", "total": 59.99, "items": 3})

        assert cache.hget("aorder:999", "customer") == "Bob"
        assert cache.hget("aorder:999", "total") == 59.99
        assert cache.hget("aorder:999", "items") == 3
        assert cache.hlen("aorder:999") == 3


class TestAsyncHashDelete:
    """Tests for ahdel."""

    @pytest.mark.asyncio
    async def test_ahdel_removes_field(self, cache: RespCache):
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
    async def test_ahlen_counts_fields(self, cache: RespCache):
        assert await cache.ahlen("aempty_hash") == 0
        cache.hset("agrowing_hash", "f1", "v1")
        assert await cache.ahlen("agrowing_hash") == 1
        cache.hset("agrowing_hash", "f2", "v2")
        assert await cache.ahlen("agrowing_hash") == 2


class TestAsyncHashExists:
    """Tests for ahexists."""

    @pytest.mark.asyncio
    async def test_ahexists_checks_field_presence(self, cache: RespCache):
        cache.hset("asettings:user", "theme", "dark")
        assert await cache.ahexists("asettings:user", "theme") is True
        assert await cache.ahexists("asettings:user", "language") is False


class TestAsyncHashKeys:
    """Tests for ahkeys."""

    @pytest.mark.asyncio
    async def test_ahkeys_returns_all_field_names(self, cache: RespCache):
        cache.hset("ainventory:001", "apples", 50)
        cache.hset("ainventory:001", "oranges", 30)
        cache.hset("ainventory:001", "bananas", 25)

        keys = await cache.ahkeys("ainventory:001")
        assert len(keys) == 3
        assert set(keys) == {"apples", "oranges", "bananas"}


class TestAsyncHashValues:
    """Tests for ahvals."""

    @pytest.mark.asyncio
    async def test_ahvals_returns_all_values(self, cache: RespCache):
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
    async def test_ahsetnx_sets_only_if_not_exists(self, cache: RespCache):
        result = await cache.ahsetnx("aunique_hash", "key1", "first")
        assert result is True
        assert cache.hget("aunique_hash", "key1") == "first"

        result = await cache.ahsetnx("aunique_hash", "key1", "second")
        assert result is False
        assert cache.hget("aunique_hash", "key1") == "first"


class TestAsyncHashIncrementOperations:
    """Tests for ahincrby and ahincrbyfloat."""

    @pytest.mark.asyncio
    async def test_ahincrby_increments_integer(self, cache: RespCache):
        cache.hset("acounters", "views", 100)

        result = await cache.ahincrby("acounters", "views", 10)
        assert result == 110

        result = await cache.ahincrby("acounters", "views", -5)
        assert result == 105

    @pytest.mark.asyncio
    async def test_ahincrby_creates_field_if_missing(self, cache: RespCache):
        result = await cache.ahincrby("anew_counters", "clicks", 1)
        assert result == 1

    @pytest.mark.asyncio
    async def test_ahincrbyfloat_increments_float(self, cache: RespCache):
        await cache.ahincrbyfloat("ametrics", "score", 10.5)

        result = await cache.ahincrbyfloat("ametrics", "score", 0.25)
        assert abs(result - 10.75) < 0.001

        result = await cache.ahincrbyfloat("ametrics", "score", -2.0)
        assert abs(result - 8.75) < 0.001

    @pytest.mark.asyncio
    async def test_ahincrbyfloat_creates_field_if_missing(self, cache: RespCache):
        result = await cache.ahincrbyfloat("anew_metrics", "rate", 2.718)
        assert abs(result - 2.718) < 0.001


class TestAsyncHashVersionSupport:
    """Tests for version parameter on async hash operations."""

    @pytest.mark.asyncio
    async def test_adifferent_versions_are_independent(self, cache: RespCache):
        await cache.ahset("adata", "key", "value_v1", version=1)
        await cache.ahset("adata", "other", "other_v1", version=1)
        await cache.ahset("adata", "key", "value_v2", version=2)

        assert await cache.ahexists("adata", "key", version=1)
        assert await cache.ahexists("adata", "other", version=1)
        assert await cache.ahexists("adata", "key", version=2)
        assert not await cache.ahexists("adata", "other", version=2)

        assert await cache.ahlen("adata", version=1) == 2
        assert await cache.ahlen("adata", version=2) == 1

        keys_v1 = await cache.ahkeys("adata", version=1)
        assert set(keys_v1) == {"key", "other"}

        keys_v2 = await cache.ahkeys("adata", version=2)
        assert keys_v2 == ["key"]

        await cache.ahdel("adata", "key", version=1)
        assert not await cache.ahexists("adata", "key", version=1)
        assert await cache.ahexists("adata", "key", version=2)


class TestAsyncHashKeyPrefixing:
    """Tests verifying async hash keys use prefixes but fields do not."""

    @pytest.mark.asyncio
    async def test_akey_prefixed_but_fields_raw(self, cache: RespCache):
        await cache.ahset("aaccount:500", "balance", 1000.00, version=2)
        await cache.ahset("aaccount:500", "currency", "USD", version=2)

        prefixed_key = cache.make_key("aaccount:500", version=2)

        adapter = cache.adapter
        assert await adapter.ahas_key(prefixed_key)
        key_type = await adapter.atype(prefixed_key)
        if isinstance(key_type, bytes):
            key_type = key_type.decode()
        assert key_type == "hash"

        raw_fields = {f.decode() if isinstance(f, bytes) else f for f in await adapter.ahkeys(prefixed_key)}
        assert "balance" in raw_fields
        assert "currency" in raw_fields
