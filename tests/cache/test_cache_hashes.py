"""Tests for hash operations."""

from django_cachex.cache import KeyValueCache


class TestHashSetAndGet:
    """Tests for hset, hget, hmget, hgetall."""

    def test_hset_creates_hash_field(self, cache: KeyValueCache):
        cache.hset("user:100", "username", "alice")
        cache.hset("user:100", "email", "alice@example.com")
        assert cache.hlen("user:100") == 2
        assert cache.hexists("user:100", "username")
        assert cache.hexists("user:100", "email")

    def test_hget_retrieves_field_value(self, cache: KeyValueCache):
        cache.hset("product:42", "name", "Widget")
        cache.hset("product:42", "metadata", {"color": "blue", "size": "medium"})

        assert cache.hget("product:42", "name") == "Widget"
        assert cache.hget("product:42", "metadata") == {"color": "blue", "size": "medium"}
        assert cache.hget("product:42", "missing_field") is None
        assert cache.hget("missing_hash", "any_field") is None

    def test_hgetall_returns_all_fields(self, cache: KeyValueCache):
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

    def test_hmget_retrieves_multiple_fields(self, cache: KeyValueCache):
        cache.hset("session:abc", "user_id", 123)
        cache.hset("session:abc", "role", "admin")
        cache.hset("session:abc", "expires", 3600)

        result = cache.hmget("session:abc", "user_id", "expires", "missing")
        assert result == [123, 3600, None]

    def test_hset_mapping_creates_multiple_fields(self, cache: KeyValueCache):
        cache.hset("order:999", mapping={"customer": "Bob", "total": 59.99, "items": 3})

        assert cache.hget("order:999", "customer") == "Bob"
        assert cache.hget("order:999", "total") == 59.99
        assert cache.hget("order:999", "items") == 3
        assert cache.hlen("order:999") == 3


class TestHashDelete:
    """Tests for hdel."""

    def test_hdel_removes_field(self, cache: KeyValueCache):
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

    def test_hlen_counts_fields(self, cache: KeyValueCache):
        assert cache.hlen("empty_hash") == 0
        cache.hset("growing_hash", "f1", "v1")
        assert cache.hlen("growing_hash") == 1
        cache.hset("growing_hash", "f2", "v2")
        assert cache.hlen("growing_hash") == 2


class TestHashKeys:
    """Tests for hkeys."""

    def test_hkeys_returns_all_field_names(self, cache: KeyValueCache):
        cache.hset("inventory:001", "apples", 50)
        cache.hset("inventory:001", "oranges", 30)
        cache.hset("inventory:001", "bananas", 25)

        keys = cache.hkeys("inventory:001")
        assert len(keys) == 3
        assert set(keys) == {"apples", "oranges", "bananas"}


class TestHashExists:
    """Tests for hexists."""

    def test_hexists_checks_field_presence(self, cache: KeyValueCache):
        cache.hset("settings:user", "theme", "dark")
        assert cache.hexists("settings:user", "theme") is True
        assert cache.hexists("settings:user", "language") is False


class TestHashVersionSupport:
    """Tests for version parameter on hash operations."""

    def test_different_versions_are_independent(self, cache: KeyValueCache):
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

    def test_key_prefixed_but_fields_raw(self, cache: KeyValueCache):
        client = cache.get_client(write=False)

        cache.hset("account:500", "balance", 1000.00, version=2)
        cache.hset("account:500", "currency", "USD", version=2)

        prefixed_key = cache.make_key("account:500", version=2)

        assert client.exists(prefixed_key)
        assert client.type(prefixed_key) == b"hash"

        raw_fields = client.hkeys(prefixed_key)
        assert b"balance" in raw_fields
        assert b"currency" in raw_fields


class TestHashIncrementOperations:
    """Tests for hincrby and hincrbyfloat."""

    def test_hincrby_increments_integer(self, cache: KeyValueCache):
        cache.hset("counters", "views", 100)

        result = cache.hincrby("counters", "views", 10)
        assert result == 110

        result = cache.hincrby("counters", "views", -5)
        assert result == 105

    def test_hincrby_creates_field_if_missing(self, cache: KeyValueCache):
        result = cache.hincrby("new_counters", "clicks", 1)
        assert result == 1

    def test_hincrbyfloat_increments_float(self, cache: KeyValueCache):
        cache.hincrbyfloat("metrics", "score", 10.5)

        result = cache.hincrbyfloat("metrics", "score", 0.25)
        assert abs(result - 10.75) < 0.001

        result = cache.hincrbyfloat("metrics", "score", -2.0)
        assert abs(result - 8.75) < 0.001

    def test_hincrbyfloat_creates_field_if_missing(self, cache: KeyValueCache):
        result = cache.hincrbyfloat("new_metrics", "rate", 2.718)
        assert abs(result - 2.718) < 0.001


class TestHashSetNX:
    """Tests for hsetnx."""

    def test_hsetnx_sets_only_if_not_exists(self, cache: KeyValueCache):
        result = cache.hsetnx("unique_hash", "key1", "first")
        assert result is True
        assert cache.hget("unique_hash", "key1") == "first"

        result = cache.hsetnx("unique_hash", "key1", "second")
        assert result is False
        assert cache.hget("unique_hash", "key1") == "first"


class TestHashValues:
    """Tests for hvals."""

    def test_hvals_returns_all_values(self, cache: KeyValueCache):
        cache.hset("numbers", "one", 1)
        cache.hset("numbers", "two", 2)
        cache.hset("numbers", "three", 3)

        values = cache.hvals("numbers")
        assert len(values) == 3
        assert set(values) == {1, 2, 3}

        assert cache.hvals("empty_hash") == []
