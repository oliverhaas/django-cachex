"""Tests for hash operations."""

from django_cachex.cache import RedisCache


class TestHashOperations:
    def test_hset(self, cache: RedisCache):
        cache.hset("foo_hash1", "foo1", "bar1")
        cache.hset("foo_hash1", "foo2", "bar2")
        assert cache.hlen("foo_hash1") == 2
        assert cache.hexists("foo_hash1", "foo1")
        assert cache.hexists("foo_hash1", "foo2")

    def test_hdel(self, cache: RedisCache):
        cache.hset("foo_hash2", "foo1", "bar1")
        cache.hset("foo_hash2", "foo2", "bar2")
        assert cache.hlen("foo_hash2") == 2
        deleted_count = cache.hdel("foo_hash2", "foo1")
        assert deleted_count == 1
        assert cache.hlen("foo_hash2") == 1
        assert not cache.hexists("foo_hash2", "foo1")
        assert cache.hexists("foo_hash2", "foo2")

    def test_hlen(self, cache: RedisCache):
        assert cache.hlen("foo_hash3") == 0
        cache.hset("foo_hash3", "foo1", "bar1")
        assert cache.hlen("foo_hash3") == 1
        cache.hset("foo_hash3", "foo2", "bar2")
        assert cache.hlen("foo_hash3") == 2

    def test_hkeys(self, cache: RedisCache):
        cache.hset("foo_hash4", "foo1", "bar1")
        cache.hset("foo_hash4", "foo2", "bar2")
        cache.hset("foo_hash4", "foo3", "bar3")
        keys = cache.hkeys("foo_hash4")
        assert len(keys) == 3
        assert set(keys) == {"foo1", "foo2", "foo3"}

    def test_hexists(self, cache: RedisCache):
        cache.hset("foo_hash5", "foo1", "bar1")
        assert cache.hexists("foo_hash5", "foo1")
        assert not cache.hexists("foo_hash5", "foo")

    def test_hash_version_support(self, cache: RedisCache):
        """Test that version parameter works correctly for hash methods."""
        # Set values with different versions
        cache.hset("my_hash", "field1", "value1", version=1)
        cache.hset("my_hash", "field2", "value2", version=1)
        cache.hset("my_hash", "field1", "different_value", version=2)

        # Verify both versions exist independently
        assert cache.hexists("my_hash", "field1", version=1)
        assert cache.hexists("my_hash", "field2", version=1)
        assert cache.hexists("my_hash", "field1", version=2)
        assert not cache.hexists("my_hash", "field2", version=2)

        # Verify hlen works with versions
        assert cache.hlen("my_hash", version=1) == 2
        assert cache.hlen("my_hash", version=2) == 1

        # Verify hkeys works with versions
        keys_v1 = cache.hkeys("my_hash", version=1)
        assert len(keys_v1) == 2
        assert set(keys_v1) == {"field1", "field2"}

        keys_v2 = cache.hkeys("my_hash", version=2)
        assert len(keys_v2) == 1
        assert "field1" in keys_v2

        # Verify hdel works with versions
        cache.hdel("my_hash", "field1", version=1)
        assert not cache.hexists("my_hash", "field1", version=1)
        assert cache.hexists("my_hash", "field1", version=2)  # v2 should still exist

    def test_hash_key_is_prefixed_but_fields_are_not(self, cache: RedisCache):
        """Test that hash keys are prefixed but fields are not."""
        # Get raw Redis client
        client = cache.get_client(write=False)

        # Set some hash data
        cache.hset("user:1000", "email", "alice@example.com", version=2)
        cache.hset("user:1000", "name", "Alice", version=2)

        # Get the actual Redis key that was created
        expected_key = cache.make_key("user:1000", version=2)

        # Verify the hash exists in Redis with the prefixed key
        assert client.exists(expected_key)
        assert client.type(expected_key) == b"hash"

        # Verify fields are stored WITHOUT prefix
        actual_fields = client.hkeys(expected_key)
        # Fields should be plain "email" and "name", not prefixed
        assert b"email" in actual_fields
        assert b"name" in actual_fields

    def test_hget(self, cache: RedisCache):
        """Test hget retrieves stored value."""
        cache.hset("test_hget", "field1", "value1")
        cache.hset("test_hget", "field2", {"nested": "data"})

        assert cache.hget("test_hget", "field1") == "value1"
        assert cache.hget("test_hget", "field2") == {"nested": "data"}
        assert cache.hget("test_hget", "nonexistent") is None
        assert cache.hget("nonexistent_hash", "field") is None

    def test_hgetall(self, cache: RedisCache):
        """Test hgetall retrieves all fields and values."""
        cache.hset("test_hgetall", "name", "Alice")
        cache.hset("test_hgetall", "age", 30)
        cache.hset("test_hgetall", "data", [1, 2, 3])

        result = cache.hgetall("test_hgetall")
        assert result == {"name": "Alice", "age": 30, "data": [1, 2, 3]}

        # Empty hash returns empty dict
        assert cache.hgetall("nonexistent_hash") == {}

    def test_hmget(self, cache: RedisCache):
        """Test hmget retrieves multiple fields."""
        cache.hset("test_hmget", "f1", "v1")
        cache.hset("test_hmget", "f2", "v2")
        cache.hset("test_hmget", "f3", "v3")

        result = cache.hmget("test_hmget", "f1", "f3", "nonexistent")
        assert result == ["v1", "v3", None]

    def test_hmset(self, cache: RedisCache):
        """Test hmset sets multiple fields at once."""
        cache.hmset("test_hmset", {"name": "Bob", "city": "NYC", "count": 42})

        assert cache.hget("test_hmset", "name") == "Bob"
        assert cache.hget("test_hmset", "city") == "NYC"
        assert cache.hget("test_hmset", "count") == 42
        assert cache.hlen("test_hmset") == 3

    def test_hincrby(self, cache: RedisCache):
        """Test hincrby increments integer values."""
        cache.hset("test_hincrby", "counter", 10)

        result = cache.hincrby("test_hincrby", "counter", 5)
        assert result == 15

        result = cache.hincrby("test_hincrby", "counter", -3)
        assert result == 12

        # Create new field if doesn't exist
        result = cache.hincrby("test_hincrby", "new_counter", 1)
        assert result == 1

    def test_hincrbyfloat(self, cache: RedisCache):
        """Test hincrbyfloat increments float values."""
        # Initialize with hincrbyfloat since hset would serialize the float
        cache.hincrbyfloat("test_hincrbyfloat", "value", 10.5)

        result = cache.hincrbyfloat("test_hincrbyfloat", "value", 0.1)
        assert abs(result - 10.6) < 0.001

        result = cache.hincrbyfloat("test_hincrbyfloat", "value", -2.5)
        assert abs(result - 8.1) < 0.001

        # Create new field if doesn't exist
        result = cache.hincrbyfloat("test_hincrbyfloat", "new_field", 3.14)
        assert abs(result - 3.14) < 0.001

    def test_hsetnx(self, cache: RedisCache):
        """Test hsetnx sets field only if not exists."""
        # Should set when field doesn't exist
        result = cache.hsetnx("test_hsetnx", "field1", "value1")
        assert result is True
        assert cache.hget("test_hsetnx", "field1") == "value1"

        # Should not overwrite existing field
        result = cache.hsetnx("test_hsetnx", "field1", "new_value")
        assert result is False
        assert cache.hget("test_hsetnx", "field1") == "value1"

    def test_hvals(self, cache: RedisCache):
        """Test hvals returns all values."""
        cache.hset("test_hvals", "a", 1)
        cache.hset("test_hvals", "b", 2)
        cache.hset("test_hvals", "c", 3)

        values = cache.hvals("test_hvals")
        assert len(values) == 3
        assert set(values) == {1, 2, 3}

        # Empty hash returns empty list
        assert cache.hvals("nonexistent_hash") == []
