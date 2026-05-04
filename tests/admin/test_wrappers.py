"""Tests for cachex DatabaseCache and BaseCachex unsupported-op contract.

LocMemCache tests live in ``tests/cache/test_locmem.py`` (it's a
cachex-native backend, not a wrapper).
"""

import pytest
from django.core.cache import caches
from django.test import override_settings

from django_cachex.cache.base import BaseCachex
from django_cachex.exceptions import NotSupportedError

DATABASE_CACHES = {
    "dbcache": {
        "BACKEND": "django_cachex.cache.DatabaseCache",
        "LOCATION": "test_cache_table",
    },
}


# =============================================================================
# DatabaseCache extension tests
# =============================================================================


class TestDatabaseCacheExtensions:
    """Tests for DatabaseCache extensions."""

    @pytest.fixture(autouse=True)
    def _setup_cache(self, db):
        from django.core.management import call_command

        with override_settings(CACHES=DATABASE_CACHES):
            call_command("createcachetable", verbosity=0)
            cache = caches["dbcache"]
            cache.clear()
            self.cache = cache
            yield

    def test_set_and_get(self):
        self.cache.set("key1", "value1")
        assert self.cache.get("key1") == "value1"

    def test_delete(self):
        self.cache.set("key1", "value1")
        self.cache.delete("key1")
        assert self.cache.get("key1") is None

    def test_clear(self):
        self.cache.set("key1", "value1")
        self.cache.clear()
        assert self.cache.get("key1") is None

    def test_keys_returns_all(self):
        self.cache.set("alpha", 1)
        self.cache.set("beta", 2)
        keys = self.cache.keys()
        assert len(keys) >= 2

    def test_ttl_missing_key(self):
        assert self.cache.ttl("nonexistent") == -2

    def test_ttl_expiring_key(self):
        self.cache.set("temp", "value", timeout=3600)
        ttl = self.cache.ttl("temp")
        assert 3500 <= ttl <= 3600

    def test_info_returns_dict(self):
        self.cache.set("key1", "value1")
        info = self.cache.info()
        assert info["backend"] == "DatabaseCache"
        assert "keyspace" in info

    def test_expire(self):
        self.cache.set("key1", "value1", timeout=3600)
        result = self.cache.expire("key1", 100)
        assert result is True

    def test_persist(self):
        self.cache.set("key1", "value1", timeout=3600)
        result = self.cache.persist("key1")
        assert result is True

    def test_hset_then_hset_same_key(self):
        # Regression: second hset reads back the existing key, which calls ttl(),
        # which subtracts now from the stored expires datetime. SQLite returns a
        # naive datetime; with USE_TZ=True, `now` is tz-aware, so the subtraction
        # used to fail with "can't subtract offset-naive and offset-aware".
        self.cache.hset("h", "field1", "value1")
        self.cache.hset("h", "field2", "value2")
        assert self.cache.hgetall("h") == {"field1": "value1", "field2": "value2"}

    def test_hash_ops_preserve_ttl(self):
        self.cache.hset("h", "f1", "v1")
        self.cache.expire("h", 3600)
        self.cache.hset("h", "f2", "v2")
        ttl = self.cache.ttl("h")
        assert 3500 <= ttl <= 3600


# =============================================================================
# BaseCachex unsupported operations
# =============================================================================


UNSUPPORTED_OPERATIONS = [
    ("keys", ("*",)),
    ("ttl", ("key",)),
    ("expire", ("key", 100)),
    ("persist", ("key",)),
    ("lrange", ("key", 0, -1)),
    ("llen", ("key",)),
    ("lpush", ("key", "value")),
    ("rpush", ("key", "value")),
    ("lpop", ("key",)),
    ("rpop", ("key",)),
    ("lrem", ("key", 0, "value")),
    ("ltrim", ("key", 0, -1)),
    ("smembers", ("key",)),
    ("scard", ("key",)),
    ("sadd", ("key", "value")),
    ("srem", ("key", "value")),
    ("spop", ("key",)),
    ("hgetall", ("key",)),
    ("hlen", ("key",)),
    ("hset", ("key", "field", "value")),
    ("hdel", ("key", "field")),
    ("zrange", ("key", 0, -1)),
    ("zcard", ("key",)),
    ("zadd", ("key", {"member": 1.0})),
    ("zrem", ("key", "member")),
    ("zpopmin", ("key",)),
    ("zpopmax", ("key",)),
    ("xlen", ("key",)),
]


class TestBaseCachexUnsupported:
    """Test that BaseCachex raises NotSupportedError for extended operations."""

    @pytest.fixture(autouse=True)
    def _setup_extensions(self):
        # Create a mock class that combines BaseCache with BaseCachex
        # to test that base extensions raise NotSupportedError

        class MockExtendedCache(BaseCachex):
            def __init__(self):
                super().__init__(params={})

        self.cache = MockExtendedCache()

    @pytest.mark.parametrize(
        ("operation", "args"),
        UNSUPPORTED_OPERATIONS,
        ids=[op for op, _ in UNSUPPORTED_OPERATIONS],
    )
    def test_unsupported_operation_raises(self, operation, args):
        method = getattr(self.cache, operation)
        with pytest.raises(NotSupportedError):
            method(*args)
