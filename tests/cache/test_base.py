"""Tests for the BaseCachex unsupported-op contract.

Per-backend tests live next to the backends they cover:

* LocMemCache → ``tests/cache/test_locmem.py``
* DatabaseCache → ``tests/cache/test_database.py`` (TBD)
* RESP backends (redis-py / valkey-py / valkey-glide / redis-rs) →
  the parametrized ``cache`` fixture in ``tests/cache/``.
"""

import pytest

from django_cachex.cache.base import BaseCachex
from django_cachex.exceptions import NotSupportedError

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
