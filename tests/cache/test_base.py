"""Tests for the BaseCachex unsupported-op contract.

Per-backend tests live next to the backends they cover:

* LocMemCache → ``tests/cache/test_locmem.py``
* DatabaseCache → ``tests/cache/test_database.py``
* RESP backends (redis-py / valkey-py / valkey-glide) →
  the parametrized ``cache`` fixture in ``tests/cache/``.
"""

import pytest

from django_cachex.cache.base import BaseCachex
from django_cachex.exceptions import NotSupportedError
from django_cachex.types import KeyType


class MockExtendedCache(BaseCachex):
    """A cachex backend that overrides nothing, so every extension hits the default."""

    def __init__(self):
        super().__init__(params={})


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


class TestBaseCachexSetFlags:
    """``set``/``aset`` default to ``NotSupportedError`` when any flag is set.

    Without flags, the call delegates to ``super().set`` (Django's
    ``BaseCache``), which raises its own ``NotImplementedError``; only the
    flag path is the cachex-contract default and is what we cover here.
    """

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.cache = MockExtendedCache()

    @pytest.mark.parametrize("flag", ["nx", "xx", "get"])
    def test_set_with_flag_raises(self, flag: str):
        with pytest.raises(NotSupportedError):
            self.cache.set("k", "v", **{flag: True})

    @pytest.mark.asyncio
    @pytest.mark.parametrize("flag", ["nx", "xx", "get"])
    async def test_aset_with_flag_raises(self, flag: str):
        with pytest.raises(NotSupportedError):
            await self.cache.aset("k", "v", **{flag: True})


class KeysOnlyCache(BaseCachex):
    """Minimal ``"limited"`` backend: a dict plus ``keys()``.

    Exercises the ``BaseCachex`` defaults that build on ``keys()``/``type()``
    without pulling in a real backend.
    """

    def __init__(self, data: dict[str, object]):
        super().__init__(params={})
        self._data = data

    def get(self, key, default=None, version=None):
        return self._data.get(key, default)

    def keys(self, pattern="*", version=None):
        return list(self._data)


class TestBaseCachexType:
    """``type()``/``atype()`` agree and honor the None-for-missing contract."""

    @pytest.fixture
    def cache(self) -> KeysOnlyCache:
        return KeysOnlyCache({"present": "v"})

    def test_present_key_reports_string(self, cache: KeysOnlyCache):
        assert cache.type("present") == KeyType.STRING

    def test_missing_key_reports_none(self, cache: KeysOnlyCache):
        assert cache.type("absent") is None

    @pytest.mark.asyncio
    async def test_atype_matches_type(self, cache: KeysOnlyCache):
        assert await cache.atype("present") == KeyType.STRING
        assert await cache.atype("absent") is None


class TestBaseCachexScan:
    """``scan()`` paginates ``keys()`` and applies ``key_type`` client-side."""

    @pytest.fixture
    def cache(self) -> KeysOnlyCache:
        return KeysOnlyCache({f"k{i}": i for i in range(5)})

    def test_explicit_count_zero_is_honored(self, cache: KeysOnlyCache):
        next_cursor, keys = cache.scan(count=0)
        assert keys == []
        assert next_cursor == 0

    def test_default_count_paginates(self, cache: KeysOnlyCache):
        next_cursor, keys = cache.scan()
        assert keys == ["k0", "k1", "k2", "k3", "k4"]
        assert next_cursor == 0

    def test_cursor_advances(self, cache: KeysOnlyCache):
        next_cursor, keys = cache.scan(count=2)
        assert keys == ["k0", "k1"]
        assert next_cursor == 2

    def test_key_type_filter_is_applied(self, cache: KeysOnlyCache):
        assert cache.scan(key_type="string")[1] == ["k0", "k1", "k2", "k3", "k4"]
        assert cache.scan(key_type="hash")[1] == []
