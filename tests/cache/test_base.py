"""Tests for the BaseCachex unsupported-op contract.

Per-backend tests live next to the backends they cover:

* LocMemCache: ``tests/cache/test_locmem.py``
* DatabaseCache: ``tests/cache/test_database.py``
* RESP backends (redis-py / valkey-py / valkey-glide): the parametrized
  ``cache`` fixture in ``tests/cache/``.
"""

import pytest

from django_cachex.cache.base import BaseCachex, _scan_hash
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
    ("clear_all_versions", ()),
    ("flush_db", ()),
    ("memory_usage", ("key",)),
    ("largest_keys", ()),
    ("info", ()),
    ("slowlog_get", ()),
    ("slowlog_len", ()),
]


@pytest.fixture
def bare_cache() -> MockExtendedCache:
    return MockExtendedCache()


@pytest.mark.parametrize(
    ("operation", "args"),
    UNSUPPORTED_OPERATIONS,
    ids=[op for op, _ in UNSUPPORTED_OPERATIONS],
)
def test_unsupported_operation_raises(bare_cache: MockExtendedCache, operation, args):
    method = getattr(bare_cache, operation)
    with pytest.raises(NotSupportedError):
        method(*args)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "args"),
    [("aclear_all_versions", ()), ("aflush_db", ()), ("amemory_usage", ("key",)), ("alargest_keys", ())],
    ids=["aclear_all_versions", "aflush_db", "amemory_usage", "alargest_keys"],
)
async def test_unsupported_async_operation_raises(bare_cache: MockExtendedCache, operation, args):
    with pytest.raises(NotSupportedError, match=operation):
        await getattr(bare_cache, operation)(*args)


# Without flags, set() delegates to Django's BaseCache.set; only the flag path is the cachex default.
@pytest.mark.parametrize("flag", ["nx", "xx", "get"])
def test_set_with_flag_raises(bare_cache: MockExtendedCache, flag: str):
    with pytest.raises(NotSupportedError):
        bare_cache.set("k", "v", **{flag: True})


@pytest.mark.asyncio
@pytest.mark.parametrize("flag", ["nx", "xx", "get"])
async def test_aset_with_flag_raises(bare_cache: MockExtendedCache, flag: str):
    with pytest.raises(NotSupportedError):
        await bare_cache.aset("k", "v", **{flag: True})


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


@pytest.fixture
def present_key_cache() -> KeysOnlyCache:
    return KeysOnlyCache({"present": "v"})


def test_type_present_key_reports_string(present_key_cache: KeysOnlyCache):
    assert present_key_cache.type("present") == KeyType.STRING


def test_type_missing_key_reports_none(present_key_cache: KeysOnlyCache):
    assert present_key_cache.type("absent") is None


@pytest.mark.asyncio
async def test_atype_matches_type(present_key_cache: KeysOnlyCache):
    assert await present_key_cache.atype("present") == KeyType.STRING
    assert await present_key_cache.atype("absent") is None


@pytest.fixture
def five_key_cache() -> KeysOnlyCache:
    return KeysOnlyCache({f"k{i}": i for i in range(5)})


def test_scan_explicit_count_zero_is_honored(five_key_cache: KeysOnlyCache):
    next_cursor, keys = five_key_cache.scan(count=0)
    assert keys == []
    assert next_cursor == 0


def test_scan_default_count_paginates(five_key_cache: KeysOnlyCache):
    next_cursor, keys = five_key_cache.scan()
    assert keys == ["k0", "k1", "k2", "k3", "k4"]
    assert next_cursor == 0


def test_scan_cursor_advances(five_key_cache: KeysOnlyCache):
    next_cursor, keys = five_key_cache.scan(count=2)
    assert len(keys) == 2
    assert next_cursor != 0
    _, rest = five_key_cache.scan(next_cursor, count=10)
    assert sorted(keys + rest) == ["k0", "k1", "k2", "k3", "k4"]


def test_scan_key_type_filter_is_applied(five_key_cache: KeysOnlyCache):
    assert five_key_cache.scan(key_type="string")[1] == ["k0", "k1", "k2", "k3", "k4"]
    assert five_key_cache.scan(key_type="hash")[1] == []


def _scan_pages(cache: BaseCachex, cursor: int = 0, count: int = 3) -> list[list[str]]:
    pages = []
    while True:
        cursor, keys = cache.scan(cursor, count=count)
        pages.append(keys)
        if cursor == 0:
            return pages


def test_scan_pages_return_every_key_once():
    cache = KeysOnlyCache({f"k{i}": i for i in range(10)})
    pages = _scan_pages(cache)
    assert all(len(page) <= 3 for page in pages)
    assert sorted(key for page in pages for key in page) == sorted(cache._data)


def test_scan_hash_is_a_fixed_function_of_the_key():
    # Pinned: the admin hands the cursor to whichever process serves the next page.
    assert _scan_hash("k0") == 5966777531559889355
    assert _scan_hash("") == 8238016292129134938


def test_scan_keeps_keys_sharing_a_position_on_one_page(mocker):
    positions = {"a": 1, "b": 5, "c": 5, "d": 9, "e": 9}
    mocker.patch("django_cachex.cache.base._scan_hash", positions.__getitem__)
    cache = KeysOnlyCache(dict.fromkeys(positions, 0))
    assert cache.scan(count=2) == (6, ["a", "b", "c"])
    assert cache.scan(6, count=2) == (0, ["d", "e"])


def test_scan_returns_remaining_keys_after_earlier_pages_are_deleted():
    cache = KeysOnlyCache({f"k{i}": i for i in range(10)})
    cursor, first = cache.scan(count=3)
    for key in first:
        del cache._data[key]
    rest = _scan_pages(cache, cursor)
    assert sorted(key for page in rest for key in page) == sorted(cache._data)
