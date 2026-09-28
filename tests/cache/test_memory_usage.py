"""Tests for MEMORY USAGE: ``memory_usage()``, its pipeline form and ``largest_keys()``."""

from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.test import override_settings

from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


def test_existing_key_reports_its_size_in_bytes(cache: RespCache):
    cache.set("mu", "x" * 4096)
    size = cache.memory_usage("mu")
    assert isinstance(size, int)
    assert size > 4096


def test_memory_usage_missing_key_is_none(cache: RespCache):
    assert cache.memory_usage("mu_missing") is None


def test_memory_usage_samples_is_forwarded(cache: RespCache):
    cache.hset("mu_hash", mapping={f"f{i}": "v" * 100 for i in range(50)})
    exact = cache.memory_usage("mu_hash", samples=0)
    sampled = cache.memory_usage("mu_hash", samples=1)
    assert isinstance(exact, int) and exact > 0
    assert isinstance(sampled, int) and sampled > 0


def test_memory_usage_version_selects_the_key(cache: RespCache):
    cache.set("mu_v", "x" * 4096, version=2)
    assert cache.memory_usage("mu_v") is None
    assert cache.memory_usage("mu_v", version=2) is not None


def test_memory_usage_pipeline(cache: RespCache):
    cache.set("mu_p1", "x" * 4096)
    pipe = cache.pipeline()
    pipe.memory_usage("mu_p1").memory_usage("mu_p_missing").memory_usage("mu_p1", samples=0)
    big, missing, exact = pipe.execute()
    assert isinstance(big, int) and big > 4096
    assert missing is None
    assert isinstance(exact, int) and exact > 4096


@pytest.mark.asyncio
async def test_amemory_usage(cache: RespCache):
    await cache.aset("mu_async", "x" * 4096)
    size = await cache.amemory_usage("mu_async")
    assert isinstance(size, int)
    assert size > 4096
    assert await cache.amemory_usage("mu_async_missing") is None


@pytest.fixture
def sized_keys(cache: RespCache) -> dict[str, int]:
    sizes = {"lk:small": 10, "lk:mid": 2_000, "lk:big": 40_000, "lk:huge": 200_000}
    for key, n in sizes.items():
        cache.set(key, "x" * n)
    cache.set("other:big", "x" * 100_000)
    return sizes


def test_largest_keys_returns_the_top_n_largest_first(cache: RespCache, sized_keys: dict[str, int]):
    result = cache.largest_keys("lk:*", count=2)
    assert [key for key, _ in result] == ["lk:huge", "lk:big"]
    assert all(isinstance(size, int) for _, size in result)
    assert result[0][1] > result[1][1] > 40_000


def test_largest_keys_count_above_matches_returns_them_all(cache: RespCache, sized_keys: dict[str, int]):
    result = cache.largest_keys("lk:*", count=50)
    assert [key for key, _ in result] == ["lk:huge", "lk:big", "lk:mid", "lk:small"]


def test_largest_keys_pattern_scopes_the_scan(cache: RespCache, sized_keys: dict[str, int]):
    keys = [key for key, _ in cache.largest_keys("other:*", count=10)]
    assert keys == ["other:big"]


def test_largest_keys_no_match_is_empty(cache: RespCache):
    assert cache.largest_keys("nothing:*") == []


def test_largest_keys_count_zero_is_empty(cache: RespCache, sized_keys: dict[str, int]):
    assert cache.largest_keys("lk:*", count=0) == []


def test_largest_keys_negative_count_is_rejected(cache: RespCache, sized_keys: dict[str, int]):
    with pytest.raises(ValueError, match="count must not be negative"):
        cache.largest_keys("lk:*", count=-1)


def test_largest_keys_version_is_honoured(cache: RespCache):
    cache.set("lk:v", "x" * 5000, version=3)
    assert cache.largest_keys("lk:*") == []
    assert [key for key, _ in cache.largest_keys("lk:*", version=3)] == ["lk:v"]


def test_largest_keys_batches_across_many_keys(cache: RespCache):
    # MEMORY USAGE reports allocator size classes, so sizes must be far apart.
    big = {"lk:many:7": 1_000, "lk:many:130": 10_000, "lk:many:249": 100_000}
    for i in range(250):
        cache.set(f"lk:many:{i}", "x" * big.get(f"lk:many:{i}", 1))
    result = cache.largest_keys("lk:many:*", count=3)
    assert [key for key, _ in result] == ["lk:many:249", "lk:many:130", "lk:many:7"]


@pytest.mark.asyncio
async def test_alargest_keys(cache: RespCache, sized_keys: dict[str, int]):
    result = await cache.alargest_keys("lk:*", count=2)
    assert [key for key, _ in result] == ["lk:huge", "lk:big"]


@pytest.mark.asyncio
async def test_alargest_keys_count_zero_and_negative(cache: RespCache, sized_keys: dict[str, int]):
    assert await cache.alargest_keys("lk:*", count=0) == []
    with pytest.raises(ValueError, match="count must not be negative"):
        await cache.alargest_keys("lk:*", count=-1)


def test_locmem_raises_not_supported():
    with override_settings(CACHES={"locmem": {"BACKEND": "django_cachex.cache.LocMemCache"}}):
        with pytest.raises(NotSupportedError, match="memory_usage"):
            caches["locmem"].memory_usage("k")
        with pytest.raises(NotSupportedError, match="largest_keys"):
            caches["locmem"].largest_keys()
