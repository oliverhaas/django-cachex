"""Tests for async Lua scripting operations (raw client methods)."""

import pytest

from django_cachex.cache import KeyValueCache


@pytest.fixture
def mk(cache: KeyValueCache):
    """Create a prefixed key for direct client async testing."""
    return lambda key, version=None: cache.make_and_validate_key(key, version=version)


class TestAsyncEval:
    """Tests for aeval() method."""

    @pytest.mark.asyncio
    async def test_aeval_simple_return(self, cache: KeyValueCache, mk):
        result = await cache._cache.aeval("return 42", 0)
        assert result == 42

    @pytest.mark.asyncio
    async def test_aeval_with_keys_and_args(self, cache: KeyValueCache, mk):
        key = mk("aeval_key")
        result = await cache._cache.aeval(
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
            1,
            key,
            "hello",
        )
        assert result == b"hello"

    @pytest.mark.asyncio
    async def test_aeval_string_return(self, cache: KeyValueCache, mk):
        result = await cache._cache.aeval("return 'async_result'", 0)
        assert result == b"async_result"
