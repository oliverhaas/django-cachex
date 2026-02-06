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


class TestAsyncEvalSHA:
    """Tests for aevalsha() method."""

    @pytest.mark.asyncio
    async def test_aevalsha_cached_script(self, cache: KeyValueCache, mk):
        # Load script first
        sha = cache._cache.script_load("return 99")
        result = await cache._cache.aevalsha(sha, 0)
        assert result == 99

    @pytest.mark.asyncio
    async def test_aevalsha_with_keys(self, cache: KeyValueCache, mk):
        script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
        sha = cache._cache.script_load(script)
        key = mk("aevalsha_key")
        result = await cache._cache.aevalsha(sha, 1, key, "test_value")
        assert result == b"test_value"


class TestAsyncScriptLoad:
    """Tests for ascript_load() method."""

    @pytest.mark.asyncio
    async def test_ascript_load(self, cache: KeyValueCache, mk):
        sha = await cache._cache.ascript_load("return 'loaded'")
        assert isinstance(sha, str)
        assert len(sha) == 40  # SHA1 hex digest

        # Verify the script is usable
        result = cache._cache.evalsha(sha, 0)
        assert result == b"loaded"


class TestAsyncScriptExists:
    """Tests for ascript_exists() method."""

    @pytest.mark.asyncio
    async def test_ascript_exists_loaded(self, cache: KeyValueCache, mk):
        sha = cache._cache.script_load("return 'exists_test'")
        result = await cache._cache.ascript_exists(sha)
        assert result == [True]

    @pytest.mark.asyncio
    async def test_ascript_exists_not_loaded(self, cache: KeyValueCache, mk):
        result = await cache._cache.ascript_exists("0000000000000000000000000000000000000000")
        assert result == [False]

    @pytest.mark.asyncio
    async def test_ascript_exists_multiple(self, cache: KeyValueCache, mk):
        sha1 = cache._cache.script_load("return 1")
        sha2 = "0000000000000000000000000000000000000000"
        result = await cache._cache.ascript_exists(sha1, sha2)
        assert result == [True, False]


class TestAsyncScriptFlush:
    """Tests for ascript_flush() method."""

    @pytest.mark.asyncio
    async def test_ascript_flush(self, cache: KeyValueCache, mk):
        sha = cache._cache.script_load("return 'will_flush'")
        # Verify script exists
        assert cache._cache.script_exists(sha) == [True]

        result = await cache._cache.ascript_flush()
        assert result is True

        # Verify script was flushed
        assert cache._cache.script_exists(sha) == [False]
