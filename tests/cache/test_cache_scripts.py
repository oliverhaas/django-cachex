"""Tests for Lua script operations."""

import pytest

from django_cachex.cache import KeyValueCache
from django_cachex.exceptions import ScriptNotRegisteredError
from django_cachex.script import (
    LuaScript,
    ScriptHelpers,
    decode_list_post,
    decode_single_post,
    full_encode_pre,
    keys_only_pre,
    noop_post,
)


class TestScriptRegistration:
    """Test script registration functionality."""

    def test_register_script_returns_lua_script(self, cache: KeyValueCache):
        """Test that register_script returns a LuaScript object."""
        script = cache.register_script(
            "test_script",
            "return 1",
        )
        assert isinstance(script, LuaScript)
        assert script.name == "test_script"
        assert script.script == "return 1"

    def test_register_script_with_all_options(self, cache: KeyValueCache):
        """Test register_script with all optional arguments."""
        script = cache.register_script(
            "full_script",
            "return KEYS[1]",
            num_keys=1,
            pre_func=keys_only_pre,
            post_func=noop_post,
        )
        assert script.num_keys == 1
        assert script.pre_func is keys_only_pre
        assert script.post_func is noop_post

    def test_register_script_overwrites_existing(self, cache: KeyValueCache):
        """Test that registering with same name overwrites."""
        cache.register_script("overwrite_test", "return 1")
        script = cache.register_script("overwrite_test", "return 2")
        assert script.script == "return 2"

    def test_eval_script_not_registered_raises(self, cache: KeyValueCache):
        """Test that eval_script raises for unregistered scripts."""
        with pytest.raises(ScriptNotRegisteredError) as exc_info:
            cache.eval_script("nonexistent_script")
        assert exc_info.value.name == "nonexistent_script"
        assert "nonexistent_script" in str(exc_info.value)


class TestScriptExecution:
    """Test script execution functionality."""

    def test_eval_script_simple(self, cache: KeyValueCache):
        """Test simple script execution."""
        cache.register_script("simple_return", "return 42")
        result = cache.eval_script("simple_return")
        assert result == 42

    def test_eval_script_with_keys_and_args(self, cache: KeyValueCache):
        """Test script execution with keys and args."""
        cache.register_script(
            "incr_by",
            """
            local current = redis.call('GET', KEYS[1]) or 0
            local new = tonumber(current) + tonumber(ARGV[1])
            redis.call('SET', KEYS[1], new)
            return new
            """,
            pre_func=keys_only_pre,
        )

        result = cache.eval_script("incr_by", keys=["counter"], args=[10])
        assert result == 10

        result = cache.eval_script("incr_by", keys=["counter"], args=[5])
        assert result == 15

    def test_eval_script_with_pre_func(self, cache: KeyValueCache):
        """Test script with pre_func for key prefixing."""
        cache.register_script(
            "set_and_get",
            """
            redis.call('SET', KEYS[1], ARGV[1])
            return redis.call('GET', KEYS[1])
            """,
            pre_func=keys_only_pre,
        )

        result = cache.eval_script("set_and_get", keys=["mykey"], args=["myvalue"])
        assert result == b"myvalue"

    def test_eval_script_with_encoding(self, cache: KeyValueCache):
        """Test script with full encoding of values."""
        cache.register_script(
            "store_object",
            """
            redis.call('SET', KEYS[1], ARGV[1])
            return redis.call('GET', KEYS[1])
            """,
            pre_func=full_encode_pre,
            post_func=decode_single_post,
        )

        test_obj = {"name": "test", "value": 123}
        result = cache.eval_script("store_object", keys=["objkey"], args=[test_obj])
        assert result == test_obj

    def test_eval_script_caches_sha(self, cache: KeyValueCache):
        """Test that script SHA is cached after first execution."""
        script = cache.register_script("cached_sha", "return 'cached'")
        assert script._sha is None

        cache.eval_script("cached_sha")
        assert script._sha is not None

        # Second execution should use cached SHA
        first_sha = script._sha
        cache.eval_script("cached_sha")
        assert script._sha == first_sha

    def test_eval_script_noscript_fallback(self, cache: KeyValueCache):
        """Test that NOSCRIPT error triggers reload."""
        script = cache.register_script("noscript_test", "return 'reloaded'")

        # Execute to cache SHA
        cache.eval_script("noscript_test")
        assert script._sha is not None

        # Flush scripts to simulate NOSCRIPT scenario
        cache._cache.script_flush()

        # Should reload and succeed
        result = cache.eval_script("noscript_test")
        assert result == b"reloaded"

    def test_eval_script_with_version(self, cache: KeyValueCache):
        """Test script execution with explicit version."""
        cache.register_script(
            "versioned_set",
            """
            redis.call('SET', KEYS[1], ARGV[1])
            return redis.call('GET', KEYS[1])
            """,
            pre_func=full_encode_pre,
            post_func=decode_single_post,
        )

        # Set with version 1
        result1 = cache.eval_script("versioned_set", keys=["vkey"], args=["v1"], version=1)
        # Set with version 2
        result2 = cache.eval_script("versioned_set", keys=["vkey"], args=["v2"], version=2)

        # Script results should be the values we stored
        assert result1 == "v1"
        assert result2 == "v2"

        # Get should return different values for different versions
        # (values are properly encoded, so cache.get() can decode them)
        v1_val = cache.get("vkey", version=1)
        v2_val = cache.get("vkey", version=2)

        # Values should be different (different prefixed keys)
        assert v1_val == "v1"
        assert v2_val == "v2"


class TestScriptHelpers:
    """Test ScriptHelpers functionality."""

    def test_script_helpers_make_keys(self, cache: KeyValueCache):
        """Test ScriptHelpers.make_keys method."""
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache._cache.encode,
            decode=cache._cache.decode,
            version=1,
        )

        keys = helpers.make_keys(["key1", "key2"])
        assert len(keys) == 2
        # Keys should be prefixed
        assert keys[0] != "key1"
        assert keys[1] != "key2"

    def test_script_helpers_encode_decode(self, cache: KeyValueCache):
        """Test ScriptHelpers encode/decode methods."""
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache._cache.encode,
            decode=cache._cache.decode,
            version=1,
        )

        original = {"key": "value", "number": 42}
        encoded = helpers.encode_values([original])
        decoded = helpers.decode_values(encoded)

        assert decoded[0] == original


class TestPreBuiltFunctions:
    """Test pre-built pre_func and post_func implementations."""

    def test_keys_only_pre(self, cache: KeyValueCache):
        """Test keys_only_pre helper."""
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache._cache.encode,
            decode=cache._cache.decode,
            version=1,
        )

        keys = ["k1", "k2"]
        args = [1, 2, "three"]

        proc_keys, proc_args = keys_only_pre(helpers, keys, args)

        # Keys should be prefixed
        assert proc_keys[0] != "k1"
        # Args should be unchanged
        assert proc_args == [1, 2, "three"]

    def test_full_encode_pre(self, cache: KeyValueCache):
        """Test full_encode_pre helper."""
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache._cache.encode,
            decode=cache._cache.decode,
            version=1,
        )

        keys = ["k1"]
        args = [{"obj": "value"}]

        proc_keys, proc_args = full_encode_pre(helpers, keys, args)

        # Keys should be prefixed
        assert proc_keys[0] != "k1"
        # Args should be encoded
        assert proc_args[0] != args[0]
        assert isinstance(proc_args[0], bytes)

    def test_decode_single_post(self, cache: KeyValueCache):
        """Test decode_single_post helper."""
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache._cache.encode,
            decode=cache._cache.decode,
            version=1,
        )

        original = {"test": "value"}
        encoded = helpers.encode(original)

        decoded = decode_single_post(helpers, encoded)
        assert decoded == original

        # None should return None
        assert decode_single_post(helpers, None) is None

    def test_decode_list_post(self, cache: KeyValueCache):
        """Test decode_list_post helper."""
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache._cache.encode,
            decode=cache._cache.decode,
            version=1,
        )

        originals = [{"a": 1}, {"b": 2}]
        encoded = [helpers.encode(o) for o in originals]

        decoded = decode_list_post(helpers, encoded)
        assert decoded == originals

        # None should return empty list
        assert decode_list_post(helpers, None) == []


class TestPipelineScripts:
    """Test script execution in pipelines."""

    def test_pipeline_eval_script(self, cache: KeyValueCache):
        """Test eval_script in pipeline."""
        cache.register_script("pipe_incr", "return redis.call('INCR', KEYS[1])", pre_func=keys_only_pre)

        with cache.pipeline() as pipe:
            pipe.eval_script("pipe_incr", keys=["pipe_counter"])
            pipe.eval_script("pipe_incr", keys=["pipe_counter"])
            pipe.eval_script("pipe_incr", keys=["pipe_counter"])
            results = pipe.execute()

        assert results == [1, 2, 3]

    def test_pipeline_eval_script_mixed(self, cache: KeyValueCache):
        """Test mixing eval_script with other operations."""
        cache.register_script(
            "pipe_set",
            "redis.call('SET', KEYS[1], ARGV[1]); return 'ok'",
            pre_func=keys_only_pre,
        )

        with cache.pipeline() as pipe:
            pipe.set("regular_key", "regular_value")
            pipe.eval_script("pipe_set", keys=["script_key"], args=["script_value"])
            pipe.get("regular_key")
            results = pipe.execute()

        assert results[0] is True  # set
        assert results[1] == b"ok"  # script
        assert results[2] == "regular_value"  # get

    def test_pipeline_eval_script_with_post_func(self, cache: KeyValueCache):
        """Test pipeline eval_script with post_func decoder."""
        cache.register_script(
            "pipe_get_obj",
            """
            redis.call('SET', KEYS[1], ARGV[1])
            return redis.call('GET', KEYS[1])
            """,
            pre_func=full_encode_pre,
            post_func=decode_single_post,
        )

        test_obj = {"data": [1, 2, 3]}

        with cache.pipeline() as pipe:
            pipe.eval_script("pipe_get_obj", keys=["objkey"], args=[test_obj])
            results = pipe.execute()

        assert results[0] == test_obj

    def test_pipeline_eval_script_not_registered(self, cache: KeyValueCache):
        """Test that unregistered script raises in pipeline."""
        with pytest.raises(ScriptNotRegisteredError), cache.pipeline() as pipe:
            pipe.eval_script("not_registered", keys=["key"])

    def test_pipeline_eval_script_chaining(self, cache: KeyValueCache):
        """Test that eval_script returns self for chaining."""
        cache.register_script("chain_test", "return 1")

        pipe = cache.pipeline()
        result = pipe.eval_script("chain_test").eval_script("chain_test")
        assert result is pipe


@pytest.mark.asyncio
class TestAsyncScriptExecution:
    """Test async script execution functionality."""

    async def test_aeval_script_simple(self, cache: KeyValueCache):
        """Test simple async script execution."""
        cache.register_script("async_return", "return 'async'")
        result = await cache.aeval_script("async_return")
        assert result == b"async"

    async def test_aeval_script_with_encoding(self, cache: KeyValueCache):
        """Test async script with encoding."""
        cache.register_script(
            "async_store",
            """
            redis.call('SET', KEYS[1], ARGV[1])
            return redis.call('GET', KEYS[1])
            """,
            pre_func=full_encode_pre,
            post_func=decode_single_post,
        )

        test_obj = {"async": True, "value": 42}
        result = await cache.aeval_script("async_store", keys=["async_key"], args=[test_obj])
        assert result == test_obj

    async def test_aeval_script_not_registered(self, cache: KeyValueCache):
        """Test that aeval_script raises for unregistered scripts."""
        with pytest.raises(ScriptNotRegisteredError):
            await cache.aeval_script("nonexistent_async")
