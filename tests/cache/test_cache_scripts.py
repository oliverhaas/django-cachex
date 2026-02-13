"""Tests for Lua script operations."""

import pytest

from django_cachex.cache import KeyValueCache
from django_cachex.script import (
    ScriptHelpers,
    decode_list_post,
    decode_single_post,
    full_encode_pre,
    keys_only_pre,
)


class TestEvalScript:
    """Test eval_script execution."""

    def test_eval_script_simple(self, cache: KeyValueCache):
        """Test simple script execution."""
        result = cache.eval_script("return 42")
        assert result == 42

    def test_eval_script_with_keys_and_args(self, cache: KeyValueCache):
        """Test script execution with keys and args."""
        script = """
        local current = redis.call('GET', KEYS[1]) or 0
        local new = tonumber(current) + tonumber(ARGV[1])
        redis.call('SET', KEYS[1], new)
        return new
        """

        result = cache.eval_script(script, keys=["counter"], args=[10], pre_hook=keys_only_pre)
        assert result == 10

        result = cache.eval_script(script, keys=["counter"], args=[5], pre_hook=keys_only_pre)
        assert result == 15

    def test_eval_script_with_pre_hook(self, cache: KeyValueCache):
        """Test script with pre_hook for key prefixing."""
        script = """
        redis.call('SET', KEYS[1], ARGV[1])
        return redis.call('GET', KEYS[1])
        """

        result = cache.eval_script(script, keys=["mykey"], args=["myvalue"], pre_hook=keys_only_pre)
        assert result == b"myvalue"

    def test_eval_script_with_encoding(self, cache: KeyValueCache):
        """Test script with full encoding of values."""
        script = """
        redis.call('SET', KEYS[1], ARGV[1])
        return redis.call('GET', KEYS[1])
        """

        test_obj = {"name": "test", "value": 123}
        result = cache.eval_script(
            script,
            keys=["objkey"],
            args=[test_obj],
            pre_hook=full_encode_pre,
            post_hook=decode_single_post,
        )
        assert result == test_obj

    def test_eval_script_with_version(self, cache: KeyValueCache):
        """Test script execution with explicit version."""
        script = """
        redis.call('SET', KEYS[1], ARGV[1])
        return redis.call('GET', KEYS[1])
        """

        # Set with version 1
        result1 = cache.eval_script(
            script,
            keys=["vkey"],
            args=["v1"],
            pre_hook=full_encode_pre,
            post_hook=decode_single_post,
            version=1,
        )
        # Set with version 2
        result2 = cache.eval_script(
            script,
            keys=["vkey"],
            args=["v2"],
            pre_hook=full_encode_pre,
            post_hook=decode_single_post,
            version=2,
        )

        assert result1 == "v1"
        assert result2 == "v2"

        # Get should return different values for different versions
        v1_val = cache.get("vkey", version=1)
        v2_val = cache.get("vkey", version=2)

        assert v1_val == "v1"
        assert v2_val == "v2"

    def test_eval_script_string_return(self, cache: KeyValueCache):
        """Test script returning a string."""
        result = cache.eval_script("return 'hello'")
        assert result == b"hello"

    def test_eval_script_no_keys(self, cache: KeyValueCache):
        """Test script with no keys or args."""
        result = cache.eval_script("return 1 + 2")
        assert result == 3


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


class TestPreBuiltHooks:
    """Test pre-built pre_hook and post_hook implementations."""

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
        script = "return redis.call('INCR', KEYS[1])"

        with cache.pipeline() as pipe:
            pipe.eval_script(script, keys=["pipe_counter"], pre_hook=keys_only_pre)
            pipe.eval_script(script, keys=["pipe_counter"], pre_hook=keys_only_pre)
            pipe.eval_script(script, keys=["pipe_counter"], pre_hook=keys_only_pre)
            results = pipe.execute()

        assert results == [1, 2, 3]

    def test_pipeline_eval_script_mixed(self, cache: KeyValueCache):
        """Test mixing eval_script with other operations."""
        script = "redis.call('SET', KEYS[1], ARGV[1]); return 'ok'"

        with cache.pipeline() as pipe:
            pipe.set("regular_key", "regular_value")
            pipe.eval_script(script, keys=["script_key"], args=["script_value"], pre_hook=keys_only_pre)
            pipe.get("regular_key")
            results = pipe.execute()

        assert results[0] is True  # set
        assert results[1] == b"ok"  # script
        assert results[2] == "regular_value"  # get

    def test_pipeline_eval_script_with_post_hook(self, cache: KeyValueCache):
        """Test pipeline eval_script with post_hook decoder."""
        script = """
        redis.call('SET', KEYS[1], ARGV[1])
        return redis.call('GET', KEYS[1])
        """

        test_obj = {"data": [1, 2, 3]}

        with cache.pipeline() as pipe:
            pipe.eval_script(
                script,
                keys=["objkey"],
                args=[test_obj],
                pre_hook=full_encode_pre,
                post_hook=decode_single_post,
            )
            results = pipe.execute()

        assert results[0] == test_obj

    def test_pipeline_eval_script_chaining(self, cache: KeyValueCache):
        """Test that eval_script returns self for chaining."""
        pipe = cache.pipeline()
        result = pipe.eval_script("return 1").eval_script("return 2")
        assert result is pipe


@pytest.mark.asyncio
class TestAsyncScriptExecution:
    """Test async script execution functionality."""

    async def test_aeval_script_simple(self, cache: KeyValueCache):
        """Test simple async script execution."""
        result = await cache.aeval_script("return 'async'")
        assert result == b"async"

    async def test_aeval_script_with_encoding(self, cache: KeyValueCache):
        """Test async script with encoding."""
        script = """
        redis.call('SET', KEYS[1], ARGV[1])
        return redis.call('GET', KEYS[1])
        """

        test_obj = {"async": True, "value": 42}
        result = await cache.aeval_script(
            script,
            keys=["async_key"],
            args=[test_obj],
            pre_hook=full_encode_pre,
            post_hook=decode_single_post,
        )
        assert result == test_obj
