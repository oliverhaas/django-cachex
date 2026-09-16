"""Tests for Lua script operations."""

from typing import TYPE_CHECKING, Any

import pytest

from django_cachex.script import (
    Encoded,
    ScriptHelpers,
    decode_list_post,
    decode_single_post,
    encoded_pre,
    full_encode_pre,
    keys_only_pre,
)

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


class TestEvalScript:
    def test_eval_script_simple(self, cache: RespCache):
        result = cache.eval_script("return 42")
        assert result == 42

    def test_eval_script_with_keys_and_args(self, cache: RespCache):
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

    def test_eval_script_with_pre_hook(self, cache: RespCache):
        """Test script with pre_hook for key prefixing."""
        script = """
        redis.call('SET', KEYS[1], ARGV[1])
        return redis.call('GET', KEYS[1])
        """

        result = cache.eval_script(script, keys=["mykey"], args=["myvalue"], pre_hook=keys_only_pre)
        assert result == b"myvalue"

    def test_eval_script_with_encoding(self, cache: RespCache):
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

    def test_eval_script_with_version(self, cache: RespCache):
        script = """
        redis.call('SET', KEYS[1], ARGV[1])
        return redis.call('GET', KEYS[1])
        """

        result1 = cache.eval_script(
            script,
            keys=["vkey"],
            args=["v1"],
            pre_hook=full_encode_pre,
            post_hook=decode_single_post,
            version=1,
        )
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

        v1_val = cache.get("vkey", version=1)
        v2_val = cache.get("vkey", version=2)

        assert v1_val == "v1"
        assert v2_val == "v2"

    def test_eval_script_string_return(self, cache: RespCache):
        result = cache.eval_script("return 'hello'")
        assert result == b"hello"

    def test_eval_script_no_keys(self, cache: RespCache):
        result = cache.eval_script("return 1 + 2")
        assert result == 3


class TestScriptHelpers:
    def test_script_helpers_make_keys(self, cache: RespCache):
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache.encode,
            decode=cache.decode,
            version=1,
        )

        keys = helpers.make_keys(["key1", "key2"])
        assert len(keys) == 2
        assert keys[0] != "key1"
        assert keys[1] != "key2"

    def test_script_helpers_encode_decode(self, cache: RespCache):
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache.encode,
            decode=cache.decode,
            version=1,
        )

        original = {"key": "value", "number": 42}
        encoded = helpers.encode_values([original])
        decoded = helpers.decode_values(encoded)

        assert decoded[0] == original


class TestPreBuiltHooks:
    """Test pre-built pre_hook and post_hook implementations."""

    def test_keys_only_pre(self, cache: RespCache):
        """Test keys_only_pre helper."""
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache.encode,
            decode=cache.decode,
            version=1,
        )

        keys = ["k1", "k2"]
        args = [1, 2, "three"]

        proc_keys, proc_args = keys_only_pre(helpers, keys, args)

        assert proc_keys[0] != "k1"
        assert proc_args == [1, 2, "three"]

    def test_full_encode_pre(self, cache: RespCache):
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache.encode,
            decode=cache.decode,
            version=1,
        )

        keys = ["k1"]
        args = [{"obj": "value"}]

        proc_keys, proc_args = full_encode_pre(helpers, keys, args)

        assert proc_keys[0] != "k1"
        assert proc_args[0] != args[0]
        assert isinstance(proc_args[0], bytes)

    def test_decode_single_post(self, cache: RespCache):
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache.encode,
            decode=cache.decode,
            version=1,
        )

        original = {"test": "value"}
        encoded = helpers.encode(original)

        decoded = decode_single_post(helpers, encoded)
        assert decoded == original

        assert decode_single_post(helpers, None) is None

    def test_decode_list_post(self, cache: RespCache):
        helpers = ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache.encode,
            decode=cache.decode,
            version=1,
        )

        originals = [{"a": 1}, {"b": 2}]
        encoded = [helpers.encode(o) for o in originals]

        decoded = decode_list_post(helpers, encoded)
        assert decoded == originals

        assert decode_list_post(helpers, None) == []


class TestPipelineScripts:
    def test_pipeline_eval_script(self, cache: RespCache):
        script = "return redis.call('INCR', KEYS[1])"

        pipe = cache.pipeline()
        pipe.eval_script(script, keys=["pipe_counter"], pre_hook=keys_only_pre)
        pipe.eval_script(script, keys=["pipe_counter"], pre_hook=keys_only_pre)
        pipe.eval_script(script, keys=["pipe_counter"], pre_hook=keys_only_pre)
        results = pipe.execute()

        assert results == [1, 2, 3]

    def test_pipeline_eval_script_mixed(self, cache: RespCache):
        script = "redis.call('SET', KEYS[1], ARGV[1]); return 'ok'"

        pipe = cache.pipeline()
        pipe.set("regular_key", "regular_value")
        pipe.eval_script(script, keys=["script_key"], args=["script_value"], pre_hook=keys_only_pre)
        pipe.get("regular_key")
        results = pipe.execute()

        assert results[0] is True  # set
        assert results[1] == b"ok"  # script
        assert results[2] == "regular_value"  # get

    def test_pipeline_eval_script_with_post_hook(self, cache: RespCache):
        script = """
        redis.call('SET', KEYS[1], ARGV[1])
        return redis.call('GET', KEYS[1])
        """

        test_obj = {"data": [1, 2, 3]}

        pipe = cache.pipeline()
        pipe.eval_script(
            script,
            keys=["objkey"],
            args=[test_obj],
            pre_hook=full_encode_pre,
            post_hook=decode_single_post,
        )
        results = pipe.execute()

        assert results[0] == test_obj

    def test_pipeline_eval_script_chaining(self, cache: RespCache):
        pipe = cache.pipeline()
        result = pipe.eval_script("return 1").eval_script("return 2")
        assert result is pipe


@pytest.fixture
def mk(cache: RespCache):
    """Create a prefixed key for direct client async testing."""
    return lambda key, version=None: cache.make_and_validate_key(key, version=version)


class TestAsyncEval:
    """Tests for aeval() method."""

    @pytest.mark.asyncio
    async def test_aeval_simple_return(self, cache: RespCache):
        result = await cache.adapter.aeval("return 42", 0)
        assert result == 42

    @pytest.mark.asyncio
    async def test_aeval_with_keys_and_args(self, cache: RespCache, mk):
        key = mk("aeval_key")
        result = await cache.adapter.aeval(
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
            1,
            key,
            "hello",
        )
        assert result == b"hello"

    @pytest.mark.asyncio
    async def test_aeval_string_return(self, cache: RespCache):
        result = await cache.adapter.aeval("return 'async_result'", 0)
        assert result == b"async_result"


@pytest.mark.asyncio
class TestAsyncEvalScript:
    """Tests for the high-level aeval_script() method."""

    async def test_aeval_script_simple(self, cache: RespCache):
        result = await cache.aeval_script("return 'async'")
        assert result == b"async"

    async def test_aeval_script_with_encoding(self, cache: RespCache):
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


SET_AND_GET = """
redis.call('SET', KEYS[1], ARGV[1])
return redis.call('GET', KEYS[1])
"""

SETEXPIRE = """
local value = ARGV[1]
local ex    = tonumber(ARGV[2])
local nx    = ARGV[3] == "1"
if nx and redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
redis.call('SET', KEYS[1], value, 'EX', ex)
return 1
"""


class TestEncodedPre:
    """``encoded_pre`` encodes only the ARGV entries wrapped in ``Encoded``."""

    def _helpers(self, cache: RespCache) -> ScriptHelpers:
        return ScriptHelpers(
            make_key=cache.make_and_validate_key,
            encode=cache.encode,
            decode=cache.decode,
            version=1,
        )

    def test_fixed_position(self, cache: RespCache):
        keys, args = encoded_pre(self._helpers(cache), ["k"], [Encoded("hello"), 300, "1", "0"])

        assert keys == [cache.make_and_validate_key("k", version=1)]
        assert args[0] == cache.encode("hello")
        assert args[1:] == [300, "1", "0"]

    def test_variadic_tail(self, cache: RespCache):
        members = ["a", "b", "c"]
        _, args = encoded_pre(self._helpers(cache), [], ["1.5", "0", *map(Encoded, members)])

        assert args[:2] == ["1.5", "0"]
        assert args[2:] == [cache.encode(m) for m in members]

    def test_alternating_pairs(self, cache: RespCache):
        mapping = {"f1": {"x": 1}, "f2": [1, 2]}
        raw: list[Any] = []
        for field, value in mapping.items():
            raw += [field, Encoded(value)]
        _, args = encoded_pre(self._helpers(cache), [], raw)

        assert args == ["f1", cache.encode({"x": 1}), "f2", cache.encode([1, 2])]

    def test_scattered(self, cache: RespCache):
        raw = [Encoded("payload"), 60, "2.5", 10, 60, Encoded("task"), Encoded("m1"), Encoded("m2")]
        _, args = encoded_pre(self._helpers(cache), [], raw)

        assert args == [
            cache.encode("payload"),
            60,
            "2.5",
            10,
            60,
            cache.encode("task"),
            cache.encode("m1"),
            cache.encode("m2"),
        ]

    def test_nothing_wrapped_matches_keys_only_pre(self, cache: RespCache):
        helpers = self._helpers(cache)
        keys, args = ["k1", "k2"], ["v", 1, b"raw"]

        assert encoded_pre(helpers, keys, args) == keys_only_pre(helpers, keys, args)

    def test_everything_wrapped_matches_full_encode_pre(self, cache: RespCache):
        helpers = self._helpers(cache)
        keys, args = ["k1", "k2"], ["v", 1, {"a": 1}]

        assert encoded_pre(helpers, keys, list(map(Encoded, args))) == full_encode_pre(helpers, keys, args)

    def test_round_trip_through_eval_script(self, cache: RespCache):
        result = cache.eval_script(
            SET_AND_GET,
            keys=["enc"],
            args=[Encoded({"n": 1})],
            pre_hook=encoded_pre,
            post_hook=decode_single_post,
        )

        assert result == {"n": 1}
        assert cache.get("enc") == {"n": 1}

    def test_wrapped_value_reads_back_and_bare_scalar_parses_in_lua(self, cache: RespCache, compressors):
        """The 1000-byte value is above every compressor's ``min_length``, so it is genuinely compressed."""
        value = "x" * 1000

        assert cache.eval_script(SETEXPIRE, keys=["se"], args=[Encoded(value), 300, "0"], pre_hook=encoded_pre) == 1
        assert cache.get("se") == value
        assert 0 < cache.ttl("se") <= 300
        assert cache.eval_script(SETEXPIRE, keys=["se"], args=[Encoded("other"), 300, "1"], pre_hook=encoded_pre) == 0

    @pytest.mark.asyncio
    async def test_aeval_script(self, cache: RespCache):
        result = await cache.aeval_script(
            SET_AND_GET,
            keys=["aenc"],
            args=[Encoded(["x"])],
            pre_hook=encoded_pre,
            post_hook=decode_single_post,
        )

        assert result == ["x"]

    def test_pipeline_eval_script(self, cache: RespCache):
        pipe = cache.pipeline()
        pipe.eval_script(
            SET_AND_GET,
            keys=["penc"],
            args=[Encoded("p")],
            pre_hook=encoded_pre,
            post_hook=decode_single_post,
        )
        pipe.get("penc")

        assert pipe.execute() == ["p", "p"]


class TestEncodedGuards:
    def test_nested_encoded_raises(self):
        with pytest.raises(TypeError, match="nested"):
            Encoded(Encoded("x"))

    def test_stray_encoded_without_hook_raises(self, cache: RespCache):
        with pytest.raises(TypeError, match="encoded_pre"):
            cache.eval_script(SET_AND_GET, keys=["k"], args=[Encoded("x")])

    def test_stray_encoded_with_keys_only_pre_raises(self, cache: RespCache):
        with pytest.raises(TypeError, match="encoded_pre"):
            cache.eval_script(SET_AND_GET, keys=["k"], args=[Encoded("x")], pre_hook=keys_only_pre)

    def test_encoded_in_keys_raises(self, cache: RespCache):
        with pytest.raises(TypeError, match="keys"):
            cache.eval_script(SET_AND_GET, keys=[Encoded("k")], args=["x"], pre_hook=encoded_pre)

    @pytest.mark.asyncio
    async def test_aeval_script_stray_encoded_raises(self, cache: RespCache):
        with pytest.raises(TypeError, match="encoded_pre"):
            await cache.aeval_script(SET_AND_GET, keys=["k"], args=[Encoded("x")])

    def test_pipeline_stray_encoded_raises(self, cache: RespCache):
        with pytest.raises(TypeError, match="encoded_pre"):
            cache.pipeline().eval_script(SET_AND_GET, keys=["k"], args=[Encoded("x")])
