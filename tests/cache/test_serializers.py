import pickle

import pytest

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.json import JsonSerializer
from django_cachex.serializers.msgpack import MsgpackSerializer
from django_cachex.serializers.ormsgpack import OrmsgpackSerializer
from django_cachex.serializers.pickle import PickleSerializer

try:
    from django_cachex.serializers.orjson import OrjsonSerializer
except ImportError:
    OrjsonSerializer = None  # type: ignore[assignment,misc]


class TestJsonSerializer:
    def test_basic_roundtrip(self):
        serializer = JsonSerializer()
        data = {"key": "value", "number": 42, "nested": {"list": [1, 2, 3]}}
        encoded = serializer.dumps(data)
        decoded = serializer.loads(encoded)
        assert decoded == data

    def test_regular_string_not_modified(self):
        serializer = JsonSerializer()
        data = {"message": "Hello world", "code": "ABC-123"}
        encoded = serializer.dumps(data)
        decoded = serializer.loads(encoded)
        assert decoded == data
        assert isinstance(decoded["message"], str)


class TestPickleSerializer:
    def test_protocol_not_explicitly_specified(self):
        serializer = PickleSerializer()
        assert serializer.protocol == pickle.DEFAULT_PROTOCOL

    def test_protocol_explicit(self):
        serializer = PickleSerializer(protocol=4)
        assert serializer.protocol == 4

    def test_protocol_too_high_raises_on_dumps(self):
        # We no longer pre-validate; pickle itself raises at dumps time.
        serializer = PickleSerializer(protocol=pickle.HIGHEST_PROTOCOL + 1)
        with pytest.raises(SerializerError):
            serializer.dumps({"x": 1})


class TestMsgpackSerializer:
    def test_basic_roundtrip(self):
        serializer = MsgpackSerializer()
        data = {"key": "value", "number": 42, "nested": {"list": [1, 2, 3]}}
        encoded = serializer.dumps(data)
        assert isinstance(encoded, bytes)
        decoded = serializer.loads(encoded)
        assert decoded == data

    def test_loads_int_passthrough(self):
        """Int values are passed through unchanged (for Redis INCR results)."""
        serializer = MsgpackSerializer()
        assert serializer.loads(42) == 42

    def test_loads_invalid_data_raises_serializer_error(self):
        serializer = MsgpackSerializer()
        with pytest.raises(SerializerError):
            serializer.loads(b"\xff\xfe\xfd")  # Invalid msgpack data

    def test_bytes_roundtrip(self):
        serializer = MsgpackSerializer()
        data = b"binary data"
        encoded = serializer.dumps(data)
        decoded = serializer.loads(encoded)
        assert decoded == data

    def test_none_roundtrip(self):
        serializer = MsgpackSerializer()
        encoded = serializer.dumps(None)
        decoded = serializer.loads(encoded)
        assert decoded is None

    def test_non_string_key_dict_roundtrip(self):
        """Dicts with non-string keys (e.g. int) must roundtrip correctly."""
        serializer = MsgpackSerializer()
        data = {1: "a", 2: "b"}
        encoded = serializer.dumps(data)
        decoded = serializer.loads(encoded)
        assert decoded == data


class TestOrmsgpackSerializer:
    def test_basic_roundtrip(self):
        serializer = OrmsgpackSerializer()
        data = {"key": "value", "number": 42, "nested": {"list": [1, 2, 3]}}
        encoded = serializer.dumps(data)
        assert isinstance(encoded, bytes)
        decoded = serializer.loads(encoded)
        assert decoded == data

    def test_loads_int_passthrough(self):
        serializer = OrmsgpackSerializer()
        assert serializer.loads(42) == 42

    def test_loads_invalid_data_raises_serializer_error(self):
        serializer = OrmsgpackSerializer()
        with pytest.raises(SerializerError):
            serializer.loads(b"\xc1")  # reserved byte in msgpack spec

    def test_none_roundtrip(self):
        serializer = OrmsgpackSerializer()
        encoded = serializer.dumps(None)
        decoded = serializer.loads(encoded)
        assert decoded is None

    def test_non_str_dict_keys_roundtrip_like_msgpack(self):
        # Regression: ormsgpack packed without OPT_NON_STR_KEYS, so int keys
        # raised where MsgpackSerializer round-tripped them.
        data = {1: "a", 2: "b", "mixed": 3}
        assert OrmsgpackSerializer().loads(OrmsgpackSerializer().dumps(data)) == data
        assert MsgpackSerializer().loads(MsgpackSerializer().dumps(data)) == data


@pytest.mark.skipif(OrjsonSerializer is None, reason="orjson not installed")
class TestOrjsonSerializer:
    def test_basic_roundtrip(self):
        serializer = OrjsonSerializer()
        data = {"key": "value", "number": 42, "nested": {"list": [1, 2, 3]}}
        encoded = serializer.dumps(data)
        assert isinstance(encoded, bytes)
        decoded = serializer.loads(encoded)
        assert decoded == data

    def test_loads_int_passthrough(self):
        serializer = OrjsonSerializer()
        assert serializer.loads(42) == 42

    def test_loads_invalid_data_raises_serializer_error(self):
        serializer = OrjsonSerializer()
        with pytest.raises(SerializerError):
            serializer.loads(b"\xff\xfe not json")

    def test_dumps_unsupported_type_raises_serializer_error(self):
        serializer = OrjsonSerializer()
        with pytest.raises(SerializerError):
            serializer.dumps({"x": object()})


def _make_cache(*, key_prefix: str = ""):
    """Construct a :class:`RedisCache` purely to exercise ``reverse_key``.

    The cache's ``adapter`` property is lazy, so no connection is opened.
    """
    from django_cachex.cache import RedisCache

    return RedisCache(server="redis://localhost:6379/0", params={"KEY_PREFIX": key_prefix})


class TestDefaultReverseKey:
    def test_basic_key_reversal(self):
        cache = _make_cache(key_prefix="myprefix")
        assert cache.reverse_key(cache.make_key("mykey")) == "mykey"

    def test_key_with_colons(self):
        # Key itself can contain colons
        cache = _make_cache(key_prefix="prefix")
        assert cache.reverse_key("prefix:1:key:with:colons") == "key:with:colons"

    def test_empty_prefix(self):
        cache = _make_cache()
        assert cache.reverse_key(":1:mykey") == "mykey"
        assert cache.reverse_key(cache.make_key("mykey")) == "mykey"

    def test_colon_in_key_prefix(self):
        # Regression: partitioning on the first colon never matched a
        # KEY_PREFIX that contained one, so keys came back unreversed.
        cache = _make_cache(key_prefix="app:v2")
        assert cache.make_key("foo") == "app:v2:1:foo"
        assert cache.reverse_key(cache.make_key("foo")) == "foo"
        assert cache.reverse_key(cache.make_key("key:with:colons")) == "key:with:colons"

    def test_unmatched_prefix_returned_unchanged(self):
        # A key made by some other cache must not lose its leading segments.
        cache = _make_cache(key_prefix="myprefix")
        assert cache.reverse_key("otherprefix:1:mykey") == "otherprefix:1:mykey"

    def test_key_without_layout_returned_unchanged(self):
        cache = _make_cache(key_prefix="myprefix")
        assert cache.reverse_key("plainkey") == "plainkey"
        # Prefix matches but there is no version:key remainder.
        assert cache.reverse_key("myprefix:1") == "myprefix:1"
