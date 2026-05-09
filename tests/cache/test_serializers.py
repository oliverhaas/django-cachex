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


def _reverse_key(key: str) -> str:
    """Reverse a made key back to original (strip version:prefix:)."""
    parts = key.split(":", 2)
    if len(parts) == 3:
        return parts[2]
    return key


class TestDefaultReverseKey:
    def test_basic_key_reversal(self):
        # Format: version:key_prefix:key
        assert _reverse_key("1:myprefix:mykey") == "mykey"

    def test_key_with_colons(self):
        # Key itself can contain colons
        assert _reverse_key("1:prefix:key:with:colons") == "key:with:colons"

    def test_empty_prefix(self):
        assert _reverse_key("1::mykey") == "mykey"
