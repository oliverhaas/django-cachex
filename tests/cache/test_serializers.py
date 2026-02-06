import pickle

import pytest
from django.core.exceptions import ImproperlyConfigured

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.json import JSONSerializer
from django_cachex.serializers.msgpack import MessagePackSerializer
from django_cachex.serializers.pickle import PickleSerializer
from django_cachex.util import default_reverse_key


class TestJSONSerializer:
    def test_basic_roundtrip(self):
        serializer = JSONSerializer()
        data = {"key": "value", "number": 42, "nested": {"list": [1, 2, 3]}}
        encoded = serializer.dumps(data)
        decoded = serializer.loads(encoded)
        assert decoded == data

    def test_regular_string_not_modified(self):
        serializer = JSONSerializer()
        data = {"message": "Hello world", "code": "ABC-123"}
        encoded = serializer.dumps(data)
        decoded = serializer.loads(encoded)
        assert decoded == data
        assert isinstance(decoded["message"], str)


class TestPickleSerializer:
    def test_protocol_not_explicitly_specified(self):
        serializer = PickleSerializer()
        assert serializer.protocol == pickle.DEFAULT_PROTOCOL

    def test_protocol_too_high(self):
        with pytest.raises(
            ImproperlyConfigured,
            match=f"protocol can't be higher than pickle.HIGHEST_PROTOCOL: {pickle.HIGHEST_PROTOCOL}",
        ):
            PickleSerializer(protocol=pickle.HIGHEST_PROTOCOL + 1)

    def test_protocol_explicit(self):
        serializer = PickleSerializer(protocol=4)
        assert serializer.protocol == 4


class TestMessagePackSerializer:
    def test_basic_roundtrip(self):
        serializer = MessagePackSerializer()
        data = {"key": "value", "number": 42, "nested": {"list": [1, 2, 3]}}
        encoded = serializer.dumps(data)
        assert isinstance(encoded, bytes)
        decoded = serializer.loads(encoded)
        assert decoded == data

    def test_loads_int_passthrough(self):
        """Int values are passed through unchanged (for Redis INCR results)."""
        serializer = MessagePackSerializer()
        assert serializer.loads(42) == 42

    def test_loads_invalid_data_raises_serializer_error(self):
        serializer = MessagePackSerializer()
        with pytest.raises(SerializerError):
            serializer.loads(b"\xff\xfe\xfd")  # Invalid msgpack data

    def test_bytes_roundtrip(self):
        serializer = MessagePackSerializer()
        data = b"binary data"
        encoded = serializer.dumps(data)
        decoded = serializer.loads(encoded)
        assert decoded == data

    def test_none_roundtrip(self):
        serializer = MessagePackSerializer()
        encoded = serializer.dumps(None)
        decoded = serializer.loads(encoded)
        assert decoded is None


class TestDefaultReverseKey:
    def test_basic_key_reversal(self):
        # Format: version:key_prefix:key
        assert default_reverse_key("1:myprefix:mykey") == "mykey"

    def test_key_with_colons(self):
        # Key itself can contain colons
        assert default_reverse_key("1:prefix:key:with:colons") == "key:with:colons"

    def test_empty_prefix(self):
        assert default_reverse_key("1::mykey") == "mykey"
