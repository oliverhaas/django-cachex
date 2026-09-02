import pickle
from enum import IntEnum

import pytest
from django.db import models

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.json import JsonSerializer
from django_cachex.serializers.msgpack import MsgpackSerializer
from django_cachex.serializers.ormsgpack import OrmsgpackSerializer
from django_cachex.serializers.pickle import PickleSerializer
from tests.cache.support import make_cache

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


class Digits(IntEnum):
    ASCII_ZERO = 48
    ASCII_NINE = 57
    ZERO = 0
    BIG = 200
    NEGATIVE = -5


class DigitChoices(models.IntegerChoices):
    ASCII_ZERO = 48, "ascii zero"
    ASCII_NINE = 57, "ascii nine"
    ZERO = 0, "zero"
    BIG = 200, "big"
    NEGATIVE = -5, "negative"


INT_SUBCLASS_MEMBERS = [*Digits, *DigitChoices]
INT_SUBCLASS_IDS = [f"{type(m).__name__}.{m.name}" for m in INT_SUBCLASS_MEMBERS]


class TestIntSubclassEncoding:
    """encode()/decode() must keep the numeric value of an int subclass.

    Regression: msgpack packs the values 48..57 as a single positive-fixint
    byte, which is the ASCII digit b"0".."9". decode()'s int fast path read
    that back as 0..9, so an IntEnum or IntegerChoices member silently
    changed value on the way out.
    """

    @pytest.mark.parametrize(
        "serializer",
        [
            "django_cachex.serializers.msgpack.MsgpackSerializer",
            "django_cachex.serializers.ormsgpack.OrmsgpackSerializer",
        ],
        ids=["msgpack", "ormsgpack"],
    )
    @pytest.mark.parametrize("member", INT_SUBCLASS_MEMBERS, ids=INT_SUBCLASS_IDS)
    def test_value_roundtrips(self, serializer: str, member: int):
        cache = make_cache(serializer=serializer)
        assert cache.decode(cache.encode(member)) == member.value

    @pytest.mark.parametrize("member", INT_SUBCLASS_MEMBERS, ids=INT_SUBCLASS_IDS)
    def test_pickle_keeps_the_enum_type(self, member: int):
        cache = make_cache(serializer="django_cachex.serializers.pickle.PickleSerializer")
        assert cache.decode(cache.encode(member)) is member
