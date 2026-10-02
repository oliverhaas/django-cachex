import pickle
from enum import IntEnum

import pytest
from django.core.exceptions import ImproperlyConfigured
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
    OrjsonSerializer = None


def test_json_basic_roundtrip():
    serializer = JsonSerializer()
    data = {"key": "value", "number": 42, "nested": {"list": [1, 2, 3]}}
    encoded = serializer.dumps(data)
    decoded = serializer.loads(encoded)
    assert decoded == data


def test_json_regular_string_not_modified():
    serializer = JsonSerializer()
    data = {"message": "Hello world", "code": "ABC-123"}
    encoded = serializer.dumps(data)
    decoded = serializer.loads(encoded)
    assert decoded == data
    assert isinstance(decoded["message"], str)


def test_pickle_protocol_not_explicitly_specified():
    serializer = PickleSerializer()
    assert serializer.protocol == pickle.DEFAULT_PROTOCOL


def test_pickle_protocol_explicit():
    serializer = PickleSerializer(protocol=4)
    assert serializer.protocol == 4


def test_pickle_protocol_too_high_is_rejected_when_the_serializer_is_built():
    with pytest.raises(ImproperlyConfigured, match="PickleSerializer"):
        PickleSerializer(protocol=pickle.HIGHEST_PROTOCOL + 1)


def test_msgpack_basic_roundtrip():
    serializer = MsgpackSerializer()
    data = {"key": "value", "number": 42, "nested": {"list": [1, 2, 3]}}
    encoded = serializer.dumps(data)
    assert isinstance(encoded, bytes)
    decoded = serializer.loads(encoded)
    assert decoded == data


def test_msgpack_loads_int_passthrough():
    """Int values are passed through unchanged (for Redis INCR results)."""
    serializer = MsgpackSerializer()
    assert serializer.loads(42) == 42


def test_msgpack_loads_invalid_data_raises_serializer_error():
    serializer = MsgpackSerializer()
    with pytest.raises(SerializerError):
        serializer.loads(b"\xff\xfe\xfd")  # Invalid msgpack data


def test_msgpack_bytes_roundtrip():
    serializer = MsgpackSerializer()
    data = b"binary data"
    encoded = serializer.dumps(data)
    decoded = serializer.loads(encoded)
    assert decoded == data


def test_msgpack_none_roundtrip():
    serializer = MsgpackSerializer()
    encoded = serializer.dumps(None)
    decoded = serializer.loads(encoded)
    assert decoded is None


def test_msgpack_non_string_key_dict_roundtrip():
    """Dicts with non-string keys (e.g. int) must roundtrip correctly."""
    serializer = MsgpackSerializer()
    data = {1: "a", 2: "b"}
    encoded = serializer.dumps(data)
    decoded = serializer.loads(encoded)
    assert decoded == data


def test_ormsgpack_basic_roundtrip():
    serializer = OrmsgpackSerializer()
    data = {"key": "value", "number": 42, "nested": {"list": [1, 2, 3]}}
    encoded = serializer.dumps(data)
    assert isinstance(encoded, bytes)
    decoded = serializer.loads(encoded)
    assert decoded == data


def test_ormsgpack_loads_int_passthrough():
    serializer = OrmsgpackSerializer()
    assert serializer.loads(42) == 42


def test_ormsgpack_loads_invalid_data_raises_serializer_error():
    serializer = OrmsgpackSerializer()
    with pytest.raises(SerializerError):
        serializer.loads(b"\xc1")  # reserved byte in msgpack spec


def test_ormsgpack_none_roundtrip():
    serializer = OrmsgpackSerializer()
    encoded = serializer.dumps(None)
    decoded = serializer.loads(encoded)
    assert decoded is None


def test_ormsgpack_non_str_dict_keys_roundtrip_like_msgpack():
    data = {1: "a", 2: "b", "mixed": 3}
    assert OrmsgpackSerializer().loads(OrmsgpackSerializer().dumps(data)) == data
    assert MsgpackSerializer().loads(MsgpackSerializer().dumps(data)) == data


@pytest.mark.skipif(OrjsonSerializer is None, reason="orjson not installed")
def test_orjson_basic_roundtrip():
    serializer = OrjsonSerializer()
    data = {"key": "value", "number": 42, "nested": {"list": [1, 2, 3]}}
    encoded = serializer.dumps(data)
    assert isinstance(encoded, bytes)
    decoded = serializer.loads(encoded)
    assert decoded == data


@pytest.mark.skipif(OrjsonSerializer is None, reason="orjson not installed")
def test_orjson_loads_int_passthrough():
    serializer = OrjsonSerializer()
    assert serializer.loads(42) == 42


@pytest.mark.skipif(OrjsonSerializer is None, reason="orjson not installed")
def test_orjson_loads_invalid_data_raises_serializer_error():
    serializer = OrjsonSerializer()
    with pytest.raises(SerializerError):
        serializer.loads(b"\xff\xfe not json")


@pytest.mark.skipif(OrjsonSerializer is None, reason="orjson not installed")
def test_orjson_dumps_unsupported_type_raises_serializer_error():
    serializer = OrjsonSerializer()
    with pytest.raises(SerializerError):
        serializer.dumps({"x": object()})


@pytest.mark.parametrize("serializer_class", [PickleSerializer, JsonSerializer, MsgpackSerializer, OrmsgpackSerializer])
def test_loads_non_bytes_raises_serializer_error(serializer_class):
    with pytest.raises(SerializerError, match="could not deserialize NoneType"):
        serializer_class().loads(None)


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


# Regression: msgpack packs 48..57 as one byte, ASCII b"0"..b"9", and decode()'s int fast path read it as 0..9.
@pytest.mark.parametrize(
    "serializer",
    [
        "django_cachex.serializers.msgpack.MsgpackSerializer",
        "django_cachex.serializers.ormsgpack.OrmsgpackSerializer",
    ],
    ids=["msgpack", "ormsgpack"],
)
@pytest.mark.parametrize("member", INT_SUBCLASS_MEMBERS, ids=INT_SUBCLASS_IDS)
def test_int_subclass_value_roundtrips(serializer: str, member: int):
    cache = make_cache(serializer=serializer)
    assert cache.decode(cache.encode(member)) == member.value


@pytest.mark.parametrize("member", INT_SUBCLASS_MEMBERS, ids=INT_SUBCLASS_IDS)
def test_pickle_keeps_the_enum_type(member: int):
    cache = make_cache(serializer="django_cachex.serializers.pickle.PickleSerializer")
    assert cache.decode(cache.encode(member)) is member
