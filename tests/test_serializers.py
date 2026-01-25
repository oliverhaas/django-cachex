import pickle

import pytest
from django.core.exceptions import ImproperlyConfigured

from django_cachex.serializers.json import JSONSerializer
from django_cachex.serializers.pickle import PickleSerializer


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
