"""Tests for serializer fallback functionality."""

import json
import pickle

import pytest
from django.core.cache import cache
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from django_cachex.exceptions import SerializerError
from django_cachex.serializers.json import JsonSerializer
from django_cachex.serializers.pickle import PickleSerializer
from tests.cache.support import make_cache

JSON_THEN_PICKLE = [
    "django_cachex.serializers.json.JsonSerializer",
    "django_cachex.serializers.pickle.PickleSerializer",
]


class TestSerializerConfig:
    def test_single_string_config_backwards_compatible(self):
        cache_obj = make_cache(serializer="django_cachex.serializers.pickle.PickleSerializer")

        assert len(cache_obj._serializers) == 1
        assert cache_obj._serializers[0].__class__.__name__ == "PickleSerializer"

    def test_list_config_with_fallback(self):
        cache_obj = make_cache(serializer=JSON_THEN_PICKLE)

        assert len(cache_obj._serializers) == 2
        assert cache_obj._serializers[0].__class__.__name__ == "JsonSerializer"
        assert cache_obj._serializers[1].__class__.__name__ == "PickleSerializer"

    def test_empty_serializer_list_rejected_at_init(self):
        """An empty serializer list fails at construction, not at first use."""
        with pytest.raises(ImproperlyConfigured):
            make_cache(serializer=[])

    def test_migration_scenario(self, redis_container):
        """A pickle-only cache is readable after switching to JSON with a pickle fallback."""
        host, port = redis_container.host, redis_container.port

        caches_pickle = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}?db=10",
                "OPTIONS": {
                    "serializer": "django_cachex.serializers.pickle.PickleSerializer",
                },
            },
        }

        with override_settings(CACHES=caches_pickle):
            cache.set("old_key", {"data": "from_pickle"})

        caches_migration = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}?db=10",
                "OPTIONS": {"serializer": JSON_THEN_PICKLE},
            },
        }

        with override_settings(CACHES=caches_migration):
            assert cache.get("old_key") == {"data": "from_pickle"}

            cache.set("new_key", {"data": "from_json"})
            assert cache.get("new_key") == {"data": "from_json"}

            cache.delete("old_key")
            cache.delete("new_key")


class TestDeserializeFallback:
    """Tests for the _deserialize fallback logic on the cache layer."""

    def test_deserialize_json_with_multiple_serializers(self):
        cache_obj = make_cache(serializer=JSON_THEN_PICKLE)
        data = {"key": "value", "number": 42}
        assert cache_obj._deserialize(json.dumps(data).encode()) == data

    def test_deserialize_pickle_with_json_first(self):
        cache_obj = make_cache(serializer=JSON_THEN_PICKLE)
        data = {"key": "value", "number": 42}
        assert cache_obj._deserialize(pickle.dumps(data)) == data

    def test_deserialize_raises_when_all_fail(self):
        cache_obj = make_cache(serializer=["django_cachex.serializers.json.JsonSerializer"])
        invalid_data = b"\x80\x04\x95\x00\x00\x00\x00"  # Pickle header, not JSON
        with pytest.raises(SerializerError, match="JsonSerializer"):
            cache_obj._deserialize(invalid_data)


class TestSerializerError:
    def test_pickle_raises_serializer_error_on_invalid_data(self):
        serializer = PickleSerializer()
        with pytest.raises(SerializerError, match="PickleSerializer could not deserialize"):
            serializer.loads(b"not valid pickle data")

    def test_json_raises_serializer_error_on_invalid_data(self):
        serializer = JsonSerializer()
        with pytest.raises(SerializerError, match="JsonSerializer could not deserialize"):
            serializer.loads(b"not valid json data")

    def test_json_raises_serializer_error_on_invalid_utf8(self):
        serializer = JsonSerializer()
        with pytest.raises(SerializerError):
            serializer.loads(b"\xff\xfe")  # Invalid UTF-8

    def test_dumps_error_names_the_serializer_and_the_cause(self):
        serializer = JsonSerializer()
        with pytest.raises(SerializerError, match="JsonSerializer could not serialize object") as excinfo:
            serializer.dumps(object())
        assert isinstance(excinfo.value.__cause__, TypeError)
