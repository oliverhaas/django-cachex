"""Tests for serializer fallback functionality."""

import json
import pickle

import pytest

from django_cachex.client import RedisCacheClient
from django_cachex.exceptions import SerializerError
from django_cachex.serializers.json import JSONSerializer
from django_cachex.serializers.pickle import PickleSerializer


class TestDefaultClientSerializerConfig:
    """Tests for RedisCacheClient serializer configuration handling."""

    def test_single_string_config_backwards_compatible(self, redis_container):
        """Test that a single string config still works (backwards compatibility)."""
        client = RedisCacheClient(
            servers=["redis://localhost:6379/0"],
            options={
                "serializer": "django_cachex.serializers.pickle.PickleSerializer",
            },
        )

        assert len(client._serializers) == 1
        assert client._serializers[0].__class__.__name__ == "PickleSerializer"

    def test_list_config_with_fallback(self, redis_container):
        """Test that a list config creates multiple serializers."""
        client = RedisCacheClient(
            servers=["redis://localhost:6379/0"],
            options={
                "serializer": [
                        "django_cachex.serializers.json.JSONSerializer",
                        "django_cachex.serializers.pickle.PickleSerializer",
                    ],
            },
        )

        assert len(client._serializers) == 2
        assert client._serializers[0].__class__.__name__ == "JSONSerializer"
        assert client._serializers[1].__class__.__name__ == "PickleSerializer"

    def test_migration_scenario(self, redis_container):
        """Test a realistic migration scenario from pickle to JSON."""
        from django.test import override_settings

        host, port = redis_container.host, redis_container.port

        # Step 1: Write with pickle (simulated old data)
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
            from django.core.cache import cache

            cache.set("old_key", {"data": "from_pickle"})

        # Step 2: Switch to JSON with pickle fallback
        caches_migration = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}?db=10",
                "OPTIONS": {
                    "serializer": [
                        "django_cachex.serializers.json.JSONSerializer",
                        "django_cachex.serializers.pickle.PickleSerializer",
                    ],
                },
            },
        }

        with override_settings(CACHES=caches_migration):
            from django.core.cache import cache

            # Should be able to read old pickle data
            assert cache.get("old_key") == {"data": "from_pickle"}

            # Write new data with JSON
            cache.set("new_key", {"data": "from_json"})
            assert cache.get("new_key") == {"data": "from_json"}

            cache.delete("old_key")
            cache.delete("new_key")


class TestDeserializeFallback:
    """Tests for the _deserialize fallback logic."""

    def test_deserialize_json_with_multiple_serializers(self, redis_container):
        """Test that _deserialize correctly deserializes JSON data."""
        client = RedisCacheClient(
            servers=["redis://localhost:6379/0"],
            options={
                "serializer": [
                        "django_cachex.serializers.json.JSONSerializer",
                        "django_cachex.serializers.pickle.PickleSerializer",
                    ],
            },
        )

        data = {"key": "value", "number": 42}
        json_data = json.dumps(data).encode()

        assert client._deserialize(json_data) == data

    def test_deserialize_pickle_with_json_first(self, redis_container):
        """Test that _deserialize falls back to pickle for pickle-serialized data."""
        client = RedisCacheClient(
            servers=["redis://localhost:6379/0"],
            options={
                "serializer": [
                        "django_cachex.serializers.json.JSONSerializer",
                        "django_cachex.serializers.pickle.PickleSerializer",
                    ],
            },
        )

        data = {"key": "value", "number": 42}
        pickle_data = pickle.dumps(data)

        # JSON will fail, pickle should succeed
        assert client._deserialize(pickle_data) == data

    def test_deserialize_raises_when_all_fail(self, redis_container):
        """Test that _deserialize raises SerializerError when all serializers fail."""
        client = RedisCacheClient(
            servers=["redis://localhost:6379/0"],
            options={
                "serializer": [
                        "django_cachex.serializers.json.JSONSerializer",
                    ],
            },
        )

        # Invalid data that can't be deserialized as JSON
        invalid_data = b"\x80\x04\x95\x00\x00\x00\x00"  # Pickle header, not JSON

        with pytest.raises(SerializerError):
            client._deserialize(invalid_data)

    def test_deserialize_continues_on_failure(self, redis_container):
        """Test that _deserialize continues to next serializer on failure."""
        client = RedisCacheClient(
            servers=["redis://localhost:6379/0"],
            options={
                "serializer": [
                        "django_cachex.serializers.json.JSONSerializer",
                        "django_cachex.serializers.pickle.PickleSerializer",
                    ],
            },
        )

        # Data that is valid pickle but not valid JSON
        data = {"key": "value"}
        pickle_data = pickle.dumps(data)

        # JSON fails, falls through to pickle
        assert client._deserialize(pickle_data) == data


class TestSerializerError:
    """Tests for SerializerError exception."""

    def test_pickle_raises_serializer_error_on_invalid_data(self):
        """Test that PickleSerializer raises SerializerError on invalid data."""
        serializer = PickleSerializer()
        with pytest.raises(SerializerError):
            serializer.loads(b"not valid pickle data")

    def test_json_raises_serializer_error_on_invalid_data(self):
        """Test that JSONSerializer raises SerializerError on invalid data."""
        serializer = JSONSerializer()
        with pytest.raises(SerializerError):
            serializer.loads(b"not valid json data")

    def test_json_raises_serializer_error_on_invalid_utf8(self):
        """Test that JSONSerializer raises SerializerError on invalid UTF-8."""
        serializer = JSONSerializer()
        with pytest.raises(SerializerError):
            serializer.loads(b"\xff\xfe")  # Invalid UTF-8
