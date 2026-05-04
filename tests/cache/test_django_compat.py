"""Tests for Django builtin Redis backend compatibility.

These tests verify that django-cachex can be used as a drop-in replacement
for Django's builtin Redis backend (django.core.cache.backends.redis.RedisCache).
"""

import pickle
from typing import TYPE_CHECKING

from django.core.cache import caches
from django.test import override_settings

if TYPE_CHECKING:
    from django_cachex.cache import RespCache
    from tests.fixtures.containers import RedisContainerInfo


class TestDjangoStyleOptions:
    """Test that Django-style configuration OPTIONS work."""

    def test_db_option(self, redis_container: RedisContainerInfo):
        host = redis_container.host
        port = redis_container.port

        # Django-style: db in OPTIONS
        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}",  # No db in URL
                "OPTIONS": {
                    "db": 2,
                },
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            # Test basic operation to verify connection works
            cache.set("test_db_option", "value")
            assert cache.get("test_db_option") == "value"
            cache.delete("test_db_option")

    def test_pool_class_option(self, redis_container: RedisContainerInfo):
        host = redis_container.host
        port = redis_container.port

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}/1",
                "OPTIONS": {
                    "pool_class": "redis.connection.ConnectionPool",
                },
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            cache.set("test_pool_class", "value")
            assert cache.get("test_pool_class") == "value"
            cache.delete("test_pool_class")

    def test_parser_class_option(self, redis_container: RedisContainerInfo):
        host = redis_container.host
        port = redis_container.port

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}/1",
                "OPTIONS": {
                    "parser_class": "redis.connection.DefaultParser",
                },
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            cache.set("test_parser_class", "value")
            assert cache.get("test_parser_class") == "value"
            cache.delete("test_parser_class")


class TestSerializerConfiguration:
    """Test various serializer configuration styles."""

    def test_serializer_class(self, redis_container: RedisContainerInfo):
        host = redis_container.host
        port = redis_container.port

        class SimpleSerializer:
            """Simple pickle-based serializer."""

            def __init__(self, options=None):
                pass

            def dumps(self, value):
                return pickle.dumps(value)

            def loads(self, value):
                return pickle.loads(value)

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}/3",
                "OPTIONS": {
                    "serializer": SimpleSerializer,  # Class, not string
                },
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            cache.set("test_class_serializer", {"key": "value"})
            result = cache.get("test_class_serializer")
            assert result == {"key": "value"}
            cache.delete("test_class_serializer")

    def test_serializer_instance(self, redis_container: RedisContainerInfo):
        host = redis_container.host
        port = redis_container.port

        class SimpleSerializer:
            """Simple pickle-based serializer."""

            def dumps(self, value):
                return pickle.dumps(value)

            def loads(self, value):
                return pickle.loads(value)

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}/4",
                "OPTIONS": {
                    "serializer": SimpleSerializer(),  # Instance, not class
                },
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            cache.set("test_instance_serializer", [1, 2, 3])
            result = cache.get("test_instance_serializer")
            assert result == [1, 2, 3]
            cache.delete("test_instance_serializer")

    def test_serializer_class_no_options(self, redis_container: RedisContainerInfo):
        host = redis_container.host
        port = redis_container.port

        class NoOptionsSerializer:
            """Serializer that doesn't accept options (like Django's RedisSerializer)."""

            def __init__(self):
                pass

            def dumps(self, value):
                return pickle.dumps(value)

            def loads(self, value):
                return pickle.loads(value)

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}/5",
                "OPTIONS": {
                    "serializer": NoOptionsSerializer,
                },
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            cache.set("test_no_options", "test_value")
            result = cache.get("test_no_options")
            assert result == "test_value"
            cache.delete("test_no_options")


class TestIntegerOptimization:
    """Test that integer optimization works correctly."""

    def test_integer_stored_efficiently(self, cache: RespCache):
        """Integers should be stored without serialization overhead."""
        cache.set("test_int", 42)
        result = cache.get("test_int")
        assert result == 42
        assert isinstance(result, int)

    def test_large_integer(self, cache: RespCache):
        large_int = 2**60
        cache.set("test_large_int", large_int)
        result = cache.get("test_large_int")
        assert result == large_int

    def test_negative_integer(self, cache: RespCache):
        cache.set("test_neg_int", -999)
        result = cache.get("test_neg_int")
        assert result == -999

    def test_boolean_not_integer_optimized(self, cache: RespCache):
        cache.set("test_bool_true", True)
        cache.set("test_bool_false", False)
        assert cache.get("test_bool_true") is True
        assert cache.get("test_bool_false") is False


class TestLocationFormats:
    """Test various LOCATION format variations."""

    def test_list_location(self, redis_container: RedisContainerInfo):
        host = redis_container.host
        port = redis_container.port
        url = f"redis://{host}:{port}/6"

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": [url, url],  # List format
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            cache.set("test_list_location", "value")
            assert cache.get("test_list_location") == "value"
            cache.delete("test_list_location")

    def test_comma_separated_location(self, redis_container: RedisContainerInfo):
        host = redis_container.host
        port = redis_container.port
        url = f"redis://{host}:{port}/6"

        caches_config = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"{url},{url}",  # Comma-separated
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            cache.set("test_comma_location", "value")
            assert cache.get("test_comma_location") == "value"
            cache.delete("test_comma_location")
