"""Tests for Django builtin Redis backend compatibility.

These tests verify that django-cachex can be used as a drop-in replacement
for Django's builtin Redis backend (django.core.cache.backends.redis.RedisCache).
"""

import importlib
import pickle
from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.test import override_settings

from tests.fixtures.cache import ADAPTER_IMAGES

if TYPE_CHECKING:
    from django_cachex.cache import RespCache
    from tests.fixtures.containers import RedisContainerInfo

# ``RedisPyAdapter`` subclasses ``ValkeyPyAdapter``, so the pool and parser
# plumbing is one code path with two sets of driver classes behind it. Each
# driver runs against its home image, the way ``resp_adapter`` picks one.
POOL_OPTION_BACKENDS = [
    pytest.param("django_cachex.cache.RedisCache", "redis", ADAPTER_IMAGES["redis-py"], id="redis-py"),
    pytest.param("django_cachex.cache.ValkeyCache", "valkey", ADAPTER_IMAGES["valkey-py"], id="valkey-py"),
]


@pytest.mark.parametrize(("backend", "driver", "resp_images"), POOL_OPTION_BACKENDS, indirect=["resp_images"])
class TestDjangoStyleOptions:
    """Django-style configuration OPTIONS reach the pool on either driver."""

    @pytest.fixture(autouse=True)
    def _driver_runs_on_its_home_image(
        self,
        driver: str,
        resp_images: tuple[str, str],
        redis_container: RedisContainerInfo,
    ):
        del resp_images
        assert redis_container.client_library == driver

    def test_db_option(self, backend: str, driver: str, redis_container: RedisContainerInfo):
        """``db`` in OPTIONS, Django-style, with no db in the URL."""
        del driver
        caches_config = {
            "default": {
                "BACKEND": backend,
                "LOCATION": f"redis://{redis_container.host}:{redis_container.port}",
                "OPTIONS": {"db": 2},
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            pool = cache.adapter._get_connection_pool(write=True)

            assert pool.connection_kwargs["db"] == 2
            cache.set("test_db_option", "value")
            assert cache.get("test_db_option") == "value"
            cache.delete("test_db_option")

    def test_pool_class_option(self, backend: str, driver: str, redis_container: RedisContainerInfo):
        pool_class = importlib.import_module(f"{driver}.connection").BlockingConnectionPool
        caches_config = {
            "default": {
                "BACKEND": backend,
                "LOCATION": f"redis://{redis_container.host}:{redis_container.port}/1",
                "OPTIONS": {"pool_class": f"{driver}.connection.BlockingConnectionPool"},
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            pool = cache.adapter._get_connection_pool(write=True)

            assert isinstance(pool, pool_class)
            cache.set("test_pool_class", "value")
            assert cache.get("test_pool_class") == "value"
            cache.delete("test_pool_class")

    def test_parser_class_option(
        self,
        backend: str,
        driver: str,
        redis_container: RedisContainerInfo,
    ):
        module, name = {
            "redis": ("redis._parsers.hiredis", "_HiredisParser"),
            "valkey": ("valkey._parsers.libvalkey", "_LibvalkeyParser"),
        }[driver]
        parser_class = getattr(importlib.import_module(module), name)
        caches_config = {
            "default": {
                "BACKEND": backend,
                "LOCATION": f"redis://{redis_container.host}:{redis_container.port}/1",
                "OPTIONS": {"parser_class": f"{parser_class.__module__}.{parser_class.__qualname__}"},
            },
        }

        with override_settings(CACHES=caches_config):
            cache = caches["default"]
            pool = cache.adapter._get_connection_pool(write=True)

            assert pool.connection_kwargs["parser_class"] is parser_class
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
