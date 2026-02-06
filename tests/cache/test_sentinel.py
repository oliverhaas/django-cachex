"""Integration tests for Sentinel setup.

Tests the Sentinel functionality where:
- Sentinel monitors primary and replica nodes
- Writes go to the primary
- Reads can be distributed to replicas
"""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.test import override_settings

if TYPE_CHECKING:
    from tests.fixtures.containers import SentinelContainerInfo


class TestSentinelSetup:
    """Tests for Sentinel setup with both Redis and Valkey."""

    def test_sentinel_containers_start(
        self,
        sentinel_container: SentinelContainerInfo,
        redis_images: tuple[str, str],
    ):
        """Test that sentinel containers start successfully."""
        _image, client_library = redis_images
        assert sentinel_container.host
        assert sentinel_container.port > 0
        assert sentinel_container.client_library == client_library

    def test_sentinel_basic_operations(
        self,
        sentinel_container: SentinelContainerInfo,
        redis_images: tuple[str, str],
    ):
        """Test basic cache operations through Sentinel."""
        image, client_library = redis_images

        # Use appropriate backend based on client library
        if client_library == "valkey":
            backend = "django_cachex.client.ValkeySentinelCache"
            scheme = "valkey"
        else:
            backend = "django_cachex.client.RedisSentinelCache"
            scheme = "redis"

        caches_config = {
            "default": {
                "BACKEND": backend,
                "LOCATION": f"{scheme}://mymaster/0",
                "OPTIONS": {
                    "sentinels": [(sentinel_container.host, sentinel_container.port)],
                },
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Test set/get
            cache.set("sentinel_test_key", "test_value", timeout=60)
            result = cache.get("sentinel_test_key")
            assert result == "test_value", f"Failed with {image}, {client_library}"

            # Test set_many/get_many
            data = {"key1": "value1", "key2": "value2", "key3": "value3"}
            cache.set_many(data, timeout=60)
            result = cache.get_many(list(data.keys()))
            assert result == data

            # Test delete
            cache.delete("sentinel_test_key")
            assert cache.get("sentinel_test_key") is None

            # Clean up
            cache.delete_many(list(data.keys()))

        with suppress(KeyError, AttributeError):
            del caches["default"]

    def test_sentinel_incr_decr(
        self,
        sentinel_container: SentinelContainerInfo,
        redis_images: tuple[str, str],
    ):
        """Test increment/decrement operations through Sentinel."""
        _image, client_library = redis_images

        if client_library == "valkey":
            backend = "django_cachex.client.ValkeySentinelCache"
            scheme = "valkey"
        else:
            backend = "django_cachex.client.RedisSentinelCache"
            scheme = "redis"

        caches_config = {
            "default": {
                "BACKEND": backend,
                "LOCATION": f"{scheme}://mymaster/0",
                "OPTIONS": {
                    "sentinels": [(sentinel_container.host, sentinel_container.port)],
                },
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Set initial value
            cache.set("counter", 10, timeout=60)

            # Increment
            result = cache.incr("counter", 5)
            assert result == 15

            # Decrement
            result = cache.decr("counter", 3)
            assert result == 12

            # Clean up
            cache.delete("counter")

        with suppress(KeyError, AttributeError):
            del caches["default"]


@pytest.mark.asyncio
class TestSentinelAsync:
    """Async tests for Sentinel setup."""

    async def test_async_operations_with_sentinel(
        self,
        sentinel_container: SentinelContainerInfo,
        redis_images: tuple[str, str],
    ):
        """Test async operations work with Sentinel setup."""
        image, client_library = redis_images

        if client_library == "valkey":
            backend = "django_cachex.client.ValkeySentinelCache"
            scheme = "valkey"
        else:
            backend = "django_cachex.client.RedisSentinelCache"
            scheme = "redis"

        caches_config = {
            "default": {
                "BACKEND": backend,
                "LOCATION": f"{scheme}://mymaster/0",
                "OPTIONS": {
                    "sentinels": [(sentinel_container.host, sentinel_container.port)],
                },
            },
        }

        with suppress(KeyError, AttributeError):
            del caches["default"]

        with override_settings(CACHES=caches_config):
            cache = caches["default"]

            # Async set/get
            await cache.aset("async_sentinel_test", "async_value", timeout=60)
            value = await cache.aget("async_sentinel_test")
            assert value == "async_value", f"Failed with {image}, {client_library}"

            # Async delete
            await cache.adelete("async_sentinel_test")
            value = await cache.aget("async_sentinel_test")
            assert value is None

        with suppress(KeyError, AttributeError):
            del caches["default"]
