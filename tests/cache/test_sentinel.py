"""Integration tests for Sentinel setup.

Covers configuration through a live Sentinel: the cache discovers the primary
through it and runs every operation against that primary. Replica reads are
not exercised here; the fixture monitors a single node.
"""

from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.test import override_settings

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django_cachex.cache import RespCache
    from tests.fixtures.containers import SentinelContainerInfo


@pytest.fixture
def sentinel_cache(
    request: pytest.FixtureRequest,
    sentinel_container: SentinelContainerInfo,
    resp_images: tuple[str, str],
) -> Iterator[RespCache]:
    """A Sentinel cache on the driver ``resp_images`` selected.

    Extra OPTIONS come from an ``indirect`` parametrization, so a test can add
    ``sentinel_kwargs`` and friends without repeating the whole config.
    """
    _image, client_library = resp_images
    scheme = "valkey" if client_library == "valkey" else "redis"
    backend = (
        "django_cachex.cache.ValkeySentinelCache"
        if client_library == "valkey"
        else "django_cachex.cache.RedisSentinelCache"
    )
    options = {
        "sentinels": [(sentinel_container.host, sentinel_container.port)],
        **getattr(request, "param", {}),
    }
    caches_config = {
        "default": {
            "BACKEND": backend,
            "LOCATION": f"{scheme}://mymaster/0",
            "OPTIONS": options,
        },
    }
    with override_settings(CACHES=caches_config):
        yield caches["default"]


class TestSentinelSetup:
    """Tests for Sentinel setup with both Redis and Valkey."""

    def test_sentinel_containers_start(
        self,
        sentinel_container: SentinelContainerInfo,
        resp_images: tuple[str, str],
    ):
        _image, client_library = resp_images
        assert sentinel_container.host
        assert sentinel_container.port > 0
        assert sentinel_container.client_library == client_library

    def test_sentinel_basic_operations(self, sentinel_cache: RespCache):
        sentinel_cache.set("sentinel_test_key", "test_value", timeout=60)
        assert sentinel_cache.get("sentinel_test_key") == "test_value"

        data = {"key1": "value1", "key2": "value2", "key3": "value3"}
        sentinel_cache.set_many(data, timeout=60)
        assert sentinel_cache.get_many(list(data.keys())) == data

        sentinel_cache.delete("sentinel_test_key")
        assert sentinel_cache.get("sentinel_test_key") is None

        sentinel_cache.delete_many(list(data.keys()))

    def test_sentinel_incr_decr(self, sentinel_cache: RespCache):
        sentinel_cache.set("counter", 10, timeout=60)

        assert sentinel_cache.incr("counter", 5) == 15
        assert sentinel_cache.decr("counter", 3) == 12

        sentinel_cache.delete("counter")

    @pytest.mark.parametrize(
        "sentinel_cache",
        [{"sentinel_kwargs": {"socket_timeout": 5.0, "socket_connect_timeout": 5.0}}],
        indirect=True,
    )
    def test_sentinel_kwargs_reach_the_discovery_clients(self, sentinel_cache: RespCache):
        """``sentinel_kwargs`` configures the clients that talk to Sentinel, not the data path."""
        adapter = sentinel_cache.adapter

        for discovery_client in adapter._sentinel.sentinels:
            assert discovery_client.connection_pool.connection_kwargs["socket_timeout"] == 5.0
            assert discovery_client.connection_pool.connection_kwargs["socket_connect_timeout"] == 5.0

        # Discovery still works, and the data path keeps its own timeouts.
        sentinel_cache.set("sentinel_kwargs_probe", "value", timeout=60)
        assert sentinel_cache.get("sentinel_kwargs_probe") == "value"
        sentinel_cache.delete("sentinel_kwargs_probe")


@pytest.mark.asyncio
class TestSentinelAsync:
    """Async tests for Sentinel setup."""

    async def test_async_operations_with_sentinel(self, sentinel_cache: RespCache):
        await sentinel_cache.aset("async_sentinel_test", "async_value", timeout=60)
        assert await sentinel_cache.aget("async_sentinel_test") == "async_value"

        await sentinel_cache.adelete("async_sentinel_test")
        assert await sentinel_cache.aget("async_sentinel_test") is None
