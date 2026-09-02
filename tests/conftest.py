"""Pytest configuration for django-cachex tests."""

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from django.core.cache import caches

from tests.fixtures import (
    cache,
    client_class,
    cluster_container,
    cluster_container_factory,
    compressors,
    native_parser,
    redis_container,
    redis_container_factory,
    replica_container_factory,
    replica_containers,
    resp_adapter,
    resp_images,
    sentinel_container,
    sentinel_container_factory,
    sentinel_mode,
    serializers,
    stampede_cache,
    stampede_topology,
    topology,
)
from tests.fixtures.cache import REDIS_PY_INTERNALS_TEST_FILES

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@pytest_asyncio.fixture
async def _aclose_caches() -> AsyncIterator[None]:
    """Disconnect the async pools a test opened before its event loop closes."""
    yield
    for backend in caches.all(initialized_only=True):
        await backend.aclose()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    skip_non_redis_py = pytest.mark.skip(
        reason="redis-py-specific internals (pools, parsers) don't apply to this adapter",
    )
    for item in items:
        if item.get_closest_marker("asyncio"):
            item.fixturenames.append("_aclose_caches")
        callspec = getattr(item, "callspec", None)
        if callspec is None:
            continue
        adapter = callspec.params.get("resp_adapter")
        if adapter is None or adapter == "redis-py":
            continue
        if item.path.name in REDIS_PY_INTERNALS_TEST_FILES:
            item.add_marker(skip_non_redis_py)


# Re-export fixtures so pytest can discover them
__all__ = [
    "cache",
    "client_class",
    "cluster_container",
    "cluster_container_factory",
    "compressors",
    "native_parser",
    "redis_container",
    "redis_container_factory",
    "replica_container_factory",
    "replica_containers",
    "resp_adapter",
    "resp_images",
    "sentinel_container",
    "sentinel_container_factory",
    "sentinel_mode",
    "serializers",
    "stampede_cache",
    "stampede_topology",
    "topology",
]
