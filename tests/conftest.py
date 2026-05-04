"""Pytest configuration for django-cachex tests."""

import pytest

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
    settings,
    stampede_cache,
)

# Tests that probe redis-py-specific internals (connection pools, parser
# class, _lib wiring). They hardcode ``redis.Redis``, ``redis.ConnectionPool``,
# etc. — so they only run for the redis-py adapter.
_REDIS_PY_INTERNALS_TEST_FILES: frozenset[str] = frozenset(
    {
        "test_internals.py",
        "test_client.py",
        "test_replica.py",
    },
)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    skip_non_redis_py = pytest.mark.skip(
        reason="redis-py-specific internals (pools, parsers) don't apply to this adapter",
    )
    for item in items:
        callspec = getattr(item, "callspec", None)
        if callspec is None:
            continue
        adapter = callspec.params.get("resp_adapter")
        if adapter is None or adapter == "redis-py":
            continue
        if item.path.name in _REDIS_PY_INTERNALS_TEST_FILES:
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
    "settings",
    "stampede_cache",
]
