"""Pytest configuration for django-cachex tests."""

import pytest

from tests.fixtures import (
    cache,
    client_class,
    cluster_container,
    cluster_container_factory,
    compressors,
    driver,
    native_parser,
    redis_container,
    redis_container_factory,
    redis_images,
    replica_container_factory,
    replica_containers,
    sentinel_container,
    sentinel_container_factory,
    sentinel_mode,
    serializers,
    settings,
    stampede_cache,
)

# Tests that probe redis-py-specific internals (connection pools, parser
# class, _lib wiring). These are inherently inapplicable to the Rust driver
# which doesn't expose those concepts.
_PY_INTERNALS_TEST_FILES: frozenset[str] = frozenset(
    {
        "test_internals.py",
        "test_client.py",
        "test_replica.py",
    },
)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    skip_rust_internals = pytest.mark.skip(
        reason="redis-py internals (pools, parsers) don't apply to the Rust driver",
    )
    for item in items:
        callspec = getattr(item, "callspec", None)
        if callspec is None or callspec.params.get("driver") != "redis-rs":
            continue
        if item.path.name in _PY_INTERNALS_TEST_FILES:
            item.add_marker(skip_rust_internals)


# Re-export fixtures so pytest can discover them
__all__ = [
    "cache",
    "client_class",
    "cluster_container",
    "cluster_container_factory",
    "compressors",
    "driver",
    "native_parser",
    "redis_container",
    "redis_container_factory",
    "redis_images",
    "replica_container_factory",
    "replica_containers",
    "sentinel_container",
    "sentinel_container_factory",
    "sentinel_mode",
    "serializers",
    "settings",
    "stampede_cache",
]
