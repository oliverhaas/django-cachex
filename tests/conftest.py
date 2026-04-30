"""Pytest configuration for django-cachex tests."""

from __future__ import annotations

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

# Tests that probe the underlying client object directly (asserting redis-py
# internal types, calling redis-py-only methods, or expecting cluster
# fan-out limitations the Rust driver doesn't share).
_RUST_RAW_CLIENT_PROBE_NODES: tuple[str, ...] = (
    "tests/cache/test_cache_hashes.py::TestHashKeyPrefixing::test_key_prefixed_but_fields_raw",
    "tests/cache/test_cache_options.py::test_custom_key_function",
    "tests/cache/test_cache_misc.py::TestScanOperations::test_scan_returns_keys",
    "tests/cache/test_cache_misc.py::TestScanOperations::test_scan_empty",
    "tests/cache/test_cache_misc_async.py::TestAsyncScan::test_ascan_returns_keys",
)

# Tests that probe redis-py-specific internals (connection pools, parser
# class, _lib wiring). These are inherently inapplicable to the Rust driver
# which doesn't expose those concepts.
_PY_INTERNALS_TEST_FILES: frozenset[str] = frozenset(
    {
        "test_django_cachex_internals.py",
        "test_client_libraries.py",
        "test_client.py",
        "test_replica.py",
    },
)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    skip_rust_internals = pytest.mark.skip(
        reason="redis-py internals (pools, parsers) don't apply to the Rust driver",
    )
    skip_rust_raw_client = pytest.mark.skip(
        reason="probes redis-py-specific raw-client behavior; not portable to the Rust driver",
    )
    for item in items:
        callspec = getattr(item, "callspec", None)
        if callspec is None or callspec.params.get("driver") != "rust":
            continue
        path_name = item.path.name
        if path_name in _PY_INTERNALS_TEST_FILES:
            item.add_marker(skip_rust_internals)
        elif any(item.nodeid.startswith(prefix) for prefix in _RUST_RAW_CLIENT_PROBE_NODES):
            item.add_marker(skip_rust_raw_client)


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
