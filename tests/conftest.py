"""Pytest configuration for django-cachex tests."""

import sys
from pathlib import Path

from tests.fixtures import (
    cache,
    client_class,
    cluster_container,
    cluster_container_factory,
    compressors,
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
)

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
    "redis_images",
    "replica_container_factory",
    "replica_containers",
    "sentinel_container",
    "sentinel_container_factory",
    "sentinel_mode",
    "serializers",
    "settings",
]


def pytest_configure(config):
    """Add tests directory to Python path."""
    sys.path.insert(0, str(Path(__file__).absolute().parent))
