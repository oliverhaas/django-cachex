"""Test fixtures for django-cachex."""

from django_cachex.cache import RespCache
from tests.fixtures.cache import (
    cache,
    client_class,
    compressors,
    native_parser,
    resp_adapter,
    sentinel_mode,
    serializers,
    stampede_cache,
)
from tests.fixtures.containers import (
    RedisContainerInfo,
    ReplicaSetContainerInfo,
    SentinelContainerInfo,
    cluster_container,
    cluster_container_factory,
    redis_container,
    redis_container_factory,
    replica_container_factory,
    replica_containers,
    resp_images,
    sentinel_container,
    sentinel_container_factory,
)
from tests.fixtures.settings import settings

__all__ = [
    "RedisContainerInfo",
    "ReplicaSetContainerInfo",
    "RespCache",
    "SentinelContainerInfo",
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
