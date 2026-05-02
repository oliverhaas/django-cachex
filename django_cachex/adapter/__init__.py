"""Adapter layer — low-level Redis/Valkey ops per driver.

Each adapter wraps a specific underlying client library (redis-py,
valkey-py, our Rust driver, valkey-glide) and exposes the operation
surface that ``KeyValueCache`` delegates to.
"""

from django_cachex.adapter.cluster import (
    BaseKeyValueClusterAdapter,
    RedisClusterAdapter,
    ValkeyClusterAdapter,
)
from django_cachex.adapter.default import (
    BaseKeyValueAdapter,
    RedisAdapter,
    ValkeyAdapter,
)
from django_cachex.adapter.glide import ValkeyGlideAdapter
from django_cachex.adapter.pipeline import Pipeline
from django_cachex.adapter.protocols import (
    CachexProtocol,
    KeyValueAdapterProtocol,
    KeyValuePipelineProtocol,
)
from django_cachex.adapter.rust import (
    RustRedisAdapter,
    RustRedisClusterAdapter,
    RustRedisSentinelAdapter,
    RustValkeyAdapter,
    RustValkeyClusterAdapter,
    RustValkeySentinelAdapter,
)
from django_cachex.adapter.sentinel import (
    BaseKeyValueSentinelAdapter,
    RedisSentinelAdapter,
    ValkeySentinelAdapter,
)

__all__ = [
    "BaseKeyValueAdapter",
    "BaseKeyValueClusterAdapter",
    "BaseKeyValueSentinelAdapter",
    "CachexProtocol",
    "KeyValueAdapterProtocol",
    "KeyValuePipelineProtocol",
    "Pipeline",
    "RedisAdapter",
    "RedisClusterAdapter",
    "RedisSentinelAdapter",
    "RustRedisAdapter",
    "RustRedisClusterAdapter",
    "RustRedisSentinelAdapter",
    "RustValkeyAdapter",
    "RustValkeyClusterAdapter",
    "RustValkeySentinelAdapter",
    "ValkeyAdapter",
    "ValkeyClusterAdapter",
    "ValkeyGlideAdapter",
    "ValkeySentinelAdapter",
]
