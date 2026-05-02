"""Adapter layer — low-level Redis/Valkey ops per driver.

Each adapter wraps a specific underlying client library (redis-py,
valkey-py, our Rust ``redis-rs`` driver, valkey-glide) and exposes the
operation surface that ``KeyValueCache`` delegates to.
"""

from django_cachex.adapter.cluster import BaseKeyValueClusterAdapter
from django_cachex.adapter.default import BaseKeyValueAdapter
from django_cachex.adapter.pipeline import (
    BaseKeyValuePipelineAdapter,
    Pipeline,
    RedisPipelineAdapter,
)
from django_cachex.adapter.pipeline_redis_rs import RedisRsValkeyPipelineAdapter
from django_cachex.adapter.protocols import (
    CachexProtocol,
    KeyValueAdapterProtocol,
    KeyValuePipelineProtocol,
)
from django_cachex.adapter.redis_py import (
    RedisAdapter,
    RedisClusterAdapter,
    RedisSentinelAdapter,
)
from django_cachex.adapter.redis_rs import (
    RedisRsValkeyAdapter,
    RedisRsValkeyClusterAdapter,
    RedisRsValkeySentinelAdapter,
)
from django_cachex.adapter.sentinel import BaseKeyValueSentinelAdapter
from django_cachex.adapter.valkey_glide import ValkeyGlideAdapter, ValkeyGlidePipelineAdapter
from django_cachex.adapter.valkey_py import (
    ValkeyAdapter,
    ValkeyClusterAdapter,
    ValkeySentinelAdapter,
)

__all__ = [
    "BaseKeyValueAdapter",
    "BaseKeyValueClusterAdapter",
    "BaseKeyValuePipelineAdapter",
    "BaseKeyValueSentinelAdapter",
    "CachexProtocol",
    "KeyValueAdapterProtocol",
    "KeyValuePipelineProtocol",
    "Pipeline",
    "RedisAdapter",
    "RedisClusterAdapter",
    "RedisPipelineAdapter",
    "RedisRsValkeyAdapter",
    "RedisRsValkeyClusterAdapter",
    "RedisRsValkeyPipelineAdapter",
    "RedisRsValkeySentinelAdapter",
    "RedisSentinelAdapter",
    "ValkeyAdapter",
    "ValkeyClusterAdapter",
    "ValkeyGlideAdapter",
    "ValkeyGlidePipelineAdapter",
    "ValkeySentinelAdapter",
]
