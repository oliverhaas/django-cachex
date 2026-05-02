"""Adapter layer — low-level Redis/Valkey ops per driver.

Each adapter wraps a specific underlying client library (redis-py,
valkey-py, our Rust ``redis-rs`` driver, valkey-glide) and exposes the
operation surface that ``KeyValueCache`` delegates to.
"""

from django_cachex.adapters.cluster import BaseKeyValueClusterAdapter
from django_cachex.adapters.default import BaseKeyValueAdapter
from django_cachex.adapters.pipeline import (
    BaseKeyValuePipelineAdapter,
    Pipeline,
    RedisPipelineAdapter,
)
from django_cachex.adapters.pipeline_redis_rs import RedisRsValkeyPipelineAdapter
from django_cachex.adapters.protocols import (
    CachexProtocol,
    KeyValueAdapterProtocol,
    KeyValuePipelineProtocol,
)
from django_cachex.adapters.redis_py import (
    RedisAdapter,
    RedisClusterAdapter,
    RedisSentinelAdapter,
)
from django_cachex.adapters.redis_rs import (
    RedisRsValkeyAdapter,
    RedisRsValkeyClusterAdapter,
    RedisRsValkeySentinelAdapter,
)
from django_cachex.adapters.sentinel import BaseKeyValueSentinelAdapter
from django_cachex.adapters.valkey_glide import ValkeyGlideAdapter, ValkeyGlidePipelineAdapter
from django_cachex.adapters.valkey_py import (
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
