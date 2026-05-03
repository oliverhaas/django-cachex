"""Adapter layer — low-level Redis/Valkey ops per driver.

Each adapter wraps a specific underlying client library (redis-py,
valkey-py, our Rust ``redis-rs`` driver, valkey-glide) and exposes the
operation surface that ``KeyValueCache`` delegates to.
"""

from django_cachex.adapters.pipeline import (
    BaseKeyValuePipelineAdapter,
    Pipeline,
    RedisPipelineAdapter,
)
from django_cachex.adapters.pipeline_redis_rs import RedisRsValkeyPipelineAdapter
from django_cachex.adapters.protocols import (
    KeyValueAdapterProtocol,
    KeyValuePipelineProtocol,
)
from django_cachex.adapters.redis_py import (
    RedisPyAdapter,
    RedisPyClusterAdapter,
    RedisPySentinelAdapter,
)
from django_cachex.adapters.redis_rs import (
    RedisRsValkeyAdapter,
    RedisRsValkeyClusterAdapter,
    RedisRsValkeySentinelAdapter,
)
from django_cachex.adapters.valkey_glide import ValkeyGlideAdapter, ValkeyGlidePipelineAdapter
from django_cachex.adapters.valkey_py import (
    ValkeyPyAdapter,
    ValkeyPyClusterAdapter,
    ValkeyPySentinelAdapter,
)

__all__ = [
    "BaseKeyValuePipelineAdapter",
    "KeyValueAdapterProtocol",
    "KeyValuePipelineProtocol",
    "Pipeline",
    "RedisPipelineAdapter",
    "RedisPyAdapter",
    "RedisPyClusterAdapter",
    "RedisPySentinelAdapter",
    "RedisRsValkeyAdapter",
    "RedisRsValkeyClusterAdapter",
    "RedisRsValkeyPipelineAdapter",
    "RedisRsValkeySentinelAdapter",
    "ValkeyGlideAdapter",
    "ValkeyGlidePipelineAdapter",
    "ValkeyPyAdapter",
    "ValkeyPyClusterAdapter",
    "ValkeyPySentinelAdapter",
]
