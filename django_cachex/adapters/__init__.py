"""Adapter layer — low-level Redis/Valkey ops per driver.

Each adapter wraps a specific underlying client library (redis-py,
valkey-py, our Rust ``redis-rs`` driver, valkey-glide) and exposes the
operation surface that ``RespCache`` delegates to.
"""

from django_cachex.adapters.pipeline import AsyncPipeline, Pipeline
from django_cachex.adapters.protocols import (
    RespAdapterProtocol,
    RespAsyncPipelineProtocol,
    RespPipelineProtocol,
)
from django_cachex.adapters.redis_py import (
    RedisPyAdapter,
    RedisPyAsyncPipelineAdapter,
    RedisPyClusterAdapter,
    RedisPyPipelineAdapter,
    RedisPySentinelAdapter,
)
from django_cachex.adapters.redis_rs import (
    RedisRsAdapter,
    RedisRsAsyncPipelineAdapter,
    RedisRsClusterAdapter,
    RedisRsPipelineAdapter,
    RedisRsSentinelAdapter,
)
from django_cachex.adapters.valkey_glide import (
    ValkeyGlideAdapter,
    ValkeyGlideAsyncPipelineAdapter,
    ValkeyGlidePipelineAdapter,
)
from django_cachex.adapters.valkey_py import (
    ValkeyPyAdapter,
    ValkeyPyAsyncPipelineAdapter,
    ValkeyPyClusterAdapter,
    ValkeyPyPipelineAdapter,
    ValkeyPySentinelAdapter,
)

__all__ = [
    "AsyncPipeline",
    "Pipeline",
    "RedisPyAdapter",
    "RedisPyAsyncPipelineAdapter",
    "RedisPyClusterAdapter",
    "RedisPyPipelineAdapter",
    "RedisPySentinelAdapter",
    "RedisRsAdapter",
    "RedisRsAsyncPipelineAdapter",
    "RedisRsClusterAdapter",
    "RedisRsPipelineAdapter",
    "RedisRsSentinelAdapter",
    "RespAdapterProtocol",
    "RespAsyncPipelineProtocol",
    "RespPipelineProtocol",
    "ValkeyGlideAdapter",
    "ValkeyGlideAsyncPipelineAdapter",
    "ValkeyGlidePipelineAdapter",
    "ValkeyPyAdapter",
    "ValkeyPyAsyncPipelineAdapter",
    "ValkeyPyClusterAdapter",
    "ValkeyPyPipelineAdapter",
    "ValkeyPySentinelAdapter",
]
