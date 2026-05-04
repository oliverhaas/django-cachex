"""Cache client backed by the Rust ``RedisRsAdapter``.

Each adapter class is a structural-typed thin wrapper around the
corresponding Rust ``#[pyclass]`` from
:mod:`django_cachex.adapters._redis_rs`, with :class:`RespAdapterProtocol`
mixed in so type checkers see the full command surface (the Rust class
itself satisfies the protocol structurally — runtime-checkable).
"""

from django_cachex.adapters._redis_rs import (
    RedisRsAdapter as _RustRedisRsAdapter,
)
from django_cachex.adapters._redis_rs import (
    RedisRsAsyncPipelineAdapter as _RustRedisRsAsyncPipelineAdapter,
)
from django_cachex.adapters._redis_rs import (
    RedisRsClusterAdapter as _RustRedisRsClusterAdapter,
)
from django_cachex.adapters._redis_rs import (
    RedisRsPipelineAdapter as _RustRedisRsPipelineAdapter,
)
from django_cachex.adapters._redis_rs import (
    RedisRsSentinelAdapter as _RustRedisRsSentinelAdapter,
)
from django_cachex.adapters.protocols import (
    RespAdapterProtocol,
    RespAsyncPipelineProtocol,
    RespPipelineProtocol,
)


class RedisRsAdapter(_RustRedisRsAdapter, RespAdapterProtocol):
    """Standard-topology Rust client.

    The Rust ``RedisRsAdapter`` connects directly to redis-rs in
    ``__init__``; ``RespAdapterProtocol`` is a structural mixin —
    runtime-checkable, contributes no methods.
    """


class RedisRsClusterAdapter(_RustRedisRsClusterAdapter, RespAdapterProtocol):
    """Rust client for Valkey/Redis cluster mode."""


class RedisRsSentinelAdapter(_RustRedisRsSentinelAdapter, RespAdapterProtocol):
    """Rust client for sentinel-managed Valkey/Redis topologies."""


class RedisRsPipelineAdapter(_RustRedisRsPipelineAdapter, RespPipelineProtocol):
    """Pipeline adapter — buffers ops, dispatches via ``pipeline_exec``."""


class RedisRsAsyncPipelineAdapter(_RustRedisRsAsyncPipelineAdapter, RespAsyncPipelineProtocol):
    """Async pipeline adapter — same buffering, awaitable ``execute()``."""


__all__ = [
    "RedisRsAdapter",
    "RedisRsAsyncPipelineAdapter",
    "RedisRsClusterAdapter",
    "RedisRsPipelineAdapter",
    "RedisRsSentinelAdapter",
]
