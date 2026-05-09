"""Cache client backed by the Rust ``RedisRsAdapter``.

Each adapter class is a structural-typed thin wrapper around the
corresponding Rust ``#[pyclass]`` from
:mod:`django_cachex.adapters._redis_rs`, with :class:`RespAdapterProtocol`
mixed in so type checkers see the full command surface (the Rust class
itself satisfies the protocol structurally — runtime-checkable).
"""

from typing import TYPE_CHECKING, Any

from django_cachex.adapters.protocols import (
    RespAdapterProtocol,
    RespAsyncPipelineProtocol,
    RespPipelineProtocol,
)

# The native ``_redis_rs`` extension ships in the optional
# ``django-cachex-redis-rs`` binary package. When only the pure wheel is
# installed, defer the friendly error to instantiation so importing the
# cache module still works (the contract the opt-out smoke test asserts).
# At type-check time we always see the real Rust classes (via the ``.pyi``
# stub); at runtime the fallback substitutes a stub whose ``__init__``
# raises a friendly ``ImportError``.
if TYPE_CHECKING:
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
else:
    try:
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
    except ImportError as _import_err:
        # ``except ... as`` clears the binding when the block exits, so
        # capture the original cause for the deferred error.
        _RUST_IMPORT_ERROR = _import_err

        class _MissingRustExtension:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                raise ImportError(
                    "django-cachex Rust driver is not available. Install with the "
                    "`redis-rs` extra to pull in the django-cachex-redis-rs binary "
                    "package: pip install django-cachex[redis-rs]. Prebuilt wheels "
                    "are published for Linux x86_64/aarch64, macOS arm64, and "
                    "Windows amd64 (cp314, cp314t).",
                ) from _RUST_IMPORT_ERROR

        _RustRedisRsAdapter = _MissingRustExtension
        _RustRedisRsAsyncPipelineAdapter = _MissingRustExtension
        _RustRedisRsClusterAdapter = _MissingRustExtension
        _RustRedisRsPipelineAdapter = _MissingRustExtension
        _RustRedisRsSentinelAdapter = _MissingRustExtension


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
