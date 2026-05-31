"""Cache client backed by the Rust ``RedisRsAdapter``.

Each adapter class is a structural-typed thin wrapper around the
corresponding Rust ``#[pyclass]`` from
:mod:`django_cachex.adapters._redis_rs`, with :class:`RespAdapterProtocol`
mixed in so type checkers see the full command surface (the Rust class
itself satisfies the protocol structurally, runtime-checkable).
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

        # The Python wrappers below (RedisRsClusterAdapter, RedisRsSentinelAdapter)
        # inherit from BOTH the Rust base and the Python RedisRsAdapter. If every
        # Rust name aliased the same _MissingRustExtension class, the resulting
        # MRO would have _MissingRustExtension appearing both as a direct base
        # and as a transitive base via RedisRsAdapter, which C3 linearization
        # rejects. Mirror the real Rust class hierarchy (cluster/sentinel extend
        # the base adapter) with distinct subclasses so module import still
        # succeeds when the Rust extension is missing; instantiation still
        # raises ImportError through the shared __init__.
        class _RustRedisRsAdapter(_MissingRustExtension):  # type: ignore[no-redef]
            pass

        class _RustRedisRsAsyncPipelineAdapter(_MissingRustExtension):  # type: ignore[no-redef]
            pass

        class _RustRedisRsClusterAdapter(_RustRedisRsAdapter):  # type: ignore[no-redef]
            pass

        class _RustRedisRsPipelineAdapter(_MissingRustExtension):  # type: ignore[no-redef]
            pass

        class _RustRedisRsSentinelAdapter(_RustRedisRsAdapter):  # type: ignore[no-redef]
            pass


class RedisRsAdapter(_RustRedisRsAdapter, RespAdapterProtocol):
    """Standard-topology Rust client.

    The Rust ``RedisRsAdapter`` connects directly to redis-rs in
    ``__init__``; ``RespAdapterProtocol`` is a structural mixin,
    runtime-checkable, contributes no methods.
    """

    def lock(
        self,
        key: str,
        lease: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        """Build a :class:`~django_cachex.lock.Lock` bound to this adapter.

        Routes to the Python ``Lock`` (which speaks the Rust adapter's
        ``lock_acquire``/``release``/``extend`` primitives), so we pass
        our own ``lease``/``timeout`` names straight through.
        """
        from django_cachex.lock import Lock

        return Lock(
            self,
            key,
            lease=lease,
            sleep=sleep,
            blocking=blocking,
            timeout=timeout,
            thread_local=thread_local,
        )

    async def alock(
        self,
        key: str,
        lease: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        """Build an :class:`~django_cachex.lock.AsyncLock` bound to this adapter."""
        from django_cachex.lock import AsyncLock

        return AsyncLock(
            self,
            key,
            lease=lease,
            sleep=sleep,
            blocking=blocking,
            timeout=timeout,
            thread_local=thread_local,
        )


# Inherit from Python ``RedisRsAdapter`` so cluster mode picks up the
# Python ``lock()`` / ``alock()`` overrides via MRO (the Rust class only
# exposes the low-level ``lock_acquire`` / ``lock_release`` primitives).
class RedisRsClusterAdapter(_RustRedisRsClusterAdapter, RedisRsAdapter):
    """Rust client for Valkey/Redis cluster mode."""


# Same MRO rationale as ``RedisRsClusterAdapter``: pick up the Python
# ``lock()`` / ``alock()`` overrides from ``RedisRsAdapter``.
class RedisRsSentinelAdapter(_RustRedisRsSentinelAdapter, RedisRsAdapter):
    """Rust client for sentinel-managed Valkey/Redis topologies."""


class RedisRsPipelineAdapter(_RustRedisRsPipelineAdapter, RespPipelineProtocol):
    """Pipeline adapter: buffers ops, dispatches via ``pipeline_exec``."""


class RedisRsAsyncPipelineAdapter(_RustRedisRsAsyncPipelineAdapter, RespAsyncPipelineProtocol):
    """Async pipeline adapter: same buffering, awaitable ``execute()``."""


__all__ = [
    "RedisRsAdapter",
    "RedisRsAsyncPipelineAdapter",
    "RedisRsClusterAdapter",
    "RedisRsPipelineAdapter",
    "RedisRsSentinelAdapter",
]
