"""Adapter layer: low-level Redis/Valkey ops per driver.

Each adapter wraps a specific underlying client library (redis-py,
valkey-py, valkey-glide) and exposes the operation surface that
``RespCache`` delegates to. Exports resolve lazily (PEP 562) so importing
``django_cachex`` doesn't drag in every driver.
"""

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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
    from django_cachex.adapters.valkey_glide import (
        ValkeyGlideAdapter,
        ValkeyGlideAsyncPipelineAdapter,
        ValkeyGlideClusterAdapter,
        ValkeyGlidePipelineAdapter,
    )
    from django_cachex.adapters.valkey_py import (
        ValkeyPyAdapter,
        ValkeyPyAsyncPipelineAdapter,
        ValkeyPyClusterAdapter,
        ValkeyPyPipelineAdapter,
        ValkeyPySentinelAdapter,
    )

# Exported name -> defining submodule.
_LAZY_EXPORTS = {
    "AsyncPipeline": "django_cachex.adapters.pipeline",
    "Pipeline": "django_cachex.adapters.pipeline",
    "RespAdapterProtocol": "django_cachex.adapters.protocols",
    "RespAsyncPipelineProtocol": "django_cachex.adapters.protocols",
    "RespPipelineProtocol": "django_cachex.adapters.protocols",
    "RedisPyAdapter": "django_cachex.adapters.redis_py",
    "RedisPyAsyncPipelineAdapter": "django_cachex.adapters.redis_py",
    "RedisPyClusterAdapter": "django_cachex.adapters.redis_py",
    "RedisPyPipelineAdapter": "django_cachex.adapters.redis_py",
    "RedisPySentinelAdapter": "django_cachex.adapters.redis_py",
    "ValkeyGlideAdapter": "django_cachex.adapters.valkey_glide",
    "ValkeyGlideAsyncPipelineAdapter": "django_cachex.adapters.valkey_glide",
    "ValkeyGlideClusterAdapter": "django_cachex.adapters.valkey_glide",
    "ValkeyGlidePipelineAdapter": "django_cachex.adapters.valkey_glide",
    "ValkeyPyAdapter": "django_cachex.adapters.valkey_py",
    "ValkeyPyAsyncPipelineAdapter": "django_cachex.adapters.valkey_py",
    "ValkeyPyClusterAdapter": "django_cachex.adapters.valkey_py",
    "ValkeyPyPipelineAdapter": "django_cachex.adapters.valkey_py",
    "ValkeyPySentinelAdapter": "django_cachex.adapters.valkey_py",
}


def __getattr__(name: str) -> Any:
    """Resolve an adapter export on first access (PEP 562)."""
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)


__all__ = [
    "AsyncPipeline",
    "Pipeline",
    "RedisPyAdapter",
    "RedisPyAsyncPipelineAdapter",
    "RedisPyClusterAdapter",
    "RedisPyPipelineAdapter",
    "RedisPySentinelAdapter",
    "RespAdapterProtocol",
    "RespAsyncPipelineProtocol",
    "RespPipelineProtocol",
    "ValkeyGlideAdapter",
    "ValkeyGlideAsyncPipelineAdapter",
    "ValkeyGlideClusterAdapter",
    "ValkeyGlidePipelineAdapter",
    "ValkeyPyAdapter",
    "ValkeyPyAsyncPipelineAdapter",
    "ValkeyPyClusterAdapter",
    "ValkeyPyPipelineAdapter",
    "ValkeyPySentinelAdapter",
]
