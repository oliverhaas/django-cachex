"""django-cachex: Redis/Valkey cache backends for Django.

Public names resolve lazily (PEP 562) so ``import django_cachex`` stays free
of driver imports.
"""

import importlib
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any

try:
    __version__ = version("django-cachex")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

if TYPE_CHECKING:
    from django_cachex.adapters.pipeline import AsyncPipeline, Pipeline
    from django_cachex.exceptions import (
        CachexError,
        CompressorError,
        KeyNotFoundError,
        NotSupportedError,
        SerializerError,
        WrongTypeError,
    )
    from django_cachex.lock import LockError, LockNotOwnedError
    from django_cachex.script import (
        ScriptHelpers,
        decode_list_post,
        decode_single_post,
        full_encode_pre,
        keys_only_pre,
    )
    from django_cachex.semaphore import (
        Semaphore,
        SemaphoreError,
        SemaphoreTimeoutError,
    )
    from django_cachex.stampede import StampedeConfig

# Exported name -> defining submodule.
_LAZY_EXPORTS = {
    "AsyncPipeline": "django_cachex.adapters.pipeline",
    "Pipeline": "django_cachex.adapters.pipeline",
    "CachexError": "django_cachex.exceptions",
    "CompressorError": "django_cachex.exceptions",
    "KeyNotFoundError": "django_cachex.exceptions",
    "NotSupportedError": "django_cachex.exceptions",
    "SerializerError": "django_cachex.exceptions",
    "WrongTypeError": "django_cachex.exceptions",
    "LockError": "django_cachex.lock",
    "LockNotOwnedError": "django_cachex.lock",
    "ScriptHelpers": "django_cachex.script",
    "decode_list_post": "django_cachex.script",
    "decode_single_post": "django_cachex.script",
    "full_encode_pre": "django_cachex.script",
    "keys_only_pre": "django_cachex.script",
    "Semaphore": "django_cachex.semaphore",
    "SemaphoreError": "django_cachex.semaphore",
    "SemaphoreTimeoutError": "django_cachex.semaphore",
    "StampedeConfig": "django_cachex.stampede",
}


def __getattr__(name: str) -> Any:
    """Resolve a public export on first access (PEP 562)."""
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
    "CachexError",
    "CompressorError",
    "KeyNotFoundError",
    "LockError",
    "LockNotOwnedError",
    "NotSupportedError",
    "Pipeline",
    "ScriptHelpers",
    "Semaphore",
    "SemaphoreError",
    "SemaphoreTimeoutError",
    "SerializerError",
    "StampedeConfig",
    "WrongTypeError",
    "__version__",
    "decode_list_post",
    "decode_single_post",
    "full_encode_pre",
    "keys_only_pre",
]

# Keep the metadata helpers out of ``django_cachex``'s public namespace.
del PackageNotFoundError, version
