from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("django-cachex")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

from django_cachex.adapters.pipeline import AsyncPipeline, Pipeline
from django_cachex.exceptions import (
    CachexError,
    CompressorError,
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

__all__ = [
    "AsyncPipeline",
    "CachexError",
    "CompressorError",
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
