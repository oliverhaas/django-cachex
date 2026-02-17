from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("django-cachex")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

# Re-export commonly used items for convenience
from django_cachex.exceptions import (
    CompressorError,
    NotSupportedError,
    SerializerError,
)
from django_cachex.script import (
    ScriptHelpers,
    decode_list_or_none_post,
    decode_list_post,
    decode_single_post,
    full_encode_pre,
    keys_only_pre,
    noop_post,
)

__all__ = [
    "CompressorError",
    "NotSupportedError",
    "ScriptHelpers",
    "SerializerError",
    "__version__",
    "decode_list_or_none_post",
    "decode_list_post",
    "decode_single_post",
    "full_encode_pre",
    "keys_only_pre",
    "noop_post",
]
