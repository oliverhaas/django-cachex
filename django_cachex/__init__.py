from importlib.metadata import PackageNotFoundError, version
from pkgutil import extend_path

# Extend the package path so the optional django-cachex-redis-rs binary package
# (which ships ``adapters/_redis_rs*`` files into this namespace) is
# discovered when both packages are installed side by side.
__path__ = extend_path(__path__, __name__)

try:
    __version__ = version("django-cachex")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

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
