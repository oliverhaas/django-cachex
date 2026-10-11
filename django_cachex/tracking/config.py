"""The ``OPTIONS["tracking"]`` setting of a Redis or Valkey cache."""

import codecs
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlsplit

from django.core.exceptions import ImproperlyConfigured

if TYPE_CHECKING:
    from collections.abc import Sequence

DEFAULT_MAX_STALENESS = 1.0
DEFAULT_MAX_BYTES = 32 * 1024 * 1024

_KEYS = frozenset({"prefix", "max_staleness", "max_bytes"})

# The client-side caching options of redis-py and of valkey-py. Either would
# track keys a second time, on the pooled connections.
_DRIVER_CACHE_OPTIONS = (
    "cache",
    "cache_config",
    "cache_factory",
    "cache_enabled",
    "client_cache",
    "cache_max_size",
    "cache_ttl",
    "cache_policy",
    "cache_deny_list",
    "cache_allow_list",
)


@dataclass(frozen=True, slots=True)
class TrackingConfig:
    """What the server tracks for a cache, how stale a copy can get, and how much the copies can hold."""

    prefix: str
    max_staleness: float = DEFAULT_MAX_STALENESS
    max_bytes: int = DEFAULT_MAX_BYTES

    def covers(self, key: object) -> bool:
        """Whether the server tracks ``key``: a str or bytes key under the prefix."""
        if isinstance(key, str):
            return key.startswith(self.prefix)
        if isinstance(key, bytes):
            return key.startswith(self.prefix.encode("utf-8", "surrogateescape"))
        return False


def parse_tracking(
    value: Any,
    *,
    backend: str,
    supported: bool,
    key_prefix: str,
    default_key_func: bool,
    options: dict[str, Any],
    urls: Sequence[str],
) -> TrackingConfig | None:
    """Validate ``OPTIONS["tracking"]`` against the other options and the ``LOCATION`` URLs; None when off."""
    if value is False:
        return None
    if value is True:
        value = {}
    elif not isinstance(value, dict):
        msg = f"OPTIONS['tracking'] must be a bool or a dict, not {value!r}."
        raise ImproperlyConfigured(msg)
    if not supported:
        msg = f"{backend} does not support OPTIONS['tracking']."
        raise ImproperlyConfigured(msg)
    unknown = sorted(map(repr, value.keys() - _KEYS))
    if unknown:
        msg = f"Unknown OPTIONS['tracking'] keys: {', '.join(unknown)}."
        raise ImproperlyConfigured(msg)
    _check_driver_options(options, urls)
    return TrackingConfig(
        prefix=_prefix(value, key_prefix=key_prefix, default_key_func=default_key_func),
        max_staleness=_max_staleness(value),
        max_bytes=_max_bytes(value),
    )


def _check_driver_options(options: dict[str, Any], urls: Sequence[str]) -> None:
    # The drivers apply a URL's query parameters over OPTIONS, so both are checked.
    queries = [parse_qs(urlsplit(url).query) for url in urls]
    conflicts = [name for name in _DRIVER_CACHE_OPTIONS if name in options or any(name in q for q in queries)]
    if conflicts:
        msg = f"OPTIONS['tracking'] cannot be combined with the driver's client-side caching: {', '.join(conflicts)}."
        raise ImproperlyConfigured(msg)
    for encoding in [options.get("encoding", "utf-8"), *(value for q in queries for value in q.get("encoding", ()))]:
        _check_encoding(encoding)


def _prefix(value: dict[str, Any], *, key_prefix: str, default_key_func: bool) -> str:
    if "prefix" not in value:
        if not default_key_func:
            msg = "OPTIONS['tracking'] needs a 'prefix' with a custom KEY_FUNCTION."
            raise ImproperlyConfigured(msg)
        return f"{key_prefix}:"
    prefix = value["prefix"]
    if not isinstance(prefix, str) or not prefix:
        msg = f"OPTIONS['tracking']['prefix'] must be a non-empty str, not {prefix!r}."
        raise ImproperlyConfigured(msg)
    return prefix


def _max_staleness(value: dict[str, Any]) -> float:
    seconds = value.get("max_staleness", DEFAULT_MAX_STALENESS)
    if isinstance(seconds, bool) or not isinstance(seconds, int | float) or not math.isfinite(seconds) or seconds <= 0:
        msg = f"OPTIONS['tracking']['max_staleness'] must be a finite number of seconds above 0, not {seconds!r}."
        raise ImproperlyConfigured(msg)
    return float(seconds)


def _max_bytes(value: dict[str, Any]) -> int:
    size = value.get("max_bytes", DEFAULT_MAX_BYTES)
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        msg = f"OPTIONS['tracking']['max_bytes'] must be an int above 0, not {size!r}."
        raise ImproperlyConfigured(msg)
    return size


def _check_encoding(encoding: Any) -> None:
    # The server sends invalidated keys as bytes, which the store decodes as UTF-8.
    # A key the driver encoded otherwise would never match its copy.
    try:
        name = codecs.lookup(encoding).name
    except LookupError, TypeError:
        name = None
    if name != "utf-8":
        msg = f"OPTIONS['tracking'] needs the UTF-8 encoding, not {encoding!r}."
        raise ImproperlyConfigured(msg)
