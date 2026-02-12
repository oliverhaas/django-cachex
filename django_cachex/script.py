"""Lua script support for django-cachex.

This module provides a high-level interface for registering and executing
Lua scripts with automatic key prefixing and value encoding/decoding.

Example:
    Register and execute a rate limiting script::

        from django.core.cache import cache
        from django_cachex.script import keys_only_pre

        cache.register_script(
            "rate_limit",
            '''
            local current = redis.call('INCR', KEYS[1])
            if current == 1 then
                redis.call('EXPIRE', KEYS[1], ARGV[1])
            end
            return current
            ''',
            num_keys=1,
            pre_func=keys_only_pre,
        )

        count = cache.eval_script("rate_limit", keys=["user:123:req"], args=[60])
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


@dataclass
class ScriptHelpers:
    """Helper functions passed to pre/post processing hooks.

    Provides access to the cache's key prefixing and value encoding/decoding
    for use in Lua script processing hooks.
    """

    make_key: Callable[[Any, int | None], Any]
    encode: Callable[[Any], bytes | int]
    decode: Callable[[Any], Any]
    version: int | None

    def make_keys(self, keys: Sequence[Any]) -> list[Any]:
        """Apply key prefixing to multiple keys."""
        return [self.make_key(k, self.version) for k in keys]

    def encode_values(self, values: Sequence[Any]) -> list[bytes | int]:
        """Encode multiple values for storage."""
        return [self.encode(v) for v in values]

    def decode_values(self, values: Sequence[Any]) -> list[Any]:
        """Decode multiple values from storage."""
        return [self.decode(v) for v in values]


@dataclass
class LuaScript:
    """Registered Lua script with metadata and processing hooks.

    Attributes:
        pre_func: (helpers, keys, args) -> (processed_keys, processed_args)
        post_func: (helpers, result) -> processed_result
    """

    name: str
    script: str
    num_keys: int | None = None
    pre_func: Callable[[ScriptHelpers, Sequence[Any], Sequence[Any]], tuple[list[Any], list[Any]]] | None = None
    post_func: Callable[[ScriptHelpers, Any], Any] | None = None

    # Cached SHA hash (populated on first execution)
    _sha: str | None = field(default=None, repr=False, compare=False)


# =============================================================================
# Pre-built pre_func helpers
# =============================================================================


def keys_only_pre(
    helpers: ScriptHelpers,
    keys: Sequence[Any],
    args: Sequence[Any],
) -> tuple[list[Any], list[Any]]:
    """Pre-processor that prefixes keys, leaves args unchanged."""
    return helpers.make_keys(keys), list(args)


def full_encode_pre(
    helpers: ScriptHelpers,
    keys: Sequence[Any],
    args: Sequence[Any],
) -> tuple[list[Any], list[Any]]:
    """Pre-processor that prefixes keys and encodes all args."""
    return helpers.make_keys(keys), helpers.encode_values(args)


# =============================================================================
# Pre-built post_func helpers
# =============================================================================


def decode_single_post(helpers: ScriptHelpers, result: Any) -> Any:
    """Post-processor that decodes a single value result. Returns None if result is None."""
    if result is None:
        return None
    return helpers.decode(result)


def decode_list_post(helpers: ScriptHelpers, result: Any) -> list[Any]:
    """Post-processor that decodes a list of values. Returns [] if result is None."""
    if result is None:
        return []
    return helpers.decode_values(result)


def decode_list_or_none_post(helpers: ScriptHelpers, result: Any) -> list[Any] | None:
    """Post-processor that decodes a list of values. Returns None if result is None."""
    if result is None:
        return None
    return helpers.decode_values(result)


def noop_post(_helpers: ScriptHelpers, result: Any) -> Any:
    """Post-processor that returns result unchanged."""
    return result
