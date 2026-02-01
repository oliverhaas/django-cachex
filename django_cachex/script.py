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

    Provides access to the cache's key prefixing and value encoding
    functions for use in Lua script processing hooks.

    Attributes:
        make_key: Function to apply cache key prefix and version.
        encode: Function to encode a value (serialize + compress).
        decode: Function to decode a value (decompress + deserialize).
        version: The key version to use for prefixing.

    Example:
        Using helpers in a custom pre_func::

            def my_pre(helpers, keys, args):
                # Prefix all keys
                processed_keys = helpers.make_keys(keys)
                # Encode value arguments
                processed_args = helpers.encode_values(args)
                return processed_keys, processed_args
    """

    make_key: Callable[[Any, int | None], Any]
    encode: Callable[[Any], bytes | int]
    decode: Callable[[Any], Any]
    version: int | None

    def make_keys(self, keys: Sequence[Any]) -> list[Any]:
        """Apply key prefixing to multiple keys.

        Args:
            keys: Sequence of cache keys to prefix.

        Returns:
            List of prefixed keys.
        """
        return [self.make_key(k, self.version) for k in keys]

    def encode_values(self, values: Sequence[Any]) -> list[bytes | int]:
        """Encode multiple values for storage.

        Args:
            values: Sequence of values to encode.

        Returns:
            List of encoded values.
        """
        return [self.encode(v) for v in values]

    def decode_values(self, values: Sequence[Any]) -> list[Any]:
        """Decode multiple values from storage.

        Args:
            values: Sequence of encoded values.

        Returns:
            List of decoded values.
        """
        return [self.decode(v) for v in values]


@dataclass
class LuaScript:
    """Registered Lua script with metadata and processing hooks.

    Attributes:
        name: Unique identifier for the script.
        script: The Lua script source code.
        num_keys: Expected number of KEYS arguments (for documentation).
        pre_func: Optional function to process keys/args before execution.
            Signature: (helpers, keys, args) -> (processed_keys, processed_args)
        post_func: Optional function to process result after execution.
            Signature: (helpers, result) -> processed_result

    Example:
        Creating a script with encoding::

            from django_cachex.script import LuaScript, full_encode_pre, decode_single_post

            script = LuaScript(
                name="get_and_set",
                script='''
                    local old = redis.call('GET', KEYS[1])
                    redis.call('SET', KEYS[1], ARGV[1])
                    return old
                ''',
                num_keys=1,
                pre_func=full_encode_pre,
                post_func=decode_single_post,
            )
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
    """Pre-processor that only prefixes keys, leaves args unchanged.

    Use this when your script works with raw values (numbers, strings)
    that don't need encoding.

    Args:
        helpers: Script helper functions.
        keys: Original keys.
        args: Original args.

    Returns:
        Tuple of (prefixed_keys, unchanged_args).

    Example:
        Rate limiting script that works with integers::

            cache.register_script(
                "rate_limit",
                '''
                local current = redis.call('INCR', KEYS[1])
                if current == 1 then
                    redis.call('EXPIRE', KEYS[1], ARGV[1])
                end
                return current
                ''',
                pre_func=keys_only_pre,
            )
    """
    return helpers.make_keys(keys), list(args)


def full_encode_pre(
    helpers: ScriptHelpers,
    keys: Sequence[Any],
    args: Sequence[Any],
) -> tuple[list[Any], list[Any]]:
    """Pre-processor that prefixes keys AND encodes all args.

    Use this when your script stores/retrieves serialized Python objects
    that should be encoded the same way as regular cache values.

    Args:
        helpers: Script helper functions.
        keys: Original keys.
        args: Original args (will be serialized).

    Returns:
        Tuple of (prefixed_keys, encoded_args).

    Example:
        Script that stores serialized objects::

            cache.register_script(
                "store_if_missing",
                '''
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SET', KEYS[1], ARGV[1])
                    return 1
                end
                return 0
                ''',
                pre_func=full_encode_pre,
            )
    """
    return helpers.make_keys(keys), helpers.encode_values(args)


# =============================================================================
# Pre-built post_func helpers
# =============================================================================


def decode_single_post(helpers: ScriptHelpers, result: Any) -> Any:
    """Post-processor for a single encoded value result.

    Returns None if result is None, otherwise decodes the value.

    Args:
        helpers: Script helper functions.
        result: Raw result from script (encoded bytes or None).

    Returns:
        Decoded Python object, or None.

    Example:
        Script that returns a single cached value::

            cache.register_script(
                "get_and_delete",
                '''
                local value = redis.call('GET', KEYS[1])
                redis.call('DEL', KEYS[1])
                return value
                ''',
                pre_func=keys_only_pre,
                post_func=decode_single_post,
            )
    """
    if result is None:
        return None
    return helpers.decode(result)


def decode_list_post(helpers: ScriptHelpers, result: Any) -> list[Any]:
    """Post-processor for a list of encoded values.

    Returns empty list if result is None, otherwise decodes each value.

    Args:
        helpers: Script helper functions.
        result: Raw result from script (list of encoded bytes or None).

    Returns:
        List of decoded Python objects.

    Example:
        Script that returns multiple values::

            cache.register_script(
                "mget_and_delete",
                '''
                local values = redis.call('MGET', unpack(KEYS))
                redis.call('DEL', unpack(KEYS))
                return values
                ''',
                pre_func=keys_only_pre,
                post_func=decode_list_post,
            )
    """
    if result is None:
        return []
    return helpers.decode_values(result)


def decode_list_or_none_post(helpers: ScriptHelpers, result: Any) -> list[Any] | None:
    """Post-processor for an optional list of encoded values.

    Returns None if result is None, otherwise decodes each value.

    Args:
        helpers: Script helper functions.
        result: Raw result from script (list of encoded bytes or None).

    Returns:
        List of decoded Python objects, or None.
    """
    if result is None:
        return None
    return helpers.decode_values(result)


def noop_post(_helpers: ScriptHelpers, result: Any) -> Any:
    """Post-processor that returns result unchanged.

    Use this when the script returns raw values (numbers, strings)
    that don't need decoding.

    Args:
        _helpers: Script helper functions (unused).
        result: Raw result from script.

    Returns:
        Unchanged result.
    """
    return result
