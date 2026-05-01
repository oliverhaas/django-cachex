"""Lua script support: pre/post hooks for key prefixing and value coding."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


@dataclass
class ScriptHelpers:
    """Helpers passed to pre/post hooks for use in custom Lua wrappers."""

    make_key: Callable[[Any, int | None], Any]
    encode: Callable[[Any], bytes | int]
    decode: Callable[[Any], Any]
    version: int | None

    def make_keys(self, keys: Sequence[Any]) -> list[Any]:
        return [self.make_key(k, self.version) for k in keys]

    def encode_values(self, values: Sequence[Any]) -> list[bytes | int]:
        return [self.encode(v) for v in values]

    def decode_values(self, values: Sequence[Any]) -> list[Any]:
        return [self.decode(v) for v in values]


def keys_only_pre(
    helpers: ScriptHelpers,
    keys: Sequence[Any],
    args: Sequence[Any],
) -> tuple[list[Any], list[Any]]:
    return helpers.make_keys(keys), list(args)


def full_encode_pre(
    helpers: ScriptHelpers,
    keys: Sequence[Any],
    args: Sequence[Any],
) -> tuple[list[Any], list[Any]]:
    return helpers.make_keys(keys), helpers.encode_values(args)


def decode_single_post(helpers: ScriptHelpers, result: Any) -> Any:
    if result is None:
        return None
    return helpers.decode(result)


def decode_list_post(helpers: ScriptHelpers, result: Any) -> list[Any]:
    if result is None:
        return []
    return helpers.decode_values(result)


def decode_list_or_none_post(helpers: ScriptHelpers, result: Any) -> list[Any] | None:
    if result is None:
        return None
    return helpers.decode_values(result)


def noop_post(_helpers: ScriptHelpers, result: Any) -> Any:
    return result
