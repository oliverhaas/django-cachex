"""Lua script support: pre/post hooks for key prefixing and value coding."""

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


@dataclass(frozen=True, slots=True)
class Encoded:
    """Marks an ARGV entry that ``encoded_pre`` must pass through ``encode()``.

    Lua scripts mix two kinds of ARGV: values that a later ``get()`` has
    to read back, and scalars that Lua itself consumes with ``tonumber``
    or a string compare. Wrap only the former.
    """

    value: Any

    def __post_init__(self) -> None:
        if isinstance(self.value, Encoded):
            msg = "Encoded(Encoded(...)) is nested; wrap the value once"
            raise TypeError(msg)


def reject_stray_encoded(keys: Sequence[Any], args: Sequence[Any]) -> None:
    """Raise if an ``Encoded`` is about to reach the adapter unwrapped.

    ``eval_script`` calls this on the raw keys and on the args *after* the
    pre-hook, so a custom hook that unwraps ``Encoded`` itself passes.
    """
    if any(isinstance(k, Encoded) for k in keys):
        msg = "Encoded() is not valid in keys; keys are prefixed, never encoded"
        raise TypeError(msg)
    if any(isinstance(a, Encoded) for a in args):
        msg = "Encoded() in args needs pre_hook=encoded_pre (or a custom hook that unwraps it)"
        raise TypeError(msg)


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


def encoded_pre(
    helpers: ScriptHelpers,
    keys: Sequence[Any],
    args: Sequence[Any],
) -> tuple[list[Any], list[Any]]:
    """Prefix keys and encode only the args wrapped in :class:`Encoded`."""
    return helpers.make_keys(keys), [helpers.encode(a.value) if isinstance(a, Encoded) else a for a in args]


def decode_single_post(helpers: ScriptHelpers, result: Any) -> Any:
    if result is None:
        return None
    return helpers.decode(result)


def decode_list_post(helpers: ScriptHelpers, result: Any) -> list[Any]:
    if result is None:
        return []
    return helpers.decode_values(result)
