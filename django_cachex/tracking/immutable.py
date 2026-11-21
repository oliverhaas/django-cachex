"""Which decoded values no caller can change, so that every read of a copy can return the same object."""

import builtins
import dataclasses
import datetime as dt
import functools
import sys
from decimal import Decimal
from enum import Enum
from fractions import Fraction
from types import NoneType
from typing import Any, Final
from uuid import UUID

# How the parts of an immutable value are reached.
_LEAF: Final = 0
_ITEMS: Final = 1
_MAPPING: Final = 2
_FIELDS: Final = 3

_KINDS: Final[dict[type, int]] = {
    **dict.fromkeys(
        (
            str,
            bytes,
            int,
            float,
            complex,
            bool,
            NoneType,
            Decimal,
            Fraction,
            dt.date,
            dt.datetime,
            dt.time,
            dt.timedelta,
            UUID,
            range,
        ),
        _LEAF,
    ),
    tuple: _ITEMS,
    frozenset: _ITEMS,
}
# A builtin from Python 3.15.
if (_frozendict := getattr(builtins, "frozendict", None)) is not None:
    _KINDS[_frozendict] = _MAPPING

_LEAVES: Final = frozenset(cls for cls, kind in _KINDS.items() if kind == _LEAF)
_UNSET: Final = object()


def _adds_no_state(cls: type) -> bool:
    """Return whether ``cls`` gives its instances no attributes: its ``__slots__`` hold at most ``__weakref__``."""
    slots = cls.__dict__.get("__slots__")
    if slots is None:
        return False
    names = (slots,) if isinstance(slots, str) else slots
    return all(name == "__weakref__" for name in names)


@functools.lru_cache(maxsize=1024)
def _kind(cls: type) -> int | None:
    """How the parts of an instance of ``cls`` are reached; None if an instance can change."""
    if issubclass(cls, Enum):
        # Unpickling returns the member itself, so every decode returns this object anyway.
        return _LEAF
    kind = None
    for base in cls.__mro__:
        found = _KINDS.get(base)
        params = base.__dict__.get("__dataclass_params__")
        if found is None and params is not None and params.frozen:
            found = _FIELDS
        if found is not None:
            kind = found if kind is None else kind
        elif base is not object and not _adds_no_state(base):
            return None
    return kind


def immutable_size(value: Any, limit: int) -> int | None:
    """Return the bytes ``value`` and its parts take; None if a caller can change any of them, or above ``limit``.

    A part reached twice counts twice. ``limit`` also ends a cycle, which only object.__setattr__() can close.
    """
    kinds = _KINDS
    leaves = _LEAVES
    getsizeof = sys.getsizeof
    size = 0
    pending = [value]
    append = pending.append
    while pending:
        value = pending.pop()
        cls = value.__class__
        kind = kinds.get(cls)
        if kind is None and (kind := _kind(cls)) is None:
            return None
        size += getsizeof(value)
        if kind == _ITEMS:
            parts = value
        elif kind == _MAPPING:
            parts = (*value.keys(), *value.values())
        elif kind == _FIELDS:
            parts = _attributes(value)
            if state := getattr(value, "__dict__", None):
                size += getsizeof(state)
        else:
            continue
        # Most parts are str, int and the like, which are counted here rather than through ``pending``.
        for part in parts:
            if part.__class__ in leaves:
                size += getsizeof(part)
            else:
                append(part)
        if size > limit:
            return None
    return size


def _attributes(value: Any) -> list[Any]:
    """The field values of a dataclass instance, and the attributes set on it with object.__setattr__()."""
    fields = dataclasses.fields(value)
    found = [item for field in fields if (item := getattr(value, field.name, _UNSET)) is not _UNSET]
    if state := getattr(value, "__dict__", None):
        # Pickle restores these too.
        names = {field.name for field in fields}
        found.extend(item for name, item in state.items() if name not in names)
    return found
