"""Freezing the results of ``cached_data()`` functions, which every caller in the process shares."""

import dataclasses
import datetime
from decimal import Decimal
from types import MappingProxyType, NoneType
from typing import Any
from uuid import UUID

from django.db.models import Model
from django.db.models.manager import BaseManager
from django.db.models.query import QuerySet

# Immutable, and the most common values: checked by their class alone.
_LEAVES = frozenset(
    {
        str,
        int,
        float,
        bool,
        NoneType,
        bytes,
        Decimal,
        datetime.date,
        datetime.datetime,
        datetime.time,
        datetime.timedelta,
        UUID,
    },
)


# Containers callers could change, and what to return instead.
_MUTABLE = (
    (list, "a list; use a tuple"),
    (dict, "a dict; only a returned dict becomes read-only, wrap this one in types.MappingProxyType"),
    (set, "a set; use a frozenset"),
    (bytearray, "a bytearray; use bytes"),
)


def _mutable(value: Any) -> str | None:
    """Return what ``value`` is and what to use instead if callers could change it, else None."""
    for cls, problem in _MUTABLE:
        if isinstance(value, cls):
            return problem
    name = type(value).__name__
    if isinstance(value, Model):
        return f"a {name} instance; return its field values, such as from values_list()"
    if isinstance(value, QuerySet | BaseManager):
        return f"a {name}; return a tuple of its field values, such as from values_list()"
    if hasattr(type(value), "__next__"):
        return "an iterator; use a tuple"
    if dataclasses.is_dataclass(value) and not isinstance(value, type) and not value.__dataclass_params__.frozen:
        return f"a {name}, a dataclass that is not frozen; use @dataclass(frozen=True)"
    return None


def freeze(result: Any, name: str) -> Any:
    """Return ``result`` with a returned dict made read-only; raise TypeError if callers could change it.

    Tuples, frozensets, read-only mappings and frozen dataclasses are checked through; other objects
    pass unchecked.
    """
    if isinstance(result, dict):
        result = MappingProxyType(dict(result))
    pending = [(result, f"{name}()")]
    while pending:
        value, path = pending.pop()
        cls = value.__class__
        if cls in _LEAVES:
            continue
        if isinstance(value, tuple):
            fields = getattr(cls, "_fields", None)
            if fields is None:
                pending.extend((item, f"{path}[{index}]") for index, item in enumerate(value))
            else:
                pending.extend((item, f"{path}.{field}") for field, item in zip(fields, value, strict=False))
        elif isinstance(value, frozenset):
            pending.extend((item, f"{path}{{...}}") for item in value)
        elif isinstance(value, MappingProxyType):
            pending.extend((item, f"{path}[{key!r}]") for key, item in value.items())
            pending.extend((key, f"{path} key {key!r}") for key in value)
        elif (problem := _mutable(value)) is not None:
            msg = f"{path} is {problem}. Callers share the results of @cached_data functions."
            raise TypeError(msg)
        elif dataclasses.is_dataclass(value) and not isinstance(value, type):
            pending.extend((getattr(value, field.name), f"{path}.{field.name}") for field in dataclasses.fields(value))
    return result
