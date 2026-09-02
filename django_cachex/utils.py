"""Helpers shared by the backends that emulate RESP semantics in Python."""

import re
import sys
from functools import lru_cache
from types import FunctionType, ModuleType
from typing import TYPE_CHECKING, Any

from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

# Walking a module's __dict__ reaches the whole import graph: one cached value
# holding `sys` cost ~11 ms vs ~14 us, all under LocMemCache.info()'s lock.
_OPAQUE_TYPES = (type, ModuleType, FunctionType)


def _format_bytes(size_bytes: int) -> str:
    size: float = float(size_bytes)
    for unit in ("B", "K", "M", "G", "T"):
        if abs(size) < 1024:
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size = size / 1024
    return f"{size:.1f}P"


def _deep_getsizeof(obj: Any, seen: set[int] | None = None) -> int:
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    size = sys.getsizeof(obj)
    if isinstance(obj, _OPAQUE_TYPES):
        return size
    if isinstance(obj, dict):
        size += sum(_deep_getsizeof(k, seen) + _deep_getsizeof(v, seen) for k, v in obj.items())
    elif isinstance(obj, (list, tuple, set, frozenset)):
        size += sum(_deep_getsizeof(item, seen) for item in obj)
    elif hasattr(obj, "__dict__"):
        size += _deep_getsizeof(obj.__dict__, seen)
    return size


def _glob_tokens(pattern: str) -> Iterator[tuple[str, str]]:
    """Split a Redis glob into ``(kind, text)`` pairs.

    ``kind`` is ``star`` for ``*``, ``any`` for ``?``, ``class`` for a
    ``[...]`` body or ``literal`` for one plain character. ``\\`` escapes the
    next character anywhere, including inside a class; an unterminated class
    runs to the end of the pattern, as it does in Redis.
    """
    i = 0
    end = len(pattern)
    while i < end:
        char = pattern[i]
        if char == "*":
            yield "star", char
            i += 1
        elif char == "?":
            yield "any", char
            i += 1
        elif char == "[":
            j = i + 2 if pattern[i + 1 : i + 2] == "^" else i + 1
            while j < end and pattern[j] != "]":
                j += 2 if pattern[j] == "\\" else 1
            yield "class", pattern[i + 1 : j]
            i = j + 1
        elif char == "\\" and i + 1 < end:
            yield "literal", pattern[i + 1]
            i += 2
        else:
            yield "literal", char
            i += 1


@lru_cache(maxsize=256)
def _glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Compile a Redis glob to an end-anchored, case-sensitive regex.

    ``fnmatch`` is close but not the same dialect: it spells negation
    ``[!a]``, has no backslash escape, and normcases both sides, which would
    make patterns case-insensitive on Windows only.
    """
    parts = []
    for kind, text in _glob_tokens(pattern):
        if kind != "class":
            parts.append({"star": ".*", "any": "."}.get(kind) or re.escape(text))
            continue
        body = text.removeprefix("^")
        # ``re.escape`` would escape the ``-`` that spells a range, so class
        # members are escaped one at a time and ranges are rebuilt by hand.
        members: list[str] = []
        i = 0
        end = len(body)
        while i < end:
            if body[i] == "\\" and i + 1 < end:
                members.append(re.escape(body[i + 1]))
                i += 2
            elif i + 2 < end and body[i + 1] == "-":
                members.append(f"{re.escape(body[i])}-{re.escape(body[i + 2])}")
                i += 3
            else:
                members.append(re.escape(body[i]))
                i += 1
        negated = "^" if text.startswith("^") else ""
        if members:
            parts.append(f"[{negated}{''.join(members)}]")
        else:
            # Redis matches nothing for ``[]`` and any one character for ``[^]``.
            parts.append("." if negated else "(?!)")
    return re.compile("".join(parts) + r"\Z", re.DOTALL)


def _glob_to_like(pattern: str, escape: Callable[[str], str]) -> tuple[str, bool]:
    """Translate a Redis glob to a SQL ``LIKE`` pattern for ``DatabaseCache``.

    Returns the pattern and whether it is exact. ``LIKE`` has no character
    classes, so a ``[...]`` widens to ``_`` and the caller has to re-filter
    the rows with :func:`_glob_to_regex`. ``escape`` is the connection's
    ``prep_for_like_query``, applied per literal character so that the
    pattern's own metacharacters survive the escaping.
    """
    out: list[str] = []
    exact = True
    for kind, text in _glob_tokens(pattern):
        if kind == "star":
            out.append("%")
        elif kind == "any":
            out.append("_")
        elif kind == "class":
            out.append("_")
            exact = False
        else:
            out.append(escape(text))
    return "".join(out), exact


def _as_score(value: Any) -> float:
    """Coerce a sorted-set score the way Redis parses one."""
    try:
        return float(value)
    except TypeError, ValueError:
        msg = "value is not a valid float"
        raise ValueError(msg) from None


def _score_bound(value: float | str, backend: str) -> float:
    """Parse a ``ZRANGEBYSCORE``-style score bound.

    ``-inf``/``+inf`` parse natively. Redis's exclusive ``(`` prefix has no
    equivalent in the inclusive comparison the native backends do in Python,
    so it is rejected rather than blowing up in ``float()``.
    """
    if isinstance(value, str) and value.lstrip().startswith("("):
        raise NotSupportedError("exclusive score bounds ('(' prefix)", backend)
    return float(value)


def _validate_pop_count(count: int | None) -> None:
    """Reject a negative pop count before it slices from the wrong end.

    A negative ``count`` turns the ``[:count]`` / ``[-count:]`` slices the
    native pops use into "everything but the last N"; Redis rejects it
    outright, for lists and sorted sets alike.
    """
    if count is not None and count < 0:
        msg = "value is out of range, must be positive"
        raise ValueError(msg)


def _validate_lpos_rank(rank: int | None) -> None:
    if rank == 0:
        msg = (
            "RANK can't be zero. Use 1 to start searching from the first matching element "
            "in the head of the list or a negative rank to start searching from the tail."
        )
        raise ValueError(msg)


def _lpos_positions(items: Sequence[Any], value: Any, rank: int | None, maxlen: int | None) -> list[int]:
    """Indexes of ``value`` in ``items``, in the order Redis ``LPOS`` scans them.

    A negative rank scans from the tail toward the head, so ``maxlen``, which
    caps the number of comparisons rather than the number of matches, applies
    to the last ``maxlen`` elements instead of the first.
    """
    if rank is not None and rank < 0:
        start = max(0, len(items) - maxlen) if maxlen else 0
        found = [start + i for i, v in enumerate(items[start:]) if v == value]
        found.reverse()
        return found[-rank - 1 :]
    window = items[:maxlen] if maxlen else items
    found = [i for i, v in enumerate(window) if v == value]
    return found[rank - 1 :] if rank is not None else found
