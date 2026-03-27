"""Shared utility functions for django-cachex."""

from __future__ import annotations

import sys
from typing import Any


def _format_bytes(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    size: float = float(size_bytes)
    for unit in ("B", "K", "M", "G", "T"):
        if abs(size) < 1024:
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size = size / 1024
    return f"{size:.1f}P"


def _deep_getsizeof(obj: Any, seen: set[int] | None = None) -> int:
    """Recursively calculate the deep size of an object in bytes."""
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        size += sum(_deep_getsizeof(k, seen) + _deep_getsizeof(v, seen) for k, v in obj.items())
    elif isinstance(obj, (list, tuple, set, frozenset)):
        size += sum(_deep_getsizeof(item, seen) for item in obj)
    elif hasattr(obj, "__dict__"):
        size += _deep_getsizeof(obj.__dict__, seen)
    return size
