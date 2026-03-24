"""Cachex LocMemCache — drop-in replacement for Django's LocMemCache.

Extends ``django.core.cache.backends.locmem.LocMemCache`` with:

- Data structure operations (lists, sets, hashes, sorted sets)
- TTL operations (ttl, expire, persist)
- Key scanning and type detection
- Admin support (info, keys, scan)

Usage::

    CACHES = {
        "default": {
            "BACKEND": "django_cachex.cache.LocMemCache",
            "LOCATION": "my-cache",
        },
    }
"""

from __future__ import annotations

import fnmatch
import sys
import time
from typing import TYPE_CHECKING, Any

from django.core.cache.backends.locmem import LocMemCache as DjangoLocMemCache

from django_cachex.cache.mixin import CachexMixin

if TYPE_CHECKING:
    from django_cachex.types import ExpiryT, KeyT


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


class LocMemCache(CachexMixin, DjangoLocMemCache):
    """LocMemCache with cachex extensions.

    Drop-in replacement for ``django.core.cache.backends.locmem.LocMemCache``.
    All standard Django cache operations work identically. Additionally provides
    data structure operations, TTL management, and admin support.
    """

    # =========================================================================
    # Internal accessors
    # =========================================================================

    def _get_internal_cache(self) -> dict[str, Any]:
        """Access the internal cache dictionary of LocMemCache."""
        return getattr(self, "_cache", {})

    def _get_expire_info(self) -> dict[str, float | None]:
        """Access the internal expiry info dictionary."""
        return getattr(self, "_expire_info", {})

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the TTL of a key in seconds.

        Returns:
            -2 if key does not exist, -1 if no expiry, else seconds remaining.
        """
        internal_key = self.make_key(str(key), version=version)
        internal_cache = self._get_internal_cache()
        if internal_key not in internal_cache:
            return -2
        expire_info = self._get_expire_info()
        exp_time = expire_info.get(internal_key)
        if exp_time is None:
            return -1
        remaining = int(exp_time - time.time())
        return max(0, remaining)

    def expire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool:
        """Set the TTL of a key."""
        from datetime import timedelta

        internal_key = self.make_key(str(key), version=version)
        internal_cache = self._get_internal_cache()
        if internal_key not in internal_cache:
            return False
        if isinstance(timeout, timedelta):
            timeout_secs = timeout.total_seconds()
        else:
            timeout_secs = float(timeout)
        expire_info = self._get_expire_info()
        expire_info[internal_key] = time.time() + timeout_secs
        return True

    def persist(self, key: KeyT, version: int | None = None) -> bool:
        """Remove the TTL from a key, making it persist indefinitely."""
        internal_key = self.make_key(str(key), version=version)
        internal_cache = self._get_internal_cache()
        if internal_key not in internal_cache:
            return False
        expire_info = self._get_expire_info()
        expire_info[internal_key] = None
        return True

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List keys matching the pattern by scanning internal cache dict."""
        internal_cache = self._get_internal_cache()
        all_keys = []
        for internal_key in internal_cache:
            parts = internal_key.split(":", 2)
            if len(parts) >= 3:
                user_key = parts[2]
                if self.key_prefix and user_key.startswith(self.key_prefix):
                    user_key = user_key[len(self.key_prefix) :]
                all_keys.append(user_key)
            else:
                all_keys.append(internal_key)
        if pattern and pattern != "*":
            all_keys = [k for k in all_keys if fnmatch.fnmatch(k, pattern)]
        all_keys.sort()
        return all_keys

    # =========================================================================
    # Info
    # =========================================================================

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get LocMemCache info in structured format."""
        internal_cache = self._get_internal_cache()
        expire_info = self._get_expire_info()
        key_count = len(internal_cache)
        current_time = time.time()
        keys_with_expiry = sum(1 for exp in expire_info.values() if exp and exp > current_time)
        try:
            total_size = sum(_deep_getsizeof(v) for v in internal_cache.values())
            total_size += sum(_deep_getsizeof(k) for k in internal_cache)
        except Exception:  # noqa: BLE001
            total_size = 0
        max_entries = getattr(self, "_max_entries", 300)
        cull_frequency = getattr(self, "_cull_frequency", 3)
        return {
            "backend": "LocMemCache",
            "server": {
                "redis_version": "LocMemCache (in-process)",
                "process_id": None,
            },
            "memory": {
                "used_memory": total_size,
                "used_memory_human": _format_bytes(total_size),
                "maxmemory": max_entries,
                "maxmemory_human": f"{max_entries} entries",
                "maxmemory_policy": f"cull 1/{cull_frequency} when full",
            },
            "keyspace": {
                "db0": {
                    "keys": key_count,
                    "expires": keys_with_expiry,
                },
            },
        }
