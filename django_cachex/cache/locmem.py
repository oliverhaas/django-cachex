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
import threading
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from django.core.cache.backends.locmem import LocMemCache as DjangoLocMemCache

from django_cachex.cache.mixin import CachexCompat
from django_cachex.utils import _deep_getsizeof, _format_bytes

if TYPE_CHECKING:
    import contextlib

    from django_cachex.types import ExpiryT, KeyT


# Reentrant per-backend-name locks for compound (read-modify-write) ops.
# Django's stock `LocMemCache._lock` is a non-reentrant `threading.Lock`, so
# we can't reuse it — wrapping a compound op in `with self._lock:` would
# deadlock the moment the inner `self.get`/`self.set` re-acquired it.
# Sharing by `name` mirrors Django's `_locks` dict so multiple LocMemCache
# instances against the same backing store agree on a single envelope lock.
_compound_locks: dict[str, threading.RLock] = {}


class LocMemCache(CachexCompat, DjangoLocMemCache):
    """LocMemCache with cachex extensions.

    Drop-in replacement for ``django.core.cache.backends.locmem.LocMemCache``.
    All standard Django cache operations work identically. Additionally provides
    data structure operations, TTL management, and admin support.
    """

    _cachex_support: str = "cachex"

    # Type-only declarations for attributes Django's BaseCache + LocMemCache
    # set in __init__ but don't surface in stubs.
    if TYPE_CHECKING:
        _cache: dict[str, Any]
        _expire_info: dict[str, float | None]
        _max_entries: int
        _cull_frequency: int

    def __init__(self, name: str, params: dict[str, Any]) -> None:
        super().__init__(name, params)
        # Per-name reentrant lock so concurrent compound ops serialize.
        # Separate from Django's `self._lock` (non-reentrant) — see the
        # module docstring on `_compound_locks`.
        self._compound_lock = _compound_locks.setdefault(name, threading.RLock())

    def _compound_op_lock(self) -> contextlib.AbstractContextManager[Any]:
        """Reentrant per-backend lock around compound (read-modify-write) ops.

        Serializes concurrent ``lpush``/``sadd``/``hincrby``/etc. on this
        backend so they don't lose updates. Reentrant so a compound op can
        nest into another (rare, but the surface allows it).
        """
        return self._compound_lock

    # =========================================================================
    # Internal accessors
    # =========================================================================

    def _get_internal_cache(self) -> dict[str, Any]:
        """Access the internal cache dictionary of LocMemCache."""
        return self._cache

    def _get_expire_info(self) -> dict[str, float | None]:
        """Access the internal expiry info dictionary."""
        return self._expire_info

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
        """List keys matching the pattern by scanning internal cache dict.

        Assumes Django's default key format: ``KEY_PREFIX:VERSION:key``.
        Custom ``KEY_FUNCTION`` settings may produce different formats.
        """
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
        max_entries = self._max_entries
        cull_frequency = self._cull_frequency
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
