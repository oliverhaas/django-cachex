"""
Cache wrappers that extend Django builtin caches with cachex functionality.

These mixins add extended operations (keys, ttl, info, etc.) to Django's
cache backends via class patching. Operations that the underlying cache
doesn't support raise NotSupportedError.
"""

from __future__ import annotations

import contextlib
import fnmatch
import time
from typing import TYPE_CHECKING, Any, Literal, cast

from django.core.cache.backends.db import DatabaseCache
from django.core.cache.backends.dummy import DummyCache
from django.core.cache.backends.filebased import FileBasedCache
from django.core.cache.backends.locmem import LocMemCache
from django.db import connection

from django_cachex.exceptions import NotSupportedError

# Try to import optional backends
try:
    from django.core.cache.backends.memcached import PyLibMCCache
except ImportError:
    PyLibMCCache = None  # type: ignore[misc, assignment]

try:
    from django.core.cache.backends.memcached import PyMemcacheCache
except ImportError:
    PyMemcacheCache = None  # type: ignore[misc, assignment]

try:
    from django.core.cache.backends.redis import RedisCache as DjangoRedisCache
except ImportError:
    DjangoRedisCache = None  # type: ignore[misc, assignment]

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence
    from pathlib import Path

    from django.core.cache.backends.base import BaseCache

    from django_cachex.client.pipeline import Pipeline
    from django_cachex.script import LuaScript, ScriptHelpers
    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT

# Alias to avoid shadowing by method names
_set = set


# =============================================================================
# Base Extensions Mixin
# =============================================================================


class BaseCacheExtensions:
    """Mixin providing CacheProtocol extension methods with NotSupportedError defaults.

    This mixin adds all the extended cache operations that aren't part of
    Django's BaseCache. Methods raise NotSupportedError by default;
    Extended* classes override the ones they can actually implement.
    """

    # Marker to detect already-extended caches and support level
    # "cachex" = native django-cachex backends (full support)
    # "wrapped" = Django builtin backends (almost full support)
    # "limited" = Unknown backends (limited support, may not work correctly)
    _cachex_support: Literal["cachex", "wrapped", "limited"] = "limited"

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the TTL of a key in seconds."""
        raise NotSupportedError("ttl", self.__class__.__name__)

    def pttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the TTL of a key in milliseconds."""
        raise NotSupportedError("pttl", self.__class__.__name__)

    def type(self, key: KeyT, version: int | None = None) -> str | None:
        """Get the data type of a key."""
        raise NotSupportedError("type", self.__class__.__name__)

    def persist(self, key: KeyT, version: int | None = None) -> bool:
        """Remove the TTL from a key."""
        raise NotSupportedError("persist", self.__class__.__name__)

    def expire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool:
        """Set expiry time on a key."""
        raise NotSupportedError("expire", self.__class__.__name__)

    def expire_at(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool:
        """Set expiry to an absolute time."""
        raise NotSupportedError("expire_at", self.__class__.__name__)

    def pexpire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool:
        """Set expiry time in milliseconds."""
        raise NotSupportedError("pexpire", self.__class__.__name__)

    def pexpire_at(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool:
        """Set expiry to an absolute time in milliseconds."""
        raise NotSupportedError("pexpire_at", self.__class__.__name__)

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List keys matching the pattern."""
        raise NotSupportedError("keys", self.__class__.__name__)

    def iter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]:
        """Iterate over keys matching pattern."""
        raise NotSupportedError("iter_keys", self.__class__.__name__)

    def scan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, list[str]]:
        """Perform a single SCAN iteration.

        Simulates cursor-based pagination using keys(). The cursor is treated
        as an offset into the sorted list of keys.

        Returns:
            Tuple of (next_cursor, keys) where next_cursor is 0 if complete.
        """
        # Get all keys (may raise NotSupportedError if keys() not implemented)
        all_keys = self.keys(pattern, version=version)  # type: ignore[attr-defined]

        # Sort for consistent ordering
        if hasattr(all_keys, "sort"):
            all_keys.sort()
        else:
            all_keys = sorted(all_keys)

        # Paginate
        count = count or 100
        start_idx = cursor
        end_idx = start_idx + count
        paginated_keys = all_keys[start_idx:end_idx]

        # Next cursor is 0 if we've reached the end
        next_cursor = end_idx if end_idx < len(all_keys) else 0

        return (next_cursor, paginated_keys)

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        """Delete all keys matching pattern."""
        raise NotSupportedError("delete_pattern", self.__class__.__name__)

    def rename(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key atomically."""
        raise NotSupportedError("rename", self.__class__.__name__)

    def renamenx(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key only if the destination does not exist."""
        raise NotSupportedError("renamenx", self.__class__.__name__)

    def make_pattern(self, pattern: str, version: int | None = None) -> str:
        """Build a pattern for key matching."""
        raise NotSupportedError("make_pattern", self.__class__.__name__)

    def reverse_key(self, key: str) -> str:
        """Reverse a made key back to original."""
        raise NotSupportedError("reverse_key", self.__class__.__name__)

    # =========================================================================
    # Lock & Pipeline
    # =========================================================================

    def lock(
        self,
        key: str,
        version: int | None = None,
        timeout: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        """Return a Lock object for distributed locking."""
        raise NotSupportedError("lock", self.__class__.__name__)

    def pipeline(self, *, transaction: bool = True, version: int | None = None) -> Pipeline:
        """Create a pipeline for batched operations."""
        raise NotSupportedError("pipeline", self.__class__.__name__)

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(self, key: KeyT, field: str, value: Any, version: int | None = None) -> int:
        """Set field in hash."""
        raise NotSupportedError("hset", self.__class__.__name__)

    def hdel(self, key: KeyT, *fields: str, version: int | None = None) -> int:
        """Delete hash fields."""
        raise NotSupportedError("hdel", self.__class__.__name__)

    def hlen(self, key: KeyT, version: int | None = None) -> int:
        """Get number of fields in hash."""
        raise NotSupportedError("hlen", self.__class__.__name__)

    def hkeys(self, key: KeyT, version: int | None = None) -> list[str]:
        """Get all field names in hash."""
        raise NotSupportedError("hkeys", self.__class__.__name__)

    def hexists(self, key: KeyT, field: str, version: int | None = None) -> bool:
        """Check if field exists in hash."""
        raise NotSupportedError("hexists", self.__class__.__name__)

    def hget(self, key: KeyT, field: str, version: int | None = None) -> Any:
        """Get value of field in hash."""
        raise NotSupportedError("hget", self.__class__.__name__)

    def hgetall(self, key: KeyT, version: int | None = None) -> dict[str, Any]:
        """Get all fields and values in hash."""
        raise NotSupportedError("hgetall", self.__class__.__name__)

    def hmget(self, key: KeyT, *fields: str, version: int | None = None) -> list[Any]:
        """Get values of multiple fields."""
        raise NotSupportedError("hmget", self.__class__.__name__)

    def hmset(self, key: KeyT, mapping: Mapping[str, Any], version: int | None = None) -> bool:
        """Set multiple hash fields."""
        raise NotSupportedError("hmset", self.__class__.__name__)

    def hincrby(self, key: KeyT, field: str, amount: int = 1, version: int | None = None) -> int:
        """Increment value of field in hash."""
        raise NotSupportedError("hincrby", self.__class__.__name__)

    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0, version: int | None = None) -> float:
        """Increment float value of field in hash."""
        raise NotSupportedError("hincrbyfloat", self.__class__.__name__)

    def hsetnx(self, key: KeyT, field: str, value: Any, version: int | None = None) -> bool:
        """Set field in hash only if it doesn't exist."""
        raise NotSupportedError("hsetnx", self.__class__.__name__)

    def hvals(self, key: KeyT, version: int | None = None) -> list[Any]:
        """Get all values in hash."""
        raise NotSupportedError("hvals", self.__class__.__name__)

    # =========================================================================
    # List Operations
    # =========================================================================

    def lpush(self, key: KeyT, *values: Any, version: int | None = None) -> int:
        """Push values onto head of list."""
        raise NotSupportedError("lpush", self.__class__.__name__)

    def rpush(self, key: KeyT, *values: Any, version: int | None = None) -> int:
        """Push values onto tail of list."""
        raise NotSupportedError("rpush", self.__class__.__name__)

    def lpop(self, key: KeyT, count: int | None = None, version: int | None = None) -> list[Any]:
        """Remove and return element(s) from head of list."""
        raise NotSupportedError("lpop", self.__class__.__name__)

    def rpop(self, key: KeyT, count: int | None = None, version: int | None = None) -> list[Any]:
        """Remove and return element(s) from tail of list."""
        raise NotSupportedError("rpop", self.__class__.__name__)

    def lrange(self, key: KeyT, start: int, end: int, version: int | None = None) -> list[Any]:
        """Get a range of elements from list."""
        raise NotSupportedError("lrange", self.__class__.__name__)

    def lindex(self, key: KeyT, index: int, version: int | None = None) -> Any:
        """Get element at index in list."""
        raise NotSupportedError("lindex", self.__class__.__name__)

    def llen(self, key: KeyT, version: int | None = None) -> int:
        """Get length of list."""
        raise NotSupportedError("llen", self.__class__.__name__)

    def lpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list."""
        raise NotSupportedError("lpos", self.__class__.__name__)

    def lmove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
        version: int | None = None,
    ) -> Any | None:
        """Atomically move an element from one list to another."""
        raise NotSupportedError("lmove", self.__class__.__name__)

    def lrem(self, key: KeyT, count: int, value: Any, version: int | None = None) -> int:
        """Remove elements from a list."""
        raise NotSupportedError("lrem", self.__class__.__name__)

    def ltrim(self, key: KeyT, start: int, end: int, version: int | None = None) -> bool:
        """Trim list to specified range."""
        raise NotSupportedError("ltrim", self.__class__.__name__)

    def lset(self, key: KeyT, index: int, value: Any, version: int | None = None) -> bool:
        """Set element at index in list."""
        raise NotSupportedError("lset", self.__class__.__name__)

    def linsert(self, key: KeyT, where: str, pivot: Any, value: Any, version: int | None = None) -> int:
        """Insert value before or after pivot in list."""
        raise NotSupportedError("linsert", self.__class__.__name__)

    def blpop(
        self,
        *keys: KeyT,
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list."""
        raise NotSupportedError("blpop", self.__class__.__name__)

    def brpop(
        self,
        *keys: KeyT,
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list."""
        raise NotSupportedError("brpop", self.__class__.__name__)

    def blmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
        version: int | None = None,
    ) -> Any | None:
        """Blocking atomically move element from one list to another."""
        raise NotSupportedError("blmove", self.__class__.__name__)

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(self, key: KeyT, *members: Any, version: int | None = None) -> int:
        """Add members to a set."""
        raise NotSupportedError("sadd", self.__class__.__name__)

    def scard(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of members in a set."""
        raise NotSupportedError("scard", self.__class__.__name__)

    def sdiff(self, *keys: KeyT, version: int | None = None) -> _set[Any]:
        """Return the difference between sets."""
        raise NotSupportedError("sdiff", self.__class__.__name__)

    def sdiffstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the difference of sets."""
        raise NotSupportedError("sdiffstore", self.__class__.__name__)

    def sinter(self, *keys: KeyT, version: int | None = None) -> _set[Any]:
        """Return the intersection of sets."""
        raise NotSupportedError("sinter", self.__class__.__name__)

    def sinterstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the intersection of sets."""
        raise NotSupportedError("sinterstore", self.__class__.__name__)

    def sismember(self, key: KeyT, member: Any, version: int | None = None) -> bool:
        """Check if member is in set."""
        raise NotSupportedError("sismember", self.__class__.__name__)

    def smembers(self, key: KeyT, version: int | None = None) -> _set[Any]:
        """Get all members of a set."""
        raise NotSupportedError("smembers", self.__class__.__name__)

    def smove(self, src: KeyT, dst: KeyT, member: Any, version: int | None = None) -> bool:
        """Move member from one set to another."""
        raise NotSupportedError("smove", self.__class__.__name__)

    def spop(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | _set[Any]:
        """Remove and return random member(s) from set."""
        raise NotSupportedError("spop", self.__class__.__name__)

    def srandmember(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | list[Any]:
        """Get random member(s) from set without removing."""
        raise NotSupportedError("srandmember", self.__class__.__name__)

    def srem(self, key: KeyT, *members: Any, version: int | None = None) -> int:
        """Remove members from a set."""
        raise NotSupportedError("srem", self.__class__.__name__)

    def sunion(self, *keys: KeyT, version: int | None = None) -> _set[Any]:
        """Return the union of sets."""
        raise NotSupportedError("sunion", self.__class__.__name__)

    def sunionstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the union of sets."""
        raise NotSupportedError("sunionstore", self.__class__.__name__)

    def smismember(self, key: KeyT, *members: Any, version: int | None = None) -> list[bool]:
        """Check if multiple values are members of a set."""
        raise NotSupportedError("smismember", self.__class__.__name__)

    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, _set[Any]]:
        """Incrementally iterate over set members."""
        raise NotSupportedError("sscan", self.__class__.__name__)

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> Iterator[Any]:
        """Iterate over set members."""
        raise NotSupportedError("sscan_iter", self.__class__.__name__)

    # =========================================================================
    # Sorted Set Operations
    # =========================================================================

    def zadd(
        self,
        key: KeyT,
        mapping: Mapping[Any, float],
        *,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        gt: bool = False,
        lt: bool = False,
        version: int | None = None,
    ) -> int:
        """Add members to a sorted set."""
        raise NotSupportedError("zadd", self.__class__.__name__)

    def zcard(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of members in a sorted set."""
        raise NotSupportedError("zcard", self.__class__.__name__)

    def zcount(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Count members with scores between min and max."""
        raise NotSupportedError("zcount", self.__class__.__name__)

    def zincrby(self, key: KeyT, amount: float, member: Any, version: int | None = None) -> float:
        """Increment the score of a member."""
        raise NotSupportedError("zincrby", self.__class__.__name__)

    def zpopmax(self, key: KeyT, count: int = 1, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        raise NotSupportedError("zpopmax", self.__class__.__name__)

    def zpopmin(self, key: KeyT, count: int = 1, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        raise NotSupportedError("zpopmin", self.__class__.__name__)

    def zrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index."""
        raise NotSupportedError("zrange", self.__class__.__name__)

    def zrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        *,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return members with scores between min and max."""
        raise NotSupportedError("zrangebyscore", self.__class__.__name__)

    def zrank(self, key: KeyT, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member."""
        raise NotSupportedError("zrank", self.__class__.__name__)

    def zrem(self, key: KeyT, *members: Any, version: int | None = None) -> int:
        """Remove members from a sorted set."""
        raise NotSupportedError("zrem", self.__class__.__name__)

    def zremrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Remove members with scores between min and max."""
        raise NotSupportedError("zremrangebyscore", self.__class__.__name__)

    def zremrangebyrank(self, key: KeyT, start: int, end: int, version: int | None = None) -> int:
        """Remove members by rank range."""
        raise NotSupportedError("zremrangebyrank", self.__class__.__name__)

    def zrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index, highest to lowest."""
        raise NotSupportedError("zrevrange", self.__class__.__name__)

    def zrevrangebyscore(
        self,
        key: KeyT,
        max_score: float | str,
        min_score: float | str,
        *,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return members with scores between max and min, highest first."""
        raise NotSupportedError("zrevrangebyscore", self.__class__.__name__)

    def zscore(self, key: KeyT, member: Any, version: int | None = None) -> float | None:
        """Get the score of a member."""
        raise NotSupportedError("zscore", self.__class__.__name__)

    def zrevrank(self, key: KeyT, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member (highest score first)."""
        raise NotSupportedError("zrevrank", self.__class__.__name__)

    def zmscore(self, key: KeyT, *members: Any, version: int | None = None) -> list[float | None]:
        """Get the scores of multiple members."""
        raise NotSupportedError("zmscore", self.__class__.__name__)

    # =========================================================================
    # Client Access & Info
    # =========================================================================

    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the underlying client."""
        raise NotSupportedError("get_client", self.__class__.__name__)

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get cache server information."""
        return {}

    def slowlog_get(self, count: int = 10) -> list[Any]:
        """Get slow query log entries."""
        return []

    def slowlog_len(self) -> int:
        """Get the number of entries in the slow query log."""
        return 0

    # =========================================================================
    # Lua Script Operations
    # =========================================================================

    def register_script(
        self,
        name: str,
        script: str,
        *,
        num_keys: int | None = None,
        pre_func: Callable[[ScriptHelpers, Sequence[Any], Sequence[Any]], tuple[list[Any], list[Any]]] | None = None,
        post_func: Callable[[ScriptHelpers, Any], Any] | None = None,
    ) -> LuaScript:
        """Register a Lua script."""
        raise NotSupportedError("register_script", self.__class__.__name__)

    def eval_script(
        self,
        name: str,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        *,
        version: int | None = None,
    ) -> Any:
        """Execute a registered Lua script."""
        raise NotSupportedError("eval_script", self.__class__.__name__)

    async def aeval_script(
        self,
        name: str,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        *,
        version: int | None = None,
    ) -> Any:
        """Execute a registered Lua script asynchronously."""
        raise NotSupportedError("aeval_script", self.__class__.__name__)


# =============================================================================
# Helper Functions
# =============================================================================


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
    import sys

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


# =============================================================================
# Wrapped Cache Classes
# =============================================================================


class WrappedLocMemCache(LocMemCache, BaseCacheExtensions):
    """LocMemCache with cachex extensions.

    Enables keys() by accessing the internal _cache dict.
    Enables ttl()/expire()/persist() by accessing the internal _expire_info dict.
    """

    _cachex_support: Literal["cachex", "wrapped", "limited"] = "wrapped"

    def _get_internal_cache(self) -> dict[str, Any]:
        """Access the internal cache dictionary of LocMemCache."""
        return getattr(self, "_cache", {})

    def _get_expire_info(self) -> dict[str, float | None]:
        """Access the internal expiry info dictionary."""
        return getattr(self, "_expire_info", {})

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the TTL of a key in seconds.

        Returns:
            TTL in seconds, -1 for no expiry, -2 for key not found.
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
            matching_keys = [k for k in all_keys if fnmatch.fnmatch(k, pattern)]
        else:
            matching_keys = all_keys

        matching_keys.sort()
        return matching_keys


class WrappedDatabaseCache(DatabaseCache, BaseCacheExtensions):
    """DatabaseCache with cachex extensions.

    Enables keys() by querying the database table.
    Enables ttl() by reading the expires column.
    """

    _cachex_support: Literal["cachex", "wrapped", "limited"] = "wrapped"

    def _get_table_name(self) -> str:
        """Get the database table name for this cache."""
        return cast("Any", self)._table

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List keys matching the pattern by querying the database."""
        table_name = self._get_table_name()
        quoted_table_name = connection.ops.quote_name(table_name)

        if pattern and pattern != "*":
            transformed_pattern = self.make_key(pattern)
            sql_pattern = transformed_pattern.replace("*", "%").replace("?", "_")
        else:
            sql_pattern = "%"

        current_time = time.time()

        with connection.cursor() as cursor:
            if connection.vendor in ("postgresql", "oracle"):
                expires_condition = "expires > to_timestamp(%s)"
            elif connection.vendor == "mysql":
                expires_condition = "expires > FROM_UNIXTIME(%s)"
            else:
                expires_condition = "expires > %s"

            keys_sql = f"""
                SELECT cache_key
                FROM {quoted_table_name}
                WHERE cache_key LIKE %s AND {expires_condition}
                ORDER BY cache_key
            """  # noqa: S608
            cursor.execute(keys_sql, [sql_pattern, current_time])
            raw_keys = [row[0] for row in cursor.fetchall()]

            keys = []
            for cache_key in raw_keys:
                if cache_key.startswith(self.key_prefix):
                    key_without_prefix = cache_key[len(self.key_prefix) :]
                    parts = key_without_prefix.split(":", 2)
                    if len(parts) >= 3:
                        original_key = parts[2]
                    else:
                        original_key = key_without_prefix
                    keys.append(original_key)
                else:
                    keys.append(cache_key)

        return keys

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get TTL by reading the expires column from database.

        Returns:
            TTL in seconds, -2 for key not found or expired.
        """
        table_name = self._get_table_name()
        quoted_table_name = connection.ops.quote_name(table_name)
        cache_key = self.make_key(str(key), version=version)
        current_time = time.time()

        with connection.cursor() as cursor:
            if connection.vendor in ("postgresql", "oracle"):
                sql = f"""
                    SELECT EXTRACT(EPOCH FROM expires) - %s
                    FROM {quoted_table_name}
                    WHERE cache_key = %s
                """  # noqa: S608
            elif connection.vendor == "mysql":
                sql = f"""
                    SELECT UNIX_TIMESTAMP(expires) - %s
                    FROM {quoted_table_name}
                    WHERE cache_key = %s
                """  # noqa: S608
            else:
                sql = f"""
                    SELECT expires - %s
                    FROM {quoted_table_name}
                    WHERE cache_key = %s
                """  # noqa: S608

            cursor.execute(sql, [current_time, cache_key])
            row = cursor.fetchone()
            if row is None:
                return -2
            ttl_val = row[0]
            if ttl_val <= 0:
                return -2
            return int(ttl_val)

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get DatabaseCache info in structured format."""
        table_name = self._get_table_name()
        quoted_table_name = connection.ops.quote_name(table_name)

        total_count: int | str = 0
        active_count: int | str = 0
        expired_count: int | str = 0

        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {quoted_table_name}")  # noqa: S608
                total_count = cursor.fetchone()[0]

                current_time = time.time()
                if connection.vendor in ("postgresql", "oracle"):
                    expires_condition = "expires > to_timestamp(%s)"
                elif connection.vendor == "mysql":
                    expires_condition = "expires > FROM_UNIXTIME(%s)"
                else:
                    expires_condition = "expires > %s"

                cursor.execute(
                    f"SELECT COUNT(*) FROM {quoted_table_name} WHERE {expires_condition}",  # noqa: S608
                    [current_time],
                )
                active_count = cursor.fetchone()[0]
                expired_count = (
                    total_count - active_count if isinstance(total_count, int) and isinstance(active_count, int) else 0
                )
        except Exception:  # noqa: BLE001
            total_count = "error"
            active_count = "error"

        return {
            "backend": "DatabaseCache",
            "server": {
                "redis_version": f"DatabaseCache ({connection.vendor})",
                "os": f"table: {table_name}",
            },
            "keyspace": {
                "db0": {
                    "keys": active_count if active_count != "error" else 0,
                    "expires": active_count if active_count != "error" else 0,
                },
            },
            "stats": {
                "expired_keys": expired_count,
            },
        }


class WrappedFileBasedCache(FileBasedCache, BaseCacheExtensions):
    """FileBasedCache with cachex extensions.

    Lists keys as MD5 hash filenames (original keys cannot be recovered).
    """

    _cachex_support: Literal["cachex", "wrapped", "limited"] = "wrapped"

    def _get_cache_dir(self) -> Path | None:
        """Get the cache directory path."""
        from pathlib import Path

        cache_dir = getattr(self, "_dir", None)
        return Path(cache_dir) if cache_dir else None

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List cache file hashes as keys."""
        cache_path = self._get_cache_dir()
        if not cache_path or not cache_path.exists():
            return []

        try:
            hashes = [fp.name for fp in cache_path.rglob("*") if fp.is_file()]

            if pattern and pattern != "*":
                hashes = [h for h in hashes if fnmatch.fnmatch(h, pattern)]

            hashes.sort()
            return hashes
        except Exception:  # noqa: BLE001
            return []

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get FileBasedCache info in structured format."""
        cache_path = self._get_cache_dir()

        if not cache_path:
            return {
                "backend": "FileBasedCache",
                "server": {"redis_version": "FileBasedCache", "os": "unknown directory"},
            }

        file_count: int = 0
        total_size: int = 0

        with contextlib.suppress(Exception):
            if cache_path.exists():
                files = list(cache_path.rglob("*"))
                file_count = sum(1 for f in files if f.is_file())
                total_size = sum(f.stat().st_size for f in files if f.is_file())

        return {
            "backend": "FileBasedCache",
            "server": {
                "redis_version": "FileBasedCache",
                "os": str(cache_path),
            },
            "memory": {
                "used_memory": total_size,
                "used_memory_human": _format_bytes(total_size),
            },
            "keyspace": {
                "db0": {
                    "keys": file_count,
                },
            },
        }


class WrappedDummyCache(DummyCache, BaseCacheExtensions):
    """DummyCache with cachex extensions."""

    _cachex_support: Literal["cachex", "wrapped", "limited"] = "wrapped"

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """DummyCache has no keys."""
        return []

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get DummyCache info in structured format."""
        return {
            "backend": "DummyCache",
            "server": {
                "redis_version": "DummyCache (no storage)",
                "os": "All data is discarded - used for development/testing",
            },
            "keyspace": {
                "db0": {
                    "keys": 0,
                },
            },
        }


# Mapping of original classes to wrapped classes
WRAPPED_CLASS_MAP: dict[type, type] = {
    LocMemCache: WrappedLocMemCache,
    DatabaseCache: WrappedDatabaseCache,
    FileBasedCache: WrappedFileBasedCache,
    DummyCache: WrappedDummyCache,
}

# Add optional backends if available
if PyLibMCCache is not None:

    class WrappedPyLibMCCache(PyLibMCCache, BaseCacheExtensions):
        """PyLibMCCache with cachex extensions."""

        _cachex_support: Literal["cachex", "wrapped", "limited"] = "wrapped"

        def info(self, section: str | None = None) -> dict[str, Any]:
            """Get memcached stats in structured format."""
            mc_stats: dict[str, Any] = {}
            if hasattr(self, "_cache") and hasattr(cast("Any", self)._cache, "stats"):
                with contextlib.suppress(Exception):
                    mc_stats = cast("Any", self)._cache.stats() or {}

            bytes_used = 0
            curr_items = 0
            for server_stats in mc_stats.values() if mc_stats else []:
                if isinstance(server_stats, dict):
                    bytes_used += int(server_stats.get("bytes", 0))
                    curr_items += int(server_stats.get("curr_items", 0))

            return {
                "backend": "Memcached",
                "server": {
                    "redis_version": "Memcached",
                },
                "memory": {
                    "used_memory": bytes_used,
                    "used_memory_human": _format_bytes(bytes_used) if bytes_used else None,
                },
                "keyspace": {
                    "db0": {
                        "keys": curr_items,
                    },
                }
                if curr_items
                else None,
            }

    WRAPPED_CLASS_MAP[PyLibMCCache] = WrappedPyLibMCCache

if PyMemcacheCache is not None:

    class WrappedPyMemcacheCache(PyMemcacheCache, BaseCacheExtensions):
        """PyMemcacheCache with cachex extensions."""

        _cachex_support: Literal["cachex", "wrapped", "limited"] = "wrapped"

        def info(self, section: str | None = None) -> dict[str, Any]:
            """Get memcached stats in structured format."""
            mc_stats: dict[str, Any] = {}
            if hasattr(self, "_cache") and hasattr(cast("Any", self)._cache, "stats"):
                with contextlib.suppress(Exception):
                    mc_stats = cast("Any", self)._cache.stats() or {}

            bytes_used = 0
            curr_items = 0
            for server_stats in mc_stats.values() if mc_stats else []:
                if isinstance(server_stats, dict):
                    bytes_used += int(server_stats.get("bytes", 0))
                    curr_items += int(server_stats.get("curr_items", 0))

            return {
                "backend": "Memcached",
                "server": {
                    "redis_version": "Memcached",
                },
                "memory": {
                    "used_memory": bytes_used,
                    "used_memory_human": _format_bytes(bytes_used) if bytes_used else None,
                },
                "keyspace": {
                    "db0": {
                        "keys": curr_items,
                    },
                }
                if curr_items
                else None,
            }

    WRAPPED_CLASS_MAP[PyMemcacheCache] = WrappedPyMemcacheCache

if DjangoRedisCache is not None:

    class WrappedDjangoRedisCache(DjangoRedisCache, BaseCacheExtensions):
        """Django RedisCache with cachex extensions."""

        _cachex_support: Literal["cachex", "wrapped", "limited"] = "wrapped"

        def info(self, section: str | None = None) -> dict[str, Any]:
            """Get Django Redis cache info in structured format."""
            return {
                "backend": "Django RedisCache",
                "server": {
                    "redis_version": "Django RedisCache (limited support)",
                    "os": "Use django-cachex ValkeyCache/RedisCache for full features",
                },
            }

    WRAPPED_CLASS_MAP[DjangoRedisCache] = WrappedDjangoRedisCache


# =============================================================================
# Public API
# =============================================================================


def wrap_cache(cache: BaseCache) -> BaseCache:
    """Extend a Django cache instance with cachex functionality.

    This function patches the cache instance's class to add extended methods
    (keys, ttl, info, etc.) while preserving all original functionality.

    Args:
        cache: A Django cache instance.

    Returns:
        The same cache instance, now with extended methods available.
    """
    # Idempotency check - already wrapped
    if hasattr(cache, "_cachex_support"):
        return cache

    # Find the extended class for this cache type
    extended_class = WRAPPED_CLASS_MAP.get(type(cache))

    if extended_class is not None:
        # Patch the instance's class
        cache.__class__ = extended_class
    else:
        # Unknown backend - add base extensions only
        # Create a dynamic extended class
        original_class = type(cache)
        extended_class = type(
            f"Extended{original_class.__name__}",
            (original_class, BaseCacheExtensions),
            {},
        )
        cache.__class__ = extended_class

    return cache
