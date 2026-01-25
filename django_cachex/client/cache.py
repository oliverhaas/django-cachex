"""Cache backend classes for key-value backends like Valkey or Redis.

This module provides cache backend classes that extend Django's BaseCache
with Redis/Valkey specific features:
- Multi-serializer fallback support
- Compression support
- Extended Redis operations (hashes, lists, sets, sorted sets)
- TTL and expiry operations
- Pattern-based operations
- Distributed locking
- Pipeline support

Architecture (matching Django's RedisCache structure):
- KeyValueCache(BaseCache): Base class with all logic, library-agnostic
- RedisCache(KeyValueCache): Sets _class = RedisCacheClient
- ValkeyCache(KeyValueCache): Sets _class = ValkeyCacheClient

Internal attributes (matching Django's RedisCache for compatibility):
- _servers: List of server URLs
- _class: The CacheClient class to use
- _options: Options dict from params["OPTIONS"]
- _cache: Cached property that instantiates the CacheClient

Usage:
    CACHES = {
        "default": {
            "BACKEND": "django_cachex.cache.RedisCache",  # or ValkeyCache
            "LOCATION": "redis://127.0.0.1:6379/1",
        }
    }
"""

from __future__ import annotations

import re
from functools import cached_property
from typing import TYPE_CHECKING, Any

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from django_cachex.client.pipeline import Pipeline
    from django_cachex.types import AbsExpiryT, EncodableT, ExpiryT, KeyT

from django_cachex.client.default import (
    KeyValueCacheClient,
    RedisCacheClient,
    ValkeyCacheClient,
)

# Alias builtin set type to avoid shadowing by the set() method
_Set = set

# Regex for escaping glob special characters
_special_re = re.compile("([*?[])")


def _glob_escape(s: str) -> str:
    """Escape glob special characters in a string."""
    return _special_re.sub(r"[\1]", s)


# =============================================================================
# KeyValueCache - base class extending Django's BaseCache
# =============================================================================


class KeyValueCache(BaseCache):
    """Generic cache backend base class.

    Extends Django's BaseCache with Redis/Valkey specific features:
    - Multi-serializer fallback support
    - Compression support
    - Extended Redis operations (hashes, lists, sets, sorted sets)
    - TTL and expiry operations
    - Pattern-based operations
    - Distributed locking
    - Pipeline support

    Internal structure matches Django's RedisCache for compatibility:
    - _servers: List of server URLs
    - _class: The CacheClient class (subclasses override)
    - _options: Options dict
    - _cache: Cached property for CacheClient instance

    Subclasses (RedisCache, ValkeyCache) specialize for their respective
    libraries by overriding _class to point to the appropriate CacheClient.
    """

    # Class attribute - subclasses override this
    _class: type[KeyValueCacheClient] = KeyValueCacheClient

    def __init__(self, server: str, params: dict[str, Any]) -> None:
        super().__init__(params)
        # Parse server(s) - matches Django's RedisCache behavior
        if isinstance(server, str):
            self._servers = re.split("[;,]", server)
        else:
            self._servers = server

        self._options = params.get("OPTIONS", {})

    @cached_property
    def _cache(self) -> KeyValueCacheClient:
        """Get the CacheClient instance (matches Django's pattern)."""
        return self._class(self._servers, **self._options)

    def get_backend_timeout(self, timeout: float | None = DEFAULT_TIMEOUT) -> int | None:
        """Convert timeout to backend format (matches Django's RedisCache)."""
        if timeout == DEFAULT_TIMEOUT:
            timeout = self.default_timeout
        # The key will be made persistent if None used as a timeout.
        # Non-positive values will cause the key to be deleted.
        return None if timeout is None else max(0, int(timeout))

    # =========================================================================
    # Pattern helpers
    # =========================================================================

    def make_pattern(self, pattern: str, version: int | None = None) -> str:
        """Build a pattern for key matching with proper escaping."""
        escaped_prefix = _glob_escape(self.key_prefix)
        ver = version if version is not None else self.version
        return self.key_func(pattern, escaped_prefix, ver)

    def reverse_key(self, key: str) -> str:
        """Reverse a made key back to original (strip prefix:version:)."""
        parts = key.split(":", 2)
        if len(parts) == 3:
            return parts[2]
        return key

    # =========================================================================
    # Core Cache Operations (Django's BaseCache interface)
    # =========================================================================

    def add(
        self, key: KeyT, value: EncodableT, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None
    ) -> bool:
        """Set a value only if the key doesn't exist."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.add(key, value, self.get_backend_timeout(timeout))

    def get(self, key: KeyT, default: Any = None, version: int | None = None) -> Any:
        """Fetch a value from the cache."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.get(key, default)

    def set(  # type: ignore[override]
        self,
        key: KeyT,
        value: EncodableT,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        **kwargs: Any,
    ) -> bool | None:
        """Set a value in the cache.

        Extended to support nx/xx flags beyond Django's standard interface.

        Args:
            key: Cache key
            value: Value to store
            timeout: Expiry time in seconds (DEFAULT_TIMEOUT uses default)
            version: Key version
            **kwargs: Extended options:
                - nx: Only set if key does not exist
                - xx: Only set if key already exists

        Returns:
            When nx or xx is True: bool indicating success
            Otherwise: None (standard Django behavior)
        """
        nx = kwargs.get("nx", False)
        xx = kwargs.get("xx", False)
        key = self.make_and_validate_key(key, version=version)
        if nx or xx:
            # Use extended method with flags - returns bool for success
            return self._cache.set_with_flags(key, value, self.get_backend_timeout(timeout), nx=nx, xx=xx)
        # Use standard Django method - returns None
        self._cache.set(key, value, self.get_backend_timeout(timeout))
        return None

    def touch(self, key: KeyT, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        """Update the timeout on a key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.touch(key, self.get_backend_timeout(timeout))

    def delete(self, key: KeyT, version: int | None = None) -> bool:
        """Remove a key from the cache."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.delete(key)

    def get_many(self, keys: list[KeyT], version: int | None = None) -> dict[KeyT, Any]:  # type: ignore[override]
        """Retrieve many keys."""
        key_map = {self.make_and_validate_key(key, version=version): key for key in keys}
        ret = self._cache.get_many(key_map.keys())
        return {key_map[k]: v for k, v in ret.items()}  # type: ignore[index]

    def has_key(self, key: KeyT, version: int | None = None) -> bool:
        """Check if a key exists."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.has_key(key)

    def incr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Increment a value."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.incr(key, delta)

    def set_many(
        self, data: Mapping[KeyT, EncodableT], timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None
    ) -> list:
        """Set multiple values."""
        if not data:
            return []
        safe_data = {self.make_and_validate_key(key, version=version): value for key, value in data.items()}
        self._cache.set_many(safe_data, self.get_backend_timeout(timeout))  # type: ignore[arg-type]
        return []

    def delete_many(self, keys: list[KeyT], version: int | None = None) -> int:  # type: ignore[override]
        """Delete multiple keys from the cache.

        Extended to return the count of deleted keys (Django's returns None).
        """
        keys = list(keys)  # Convert generator to list
        if not keys:
            return 0
        safe_keys = [self.make_and_validate_key(key, version=version) for key in keys]
        return self._cache.delete_many(safe_keys)

    def clear(self) -> bool:  # type: ignore[override]
        """Flush the database."""
        return self._cache.clear()

    def close(self, **kwargs: Any) -> None:
        """Close all connection pools."""
        self._cache.close(**kwargs)

    # =========================================================================
    # Extended Methods (beyond Django's BaseCache)
    # =========================================================================

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the time-to-live of a key in seconds."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.ttl(key)

    def pttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the time-to-live of a key in milliseconds."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.pttl(key)

    def persist(self, key: KeyT, version: int | None = None) -> bool:
        """Remove the expiry from a key, making it persistent."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.persist(key)

    def expire(
        self,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry time on a key in seconds."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.expire(key, timeout)

    def expire_at(
        self,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry to an absolute time."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.expireat(key, when)

    def pexpire(
        self,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry time on a key in milliseconds."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.pexpire(key, timeout)

    def pexpire_at(
        self,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry to an absolute time in milliseconds."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.pexpireat(key, when)

    def keys(
        self,
        pattern: str = "*",
        version: int | None = None,
    ) -> list[str]:
        """Return all keys matching pattern (returns original keys without prefix)."""
        full_pattern = self.make_pattern(pattern, version=version)
        raw_keys = self._cache.keys(full_pattern)
        return [self.reverse_key(k) for k in raw_keys]

    def iter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]:
        """Iterate over keys matching pattern using SCAN."""
        full_pattern = self.make_pattern(pattern, version=version)
        for key in self._cache.iter_keys(full_pattern, itersize=itersize):
            yield self.reverse_key(key)

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        """Delete all keys matching pattern."""
        full_pattern = self.make_pattern(pattern, version=version)
        return self._cache.delete_pattern(full_pattern, itersize=itersize)

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
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lock(
            key,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    def pipeline(
        self,
        *,
        transaction: bool = True,
        version: int | None = None,
    ) -> Pipeline:
        """Create a pipeline for batched operations."""
        pipe = self._cache.pipeline(
            transaction=transaction,
            version=version if version is not None else self.version,
        )
        # Set key_func for proper key prefixing
        pipe._key_func = self.make_and_validate_key
        return pipe

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(
        self,
        key: KeyT,
        field: str,
        value: EncodableT,
        version: int | None = None,
    ) -> int:
        """Set field in hash at key to value."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hset(key, field, value)

    def hdel(
        self,
        key: KeyT,
        *fields: str,
        version: int | None = None,
    ) -> int:
        """Delete one or more hash fields."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hdel(key, *fields)

    def hlen(self, key: KeyT, version: int | None = None) -> int:
        """Get number of fields in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hlen(key)

    def hkeys(self, key: KeyT, version: int | None = None) -> list[str]:
        """Get all field names in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hkeys(key)

    def hexists(
        self,
        key: KeyT,
        field: str,
        version: int | None = None,
    ) -> bool:
        """Check if field exists in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hexists(key, field)

    def hget(
        self,
        key: KeyT,
        field: str,
        version: int | None = None,
    ) -> Any:
        """Get value of field in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hget(key, field)

    def hgetall(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> dict[str, Any]:
        """Get all fields and values in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hgetall(key)

    def hmget(
        self,
        key: KeyT,
        *fields: str,
        version: int | None = None,
    ) -> list[Any]:
        """Get values of multiple fields in hash."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hmget(key, *fields)

    def hmset(
        self,
        key: KeyT,
        mapping: Mapping[str, EncodableT],
        version: int | None = None,
    ) -> bool:
        """Set multiple hash fields to multiple values."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hmset(key, mapping)

    def hincrby(
        self,
        key: KeyT,
        field: str,
        amount: int = 1,
        version: int | None = None,
    ) -> int:
        """Increment value of field in hash by amount."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hincrby(key, field, amount)

    def hincrbyfloat(
        self,
        key: KeyT,
        field: str,
        amount: float = 1.0,
        version: int | None = None,
    ) -> float:
        """Increment float value of field in hash by amount."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hincrbyfloat(key, field, amount)

    def hsetnx(
        self,
        key: KeyT,
        field: str,
        value: EncodableT,
        version: int | None = None,
    ) -> bool:
        """Set field in hash only if it doesn't exist."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hsetnx(key, field, value)

    def hvals(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> list[Any]:
        """Get all values in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hvals(key)

    # =========================================================================
    # List Operations
    # =========================================================================

    def lpush(
        self,
        key: KeyT,
        *values: EncodableT,
        version: int | None = None,
    ) -> int:
        """Push values onto head of list at key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lpush(key, *values)

    def rpush(
        self,
        key: KeyT,
        *values: EncodableT,
        version: int | None = None,
    ) -> int:
        """Push values onto tail of list at key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.rpush(key, *values)

    def lpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from head of list.

        Args:
            key: The list key
            count: Optional number of elements to pop (default: 1, returns single value)
            version: Key version

        Returns:
            Single value if count is None, list of values if count is specified
        """
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lpop(key, count=count)

    def rpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from tail of list.

        Args:
            key: The list key
            count: Optional number of elements to pop (default: 1, returns single value)
            version: Key version

        Returns:
            Single value if count is None, list of values if count is specified
        """
        key = self.make_and_validate_key(key, version=version)
        return self._cache.rpop(key, count=count)

    def lrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        version: int | None = None,
    ) -> list[Any]:
        """Get a range of elements from list."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lrange(key, start, end)

    def lindex(
        self,
        key: KeyT,
        index: int,
        version: int | None = None,
    ) -> Any:
        """Get element at index in list."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lindex(key, index)

    def llen(self, key: KeyT, version: int | None = None) -> int:
        """Get length of list at key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.llen(key)

    def lpos(
        self,
        key: KeyT,
        value: EncodableT,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list.

        Args:
            key: List key
            value: Value to search for
            rank: Rank of first match to return (1 for first, -1 for last, etc.)
            count: Number of matches to return (0 for all)
            maxlen: Limit search to first N elements
            version: Key version

        Returns:
            Index if count is None, list of indices if count is specified, None if not found
        """
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lpos(key, value, rank=rank, count=count, maxlen=maxlen)

    def lmove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
        version: int | None = None,
    ) -> Any | None:
        """Atomically move an element from one list to another.

        Args:
            src: Source list key
            dst: Destination list key
            wherefrom: Where to pop from source ('LEFT' or 'RIGHT')
            whereto: Where to push to destination ('LEFT' or 'RIGHT')
            version: Key version for both lists

        Returns:
            The moved element, or None if src is empty
        """
        src = self.make_and_validate_key(src, version=version)
        dst = self.make_and_validate_key(dst, version=version)
        return self._cache.lmove(src, dst, wherefrom, whereto)

    def lrem(
        self,
        key: KeyT,
        count: int,
        value: EncodableT,
        version: int | None = None,
    ) -> int:
        """Remove elements equal to value from list."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lrem(key, count, value)

    def ltrim(
        self,
        key: KeyT,
        start: int,
        end: int,
        version: int | None = None,
    ) -> bool:
        """Trim list to specified range."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.ltrim(key, start, end)

    def lset(
        self,
        key: KeyT,
        index: int,
        value: EncodableT,
        version: int | None = None,
    ) -> bool:
        """Set element at index in list."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lset(key, index, value)

    def linsert(
        self,
        key: KeyT,
        where: str,
        pivot: EncodableT,
        value: EncodableT,
        version: int | None = None,
    ) -> int:
        """Insert value before or after pivot in list."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.linsert(key, where, pivot, value)

    def blpop(
        self,
        *keys: KeyT,
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list.

        Blocks until an element is available or timeout expires.

        Args:
            *keys: One or more list keys to pop from (first available)
            timeout: Seconds to block (0 = block indefinitely)
            version: Key version

        Returns:
            Tuple of (original_key, value) or None if timeout expires.
        """
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        result = self._cache.blpop(nkeys, timeout=timeout)
        if result is None:
            return None
        # Reverse the key back to original
        return (self.reverse_key(result[0]), result[1])

    def brpop(
        self,
        *keys: KeyT,
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list.

        Blocks until an element is available or timeout expires.

        Args:
            *keys: One or more list keys to pop from (first available)
            timeout: Seconds to block (0 = block indefinitely)
            version: Key version

        Returns:
            Tuple of (original_key, value) or None if timeout expires.
        """
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        result = self._cache.brpop(nkeys, timeout=timeout)
        if result is None:
            return None
        # Reverse the key back to original
        return (self.reverse_key(result[0]), result[1])

    def blmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
        version: int | None = None,
    ) -> Any | None:
        """Blocking atomically move element from one list to another.

        Blocks until an element is available in src or timeout expires.

        Args:
            src: Source list key
            dst: Destination list key
            timeout: Seconds to block (0 = block indefinitely)
            wherefrom: Where to pop from source ('LEFT' or 'RIGHT')
            whereto: Where to push to destination ('LEFT' or 'RIGHT')
            version: Key version for both lists

        Returns:
            The moved element, or None if timeout expires.
        """
        src = self.make_and_validate_key(src, version=version)
        dst = self.make_and_validate_key(dst, version=version)
        return self._cache.blmove(src, dst, timeout, wherefrom, whereto)

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(
        self,
        key: KeyT,
        *members: EncodableT,
        version: int | None = None,
    ) -> int:
        """Add members to a set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.sadd(key, *members)

    def scard(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of members in a set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.scard(key)

    def sdiff(
        self,
        *keys: KeyT,
        version: int | None = None,
    ) -> _Set[Any]:
        """Return the difference between the first set and all successive sets."""
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return self._cache.sdiff(nkeys)

    def sdiffstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the difference of sets at dest.

        Args:
            dest: Destination key to store the result
            *keys: Source keys to compute difference from
            version: Version for all keys (default for dest and keys)
            version_dest: Override version for destination key
            version_keys: Override version for source keys
        """
        # Use specific versions if provided, otherwise fall back to version
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return self._cache.sdiffstore(dest, nkeys)

    def sinter(
        self,
        *keys: KeyT,
        version: int | None = None,
    ) -> _Set[Any]:
        """Return the intersection of all sets."""
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return self._cache.sinter(nkeys)

    def sinterstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the intersection of sets at dest.

        Args:
            dest: Destination key to store the result
            *keys: Source keys to compute intersection from
            version: Version for all keys (default for dest and keys)
            version_dest: Override version for destination key
            version_keys: Override version for source keys
        """
        # Use specific versions if provided, otherwise fall back to version
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return self._cache.sinterstore(dest, nkeys)

    def sismember(
        self,
        key: KeyT,
        member: EncodableT,
        version: int | None = None,
    ) -> bool:
        """Check if member is in set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.sismember(key, member)

    def smembers(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> _Set[Any]:
        """Get all members of a set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.smembers(key)

    def smove(
        self,
        src: KeyT,
        dst: KeyT,
        member: EncodableT,
        version: int | None = None,
    ) -> bool:
        """Move member from one set to another."""
        src = self.make_and_validate_key(src, version=version)
        dst = self.make_and_validate_key(dst, version=version)
        return self._cache.smove(src, dst, member)

    def spop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | _Set[Any]:
        """Remove and return random member(s) from set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.spop(key, count)

    def srandmember(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any]:
        """Get random member(s) from set without removing."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.srandmember(key, count)

    def srem(
        self,
        key: KeyT,
        *members: EncodableT,
        version: int | None = None,
    ) -> int:
        """Remove members from a set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.srem(key, *members)

    def sunion(
        self,
        *keys: KeyT,
        version: int | None = None,
    ) -> _Set[Any]:
        """Return the union of all sets."""
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return self._cache.sunion(nkeys)

    def sunionstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the union of sets at dest.

        Args:
            dest: Destination key to store the result
            *keys: Source keys to compute union from
            version: Version for all keys (default for dest and keys)
            version_dest: Override version for destination key
            version_keys: Override version for source keys
        """
        # Use specific versions if provided, otherwise fall back to version
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return self._cache.sunionstore(dest, nkeys)

    def smismember(
        self,
        key: KeyT,
        *members: EncodableT,
        version: int | None = None,
    ) -> list[bool]:
        """Check if multiple values are members of a set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.smismember(key, *members)

    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, _Set[Any]]:
        """Incrementally iterate over set members.

        Args:
            key: The set key
            cursor: Cursor position (0 to start)
            match: Pattern to filter members
            count: Hint for number of elements per batch
            version: Key version

        Returns:
            Tuple of (next_cursor, set of members)
        """
        key = self.make_and_validate_key(key, version=version)
        return self._cache.sscan(key, cursor=cursor, match=match, count=count)

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> Iterator[Any]:
        """Iterate over set members using SSCAN.

        Args:
            key: The set key
            match: Pattern to filter members
            count: Hint for number of elements per batch
            version: Key version

        Yields:
            Decoded member values
        """
        key = self.make_and_validate_key(key, version=version)
        yield from self._cache.sscan_iter(key, match=match, count=count)

    # =========================================================================
    # Sorted Set Operations
    # =========================================================================

    def zadd(
        self,
        key: KeyT,
        mapping: Mapping[EncodableT, float],
        *,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        gt: bool = False,
        lt: bool = False,
        version: int | None = None,
    ) -> int:
        """Add members to a sorted set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zadd(key, mapping, nx=nx, xx=xx, ch=ch, gt=gt, lt=lt)

    def zcard(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of members in a sorted set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zcard(key)

    def zcount(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Count members with scores between min and max."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zcount(key, min_score, max_score)

    def zincrby(
        self,
        key: KeyT,
        amount: float,
        member: EncodableT,
        version: int | None = None,
    ) -> float:
        """Increment the score of a member."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zincrby(key, amount, member)

    def zpopmax(
        self,
        key: KeyT,
        count: int = 1,
        version: int | None = None,
    ) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zpopmax(key, count)

    def zpopmin(
        self,
        key: KeyT,
        count: int = 1,
        version: int | None = None,
    ) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zpopmin(key, count)

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
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zrange(key, start, end, withscores=withscores)

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
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zrangebyscore(key, min_score, max_score, withscores=withscores, start=start, num=num)

    def zrank(
        self,
        key: KeyT,
        member: EncodableT,
        version: int | None = None,
    ) -> int | None:
        """Get the rank of a member (0-based, lowest score first)."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zrank(key, member)

    def zrem(
        self,
        key: KeyT,
        *members: EncodableT,
        version: int | None = None,
    ) -> int:
        """Remove members from a sorted set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zrem(key, *members)

    def zremrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Remove members with scores between min and max."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zremrangebyscore(key, min_score, max_score)

    def zremrangebyrank(
        self,
        key: KeyT,
        start: int,
        end: int,
        version: int | None = None,
    ) -> int:
        """Remove members by rank range."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zremrangebyrank(key, start, end)

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
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zrevrange(key, start, end, withscores=withscores)

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
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zrevrangebyscore(key, max_score, min_score, withscores=withscores, start=start, num=num)

    def zscore(
        self,
        key: KeyT,
        member: EncodableT,
        version: int | None = None,
    ) -> float | None:
        """Get the score of a member."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zscore(key, member)

    def zrevrank(
        self,
        key: KeyT,
        member: EncodableT,
        version: int | None = None,
    ) -> int | None:
        """Get the rank of a member (0-based, highest score first)."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zrevrank(key, member)

    def zmscore(
        self,
        key: KeyT,
        *members: EncodableT,
        version: int | None = None,
    ) -> list[float | None]:
        """Get the scores of multiple members."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zmscore(key, *members)

    # =========================================================================
    # Direct Client Access
    # =========================================================================

    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the underlying Redis client."""
        return self._cache.get_client(key, write=write)

    # =========================================================================
    # Key Operations
    # =========================================================================

    def rename(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key atomically.

        Renames the key from src to dst. If dst already exists, it is overwritten.
        The TTL is preserved.

        Args:
            src: Source key name
            dst: Destination key name
            version: Version for both keys (default)
            version_src: Override version for source key
            version_dst: Override version for destination key

        Returns:
            True on success

        Raises:
            ValueError: If src does not exist
        """
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src_key = self.make_and_validate_key(src, version=src_ver)
        dst_key = self.make_and_validate_key(dst, version=dst_ver)
        return self._cache.rename(src_key, dst_key)

    def renamenx(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key only if the destination does not exist.

        Atomically renames src to dst only if dst does not already exist.
        The TTL is preserved.

        Args:
            src: Source key name
            dst: Destination key name
            version: Version for both keys (default)
            version_src: Override version for source key
            version_dst: Override version for destination key

        Returns:
            True if renamed, False if dst already exists

        Raises:
            ValueError: If src does not exist
        """
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src_key = self.make_and_validate_key(src, version=src_ver)
        dst_key = self.make_and_validate_key(dst, version=dst_ver)
        return self._cache.renamenx(src_key, dst_key)

    def incr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Atomically increment the version of a key using RENAME.

        This is more efficient than Django's default implementation which uses
        GET + SET + DELETE. RENAME is O(1), atomic, and preserves TTL.

        Args:
            key: The cache key
            delta: Amount to increment version by (default 1)
            version: Current version (defaults to cache's default version)

        Returns:
            The new version number

        Raises:
            ValueError: If the key does not exist
        """
        if version is None:
            version = self.version
        old_key = self.make_and_validate_key(key, version=version)
        new_key = self.make_and_validate_key(key, version=version + delta)
        self._cache.rename(old_key, new_key)
        return version + delta

    def decr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Atomically decrement the version of a key.

        This is a convenience method that calls incr_version with a negative delta.

        Args:
            key: The cache key
            delta: Amount to decrement version by (default 1)
            version: Current version (defaults to cache's default version)

        Returns:
            The new version number

        Raises:
            ValueError: If the key does not exist
        """
        return self.incr_version(key, -delta, version)


# =============================================================================
# RedisCache - concrete implementation for redis-py
# =============================================================================


class RedisCache(KeyValueCache):
    """Redis cache backend.

    Concrete implementation of KeyValueCache for redis-py.
    Uses RedisCacheClient which uses redis-py library.
    """

    _class = RedisCacheClient


# =============================================================================
# ValkeyCache - concrete implementation for valkey-py
# =============================================================================


class ValkeyCache(KeyValueCache):
    """Valkey cache backend.

    Concrete implementation of KeyValueCache for valkey-py.
    Uses ValkeyCacheClient which uses valkey-py library.
    """

    _class = ValkeyCacheClient


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "KeyValueCache",
    "RedisCache",
    "ValkeyCache",
]
