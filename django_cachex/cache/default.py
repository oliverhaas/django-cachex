"""Cache backend classes for key-value backends like Valkey or Redis.

Extends Django's BaseCache with Redis/Valkey features: data structures,
TTL operations, pattern matching, distributed locking, pipelines, and
multi-serializer/compressor support.
"""

from __future__ import annotations

import logging
import re
from functools import cached_property
from typing import TYPE_CHECKING, Any, override

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.utils.module_loading import import_string

if TYPE_CHECKING:
    import builtins
    from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence

    from django_cachex.client.pipeline import Pipeline
    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT, KeyType, _Set

from django_cachex.client.default import (
    KeyValueCacheClient,
    RedisCacheClient,
    ValkeyCacheClient,
)
from django_cachex.omit_exception import omit_exception
from django_cachex.script import ScriptHelpers

# Sentinel value for methods with dynamic return values (e.g., get() returns default arg)
CONNECTION_INTERRUPTED = object()

# Regex for escaping glob special characters
_special_re = re.compile("([*?[])")


def _glob_escape(s: str) -> str:
    """Escape glob special characters in a string."""
    return _special_re.sub(r"[\1]", s)


# =============================================================================
# KeyValueCache - base class extending Django's BaseCache
# =============================================================================


class KeyValueCache(BaseCache):
    """Django cache backend for Redis/Valkey with extended features.

    Base class for all django-cachex backends. Extends Django's ``BaseCache``
    with data structures, TTL ops, pattern matching, locking, and pipelines.
    """

    # Support level marker for admin interface
    _cachex_support: str = "cachex"

    # Class attribute - subclasses override this
    _class: builtins.type[KeyValueCacheClient] = KeyValueCacheClient

    def __init__(self, server: str, params: dict[str, Any]) -> None:
        super().__init__(params)
        # Parse server(s) - matches Django's RedisCache behavior
        if isinstance(server, str):
            self._servers = re.split("[;,]", server)
        else:
            self._servers = server

        self._options = params.get("OPTIONS", {})

        # Handle reverse_key_function option (mirrors Django's KEY_FUNCTION handling)
        reverse_key_func = self._options.get("reverse_key_function")
        if reverse_key_func is not None:
            if isinstance(reverse_key_func, str):
                self._reverse_key_func: Callable[[str], str] | None = import_string(reverse_key_func)
            else:
                self._reverse_key_func = reverse_key_func
        else:
            self._reverse_key_func = None

        # Exception handling config (from OPTIONS)
        self._ignore_exceptions = self._options.get("ignore_exceptions", False)
        self._log_ignored_exceptions = self._options.get("log_ignored_exceptions", False)
        self._logger = logging.getLogger(__name__) if self._log_ignored_exceptions else None

    @cached_property
    def _cache(self) -> KeyValueCacheClient:
        """Get the KeyValueCacheClient instance (matches Django's pattern)."""
        return self._class(self._servers, **self._options)

    def get_backend_timeout(self, timeout: float | None = DEFAULT_TIMEOUT) -> int | None:
        """Convert timeout to backend format (matches Django's RedisCache).

        Negative values are clamped to 0, causing immediate key deletion.
        This matches Django's behavior where negative timeouts expire keys.
        """
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
        if self._reverse_key_func is not None:
            return self._reverse_key_func(key)
        parts = key.split(":", 2)
        if len(parts) == 3:
            return parts[2]
        return key

    # =========================================================================
    # Core Cache Operations (Django's BaseCache interface)
    # =========================================================================

    @omit_exception(return_value=False)
    @override
    def add(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        """Set a value only if the key doesn't exist."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.add(key, value, self.get_backend_timeout(timeout))

    @omit_exception(return_value=False)
    @override
    async def aadd(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        """Set a value only if the key doesn't exist, asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.aadd(key, value, self.get_backend_timeout(timeout))

    @omit_exception(return_value=CONNECTION_INTERRUPTED)
    def _get(self, key: KeyT, version: int | None = None) -> Any:
        """Internal get with exception handling."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.get(key)

    @override
    def get(self, key: KeyT, default: Any = None, version: int | None = None) -> Any:
        """Fetch a value from the cache."""
        value = self._get(key, version=version)
        if value is CONNECTION_INTERRUPTED:
            return default
        if value is None:
            return default
        return value

    @omit_exception(return_value=CONNECTION_INTERRUPTED)
    async def _aget(self, key: KeyT, version: int | None = None) -> Any:
        """Internal async get with exception handling."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.aget(key)

    @override
    async def aget(self, key: KeyT, default: Any = None, version: int | None = None) -> Any:
        """Fetch a value from the cache asynchronously."""
        value = await self._aget(key, version=version)
        if value is CONNECTION_INTERRUPTED:
            return default
        if value is None:
            return default
        return value

    @omit_exception
    @override
    async def aset(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """Set a value in the cache asynchronously.

        Supports nx/xx/get kwargs: nx=True only sets if key doesn't exist,
        xx=True only sets if key exists, get=True returns the old value.
        """
        nx = kwargs.get("nx", False)
        xx = kwargs.get("xx", False)
        get = kwargs.get("get", False)
        key = self.make_and_validate_key(key, version=version)
        if nx or xx or get:
            return await self._cache.aset_with_flags(
                key,
                value,
                self.get_backend_timeout(timeout),
                nx=nx,
                xx=xx,
                get=get,
            )
        await self._cache.aset(key, value, self.get_backend_timeout(timeout))
        return None

    @omit_exception
    @override
    def set(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        **kwargs: Any,
    ) -> bool | Any:
        """Set a value in the cache.

        Supports nx/xx/get kwargs: nx=True only sets if key doesn't exist,
        xx=True only sets if key exists, get=True returns the old value.
        Returns bool when nx/xx is used, old value when get=True, None otherwise.
        """
        nx = kwargs.get("nx", False)
        xx = kwargs.get("xx", False)
        get = kwargs.get("get", False)
        key = self.make_and_validate_key(key, version=version)
        if nx or xx or get:
            return self._cache.set_with_flags(
                key,
                value,
                self.get_backend_timeout(timeout),
                nx=nx,
                xx=xx,
                get=get,
            )
        # Use standard Django method - returns None
        self._cache.set(key, value, self.get_backend_timeout(timeout))
        return None

    @omit_exception(return_value=False)
    @override
    def touch(self, key: KeyT, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        """Update the timeout on a key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.touch(key, self.get_backend_timeout(timeout))

    @omit_exception(return_value=False)
    @override
    async def atouch(self, key: KeyT, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        """Update the timeout on a key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.atouch(key, self.get_backend_timeout(timeout))

    @omit_exception(return_value=False)
    @override
    def delete(self, key: KeyT, version: int | None = None) -> bool:
        """Remove a key from the cache."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.delete(key)

    @omit_exception(return_value=False)
    @override
    async def adelete(self, key: KeyT, version: int | None = None) -> bool:
        """Remove a key from the cache asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.adelete(key)

    @omit_exception(return_value={})
    def _get_many(self, keys: list[KeyT], version: int | None = None) -> dict[KeyT, Any]:
        """Internal get_many with exception handling."""
        key_map = {self.make_and_validate_key(key, version=version): key for key in keys}
        ret = self._cache.get_many(key_map.keys())
        return {key_map[k]: v for k, v in ret.items()}  # type: ignore[index]

    @override
    def get_many(self, keys: list[KeyT], version: int | None = None) -> dict[KeyT, Any]:  # type: ignore[override]
        """Retrieve many keys."""
        return self._get_many(keys, version=version)

    @omit_exception(return_value={})
    async def _aget_many(self, keys: list[KeyT], version: int | None = None) -> dict[KeyT, Any]:
        """Internal async get_many with exception handling."""
        key_map = {self.make_and_validate_key(key, version=version): key for key in keys}
        ret = await self._cache.aget_many(key_map.keys())
        return {key_map[k]: v for k, v in ret.items()}  # type: ignore[index]

    @override
    async def aget_many(self, keys: list[KeyT], version: int | None = None) -> dict[KeyT, Any]:  # type: ignore[override]
        """Retrieve many keys asynchronously."""
        return await self._aget_many(keys, version=version)

    @omit_exception(return_value=False)
    @override
    def has_key(self, key: KeyT, version: int | None = None) -> bool:
        """Check if a key exists."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.has_key(key)

    @omit_exception(return_value=False)
    @override
    async def ahas_key(self, key: KeyT, version: int | None = None) -> bool:
        """Check if a key exists asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahas_key(key)

    @omit_exception(return_value=0)
    @override
    def incr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Increment a value."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.incr(key, delta)

    @omit_exception(return_value=0)
    @override
    async def aincr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Increment a value asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.aincr(key, delta)

    @omit_exception(return_value=0)
    @override
    async def adecr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Decrement a value asynchronously."""
        return await self.aincr(key, -delta, version)

    @override
    def get_or_set(
        self,
        key: KeyT,
        default: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> Any:
        """Fetch a key from the cache, setting it to default if missing."""
        val = self.get(key, self._missing_key, version=version)
        if val is self._missing_key:
            if callable(default):
                default = default()
            self.add(key, default, timeout=timeout, version=version)
            # Fetch the value again to avoid a race condition if another caller
            # added a value between the first get() and the add() above.
            return self.get(key, default, version=version)
        return val

    @override
    async def aget_or_set(
        self,
        key: KeyT,
        default: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> Any:
        """Fetch a key from the cache asynchronously, setting it to default if missing."""
        val = await self.aget(key, self._missing_key, version=version)
        if val is self._missing_key:
            if callable(default):
                default = default()
            await self.aadd(key, default, timeout=timeout, version=version)
            # Fetch the value again to avoid a race condition if another caller
            # added a value between the first aget() and the aadd() above.
            return await self.aget(key, default, version=version)
        return val

    @omit_exception(return_value=[])
    @override
    def set_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list:
        """Set multiple values."""
        if not data:
            return []
        safe_data = {self.make_and_validate_key(key, version=version): value for key, value in data.items()}
        self._cache.set_many(safe_data, self.get_backend_timeout(timeout))  # type: ignore[arg-type]
        return []

    @omit_exception(return_value=[])
    @override
    async def aset_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list:
        """Set multiple values asynchronously."""
        if not data:
            return []
        safe_data = {self.make_and_validate_key(key, version=version): value for key, value in data.items()}
        await self._cache.aset_many(safe_data, self.get_backend_timeout(timeout))  # type: ignore[arg-type]
        return []

    @omit_exception(return_value=0)
    @override
    def delete_many(self, keys: list[KeyT], version: int | None = None) -> int:
        """Delete multiple keys from the cache."""
        keys = list(keys)  # Convert generator to list
        if not keys:
            return 0
        safe_keys = [self.make_and_validate_key(key, version=version) for key in keys]
        return self._cache.delete_many(safe_keys)

    @omit_exception(return_value=0)
    @override
    async def adelete_many(self, keys: list[KeyT], version: int | None = None) -> int:
        """Delete multiple keys from the cache asynchronously."""
        keys = list(keys)  # Convert generator to list
        if not keys:
            return 0
        safe_keys = [self.make_and_validate_key(key, version=version) for key in keys]
        return await self._cache.adelete_many(safe_keys)

    @omit_exception(return_value=False)
    @override
    def clear(self) -> bool:
        """Delete all keys in this cache's namespace (prefix + version).

        Unlike Django's default ``RedisCache.clear()`` which calls ``FLUSHDB``
        and destroys *all* data in the Redis database, this only removes keys
        belonging to this cache instance. Safe when multiple apps share a
        Redis database.

        To flush the entire database, use ``cache.flush_db()``.
        """
        return self.delete_pattern("*") >= 0

    @omit_exception(return_value=False)
    @override
    async def aclear(self) -> bool:
        """Delete all keys in this cache's namespace asynchronously."""
        return (await self.adelete_pattern("*")) >= 0

    @override
    def close(self, **kwargs: Any) -> None:
        """Delegate to the client. By default a no-op (pools live for the instance's lifetime)."""
        self._cache.close(**kwargs)

    @override
    async def aclose(self, **kwargs: Any) -> None:
        """Delegate to the client. By default a no-op (pools live for the instance's lifetime)."""
        await self._cache.aclose(**kwargs)

    # =========================================================================
    # Extended Methods (beyond Django's BaseCache)
    # =========================================================================

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get TTL in seconds. Returns None if no expiry, -2 if key doesn't exist."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.ttl(key)

    async def attl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get TTL in seconds asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.attl(key)

    def pttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get TTL in milliseconds. Returns None if no expiry, -2 if key doesn't exist."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.pttl(key)

    async def apttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get TTL in milliseconds asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.apttl(key)

    def type(self, key: KeyT, version: int | None = None) -> KeyType | None:
        """Get the Redis data type of a key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.type(key)

    async def atype(self, key: KeyT, version: int | None = None) -> KeyType | None:
        """Get the Redis data type of a key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.atype(key)

    def persist(self, key: KeyT, version: int | None = None) -> bool:
        """Remove the expiry from a key, making it persistent."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.persist(key)

    async def apersist(self, key: KeyT, version: int | None = None) -> bool:
        """Remove the expiry from a key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.apersist(key)

    def expire(
        self,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry time on a key in seconds."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.expire(key, timeout)

    async def aexpire(
        self,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry time on a key in seconds asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.aexpire(key, timeout)

    def expire_at(
        self,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry to an absolute time."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.expireat(key, when)

    async def aexpire_at(
        self,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry to an absolute time asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.aexpireat(key, when)

    def pexpire(
        self,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry time on a key in milliseconds."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.pexpire(key, timeout)

    async def apexpire(
        self,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry time on a key in milliseconds asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.apexpire(key, timeout)

    def pexpire_at(
        self,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry to an absolute time with millisecond precision."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.pexpireat(key, when)

    async def apexpire_at(
        self,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
    ) -> bool:
        """Set expiry to an absolute time with millisecond precision asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.apexpireat(key, when)

    def expiretime(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the absolute Unix timestamp (seconds) when a key will expire.

        Returns None if the key has no expiry, -2 if the key doesn't exist.
        Requires Redis 7.0+ / Valkey 7.2+.
        """
        key = self.make_and_validate_key(key, version=version)
        return self._cache.expiretime(key)

    async def aexpiretime(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the absolute Unix timestamp (seconds) when a key will expire asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.aexpiretime(key)

    def keys(
        self,
        pattern: str = "*",
        version: int | None = None,
    ) -> list[str]:
        """Return all keys matching pattern (returns original keys without prefix)."""
        full_pattern = self.make_pattern(pattern, version=version)
        raw_keys = self._cache.keys(full_pattern)
        return [self.reverse_key(k) for k in raw_keys]

    async def akeys(
        self,
        pattern: str = "*",
        version: int | None = None,
    ) -> list[str]:
        """Return all keys matching pattern asynchronously."""
        full_pattern = self.make_pattern(pattern, version=version)
        raw_keys = await self._cache.akeys(full_pattern)
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

    async def aiter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> AsyncIterator[str]:
        """Iterate over keys matching pattern using SCAN asynchronously."""
        full_pattern = self.make_pattern(pattern, version=version)
        async for key in self._cache.aiter_keys(full_pattern, itersize=itersize):
            yield self.reverse_key(key)

    def scan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]:
        """Perform a single SCAN iteration returning cursor and keys."""
        full_pattern = self.make_pattern(pattern, version=version)
        next_cursor, raw_keys = self._cache.scan(
            cursor=cursor,
            match=full_pattern,
            count=count,
            _type=key_type,
        )
        return next_cursor, [self.reverse_key(k) for k in raw_keys]

    async def ascan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]:
        """Perform a single SCAN iteration asynchronously."""
        full_pattern = self.make_pattern(pattern, version=version)
        next_cursor, raw_keys = await self._cache.ascan(
            cursor=cursor,
            match=full_pattern,
            count=count,
            _type=key_type,
        )
        return next_cursor, [self.reverse_key(k) for k in raw_keys]

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        """Delete all keys matching pattern."""
        full_pattern = self.make_pattern(pattern, version=version)
        return self._cache.delete_pattern(full_pattern, itersize=itersize)

    async def adelete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        """Delete all keys matching pattern asynchronously."""
        full_pattern = self.make_pattern(pattern, version=version)
        return await self._cache.adelete_pattern(full_pattern, itersize=itersize)

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

    def alock(
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
        """Return an async Lock object for distributed locking."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.alock(
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
        pipe._cache_version = self.version
        return pipe

    # =========================================================================
    # Destructive Operations
    # =========================================================================

    def clear_all_versions(self, itersize: int | None = None) -> int:
        """Delete all keys for this cache's prefix, across ALL versions.

        More destructive than ``clear()`` which only removes the current version.
        Useful when rotating cache versions and cleaning up old keys.
        """
        escaped_prefix = _glob_escape(self.key_prefix)
        full_pattern = self.key_func("*", escaped_prefix, "*")
        return self._cache.delete_pattern(full_pattern, itersize=itersize)

    async def aclear_all_versions(self, itersize: int | None = None) -> int:
        """Delete all keys for this cache's prefix across ALL versions (async)."""
        escaped_prefix = _glob_escape(self.key_prefix)
        full_pattern = self.key_func("*", escaped_prefix, "*")
        return await self._cache.adelete_pattern(full_pattern, itersize=itersize)

    def flush_db(self) -> bool:
        """Flush the entire Redis database (``FLUSHDB``).

        **Danger:** This removes *all* keys in the database, not just those
        belonging to this cache. Only use when this cache has a dedicated
        Redis database.
        """
        return self._cache.clear()

    async def aflush_db(self) -> bool:
        """Flush the entire Redis database asynchronously (``FLUSHDB``)."""
        return await self._cache.aclear()

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        """Set hash field(s). Use field/value, mapping, or items (flat key-value pairs)."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.hset(key, field, value, mapping=mapping, items=items)

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
        value: Any,
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

    async def ahset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        """Set hash field(s) asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahset(key, field, value, mapping=mapping, items=items)

    async def ahdel(
        self,
        key: KeyT,
        *fields: str,
        version: int | None = None,
    ) -> int:
        """Delete one or more hash fields asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahdel(key, *fields)

    async def ahlen(self, key: KeyT, version: int | None = None) -> int:
        """Get number of fields in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahlen(key)

    async def ahkeys(self, key: KeyT, version: int | None = None) -> list[str]:
        """Get all field names in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahkeys(key)

    async def ahexists(
        self,
        key: KeyT,
        field: str,
        version: int | None = None,
    ) -> bool:
        """Check if field exists in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahexists(key, field)

    async def ahget(
        self,
        key: KeyT,
        field: str,
        version: int | None = None,
    ) -> Any:
        """Get value of field in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahget(key, field)

    async def ahgetall(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> dict[str, Any]:
        """Get all fields and values in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahgetall(key)

    async def ahmget(
        self,
        key: KeyT,
        *fields: str,
        version: int | None = None,
    ) -> list[Any]:
        """Get values of multiple fields in hash asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahmget(key, *fields)

    async def ahincrby(
        self,
        key: KeyT,
        field: str,
        amount: int = 1,
        version: int | None = None,
    ) -> int:
        """Increment value of field in hash by amount asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahincrby(key, field, amount)

    async def ahincrbyfloat(
        self,
        key: KeyT,
        field: str,
        amount: float = 1.0,
        version: int | None = None,
    ) -> float:
        """Increment float value of field in hash by amount asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahincrbyfloat(key, field, amount)

    async def ahsetnx(
        self,
        key: KeyT,
        field: str,
        value: Any,
        version: int | None = None,
    ) -> bool:
        """Set field in hash only if it doesn't exist asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahsetnx(key, field, value)

    async def ahvals(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> list[Any]:
        """Get all values in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ahvals(key)

    # =========================================================================
    # List Operations
    # =========================================================================

    def lpush(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
    ) -> int:
        """Push values onto head of list at key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lpush(key, *values)

    def rpush(
        self,
        key: KeyT,
        *values: Any,
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
        """Remove and return element(s) from head of list."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lpop(key, count=count)

    def rpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from tail of list."""
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
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lpos(key, value, rank=rank, count=count, maxlen=maxlen)

    def lmove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None:
        """Atomically move an element from one list to another."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src = self.make_and_validate_key(src, version=src_ver)
        dst = self.make_and_validate_key(dst, version=dst_ver)
        return self._cache.lmove(src, dst, wherefrom, whereto)

    def lrem(
        self,
        key: KeyT,
        count: int,
        value: Any,
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
        value: Any,
        version: int | None = None,
    ) -> bool:
        """Set element at index in list."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.lset(key, index, value)

    def linsert(
        self,
        key: KeyT,
        where: str,
        pivot: Any,
        value: Any,
        version: int | None = None,
    ) -> int:
        """Insert value before or after pivot in list."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.linsert(key, where, pivot, value)

    def blpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        result = self._cache.blpop(nkeys, timeout=timeout)
        if result is None:
            return None
        # Reverse the key back to original
        return (self.reverse_key(result[0]), result[1])

    def brpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
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
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None:
        """Blocking atomically move element from one list to another."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src = self.make_and_validate_key(src, version=src_ver)
        dst = self.make_and_validate_key(dst, version=dst_ver)
        return self._cache.blmove(src, dst, timeout, wherefrom, whereto)

    async def alpush(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
    ) -> int:
        """Push values onto head of list at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.alpush(key, *values)

    async def arpush(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
    ) -> int:
        """Push values onto tail of list at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.arpush(key, *values)

    async def alpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from head of list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.alpop(key, count=count)

    async def arpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from tail of list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.arpop(key, count=count)

    async def alrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        version: int | None = None,
    ) -> list[Any]:
        """Get a range of elements from list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.alrange(key, start, end)

    async def alindex(
        self,
        key: KeyT,
        index: int,
        version: int | None = None,
    ) -> Any:
        """Get element at index in list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.alindex(key, index)

    async def allen(self, key: KeyT, version: int | None = None) -> int:
        """Get length of list at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.allen(key)

    async def alpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.alpos(key, value, rank=rank, count=count, maxlen=maxlen)

    async def almove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None:
        """Atomically move an element from one list to another asynchronously."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src = self.make_and_validate_key(src, version=src_ver)
        dst = self.make_and_validate_key(dst, version=dst_ver)
        return await self._cache.almove(src, dst, wherefrom, whereto)

    async def alrem(
        self,
        key: KeyT,
        count: int,
        value: Any,
        version: int | None = None,
    ) -> int:
        """Remove elements equal to value from list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.alrem(key, count, value)

    async def altrim(
        self,
        key: KeyT,
        start: int,
        end: int,
        version: int | None = None,
    ) -> bool:
        """Trim list to specified range asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.altrim(key, start, end)

    async def alset(
        self,
        key: KeyT,
        index: int,
        value: Any,
        version: int | None = None,
    ) -> bool:
        """Set element at index in list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.alset(key, index, value)

    async def alinsert(
        self,
        key: KeyT,
        where: str,
        pivot: Any,
        value: Any,
        version: int | None = None,
    ) -> int:
        """Insert value before or after pivot in list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.alinsert(key, where, pivot, value)

    async def ablpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list asynchronously."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        result = await self._cache.ablpop(nkeys, timeout=timeout)
        if result is None:
            return None
        return (self.reverse_key(result[0]), result[1])

    async def abrpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list asynchronously."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        result = await self._cache.abrpop(nkeys, timeout=timeout)
        if result is None:
            return None
        return (self.reverse_key(result[0]), result[1])

    async def ablmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None:
        """Blocking atomically move element from one list to another asynchronously."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src = self.make_and_validate_key(src, version=src_ver)
        dst = self.make_and_validate_key(dst, version=dst_ver)
        return await self._cache.ablmove(src, dst, timeout, wherefrom, whereto)

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(
        self,
        key: KeyT,
        *members: Any,
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
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
    ) -> _Set[Any]:
        """Return the difference between the first set and all successive sets."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return self._cache.sdiff(nkeys)

    def sdiffstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the difference of sets at dest."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        # Use specific versions if provided, otherwise fall back to version
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return self._cache.sdiffstore(dest, nkeys)

    def sinter(
        self,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
    ) -> _Set[Any]:
        """Return the intersection of all sets."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return self._cache.sinter(nkeys)

    def sinterstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the intersection of sets at dest."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        # Use specific versions if provided, otherwise fall back to version
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return self._cache.sinterstore(dest, nkeys)

    def sismember(
        self,
        key: KeyT,
        member: Any,
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
        member: Any,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Move member from one set to another."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src = self.make_and_validate_key(src, version=src_ver)
        dst = self.make_and_validate_key(dst, version=dst_ver)
        return self._cache.smove(src, dst, member)

    def spop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
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
        *members: Any,
        version: int | None = None,
    ) -> int:
        """Remove members from a set."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.srem(key, *members)

    def sunion(
        self,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
    ) -> _Set[Any]:
        """Return the union of all sets."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return self._cache.sunion(nkeys)

    def sunionstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the union of sets at dest."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        # Use specific versions if provided, otherwise fall back to version
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return self._cache.sunionstore(dest, nkeys)

    def smismember(
        self,
        key: KeyT,
        *members: Any,
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
        """Incrementally iterate over set members."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.sscan(key, cursor=cursor, match=match, count=count)

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> Iterator[Any]:
        """Iterate over set members using SSCAN."""
        key = self.make_and_validate_key(key, version=version)
        yield from self._cache.sscan_iter(key, match=match, count=count)

    async def asadd(
        self,
        key: KeyT,
        *members: Any,
        version: int | None = None,
    ) -> int:
        """Add members to a set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.asadd(key, *members)

    async def ascard(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of members in a set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.ascard(key)

    async def asdiff(
        self,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
    ) -> _Set[Any]:
        """Return the difference between the first set and all successive sets asynchronously."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return await self._cache.asdiff(nkeys)

    async def asdiffstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the difference of sets at dest asynchronously."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return await self._cache.asdiffstore(dest, nkeys)

    async def asinter(
        self,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
    ) -> _Set[Any]:
        """Return the intersection of all sets asynchronously."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return await self._cache.asinter(nkeys)

    async def asinterstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the intersection of sets at dest asynchronously."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return await self._cache.asinterstore(dest, nkeys)

    async def asismember(
        self,
        key: KeyT,
        member: Any,
        version: int | None = None,
    ) -> bool:
        """Check if member is in set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.asismember(key, member)

    async def asmembers(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> _Set[Any]:
        """Get all members of a set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.asmembers(key)

    async def asmove(
        self,
        src: KeyT,
        dst: KeyT,
        member: Any,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Move member from one set to another asynchronously."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src = self.make_and_validate_key(src, version=src_ver)
        dst = self.make_and_validate_key(dst, version=dst_ver)
        return await self._cache.asmove(src, dst, member)

    async def aspop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return random member(s) from set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.aspop(key, count)

    async def asrandmember(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any]:
        """Get random member(s) from set without removing asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.asrandmember(key, count)

    async def asrem(
        self,
        key: KeyT,
        *members: Any,
        version: int | None = None,
    ) -> int:
        """Remove members from a set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.asrem(key, *members)

    async def asunion(
        self,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
    ) -> _Set[Any]:
        """Return the union of all sets asynchronously."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return await self._cache.asunion(nkeys)

    async def asunionstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the union of sets at dest asynchronously."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return await self._cache.asunionstore(dest, nkeys)

    async def asmismember(
        self,
        key: KeyT,
        *members: Any,
        version: int | None = None,
    ) -> list[bool]:
        """Check if multiple values are members of a set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.asmismember(key, *members)

    async def asscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, _Set[Any]]:
        """Incrementally iterate over set members asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.asscan(key, cursor=cursor, match=match, count=count)

    async def asscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> AsyncIterator[Any]:
        """Iterate over set members using SSCAN asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        async for member in self._cache.asscan_iter(key, match=match, count=count):
            yield member

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
        member: Any,
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
        member: Any,
        version: int | None = None,
    ) -> int | None:
        """Get the rank of a member (0-based, lowest score first)."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zrank(key, member)

    def zrem(
        self,
        key: KeyT,
        *members: Any,
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
        member: Any,
        version: int | None = None,
    ) -> float | None:
        """Get the score of a member."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zscore(key, member)

    def zrevrank(
        self,
        key: KeyT,
        member: Any,
        version: int | None = None,
    ) -> int | None:
        """Get the rank of a member (0-based, highest score first)."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zrevrank(key, member)

    def zmscore(
        self,
        key: KeyT,
        *members: Any,
        version: int | None = None,
    ) -> list[float | None]:
        """Get the scores of multiple members."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.zmscore(key, *members)

    async def azadd(
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
        """Add members to a sorted set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azadd(key, mapping, nx=nx, xx=xx, ch=ch, gt=gt, lt=lt)

    async def azcard(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of members in a sorted set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azcard(key)

    async def azcount(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Count members with scores between min and max asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azcount(key, min_score, max_score)

    async def azincrby(
        self,
        key: KeyT,
        amount: float,
        member: Any,
        version: int | None = None,
    ) -> float:
        """Increment the score of a member asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azincrby(key, amount, member)

    async def azpopmax(
        self,
        key: KeyT,
        count: int = 1,
        version: int | None = None,
    ) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azpopmax(key, count)

    async def azpopmin(
        self,
        key: KeyT,
        count: int = 1,
        version: int | None = None,
    ) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azpopmin(key, count)

    async def azrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azrange(key, start, end, withscores=withscores)

    async def azrangebyscore(
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
        """Return members with scores between min and max asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azrangebyscore(key, min_score, max_score, withscores=withscores, start=start, num=num)

    async def azrank(
        self,
        key: KeyT,
        member: Any,
        version: int | None = None,
    ) -> int | None:
        """Get the rank of a member (0-based, lowest score first) asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azrank(key, member)

    async def azrem(
        self,
        key: KeyT,
        *members: Any,
        version: int | None = None,
    ) -> int:
        """Remove members from a sorted set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azrem(key, *members)

    async def azremrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Remove members with scores between min and max asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azremrangebyscore(key, min_score, max_score)

    async def azremrangebyrank(
        self,
        key: KeyT,
        start: int,
        end: int,
        version: int | None = None,
    ) -> int:
        """Remove members by rank range asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azremrangebyrank(key, start, end)

    async def azrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index, highest to lowest, asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azrevrange(key, start, end, withscores=withscores)

    async def azrevrangebyscore(
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
        """Return members with scores between max and min, highest first, asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azrevrangebyscore(
            key,
            max_score,
            min_score,
            withscores=withscores,
            start=start,
            num=num,
        )

    async def azscore(
        self,
        key: KeyT,
        member: Any,
        version: int | None = None,
    ) -> float | None:
        """Get the score of a member asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azscore(key, member)

    async def azrevrank(
        self,
        key: KeyT,
        member: Any,
        version: int | None = None,
    ) -> int | None:
        """Get the rank of a member (0-based, highest score first) asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azrevrank(key, member)

    async def azmscore(
        self,
        key: KeyT,
        *members: Any,
        version: int | None = None,
    ) -> list[float | None]:
        """Get the scores of multiple members asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.azmscore(key, *members)

    # =========================================================================
    # Stream Operations
    # =========================================================================

    def xadd(
        self,
        key: KeyT,
        fields: dict[str, Any],
        entry_id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> str:
        """Add an entry to a stream."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xadd(
            key,
            fields,
            entry_id,
            maxlen=maxlen,
            approximate=approximate,
            nomkstream=nomkstream,
            minid=minid,
            limit=limit,
        )

    async def axadd(
        self,
        key: KeyT,
        fields: dict[str, Any],
        entry_id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> str:
        """Add an entry to a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axadd(
            key,
            fields,
            entry_id,
            maxlen=maxlen,
            approximate=approximate,
            nomkstream=nomkstream,
            minid=minid,
            limit=limit,
        )

    def xlen(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of entries in a stream."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xlen(key)

    async def axlen(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of entries in a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axlen(key)

    def xrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in ascending order."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xrange(key, start, end, count=count)

    async def axrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in ascending order asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axrange(key, start, end, count=count)

    def xrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in descending order."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xrevrange(key, end, start, count=count)

    async def axrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in descending order asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axrevrange(key, end, start, count=count)

    def xread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from one or more streams."""
        nstreams: dict[KeyT, str] = {self.make_and_validate_key(k, version=version): v for k, v in streams.items()}
        return self._cache.xread(nstreams, count=count, block=block)

    async def axread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from one or more streams asynchronously."""
        nstreams: dict[KeyT, str] = {self.make_and_validate_key(k, version=version): v for k, v in streams.items()}
        return await self._cache.axread(nstreams, count=count, block=block)

    def xtrim(
        self,
        key: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> int:
        """Trim a stream to a maximum length or minimum ID."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xtrim(key, maxlen=maxlen, approximate=approximate, minid=minid, limit=limit)

    async def axtrim(
        self,
        key: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> int:
        """Trim a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axtrim(key, maxlen=maxlen, approximate=approximate, minid=minid, limit=limit)

    def xdel(self, key: KeyT, *entry_ids: str, version: int | None = None) -> int:
        """Delete entries from a stream."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xdel(key, *entry_ids)

    async def axdel(self, key: KeyT, *entry_ids: str, version: int | None = None) -> int:
        """Delete entries from a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axdel(key, *entry_ids)

    def xinfo_stream(self, key: KeyT, full: bool = False, version: int | None = None) -> dict[str, Any]:
        """Get information about a stream."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xinfo_stream(key, full=full)

    async def axinfo_stream(self, key: KeyT, full: bool = False, version: int | None = None) -> dict[str, Any]:
        """Get information about a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axinfo_stream(key, full=full)

    def xinfo_groups(self, key: KeyT, version: int | None = None) -> list[dict[str, Any]]:
        """Get information about consumer groups for a stream."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xinfo_groups(key)

    async def axinfo_groups(self, key: KeyT, version: int | None = None) -> list[dict[str, Any]]:
        """Get information about consumer groups for a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axinfo_groups(key)

    def xinfo_consumers(self, key: KeyT, group: str, version: int | None = None) -> list[dict[str, Any]]:
        """Get information about consumers in a group."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xinfo_consumers(key, group)

    async def axinfo_consumers(self, key: KeyT, group: str, version: int | None = None) -> list[dict[str, Any]]:
        """Get information about consumers in a group asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axinfo_consumers(key, group)

    def xgroup_create(
        self,
        key: KeyT,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool:
        """Create a consumer group."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xgroup_create(key, group, entry_id, mkstream=mkstream, entries_read=entries_read)

    async def axgroup_create(
        self,
        key: KeyT,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool:
        """Create a consumer group asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axgroup_create(key, group, entry_id, mkstream=mkstream, entries_read=entries_read)

    def xgroup_destroy(self, key: KeyT, group: str, version: int | None = None) -> int:
        """Destroy a consumer group."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xgroup_destroy(key, group)

    async def axgroup_destroy(self, key: KeyT, group: str, version: int | None = None) -> int:
        """Destroy a consumer group asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axgroup_destroy(key, group)

    def xgroup_setid(
        self,
        key: KeyT,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool:
        """Set the last delivered ID for a consumer group."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xgroup_setid(key, group, entry_id, entries_read=entries_read)

    async def axgroup_setid(
        self,
        key: KeyT,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool:
        """Set the last delivered ID for a consumer group asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axgroup_setid(key, group, entry_id, entries_read=entries_read)

    def xgroup_delconsumer(self, key: KeyT, group: str, consumer: str, version: int | None = None) -> int:
        """Remove a consumer from a group."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xgroup_delconsumer(key, group, consumer)

    async def axgroup_delconsumer(self, key: KeyT, group: str, consumer: str, version: int | None = None) -> int:
        """Remove a consumer from a group asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axgroup_delconsumer(key, group, consumer)

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from streams as a consumer group member."""
        nstreams: dict[KeyT, str] = {self.make_and_validate_key(k, version=version): v for k, v in streams.items()}
        return self._cache.xreadgroup(group, consumer, nstreams, count=count, block=block, noack=noack)

    async def axreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from streams as a consumer group member asynchronously."""
        nstreams: dict[KeyT, str] = {self.make_and_validate_key(k, version=version): v for k, v in streams.items()}
        return await self._cache.axreadgroup(group, consumer, nstreams, count=count, block=block, noack=noack)

    def xack(self, key: KeyT, group: str, *entry_ids: str, version: int | None = None) -> int:
        """Acknowledge message processing."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xack(key, group, *entry_ids)

    async def axack(self, key: KeyT, group: str, *entry_ids: str, version: int | None = None) -> int:
        """Acknowledge message processing asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axack(key, group, *entry_ids)

    def xpending(
        self,
        key: KeyT,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
        version: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Get pending entries information."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xpending(key, group, start=start, end=end, count=count, consumer=consumer, idle=idle)

    async def axpending(
        self,
        key: KeyT,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
        version: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Get pending entries information asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axpending(key, group, start=start, end=end, count=count, consumer=consumer, idle=idle)

    def xclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        entry_ids: list[str],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]] | list[str]:
        """Claim pending messages."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xclaim(
            key,
            group,
            consumer,
            min_idle_time,
            entry_ids,
            idle=idle,
            time=time,
            retrycount=retrycount,
            force=force,
            justid=justid,
        )

    async def axclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        entry_ids: list[str],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]] | list[str]:
        """Claim pending messages asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axclaim(
            key,
            group,
            consumer,
            min_idle_time,
            entry_ids,
            idle=idle,
            time=time,
            retrycount=retrycount,
            force=force,
            justid=justid,
        )

    def xautoclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
        version: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]] | list[str], list[str]]:
        """Auto-claim pending messages that have been idle."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.xautoclaim(key, group, consumer, min_idle_time, start_id, count=count, justid=justid)

    async def axautoclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
        version: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]] | list[str], list[str]]:
        """Auto-claim pending messages asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.axautoclaim(key, group, consumer, min_idle_time, start_id, count=count, justid=justid)

    # =========================================================================
    # Direct Client Access
    # =========================================================================

    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the underlying Redis client."""
        return self._cache.get_client(key, write=write)

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get server information and statistics.

        Returns the Redis/Valkey INFO command output as a dictionary.
        Optionally filter by section (e.g., 'server', 'memory', 'stats').
        """
        if section:
            return dict(self._cache.info(section))
        return dict(self._cache.info())

    def slowlog_get(self, count: int = 10) -> list[Any]:
        """Get slow query log entries.

        Returns up to ``count`` entries from the Redis SLOWLOG.
        """
        return list(self._cache.slowlog_get(count))

    def slowlog_len(self) -> int:
        """Get the number of entries in the slow query log."""
        return int(self._cache.slowlog_len())

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
        """Rename a key atomically."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src_key = self.make_and_validate_key(src, version=src_ver)
        dst_key = self.make_and_validate_key(dst, version=dst_ver)
        return self._cache.rename(src_key, dst_key)

    async def arename(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key atomically asynchronously."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src_key = self.make_and_validate_key(src, version=src_ver)
        dst_key = self.make_and_validate_key(dst, version=dst_ver)
        return await self._cache.arename(src_key, dst_key)

    def renamenx(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key only if the destination does not exist."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src_key = self.make_and_validate_key(src, version=src_ver)
        dst_key = self.make_and_validate_key(dst, version=dst_ver)
        return self._cache.renamenx(src_key, dst_key)

    async def arenamenx(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key only if the destination does not exist asynchronously."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src_key = self.make_and_validate_key(src, version=src_ver)
        dst_key = self.make_and_validate_key(dst, version=dst_ver)
        return await self._cache.arenamenx(src_key, dst_key)

    @override
    def incr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Atomically increment the version of a key using RENAME."""
        if version is None:
            version = self.version
        old_key = self.make_and_validate_key(key, version=version)
        new_key = self.make_and_validate_key(key, version=version + delta)
        self._cache.rename(old_key, new_key)
        return version + delta

    @override
    def decr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Atomically decrement the version of a key."""
        return self.incr_version(key, -delta, version)

    @override
    async def aincr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Atomically increment the version of a key using RENAME asynchronously."""
        if version is None:
            version = self.version
        old_key = self.make_and_validate_key(key, version=version)
        new_key = self.make_and_validate_key(key, version=version + delta)
        await self._cache.arename(old_key, new_key)
        return version + delta

    @override
    async def adecr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Atomically decrement the version of a key asynchronously."""
        return await self.aincr_version(key, -delta, version)

    # =========================================================================
    # Lua Script Operations
    # =========================================================================

    def _create_script_helpers(self, version: int | None) -> ScriptHelpers:
        """Create a ScriptHelpers instance for script processing."""
        return ScriptHelpers(
            make_key=self.make_and_validate_key,
            encode=self._cache.encode,
            decode=self._cache.decode,
            version=version if version is not None else self.version,
        )

    def eval_script(
        self,
        script: str,
        *,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        pre_hook: Callable[[ScriptHelpers, Sequence[Any], Sequence[Any]], tuple[list[Any], list[Any]]] | None = None,
        post_hook: Callable[[ScriptHelpers, Any], Any] | None = None,
        version: int | None = None,
    ) -> Any:
        """Execute a Lua script.

        Args:
            script: Lua script source code.
            keys: KEYS to pass to the script.
            args: ARGV to pass to the script.
            pre_hook: Pre-processing hook: ``(helpers, keys, args) -> (keys, args)``.
            post_hook: Post-processing hook: ``(helpers, result) -> result``.
            version: Key version for prefixing.
        """
        helpers = self._create_script_helpers(version)

        proc_keys: list[Any] = list(keys)
        proc_args: list[Any] = list(args)
        if pre_hook is not None:
            proc_keys, proc_args = pre_hook(helpers, proc_keys, proc_args)

        result = self._cache.eval(script, len(proc_keys), *proc_keys, *proc_args)

        if post_hook is not None:
            result = post_hook(helpers, result)

        return result

    async def aeval_script(
        self,
        script: str,
        *,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        pre_hook: Callable[[ScriptHelpers, Sequence[Any], Sequence[Any]], tuple[list[Any], list[Any]]] | None = None,
        post_hook: Callable[[ScriptHelpers, Any], Any] | None = None,
        version: int | None = None,
    ) -> Any:
        """Execute a Lua script asynchronously.

        Args:
            script: Lua script source code.
            keys: KEYS to pass to the script.
            args: ARGV to pass to the script.
            pre_hook: Pre-processing hook: ``(helpers, keys, args) -> (keys, args)``.
            post_hook: Post-processing hook: ``(helpers, result) -> result``.
            version: Key version for prefixing.
        """
        helpers = self._create_script_helpers(version)

        proc_keys: list[Any] = list(keys)
        proc_args: list[Any] = list(args)
        if pre_hook is not None:
            proc_keys, proc_args = pre_hook(helpers, proc_keys, proc_args)

        result = await self._cache.aeval(script, len(proc_keys), *proc_keys, *proc_args)

        if post_hook is not None:
            result = post_hook(helpers, result)

        return result


# =============================================================================
# RedisCache - concrete implementation for redis-py
# =============================================================================


class RedisCache(KeyValueCache):
    """Django cache backend using the redis-py library."""

    _class = RedisCacheClient


# =============================================================================
# ValkeyCache - concrete implementation for valkey-py
# =============================================================================


class ValkeyCache(KeyValueCache):
    """Django cache backend using the valkey-py library."""

    _class = ValkeyCacheClient


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "KeyValueCache",
    "RedisCache",
    "ValkeyCache",
]
