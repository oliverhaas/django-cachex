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

import logging
import re
from functools import cached_property
from typing import TYPE_CHECKING, Any, override

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.utils.module_loading import import_string

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from django_cachex.client.pipeline import Pipeline
    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT

from django_cachex.client.default import (
    KeyValueCacheClient,
    RedisCacheClient,
    ValkeyCacheClient,
)
from django_cachex.exceptions import ScriptNotRegisteredError
from django_cachex.omit_exception import aomit_exception, omit_exception
from django_cachex.script import LuaScript, ScriptHelpers

# Sentinel value for methods with dynamic return values (e.g., get() returns default arg)
CONNECTION_INTERRUPTED = object()

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
    """Django cache backend for Redis/Valkey with extended features.

    This is the base class for all django-cachex cache backends. It extends
    Django's ``BaseCache`` with Redis/Valkey specific features while maintaining
    full compatibility with Django's cache API.

    Features:
        - **Full Django Cache API**: All standard methods (get, set, delete, etc.)
          plus their async variants (aget, aset, adelete, etc.)
        - **Multi-serializer fallback**: Safely migrate between serialization formats
        - **Compression**: Optional compression with multiple algorithm support
        - **Extended operations**: Redis data structures (hashes, lists, sets,
          sorted sets) with automatic serialization
        - **TTL operations**: Query and modify key expiration times
        - **Pattern operations**: Find and delete keys by pattern
        - **Distributed locking**: Redis-based locks for distributed systems
        - **Pipeline support**: Batch operations for improved performance
        - **Master-replica support**: Configure multiple servers for read scaling

    Usage:
        Don't use this class directly. Use ``RedisCache`` or ``ValkeyCache``::

            # settings.py
            CACHES = {
                "default": {
                    "BACKEND": "django_cachex.cache.RedisCache",
                    "LOCATION": "redis://127.0.0.1:6379/1",
                    "OPTIONS": {
                        "serializer": "django_cachex.serializers.pickle.PickleSerializer",
                        "compressor": "django_cachex.compressors.zstd.ZstdCompressor",
                    }
                }
            }

        Then use via Django's cache framework::

            from django.core.cache import cache

            # Standard operations
            cache.set("key", {"data": "value"}, timeout=300)
            value = cache.get("key")

            # Async operations (Django 4.0+)
            await cache.aset("key", "value")
            value = await cache.aget("key")

            # Extended operations
            cache.hset("user:1", "name", "Alice")
            cache.lpush("queue", "task1", "task2")
            cache.sadd("tags", "python", "django")

    Attributes:
        _servers: List of server URLs (first is master, rest are replicas).
        _options: Configuration options from Django's CACHES setting.
        _class: The CacheClient class to use (set by subclasses).
        _cache: Lazily-initialized CacheClient instance.

    See Also:
        - ``RedisCache``: For redis-py library
        - ``ValkeyCache``: For valkey-py library
        - ``RedisClusterCache``: For Redis Cluster mode
        - ``RedisSentinelCache``: For Redis Sentinel mode
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

        # Lua script registry
        self._scripts: dict[str, LuaScript] = {}

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

    @aomit_exception(return_value=False)
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

    @aomit_exception(return_value=CONNECTION_INTERRUPTED)
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

    @aomit_exception
    @override
    async def aset(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> None:
        """Set a value in the cache asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        await self._cache.aset(key, value, self.get_backend_timeout(timeout))

    @omit_exception
    @override
    def set(
        self,
        key: KeyT,
        value: Any,
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

    @omit_exception(return_value=False)
    @override
    def touch(self, key: KeyT, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        """Update the timeout on a key."""
        key = self.make_and_validate_key(key, version=version)
        return self._cache.touch(key, self.get_backend_timeout(timeout))

    @aomit_exception(return_value=False)
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

    @aomit_exception(return_value=False)
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

    @aomit_exception(return_value={})
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

    @aomit_exception(return_value=False)
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

    @aomit_exception(return_value=0)
    @override
    async def aincr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Increment a value asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self._cache.aincr(key, delta)

    @aomit_exception(return_value=0)
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
        """Fetch a given key from the cache. If the key does not exist,
        add the key and set it to the default value.

        The default value can also be any callable. If timeout is given,
        use that timeout for the key; otherwise use the default cache timeout.

        Return the value of the key stored or retrieved.
        """
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
        """Fetch a given key from the cache asynchronously. If the key does not exist,
        add the key and set it to the default value.

        The default value can also be any callable. If timeout is given,
        use that timeout for the key; otherwise use the default cache timeout.

        Return the value of the key stored or retrieved.
        """
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

    @aomit_exception(return_value=[])
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
        """Delete multiple keys from the cache.

        Extended to return the count of deleted keys (Django's returns None).
        """
        keys = list(keys)  # Convert generator to list
        if not keys:
            return 0
        safe_keys = [self.make_and_validate_key(key, version=version) for key in keys]
        return self._cache.delete_many(safe_keys)

    @aomit_exception(return_value=0)
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
        """Flush the database."""
        return self._cache.clear()

    @aomit_exception(return_value=False)
    @override
    async def aclear(self) -> bool:
        """Flush the database asynchronously."""
        return await self._cache.aclear()

    @override
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
        # Set scripts registry for eval_script support
        pipe._scripts = self._scripts
        pipe._cache_version = self.version
        return pipe

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(
        self,
        key: KeyT,
        field: str,
        value: Any,
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
        mapping: Mapping[str, Any],
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
        value: Any,
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
        *members: Any,
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

    @override
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

    @override
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

    @override
    async def aincr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Atomically increment the version of a key using RENAME asynchronously.

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
        await self._cache.arename(old_key, new_key)
        return version + delta

    @override
    async def adecr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        """Atomically decrement the version of a key asynchronously.

        This is a convenience method that calls aincr_version with a negative delta.

        Args:
            key: The cache key
            delta: Amount to decrement version by (default 1)
            version: Current version (defaults to cache's default version)

        Returns:
            The new version number

        Raises:
            ValueError: If the key does not exist
        """
        return await self.aincr_version(key, -delta, version)

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
        """Register a Lua script for later execution.

        Scripts are registered per-cache-instance, allowing different caches
        to use different encoders and key prefixes.

        Args:
            name: Unique name for the script within this cache instance.
            script: Lua script source code.
            num_keys: Expected number of KEYS arguments (for documentation only).
            pre_func: Optional function to process keys/args before execution.
                Signature: ``(helpers, keys, args) -> (processed_keys, processed_args)``
            post_func: Optional function to process result after execution.
                Signature: ``(helpers, result) -> processed_result``

        Returns:
            The registered LuaScript object.

        Example:
            Simple script (no encoding needed)::

                from django_cachex.script import keys_only_pre

                cache.register_script(
                    "rate_limit",
                    '''
                    local current = redis.call('INCR', KEYS[1])
                    if current == 1 then
                        redis.call('EXPIRE', KEYS[1], ARGV[1])
                    end
                    return current
                    ''',
                    num_keys=1,
                    pre_func=keys_only_pre,
                )

            Script with encoding::

                from django_cachex.script import full_encode_pre, decode_single_post

                cache.register_script(
                    "get_and_set",
                    '''
                    local old = redis.call('GET', KEYS[1])
                    redis.call('SET', KEYS[1], ARGV[1])
                    return old
                    ''',
                    pre_func=full_encode_pre,
                    post_func=decode_single_post,
                )
        """
        lua_script = LuaScript(
            name=name,
            script=script,
            num_keys=num_keys,
            pre_func=pre_func,
            post_func=post_func,
        )
        self._scripts[name] = lua_script
        return lua_script

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
        name: str,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        *,
        version: int | None = None,
    ) -> Any:
        """Execute a registered Lua script.

        Args:
            name: Name of the registered script.
            keys: KEYS to pass to the script.
            args: ARGV to pass to the script.
            version: Key version for prefixing.

        Returns:
            Script result, processed by post_func if defined.

        Raises:
            ScriptNotRegisteredError: If script name is not registered.

        Example:
            Execute a previously registered script::

                count = cache.eval_script("rate_limit", keys=["user:123:req"], args=[60])
                if count > 100:
                    raise RateLimitExceeded()
        """
        if name not in self._scripts:
            raise ScriptNotRegisteredError(name)

        script = self._scripts[name]
        helpers = self._create_script_helpers(version)

        # Process keys and args through pre_func
        proc_keys: list[Any] = list(keys)
        proc_args: list[Any] = list(args)
        if script.pre_func is not None:
            proc_keys, proc_args = script.pre_func(helpers, proc_keys, proc_args)

        # Ensure SHA is cached
        if script._sha is None:
            script._sha = self._cache.script_load(script.script)

        # Execute via evalsha with NOSCRIPT fallback
        try:
            result = self._cache.evalsha(script._sha, len(proc_keys), *proc_keys, *proc_args)
        except Exception as e:
            # Check for NOSCRIPT error and reload
            # NoScriptError may be wrapped in ConnectionInterruptedError
            err_str = str(e)
            if "NOSCRIPT" in err_str or "NoScriptError" in err_str:
                script._sha = self._cache.script_load(script.script)
                result = self._cache.evalsha(script._sha, len(proc_keys), *proc_keys, *proc_args)
            else:
                raise

        # Process result through post_func
        if script.post_func is not None:
            result = script.post_func(helpers, result)

        return result

    async def aeval_script(
        self,
        name: str,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        *,
        version: int | None = None,
    ) -> Any:
        """Execute a registered Lua script asynchronously.

        Args:
            name: Name of the registered script.
            keys: KEYS to pass to the script.
            args: ARGV to pass to the script.
            version: Key version for prefixing.

        Returns:
            Script result, processed by post_func if defined.

        Raises:
            ScriptNotRegisteredError: If script name is not registered.

        Example:
            Execute a script asynchronously::

                count = await cache.aeval_script("rate_limit", keys=["user:123:req"], args=[60])
        """
        if name not in self._scripts:
            raise ScriptNotRegisteredError(name)

        script = self._scripts[name]
        helpers = self._create_script_helpers(version)

        # Process keys and args through pre_func
        proc_keys: list[Any] = list(keys)
        proc_args: list[Any] = list(args)
        if script.pre_func is not None:
            proc_keys, proc_args = script.pre_func(helpers, proc_keys, proc_args)

        # Ensure SHA is cached
        if script._sha is None:
            script._sha = await self._cache.ascript_load(script.script)

        # Execute via evalsha with NOSCRIPT fallback
        try:
            result = await self._cache.aevalsha(script._sha, len(proc_keys), *proc_keys, *proc_args)
        except Exception as e:
            # Check for NOSCRIPT error and reload
            # NoScriptError may be wrapped in ConnectionInterruptedError
            err_str = str(e)
            if "NOSCRIPT" in err_str or "NoScriptError" in err_str:
                script._sha = await self._cache.ascript_load(script.script)
                result = await self._cache.aevalsha(script._sha, len(proc_keys), *proc_keys, *proc_args)
            else:
                raise

        # Process result through post_func
        if script.post_func is not None:
            result = script.post_func(helpers, result)

        return result


# =============================================================================
# RedisCache - concrete implementation for redis-py
# =============================================================================


class RedisCache(KeyValueCache):
    """Django cache backend using the redis-py library.

    This is the primary cache backend for connecting to Redis servers.
    It provides all features of ``KeyValueCache`` using the official
    redis-py client library.

    Requirements:
        Requires redis-py to be installed::

            pip install redis

    Example:
        Basic configuration::

            CACHES = {
                "default": {
                    "BACKEND": "django_cachex.cache.RedisCache",
                    "LOCATION": "redis://127.0.0.1:6379/1",
                }
            }

        With authentication and options::

            CACHES = {
                "default": {
                    "BACKEND": "django_cachex.cache.RedisCache",
                    "LOCATION": "redis://username:password@hostname:6379/1",
                    "OPTIONS": {
                        "max_connections": 50,
                        "socket_timeout": 5,
                        "serializer": "django_cachex.serializers.json.JSONSerializer",
                    }
                }
            }

        Master-replica setup for read scaling::

            CACHES = {
                "default": {
                    "BACKEND": "django_cachex.cache.RedisCache",
                    "LOCATION": [
                        "redis://master:6379/1",
                        "redis://replica1:6379/1",
                        "redis://replica2:6379/1",
                    ],
                }
            }

    See Also:
        - ``ValkeyCache``: For Valkey servers using valkey-py
        - ``RedisClusterCache``: For Redis Cluster mode
        - ``RedisSentinelCache``: For Redis Sentinel high availability
    """

    _class = RedisCacheClient


# =============================================================================
# ValkeyCache - concrete implementation for valkey-py
# =============================================================================


class ValkeyCache(KeyValueCache):
    """Django cache backend using the valkey-py library.

    This cache backend is for connecting to Valkey servers using the
    official valkey-py client library. Valkey is an open-source,
    Redis-compatible key-value store.

    Requirements:
        Requires valkey-py to be installed::

            pip install valkey

    Example:
        Basic configuration::

            CACHES = {
                "default": {
                    "BACKEND": "django_cachex.cache.ValkeyCache",
                    "LOCATION": "valkey://127.0.0.1:6379/1",
                }
            }

        With options::

            CACHES = {
                "default": {
                    "BACKEND": "django_cachex.cache.ValkeyCache",
                    "LOCATION": "valkey://hostname:6379/1",
                    "OPTIONS": {
                        "max_connections": 50,
                        "serializer": "django_cachex.serializers.pickle.PickleSerializer",
                    }
                }
            }

    Note:
        Valkey is wire-protocol compatible with Redis, so you can also use
        ``RedisCache`` with redis-py to connect to Valkey servers if preferred.

    See Also:
        - ``RedisCache``: For Redis servers using redis-py
        - ``ValkeyClusterCache``: For Valkey Cluster mode
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
