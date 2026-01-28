"""Cache client classes for Redis-compatible backends.

This module provides cache client classes that replicate Django's RedisCacheClient
structure but with configurable library selection via class attributes.

Architecture:
- KeyValueCacheClient: Base class with all logic, library-agnostic
- RedisCacheClient: Sets class attributes for redis-py
- ValkeyCacheClient: Sets class attributes for valkey-py

The class attributes pattern allows subclasses to swap the underlying library
while inheriting all the extended functionality.

Internal attributes (matching Django's RedisCacheClient):
- _lib: The library module (redis or valkey)
- _servers: List of server URLs
- _pools: Dict of connection pools by index
- _client: The client class (Redis or Valkey)
- _pool_class: The connection pool class
- _pool_options: Options passed to connection pool

Extended attributes (our additions):
- _options: Full options dict
- _compressors: List of compressor instances
- _serializers: List of serializer instances (for fallback)
"""

from __future__ import annotations

import asyncio
import logging
import socket
import weakref
from typing import TYPE_CHECKING, Any, cast

from django.utils.module_loading import import_string

from django_cachex.compat import create_compressor, create_serializer
from django_cachex.exceptions import CompressorError, ConnectionInterruptedError, SerializerError

# Alias builtin set type to avoid shadowing by the set() method
_Set = set

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence

    from django_cachex.client.pipeline import Pipeline
    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT

# Try to import redis-py and/or valkey-py
_REDIS_AVAILABLE = False
_VALKEY_AVAILABLE = False
_exception_list: list[type[Exception]] = [socket.timeout]

try:
    import redis
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import RedisClusterException
    from redis.exceptions import ResponseError as RedisResponseError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    _REDIS_AVAILABLE = True
    _exception_list.extend([RedisConnectionError, RedisTimeoutError, RedisResponseError, RedisClusterException])
except ImportError:
    redis = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]

try:
    import valkey
    from valkey.exceptions import ConnectionError as ValkeyConnectionError
    from valkey.exceptions import ResponseError as ValkeyResponseError
    from valkey.exceptions import TimeoutError as ValkeyTimeoutError

    _VALKEY_AVAILABLE = True
    _exception_list.extend([ValkeyConnectionError, ValkeyTimeoutError, ValkeyResponseError])
except ImportError:
    valkey = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]

_main_exceptions = tuple(_exception_list)

# ResponseError tuple for incr handling
_response_errors: list[type[Exception]] = []
if _REDIS_AVAILABLE:
    _response_errors.append(RedisResponseError)
if _VALKEY_AVAILABLE:
    _response_errors.append(ValkeyResponseError)
_ResponseError = tuple(_response_errors) if _response_errors else (Exception,)

logger = logging.getLogger(__name__)


# =============================================================================
# KeyValueCacheClient - base class (library-agnostic)
# =============================================================================


class KeyValueCacheClient:
    """Base cache client class with configurable library.

    This class replicates Django's RedisCacheClient structure but uses class
    attributes to allow subclasses to swap the underlying library.

    Subclasses must set:
    - _lib: The library module (e.g., valkey or redis)
    - _client_class: The client class (e.g., valkey.Valkey)
    - _pool_class: The connection pool class

    Internal attributes match Django's RedisCacheClient for compatibility.
    """

    # Class attributes - subclasses override these
    _lib: Any = None  # The library module
    _client_class: type | None = None  # e.g., valkey.Valkey
    _pool_class: type | None = None  # e.g., valkey.ConnectionPool
    _async_client_class: type | None = None  # e.g., valkey.asyncio.Valkey
    _async_pool_class: type | None = None  # e.g., valkey.asyncio.ConnectionPool

    # Default scan iteration batch size
    _default_scan_itersize: int = 100

    # Options that shouldn't be passed to the connection pool
    _CLIENT_ONLY_OPTIONS = frozenset(
        {
            "compressor",
            "serializer",
            "ignore_exceptions",
            "log_ignored_exceptions",
            "close_connection",
            "sentinels",
            "sentinel_kwargs",
            "async_pool_class",
            "reverse_key_function",
        },
    )

    def __init__(
        self,
        servers: list[str],
        serializer: str | list | type | None = None,
        pool_class: str | type | None = None,
        parser_class: str | type | None = None,
        async_pool_class: str | type | None = None,
        **options: Any,
    ) -> None:
        """Initialize the cache client.

        Args:
            servers: List of server URLs
            serializer: Serializer instance or import path (Django compatibility)
            pool_class: Connection pool class or import path
            parser_class: Parser class or import path
            async_pool_class: Async connection pool class or import path
            **options: Additional options passed to connection pool
        """
        # Store servers
        self._servers = servers
        self._pools: dict[int, Any] = {}

        # Async pools: WeakKeyDictionary keyed by event loop -> {server_index: pool}
        # Using WeakKeyDictionary ensures automatic cleanup when the event loop is GC'd
        self._async_pools: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[int, Any]] = (
            weakref.WeakKeyDictionary()
        )

        # Set up pool class (can be overridden via argument)
        if isinstance(pool_class, str):
            pool_class = import_string(pool_class)
        self._pool_class = pool_class or self.__class__._pool_class  # type: ignore[assignment]

        # Set up async pool class (can be overridden via argument)
        if isinstance(async_pool_class, str):
            async_pool_class = import_string(async_pool_class)
        self._async_pool_class = async_pool_class or self.__class__._async_pool_class  # type: ignore[assignment]

        # Set up parser class
        if isinstance(parser_class, str):
            parser_class = import_string(parser_class)
        if parser_class is None and self._lib is not None:
            parser_class = self._lib.connection.DefaultParser

        # Build pool options (filter out client-only options)
        self._pool_options = {"parser_class": parser_class}
        for key, value in options.items():
            if key not in self._CLIENT_ONLY_OPTIONS:
                self._pool_options[key] = value

        # Store full options for our extensions
        self._options = options

        # Setup compressors (extension beyond Django)
        compressor_config = options.get("compressor")
        self._compressors = self._create_compressors(compressor_config)

        # Setup multi-serializer fallback (extension beyond Django)
        # Use the explicit serializer parameter if provided, otherwise check options
        serializer_config = (
            serializer
            if serializer is not None
            else options.get("serializer", "django_cachex.serializers.pickle.PickleSerializer")
        )
        self._serializers = self._create_serializers(serializer_config)

    # =========================================================================
    # Serializer/Compressor Setup
    # =========================================================================

    def _create_serializers(self, config: str | list | type | Any) -> list:
        """Create serializer instance(s) from config."""
        if isinstance(config, list):
            return [create_serializer(item) for item in config]
        return [create_serializer(config)]

    def _create_compressors(self, config: str | list | type | Any | None) -> list:
        """Create compressor instance(s) from config."""
        if config is None:
            return []
        if isinstance(config, list):
            return [create_compressor(item) for item in config]
        return [create_compressor(config)]

    def _decompress(self, value: bytes) -> bytes:
        """Decompress with fallback support for multiple compressors."""
        for compressor in self._compressors:
            try:
                return compressor.decompress(value)
            except CompressorError:
                continue
        return value

    def _deserialize(self, value: bytes) -> Any:
        """Deserialize with fallback support for multiple serializers."""
        last_error: SerializerError | None = None
        for serializer in self._serializers:
            try:
                return serializer.loads(value)
            except SerializerError as e:
                last_error = e
                continue

        if last_error is not None:
            raise last_error
        raise SerializerError("No serializers configured")

    # =========================================================================
    # Encoding/Decoding
    # =========================================================================

    def encode(self, value: Any) -> bytes | int:
        """Encode a value for storage (serialize + compress)."""
        if isinstance(value, bool) or not isinstance(value, int):
            value = self._serializers[0].dumps(value)
            if self._compressors:
                return self._compressors[0].compress(value)
            return value
        return value

    def decode(self, value: Any) -> Any:
        """Decode a value from storage (decompress + deserialize)."""
        try:
            return int(value)
        except (ValueError, TypeError):
            value = self._decompress(value)
            return self._deserialize(value)

    # =========================================================================
    # Connection Pool Management (matches Django's structure)
    # =========================================================================

    def _get_connection_pool_index(self, *, write: bool) -> int:
        """Get the pool index for read/write operations."""
        # Write to first server, read from any
        if write or len(self._servers) == 1:
            return 0
        import random

        return random.randint(1, len(self._servers) - 1)  # noqa: S311

    def _get_connection_pool(self, *, write: bool) -> Any:
        """Get a connection pool for the given operation type."""
        index = self._get_connection_pool_index(write=write)
        if index not in self._pools:
            assert self._pool_class is not None, "Subclasses must set _pool_class"  # noqa: S101
            self._pools[index] = self._pool_class.from_url(  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                self._servers[index],
                **self._pool_options,
            )
        return self._pools[index]

    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get a client connection.

        Args:
            key: Optional key (for sharding implementations)
            write: Whether this is a write operation
        """
        pool = self._get_connection_pool(write=write)
        assert self._client_class is not None, "Subclasses must set _client_class"  # noqa: S101
        return self._client_class(connection_pool=pool)

    # =========================================================================
    # Async Connection Pool Management
    # =========================================================================

    def _get_async_connection_pool(self, *, write: bool) -> Any:
        """Get an async connection pool for the given operation type.

        Async pools are cached per event loop to ensure proper asyncio semantics.
        Each event loop gets its own set of connection pools. Using WeakKeyDictionary
        ensures automatic cleanup when the event loop is garbage collected.

        Args:
            write: Whether this is a write operation

        Returns:
            An async connection pool for the current event loop

        Raises:
            RuntimeError: If no event loop is running or async pool class is not set
        """
        loop = asyncio.get_running_loop()
        index = self._get_connection_pool_index(write=write)

        # Check instance-level cache first
        if loop in self._async_pools and index in self._async_pools[loop]:
            return self._async_pools[loop][index]

        if self._async_pool_class is None:
            msg = "Async operations require _async_pool_class to be set. Use RedisCacheClient or ValkeyCacheClient."
            raise RuntimeError(msg)

        # Filter out parser_class from pool options for async - it's sync-specific
        async_pool_options = {k: v for k, v in self._pool_options.items() if k != "parser_class"}
        pool = self._async_pool_class.from_url(  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            self._servers[index],
            **async_pool_options,
        )

        # Cache on instance for fast access
        if loop not in self._async_pools:
            self._async_pools[loop] = {}
        self._async_pools[loop][index] = pool
        return pool

    def get_async_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get an async client connection.

        Args:
            key: Optional key (for sharding implementations)
            write: Whether this is a write operation

        Returns:
            An async Redis/Valkey client for the current event loop

        Raises:
            RuntimeError: If no event loop is running or async client class is not set
        """
        pool = self._get_async_connection_pool(write=write)
        if self._async_client_class is None:
            msg = "Async operations require _async_client_class to be set. Use RedisCacheClient or ValkeyCacheClient."
            raise RuntimeError(msg)
        return self._async_client_class(connection_pool=pool)

    # =========================================================================
    # Core Cache Operations
    # =========================================================================

    def add(self, key: KeyT, value: Any, timeout: int | None) -> bool:
        """Set a value only if the key doesn't exist."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            if timeout == 0:
                if ret := bool(client.set(key, nvalue, nx=True)):
                    client.delete(key)
                return ret
            return bool(client.set(key, nvalue, nx=True, ex=timeout))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def aadd(self, key: KeyT, value: Any, timeout: int | None) -> bool:
        """Set a value only if the key doesn't exist, asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        try:
            if timeout == 0:
                if ret := bool(await client.set(key, nvalue, nx=True)):
                    await client.delete(key)
                return ret
            return bool(await client.set(key, nvalue, nx=True, ex=timeout))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def get(self, key: KeyT) -> Any:
        """Fetch a value from the cache.

        Returns the decoded value or None if not found.
        """
        client = self.get_client(key, write=False)
        try:
            val = client.get(key)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        if val is None:
            return None
        return self.decode(val)

    async def aget(self, key: KeyT) -> Any:
        """Fetch a value from the cache asynchronously.

        Returns the decoded value or None if not found.
        """
        client = self.get_async_client(key, write=False)
        try:
            val = await client.get(key)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        if val is None:
            return None
        return self.decode(val)

    def set(self, key: KeyT, value: Any, timeout: int | None) -> None:
        """Set a value in the cache (standard Django interface)."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            if timeout == 0:
                client.delete(key)
            else:
                client.set(key, nvalue, ex=timeout)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def aset(self, key: KeyT, value: Any, timeout: int | None) -> None:
        """Set a value in the cache asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        try:
            if timeout == 0:
                await client.delete(key)
            else:
                await client.set(key, nvalue, ex=timeout)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def set_with_flags(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set a value with nx/xx flags, returning success status."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            if timeout == 0:
                return False
            result = client.set(key, nvalue, ex=timeout, nx=nx, xx=xx)
            return bool(result)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def touch(self, key: KeyT, timeout: int | None) -> bool:
        """Update the timeout on a key."""
        client = self.get_client(key, write=True)

        try:
            if timeout is None:
                return bool(client.persist(key))
            return bool(client.expire(key, timeout))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def atouch(self, key: KeyT, timeout: int | None) -> bool:
        """Update the timeout on a key asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            if timeout is None:
                return bool(await client.persist(key))
            return bool(await client.expire(key, timeout))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def delete(self, key: KeyT) -> bool:
        """Remove a key from the cache."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.delete(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def adelete(self, key: KeyT) -> bool:
        """Remove a key from the cache asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return bool(await client.delete(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def get_many(self, keys: Iterable[KeyT]) -> dict[KeyT, Any]:
        """Retrieve many keys."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_client(write=False)
        try:
            results = client.mget(keys)
            return {k: self.decode(v) for k, v in zip(keys, results, strict=False) if v is not None}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def aget_many(self, keys: Iterable[KeyT]) -> dict[KeyT, Any]:
        """Retrieve many keys asynchronously."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_async_client(write=False)
        try:
            results = await client.mget(keys)
            return {k: self.decode(v) for k, v in zip(keys, results, strict=False) if v is not None}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def has_key(self, key: KeyT) -> bool:
        """Check if a key exists."""
        client = self.get_client(key, write=False)

        try:
            return bool(client.exists(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahas_key(self, key: KeyT) -> bool:
        """Check if a key exists asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            return bool(await client.exists(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def incr(self, key: KeyT, delta: int = 1) -> int:
        """Increment a value.

        Uses Redis INCR for atomic increments, but falls back to GET+SET
        for values that would overflow Redis's 64-bit signed integer limit
        or for non-integer (serialized) values.
        """
        client = self.get_client(key, write=True)

        try:
            if not client.exists(key):
                raise ValueError(f"Key {key!r} not found.")
            return client.incr(key, delta)
        except _ResponseError as e:
            # Handle overflow or non-integer by falling back to GET + SET
            err_msg = str(e).lower()
            if "overflow" in err_msg or "not an integer" in err_msg:
                try:
                    val = client.get(key)
                    if val is None:
                        raise ValueError(f"Key {key!r} not found.")
                    # Decode the value, add delta, and set back
                    new_value = self.decode(val) + delta
                    nvalue = self.encode(new_value)
                    client.set(key, nvalue, keepttl=True)
                except _main_exceptions as e2:
                    raise ConnectionInterruptedError(connection=client) from e2
                else:
                    return new_value
            raise ConnectionInterruptedError(connection=client) from e
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def aincr(self, key: KeyT, delta: int = 1) -> int:
        """Increment a value asynchronously.

        Uses Redis INCR for atomic increments, but falls back to GET+SET
        for values that would overflow Redis's 64-bit signed integer limit
        or for non-integer (serialized) values.
        """
        client = self.get_async_client(key, write=True)

        try:
            if not await client.exists(key):
                raise ValueError(f"Key {key!r} not found.")
            return await client.incr(key, delta)
        except _ResponseError as e:
            # Handle overflow or non-integer by falling back to GET + SET
            err_msg = str(e).lower()
            if "overflow" in err_msg or "not an integer" in err_msg:
                try:
                    val = await client.get(key)
                    if val is None:
                        raise ValueError(f"Key {key!r} not found.")
                    # Decode the value, add delta, and set back
                    new_value = self.decode(val) + delta
                    nvalue = self.encode(new_value)
                    await client.set(key, nvalue, keepttl=True)
                except _main_exceptions as e2:
                    raise ConnectionInterruptedError(connection=client) from e2
                else:
                    return new_value
            raise ConnectionInterruptedError(connection=client) from e
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def set_many(self, data: Mapping[KeyT, Any], timeout: int | None) -> list:
        """Set multiple values."""
        if not data:
            return []

        client = self.get_client(write=True)
        prepared = {k: self.encode(v) for k, v in data.items()}

        try:
            if timeout == 0:
                client.delete(*prepared.keys())
            elif timeout is None:
                client.mset(prepared)
            else:
                pipe = client.pipeline()
                pipe.mset(prepared)
                for key in prepared:
                    pipe.expire(key, timeout)
                pipe.execute()
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        return []

    async def aset_many(self, data: Mapping[KeyT, Any], timeout: int | None) -> list:
        """Set multiple values asynchronously."""
        if not data:
            return []

        client = self.get_async_client(write=True)
        prepared = {k: self.encode(v) for k, v in data.items()}

        try:
            if timeout == 0:
                await client.delete(*prepared.keys())
            elif timeout is None:
                await client.mset(prepared)
            else:
                pipe = client.pipeline()
                pipe.mset(prepared)
                for key in prepared:
                    pipe.expire(key, timeout)
                await pipe.execute()
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

        return []

    def delete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys."""
        if not keys:
            return 0

        client = self.get_client(write=True)

        try:
            return client.delete(*keys)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def adelete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys asynchronously."""
        if not keys:
            return 0

        client = self.get_async_client(write=True)

        try:
            return await client.delete(*keys)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def clear(self) -> bool:
        """Flush the database."""
        client = self.get_client(write=True)

        try:
            return bool(client.flushdb())
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def aclear(self) -> bool:
        """Flush the database asynchronously."""
        client = self.get_async_client(write=True)

        try:
            return bool(await client.flushdb())
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def close(self, **kwargs: Any) -> None:
        """Close connections if configured."""
        if self._options.get("close_connection", False):
            for pool in self._pools.values():
                pool.disconnect()
            self._pools.clear()
            # Also clear async pool references (actual disconnection happens via aclose)
            self._async_pools.clear()

    async def aclose(self, **kwargs: Any) -> None:
        """Async close - disconnect async pools for current event loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running event loop
            return

        pool_dict = self._async_pools.get(loop)
        if pool_dict is not None:
            for pool in pool_dict.values():
                await pool.disconnect()
            # Remove from tracking
            if loop in self._async_pools:
                del self._async_pools[loop]

    # =========================================================================
    # Extended Operations (beyond Django's BaseCache)
    # =========================================================================

    def ttl(self, key: KeyT) -> int | None:
        """Get TTL in seconds. Returns None if no expiry, -2 if key doesn't exist."""
        client = self.get_client(key, write=False)

        try:
            result = client.ttl(key)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            if result == -1:
                return None
            if result == -2:
                return -2
            return result

    def pttl(self, key: KeyT) -> int | None:
        """Get TTL in milliseconds."""
        client = self.get_client(key, write=False)

        try:
            result = client.pttl(key)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            if result == -1:
                return None
            if result == -2:
                return -2
            return result

    def expire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry on a key."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.expire(key, timeout))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def pexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry in milliseconds."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.pexpire(key, timeout))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def expireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.expireat(key, when))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def pexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time in milliseconds."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.pexpireat(key, when))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def persist(self, key: KeyT) -> bool:
        """Remove expiry from a key."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.persist(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def attl(self, key: KeyT) -> int | None:
        """Get TTL in seconds asynchronously. Returns None if no expiry, -2 if key doesn't exist."""
        client = self.get_async_client(key, write=False)

        try:
            result = await client.ttl(key)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            if result == -1:
                return None
            if result == -2:
                return -2
            return result

    async def apttl(self, key: KeyT) -> int | None:
        """Get TTL in milliseconds asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            result = await client.pttl(key)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            if result == -1:
                return None
            if result == -2:
                return -2
            return result

    async def aexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry on a key asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return bool(await client.expire(key, timeout))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def apexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry in milliseconds asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return bool(await client.pexpire(key, timeout))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def aexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return bool(await client.expireat(key, when))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def apexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time in milliseconds asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return bool(await client.pexpireat(key, when))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def apersist(self, key: KeyT) -> bool:
        """Remove expiry from a key asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return bool(await client.persist(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def keys(self, pattern: str) -> list[str]:
        """Get all keys matching pattern (already prefixed)."""
        client = self.get_client(write=False)

        try:
            keys_result = client.keys(pattern)
            return [k.decode() if isinstance(k, bytes) else k for k in keys_result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def iter_keys(self, pattern: str, itersize: int | None = None) -> Iterator[str]:
        """Iterate keys matching pattern (already prefixed)."""
        client = self.get_client(write=False)

        if itersize is None:
            itersize = self._default_scan_itersize

        for item in client.scan_iter(match=pattern, count=itersize):
            yield item.decode() if isinstance(item, bytes) else item

    def delete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        """Delete all keys matching pattern (already prefixed)."""
        client = self.get_client(write=True)

        if itersize is None:
            itersize = self._default_scan_itersize

        try:
            keys_list = list(client.scan_iter(match=pattern, count=itersize))
            if not keys_list:
                return 0
            return cast("int", client.delete(*keys_list))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def rename(self, src: KeyT, dst: KeyT) -> bool:
        """Rename a key.

        Atomically renames src to dst. If dst already exists, it is overwritten.

        Args:
            src: Source key name
            dst: Destination key name

        Returns:
            True on success

        Raises:
            ValueError: If src does not exist
        """
        client = self.get_client(src, write=True)

        try:
            client.rename(src, dst)
        except _main_exceptions as e:
            err_msg = str(e).lower()
            if "no such key" in err_msg:
                raise ValueError(f"Key {src!r} not found") from None
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return True

    def renamenx(self, src: KeyT, dst: KeyT) -> bool:
        """Rename a key only if the destination does not exist.

        Atomically renames src to dst only if dst does not already exist.

        Args:
            src: Source key name
            dst: Destination key name

        Returns:
            True if renamed, False if dst already exists

        Raises:
            ValueError: If src does not exist
        """
        client = self.get_client(src, write=True)

        try:
            return bool(client.renamenx(src, dst))
        except _main_exceptions as e:
            err_msg = str(e).lower()
            if "no such key" in err_msg:
                raise ValueError(f"Key {src!r} not found") from None
            raise ConnectionInterruptedError(connection=client) from e

    async def akeys(self, pattern: str) -> list[str]:
        """Get all keys matching pattern (already prefixed) asynchronously."""
        client = self.get_async_client(write=False)

        try:
            keys_result = await client.keys(pattern)
            return [k.decode() if isinstance(k, bytes) else k for k in keys_result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def aiter_keys(self, pattern: str, itersize: int | None = None) -> AsyncIterator[str]:
        """Iterate keys matching pattern (already prefixed) asynchronously."""
        client = self.get_async_client(write=False)

        if itersize is None:
            itersize = self._default_scan_itersize

        async for item in client.scan_iter(match=pattern, count=itersize):
            yield item.decode() if isinstance(item, bytes) else item

    async def adelete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        """Delete all keys matching pattern (already prefixed) asynchronously."""
        client = self.get_async_client(write=True)

        if itersize is None:
            itersize = self._default_scan_itersize

        try:
            keys_list = [key async for key in client.scan_iter(match=pattern, count=itersize)]
            if not keys_list:
                return 0
            return cast("int", await client.delete(*keys_list))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def arename(self, src: KeyT, dst: KeyT) -> bool:
        """Rename a key asynchronously.

        Atomically renames src to dst. If dst already exists, it is overwritten.

        Args:
            src: Source key name
            dst: Destination key name

        Returns:
            True on success

        Raises:
            ValueError: If src does not exist
        """
        client = self.get_async_client(src, write=True)

        try:
            await client.rename(src, dst)
        except _main_exceptions as e:
            err_msg = str(e).lower()
            if "no such key" in err_msg:
                raise ValueError(f"Key {src!r} not found") from None
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return True

    async def arenamenx(self, src: KeyT, dst: KeyT) -> bool:
        """Rename a key only if the destination does not exist, asynchronously.

        Atomically renames src to dst only if dst does not already exist.

        Args:
            src: Source key name
            dst: Destination key name

        Returns:
            True if renamed, False if dst already exists

        Raises:
            ValueError: If src does not exist
        """
        client = self.get_async_client(src, write=True)

        try:
            return bool(await client.renamenx(src, dst))
        except _main_exceptions as e:
            err_msg = str(e).lower()
            if "no such key" in err_msg:
                raise ValueError(f"Key {src!r} not found") from None
            raise ConnectionInterruptedError(connection=client) from e

    def lock(
        self,
        key: str,
        timeout: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        """Get a distributed lock."""
        client = self.get_client(key, write=True)
        return client.lock(
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
        from django_cachex.client.pipeline import Pipeline

        client = self.get_client(write=True)
        raw_pipeline = client.pipeline(transaction=transaction)
        return Pipeline(cache_client=self, pipeline=raw_pipeline, version=version)

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(self, key: KeyT, field: str, value: Any) -> int:
        """Set a hash field."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            return cast("int", client.hset(key, field, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        """Set a hash field only if it doesn't exist."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            return bool(client.hsetnx(key, field, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hget(self, key: KeyT, field: str) -> Any | None:
        """Get a hash field."""
        client = self.get_client(key, write=False)

        try:
            val = client.hget(key, field)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hmset(self, key: KeyT, mapping: Mapping[str, Any]) -> bool:
        """Set multiple hash fields."""
        client = self.get_client(key, write=True)
        nmap = {f: self.encode(v) for f, v in mapping.items()}

        try:
            # hmset is deprecated, use hset with mapping
            client.hset(key, mapping=nmap)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return True

    def hmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        """Get multiple hash fields."""
        client = self.get_client(key, write=False)

        try:
            values = client.hmget(key, fields)
            return [self.decode(v) if v is not None else None for v in values]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hgetall(self, key: KeyT) -> dict[str, Any]:
        """Get all hash fields."""
        client = self.get_client(key, write=False)

        try:
            raw = client.hgetall(key)
            return {(f.decode() if isinstance(f, bytes) else f): self.decode(v) for f, v in raw.items()}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hdel(self, key: KeyT, *fields: str) -> int:
        """Delete hash fields."""
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.hdel(key, *fields))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hexists(self, key: KeyT, field: str) -> bool:
        """Check if a hash field exists."""
        client = self.get_client(key, write=False)

        try:
            return bool(client.hexists(key, field))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hlen(self, key: KeyT) -> int:
        """Get the number of fields in a hash."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.hlen(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hkeys(self, key: KeyT) -> list[str]:
        """Get all field names in a hash."""
        client = self.get_client(key, write=False)

        try:
            fields = client.hkeys(key)
            return [f.decode() if isinstance(f, bytes) else f for f in fields]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hvals(self, key: KeyT) -> list[Any]:
        """Get all values in a hash."""
        client = self.get_client(key, write=False)

        try:
            values = client.hvals(key)
            return [self.decode(v) for v in values]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        """Increment a hash field by an integer."""
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.hincrby(key, field, amount))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0) -> float:
        """Increment a hash field by a float."""
        client = self.get_client(key, write=True)

        try:
            return float(client.hincrbyfloat(key, field, amount))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahset(self, key: KeyT, field: str, value: Any) -> int:
        """Set a hash field asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        try:
            return cast("int", await client.hset(key, field, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        """Set a hash field only if it doesn't exist, asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        try:
            return bool(await client.hsetnx(key, field, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahget(self, key: KeyT, field: str) -> Any | None:
        """Get a hash field asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            val = await client.hget(key, field)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahmset(self, key: KeyT, mapping: Mapping[str, Any]) -> bool:
        """Set multiple hash fields asynchronously."""
        client = self.get_async_client(key, write=True)
        nmap = {f: self.encode(v) for f, v in mapping.items()}

        try:
            # hmset is deprecated, use hset with mapping
            await client.hset(key, mapping=nmap)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return True

    async def ahmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        """Get multiple hash fields asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            values = await client.hmget(key, fields)
            return [self.decode(v) if v is not None else None for v in values]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahgetall(self, key: KeyT) -> dict[str, Any]:
        """Get all hash fields asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            raw = await client.hgetall(key)
            return {(f.decode() if isinstance(f, bytes) else f): self.decode(v) for f, v in raw.items()}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahdel(self, key: KeyT, *fields: str) -> int:
        """Delete hash fields asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return cast("int", await client.hdel(key, *fields))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahexists(self, key: KeyT, field: str) -> bool:
        """Check if a hash field exists asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            return bool(await client.hexists(key, field))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahlen(self, key: KeyT) -> int:
        """Get the number of fields in a hash asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            return cast("int", await client.hlen(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahkeys(self, key: KeyT) -> list[str]:
        """Get all field names in a hash asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            fields = await client.hkeys(key)
            return [f.decode() if isinstance(f, bytes) else f for f in fields]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahvals(self, key: KeyT) -> list[Any]:
        """Get all values in a hash asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            values = await client.hvals(key)
            return [self.decode(v) for v in values]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        """Increment a hash field by an integer asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return cast("int", await client.hincrby(key, field, amount))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0) -> float:
        """Increment a hash field by a float asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return float(await client.hincrbyfloat(key, field, amount))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    # =========================================================================
    # List Operations
    # =========================================================================

    def lpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the left of a list."""
        client = self.get_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        try:
            return cast("int", client.lpush(key, *nvalues))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def rpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the right of a list."""
        client = self.get_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        try:
            return cast("int", client.rpush(key, *nvalues))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def lpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the left of a list.

        Args:
            key: The list key
            count: Optional number of elements to pop (default: 1, returns single value)

        Returns:
            Single value if count is None, list of values if count is specified
        """
        client = self.get_client(key, write=True)

        try:
            if count is not None:
                vals = client.lpop(key, count)
                return [self.decode(v) for v in vals] if vals else []
            val = client.lpop(key)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def rpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the right of a list.

        Args:
            key: The list key
            count: Optional number of elements to pop (default: 1, returns single value)

        Returns:
            Single value if count is None, list of values if count is specified
        """
        client = self.get_client(key, write=True)

        try:
            if count is not None:
                vals = client.rpop(key, count)
                return [self.decode(v) for v in vals] if vals else []
            val = client.rpop(key)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def llen(self, key: KeyT) -> int:
        """Get the length of a list."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.llen(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def lpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list.

        Args:
            key: List key
            value: Value to search for
            rank: Rank of first match to return (1 for first, -1 for last, etc.)
            count: Number of matches to return (0 for all)
            maxlen: Limit search to first N elements

        Returns:
            Index if count is None, list of indices if count is specified, None if not found
        """
        client = self.get_client(key, write=False)
        encoded_value = self.encode(value)

        try:
            kwargs: dict[str, Any] = {}
            if rank is not None:
                kwargs["rank"] = rank
            if count is not None:
                kwargs["count"] = count
            if maxlen is not None:
                kwargs["maxlen"] = maxlen

            return client.lpos(key, encoded_value, **kwargs)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def lmove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
    ) -> Any | None:
        """Atomically move an element from one list to another.

        Args:
            src: Source list key
            dst: Destination list key
            wherefrom: Where to pop from source ('LEFT' or 'RIGHT')
            whereto: Where to push to destination ('LEFT' or 'RIGHT')

        Returns:
            The moved element, or None if src is empty
        """
        client = self.get_client(src, write=True)

        try:
            val = client.lmove(src, dst, wherefrom, whereto)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def lrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        """Get a range of elements from a list."""
        client = self.get_client(key, write=False)

        try:
            values = client.lrange(key, start, end)
            return [self.decode(v) for v in values]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def lindex(self, key: KeyT, index: int) -> Any | None:
        """Get an element from a list by index."""
        client = self.get_client(key, write=False)

        try:
            val = client.lindex(key, index)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def lset(self, key: KeyT, index: int, value: Any) -> bool:
        """Set an element in a list by index."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            client.lset(key, index, nvalue)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return True

    def lrem(self, key: KeyT, count: int, value: Any) -> int:
        """Remove elements from a list."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            return cast("int", client.lrem(key, count, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def ltrim(self, key: KeyT, start: int, end: int) -> bool:
        """Trim a list to the specified range."""
        client = self.get_client(key, write=True)

        try:
            client.ltrim(key, start, end)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return True

    def linsert(
        self,
        key: KeyT,
        where: str,
        pivot: Any,
        value: Any,
    ) -> int:
        """Insert an element before or after another element."""
        client = self.get_client(key, write=True)
        npivot = self.encode(pivot)
        nvalue = self.encode(value)

        try:
            return cast("int", client.linsert(key, where, npivot, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def blpop(
        self,
        keys: Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list.

        Blocks until an element is available or timeout expires.

        Args:
            keys: One or more list keys to pop from (first available)
            timeout: Seconds to block (0 = block indefinitely)

        Returns:
            Tuple of (key, value) or None if timeout expires.
        """
        client = self.get_client(write=True)

        try:
            result = client.blpop(keys, timeout=timeout)
            if result is None:
                return None
            key_bytes, value_bytes = result
            key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
            return (key_str, self.decode(value_bytes))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def brpop(
        self,
        keys: Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list.

        Blocks until an element is available or timeout expires.

        Args:
            keys: One or more list keys to pop from (first available)
            timeout: Seconds to block (0 = block indefinitely)

        Returns:
            Tuple of (key, value) or None if timeout expires.
        """
        client = self.get_client(write=True)

        try:
            result = client.brpop(keys, timeout=timeout)
            if result is None:
                return None
            key_bytes, value_bytes = result
            key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
            return (key_str, self.decode(value_bytes))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def blmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
    ) -> Any | None:
        """Blocking atomically move element from one list to another.

        Blocks until an element is available in src or timeout expires.

        Args:
            src: Source list key
            dst: Destination list key
            timeout: Seconds to block (0 = block indefinitely)
            wherefrom: Where to pop from source ('LEFT' or 'RIGHT')
            whereto: Where to push to destination ('LEFT' or 'RIGHT')

        Returns:
            The moved element, or None if timeout expires.
        """
        client = self.get_client(src, write=True)

        try:
            val = client.blmove(src, dst, timeout, wherefrom, whereto)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def alpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the left of a list asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        try:
            return cast("int", await client.lpush(key, *nvalues))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def arpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the right of a list asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        try:
            return cast("int", await client.rpush(key, *nvalues))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def alpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the left of a list asynchronously.

        Args:
            key: The list key
            count: Optional number of elements to pop (default: 1, returns single value)

        Returns:
            Single value if count is None, list of values if count is specified
        """
        client = self.get_async_client(key, write=True)

        try:
            if count is not None:
                vals = await client.lpop(key, count)
                return [self.decode(v) for v in vals] if vals else []
            val = await client.lpop(key)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def arpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the right of a list asynchronously.

        Args:
            key: The list key
            count: Optional number of elements to pop (default: 1, returns single value)

        Returns:
            Single value if count is None, list of values if count is specified
        """
        client = self.get_async_client(key, write=True)

        try:
            if count is not None:
                vals = await client.rpop(key, count)
                return [self.decode(v) for v in vals] if vals else []
            val = await client.rpop(key)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def allen(self, key: KeyT) -> int:
        """Get the length of a list asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            return cast("int", await client.llen(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def alpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list asynchronously.

        Args:
            key: List key
            value: Value to search for
            rank: Rank of first match to return (1 for first, -1 for last, etc.)
            count: Number of matches to return (0 for all)
            maxlen: Limit search to first N elements

        Returns:
            Index if count is None, list of indices if count is specified, None if not found
        """
        client = self.get_async_client(key, write=False)
        encoded_value = self.encode(value)

        try:
            kwargs: dict[str, Any] = {}
            if rank is not None:
                kwargs["rank"] = rank
            if count is not None:
                kwargs["count"] = count
            if maxlen is not None:
                kwargs["maxlen"] = maxlen

            return await client.lpos(key, encoded_value, **kwargs)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def almove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
    ) -> Any | None:
        """Atomically move an element from one list to another asynchronously.

        Args:
            src: Source list key
            dst: Destination list key
            wherefrom: Where to pop from source ('LEFT' or 'RIGHT')
            whereto: Where to push to destination ('LEFT' or 'RIGHT')

        Returns:
            The moved element, or None if src is empty
        """
        client = self.get_async_client(src, write=True)

        try:
            val = await client.lmove(src, dst, wherefrom, whereto)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def alrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        """Get a range of elements from a list asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            values = await client.lrange(key, start, end)
            return [self.decode(v) for v in values]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def alindex(self, key: KeyT, index: int) -> Any | None:
        """Get an element from a list by index asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            val = await client.lindex(key, index)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def alset(self, key: KeyT, index: int, value: Any) -> bool:
        """Set an element in a list by index asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        try:
            await client.lset(key, index, nvalue)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return True

    async def alrem(self, key: KeyT, count: int, value: Any) -> int:
        """Remove elements from a list asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        try:
            return cast("int", await client.lrem(key, count, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def altrim(self, key: KeyT, start: int, end: int) -> bool:
        """Trim a list to the specified range asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            await client.ltrim(key, start, end)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return True

    async def alinsert(
        self,
        key: KeyT,
        where: str,
        pivot: Any,
        value: Any,
    ) -> int:
        """Insert an element before or after another element asynchronously."""
        client = self.get_async_client(key, write=True)
        npivot = self.encode(pivot)
        nvalue = self.encode(value)

        try:
            return cast("int", await client.linsert(key, where, npivot, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ablpop(
        self,
        keys: Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list asynchronously.

        Blocks until an element is available or timeout expires.

        Args:
            keys: One or more list keys to pop from (first available)
            timeout: Seconds to block (0 = block indefinitely)

        Returns:
            Tuple of (key, value) or None if timeout expires.
        """
        client = self.get_async_client(write=True)

        try:
            result = await client.blpop(keys, timeout=timeout)
            if result is None:
                return None
            key_bytes, value_bytes = result
            key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
            return (key_str, self.decode(value_bytes))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def abrpop(
        self,
        keys: Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list asynchronously.

        Blocks until an element is available or timeout expires.

        Args:
            keys: One or more list keys to pop from (first available)
            timeout: Seconds to block (0 = block indefinitely)

        Returns:
            Tuple of (key, value) or None if timeout expires.
        """
        client = self.get_async_client(write=True)

        try:
            result = await client.brpop(keys, timeout=timeout)
            if result is None:
                return None
            key_bytes, value_bytes = result
            key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
            return (key_str, self.decode(value_bytes))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ablmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
    ) -> Any | None:
        """Blocking atomically move element from one list to another asynchronously.

        Blocks until an element is available in src or timeout expires.

        Args:
            src: Source list key
            dst: Destination list key
            timeout: Seconds to block (0 = block indefinitely)
            wherefrom: Where to pop from source ('LEFT' or 'RIGHT')
            whereto: Where to push to destination ('LEFT' or 'RIGHT')

        Returns:
            The moved element, or None if timeout expires.
        """
        client = self.get_async_client(src, write=True)

        try:
            val = await client.blmove(src, dst, timeout, wherefrom, whereto)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(self, key: KeyT, *members: Any) -> int:
        """Add members to a set."""
        client = self.get_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", client.sadd(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def srem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a set."""
        client = self.get_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", client.srem(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def smembers(self, key: KeyT) -> _Set[Any]:
        """Get all members of a set."""
        client = self.get_client(key, write=False)

        try:
            result = client.smembers(key)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def sismember(self, key: KeyT, member: Any) -> bool:
        """Check if a value is a member of a set."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        try:
            return bool(client.sismember(key, nmember))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def scard(self, key: KeyT) -> int:
        """Get the number of members in a set."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.scard(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def spop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Remove and return random member(s) from a set."""
        client = self.get_client(key, write=True)

        try:
            if count is None:
                val = client.spop(key)
                return self.decode(val) if val is not None else None
            vals = client.spop(key, count)
            return [self.decode(v) for v in vals] if vals else []
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def srandmember(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Get random member(s) from a set."""
        client = self.get_client(key, write=False)

        try:
            if count is None:
                val = client.srandmember(key)
                return self.decode(val) if val is not None else None
            vals = client.srandmember(key, count)
            return [self.decode(v) for v in vals] if vals else []
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def smove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        """Move a member from one set to another."""
        client = self.get_client(write=True)
        nmember = self.encode(member)

        try:
            return bool(client.smove(src, dst, nmember))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def sdiff(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the difference of sets."""
        client = self.get_client(write=False)

        try:
            result = client.sdiff(*keys)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def sdiffstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the difference of sets."""
        client = self.get_client(write=True)

        try:
            return cast("int", client.sdiffstore(dest, *keys))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def sinter(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the intersection of sets."""
        client = self.get_client(write=False)

        try:
            result = client.sinter(*keys)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def sinterstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the intersection of sets."""
        client = self.get_client(write=True)

        try:
            return cast("int", client.sinterstore(dest, *keys))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def sunion(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the union of sets."""
        client = self.get_client(write=False)

        try:
            result = client.sunion(*keys)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def sunionstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the union of sets."""
        client = self.get_client(write=True)

        try:
            return cast("int", client.sunionstore(dest, *keys))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def smismember(self, key: KeyT, *members: Any) -> list[bool]:
        """Check if multiple values are members of a set."""
        client = self.get_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        try:
            result = client.smismember(key, nmembers)
            return [bool(v) for v in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, _Set[Any]]:
        """Incrementally iterate over set members.

        Args:
            key: The set key
            cursor: Cursor position (0 to start)
            match: Pattern to filter members
            count: Hint for number of elements per batch

        Returns:
            Tuple of (next_cursor, set of members)
        """
        client = self.get_client(key, write=False)

        try:
            next_cursor, members = client.sscan(key, cursor=cursor, match=match, count=count)
            return next_cursor, {self.decode(m) for m in members}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
    ) -> Iterator[Any]:
        """Iterate over set members using SSCAN.

        Args:
            key: The set key
            match: Pattern to filter members
            count: Hint for number of elements per batch

        Yields:
            Decoded member values
        """
        client = self.get_client(key, write=False)

        for member in client.sscan_iter(key, match=match, count=count):
            yield self.decode(member)

    async def asadd(self, key: KeyT, *members: Any) -> int:
        """Add members to a set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", await client.sadd(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asrem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", await client.srem(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asmembers(self, key: KeyT) -> _Set[Any]:
        """Get all members of a set asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            result = await client.smembers(key)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asismember(self, key: KeyT, member: Any) -> bool:
        """Check if a value is a member of a set asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = self.encode(member)

        try:
            return bool(await client.sismember(key, nmember))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ascard(self, key: KeyT) -> int:
        """Get the number of members in a set asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            return cast("int", await client.scard(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def aspop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Remove and return random member(s) from a set asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            if count is None:
                val = await client.spop(key)
                return self.decode(val) if val is not None else None
            vals = await client.spop(key, count)
            return [self.decode(v) for v in vals] if vals else []
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asrandmember(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Get random member(s) from a set asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            if count is None:
                val = await client.srandmember(key)
                return self.decode(val) if val is not None else None
            vals = await client.srandmember(key, count)
            return [self.decode(v) for v in vals] if vals else []
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asmove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        """Move a member from one set to another asynchronously."""
        client = self.get_async_client(write=True)
        nmember = self.encode(member)

        try:
            return bool(await client.smove(src, dst, nmember))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asdiff(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the difference of sets asynchronously."""
        client = self.get_async_client(write=False)

        try:
            result = await client.sdiff(*keys)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asdiffstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the difference of sets asynchronously."""
        client = self.get_async_client(write=True)

        try:
            return cast("int", await client.sdiffstore(dest, *keys))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asinter(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the intersection of sets asynchronously."""
        client = self.get_async_client(write=False)

        try:
            result = await client.sinter(*keys)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asinterstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the intersection of sets asynchronously."""
        client = self.get_async_client(write=True)

        try:
            return cast("int", await client.sinterstore(dest, *keys))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asunion(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the union of sets asynchronously."""
        client = self.get_async_client(write=False)

        try:
            result = await client.sunion(*keys)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asunionstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the union of sets asynchronously."""
        client = self.get_async_client(write=True)

        try:
            return cast("int", await client.sunionstore(dest, *keys))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asmismember(self, key: KeyT, *members: Any) -> list[bool]:
        """Check if multiple values are members of a set asynchronously."""
        client = self.get_async_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        try:
            result = await client.smismember(key, nmembers)
            return [bool(v) for v in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, _Set[Any]]:
        """Incrementally iterate over set members asynchronously.

        Args:
            key: The set key
            cursor: Cursor position (0 to start)
            match: Pattern to filter members
            count: Hint for number of elements per batch

        Returns:
            Tuple of (next_cursor, set of members)
        """
        client = self.get_async_client(key, write=False)

        try:
            next_cursor, members = await client.sscan(key, cursor=cursor, match=match, count=count)
            return next_cursor, {self.decode(m) for m in members}
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
    ) -> AsyncIterator[Any]:
        """Iterate over set members using SSCAN asynchronously.

        Args:
            key: The set key
            match: Pattern to filter members
            count: Hint for number of elements per batch

        Yields:
            Decoded member values
        """
        client = self.get_async_client(key, write=False)

        async for member in client.sscan_iter(key, match=match, count=count):
            yield self.decode(member)

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
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
    ) -> int:
        """Add members to a sorted set."""
        client = self.get_client(key, write=True)
        scored_mapping = {self.encode(m): s for m, s in mapping.items()}

        try:
            return cast("int", client.zadd(key, scored_mapping, nx=nx, xx=xx, gt=gt, lt=lt, ch=ch))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zrem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a sorted set."""
        client = self.get_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", client.zrem(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zscore(self, key: KeyT, member: Any) -> float | None:
        """Get the score of a member."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        try:
            result = client.zscore(key, nmember)
            return float(result) if result is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zrank(self, key: KeyT, member: Any) -> int | None:
        """Get the rank of a member (0-based)."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        try:
            return client.zrank(key, nmember)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zrevrank(self, key: KeyT, member: Any) -> int | None:
        """Get the reverse rank of a member."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        try:
            return client.zrevrank(key, nmember)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zcard(self, key: KeyT) -> int:
        """Get the number of members in a sorted set."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.zcard(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zcount(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Count members in a score range."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.zcount(key, min_score, max_score))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zincrby(self, key: KeyT, amount: float, member: Any) -> float:
        """Increment the score of a member."""
        client = self.get_client(key, write=True)
        nmember = self.encode(member)

        try:
            return float(client.zincrby(key, amount, nmember))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get a range of members by index."""
        client = self.get_client(key, write=False)

        try:
            result = client.zrange(key, start, end, withscores=withscores)
            if withscores:
                return [(self.decode(m), float(s)) for m, s in result]
            return [self.decode(m) for m in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get a range of members by index, reversed."""
        client = self.get_client(key, write=False)

        try:
            result = client.zrevrange(key, start, end, withscores=withscores)
            if withscores:
                return [(self.decode(m), float(s)) for m, s in result]
            return [self.decode(m) for m in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        *,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get members by score range."""
        client = self.get_client(key, write=False)

        try:
            result = client.zrangebyscore(
                key,
                min_score,
                max_score,
                start=start,
                num=num,
                withscores=withscores,
            )
            if withscores:
                return [(self.decode(m), float(s)) for m, s in result]
            return [self.decode(m) for m in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zrevrangebyscore(
        self,
        key: KeyT,
        max_score: float | str,
        min_score: float | str,
        *,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get members by score range, reversed."""
        client = self.get_client(key, write=False)

        try:
            result = client.zrevrangebyscore(
                key,
                max_score,
                min_score,
                start=start,
                num=num,
                withscores=withscores,
            )
            if withscores:
                return [(self.decode(m), float(s)) for m, s in result]
            return [self.decode(m) for m in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        """Remove members by rank range."""
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.zremrangebyrank(key, start, end))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zremrangebyscore(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Remove members by score range."""
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.zremrangebyscore(key, min_score, max_score))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zpopmin(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        client = self.get_client(key, write=True)

        try:
            result = client.zpopmin(key, count)
            return [(self.decode(m), float(s)) for m, s in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zpopmax(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        client = self.get_client(key, write=True)

        try:
            result = client.zpopmax(key, count)
            return [(self.decode(m), float(s)) for m, s in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zmscore(self, key: KeyT, *members: Any) -> list[float | None]:
        """Get scores for multiple members."""
        client = self.get_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        try:
            results = client.zmscore(key, nmembers)
            return [float(r) if r is not None else None for r in results]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azadd(
        self,
        key: KeyT,
        mapping: Mapping[Any, float],
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
    ) -> int:
        """Add members to a sorted set asynchronously."""
        client = self.get_async_client(key, write=True)
        scored_mapping = {self.encode(m): s for m, s in mapping.items()}

        try:
            return cast("int", await client.zadd(key, scored_mapping, nx=nx, xx=xx, gt=gt, lt=lt, ch=ch))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azrem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a sorted set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", await client.zrem(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azscore(self, key: KeyT, member: Any) -> float | None:
        """Get the score of a member asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = self.encode(member)

        try:
            result = await client.zscore(key, nmember)
            return float(result) if result is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azrank(self, key: KeyT, member: Any) -> int | None:
        """Get the rank of a member (0-based) asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = self.encode(member)

        try:
            return await client.zrank(key, nmember)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azrevrank(self, key: KeyT, member: Any) -> int | None:
        """Get the reverse rank of a member asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = self.encode(member)

        try:
            return await client.zrevrank(key, nmember)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azcard(self, key: KeyT) -> int:
        """Get the number of members in a sorted set asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            return cast("int", await client.zcard(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azcount(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Count members in a score range asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            return cast("int", await client.zcount(key, min_score, max_score))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azincrby(self, key: KeyT, amount: float, member: Any) -> float:
        """Increment the score of a member asynchronously."""
        client = self.get_async_client(key, write=True)
        nmember = self.encode(member)

        try:
            return float(await client.zincrby(key, amount, nmember))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get a range of members by index asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            result = await client.zrange(key, start, end, withscores=withscores)
            if withscores:
                return [(self.decode(m), float(s)) for m, s in result]
            return [self.decode(m) for m in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get a range of members by index, reversed, asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            result = await client.zrevrange(key, start, end, withscores=withscores)
            if withscores:
                return [(self.decode(m), float(s)) for m, s in result]
            return [self.decode(m) for m in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        *,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get members by score range asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            result = await client.zrangebyscore(
                key,
                min_score,
                max_score,
                start=start,
                num=num,
                withscores=withscores,
            )
            if withscores:
                return [(self.decode(m), float(s)) for m, s in result]
            return [self.decode(m) for m in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azrevrangebyscore(
        self,
        key: KeyT,
        max_score: float | str,
        min_score: float | str,
        *,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get members by score range, reversed, asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            result = await client.zrevrangebyscore(
                key,
                max_score,
                min_score,
                start=start,
                num=num,
                withscores=withscores,
            )
            if withscores:
                return [(self.decode(m), float(s)) for m, s in result]
            return [self.decode(m) for m in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        """Remove members by rank range asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return cast("int", await client.zremrangebyrank(key, start, end))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azremrangebyscore(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Remove members by score range asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return cast("int", await client.zremrangebyscore(key, min_score, max_score))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azpopmin(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            result = await client.zpopmin(key, count)
            return [(self.decode(m), float(s)) for m, s in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azpopmax(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            result = await client.zpopmax(key, count)
            return [(self.decode(m), float(s)) for m, s in result]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azmscore(self, key: KeyT, *members: Any) -> list[float | None]:
        """Get scores for multiple members asynchronously."""
        client = self.get_async_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        try:
            results = await client.zmscore(key, nmembers)
            return [float(r) if r is not None else None for r in results]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    # =========================================================================
    # Streams Operations (Sync)
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
    ) -> str:
        """Add an entry to a stream.

        Args:
            key: Stream key
            fields: Dictionary of field-value pairs
            entry_id: Entry ID ("*" for auto-generated)
            maxlen: Maximum stream length (trim after adding)
            approximate: Use "~" for approximate trimming (more efficient)
            nomkstream: Don't create stream if it doesn't exist
            minid: Trim entries with IDs lower than this
            limit: Maximum entries to trim in one call

        Returns:
            The entry ID of the added entry
        """
        client = self.get_client(key, write=True)
        encoded_fields = {k: self.encode(v) for k, v in fields.items()}

        try:
            result = client.xadd(
                key,
                encoded_fields,
                id=entry_id,
                maxlen=maxlen,
                approximate=approximate,
                nomkstream=nomkstream,
                minid=minid,
                limit=limit,
            )
            return result.decode() if isinstance(result, bytes) else result
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xlen(self, key: KeyT) -> int:
        """Get the number of entries in a stream."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.xlen(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in ascending order.

        Args:
            key: Stream key
            start: Minimum entry ID ("-" for beginning)
            end: Maximum entry ID ("+" for end)
            count: Maximum number of entries to return

        Returns:
            List of (entry_id, fields_dict) tuples
        """
        client = self.get_client(key, write=False)

        try:
            results = client.xrange(key, min=start, max=end, count=count)
            return [
                (
                    entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                    {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                )
                for entry_id, fields in results
            ]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in descending order.

        Args:
            key: Stream key
            end: Maximum entry ID ("+" for end)
            start: Minimum entry ID ("-" for beginning)
            count: Maximum number of entries to return

        Returns:
            List of (entry_id, fields_dict) tuples
        """
        client = self.get_client(key, write=False)

        try:
            results = client.xrevrange(key, max=end, min=start, count=count)
            return [
                (
                    entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                    {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                )
                for entry_id, fields in results
            ]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from one or more streams.

        Args:
            streams: Dict mapping stream keys to last-seen entry IDs
                    (use "0" or "$" for beginning/end)
            count: Maximum entries per stream
            block: Block for N milliseconds (None = don't block, 0 = block forever)

        Returns:
            Dict mapping stream keys to list of (entry_id, fields) tuples,
            or None if blocking timed out
        """
        client = self.get_client(write=False)

        try:
            results = client.xread(streams=streams, count=count, block=block)
            if results is None:
                return None

            return {
                (stream_key.decode() if isinstance(stream_key, bytes) else stream_key): [
                    (
                        entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                        {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                    )
                    for entry_id, fields in entries
                ]
                for stream_key, entries in results
            }
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xtrim(
        self,
        key: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
    ) -> int:
        """Trim a stream to a maximum length or minimum ID.

        Args:
            key: Stream key
            maxlen: Maximum stream length
            approximate: Use "~" for approximate trimming (more efficient)
            minid: Remove entries with IDs lower than this
            limit: Maximum entries to trim in one call

        Returns:
            Number of entries removed
        """
        client = self.get_client(key, write=True)

        try:
            return cast(
                "int",
                client.xtrim(key, maxlen=maxlen, approximate=approximate, minid=minid, limit=limit),
            )
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xdel(self, key: KeyT, *entry_ids: str) -> int:
        """Delete entries from a stream.

        Args:
            key: Stream key
            entry_ids: Entry IDs to delete

        Returns:
            Number of entries deleted
        """
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.xdel(key, *entry_ids))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xinfo_stream(self, key: KeyT, full: bool = False) -> dict[str, Any]:
        """Get information about a stream.

        Args:
            key: Stream key
            full: Return full stream info including entries

        Returns:
            Dictionary with stream information
        """
        client = self.get_client(key, write=False)

        try:
            if full:
                return client.xinfo_stream(key, full=True)
            return client.xinfo_stream(key)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xinfo_groups(self, key: KeyT) -> list[dict[str, Any]]:
        """Get information about consumer groups for a stream.

        Args:
            key: Stream key

        Returns:
            List of dictionaries with group information
        """
        client = self.get_client(key, write=False)

        try:
            return client.xinfo_groups(key)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xinfo_consumers(self, key: KeyT, group: str) -> list[dict[str, Any]]:
        """Get information about consumers in a group.

        Args:
            key: Stream key
            group: Consumer group name

        Returns:
            List of dictionaries with consumer information
        """
        client = self.get_client(key, write=False)

        try:
            return client.xinfo_consumers(key, group)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xgroup_create(
        self,
        key: KeyT,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> bool:
        """Create a consumer group.

        Args:
            key: Stream key
            group: Group name
            entry_id: ID from which to start reading ("$" for new entries only, "0" for all)
            mkstream: Create stream if it doesn't exist
            entries_read: Set the group's entries-read counter

        Returns:
            True if successful
        """
        client = self.get_client(key, write=True)

        try:
            client.xgroup_create(key, group, id=entry_id, mkstream=mkstream, entries_read=entries_read)
            return True
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xgroup_destroy(self, key: KeyT, group: str) -> int:
        """Destroy a consumer group.

        Args:
            key: Stream key
            group: Group name

        Returns:
            Number of destroyed groups (0 or 1)
        """
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.xgroup_destroy(key, group))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xgroup_setid(
        self,
        key: KeyT,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
    ) -> bool:
        """Set the last delivered ID for a consumer group.

        Args:
            key: Stream key
            group: Group name
            entry_id: New last delivered ID
            entries_read: Set the group's entries-read counter

        Returns:
            True if successful
        """
        client = self.get_client(key, write=True)

        try:
            client.xgroup_setid(key, group, id=entry_id, entries_read=entries_read)
            return True
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xgroup_delconsumer(self, key: KeyT, group: str, consumer: str) -> int:
        """Remove a consumer from a group.

        Args:
            key: Stream key
            group: Group name
            consumer: Consumer name

        Returns:
            Number of pending messages that were deleted
        """
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.xgroup_delconsumer(key, group, consumer))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from streams as a consumer group member.

        Args:
            group: Consumer group name
            consumer: Consumer name
            streams: Dict mapping stream keys to entry IDs (">" for new messages)
            count: Maximum entries per stream
            block: Block for N milliseconds (None = don't block, 0 = block forever)
            noack: Don't add messages to pending list

        Returns:
            Dict mapping stream keys to list of (entry_id, fields) tuples,
            or None if blocking timed out
        """
        client = self.get_client(write=True)

        try:
            results = client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams=streams,
                count=count,
                block=block,
                noack=noack,
            )
            if results is None:
                return None

            return {
                (stream_key.decode() if isinstance(stream_key, bytes) else stream_key): [
                    (
                        entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                        {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                    )
                    for entry_id, fields in entries
                ]
                for stream_key, entries in results
            }
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xack(self, key: KeyT, group: str, *entry_ids: str) -> int:
        """Acknowledge message processing.

        Args:
            key: Stream key
            group: Consumer group name
            entry_ids: Entry IDs to acknowledge

        Returns:
            Number of messages acknowledged
        """
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.xack(key, group, *entry_ids))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xpending(
        self,
        key: KeyT,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Get pending entries information.

        Without start/end/count: returns summary info.
        With start/end/count: returns detailed list of pending entries.

        Args:
            key: Stream key
            group: Consumer group name
            start: Minimum entry ID (for detailed query)
            end: Maximum entry ID (for detailed query)
            count: Maximum entries (for detailed query)
            consumer: Filter by consumer name
            idle: Filter by minimum idle time in ms

        Returns:
            Summary dict or list of pending entry details
        """
        client = self.get_client(key, write=False)

        try:
            if start is not None and end is not None and count is not None:
                return client.xpending_range(
                    key,
                    group,
                    min=start,
                    max=end,
                    count=count,
                    consumername=consumer,
                    idle=idle,
                )
            return client.xpending(key, group)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

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
    ) -> list[tuple[str, dict[str, Any]]] | list[str]:
        """Claim pending messages.

        Args:
            key: Stream key
            group: Consumer group name
            consumer: Consumer name to claim for
            min_idle_time: Minimum idle time in ms
            entry_ids: Entry IDs to claim
            idle: Set idle time (ms)
            time: Set idle time as Unix timestamp (ms)
            retrycount: Set retry counter
            force: Create entry in PEL even if it doesn't exist
            justid: Return only entry IDs, not full entries

        Returns:
            List of (entry_id, fields) tuples, or list of entry IDs if justid=True
        """
        client = self.get_client(key, write=True)

        try:
            results = client.xclaim(
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
            if justid:
                return [r.decode() if isinstance(r, bytes) else r for r in results]
            return [
                (
                    entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                    {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                )
                for entry_id, fields in results
            ]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def xautoclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]] | list[str], list[str]]:
        """Auto-claim pending messages that have been idle.

        Args:
            key: Stream key
            group: Consumer group name
            consumer: Consumer name to claim for
            min_idle_time: Minimum idle time in ms
            start_id: Start scanning from this ID
            count: Maximum messages to claim
            justid: Return only entry IDs, not full entries

        Returns:
            Tuple of (next_start_id, claimed_entries, deleted_ids)
        """
        client = self.get_client(key, write=True)

        try:
            result = client.xautoclaim(
                key,
                group,
                consumer,
                min_idle_time,
                start_id=start_id,
                count=count,
                justid=justid,
            )
            next_id = result[0].decode() if isinstance(result[0], bytes) else result[0]
            deleted = [d.decode() if isinstance(d, bytes) else d for d in result[2]] if len(result) > 2 else []

            if justid:
                claimed = [r.decode() if isinstance(r, bytes) else r for r in result[1]]
            else:
                claimed = [
                    (
                        entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                        {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                    )
                    for entry_id, fields in result[1]
                ]
            return (next_id, claimed, deleted)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    # =========================================================================
    # Streams Operations (Async)
    # =========================================================================

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
    ) -> str:
        """Add an entry to a stream asynchronously."""
        client = self.get_async_client(key, write=True)
        encoded_fields = {k: self.encode(v) for k, v in fields.items()}

        try:
            result = await client.xadd(
                key,
                encoded_fields,
                id=entry_id,
                maxlen=maxlen,
                approximate=approximate,
                nomkstream=nomkstream,
                minid=minid,
                limit=limit,
            )
            return result.decode() if isinstance(result, bytes) else result
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axlen(self, key: KeyT) -> int:
        """Get the number of entries in a stream asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            return cast("int", await client.xlen(key))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in ascending order asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            results = await client.xrange(key, min=start, max=end, count=count)
            return [
                (
                    entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                    {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                )
                for entry_id, fields in results
            ]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in descending order asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            results = await client.xrevrange(key, max=end, min=start, count=count)
            return [
                (
                    entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                    {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                )
                for entry_id, fields in results
            ]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from one or more streams asynchronously."""
        client = self.get_async_client(write=False)

        try:
            results = await client.xread(streams=streams, count=count, block=block)
            if results is None:
                return None

            return {
                (stream_key.decode() if isinstance(stream_key, bytes) else stream_key): [
                    (
                        entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                        {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                    )
                    for entry_id, fields in entries
                ]
                for stream_key, entries in results
            }
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axtrim(
        self,
        key: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
    ) -> int:
        """Trim a stream asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return cast(
                "int",
                await client.xtrim(key, maxlen=maxlen, approximate=approximate, minid=minid, limit=limit),
            )
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axdel(self, key: KeyT, *entry_ids: str) -> int:
        """Delete entries from a stream asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return cast("int", await client.xdel(key, *entry_ids))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axinfo_stream(self, key: KeyT, full: bool = False) -> dict[str, Any]:
        """Get information about a stream asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            if full:
                return await client.xinfo_stream(key, full=True)
            return await client.xinfo_stream(key)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axinfo_groups(self, key: KeyT) -> list[dict[str, Any]]:
        """Get information about consumer groups for a stream asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            return await client.xinfo_groups(key)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axinfo_consumers(self, key: KeyT, group: str) -> list[dict[str, Any]]:
        """Get information about consumers in a group asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            return await client.xinfo_consumers(key, group)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axgroup_create(
        self,
        key: KeyT,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> bool:
        """Create a consumer group asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            await client.xgroup_create(key, group, id=entry_id, mkstream=mkstream, entries_read=entries_read)
            return True
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axgroup_destroy(self, key: KeyT, group: str) -> int:
        """Destroy a consumer group asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return cast("int", await client.xgroup_destroy(key, group))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axgroup_setid(
        self,
        key: KeyT,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
    ) -> bool:
        """Set the last delivered ID for a consumer group asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            await client.xgroup_setid(key, group, id=entry_id, entries_read=entries_read)
            return True
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axgroup_delconsumer(self, key: KeyT, group: str, consumer: str) -> int:
        """Remove a consumer from a group asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return cast("int", await client.xgroup_delconsumer(key, group, consumer))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from streams as a consumer group member asynchronously."""
        client = self.get_async_client(write=True)

        try:
            results = await client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams=streams,
                count=count,
                block=block,
                noack=noack,
            )
            if results is None:
                return None

            return {
                (stream_key.decode() if isinstance(stream_key, bytes) else stream_key): [
                    (
                        entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                        {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                    )
                    for entry_id, fields in entries
                ]
                for stream_key, entries in results
            }
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axack(self, key: KeyT, group: str, *entry_ids: str) -> int:
        """Acknowledge message processing asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            return cast("int", await client.xack(key, group, *entry_ids))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axpending(
        self,
        key: KeyT,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Get pending entries information asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            if start is not None and end is not None and count is not None:
                return await client.xpending_range(
                    key,
                    group,
                    min=start,
                    max=end,
                    count=count,
                    consumername=consumer,
                    idle=idle,
                )
            return await client.xpending(key, group)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

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
    ) -> list[tuple[str, dict[str, Any]]] | list[str]:
        """Claim pending messages asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            results = await client.xclaim(
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
            if justid:
                return [r.decode() if isinstance(r, bytes) else r for r in results]
            return [
                (
                    entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                    {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                )
                for entry_id, fields in results
            ]
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def axautoclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]] | list[str], list[str]]:
        """Auto-claim pending messages asynchronously."""
        client = self.get_async_client(key, write=True)

        try:
            result = await client.xautoclaim(
                key,
                group,
                consumer,
                min_idle_time,
                start_id=start_id,
                count=count,
                justid=justid,
            )
            next_id = result[0].decode() if isinstance(result[0], bytes) else result[0]
            deleted = [d.decode() if isinstance(d, bytes) else d for d in result[2]] if len(result) > 2 else []

            if justid:
                claimed = [r.decode() if isinstance(r, bytes) else r for r in result[1]]
            else:
                claimed = [
                    (
                        entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                        {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
                    )
                    for entry_id, fields in result[1]
                ]
            return (next_id, claimed, deleted)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    # =========================================================================
    # Lua Scripting Operations
    # =========================================================================

    def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: Any,
    ) -> Any:
        """Execute a Lua script server-side.

        Args:
            script: The Lua script to execute
            numkeys: Number of keys in keys_and_args
            *keys_and_args: Keys followed by arguments

        Returns:
            The result of the script execution
        """
        client = self.get_client(write=True)
        try:
            return client.eval(script, numkeys, *keys_and_args)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def aeval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: Any,
    ) -> Any:
        """Execute a Lua script server-side asynchronously.

        Args:
            script: The Lua script to execute
            numkeys: Number of keys in keys_and_args
            *keys_and_args: Keys followed by arguments

        Returns:
            The result of the script execution
        """
        client = self.get_async_client(write=True)
        try:
            return await client.eval(script, numkeys, *keys_and_args)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def evalsha(
        self,
        sha: str,
        numkeys: int,
        *keys_and_args: Any,
    ) -> Any:
        """Execute a cached Lua script by its SHA1 hash.

        Args:
            sha: The SHA1 hash of the script
            numkeys: Number of keys in keys_and_args
            *keys_and_args: Keys followed by arguments

        Returns:
            The result of the script execution
        """
        client = self.get_client(write=True)
        try:
            return client.evalsha(sha, numkeys, *keys_and_args)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def aevalsha(
        self,
        sha: str,
        numkeys: int,
        *keys_and_args: Any,
    ) -> Any:
        """Execute a cached Lua script by its SHA1 hash asynchronously.

        Args:
            sha: The SHA1 hash of the script
            numkeys: Number of keys in keys_and_args
            *keys_and_args: Keys followed by arguments

        Returns:
            The result of the script execution
        """
        client = self.get_async_client(write=True)
        try:
            return await client.evalsha(sha, numkeys, *keys_and_args)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def script_load(self, script: str) -> str:
        """Load a Lua script into the script cache.

        Args:
            script: The Lua script to load

        Returns:
            The SHA1 hash of the script
        """
        client = self.get_client(write=True)
        try:
            return cast("str", client.script_load(script))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ascript_load(self, script: str) -> str:
        """Load a Lua script into the script cache asynchronously.

        Args:
            script: The Lua script to load

        Returns:
            The SHA1 hash of the script
        """
        client = self.get_async_client(write=True)
        try:
            return cast("str", await client.script_load(script))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def script_exists(self, *shas: str) -> list[bool]:
        """Check if scripts exist in the script cache.

        Args:
            *shas: SHA1 hashes to check

        Returns:
            List of booleans indicating existence
        """
        client = self.get_client(write=False)
        try:
            return cast("list[bool]", client.script_exists(*shas))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ascript_exists(self, *shas: str) -> list[bool]:
        """Check if scripts exist in the script cache asynchronously.

        Args:
            *shas: SHA1 hashes to check

        Returns:
            List of booleans indicating existence
        """
        client = self.get_async_client(write=False)
        try:
            return cast("list[bool]", await client.script_exists(*shas))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def script_flush(self, sync_type: str = "SYNC") -> bool:
        """Remove all scripts from the script cache.

        Args:
            sync_type: SYNC or ASYNC flush mode

        Returns:
            True if successful
        """
        client = self.get_client(write=True)
        try:
            return cast("bool", client.script_flush(sync_type))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ascript_flush(self, sync_type: str = "SYNC") -> bool:
        """Remove all scripts from the script cache asynchronously.

        Args:
            sync_type: SYNC or ASYNC flush mode

        Returns:
            True if successful
        """
        client = self.get_async_client(write=True)
        try:
            return cast("bool", await client.script_flush(sync_type))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def script_kill(self) -> bool:
        """Kill the currently executing Lua script.

        Returns:
            True if successful
        """
        client = self.get_client(write=True)
        try:
            return cast("bool", client.script_kill())
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ascript_kill(self) -> bool:
        """Kill the currently executing Lua script asynchronously.

        Returns:
            True if successful
        """
        client = self.get_async_client(write=True)
        try:
            return cast("bool", await client.script_kill())
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e


# =============================================================================
# RedisCacheClient - concrete implementation for redis-py
# =============================================================================

if _REDIS_AVAILABLE:
    from redis.asyncio import ConnectionPool as RedisAsyncConnectionPool
    from redis.asyncio import Redis as RedisAsyncClient

    class RedisCacheClient(KeyValueCacheClient):
        """Redis cache client using redis-py."""

        _lib = redis
        _client_class = redis.Redis
        _pool_class = redis.ConnectionPool
        _async_client_class = RedisAsyncClient
        _async_pool_class = RedisAsyncConnectionPool

else:

    class RedisCacheClient(KeyValueCacheClient):  # type: ignore[no-redef]
        """Redis cache client (requires redis-py)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            msg = "RedisCacheClient requires redis-py. Install with: pip install redis"
            raise ImportError(msg)


# =============================================================================
# ValkeyCacheClient - concrete implementation for valkey-py
# =============================================================================

if _VALKEY_AVAILABLE:
    from valkey.asyncio import ConnectionPool as ValkeyAsyncConnectionPool
    from valkey.asyncio import Valkey as ValkeyAsyncClient

    class ValkeyCacheClient(KeyValueCacheClient):
        """Valkey cache client using valkey-py."""

        _lib = valkey
        _client_class = valkey.Valkey
        _pool_class = valkey.ConnectionPool
        _async_client_class = ValkeyAsyncClient
        _async_pool_class = ValkeyAsyncConnectionPool

else:

    class ValkeyCacheClient(KeyValueCacheClient):  # type: ignore[no-redef]
        """Valkey cache client (requires valkey-py)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError("ValkeyCacheClient requires valkey-py. Install with: pip install valkey")


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "_REDIS_AVAILABLE",
    "_VALKEY_AVAILABLE",
    "KeyValueCacheClient",
    "RedisCacheClient",
    "ValkeyCacheClient",
    "_main_exceptions",
]
