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
- _ignore_exceptions, _log_ignored_exceptions, _logger: Exception handling
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
    from django_cachex.types import AbsExpiryT, EncodableT, ExpiryT, KeyT

# Try to import redis-py and/or valkey-py
_REDIS_AVAILABLE = False
_VALKEY_AVAILABLE = False
_exception_list: list[type[Exception]] = [socket.timeout]

try:
    import redis
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import ResponseError as RedisResponseError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    _REDIS_AVAILABLE = True
    _exception_list.extend([RedisConnectionError, RedisTimeoutError, RedisResponseError])
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
        }
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
        serializer_config = options.get(
            "serializer",
            "django_cachex.serializers.pickle.PickleSerializer",
        )
        self._serializers = self._create_serializers(serializer_config)

        # Exception handling configuration
        self._ignore_exceptions = options.get("ignore_exceptions", False)
        self._log_ignored_exceptions = options.get("log_ignored_exceptions", False)
        self._logger = logger if self._log_ignored_exceptions else None

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

    def encode(self, value: EncodableT) -> bytes | int:
        """Encode a value for storage (serialize + compress)."""
        if isinstance(value, bool) or not isinstance(value, int):
            value = self._serializers[0].dumps(value)
            if self._compressors:
                return self._compressors[0].compress(value)
            return value  # type: ignore[return-value]
        return value

    def decode(self, value: EncodableT) -> Any:
        """Decode a value from storage (decompress + deserialize)."""
        try:
            return int(value)
        except (ValueError, TypeError):
            value = self._decompress(value)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
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
            self._pools[index] = self._pool_class.from_url(  # type: ignore[attr-defined]
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
        pool = self._async_pool_class.from_url(  # type: ignore[attr-defined]
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

    def add(self, key: KeyT, value: EncodableT, timeout: int | None) -> bool:
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

    async def aadd(self, key: KeyT, value: EncodableT, timeout: int | None) -> bool:
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

    def get(self, key: KeyT, default: Any = None) -> Any:
        """Fetch a value from the cache."""
        client = self.get_client(key, write=False)

        try:
            val = client.get(key)
        except _main_exceptions as e:
            if self._ignore_exceptions:
                if self._log_ignored_exceptions and self._logger is not None:
                    self._logger.exception("Exception ignored")
                return default
            raise ConnectionInterruptedError(connection=client) from e

        if val is None:
            return default
        return self.decode(val)

    async def aget(self, key: KeyT, default: Any = None) -> Any:
        """Fetch a value from the cache asynchronously."""
        client = self.get_async_client(key, write=False)

        try:
            val = await client.get(key)
        except _main_exceptions as e:
            if self._ignore_exceptions:
                if self._log_ignored_exceptions and self._logger is not None:
                    self._logger.exception("Exception ignored")
                return default
            raise ConnectionInterruptedError(connection=client) from e

        if val is None:
            return default
        return self.decode(val)

    def set(self, key: KeyT, value: EncodableT, timeout: int | None) -> None:
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

    async def aset(self, key: KeyT, value: EncodableT, timeout: int | None) -> None:
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
        self, key: KeyT, value: EncodableT, timeout: int | None, *, nx: bool = False, xx: bool = False
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

    def set_many(self, data: Mapping[KeyT, EncodableT], timeout: int | None) -> list:
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

    async def aset_many(self, data: Mapping[KeyT, EncodableT], timeout: int | None) -> list:
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

    def hset(self, key: KeyT, field: str, value: EncodableT) -> int:
        """Set a hash field."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            return cast("int", client.hset(key, field, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def hsetnx(self, key: KeyT, field: str, value: EncodableT) -> bool:
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

    def hmset(self, key: KeyT, mapping: Mapping[str, EncodableT]) -> bool:
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

    async def ahset(self, key: KeyT, field: str, value: EncodableT) -> int:
        """Set a hash field asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        try:
            return cast("int", await client.hset(key, field, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def ahsetnx(self, key: KeyT, field: str, value: EncodableT) -> bool:
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

    async def ahmset(self, key: KeyT, mapping: Mapping[str, EncodableT]) -> bool:
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

    def lpush(self, key: KeyT, *values: EncodableT) -> int:
        """Push values to the left of a list."""
        client = self.get_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        try:
            return cast("int", client.lpush(key, *nvalues))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def rpush(self, key: KeyT, *values: EncodableT) -> int:
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
        value: EncodableT,
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

    def lset(self, key: KeyT, index: int, value: EncodableT) -> bool:
        """Set an element in a list by index."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            client.lset(key, index, nvalue)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return True

    def lrem(self, key: KeyT, count: int, value: EncodableT) -> int:
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
        pivot: EncodableT,
        value: EncodableT,
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

    async def alpush(self, key: KeyT, *values: EncodableT) -> int:
        """Push values to the left of a list asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        try:
            return cast("int", await client.lpush(key, *nvalues))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def arpush(self, key: KeyT, *values: EncodableT) -> int:
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
        value: EncodableT,
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

    async def alset(self, key: KeyT, index: int, value: EncodableT) -> bool:
        """Set an element in a list by index asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        try:
            await client.lset(key, index, nvalue)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e
        else:
            return True

    async def alrem(self, key: KeyT, count: int, value: EncodableT) -> int:
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
        pivot: EncodableT,
        value: EncodableT,
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

    def sadd(self, key: KeyT, *members: EncodableT) -> int:
        """Add members to a set."""
        client = self.get_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", client.sadd(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def srem(self, key: KeyT, *members: EncodableT) -> int:
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

    def sismember(self, key: KeyT, member: EncodableT) -> bool:
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

    def smove(self, src: KeyT, dst: KeyT, member: EncodableT) -> bool:
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

    def smismember(self, key: KeyT, *members: EncodableT) -> list[bool]:
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

    async def asadd(self, key: KeyT, *members: EncodableT) -> int:
        """Add members to a set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", await client.sadd(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def asrem(self, key: KeyT, *members: EncodableT) -> int:
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

    async def asismember(self, key: KeyT, member: EncodableT) -> bool:
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

    async def asmove(self, src: KeyT, dst: KeyT, member: EncodableT) -> bool:
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

    async def asmismember(self, key: KeyT, *members: EncodableT) -> list[bool]:
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
        mapping: Mapping[EncodableT, float],
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

    def zrem(self, key: KeyT, *members: EncodableT) -> int:
        """Remove members from a sorted set."""
        client = self.get_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", client.zrem(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zscore(self, key: KeyT, member: EncodableT) -> float | None:
        """Get the score of a member."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        try:
            result = client.zscore(key, nmember)
            return float(result) if result is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zrank(self, key: KeyT, member: EncodableT) -> int | None:
        """Get the rank of a member (0-based)."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        try:
            return client.zrank(key, nmember)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    def zrevrank(self, key: KeyT, member: EncodableT) -> int | None:
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

    def zincrby(self, key: KeyT, amount: float, member: EncodableT) -> float:
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

    def zmscore(self, key: KeyT, *members: EncodableT) -> list[float | None]:
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
        mapping: Mapping[EncodableT, float],
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

    async def azrem(self, key: KeyT, *members: EncodableT) -> int:
        """Remove members from a sorted set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", await client.zrem(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azscore(self, key: KeyT, member: EncodableT) -> float | None:
        """Get the score of a member asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = self.encode(member)

        try:
            result = await client.zscore(key, nmember)
            return float(result) if result is not None else None
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azrank(self, key: KeyT, member: EncodableT) -> int | None:
        """Get the rank of a member (0-based) asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = self.encode(member)

        try:
            return await client.zrank(key, nmember)
        except _main_exceptions as e:
            raise ConnectionInterruptedError(connection=client) from e

    async def azrevrank(self, key: KeyT, member: EncodableT) -> int | None:
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

    async def azincrby(self, key: KeyT, amount: float, member: EncodableT) -> float:
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

    async def azmscore(self, key: KeyT, *members: EncodableT) -> list[float | None]:
        """Get scores for multiple members asynchronously."""
        client = self.get_async_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        try:
            results = await client.zmscore(key, nmembers)
            return [float(r) if r is not None else None for r in results]
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
