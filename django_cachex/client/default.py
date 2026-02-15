"""Cache client classes for Redis-compatible backends.

Provides library-agnostic KeyValueCacheClient base class with RedisCacheClient
and ValkeyCacheClient subclasses that swap the underlying library via class attributes.
"""

from __future__ import annotations

import asyncio
import weakref
from typing import TYPE_CHECKING, Any, cast

from django.utils.module_loading import import_string

from django_cachex.compat import create_compressor, create_serializer
from django_cachex.exceptions import CompressorError, SerializerError, _main_exceptions, _ResponseError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    import builtins
    from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence

    from django_cachex.client.pipeline import Pipeline
    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT, _Set

# Try to import redis-py and/or valkey-py
_REDIS_AVAILABLE = False
_VALKEY_AVAILABLE = False

try:
    import redis

    _REDIS_AVAILABLE = True
except ImportError:
    redis = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]

try:
    import valkey

    _VALKEY_AVAILABLE = True
except ImportError:
    valkey = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]


# =============================================================================
# KeyValueCacheClient - base class (library-agnostic)
# =============================================================================


class KeyValueCacheClient:
    """Base cache client class with configurable library.

    Subclasses must set _lib, _client_class, and _pool_class class attributes.
    """

    # Class attributes - subclasses override these
    _lib: Any = None  # The library module
    _client_class: builtins.type[Any] | None = None  # e.g., valkey.Valkey
    _pool_class: builtins.type[Any] | None = None  # e.g., valkey.ConnectionPool
    _async_client_class: builtins.type[Any] | None = None  # e.g., valkey.asyncio.Valkey
    _async_pool_class: builtins.type[Any] | None = None  # e.g., valkey.asyncio.ConnectionPool

    # Default scan iteration batch size
    _default_scan_itersize: int = 100

    # Options that shouldn't be passed to the connection pool
    _CLIENT_ONLY_OPTIONS = frozenset(
        {
            "compressor",
            "serializer",
            "ignore_exceptions",
            "log_ignored_exceptions",
            "sentinels",
            "sentinel_kwargs",
            "async_pool_class",
            "reverse_key_function",
        },
    )

    def __init__(
        self,
        servers: list[str],
        serializer: str | list | builtins.type[Any] | None = None,
        pool_class: str | builtins.type[Any] | None = None,
        parser_class: str | builtins.type[Any] | None = None,
        async_pool_class: str | builtins.type[Any] | None = None,
        **options: Any,
    ) -> None:
        """Initialize the cache client."""
        # Store servers
        self._servers = servers
        self._pools: dict[int, Any] = {}

        # Async pools: WeakKeyDictionary keyed by event loop -> {server_index: pool}
        # Keyed by event loop because async pools are bound to the loop they're created on.
        # The same client instance can see multiple loops (e.g., WSGI thread that calls
        # asyncio.run() multiple times — ContextVar copies mean the same cache instance
        # is shared with the async context). WeakKeyDictionary ensures automatic cleanup
        # when a loop is GC'd.
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

    def _create_serializers(self, config: str | list | builtins.type[Any] | Any) -> list:
        """Create serializer instance(s) from config."""
        if isinstance(config, list):
            return [create_serializer(item) for item in config]
        return [create_serializer(config)]

    def _create_compressors(self, config: str | list | builtins.type[Any] | Any | None) -> list:
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
        """Encode a value for storage (serialize + compress). Plain ints pass through unchanged."""
        if isinstance(value, bool) or not isinstance(value, int):
            value = self._serializers[0].dumps(value)
            if self._compressors:
                return self._compressors[0].compress(value)
            return value
        return value

    def decode(self, value: Any) -> Any:
        """Decode a value from storage. Returns int directly if parseable, otherwise decompress + deserialize."""
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
        # Write to first server, read from any replica
        if write or len(self._servers) == 1:
            return 0
        import random

        return random.randint(1, len(self._servers) - 1)  # noqa: S311

    def _get_connection_pool(self, *, write: bool) -> Any:
        """Get a connection pool for the given operation type."""
        index = self._get_connection_pool_index(write=write)
        if index not in self._pools:
            assert self._pool_class is not None, "Subclasses must set _pool_class"  # noqa: S101
            self._pools[index] = self._pool_class.from_url(
                self._servers[index],
                **self._pool_options,
            )
        return self._pools[index]

    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get a client connection."""
        pool = self._get_connection_pool(write=write)
        assert self._client_class is not None, "Subclasses must set _client_class"  # noqa: S101
        return self._client_class(connection_pool=pool)

    # =========================================================================
    # Async Connection Pool Management
    # =========================================================================

    def _get_async_connection_pool(self, *, write: bool) -> Any:
        """Get an async connection pool, cached per event loop."""
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
        pool = self._async_pool_class.from_url(
            self._servers[index],
            **async_pool_options,
        )

        # Cache on instance for fast access
        if loop not in self._async_pools:
            self._async_pools[loop] = {}
        self._async_pools[loop][index] = pool
        return pool

    def get_async_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get an async client connection."""
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

        if timeout == 0:
            if ret := bool(client.set(key, nvalue, nx=True)):
                client.delete(key)
            return ret
        return bool(client.set(key, nvalue, nx=True, ex=timeout))

    async def aadd(self, key: KeyT, value: Any, timeout: int | None) -> bool:
        """Set a value only if the key doesn't exist, asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        if timeout == 0:
            if ret := bool(await client.set(key, nvalue, nx=True)):
                await client.delete(key)
            return ret
        return bool(await client.set(key, nvalue, nx=True, ex=timeout))

    def get(self, key: KeyT) -> Any:
        """Fetch a value from the cache."""
        client = self.get_client(key, write=False)
        val = client.get(key)
        if val is None:
            return None
        return self.decode(val)

    async def aget(self, key: KeyT) -> Any:
        """Fetch a value from the cache asynchronously."""
        client = self.get_async_client(key, write=False)
        val = await client.get(key)
        if val is None:
            return None
        return self.decode(val)

    def set(self, key: KeyT, value: Any, timeout: int | None) -> None:
        """Set a value in the cache (standard Django interface)."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        if timeout == 0:
            client.delete(key)
        else:
            client.set(key, nvalue, ex=timeout)

    async def aset(self, key: KeyT, value: Any, timeout: int | None) -> None:
        """Set a value in the cache asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        if timeout == 0:
            await client.delete(key)
        else:
            await client.set(key, nvalue, ex=timeout)

    def set_with_flags(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
    ) -> bool | Any:
        """Set a value with nx/xx/get flags.

        Returns bool for nx/xx (success status), or the previous value
        (decoded) when get=True.
        """
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        if timeout == 0:
            if get:
                return None
            return False
        result = client.set(key, nvalue, ex=timeout, nx=nx, xx=xx, get=get)
        if get:
            if result is None:
                return None
            return self.decode(result)
        return bool(result)

    async def aset_with_flags(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
    ) -> bool | Any:
        """Set a value with nx/xx/get flags asynchronously.

        Returns bool for nx/xx (success status), or the previous value
        (decoded) when get=True.
        """
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        if timeout == 0:
            if get:
                return None
            return False
        result = await client.set(key, nvalue, ex=timeout, nx=nx, xx=xx, get=get)
        if get:
            if result is None:
                return None
            return self.decode(result)
        return bool(result)

    def touch(self, key: KeyT, timeout: int | None) -> bool:
        """Update the timeout on a key."""
        client = self.get_client(key, write=True)

        if timeout is None:
            return bool(client.persist(key))
        return bool(client.expire(key, timeout))

    async def atouch(self, key: KeyT, timeout: int | None) -> bool:
        """Update the timeout on a key asynchronously."""
        client = self.get_async_client(key, write=True)

        if timeout is None:
            return bool(await client.persist(key))
        return bool(await client.expire(key, timeout))

    def delete(self, key: KeyT) -> bool:
        """Remove a key from the cache."""
        client = self.get_client(key, write=True)

        return bool(client.delete(key))

    async def adelete(self, key: KeyT) -> bool:
        """Remove a key from the cache asynchronously."""
        client = self.get_async_client(key, write=True)

        return bool(await client.delete(key))

    def get_many(self, keys: Iterable[KeyT]) -> dict[KeyT, Any]:
        """Retrieve many keys."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_client(write=False)
        results = client.mget(keys)
        return {k: self.decode(v) for k, v in zip(keys, results, strict=False) if v is not None}

    async def aget_many(self, keys: Iterable[KeyT]) -> dict[KeyT, Any]:
        """Retrieve many keys asynchronously."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_async_client(write=False)
        results = await client.mget(keys)
        return {k: self.decode(v) for k, v in zip(keys, results, strict=False) if v is not None}

    def has_key(self, key: KeyT) -> bool:
        """Check if a key exists."""
        client = self.get_client(key, write=False)

        return bool(client.exists(key))

    async def ahas_key(self, key: KeyT) -> bool:
        """Check if a key exists asynchronously."""
        client = self.get_async_client(key, write=False)

        return bool(await client.exists(key))

    def type(self, key: KeyT) -> KeyType | None:
        """Get the Redis data type of a key."""
        client = self.get_client(key, write=False)

        result = client.type(key)
        if isinstance(result, bytes):
            result = result.decode("utf-8")
        return None if result == "none" else KeyType(result)

    async def atype(self, key: KeyT) -> KeyType | None:
        """Get the Redis data type of a key asynchronously."""
        client = self.get_async_client(key, write=False)

        result = await client.type(key)
        if isinstance(result, bytes):
            result = result.decode("utf-8")
        return None if result == "none" else KeyType(result)

    def incr(self, key: KeyT, delta: int = 1) -> int:
        """Increment a value. Raises ValueError if key doesn't exist. Falls back to GET+SET on overflow."""
        client = self.get_client(key, write=True)

        try:
            if not client.exists(key):
                raise ValueError(f"Key {key!r} not found.")
            return client.incr(key, delta)
        except _ResponseError as e:
            # Handle overflow or non-integer by falling back to GET + SET
            err_msg = str(e).lower()
            if "overflow" in err_msg or "not an integer" in err_msg:
                val = client.get(key)
                if val is None:
                    raise ValueError(f"Key {key!r} not found.") from None
                new_value = self.decode(val) + delta
                nvalue = self.encode(new_value)
                client.set(key, nvalue, keepttl=True)
                return new_value
            raise

    async def aincr(self, key: KeyT, delta: int = 1) -> int:
        """Async increment. Raises ValueError if key doesn't exist. Falls back to GET+SET on overflow."""
        client = self.get_async_client(key, write=True)

        try:
            if not await client.exists(key):
                raise ValueError(f"Key {key!r} not found.")
            return await client.incr(key, delta)
        except _ResponseError as e:
            # Handle overflow or non-integer by falling back to GET + SET
            err_msg = str(e).lower()
            if "overflow" in err_msg or "not an integer" in err_msg:
                val = await client.get(key)
                if val is None:
                    raise ValueError(f"Key {key!r} not found.") from None
                new_value = self.decode(val) + delta
                nvalue = self.encode(new_value)
                await client.set(key, nvalue, keepttl=True)
                return new_value
            raise

    def set_many(self, data: Mapping[KeyT, Any], timeout: int | None) -> list:
        """Set multiple values. timeout=0 deletes keys, None sets without expiry."""
        if not data:
            return []

        client = self.get_client(write=True)
        prepared = {k: self.encode(v) for k, v in data.items()}

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
        return []

    async def aset_many(self, data: Mapping[KeyT, Any], timeout: int | None) -> list:
        """Set multiple values asynchronously."""
        if not data:
            return []

        client = self.get_async_client(write=True)
        prepared = {k: self.encode(v) for k, v in data.items()}

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
        return []

    def delete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys."""
        if not keys:
            return 0

        client = self.get_client(write=True)

        return client.delete(*keys)

    async def adelete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys asynchronously."""
        if not keys:
            return 0

        client = self.get_async_client(write=True)

        return await client.delete(*keys)

    def clear(self) -> bool:
        """Flush the database."""
        client = self.get_client(write=True)

        return bool(client.flushdb())

    async def aclear(self) -> bool:
        """Flush the database asynchronously."""
        client = self.get_async_client(write=True)

        return bool(await client.flushdb())

    def close(self, **kwargs: Any) -> None:
        """No-op. Pools live for the instance's lifetime (matches Django's BaseCache)."""

    async def aclose(self, **kwargs: Any) -> None:
        """No-op. Pools live for the instance's lifetime (matches Django's BaseCache)."""

    # =========================================================================
    # Extended Operations (beyond Django's BaseCache)
    # =========================================================================

    def ttl(self, key: KeyT) -> int | None:
        """Get TTL in seconds. Returns None if no expiry, -2 if key doesn't exist."""
        client = self.get_client(key, write=False)

        result = client.ttl(key)
        if result == -1:
            return None
        if result == -2:
            return -2
        return result

    def pttl(self, key: KeyT) -> int | None:
        """Get TTL in milliseconds. Returns None if no expiry, -2 if key doesn't exist."""
        client = self.get_client(key, write=False)

        result = client.pttl(key)
        if result == -1:
            return None
        if result == -2:
            return -2
        return result

    def expiretime(self, key: KeyT) -> int | None:
        """Get the absolute Unix timestamp (seconds) when a key will expire.

        Returns None if the key has no expiry, -2 if the key doesn't exist.
        Requires Redis 7.0+ / Valkey 7.2+.
        """
        client = self.get_client(key, write=False)

        result = client.expiretime(key)
        if result == -1:
            return None
        if result == -2:
            return -2
        return result

    def expire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry on a key."""
        client = self.get_client(key, write=True)

        return bool(client.expire(key, timeout))

    def pexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry in milliseconds."""
        client = self.get_client(key, write=True)

        return bool(client.pexpire(key, timeout))

    def expireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time."""
        client = self.get_client(key, write=True)

        return bool(client.expireat(key, when))

    def pexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time in milliseconds."""
        client = self.get_client(key, write=True)

        return bool(client.pexpireat(key, when))

    def persist(self, key: KeyT) -> bool:
        """Remove expiry from a key."""
        client = self.get_client(key, write=True)

        return bool(client.persist(key))

    async def attl(self, key: KeyT) -> int | None:
        """Get TTL in seconds asynchronously. Returns None if no expiry, -2 if key doesn't exist."""
        client = self.get_async_client(key, write=False)

        result = await client.ttl(key)
        if result == -1:
            return None
        if result == -2:
            return -2
        return result

    async def apttl(self, key: KeyT) -> int | None:
        """Get TTL in milliseconds asynchronously."""
        client = self.get_async_client(key, write=False)

        result = await client.pttl(key)
        if result == -1:
            return None
        if result == -2:
            return -2
        return result

    async def aexpiretime(self, key: KeyT) -> int | None:
        """Get the absolute Unix timestamp (seconds) when a key will expire asynchronously.

        Returns None if the key has no expiry, -2 if the key doesn't exist.
        Requires Redis 7.0+ / Valkey 7.2+.
        """
        client = self.get_async_client(key, write=False)

        result = await client.expiretime(key)
        if result == -1:
            return None
        if result == -2:
            return -2
        return result

    async def aexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry on a key asynchronously."""
        client = self.get_async_client(key, write=True)

        return bool(await client.expire(key, timeout))

    async def apexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry in milliseconds asynchronously."""
        client = self.get_async_client(key, write=True)

        return bool(await client.pexpire(key, timeout))

    async def aexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time asynchronously."""
        client = self.get_async_client(key, write=True)

        return bool(await client.expireat(key, when))

    async def apexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time in milliseconds asynchronously."""
        client = self.get_async_client(key, write=True)

        return bool(await client.pexpireat(key, when))

    async def apersist(self, key: KeyT) -> bool:
        """Remove expiry from a key asynchronously."""
        client = self.get_async_client(key, write=True)

        return bool(await client.persist(key))

    def keys(self, pattern: str) -> list[str]:
        """Get all keys matching pattern (already prefixed)."""
        client = self.get_client(write=False)

        keys_result = client.keys(pattern)
        return [k.decode() if isinstance(k, bytes) else k for k in keys_result]

    def iter_keys(self, pattern: str, itersize: int | None = None) -> Iterator[str]:
        """Iterate keys matching pattern (already prefixed)."""
        client = self.get_client(write=False)

        if itersize is None:
            itersize = self._default_scan_itersize

        for item in client.scan_iter(match=pattern, count=itersize):
            yield item.decode() if isinstance(item, bytes) else item

    def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        """Perform a single SCAN iteration returning (next_cursor, keys)."""
        client = self.get_client(write=False)

        if count is None:
            count = self._default_scan_itersize

        next_cursor, keys = client.scan(
            cursor=cursor,
            match=match,
            count=count,
            _type=_type,
        )
        decoded_keys = [k.decode() if isinstance(k, bytes) else k for k in keys]
        return next_cursor, decoded_keys

    def delete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        """Delete all keys matching pattern (already prefixed)."""
        client = self.get_client(write=True)

        if itersize is None:
            itersize = self._default_scan_itersize

        keys_list = list(client.scan_iter(match=pattern, count=itersize))
        if not keys_list:
            return 0
        return cast("int", client.delete(*keys_list))

    def rename(self, src: KeyT, dst: KeyT) -> bool:
        """Rename a key."""
        client = self.get_client(src, write=True)

        try:
            client.rename(src, dst)
        except _main_exceptions as e:
            err_msg = str(e).lower()
            if "no such key" in err_msg:
                raise ValueError(f"Key {src!r} not found") from None
            raise
        else:
            return True

    def renamenx(self, src: KeyT, dst: KeyT) -> bool:
        """Rename a key only if the destination does not exist."""
        client = self.get_client(src, write=True)

        try:
            return bool(client.renamenx(src, dst))
        except _main_exceptions as e:
            err_msg = str(e).lower()
            if "no such key" in err_msg:
                raise ValueError(f"Key {src!r} not found") from None
            raise

    async def akeys(self, pattern: str) -> list[str]:
        """Get all keys matching pattern (already prefixed) asynchronously."""
        client = self.get_async_client(write=False)

        keys_result = await client.keys(pattern)
        return [k.decode() if isinstance(k, bytes) else k for k in keys_result]

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

        keys_list = [key async for key in client.scan_iter(match=pattern, count=itersize)]
        if not keys_list:
            return 0
        return cast("int", await client.delete(*keys_list))

    async def arename(self, src: KeyT, dst: KeyT) -> bool:
        """Rename a key asynchronously."""
        client = self.get_async_client(src, write=True)

        try:
            await client.rename(src, dst)
        except _main_exceptions as e:
            err_msg = str(e).lower()
            if "no such key" in err_msg:
                raise ValueError(f"Key {src!r} not found") from None
            raise
        else:
            return True

    async def arenamenx(self, src: KeyT, dst: KeyT) -> bool:
        """Rename a key only if the destination does not exist, asynchronously."""
        client = self.get_async_client(src, write=True)

        try:
            return bool(await client.renamenx(src, dst))
        except _main_exceptions as e:
            err_msg = str(e).lower()
            if "no such key" in err_msg:
                raise ValueError(f"Key {src!r} not found") from None
            raise

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
    # Server Operations
    # =========================================================================

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get server information and statistics."""
        client = self.get_client(write=False)

        if section:
            return dict(client.info(section))
        return dict(client.info())

    def slowlog_get(self, count: int = 10) -> list[dict[str, Any]]:
        """Get slow query log entries with decoded bytes."""
        client = self.get_client(write=False)

        raw_entries = client.slowlog_get(count)

        def decode_bytes(value: Any) -> Any:
            """Decode bytes to string."""
            if isinstance(value, bytes):
                return value.decode("utf-8", errors="replace")
            return value

        def decode_command(cmd: Any) -> list[str]:
            """Decode command to list of strings."""
            if isinstance(cmd, bytes):
                return [cmd.decode("utf-8", errors="replace")]
            if isinstance(cmd, (list, tuple)):
                return [decode_bytes(arg) for arg in cmd]
            return []

        entries = []
        for entry in raw_entries:
            if isinstance(entry, dict):
                entries.append(
                    {
                        "id": entry.get("id"),
                        "start_time": entry.get("start_time"),
                        "duration": entry.get("duration"),
                        "command": decode_command(entry.get("command")),
                        "client_address": decode_bytes(entry.get("client_address")),
                        "client_name": decode_bytes(entry.get("client_name")),
                    },
                )
            elif isinstance(entry, (list, tuple)) and len(entry) >= 4:
                entries.append(
                    {
                        "id": entry[0],
                        "start_time": entry[1],
                        "duration": entry[2],
                        "command": decode_command(entry[3]) if len(entry) > 3 else [],
                        "client_address": decode_bytes(entry[4]) if len(entry) > 4 else None,
                        "client_name": decode_bytes(entry[5]) if len(entry) > 5 else None,
                    },
                )
            else:
                entries.append(entry)
        return entries

    def slowlog_len(self) -> int:
        """Get the number of entries in the slow query log."""
        client = self.get_client(write=False)

        return int(client.slowlog_len())

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        mapping: Mapping[str, Any] | None = None,
    ) -> int:
        """Set hash field(s). Use field/value for a single field, mapping for multiple."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value) if field is not None else None
        nmapping = {f: self.encode(v) for f, v in mapping.items()} if mapping else None

        return cast("int", client.hset(key, field, nvalue, mapping=nmapping))

    def hsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        """Set a hash field only if it doesn't exist."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        return bool(client.hsetnx(key, field, nvalue))

    def hget(self, key: KeyT, field: str) -> Any | None:
        """Get a hash field."""
        client = self.get_client(key, write=False)

        val = client.hget(key, field)
        return self.decode(val) if val is not None else None

    def hmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        """Get multiple hash fields."""
        client = self.get_client(key, write=False)

        values = client.hmget(key, fields)
        return [self.decode(v) if v is not None else None for v in values]

    def hgetall(self, key: KeyT) -> dict[str, Any]:
        """Get all hash fields."""
        client = self.get_client(key, write=False)

        raw = client.hgetall(key)
        return {(f.decode() if isinstance(f, bytes) else f): self.decode(v) for f, v in raw.items()}

    def hdel(self, key: KeyT, *fields: str) -> int:
        """Delete hash fields."""
        client = self.get_client(key, write=True)

        return cast("int", client.hdel(key, *fields))

    def hexists(self, key: KeyT, field: str) -> bool:
        """Check if a hash field exists."""
        client = self.get_client(key, write=False)

        return bool(client.hexists(key, field))

    def hlen(self, key: KeyT) -> int:
        """Get the number of fields in a hash."""
        client = self.get_client(key, write=False)

        return cast("int", client.hlen(key))

    def hkeys(self, key: KeyT) -> list[str]:
        """Get all field names in a hash."""
        client = self.get_client(key, write=False)

        fields = client.hkeys(key)
        return [f.decode() if isinstance(f, bytes) else f for f in fields]

    def hvals(self, key: KeyT) -> list[Any]:
        """Get all values in a hash."""
        client = self.get_client(key, write=False)

        values = client.hvals(key)
        return [self.decode(v) for v in values]

    def hincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        """Increment a hash field by an integer."""
        client = self.get_client(key, write=True)

        return cast("int", client.hincrby(key, field, amount))

    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0) -> float:
        """Increment a hash field by a float."""
        client = self.get_client(key, write=True)

        return float(client.hincrbyfloat(key, field, amount))

    async def ahset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        mapping: Mapping[str, Any] | None = None,
    ) -> int:
        """Set hash field(s) asynchronously. Use field/value for a single field, mapping for multiple."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value) if field is not None else None
        nmapping = {f: self.encode(v) for f, v in mapping.items()} if mapping else None

        return cast("int", await client.hset(key, field, nvalue, mapping=nmapping))

    async def ahsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        """Set a hash field only if it doesn't exist, asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        return bool(await client.hsetnx(key, field, nvalue))

    async def ahget(self, key: KeyT, field: str) -> Any | None:
        """Get a hash field asynchronously."""
        client = self.get_async_client(key, write=False)

        val = await client.hget(key, field)
        return self.decode(val) if val is not None else None

    async def ahmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        """Get multiple hash fields asynchronously."""
        client = self.get_async_client(key, write=False)

        values = await client.hmget(key, fields)
        return [self.decode(v) if v is not None else None for v in values]

    async def ahgetall(self, key: KeyT) -> dict[str, Any]:
        """Get all hash fields asynchronously."""
        client = self.get_async_client(key, write=False)

        raw = await client.hgetall(key)
        return {(f.decode() if isinstance(f, bytes) else f): self.decode(v) for f, v in raw.items()}

    async def ahdel(self, key: KeyT, *fields: str) -> int:
        """Delete hash fields asynchronously."""
        client = self.get_async_client(key, write=True)

        return cast("int", await client.hdel(key, *fields))

    async def ahexists(self, key: KeyT, field: str) -> bool:
        """Check if a hash field exists asynchronously."""
        client = self.get_async_client(key, write=False)

        return bool(await client.hexists(key, field))

    async def ahlen(self, key: KeyT) -> int:
        """Get the number of fields in a hash asynchronously."""
        client = self.get_async_client(key, write=False)

        return cast("int", await client.hlen(key))

    async def ahkeys(self, key: KeyT) -> list[str]:
        """Get all field names in a hash asynchronously."""
        client = self.get_async_client(key, write=False)

        fields = await client.hkeys(key)
        return [f.decode() if isinstance(f, bytes) else f for f in fields]

    async def ahvals(self, key: KeyT) -> list[Any]:
        """Get all values in a hash asynchronously."""
        client = self.get_async_client(key, write=False)

        values = await client.hvals(key)
        return [self.decode(v) for v in values]

    async def ahincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        """Increment a hash field by an integer asynchronously."""
        client = self.get_async_client(key, write=True)

        return cast("int", await client.hincrby(key, field, amount))

    async def ahincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0) -> float:
        """Increment a hash field by a float asynchronously."""
        client = self.get_async_client(key, write=True)

        return float(await client.hincrbyfloat(key, field, amount))

    # =========================================================================
    # List Operations
    # =========================================================================

    def lpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the left of a list."""
        client = self.get_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        return cast("int", client.lpush(key, *nvalues))

    def rpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the right of a list."""
        client = self.get_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        return cast("int", client.rpush(key, *nvalues))

    def lpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the left of a list."""
        client = self.get_client(key, write=True)

        if count is not None:
            vals = client.lpop(key, count)
            return [self.decode(v) for v in vals] if vals else []
        val = client.lpop(key)
        return self.decode(val) if val is not None else None

    def rpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the right of a list."""
        client = self.get_client(key, write=True)

        if count is not None:
            vals = client.rpop(key, count)
            return [self.decode(v) for v in vals] if vals else []
        val = client.rpop(key)
        return self.decode(val) if val is not None else None

    def llen(self, key: KeyT) -> int:
        """Get the length of a list."""
        client = self.get_client(key, write=False)

        return cast("int", client.llen(key))

    def lpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list."""
        client = self.get_client(key, write=False)
        encoded_value = self.encode(value)

        kwargs: dict[str, Any] = {}
        if rank is not None:
            kwargs["rank"] = rank
        if count is not None:
            kwargs["count"] = count
        if maxlen is not None:
            kwargs["maxlen"] = maxlen

        return client.lpos(key, encoded_value, **kwargs)

    def lmove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
    ) -> Any | None:
        """Atomically move an element from one list to another."""
        client = self.get_client(src, write=True)

        val = client.lmove(src, dst, wherefrom, whereto)
        return self.decode(val) if val is not None else None

    def lrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        """Get a range of elements from a list."""
        client = self.get_client(key, write=False)

        values = client.lrange(key, start, end)
        return [self.decode(v) for v in values]

    def lindex(self, key: KeyT, index: int) -> Any | None:
        """Get an element from a list by index."""
        client = self.get_client(key, write=False)

        val = client.lindex(key, index)
        return self.decode(val) if val is not None else None

    def lset(self, key: KeyT, index: int, value: Any) -> bool:
        """Set an element in a list by index."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        client.lset(key, index, nvalue)
        return True

    def lrem(self, key: KeyT, count: int, value: Any) -> int:
        """Remove elements from a list."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        return cast("int", client.lrem(key, count, nvalue))

    def ltrim(self, key: KeyT, start: int, end: int) -> bool:
        """Trim a list to the specified range."""
        client = self.get_client(key, write=True)

        client.ltrim(key, start, end)
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

        return cast("int", client.linsert(key, where, npivot, nvalue))

    def blpop(
        self,
        keys: Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list."""
        client = self.get_client(write=True)

        result = client.blpop(keys, timeout=timeout)
        if result is None:
            return None
        key_bytes, value_bytes = result
        key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
        return (key_str, self.decode(value_bytes))

    def brpop(
        self,
        keys: Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list."""
        client = self.get_client(write=True)

        result = client.brpop(keys, timeout=timeout)
        if result is None:
            return None
        key_bytes, value_bytes = result
        key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
        return (key_str, self.decode(value_bytes))

    def blmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
    ) -> Any | None:
        """Blocking atomically move element from one list to another."""
        client = self.get_client(src, write=True)

        val = client.blmove(src, dst, timeout, wherefrom, whereto)
        return self.decode(val) if val is not None else None

    async def alpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the left of a list asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        return cast("int", await client.lpush(key, *nvalues))

    async def arpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the right of a list asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        return cast("int", await client.rpush(key, *nvalues))

    async def alpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the left of a list asynchronously."""
        client = self.get_async_client(key, write=True)

        if count is not None:
            vals = await client.lpop(key, count)
            return [self.decode(v) for v in vals] if vals else []
        val = await client.lpop(key)
        return self.decode(val) if val is not None else None

    async def arpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the right of a list asynchronously."""
        client = self.get_async_client(key, write=True)

        if count is not None:
            vals = await client.rpop(key, count)
            return [self.decode(v) for v in vals] if vals else []
        val = await client.rpop(key)
        return self.decode(val) if val is not None else None

    async def allen(self, key: KeyT) -> int:
        """Get the length of a list asynchronously."""
        client = self.get_async_client(key, write=False)

        return cast("int", await client.llen(key))

    async def alpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list asynchronously."""
        client = self.get_async_client(key, write=False)
        encoded_value = self.encode(value)

        kwargs: dict[str, Any] = {}
        if rank is not None:
            kwargs["rank"] = rank
        if count is not None:
            kwargs["count"] = count
        if maxlen is not None:
            kwargs["maxlen"] = maxlen

        return await client.lpos(key, encoded_value, **kwargs)

    async def almove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
    ) -> Any | None:
        """Atomically move an element from one list to another asynchronously."""
        client = self.get_async_client(src, write=True)

        val = await client.lmove(src, dst, wherefrom, whereto)
        return self.decode(val) if val is not None else None

    async def alrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        """Get a range of elements from a list asynchronously."""
        client = self.get_async_client(key, write=False)

        values = await client.lrange(key, start, end)
        return [self.decode(v) for v in values]

    async def alindex(self, key: KeyT, index: int) -> Any | None:
        """Get an element from a list by index asynchronously."""
        client = self.get_async_client(key, write=False)

        val = await client.lindex(key, index)
        return self.decode(val) if val is not None else None

    async def alset(self, key: KeyT, index: int, value: Any) -> bool:
        """Set an element in a list by index asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        await client.lset(key, index, nvalue)
        return True

    async def alrem(self, key: KeyT, count: int, value: Any) -> int:
        """Remove elements from a list asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = self.encode(value)

        return cast("int", await client.lrem(key, count, nvalue))

    async def altrim(self, key: KeyT, start: int, end: int) -> bool:
        """Trim a list to the specified range asynchronously."""
        client = self.get_async_client(key, write=True)

        await client.ltrim(key, start, end)
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

        return cast("int", await client.linsert(key, where, npivot, nvalue))

    async def ablpop(
        self,
        keys: Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list asynchronously."""
        client = self.get_async_client(write=True)

        result = await client.blpop(keys, timeout=timeout)
        if result is None:
            return None
        key_bytes, value_bytes = result
        key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
        return (key_str, self.decode(value_bytes))

    async def abrpop(
        self,
        keys: Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list asynchronously."""
        client = self.get_async_client(write=True)

        result = await client.brpop(keys, timeout=timeout)
        if result is None:
            return None
        key_bytes, value_bytes = result
        key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
        return (key_str, self.decode(value_bytes))

    async def ablmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
    ) -> Any | None:
        """Blocking atomically move element from one list to another asynchronously."""
        client = self.get_async_client(src, write=True)

        val = await client.blmove(src, dst, timeout, wherefrom, whereto)
        return self.decode(val) if val is not None else None

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(self, key: KeyT, *members: Any) -> int:
        """Add members to a set."""
        client = self.get_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        return cast("int", client.sadd(key, *nmembers))

    def srem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a set."""
        client = self.get_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        return cast("int", client.srem(key, *nmembers))

    def smembers(self, key: KeyT) -> _Set[Any]:
        """Get all members of a set."""
        client = self.get_client(key, write=False)

        result = client.smembers(key)
        return {self.decode(v) for v in result}

    def sismember(self, key: KeyT, member: Any) -> bool:
        """Check if a value is a member of a set."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        return bool(client.sismember(key, nmember))

    def scard(self, key: KeyT) -> int:
        """Get the number of members in a set."""
        client = self.get_client(key, write=False)

        return cast("int", client.scard(key))

    def spop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Remove and return random member(s) from a set."""
        client = self.get_client(key, write=True)

        if count is None:
            val = client.spop(key)
            return self.decode(val) if val is not None else None
        vals = client.spop(key, count)
        return [self.decode(v) for v in vals] if vals else []

    def srandmember(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Get random member(s) from a set."""
        client = self.get_client(key, write=False)

        if count is None:
            val = client.srandmember(key)
            return self.decode(val) if val is not None else None
        vals = client.srandmember(key, count)
        return [self.decode(v) for v in vals] if vals else []

    def smove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        """Move a member from one set to another."""
        client = self.get_client(write=True)
        nmember = self.encode(member)

        return bool(client.smove(src, dst, nmember))

    def sdiff(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the difference of sets."""
        client = self.get_client(write=False)

        result = client.sdiff(*keys)
        return {self.decode(v) for v in result}

    def sdiffstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the difference of sets."""
        client = self.get_client(write=True)

        return cast("int", client.sdiffstore(dest, *keys))

    def sinter(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the intersection of sets."""
        client = self.get_client(write=False)

        result = client.sinter(*keys)
        return {self.decode(v) for v in result}

    def sinterstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the intersection of sets."""
        client = self.get_client(write=True)

        return cast("int", client.sinterstore(dest, *keys))

    def sunion(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the union of sets."""
        client = self.get_client(write=False)

        result = client.sunion(*keys)
        return {self.decode(v) for v in result}

    def sunionstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the union of sets."""
        client = self.get_client(write=True)

        return cast("int", client.sunionstore(dest, *keys))

    def smismember(self, key: KeyT, *members: Any) -> list[bool]:
        """Check if multiple values are members of a set."""
        client = self.get_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        result = client.smismember(key, nmembers)
        return [bool(v) for v in result]

    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, _Set[Any]]:
        """Incrementally iterate over set members."""
        client = self.get_client(key, write=False)

        next_cursor, members = client.sscan(key, cursor=cursor, match=match, count=count)
        return next_cursor, {self.decode(m) for m in members}

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
    ) -> Iterator[Any]:
        """Iterate over set members using SSCAN."""
        client = self.get_client(key, write=False)

        for member in client.sscan_iter(key, match=match, count=count):
            yield self.decode(member)

    async def asadd(self, key: KeyT, *members: Any) -> int:
        """Add members to a set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        return cast("int", await client.sadd(key, *nmembers))

    async def asrem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        return cast("int", await client.srem(key, *nmembers))

    async def asmembers(self, key: KeyT) -> _Set[Any]:
        """Get all members of a set asynchronously."""
        client = self.get_async_client(key, write=False)

        result = await client.smembers(key)
        return {self.decode(v) for v in result}

    async def asismember(self, key: KeyT, member: Any) -> bool:
        """Check if a value is a member of a set asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = self.encode(member)

        return bool(await client.sismember(key, nmember))

    async def ascard(self, key: KeyT) -> int:
        """Get the number of members in a set asynchronously."""
        client = self.get_async_client(key, write=False)

        return cast("int", await client.scard(key))

    async def aspop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Remove and return random member(s) from a set asynchronously."""
        client = self.get_async_client(key, write=True)

        if count is None:
            val = await client.spop(key)
            return self.decode(val) if val is not None else None
        vals = await client.spop(key, count)
        return [self.decode(v) for v in vals] if vals else []

    async def asrandmember(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Get random member(s) from a set asynchronously."""
        client = self.get_async_client(key, write=False)

        if count is None:
            val = await client.srandmember(key)
            return self.decode(val) if val is not None else None
        vals = await client.srandmember(key, count)
        return [self.decode(v) for v in vals] if vals else []

    async def asmove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        """Move a member from one set to another asynchronously."""
        client = self.get_async_client(write=True)
        nmember = self.encode(member)

        return bool(await client.smove(src, dst, nmember))

    async def asdiff(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the difference of sets asynchronously."""
        client = self.get_async_client(write=False)

        result = await client.sdiff(*keys)
        return {self.decode(v) for v in result}

    async def asdiffstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the difference of sets asynchronously."""
        client = self.get_async_client(write=True)

        return cast("int", await client.sdiffstore(dest, *keys))

    async def asinter(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the intersection of sets asynchronously."""
        client = self.get_async_client(write=False)

        result = await client.sinter(*keys)
        return {self.decode(v) for v in result}

    async def asinterstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the intersection of sets asynchronously."""
        client = self.get_async_client(write=True)

        return cast("int", await client.sinterstore(dest, *keys))

    async def asunion(self, keys: Sequence[KeyT]) -> _Set[Any]:
        """Return the union of sets asynchronously."""
        client = self.get_async_client(write=False)

        result = await client.sunion(*keys)
        return {self.decode(v) for v in result}

    async def asunionstore(self, dest: KeyT, keys: Sequence[KeyT]) -> int:
        """Store the union of sets asynchronously."""
        client = self.get_async_client(write=True)

        return cast("int", await client.sunionstore(dest, *keys))

    async def asmismember(self, key: KeyT, *members: Any) -> list[bool]:
        """Check if multiple values are members of a set asynchronously."""
        client = self.get_async_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        result = await client.smismember(key, nmembers)
        return [bool(v) for v in result]

    async def asscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, _Set[Any]]:
        """Incrementally iterate over set members asynchronously."""
        client = self.get_async_client(key, write=False)

        next_cursor, members = await client.sscan(key, cursor=cursor, match=match, count=count)
        return next_cursor, {self.decode(m) for m in members}

    async def asscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
    ) -> AsyncIterator[Any]:
        """Iterate over set members using SSCAN asynchronously."""
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

        return cast("int", client.zadd(key, scored_mapping, nx=nx, xx=xx, gt=gt, lt=lt, ch=ch))

    def zrem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a sorted set."""
        client = self.get_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        return cast("int", client.zrem(key, *nmembers))

    def zscore(self, key: KeyT, member: Any) -> float | None:
        """Get the score of a member."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        result = client.zscore(key, nmember)
        return float(result) if result is not None else None

    def zrank(self, key: KeyT, member: Any) -> int | None:
        """Get the rank of a member (0-based)."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        return client.zrank(key, nmember)

    def zrevrank(self, key: KeyT, member: Any) -> int | None:
        """Get the reverse rank of a member."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        return client.zrevrank(key, nmember)

    def zcard(self, key: KeyT) -> int:
        """Get the number of members in a sorted set."""
        client = self.get_client(key, write=False)

        return cast("int", client.zcard(key))

    def zcount(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Count members in a score range."""
        client = self.get_client(key, write=False)

        return cast("int", client.zcount(key, min_score, max_score))

    def zincrby(self, key: KeyT, amount: float, member: Any) -> float:
        """Increment the score of a member."""
        client = self.get_client(key, write=True)
        nmember = self.encode(member)

        return float(client.zincrby(key, amount, nmember))

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

        result = client.zrange(key, start, end, withscores=withscores)
        if withscores:
            return [(self.decode(m), float(s)) for m, s in result]
        return [self.decode(m) for m in result]

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

        result = client.zrevrange(key, start, end, withscores=withscores)
        if withscores:
            return [(self.decode(m), float(s)) for m, s in result]
        return [self.decode(m) for m in result]

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

    def zremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        """Remove members by rank range."""
        client = self.get_client(key, write=True)

        return cast("int", client.zremrangebyrank(key, start, end))

    def zremrangebyscore(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Remove members by score range."""
        client = self.get_client(key, write=True)

        return cast("int", client.zremrangebyscore(key, min_score, max_score))

    def zpopmin(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        client = self.get_client(key, write=True)

        result = client.zpopmin(key, count)
        return [(self.decode(m), float(s)) for m, s in result]

    def zpopmax(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        client = self.get_client(key, write=True)

        result = client.zpopmax(key, count)
        return [(self.decode(m), float(s)) for m, s in result]

    def zmscore(self, key: KeyT, *members: Any) -> list[float | None]:
        """Get scores for multiple members."""
        client = self.get_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        results = client.zmscore(key, nmembers)
        return [float(r) if r is not None else None for r in results]

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

        return cast("int", await client.zadd(key, scored_mapping, nx=nx, xx=xx, gt=gt, lt=lt, ch=ch))

    async def azrem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a sorted set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        return cast("int", await client.zrem(key, *nmembers))

    async def azscore(self, key: KeyT, member: Any) -> float | None:
        """Get the score of a member asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = self.encode(member)

        result = await client.zscore(key, nmember)
        return float(result) if result is not None else None

    async def azrank(self, key: KeyT, member: Any) -> int | None:
        """Get the rank of a member (0-based) asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = self.encode(member)

        return await client.zrank(key, nmember)

    async def azrevrank(self, key: KeyT, member: Any) -> int | None:
        """Get the reverse rank of a member asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = self.encode(member)

        return await client.zrevrank(key, nmember)

    async def azcard(self, key: KeyT) -> int:
        """Get the number of members in a sorted set asynchronously."""
        client = self.get_async_client(key, write=False)

        return cast("int", await client.zcard(key))

    async def azcount(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Count members in a score range asynchronously."""
        client = self.get_async_client(key, write=False)

        return cast("int", await client.zcount(key, min_score, max_score))

    async def azincrby(self, key: KeyT, amount: float, member: Any) -> float:
        """Increment the score of a member asynchronously."""
        client = self.get_async_client(key, write=True)
        nmember = self.encode(member)

        return float(await client.zincrby(key, amount, nmember))

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

        result = await client.zrange(key, start, end, withscores=withscores)
        if withscores:
            return [(self.decode(m), float(s)) for m, s in result]
        return [self.decode(m) for m in result]

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

        result = await client.zrevrange(key, start, end, withscores=withscores)
        if withscores:
            return [(self.decode(m), float(s)) for m, s in result]
        return [self.decode(m) for m in result]

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

    async def azremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        """Remove members by rank range asynchronously."""
        client = self.get_async_client(key, write=True)

        return cast("int", await client.zremrangebyrank(key, start, end))

    async def azremrangebyscore(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Remove members by score range asynchronously."""
        client = self.get_async_client(key, write=True)

        return cast("int", await client.zremrangebyscore(key, min_score, max_score))

    async def azpopmin(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores asynchronously."""
        client = self.get_async_client(key, write=True)

        result = await client.zpopmin(key, count)
        return [(self.decode(m), float(s)) for m, s in result]

    async def azpopmax(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores asynchronously."""
        client = self.get_async_client(key, write=True)

        result = await client.zpopmax(key, count)
        return [(self.decode(m), float(s)) for m, s in result]

    async def azmscore(self, key: KeyT, *members: Any) -> list[float | None]:
        """Get scores for multiple members asynchronously."""
        client = self.get_async_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        results = await client.zmscore(key, nmembers)
        return [float(r) if r is not None else None for r in results]

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
        """Add an entry to a stream."""
        client = self.get_client(key, write=True)
        encoded_fields = {k: self.encode(v) for k, v in fields.items()}

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

    def xlen(self, key: KeyT) -> int:
        """Get the number of entries in a stream."""
        client = self.get_client(key, write=False)

        return cast("int", client.xlen(key))

    def xrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in ascending order."""
        client = self.get_client(key, write=False)

        results = client.xrange(key, min=start, max=end, count=count)
        return [
            (
                entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
            )
            for entry_id, fields in results
        ]

    def xrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in descending order."""
        client = self.get_client(key, write=False)

        results = client.xrevrange(key, max=end, min=start, count=count)
        return [
            (
                entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
            )
            for entry_id, fields in results
        ]

    def xread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from one or more streams."""
        client = self.get_client(write=False)

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

    def xtrim(
        self,
        key: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
    ) -> int:
        """Trim a stream to a maximum length or minimum ID."""
        client = self.get_client(key, write=True)

        return cast(
            "int",
            client.xtrim(key, maxlen=maxlen, approximate=approximate, minid=minid, limit=limit),
        )

    def xdel(self, key: KeyT, *entry_ids: str) -> int:
        """Delete entries from a stream."""
        client = self.get_client(key, write=True)

        return cast("int", client.xdel(key, *entry_ids))

    def xinfo_stream(self, key: KeyT, full: bool = False) -> dict[str, Any]:
        """Get information about a stream."""
        client = self.get_client(key, write=False)

        if full:
            return client.xinfo_stream(key, full=True)
        return client.xinfo_stream(key)

    def xinfo_groups(self, key: KeyT) -> list[dict[str, Any]]:
        """Get information about consumer groups for a stream."""
        client = self.get_client(key, write=False)

        return client.xinfo_groups(key)

    def xinfo_consumers(self, key: KeyT, group: str) -> list[dict[str, Any]]:
        """Get information about consumers in a group."""
        client = self.get_client(key, write=False)

        return client.xinfo_consumers(key, group)

    def xgroup_create(
        self,
        key: KeyT,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> bool:
        """Create a consumer group."""
        client = self.get_client(key, write=True)

        client.xgroup_create(key, group, id=entry_id, mkstream=mkstream, entries_read=entries_read)
        return True

    def xgroup_destroy(self, key: KeyT, group: str) -> int:
        """Destroy a consumer group."""
        client = self.get_client(key, write=True)

        return cast("int", client.xgroup_destroy(key, group))

    def xgroup_setid(
        self,
        key: KeyT,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
    ) -> bool:
        """Set the last delivered ID for a consumer group."""
        client = self.get_client(key, write=True)

        client.xgroup_setid(key, group, id=entry_id, entries_read=entries_read)
        return True

    def xgroup_delconsumer(self, key: KeyT, group: str, consumer: str) -> int:
        """Remove a consumer from a group."""
        client = self.get_client(key, write=True)

        return cast("int", client.xgroup_delconsumer(key, group, consumer))

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from streams as a consumer group member."""
        client = self.get_client(write=True)

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

    def xack(self, key: KeyT, group: str, *entry_ids: str) -> int:
        """Acknowledge message processing."""
        client = self.get_client(key, write=True)

        return cast("int", client.xack(key, group, *entry_ids))

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
        """Get pending entries information."""
        client = self.get_client(key, write=False)

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
        """Claim pending messages."""
        client = self.get_client(key, write=True)

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
        """Auto-claim pending messages that have been idle."""
        client = self.get_client(key, write=True)

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

    async def axlen(self, key: KeyT) -> int:
        """Get the number of entries in a stream asynchronously."""
        client = self.get_async_client(key, write=False)

        return cast("int", await client.xlen(key))

    async def axrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in ascending order asynchronously."""
        client = self.get_async_client(key, write=False)

        results = await client.xrange(key, min=start, max=end, count=count)
        return [
            (
                entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
            )
            for entry_id, fields in results
        ]

    async def axrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in descending order asynchronously."""
        client = self.get_async_client(key, write=False)

        results = await client.xrevrange(key, max=end, min=start, count=count)
        return [
            (
                entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                {k.decode() if isinstance(k, bytes) else k: self.decode(v) for k, v in fields.items()},
            )
            for entry_id, fields in results
        ]

    async def axread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from one or more streams asynchronously."""
        client = self.get_async_client(write=False)

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

        return cast(
            "int",
            await client.xtrim(key, maxlen=maxlen, approximate=approximate, minid=minid, limit=limit),
        )

    async def axdel(self, key: KeyT, *entry_ids: str) -> int:
        """Delete entries from a stream asynchronously."""
        client = self.get_async_client(key, write=True)

        return cast("int", await client.xdel(key, *entry_ids))

    async def axinfo_stream(self, key: KeyT, full: bool = False) -> dict[str, Any]:
        """Get information about a stream asynchronously."""
        client = self.get_async_client(key, write=False)

        if full:
            return await client.xinfo_stream(key, full=True)
        return await client.xinfo_stream(key)

    async def axinfo_groups(self, key: KeyT) -> list[dict[str, Any]]:
        """Get information about consumer groups for a stream asynchronously."""
        client = self.get_async_client(key, write=False)

        return await client.xinfo_groups(key)

    async def axinfo_consumers(self, key: KeyT, group: str) -> list[dict[str, Any]]:
        """Get information about consumers in a group asynchronously."""
        client = self.get_async_client(key, write=False)

        return await client.xinfo_consumers(key, group)

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

        await client.xgroup_create(key, group, id=entry_id, mkstream=mkstream, entries_read=entries_read)
        return True

    async def axgroup_destroy(self, key: KeyT, group: str) -> int:
        """Destroy a consumer group asynchronously."""
        client = self.get_async_client(key, write=True)

        return cast("int", await client.xgroup_destroy(key, group))

    async def axgroup_setid(
        self,
        key: KeyT,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
    ) -> bool:
        """Set the last delivered ID for a consumer group asynchronously."""
        client = self.get_async_client(key, write=True)

        await client.xgroup_setid(key, group, id=entry_id, entries_read=entries_read)
        return True

    async def axgroup_delconsumer(self, key: KeyT, group: str, consumer: str) -> int:
        """Remove a consumer from a group asynchronously."""
        client = self.get_async_client(key, write=True)

        return cast("int", await client.xgroup_delconsumer(key, group, consumer))

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

    async def axack(self, key: KeyT, group: str, *entry_ids: str) -> int:
        """Acknowledge message processing asynchronously."""
        client = self.get_async_client(key, write=True)

        return cast("int", await client.xack(key, group, *entry_ids))

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

    # =========================================================================
    # Lua Scripting Operations
    # =========================================================================

    def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: Any,
    ) -> Any:
        """Execute a Lua script server-side."""
        client = self.get_client(write=True)
        return client.eval(script, numkeys, *keys_and_args)

    async def aeval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: Any,
    ) -> Any:
        """Execute a Lua script server-side asynchronously."""
        client = self.get_async_client(write=True)
        return await client.eval(script, numkeys, *keys_and_args)


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
    "KeyValueCacheClient",
    "RedisCacheClient",
    "ValkeyCacheClient",
]
