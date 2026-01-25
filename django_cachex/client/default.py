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
- _serializer: Serializer instance (Django compatibility)
- _pool_options: Options passed to connection pool

Extended attributes (our additions):
- _options: Full options dict
- _compressors: List of compressor instances
- _serializers: List of serializer instances (for fallback)
- _ignore_exceptions, _log_ignored_exceptions, _logger: Exception handling
"""

from __future__ import annotations

import logging
import re
import socket
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any, cast

from django.conf import settings
from django.utils.module_loading import import_string

from django_cachex.compat import create_compressor, create_serializer
from django_cachex.exceptions import CompressorError, ConnectionInterrupted, SerializerError
from django_cachex.types import AbsExpiryT, EncodableT, ExpiryT, KeyT

# Alias builtin set type to avoid shadowing by the set() method
_Set = set

if TYPE_CHECKING:
    from django_cachex.client.pipeline import Pipeline

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
    redis = None  # type: ignore[assignment]

try:
    import valkey
    from valkey.exceptions import ConnectionError as ValkeyConnectionError
    from valkey.exceptions import ResponseError as ValkeyResponseError
    from valkey.exceptions import TimeoutError as ValkeyTimeoutError

    _VALKEY_AVAILABLE = True
    _exception_list.extend([ValkeyConnectionError, ValkeyTimeoutError, ValkeyResponseError])
except ImportError:
    valkey = None  # type: ignore[assignment]

_main_exceptions = tuple(_exception_list)

# ResponseError tuple for incr handling
_response_errors: list[type[Exception]] = []
if _REDIS_AVAILABLE:
    _response_errors.append(RedisResponseError)
if _VALKEY_AVAILABLE:
    _response_errors.append(ValkeyResponseError)
_ResponseError = tuple(_response_errors) if _response_errors else (Exception,)

# Regex for escaping glob special characters
_special_re = re.compile("([*?[])")


def glob_escape(s: str) -> str:
    """Escape glob special characters in a string."""
    return _special_re.sub(r"[\1]", s)


logger = logging.getLogger(__name__)


# =============================================================================
# KeyValueCacheClient - base class (library-agnostic)
# =============================================================================


class KeyValueCacheClient:
    """Base cache client class with configurable library.

    This class replicates Django's RedisCacheClient structure but uses class
    attributes to allow subclasses to swap the underlying library.

    Subclasses must set:
    - _lib: The library module (e.g., redis or valkey)
    - _client_class: The client class (e.g., redis.Redis)
    - _pool_class: The connection pool class

    Internal attributes match Django's RedisCacheClient for compatibility.
    """

    # Class attributes - subclasses override these
    _lib: Any = None  # The library module
    _client_class: type = None  # type: ignore[assignment]  # e.g., redis.Redis
    _pool_class: type = None  # type: ignore[assignment]  # e.g., redis.ConnectionPool

    # Default scan iteration batch size
    _default_scan_itersize: int = 10

    # Options that shouldn't be passed to the connection pool
    _CLIENT_ONLY_OPTIONS = frozenset({
        "compressor",
        "serializer",
        "ignore_exceptions",
        "log_ignored_exceptions",
        "close_connection",
        "sentinels",
        "sentinel_kwargs",
    })

    def __init__(
        self,
        servers,
        serializer=None,
        pool_class=None,
        parser_class=None,
        **options,
    ):
        """Initialize the cache client.

        Args:
            servers: List of server URLs
            serializer: Serializer instance or import path (Django compatibility)
            pool_class: Connection pool class or import path
            parser_class: Parser class or import path
            **options: Additional options passed to connection pool
        """
        # Store servers
        self._servers = servers
        self._pools: dict[int, Any] = {}

        # Set up pool class (can be overridden via argument)
        if isinstance(pool_class, str):
            pool_class = import_string(pool_class)
        self._pool_class = pool_class or self.__class__._pool_class

        # Set up serializer (Django compatibility - single serializer)
        if isinstance(serializer, str):
            serializer = import_string(serializer)
        if callable(serializer) and not hasattr(serializer, "dumps"):
            serializer = serializer()
        self._serializer = serializer

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
        self._ignore_exceptions = options.get(
            "ignore_exceptions",
            getattr(settings, "DJANGO_REDIS_IGNORE_EXCEPTIONS", False),
        )
        self._log_ignored_exceptions = options.get(
            "log_ignored_exceptions",
            getattr(settings, "DJANGO_REDIS_LOG_IGNORED_EXCEPTIONS", False),
        )
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
            return [create_compressor("django_cachex.compressors.identity.IdentityCompressor")]
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

    def _has_compression_enabled(self) -> bool:
        """Check if compression is enabled."""
        if not self._compressors:
            return False
        primary = self._compressors[0]
        return primary.__class__.__name__ != "IdentityCompressor"

    # =========================================================================
    # Encoding/Decoding
    # =========================================================================

    def encode(self, value: EncodableT) -> bytes | int:
        """Encode a value for storage (serialize + compress)."""
        if isinstance(value, bool) or not isinstance(value, int):
            value = self._serializers[0].dumps(value)
            return self._compressors[0].compress(value)
        return value

    def decode(self, value: EncodableT) -> Any:
        """Decode a value from storage (decompress + deserialize)."""
        try:
            return int(value)
        except (ValueError, TypeError):
            value = self._decompress(value)
            return self._deserialize(value)

    # =========================================================================
    # Connection Pool Management (matches Django's structure)
    # =========================================================================

    def _get_connection_pool_index(self, write: bool) -> int:
        """Get the pool index for read/write operations."""
        # Write to first server, read from any
        if write or len(self._servers) == 1:
            return 0
        import random
        return random.randint(1, len(self._servers) - 1)

    def _get_connection_pool(self, write: bool):
        """Get a connection pool for the given operation type."""
        index = self._get_connection_pool_index(write)
        if index not in self._pools:
            self._pools[index] = self._pool_class.from_url(
                self._servers[index],
                **self._pool_options,
            )
        return self._pools[index]

    def get_client(self, key: KeyT | None = None, *, write: bool = False):
        """Get a client connection.

        Args:
            key: Optional key (for sharding implementations)
            write: Whether this is a write operation
        """
        pool = self._get_connection_pool(write)
        return self._client_class(connection_pool=pool)

    # =========================================================================
    # Core Cache Operations
    # =========================================================================

    def add(self, key, value, timeout):
        """Set a value only if the key doesn't exist."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            if timeout == 0:
                if ret := bool(client.set(key, nvalue, nx=True)):
                    client.delete(key)
                return ret
            if timeout is None:
                return bool(client.set(key, nvalue, nx=True))
            timeout_ms = int(timeout * 1000)
            return bool(client.set(key, nvalue, nx=True, px=timeout_ms))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def get(self, key, default=None):
        """Fetch a value from the cache."""
        client = self.get_client(key, write=False)

        try:
            val = client.get(key)
        except _main_exceptions as e:
            if self._ignore_exceptions:
                if self._log_ignored_exceptions:
                    self._logger.exception("Exception ignored")
                return default
            raise ConnectionInterrupted(connection=client) from e

        if val is None:
            return default
        return self.decode(val)

    def set(self, key, value, timeout):
        """Set a value in the cache (standard Django interface)."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            if timeout == 0:
                client.delete(key)
            elif timeout is None:
                client.set(key, nvalue)
            else:
                timeout_ms = int(timeout * 1000)
                client.set(key, nvalue, px=timeout_ms)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def set_with_flags(self, key, value, timeout, *, nx: bool = False, xx: bool = False) -> bool:
        """Set a value with nx/xx flags, returning success status."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            if timeout == 0:
                return False
            if timeout is None:
                result = client.set(key, nvalue, nx=nx, xx=xx)
            else:
                timeout_ms = int(timeout * 1000)
                result = client.set(key, nvalue, px=timeout_ms, nx=nx, xx=xx)
            return bool(result)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def touch(self, key, timeout):
        """Update the timeout on a key."""
        client = self.get_client(key, write=True)

        try:
            if timeout is None:
                return bool(client.persist(key))
            timeout_ms = int(timeout * 1000)
            return bool(client.pexpire(key, timeout_ms))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def delete(self, key):
        """Remove a key from the cache."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.delete(key))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def get_many(self, keys):
        """Retrieve many keys."""
        if not keys:
            return {}

        client = self.get_client(write=False)

        try:
            results = client.mget(keys)
            return {
                k: self.decode(v)
                for k, v in zip(keys, results, strict=False)
                if v is not None
            }
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def has_key(self, key):
        """Check if a key exists."""
        client = self.get_client(key, write=False)

        try:
            return bool(client.exists(key))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def incr(self, key, delta=1):
        """Increment a value.

        Uses Redis INCR for atomic increments, but falls back to GET+SET
        for values that would overflow Redis's 64-bit signed integer limit
        or for non-integer (serialized) values.
        """
        client = self.get_client(key, write=True)

        try:
            if not client.exists(key):
                raise ValueError("Key '%s' not found." % key)
            return client.incr(key, delta)
        except _ResponseError as e:
            # Handle overflow or non-integer by falling back to GET + SET
            err_msg = str(e).lower()
            if "overflow" in err_msg or "not an integer" in err_msg:
                try:
                    val = client.get(key)
                    if val is None:
                        raise ValueError("Key '%s' not found." % key)
                    # Decode the value, add delta, and set back
                    new_value = self.decode(val) + delta
                    nvalue = self.encode(new_value)
                    client.set(key, nvalue, keepttl=True)
                    return new_value
                except _main_exceptions as e2:
                    raise ConnectionInterrupted(connection=client) from e2
            raise ConnectionInterrupted(connection=client) from e
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def set_many(self, data, timeout):
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
                timeout_ms = int(timeout * 1000)
                for key in prepared:
                    pipe.pexpire(key, timeout_ms)
                pipe.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return []

    def delete_many(self, keys):
        """Remove multiple keys."""
        if not keys:
            return 0

        client = self.get_client(write=True)

        try:
            return client.delete(*keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def clear(self):
        """Flush the database."""
        client = self.get_client(write=True)

        try:
            return bool(client.flushdb())
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def close(self, **kwargs):
        """Close connections if configured."""
        close_flag = self._options.get(
            "close_connection",
            getattr(settings, "DJANGO_REDIS_CLOSE_CONNECTION", False),
        )
        if close_flag:
            for pool in self._pools.values():
                pool.disconnect()
            self._pools.clear()

    # =========================================================================
    # Extended Operations (beyond Django's BaseCache)
    # =========================================================================

    def ttl(self, key: KeyT) -> int | None:
        """Get TTL in seconds. Returns None if no expiry, -2 if key doesn't exist."""
        client = self.get_client(key, write=False)

        try:
            result = client.ttl(key)
            if result == -1:
                return None
            if result == -2:
                return -2
            return result
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def pttl(self, key: KeyT) -> int | None:
        """Get TTL in milliseconds."""
        client = self.get_client(key, write=False)

        try:
            result = client.pttl(key)
            if result == -1:
                return None
            if result == -2:
                return -2
            return result
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def expire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry on a key."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.expire(key, timeout))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def pexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry in milliseconds."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.pexpire(key, timeout))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def expireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.expireat(key, when))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def pexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time in milliseconds."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.pexpireat(key, when))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def persist(self, key: KeyT) -> bool:
        """Remove expiry from a key."""
        client = self.get_client(key, write=True)

        try:
            return bool(client.persist(key))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def keys(self, pattern: str) -> list[str]:
        """Get all keys matching pattern (already prefixed)."""
        client = self.get_client(write=False)

        try:
            keys_result = client.keys(pattern)
            return [k.decode() if isinstance(k, bytes) else k for k in keys_result]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

    def incr_version(self, key: KeyT, delta: int = 1) -> int:
        """Increment the version of a key."""
        # This is typically handled at the Cache level, not CacheClient
        raise NotImplementedError("incr_version should be called on Cache, not CacheClient")

    def lock(
        self,
        key: str,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        thread_local: bool = True,
    ):
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
            raise ConnectionInterrupted(connection=client) from e

    def hsetnx(self, key: KeyT, field: str, value: EncodableT) -> bool:
        """Set a hash field only if it doesn't exist."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            return bool(client.hsetnx(key, field, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hget(self, key: KeyT, field: str) -> Any | None:
        """Get a hash field."""
        client = self.get_client(key, write=False)

        try:
            val = client.hget(key, field)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hmset(self, key: KeyT, mapping: Mapping[str, EncodableT]) -> bool:
        """Set multiple hash fields."""
        client = self.get_client(key, write=True)
        nmap = {f: self.encode(v) for f, v in mapping.items()}

        try:
            # hmset is deprecated, use hset with mapping
            client.hset(key, mapping=nmap)
            return True
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        """Get multiple hash fields."""
        client = self.get_client(key, write=False)

        try:
            values = client.hmget(key, fields)
            return [self.decode(v) if v is not None else None for v in values]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hgetall(self, key: KeyT) -> dict[str, Any]:
        """Get all hash fields."""
        client = self.get_client(key, write=False)

        try:
            raw = client.hgetall(key)
            return {
                (f.decode() if isinstance(f, bytes) else f): self.decode(v)
                for f, v in raw.items()
            }
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hdel(self, key: KeyT, *fields: str) -> int:
        """Delete hash fields."""
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.hdel(key, *fields))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hexists(self, key: KeyT, field: str) -> bool:
        """Check if a hash field exists."""
        client = self.get_client(key, write=False)

        try:
            return bool(client.hexists(key, field))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hlen(self, key: KeyT) -> int:
        """Get the number of fields in a hash."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.hlen(key))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hkeys(self, key: KeyT) -> list[str]:
        """Get all field names in a hash."""
        client = self.get_client(key, write=False)

        try:
            fields = client.hkeys(key)
            return [f.decode() if isinstance(f, bytes) else f for f in fields]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hvals(self, key: KeyT) -> list[Any]:
        """Get all values in a hash."""
        client = self.get_client(key, write=False)

        try:
            values = client.hvals(key)
            return [self.decode(v) for v in values]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        """Increment a hash field by an integer."""
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.hincrby(key, field, amount))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0) -> float:
        """Increment a hash field by a float."""
        client = self.get_client(key, write=True)

        try:
            return float(client.hincrbyfloat(key, field, amount))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

    def rpush(self, key: KeyT, *values: EncodableT) -> int:
        """Push values to the right of a list."""
        client = self.get_client(key, write=True)
        nvalues = [self.encode(v) for v in values]

        try:
            return cast("int", client.rpush(key, *nvalues))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

    def llen(self, key: KeyT) -> int:
        """Get the length of a list."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.llen(key))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

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

            result = client.lpos(key, encoded_value, **kwargs)
            return result
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

    def lrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        """Get a range of elements from a list."""
        client = self.get_client(key, write=False)

        try:
            values = client.lrange(key, start, end)
            return [self.decode(v) for v in values]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def lindex(self, key: KeyT, index: int) -> Any | None:
        """Get an element from a list by index."""
        client = self.get_client(key, write=False)

        try:
            val = client.lindex(key, index)
            return self.decode(val) if val is not None else None
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def lset(self, key: KeyT, index: int, value: EncodableT) -> bool:
        """Set an element in a list by index."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            client.lset(key, index, nvalue)
            return True
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def lrem(self, key: KeyT, count: int, value: EncodableT) -> int:
        """Remove elements from a list."""
        client = self.get_client(key, write=True)
        nvalue = self.encode(value)

        try:
            return cast("int", client.lrem(key, count, nvalue))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def ltrim(self, key: KeyT, start: int, end: int) -> bool:
        """Trim a list to the specified range."""
        client = self.get_client(key, write=True)

        try:
            client.ltrim(key, start, end)
            return True
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

    def blpop(
        self,
        keys: list[KeyT],
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
            raise ConnectionInterrupted(connection=client) from e

    def brpop(
        self,
        keys: list[KeyT],
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
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

    def srem(self, key: KeyT, *members: EncodableT) -> int:
        """Remove members from a set."""
        client = self.get_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", client.srem(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def smembers(self, key: KeyT) -> _Set[Any]:
        """Get all members of a set."""
        client = self.get_client(key, write=False)

        try:
            result = client.smembers(key)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sismember(self, key: KeyT, member: EncodableT) -> bool:
        """Check if a value is a member of a set."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        try:
            return bool(client.sismember(key, nmember))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def scard(self, key: KeyT) -> int:
        """Get the number of members in a set."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.scard(key))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

    def smove(self, src: KeyT, dst: KeyT, member: EncodableT) -> bool:
        """Move a member from one set to another."""
        client = self.get_client(write=True)
        nmember = self.encode(member)

        try:
            return bool(client.smove(src, dst, nmember))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sdiff(self, keys: list[KeyT]) -> _Set[Any]:
        """Return the difference of sets."""
        client = self.get_client(write=False)

        try:
            result = client.sdiff(*keys)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sdiffstore(self, dest: KeyT, keys: list[KeyT]) -> int:
        """Store the difference of sets."""
        client = self.get_client(write=True)

        try:
            return cast("int", client.sdiffstore(dest, *keys))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sinter(self, keys: list[KeyT]) -> _Set[Any]:
        """Return the intersection of sets."""
        client = self.get_client(write=False)

        try:
            result = client.sinter(*keys)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sinterstore(self, dest: KeyT, keys: list[KeyT]) -> int:
        """Store the intersection of sets."""
        client = self.get_client(write=True)

        try:
            return cast("int", client.sinterstore(dest, *keys))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sunion(self, keys: list[KeyT]) -> _Set[Any]:
        """Return the union of sets."""
        client = self.get_client(write=False)

        try:
            result = client.sunion(*keys)
            return {self.decode(v) for v in result}
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def sunionstore(self, dest: KeyT, keys: list[KeyT]) -> int:
        """Store the union of sets."""
        client = self.get_client(write=True)

        try:
            return cast("int", client.sunionstore(dest, *keys))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def smismember(self, key: KeyT, *members: EncodableT) -> list[bool]:
        """Check if multiple values are members of a set."""
        client = self.get_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        try:
            result = client.smismember(key, nmembers)
            return [bool(v) for v in result]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

    def zrem(self, key: KeyT, *members: EncodableT) -> int:
        """Remove members from a sorted set."""
        client = self.get_client(key, write=True)
        nmembers = [self.encode(m) for m in members]

        try:
            return cast("int", client.zrem(key, *nmembers))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zscore(self, key: KeyT, member: EncodableT) -> float | None:
        """Get the score of a member."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        try:
            result = client.zscore(key, nmember)
            return float(result) if result is not None else None
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zrank(self, key: KeyT, member: EncodableT) -> int | None:
        """Get the rank of a member (0-based)."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        try:
            return client.zrank(key, nmember)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zrevrank(self, key: KeyT, member: EncodableT) -> int | None:
        """Get the reverse rank of a member."""
        client = self.get_client(key, write=False)
        nmember = self.encode(member)

        try:
            return client.zrevrank(key, nmember)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zcard(self, key: KeyT) -> int:
        """Get the number of members in a sorted set."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.zcard(key))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zcount(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Count members in a score range."""
        client = self.get_client(key, write=False)

        try:
            return cast("int", client.zcount(key, min_score, max_score))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zincrby(self, key: KeyT, amount: float, member: EncodableT) -> float:
        """Increment the score of a member."""
        client = self.get_client(key, write=True)
        nmember = self.encode(member)

        try:
            return float(client.zincrby(key, amount, nmember))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

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
            raise ConnectionInterrupted(connection=client) from e

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
                key, min_score, max_score,
                start=start, num=num, withscores=withscores,
            )
            if withscores:
                return [(self.decode(m), float(s)) for m, s in result]
            return [self.decode(m) for m in result]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

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
                key, max_score, min_score,
                start=start, num=num, withscores=withscores,
            )
            if withscores:
                return [(self.decode(m), float(s)) for m, s in result]
            return [self.decode(m) for m in result]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        """Remove members by rank range."""
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.zremrangebyrank(key, start, end))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zremrangebyscore(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Remove members by score range."""
        client = self.get_client(key, write=True)

        try:
            return cast("int", client.zremrangebyscore(key, min_score, max_score))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zpopmin(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        client = self.get_client(key, write=True)

        try:
            result = client.zpopmin(key, count)
            return [(self.decode(m), float(s)) for m, s in result]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zpopmax(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        client = self.get_client(key, write=True)

        try:
            result = client.zpopmax(key, count)
            return [(self.decode(m), float(s)) for m, s in result]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def zmscore(self, key: KeyT, *members: EncodableT) -> list[float | None]:
        """Get scores for multiple members."""
        client = self.get_client(key, write=False)
        nmembers = [self.encode(m) for m in members]

        try:
            results = client.zmscore(key, nmembers)
            return [float(r) if r is not None else None for r in results]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e


# =============================================================================
# RedisCacheClient - concrete implementation for redis-py
# =============================================================================

if _REDIS_AVAILABLE:

    class RedisCacheClient(KeyValueCacheClient):
        """Redis cache client using redis-py."""

        _lib = redis
        _client_class = redis.Redis
        _pool_class = redis.ConnectionPool

else:

    class RedisCacheClient(KeyValueCacheClient):  # type: ignore[no-redef]
        """Redis cache client (requires redis-py)."""

        def __init__(self, *args, **kwargs):
            msg = (
                "RedisCacheClient requires redis-py. "
                "Install with: pip install redis"
            )
            raise ImportError(msg)


# =============================================================================
# ValkeyCacheClient - concrete implementation for valkey-py
# =============================================================================

if _VALKEY_AVAILABLE:

    class ValkeyCacheClient(KeyValueCacheClient):
        """Valkey cache client using valkey-py."""

        _lib = valkey
        _client_class = valkey.Valkey
        _pool_class = valkey.ConnectionPool

else:

    class ValkeyCacheClient(KeyValueCacheClient):  # type: ignore[no-redef]
        """Valkey cache client (requires valkey-py)."""

        def __init__(self, *args, **kwargs):
            raise ImportError(
                "ValkeyCacheClient requires valkey-py. Install with: pip install valkey"
            )


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
