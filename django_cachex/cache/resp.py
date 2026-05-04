"""Library-agnostic cache backend base class.

:class:`RespCache` extends Django's ``BaseCache`` with Redis/Valkey
features (data structures, TTL operations, pattern matching, distributed
locking, pipelines, and multi-serializer/compressor support). Per-driver
concrete subclasses live in:

- :mod:`django_cachex.cache.valkey_py` — ``valkey-py``
- :mod:`django_cachex.cache.redis_py` — ``redis-py``
- :mod:`django_cachex.cache.redis_rs` — Rust ``redis-rs`` driver
- :mod:`django_cachex.cache.valkey_glide` — ``valkey-glide``
"""

import inspect
import re
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast, override

from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.utils.module_loading import import_string

if TYPE_CHECKING:
    import builtins
    from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
    from datetime import datetime, timedelta

    from django_cachex.adapters.pipeline import AsyncPipeline, Pipeline
    from django_cachex.adapters.protocols import RespAdapterProtocol
    from django_cachex.types import KeyType

from django_cachex.cache.base import BaseCachex
from django_cachex.exceptions import CompressorError, SerializerError
from django_cachex.script import ScriptHelpers

# Alias for the `set` builtin shadowed by the `set` method (PEP 649 defers
# annotations at runtime, but type checkers still resolve them in class scope).
# The `type` shadow is worked around with `builtins.type[X]` directly when
# subscripts are needed — module-level aliases of `type` don't survive
# subscript through mypy's name resolution.
_set = set

# Regex for escaping glob special characters
_special_re = re.compile("([*?[])")


def _glob_escape(s: str) -> str:
    """Escape glob special characters in a string."""
    return _special_re.sub(r"[\1]", s)


def _load_codec(config: str | type | Any) -> Any:
    """Resolve a serializer/compressor config: dotted-path / class / instance → instance."""
    if isinstance(config, str):
        config = import_string(config)
    if callable(config):
        return config()
    return config


# =============================================================================
# RespCache - base class extending Django's BaseCache
# =============================================================================


class RespCache(BaseCachex):
    """Django cache backend for Redis/Valkey with extended features.

    Base class for all django-cachex Redis/Valkey backends. Inherits the
    cachex contract from :class:`~django_cachex.cache.base.BaseCachex`
    (which itself extends Django's ``BaseCache``) and overrides the
    extension surface with real implementations backed by an adapter.
    Concrete cache classes pick a specific adapter via ``_adapter_class``.
    """

    # Support level marker for admin interface
    _cachex_support: str = "cachex"

    # Class attribute - subclasses override this
    _adapter_class: builtins.type[RespAdapterProtocol]

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

        # Setup serializer chain (multi-serializer fallback)
        serializer_config = self._options.get(
            "serializer",
            "django_cachex.serializers.pickle.PickleSerializer",
        )
        self._serializers: list[Any] = self._create_serializers(serializer_config)

        # Setup compressor chain (optional; empty = no compression)
        self._compressors: list[Any] = self._create_compressors(self._options.get("compressor"))

    @cached_property
    def adapter(self) -> RespAdapterProtocol:
        """Get the adapter instance (matches Django's pattern)."""
        return self._adapter_class(self._servers, **self._options)

    # =========================================================================
    # Serializer / Compressor stack — encoding lives at the cache layer
    # =========================================================================

    @staticmethod
    def _create_serializers(config: Any) -> list[Any]:
        if config is None:
            config = "django_cachex.serializers.pickle.PickleSerializer"
        items: list = config if isinstance(config, list) else [config]
        return [_load_codec(item) for item in items]

    @staticmethod
    def _create_compressors(config: Any) -> list[Any]:
        if config is None:
            return []
        items: list = config if isinstance(config, list) else [config]
        return [_load_codec(item) for item in items]

    def _decompress(self, value: bytes) -> bytes:
        """Decompress with fallback support for multiple compressors.

        Returns ``value`` unchanged when no compressors are configured.
        Raises ``CompressorError`` if every configured compressor fails. To
        accept uncompressed payloads alongside compressed ones (e.g.
        mid-migration), keep the previously-used compressor at the end of
        the fallback chain.
        """
        if not self._compressors:
            return value
        last_error: CompressorError | None = None
        for compressor in self._compressors:
            try:
                return compressor.decompress(value)
            except CompressorError as e:
                last_error = e
        raise last_error if last_error is not None else CompressorError("decompression failed")

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
        except ValueError, TypeError:
            value = self._decompress(value)
            return self._deserialize(value)

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

    @override
    def add(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool:
        """Set a value only if the key doesn't exist."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.add(
            key,
            self.encode(value),
            self.get_backend_timeout(timeout),
            stampede_prevention=stampede_prevention,
        )

    @override
    async def aadd(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool:
        """Set a value only if the key doesn't exist, asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.aadd(
            key,
            self.encode(value),
            self.get_backend_timeout(timeout),
            stampede_prevention=stampede_prevention,
        )

    @override
    def get(
        self,
        key: str,
        default: Any = None,
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> Any:
        """Fetch a value from the cache."""
        key = self.make_and_validate_key(key, version=version)
        value = self.adapter.get(key, stampede_prevention=stampede_prevention)
        if value is None:
            return default
        return self.decode(value)

    @override
    async def aget(
        self,
        key: str,
        default: Any = None,
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> Any:
        """Fetch a value from the cache asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        value = await self.adapter.aget(key, stampede_prevention=stampede_prevention)
        if value is None:
            return default
        return self.decode(value)

    @override
    async def aset(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
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
            result = await self.adapter.aset_with_flags(
                key,
                self.encode(value),
                self.get_backend_timeout(timeout),
                nx=nx,
                xx=xx,
                get=get,
                stampede_prevention=stampede_prevention,
            )
            # set_with_flags returns the previous value when get=True (bytes or None);
            # otherwise a bool indicating NX/XX success.
            if get:
                return self.decode(result) if result is not None else None
            return result
        await self.adapter.aset(
            key,
            self.encode(value),
            self.get_backend_timeout(timeout),
            stampede_prevention=stampede_prevention,
        )
        return None

    @override
    def set(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
        **kwargs: Any,
    ) -> Any:
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
            result = self.adapter.set_with_flags(
                key,
                self.encode(value),
                self.get_backend_timeout(timeout),
                nx=nx,
                xx=xx,
                get=get,
                stampede_prevention=stampede_prevention,
            )
            if get:
                return self.decode(result) if result is not None else None
            return result
        # Use standard Django method - returns None
        self.adapter.set(
            key,
            self.encode(value),
            self.get_backend_timeout(timeout),
            stampede_prevention=stampede_prevention,
        )
        return None

    @override
    def touch(self, key: str, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        """Update the timeout on a key."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.touch(key, self.get_backend_timeout(timeout))

    @override
    async def atouch(self, key: str, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        """Update the timeout on a key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.atouch(key, self.get_backend_timeout(timeout))

    @override
    def delete(self, key: str, version: int | None = None) -> bool:
        """Remove a key from the cache."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.delete(key)

    @override
    async def adelete(self, key: str, version: int | None = None) -> bool:
        """Remove a key from the cache asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.adelete(key)

    @override
    def get_many(  # type: ignore[override]
        self,
        keys: list[str],
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> dict[str, Any]:
        """Retrieve many keys."""
        key_map = {self.make_and_validate_key(key, version=version): key for key in keys}
        ret = self.adapter.get_many(list(key_map), stampede_prevention=stampede_prevention)
        return {key_map[k]: self.decode(v) for k, v in ret.items()}

    @override
    async def aget_many(  # type: ignore[override]
        self,
        keys: list[str],
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> dict[str, Any]:
        """Retrieve many keys asynchronously."""
        key_map = {self.make_and_validate_key(key, version=version): key for key in keys}
        ret = await self.adapter.aget_many(list(key_map), stampede_prevention=stampede_prevention)
        return {key_map[k]: self.decode(v) for k, v in ret.items()}

    @override
    def has_key(self, key: str, version: int | None = None) -> bool:
        """Check if a key exists."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.has_key(key)

    @override
    async def ahas_key(self, key: str, version: int | None = None) -> bool:
        """Check if a key exists asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.ahas_key(key)

    @override
    def incr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Increment a value."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.incr(key, delta)

    @override
    async def aincr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Increment a value asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.aincr(key, delta)

    @override
    async def adecr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Decrement a value asynchronously."""
        return await self.aincr(key, -delta, version)

    @override
    def get_or_set(
        self,
        key: str,
        default: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> Any:
        """Fetch a key from the cache, setting it to default if missing."""
        val = self.get(key, self._missing_key, version=version, stampede_prevention=stampede_prevention)
        if val is self._missing_key:
            if callable(default):
                default = default()
            if self.adapter._resolve_stampede(stampede_prevention):
                # Stampede may return "miss" for a key that still physically exists.
                # Use set() (unconditional write) instead of add() (NX) so the
                # recomputed value actually overwrites the stale key.
                self.set(key, default, timeout=timeout, version=version, stampede_prevention=stampede_prevention)
            else:
                self.add(key, default, timeout=timeout, version=version)
            # Fetch the value again to avoid a race condition if another caller
            # set between the first get() and the set/add() above.
            # Disable stampede here — we just wrote the value, don't re-trigger.
            return self.get(key, default, version=version)
        return val

    @override
    async def aget_or_set(
        self,
        key: str,
        default: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> Any:
        """Fetch a key from the cache asynchronously, setting it to default if missing."""
        val = await self.aget(key, self._missing_key, version=version, stampede_prevention=stampede_prevention)
        if val is self._missing_key:
            if callable(default):
                default = await default() if inspect.iscoroutinefunction(default) else default()
            if self.adapter._resolve_stampede(stampede_prevention):
                await self.aset(key, default, timeout=timeout, version=version, stampede_prevention=stampede_prevention)
            else:
                await self.aadd(key, default, timeout=timeout, version=version)
            # Fetch the value again to avoid a race condition if another caller
            # set between the first aget() and the aset/aadd() above.
            # Disable stampede here — we just wrote the value, don't re-trigger.
            return await self.aget(key, default, version=version)
        return val

    @override
    def set_many(
        self,
        data: Mapping[str, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        """Set multiple values."""
        if not data:
            return []
        safe_data = {
            self.make_and_validate_key(key, version=version): self.encode(value) for key, value in data.items()
        }
        self.adapter.set_many(safe_data, self.get_backend_timeout(timeout), stampede_prevention=stampede_prevention)
        return []

    @override
    async def aset_many(
        self,
        data: Mapping[str, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        """Set multiple values asynchronously."""
        if not data:
            return []
        safe_data = {
            self.make_and_validate_key(key, version=version): self.encode(value) for key, value in data.items()
        }
        await self.adapter.aset_many(
            safe_data,
            self.get_backend_timeout(timeout),
            stampede_prevention=stampede_prevention,
        )
        return []

    @override
    def delete_many(self, keys: list[str], version: int | None = None) -> int:  # type: ignore[override]
        """Delete multiple keys from the cache."""
        keys = list(keys)  # Convert generator to list
        if not keys:
            return 0
        safe_keys = [self.make_and_validate_key(key, version=version) for key in keys]
        return self.adapter.delete_many(safe_keys)

    @override
    async def adelete_many(self, keys: list[str], version: int | None = None) -> int:  # type: ignore[override]
        """Delete multiple keys from the cache asynchronously."""
        keys = list(keys)  # Convert generator to list
        if not keys:
            return 0
        safe_keys = [self.make_and_validate_key(key, version=version) for key in keys]
        return await self.adapter.adelete_many(safe_keys)

    @override
    def clear(self) -> bool:  # type: ignore[override]
        """Delete all keys in this cache's namespace (prefix + version).

        Unlike Django's default ``RedisCache.clear()`` which calls ``FLUSHDB``
        and destroys *all* data in the Redis database, this only removes keys
        belonging to this cache instance. Safe when multiple apps share a
        Redis database.

        To flush the entire database, use ``cache.flush_db()``.
        """
        return self.delete_pattern("*") >= 0

    @override
    async def aclear(self) -> bool:  # type: ignore[override]
        """Delete all keys in this cache's namespace asynchronously."""
        return (await self.adelete_pattern("*")) >= 0

    @override
    def close(self, **kwargs: Any) -> None:
        """Delegate to the adapter. Skip if no adapter has been built yet."""
        if "adapter" in self.__dict__:
            self.adapter.close(**kwargs)

    @override
    async def aclose(self, **kwargs: Any) -> None:
        """Delegate to the adapter. Skip if no adapter has been built yet."""
        if "adapter" in self.__dict__:
            await self.adapter.aclose(**kwargs)

    # =========================================================================
    # Extended Methods (beyond Django's BaseCache)
    # =========================================================================

    def ttl(self, key: str, version: int | None = None) -> int | None:
        """Get TTL in seconds. Returns None if no expiry, -2 if key doesn't exist."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.ttl(key)

    async def attl(self, key: str, version: int | None = None) -> int | None:
        """Get TTL in seconds asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.attl(key)

    def pttl(self, key: str, version: int | None = None) -> int | None:
        """Get TTL in milliseconds. Returns None if no expiry, -2 if key doesn't exist."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.pttl(key)

    async def apttl(self, key: str, version: int | None = None) -> int | None:
        """Get TTL in milliseconds asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.apttl(key)

    def type(self, key: str, version: int | None = None) -> KeyType | None:
        """Get the Redis data type of a key."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.type(key)

    async def atype(self, key: str, version: int | None = None) -> KeyType | None:
        """Get the Redis data type of a key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.atype(key)

    def persist(self, key: str, version: int | None = None) -> bool:
        """Remove the expiry from a key, making it persistent."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.persist(key)

    async def apersist(self, key: str, version: int | None = None) -> bool:
        """Remove the expiry from a key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.apersist(key)

    def expire(
        self,
        key: str,
        timeout: int | timedelta,
        version: int | None = None,
    ) -> bool:
        """Set expiry time on a key in seconds."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.expire(key, timeout)

    async def aexpire(
        self,
        key: str,
        timeout: int | timedelta,
        version: int | None = None,
    ) -> bool:
        """Set expiry time on a key in seconds asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.aexpire(key, timeout)

    def expireat(
        self,
        key: str,
        when: int | datetime,
        version: int | None = None,
    ) -> bool:
        """Set expiry to an absolute time."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.expireat(key, when)

    async def aexpireat(
        self,
        key: str,
        when: int | datetime,
        version: int | None = None,
    ) -> bool:
        """Set expiry to an absolute time asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.aexpireat(key, when)

    def pexpire(
        self,
        key: str,
        timeout: int | timedelta,
        version: int | None = None,
    ) -> bool:
        """Set expiry time on a key in milliseconds."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.pexpire(key, timeout)

    async def apexpire(
        self,
        key: str,
        timeout: int | timedelta,
        version: int | None = None,
    ) -> bool:
        """Set expiry time on a key in milliseconds asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.apexpire(key, timeout)

    def pexpireat(
        self,
        key: str,
        when: int | datetime,
        version: int | None = None,
    ) -> bool:
        """Set expiry to an absolute time with millisecond precision."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.pexpireat(key, when)

    async def apexpireat(
        self,
        key: str,
        when: int | datetime,
        version: int | None = None,
    ) -> bool:
        """Set expiry to an absolute time with millisecond precision asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.apexpireat(key, when)

    def expiretime(self, key: str, version: int | None = None) -> int | None:
        """Get the absolute Unix timestamp (seconds) when a key will expire.

        Returns None if the key has no expiry, -2 if the key doesn't exist.
        Requires Redis 7.0+ / Valkey 7.2+.
        """
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.expiretime(key)

    async def aexpiretime(self, key: str, version: int | None = None) -> int | None:
        """Get the absolute Unix timestamp (seconds) when a key will expire asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.aexpiretime(key)

    def keys(
        self,
        pattern: str = "*",
        version: int | None = None,
    ) -> list[str]:
        """Return all keys matching pattern (returns original keys without prefix)."""
        full_pattern = self.make_pattern(pattern, version=version)
        raw_keys = self.adapter.keys(full_pattern)
        return [self.reverse_key(k) for k in raw_keys]

    async def akeys(
        self,
        pattern: str = "*",
        version: int | None = None,
    ) -> list[str]:
        """Return all keys matching pattern asynchronously."""
        full_pattern = self.make_pattern(pattern, version=version)
        raw_keys = await self.adapter.akeys(full_pattern)
        return [self.reverse_key(k) for k in raw_keys]

    def iter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]:
        """Iterate over keys matching pattern using SCAN."""
        full_pattern = self.make_pattern(pattern, version=version)
        for key in self.adapter.iter_keys(full_pattern, itersize=itersize):
            yield self.reverse_key(key)

    async def aiter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> AsyncIterator[str]:
        """Iterate over keys matching pattern using SCAN asynchronously."""
        full_pattern = self.make_pattern(pattern, version=version)
        async for key in self.adapter.aiter_keys(full_pattern, itersize=itersize):
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
        next_cursor, raw_keys = self.adapter.scan(
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
        next_cursor, raw_keys = await self.adapter.ascan(
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
        return self.adapter.delete_pattern(full_pattern, itersize=itersize)

    async def adelete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        """Delete all keys matching pattern asynchronously."""
        full_pattern = self.make_pattern(pattern, version=version)
        return await self.adapter.adelete_pattern(full_pattern, itersize=itersize)

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
        return self.adapter.lock(
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
        return self.adapter.alock(
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
        from django_cachex.adapters.pipeline import Pipeline

        v = version if version is not None else self.version
        pipeline_adapter = self.adapter.pipeline(transaction=transaction)
        pipe = Pipeline(cache=self, pipeline_adapter=pipeline_adapter, version=v)
        # Set key_func for proper key prefixing
        pipe._key_func = self.make_and_validate_key
        pipe._cache_version = v
        return pipe

    def apipeline(
        self,
        *,
        transaction: bool = True,
        version: int | None = None,
    ) -> AsyncPipeline:
        """Create an async pipeline for batched operations.

        ``apipeline()`` itself is sync — it just constructs the wrapper.
        Only ``await pipeline.execute()`` performs I/O. Mirrors how
        ``redis.asyncio.Redis().pipeline()`` works.
        """
        from django_cachex.adapters.pipeline import AsyncPipeline

        v = version if version is not None else self.version
        pipeline_adapter = self.adapter.apipeline(transaction=transaction)
        pipe = AsyncPipeline(cache=self, pipeline_adapter=pipeline_adapter, version=v)
        pipe._key_func = self.make_and_validate_key
        pipe._cache_version = v
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
        return self.adapter.delete_pattern(full_pattern, itersize=itersize)

    async def aclear_all_versions(self, itersize: int | None = None) -> int:
        """Delete all keys for this cache's prefix across ALL versions (async)."""
        escaped_prefix = _glob_escape(self.key_prefix)
        full_pattern = self.key_func("*", escaped_prefix, "*")
        return await self.adapter.adelete_pattern(full_pattern, itersize=itersize)

    def flush_db(self) -> bool:
        """Flush the entire Redis database (``FLUSHDB``).

        Removes all keys in the database, not just those belonging to this
        cache. Only use when this cache has a dedicated Redis database;
        otherwise use ``clear()``.
        """
        return self.adapter.clear()

    async def aflush_db(self) -> bool:
        """Flush the entire Redis database asynchronously (``FLUSHDB``)."""
        return await self.adapter.aclear()

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        """Set hash field(s). Use field/value, mapping, or items (flat key-value pairs)."""
        key = self.make_and_validate_key(key, version=version)
        nvalue = self.encode(value) if field is not None else None
        nmapping = {f: self.encode(v) for f, v in mapping.items()} if mapping else None
        nitems = [self.encode(v) if i % 2 else v for i, v in enumerate(items)] if items else None
        return self.adapter.hset(key, field, nvalue, mapping=nmapping, items=nitems)

    def hdel(
        self,
        key: str,
        *fields: str,
        version: int | None = None,
    ) -> int:
        """Delete one or more hash fields."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.hdel(key, *fields)

    def hlen(self, key: str, version: int | None = None) -> int:
        """Get number of fields in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.hlen(key)

    def hkeys(self, key: str, version: int | None = None) -> list[str]:
        """Get all field names in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return [f.decode() if isinstance(f, bytes) else f for f in self.adapter.hkeys(key)]

    def hexists(
        self,
        key: str,
        field: str,
        version: int | None = None,
    ) -> bool:
        """Check if field exists in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.hexists(key, field)

    def hget(
        self,
        key: str,
        field: str,
        version: int | None = None,
    ) -> Any:
        """Get value of field in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        result = self.adapter.hget(key, field)
        return self.decode(result) if result is not None else None

    def hgetall(
        self,
        key: str,
        version: int | None = None,
    ) -> dict[str, Any]:
        """Get all fields and values in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return {
            (f.decode() if isinstance(f, bytes) else f): self.decode(v) for f, v in self.adapter.hgetall(key).items()
        }

    def hmget(
        self,
        key: str,
        *fields: str,
        version: int | None = None,
    ) -> list[Any]:
        """Get values of multiple fields in hash."""
        key = self.make_and_validate_key(key, version=version)
        return [self.decode(v) if v is not None else None for v in self.adapter.hmget(key, *fields)]

    def hincrby(
        self,
        key: str,
        field: str,
        amount: int = 1,
        version: int | None = None,
    ) -> int:
        """Increment value of field in hash by amount."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.hincrby(key, field, amount)

    def hincrbyfloat(
        self,
        key: str,
        field: str,
        amount: float = 1.0,
        version: int | None = None,
    ) -> float:
        """Increment float value of field in hash by amount."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.hincrbyfloat(key, field, amount)

    def hsetnx(
        self,
        key: str,
        field: str,
        value: Any,
        version: int | None = None,
    ) -> bool:
        """Set field in hash only if it doesn't exist."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.hsetnx(key, field, self.encode(value))

    def hvals(
        self,
        key: str,
        version: int | None = None,
    ) -> list[Any]:
        """Get all values in hash at key."""
        key = self.make_and_validate_key(key, version=version)
        return [self.decode(v) for v in self.adapter.hvals(key)]

    async def ahset(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        """Set hash field(s) asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        nvalue = self.encode(value) if field is not None else None
        nmapping = {f: self.encode(v) for f, v in mapping.items()} if mapping else None
        nitems = [self.encode(v) if i % 2 else v for i, v in enumerate(items)] if items else None
        return await self.adapter.ahset(key, field, nvalue, mapping=nmapping, items=nitems)

    async def ahdel(
        self,
        key: str,
        *fields: str,
        version: int | None = None,
    ) -> int:
        """Delete one or more hash fields asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.ahdel(key, *fields)

    async def ahlen(self, key: str, version: int | None = None) -> int:
        """Get number of fields in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.ahlen(key)

    async def ahkeys(self, key: str, version: int | None = None) -> list[str]:
        """Get all field names in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return [f.decode() if isinstance(f, bytes) else f for f in await self.adapter.ahkeys(key)]

    async def ahexists(
        self,
        key: str,
        field: str,
        version: int | None = None,
    ) -> bool:
        """Check if field exists in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.ahexists(key, field)

    async def ahget(
        self,
        key: str,
        field: str,
        version: int | None = None,
    ) -> Any:
        """Get value of field in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        result = await self.adapter.ahget(key, field)
        return self.decode(result) if result is not None else None

    async def ahgetall(
        self,
        key: str,
        version: int | None = None,
    ) -> dict[str, Any]:
        """Get all fields and values in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        raw = await self.adapter.ahgetall(key)
        return {(f.decode() if isinstance(f, bytes) else f): self.decode(v) for f, v in raw.items()}

    async def ahmget(
        self,
        key: str,
        *fields: str,
        version: int | None = None,
    ) -> list[Any]:
        """Get values of multiple fields in hash asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return [self.decode(v) if v is not None else None for v in await self.adapter.ahmget(key, *fields)]

    async def ahincrby(
        self,
        key: str,
        field: str,
        amount: int = 1,
        version: int | None = None,
    ) -> int:
        """Increment value of field in hash by amount asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.ahincrby(key, field, amount)

    async def ahincrbyfloat(
        self,
        key: str,
        field: str,
        amount: float = 1.0,
        version: int | None = None,
    ) -> float:
        """Increment float value of field in hash by amount asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.ahincrbyfloat(key, field, amount)

    async def ahsetnx(
        self,
        key: str,
        field: str,
        value: Any,
        version: int | None = None,
    ) -> bool:
        """Set field in hash only if it doesn't exist asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.ahsetnx(key, field, self.encode(value))

    async def ahvals(
        self,
        key: str,
        version: int | None = None,
    ) -> list[Any]:
        """Get all values in hash at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return [self.decode(v) for v in await self.adapter.ahvals(key)]

    # =========================================================================
    # List Operations
    # =========================================================================

    def lpush(
        self,
        key: str,
        *values: Any,
        version: int | None = None,
    ) -> int:
        """Push values onto head of list at key."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.lpush(key, *(self.encode(v) for v in values))

    def rpush(
        self,
        key: str,
        *values: Any,
        version: int | None = None,
    ) -> int:
        """Push values onto tail of list at key."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.rpush(key, *(self.encode(v) for v in values))

    def lpop(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from head of list."""
        key = self.make_and_validate_key(key, version=version)
        result = self.adapter.lpop(key, count=count)
        if result is None:
            return None
        if isinstance(result, list):
            return [self.decode(v) for v in result]
        return self.decode(result)

    def rpop(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from tail of list."""
        key = self.make_and_validate_key(key, version=version)
        result = self.adapter.rpop(key, count=count)
        if result is None:
            return None
        if isinstance(result, list):
            return [self.decode(v) for v in result]
        return self.decode(result)

    def lrange(
        self,
        key: str,
        start: int,
        end: int,
        version: int | None = None,
    ) -> list[Any]:
        """Get a range of elements from list."""
        key = self.make_and_validate_key(key, version=version)
        return [self.decode(v) for v in self.adapter.lrange(key, start, end)]

    def lindex(
        self,
        key: str,
        index: int,
        version: int | None = None,
    ) -> Any:
        """Get element at index in list."""
        key = self.make_and_validate_key(key, version=version)
        result = self.adapter.lindex(key, index)
        return self.decode(result) if result is not None else None

    def llen(self, key: str, version: int | None = None) -> int:
        """Get length of list at key."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.llen(key)

    def lpos(
        self,
        key: str,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.lpos(key, self.encode(value), rank=rank, count=count, maxlen=maxlen)

    def lmove(
        self,
        src: str,
        dst: str,
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
        result = self.adapter.lmove(src, dst, wherefrom, whereto)
        return self.decode(result) if result is not None else None

    def lrem(
        self,
        key: str,
        count: int,
        value: Any,
        version: int | None = None,
    ) -> int:
        """Remove elements equal to value from list."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.lrem(key, count, self.encode(value))

    def ltrim(
        self,
        key: str,
        start: int,
        end: int,
        version: int | None = None,
    ) -> bool:
        """Trim list to specified range."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.ltrim(key, start, end)

    def lset(
        self,
        key: str,
        index: int,
        value: Any,
        version: int | None = None,
    ) -> bool:
        """Set element at index in list."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.lset(key, index, self.encode(value))

    def linsert(
        self,
        key: str,
        where: str,
        pivot: Any,
        value: Any,
        version: int | None = None,
    ) -> int:
        """Insert value before or after pivot in list."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.linsert(key, where, self.encode(pivot), self.encode(value))

    def blpop(
        self,
        keys: str | Sequence[str],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list."""
        keys = [keys] if isinstance(keys, str) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        result = self.adapter.blpop(nkeys, timeout=timeout)
        if result is None:
            return None
        # Reverse the key back to original
        return (self.reverse_key(result[0]), self.decode(result[1]))

    def brpop(
        self,
        keys: str | Sequence[str],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list."""
        keys = [keys] if isinstance(keys, str) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        result = self.adapter.brpop(nkeys, timeout=timeout)
        if result is None:
            return None
        # Reverse the key back to original
        return (self.reverse_key(result[0]), self.decode(result[1]))

    def blmove(
        self,
        src: str,
        dst: str,
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
        result = self.adapter.blmove(src, dst, timeout, wherefrom, whereto)
        return self.decode(result) if result is not None else None

    async def alpush(
        self,
        key: str,
        *values: Any,
        version: int | None = None,
    ) -> int:
        """Push values onto head of list at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.alpush(key, *(self.encode(v) for v in values))

    async def arpush(
        self,
        key: str,
        *values: Any,
        version: int | None = None,
    ) -> int:
        """Push values onto tail of list at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.arpush(key, *(self.encode(v) for v in values))

    async def alpop(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from head of list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        result = await self.adapter.alpop(key, count=count)
        if result is None:
            return None
        if isinstance(result, list):
            return [self.decode(v) for v in result]
        return self.decode(result)

    async def arpop(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from tail of list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        result = await self.adapter.arpop(key, count=count)
        if result is None:
            return None
        if isinstance(result, list):
            return [self.decode(v) for v in result]
        return self.decode(result)

    async def alrange(
        self,
        key: str,
        start: int,
        end: int,
        version: int | None = None,
    ) -> list[Any]:
        """Get a range of elements from list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return [self.decode(v) for v in await self.adapter.alrange(key, start, end)]

    async def alindex(
        self,
        key: str,
        index: int,
        version: int | None = None,
    ) -> Any:
        """Get element at index in list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        result = await self.adapter.alindex(key, index)
        return self.decode(result) if result is not None else None

    async def allen(self, key: str, version: int | None = None) -> int:
        """Get length of list at key asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.allen(key)

    async def alpos(
        self,
        key: str,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.alpos(key, self.encode(value), rank=rank, count=count, maxlen=maxlen)

    async def almove(
        self,
        src: str,
        dst: str,
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
        result = await self.adapter.almove(src, dst, wherefrom, whereto)
        return self.decode(result) if result is not None else None

    async def alrem(
        self,
        key: str,
        count: int,
        value: Any,
        version: int | None = None,
    ) -> int:
        """Remove elements equal to value from list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.alrem(key, count, self.encode(value))

    async def altrim(
        self,
        key: str,
        start: int,
        end: int,
        version: int | None = None,
    ) -> bool:
        """Trim list to specified range asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.altrim(key, start, end)

    async def alset(
        self,
        key: str,
        index: int,
        value: Any,
        version: int | None = None,
    ) -> bool:
        """Set element at index in list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.alset(key, index, self.encode(value))

    async def alinsert(
        self,
        key: str,
        where: str,
        pivot: Any,
        value: Any,
        version: int | None = None,
    ) -> int:
        """Insert value before or after pivot in list asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.alinsert(key, where, self.encode(pivot), self.encode(value))

    async def ablpop(
        self,
        keys: str | Sequence[str],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list asynchronously."""
        keys = [keys] if isinstance(keys, str) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        result = await self.adapter.ablpop(nkeys, timeout=timeout)
        if result is None:
            return None
        return (self.reverse_key(result[0]), self.decode(result[1]))

    async def abrpop(
        self,
        keys: str | Sequence[str],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list asynchronously."""
        keys = [keys] if isinstance(keys, str) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        result = await self.adapter.abrpop(nkeys, timeout=timeout)
        if result is None:
            return None
        return (self.reverse_key(result[0]), self.decode(result[1]))

    async def ablmove(
        self,
        src: str,
        dst: str,
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
        result = await self.adapter.ablmove(src, dst, timeout, wherefrom, whereto)
        return self.decode(result) if result is not None else None

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(
        self,
        key: str,
        *members: Any,
        version: int | None = None,
    ) -> int:
        """Add members to a set."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.sadd(key, *(self.encode(m) for m in members))

    def scard(self, key: str, version: int | None = None) -> int:
        """Get the number of members in a set."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.scard(key)

    def sdiff(
        self,
        keys: str | Sequence[str],
        version: int | None = None,
    ) -> _set[Any]:
        """Return the difference between the first set and all successive sets."""
        keys = [keys] if isinstance(keys, str) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return {self.decode(m) for m in self.adapter.sdiff(nkeys)}

    def sdiffstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the difference of sets at dest."""
        keys = [keys] if isinstance(keys, str) else keys
        # Use specific versions if provided, otherwise fall back to version
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return self.adapter.sdiffstore(dest, nkeys)

    def sinter(
        self,
        keys: str | Sequence[str],
        version: int | None = None,
    ) -> _set[Any]:
        """Return the intersection of all sets."""
        keys = [keys] if isinstance(keys, str) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return {self.decode(m) for m in self.adapter.sinter(nkeys)}

    def sinterstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the intersection of sets at dest."""
        keys = [keys] if isinstance(keys, str) else keys
        # Use specific versions if provided, otherwise fall back to version
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return self.adapter.sinterstore(dest, nkeys)

    def sismember(
        self,
        key: str,
        member: Any,
        version: int | None = None,
    ) -> bool:
        """Check if member is in set."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.sismember(key, self.encode(member))

    def smembers(
        self,
        key: str,
        version: int | None = None,
    ) -> _set[Any]:
        """Get all members of a set."""
        key = self.make_and_validate_key(key, version=version)
        return {self.decode(m) for m in self.adapter.smembers(key)}

    def smove(
        self,
        src: str,
        dst: str,
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
        return self.adapter.smove(src, dst, self.encode(member))

    def spop(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return random member(s) from set."""
        key = self.make_and_validate_key(key, version=version)
        result = self.adapter.spop(key, count)
        if result is None:
            return None
        if isinstance(result, (list, set)):
            return type(result)(self.decode(m) for m in result)
        return self.decode(result)

    def srandmember(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any]:
        """Get random member(s) from set without removing."""
        key = self.make_and_validate_key(key, version=version)
        result = self.adapter.srandmember(key, count)
        if result is None:
            return None
        if isinstance(result, list):
            return [self.decode(m) for m in result]
        return self.decode(result)

    def srem(
        self,
        key: str,
        *members: Any,
        version: int | None = None,
    ) -> int:
        """Remove members from a set."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.srem(key, *(self.encode(m) for m in members))

    def sunion(
        self,
        keys: str | Sequence[str],
        version: int | None = None,
    ) -> _set[Any]:
        """Return the union of all sets."""
        keys = [keys] if isinstance(keys, str) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return {self.decode(m) for m in self.adapter.sunion(nkeys)}

    def sunionstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the union of sets at dest."""
        keys = [keys] if isinstance(keys, str) else keys
        # Use specific versions if provided, otherwise fall back to version
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return self.adapter.sunionstore(dest, nkeys)

    def smismember(
        self,
        key: str,
        *members: Any,
        version: int | None = None,
    ) -> list[bool]:
        """Check if multiple values are members of a set."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.smismember(key, *(self.encode(m) for m in members))

    def sscan(
        self,
        key: str,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, _set[Any]]:
        """Incrementally iterate over set members."""
        key = self.make_and_validate_key(key, version=version)
        new_cursor, members = self.adapter.sscan(key, cursor=cursor, match=match, count=count)
        return (new_cursor, {self.decode(m) for m in members})

    def sscan_iter(
        self,
        key: str,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> Iterator[Any]:
        """Iterate over set members using SSCAN."""
        key = self.make_and_validate_key(key, version=version)
        for member in self.adapter.sscan_iter(key, match=match, count=count):
            yield self.decode(member)

    async def asadd(
        self,
        key: str,
        *members: Any,
        version: int | None = None,
    ) -> int:
        """Add members to a set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.asadd(key, *(self.encode(m) for m in members))

    async def ascard(self, key: str, version: int | None = None) -> int:
        """Get the number of members in a set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.ascard(key)

    async def asdiff(
        self,
        keys: str | Sequence[str],
        version: int | None = None,
    ) -> _set[Any]:
        """Return the difference between the first set and all successive sets asynchronously."""
        keys = [keys] if isinstance(keys, str) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return {self.decode(m) for m in await self.adapter.asdiff(nkeys)}

    async def asdiffstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the difference of sets at dest asynchronously."""
        keys = [keys] if isinstance(keys, str) else keys
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return await self.adapter.asdiffstore(dest, nkeys)

    async def asinter(
        self,
        keys: str | Sequence[str],
        version: int | None = None,
    ) -> _set[Any]:
        """Return the intersection of all sets asynchronously."""
        keys = [keys] if isinstance(keys, str) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return {self.decode(m) for m in await self.adapter.asinter(nkeys)}

    async def asinterstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the intersection of sets at dest asynchronously."""
        keys = [keys] if isinstance(keys, str) else keys
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return await self.adapter.asinterstore(dest, nkeys)

    async def asismember(
        self,
        key: str,
        member: Any,
        version: int | None = None,
    ) -> bool:
        """Check if member is in set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.asismember(key, self.encode(member))

    async def asmembers(
        self,
        key: str,
        version: int | None = None,
    ) -> _set[Any]:
        """Get all members of a set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return {self.decode(m) for m in await self.adapter.asmembers(key)}

    async def asmove(
        self,
        src: str,
        dst: str,
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
        return await self.adapter.asmove(src, dst, self.encode(member))

    async def aspop(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return random member(s) from set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        result = await self.adapter.aspop(key, count)
        if result is None:
            return None
        if isinstance(result, (list, set)):
            return type(result)(self.decode(m) for m in result)
        return self.decode(result)

    async def asrandmember(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any]:
        """Get random member(s) from set without removing asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        result = await self.adapter.asrandmember(key, count)
        if result is None:
            return None
        if isinstance(result, list):
            return [self.decode(m) for m in result]
        return self.decode(result)

    async def asrem(
        self,
        key: str,
        *members: Any,
        version: int | None = None,
    ) -> int:
        """Remove members from a set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.asrem(key, *(self.encode(m) for m in members))

    async def asunion(
        self,
        keys: str | Sequence[str],
        version: int | None = None,
    ) -> _set[Any]:
        """Return the union of all sets asynchronously."""
        keys = [keys] if isinstance(keys, str) else keys
        nkeys = [self.make_and_validate_key(k, version=version) for k in keys]
        return {self.decode(m) for m in await self.adapter.asunion(nkeys)}

    async def asunionstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the union of sets at dest asynchronously."""
        keys = [keys] if isinstance(keys, str) else keys
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        dest = self.make_and_validate_key(dest, version=dest_ver)
        nkeys = [self.make_and_validate_key(k, version=keys_ver) for k in keys]
        return await self.adapter.asunionstore(dest, nkeys)

    async def asmismember(
        self,
        key: str,
        *members: Any,
        version: int | None = None,
    ) -> list[bool]:
        """Check if multiple values are members of a set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.asmismember(key, *(self.encode(m) for m in members))

    async def asscan(
        self,
        key: str,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, _set[Any]]:
        """Incrementally iterate over set members asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        new_cursor, members = await self.adapter.asscan(key, cursor=cursor, match=match, count=count)
        return (new_cursor, {self.decode(m) for m in members})

    async def asscan_iter(
        self,
        key: str,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> AsyncIterator[Any]:
        """Iterate over set members using SSCAN asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        async for member in self.adapter.asscan_iter(key, match=match, count=count):
            yield self.decode(member)

    # =========================================================================
    # Sorted Set Operations
    # =========================================================================

    def zadd(
        self,
        key: str,
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
        encoded = {self.encode(member): score for member, score in mapping.items()}
        return self.adapter.zadd(key, encoded, nx=nx, xx=xx, ch=ch, gt=gt, lt=lt)

    def zcard(self, key: str, version: int | None = None) -> int:
        """Get the number of members in a sorted set."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.zcard(key)

    def zcount(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Count members with scores between min and max."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.zcount(key, min_score, max_score)

    def zincrby(
        self,
        key: str,
        amount: float,
        member: Any,
        version: int | None = None,
    ) -> float:
        """Increment the score of a member."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.zincrby(key, amount, self.encode(member))

    def zpopmax(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        key = self.make_and_validate_key(key, version=version)
        return [(self.decode(m), s) for m, s in self.adapter.zpopmax(key, count)]

    def zpopmin(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        key = self.make_and_validate_key(key, version=version)
        return [(self.decode(m), s) for m, s in self.adapter.zpopmin(key, count)]

    def zrange(
        self,
        key: str,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index."""
        key = self.make_and_validate_key(key, version=version)
        result = self.adapter.zrange(key, start, end, withscores=withscores)
        if withscores:
            return [(self.decode(m), s) for m, s in result]
        return [self.decode(m) for m in result]

    def zrangebyscore(
        self,
        key: str,
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
        result = self.adapter.zrangebyscore(key, min_score, max_score, withscores=withscores, start=start, num=num)
        if withscores:
            return [(self.decode(m), s) for m, s in result]
        return [self.decode(m) for m in result]

    def zrank(
        self,
        key: str,
        member: Any,
        version: int | None = None,
    ) -> int | None:
        """Get the rank of a member (0-based, lowest score first)."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.zrank(key, self.encode(member))

    def zrem(
        self,
        key: str,
        *members: Any,
        version: int | None = None,
    ) -> int:
        """Remove members from a sorted set."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.zrem(key, *(self.encode(m) for m in members))

    def zremrangebyscore(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Remove members with scores between min and max."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.zremrangebyscore(key, min_score, max_score)

    def zremrangebyrank(
        self,
        key: str,
        start: int,
        end: int,
        version: int | None = None,
    ) -> int:
        """Remove members by rank range."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.zremrangebyrank(key, start, end)

    def zrevrange(
        self,
        key: str,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index, highest to lowest."""
        key = self.make_and_validate_key(key, version=version)
        result = self.adapter.zrevrange(key, start, end, withscores=withscores)
        if withscores:
            return [(self.decode(m), s) for m, s in result]
        return [self.decode(m) for m in result]

    def zrevrangebyscore(
        self,
        key: str,
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
        result = self.adapter.zrevrangebyscore(key, max_score, min_score, withscores=withscores, start=start, num=num)
        if withscores:
            return [(self.decode(m), s) for m, s in result]
        return [self.decode(m) for m in result]

    def zscore(
        self,
        key: str,
        member: Any,
        version: int | None = None,
    ) -> float | None:
        """Get the score of a member."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.zscore(key, self.encode(member))

    def zrevrank(
        self,
        key: str,
        member: Any,
        version: int | None = None,
    ) -> int | None:
        """Get the rank of a member (0-based, highest score first)."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.zrevrank(key, self.encode(member))

    def zmscore(
        self,
        key: str,
        *members: Any,
        version: int | None = None,
    ) -> list[float | None]:
        """Get the scores of multiple members."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.zmscore(key, *(self.encode(m) for m in members))

    async def azadd(
        self,
        key: str,
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
        encoded = {self.encode(member): score for member, score in mapping.items()}
        return await self.adapter.azadd(key, encoded, nx=nx, xx=xx, ch=ch, gt=gt, lt=lt)

    async def azcard(self, key: str, version: int | None = None) -> int:
        """Get the number of members in a sorted set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.azcard(key)

    async def azcount(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Count members with scores between min and max asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.azcount(key, min_score, max_score)

    async def azincrby(
        self,
        key: str,
        amount: float,
        member: Any,
        version: int | None = None,
    ) -> float:
        """Increment the score of a member asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.azincrby(key, amount, self.encode(member))

    async def azpopmax(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return [(self.decode(m), s) for m, s in await self.adapter.azpopmax(key, count)]

    async def azpopmin(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return [(self.decode(m), s) for m, s in await self.adapter.azpopmin(key, count)]

    async def azrange(
        self,
        key: str,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        result = await self.adapter.azrange(key, start, end, withscores=withscores)
        if withscores:
            return [(self.decode(m), s) for m, s in result]
        return [self.decode(m) for m in result]

    async def azrangebyscore(
        self,
        key: str,
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
        result = await self.adapter.azrangebyscore(
            key,
            min_score,
            max_score,
            withscores=withscores,
            start=start,
            num=num,
        )
        if withscores:
            return [(self.decode(m), s) for m, s in result]
        return [self.decode(m) for m in result]

    async def azrank(
        self,
        key: str,
        member: Any,
        version: int | None = None,
    ) -> int | None:
        """Get the rank of a member (0-based, lowest score first) asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.azrank(key, self.encode(member))

    async def azrem(
        self,
        key: str,
        *members: Any,
        version: int | None = None,
    ) -> int:
        """Remove members from a sorted set asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.azrem(key, *(self.encode(m) for m in members))

    async def azremrangebyscore(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Remove members with scores between min and max asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.azremrangebyscore(key, min_score, max_score)

    async def azremrangebyrank(
        self,
        key: str,
        start: int,
        end: int,
        version: int | None = None,
    ) -> int:
        """Remove members by rank range asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.azremrangebyrank(key, start, end)

    async def azrevrange(
        self,
        key: str,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index, highest to lowest, asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        result = await self.adapter.azrevrange(key, start, end, withscores=withscores)
        if withscores:
            return [(self.decode(m), s) for m, s in result]
        return [self.decode(m) for m in result]

    async def azrevrangebyscore(
        self,
        key: str,
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
        result = await self.adapter.azrevrangebyscore(
            key,
            max_score,
            min_score,
            withscores=withscores,
            start=start,
            num=num,
        )
        if withscores:
            return [(self.decode(m), s) for m, s in result]
        return [self.decode(m) for m in result]

    async def azscore(
        self,
        key: str,
        member: Any,
        version: int | None = None,
    ) -> float | None:
        """Get the score of a member asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.azscore(key, self.encode(member))

    async def azrevrank(
        self,
        key: str,
        member: Any,
        version: int | None = None,
    ) -> int | None:
        """Get the rank of a member (0-based, highest score first) asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.azrevrank(key, self.encode(member))

    async def azmscore(
        self,
        key: str,
        *members: Any,
        version: int | None = None,
    ) -> list[float | None]:
        """Get the scores of multiple members asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.azmscore(key, *(self.encode(m) for m in members))

    # =========================================================================
    # Stream Operations
    # =========================================================================

    def xadd(
        self,
        key: str,
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
        encoded_fields = {f: self.encode(v) for f, v in fields.items()}
        return self.adapter.xadd(
            key,
            encoded_fields,
            entry_id,
            maxlen=maxlen,
            approximate=approximate,
            nomkstream=nomkstream,
            minid=minid,
            limit=limit,
        )

    async def axadd(
        self,
        key: str,
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
        encoded_fields = {f: self.encode(v) for f, v in fields.items()}
        return await self.adapter.axadd(
            key,
            encoded_fields,
            entry_id,
            maxlen=maxlen,
            approximate=approximate,
            nomkstream=nomkstream,
            minid=minid,
            limit=limit,
        )

    def xlen(self, key: str, version: int | None = None) -> int:
        """Get the number of entries in a stream."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xlen(key)

    async def axlen(self, key: str, version: int | None = None) -> int:
        """Get the number of entries in a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axlen(key)

    def _decode_stream_entries(
        self,
        entries: list[tuple[str, dict[str, Any]]],
    ) -> list[tuple[str, dict[str, Any]]]:
        """Decode field values in a list of (entry_id, fields_dict) entries."""
        return [(entry_id, {f: self.decode(v) for f, v in fields.items()}) for entry_id, fields in entries]

    def xrange(
        self,
        key: str,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in ascending order."""
        key = self.make_and_validate_key(key, version=version)
        return self._decode_stream_entries(self.adapter.xrange(key, start, end, count=count))

    async def axrange(
        self,
        key: str,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in ascending order asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return self._decode_stream_entries(await self.adapter.axrange(key, start, end, count=count))

    def xrevrange(
        self,
        key: str,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in descending order."""
        key = self.make_and_validate_key(key, version=version)
        return self._decode_stream_entries(self.adapter.xrevrange(key, end, start, count=count))

    async def axrevrange(
        self,
        key: str,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Get entries from a stream in descending order asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return self._decode_stream_entries(await self.adapter.axrevrange(key, end, start, count=count))

    def xread(
        self,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from one or more streams."""
        key_map = {self.make_and_validate_key(k, version=version): k for k in streams}
        nstreams: dict[str, str] = {nk: streams[ok] for nk, ok in key_map.items()}
        result = self.adapter.xread(nstreams, count=count, block=block)
        if result is None:
            return None
        return {str(key_map.get(k, k)): self._decode_stream_entries(v) for k, v in result.items()}

    async def axread(
        self,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from one or more streams asynchronously."""
        key_map = {self.make_and_validate_key(k, version=version): k for k in streams}
        nstreams: dict[str, str] = {nk: streams[ok] for nk, ok in key_map.items()}
        result = await self.adapter.axread(nstreams, count=count, block=block)
        if result is None:
            return None
        return {str(key_map.get(k, k)): self._decode_stream_entries(v) for k, v in result.items()}

    def xtrim(
        self,
        key: str,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> int:
        """Trim a stream to a maximum length or minimum ID."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xtrim(key, maxlen=maxlen, approximate=approximate, minid=minid, limit=limit)

    async def axtrim(
        self,
        key: str,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> int:
        """Trim a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axtrim(key, maxlen=maxlen, approximate=approximate, minid=minid, limit=limit)

    def xdel(self, key: str, *entry_ids: str, version: int | None = None) -> int:
        """Delete entries from a stream."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xdel(key, *entry_ids)

    async def axdel(self, key: str, *entry_ids: str, version: int | None = None) -> int:
        """Delete entries from a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axdel(key, *entry_ids)

    def xinfo_stream(self, key: str, full: bool = False, version: int | None = None) -> dict[str, Any]:
        """Get information about a stream."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xinfo_stream(key, full=full)

    async def axinfo_stream(self, key: str, full: bool = False, version: int | None = None) -> dict[str, Any]:
        """Get information about a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axinfo_stream(key, full=full)

    def xinfo_groups(self, key: str, version: int | None = None) -> list[dict[str, Any]]:
        """Get information about consumer groups for a stream."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xinfo_groups(key)

    async def axinfo_groups(self, key: str, version: int | None = None) -> list[dict[str, Any]]:
        """Get information about consumer groups for a stream asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axinfo_groups(key)

    def xinfo_consumers(self, key: str, group: str, version: int | None = None) -> list[dict[str, Any]]:
        """Get information about consumers in a group."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xinfo_consumers(key, group)

    async def axinfo_consumers(self, key: str, group: str, version: int | None = None) -> list[dict[str, Any]]:
        """Get information about consumers in a group asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axinfo_consumers(key, group)

    def xgroup_create(
        self,
        key: str,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool:
        """Create a consumer group."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xgroup_create(key, group, entry_id, mkstream=mkstream, entries_read=entries_read)

    async def axgroup_create(
        self,
        key: str,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool:
        """Create a consumer group asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axgroup_create(key, group, entry_id, mkstream=mkstream, entries_read=entries_read)

    def xgroup_destroy(self, key: str, group: str, version: int | None = None) -> int:
        """Destroy a consumer group."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xgroup_destroy(key, group)

    async def axgroup_destroy(self, key: str, group: str, version: int | None = None) -> int:
        """Destroy a consumer group asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axgroup_destroy(key, group)

    def xgroup_setid(
        self,
        key: str,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool:
        """Set the last delivered ID for a consumer group."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xgroup_setid(key, group, entry_id, entries_read=entries_read)

    async def axgroup_setid(
        self,
        key: str,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool:
        """Set the last delivered ID for a consumer group asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axgroup_setid(key, group, entry_id, entries_read=entries_read)

    def xgroup_delconsumer(self, key: str, group: str, consumer: str, version: int | None = None) -> int:
        """Remove a consumer from a group."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xgroup_delconsumer(key, group, consumer)

    async def axgroup_delconsumer(self, key: str, group: str, consumer: str, version: int | None = None) -> int:
        """Remove a consumer from a group asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axgroup_delconsumer(key, group, consumer)

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from streams as a consumer group member."""
        key_map = {self.make_and_validate_key(k, version=version): k for k in streams}
        nstreams: dict[str, str] = {nk: streams[ok] for nk, ok in key_map.items()}
        result = self.adapter.xreadgroup(group, consumer, nstreams, count=count, block=block, noack=noack)
        if result is None:
            return None
        return {str(key_map.get(k, k)): self._decode_stream_entries(v) for k, v in result.items()}

    async def axreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Read entries from streams as a consumer group member asynchronously."""
        key_map = {self.make_and_validate_key(k, version=version): k for k in streams}
        nstreams: dict[str, str] = {nk: streams[ok] for nk, ok in key_map.items()}
        result = await self.adapter.axreadgroup(group, consumer, nstreams, count=count, block=block, noack=noack)
        if result is None:
            return None
        return {str(key_map.get(k, k)): self._decode_stream_entries(v) for k, v in result.items()}

    def xack(self, key: str, group: str, *entry_ids: str, version: int | None = None) -> int:
        """Acknowledge message processing."""
        key = self.make_and_validate_key(key, version=version)
        return self.adapter.xack(key, group, *entry_ids)

    async def axack(self, key: str, group: str, *entry_ids: str, version: int | None = None) -> int:
        """Acknowledge message processing asynchronously."""
        key = self.make_and_validate_key(key, version=version)
        return await self.adapter.axack(key, group, *entry_ids)

    def xpending(
        self,
        key: str,
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
        return self.adapter.xpending(key, group, start=start, end=end, count=count, consumer=consumer, idle=idle)

    async def axpending(
        self,
        key: str,
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
        return await self.adapter.axpending(key, group, start=start, end=end, count=count, consumer=consumer, idle=idle)

    def xclaim(
        self,
        key: str,
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
        result = self.adapter.xclaim(
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
        # justid=True returns list[str] (entry IDs only); else list[(id, fields_dict)].
        if justid:
            return result
        return self._decode_stream_entries(cast("list[tuple[str, dict[str, Any]]]", result))

    async def axclaim(
        self,
        key: str,
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
        result = await self.adapter.axclaim(
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
            return result
        return self._decode_stream_entries(cast("list[tuple[str, dict[str, Any]]]", result))

    def xautoclaim(
        self,
        key: str,
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
        next_id, claimed, deleted = self.adapter.xautoclaim(
            key,
            group,
            consumer,
            min_idle_time,
            start_id,
            count=count,
            justid=justid,
        )
        if justid:
            return (next_id, claimed, deleted)
        return (next_id, self._decode_stream_entries(cast("list[tuple[str, dict[str, Any]]]", claimed)), deleted)

    async def axautoclaim(
        self,
        key: str,
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
        next_id, claimed, deleted = await self.adapter.axautoclaim(
            key,
            group,
            consumer,
            min_idle_time,
            start_id,
            count=count,
            justid=justid,
        )
        if justid:
            return (next_id, claimed, deleted)
        return (next_id, self._decode_stream_entries(cast("list[tuple[str, dict[str, Any]]]", claimed)), deleted)

    # =========================================================================
    # Direct Client Access
    # =========================================================================

    def get_client(self, key: str | None = None, *, write: bool = False) -> Any:
        """Get the underlying Redis client."""
        return self.adapter.get_client(key, write=write)

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get server information and statistics.

        Returns the Redis/Valkey INFO command output as a dictionary.
        Optionally filter by section (e.g., 'server', 'memory', 'stats').
        """
        if section:
            return self.adapter.info(section)
        return self.adapter.info()

    def slowlog_get(self, count: int = 10) -> list[Any]:
        """Get slow query log entries.

        Returns up to ``count`` entries from the Redis SLOWLOG.
        """
        return list(self.adapter.slowlog_get(count))

    def slowlog_len(self) -> int:
        """Get the number of entries in the slow query log."""
        return int(self.adapter.slowlog_len())

    # =========================================================================
    # Key Operations
    # =========================================================================

    def rename(
        self,
        src: str,
        dst: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key atomically."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src_key = self.make_and_validate_key(src, version=src_ver)
        dst_key = self.make_and_validate_key(dst, version=dst_ver)
        return self.adapter.rename(src_key, dst_key)

    async def arename(
        self,
        src: str,
        dst: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key atomically asynchronously."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src_key = self.make_and_validate_key(src, version=src_ver)
        dst_key = self.make_and_validate_key(dst, version=dst_ver)
        return await self.adapter.arename(src_key, dst_key)

    def renamenx(
        self,
        src: str,
        dst: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key only if the destination does not exist."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src_key = self.make_and_validate_key(src, version=src_ver)
        dst_key = self.make_and_validate_key(dst, version=dst_ver)
        return self.adapter.renamenx(src_key, dst_key)

    async def arenamenx(
        self,
        src: str,
        dst: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key only if the destination does not exist asynchronously."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        src_key = self.make_and_validate_key(src, version=src_ver)
        dst_key = self.make_and_validate_key(dst, version=dst_ver)
        return await self.adapter.arenamenx(src_key, dst_key)

    @override
    def incr_version(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Atomically increment the version of a key using RENAME."""
        if version is None:
            version = self.version
        old_key = self.make_and_validate_key(key, version=version)
        new_key = self.make_and_validate_key(key, version=version + delta)
        self.adapter.rename(old_key, new_key)
        return version + delta

    @override
    def decr_version(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Atomically decrement the version of a key."""
        return self.incr_version(key, -delta, version)

    @override
    async def aincr_version(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Atomically increment the version of a key using RENAME asynchronously."""
        if version is None:
            version = self.version
        old_key = self.make_and_validate_key(key, version=version)
        new_key = self.make_and_validate_key(key, version=version + delta)
        await self.adapter.arename(old_key, new_key)
        return version + delta

    @override
    async def adecr_version(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Atomically decrement the version of a key asynchronously."""
        return await self.aincr_version(key, -delta, version)

    # =========================================================================
    # Lua Script Operations
    # =========================================================================

    def _create_script_helpers(self, version: int | None) -> ScriptHelpers:
        """Create a ScriptHelpers instance for script processing."""
        return ScriptHelpers(
            make_key=self.make_and_validate_key,
            encode=self.encode,
            decode=self.decode,
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

        result = self.adapter.eval(script, len(proc_keys), *proc_keys, *proc_args)

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

        result = await self.adapter.aeval(script, len(proc_keys), *proc_keys, *proc_args)

        if post_hook is not None:
            result = post_hook(helpers, result)

        return result


class RespClusterCache(RespCache):
    """Cluster cache backend base class.

    Extends :class:`RespCache` with cluster-specific behaviour (no
    transactions on pipelines). Subclasses set ``_adapter_class`` to
    their specific cluster adapter.
    """

    def pipeline(
        self,
        *,
        transaction: bool = True,
        version: int | None = None,
    ) -> Pipeline:
        """Create a pipeline. Cluster pipelines never use transactions."""
        return super().pipeline(transaction=False, version=version)

    def apipeline(
        self,
        *,
        transaction: bool = True,
        version: int | None = None,
    ) -> AsyncPipeline:
        """Create an async pipeline. Cluster pipelines never use transactions."""
        return super().apipeline(transaction=False, version=version)


class RespSentinelCache(RespCache):
    """Sentinel cache backend base class.

    Subclasses set ``_adapter_class`` to their specific sentinel adapter.
    """


__all__ = ["RespCache", "RespClusterCache", "RespSentinelCache"]
