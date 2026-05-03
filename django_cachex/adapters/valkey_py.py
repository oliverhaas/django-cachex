"""Adapters for the ``valkey-py`` driver.

Three topologies live here, each a self-contained subclass of
:class:`~django_cachex.adapters.protocols.RespAdapterProtocol`:

- :class:`ValkeyPyAdapter` — single-node / replicated. Carries the full
  command surface against a redis-py-API-compatible client and the
  pool-construction machinery; subclasses (including
  :class:`~django_cachex.adapters.redis_py.RedisPyAdapter`) inherit it
  wholesale and only swap in lib-specific class slots.
- :class:`ValkeyPySentinelAdapter` — Sentinel-discovered primary/replicas
  on top of the single-node base.
- :class:`ValkeyPyClusterAdapter` — Cluster mode on top of the single-node
  base.

If ``valkey-py`` isn't installed, the constructor raises a clean
:class:`ImportError` instead of failing later with an obscure attribute
error. Subclasses for other libs (``redis-py`` via
:mod:`~django_cachex.adapters.redis_py`) override ``_LIB_AVAILABLE``
and ``_missing_lib_error`` to redirect the check.
"""

import asyncio
import random
import weakref
from collections import defaultdict
from itertools import batched
from typing import TYPE_CHECKING, Any, cast, override
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from django_cachex.adapters.protocols import RespAdapterProtocol, RespAsyncPipelineProtocol, RespPipelineProtocol
from django_cachex.exceptions import NotSupportedError, _main_exceptions
from django_cachex.stampede import (
    StampedeConfig,
    get_timeout_with_buffer,
    make_stampede_config,
    resolve_stampede,
    should_recompute,
)
from django_cachex.types import KeyType

if TYPE_CHECKING:
    import builtins
    from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Mapping, Sequence
    from datetime import datetime, timedelta

    from redis.connection import ConnectionPool

    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT

# Alias for the `set` builtin shadowed by the `set` method (PEP 649 defers
# annotations at runtime, but type checkers still resolve them in class scope).
# The `type` shadow uses ``builtins.type[X]`` directly — module-level
# aliases of `type` don't survive subscript through mypy's name resolution.
_set = set


# =============================================================================
# Process-wide async connection pool registry
# =============================================================================
# Django's ``asgiref.local.Local``-backed cache handler returns a fresh
# ``BaseCache`` instance per asyncio task, which means our cached adapter
# (and any instance-level pool dict) is fresh per task. Without a
# process-wide registry every async cache call creates a brand-new pool —
# ``max_connections`` is moot because each call gets its own pool. Sharing
# pools at the module level by event loop + config makes the cap effective
# and brings pool-based backends in line with the multiplexed rust-valkey
# driver.
#
# The registry itself lives on each driver-specific adapter class as the
# ``_async_pools`` class attribute (see :mod:`~django_cachex.adapters.redis_py`
# for the parallel one); each driver gets its own, isolated by construction.
#
# Outer ``WeakKeyDictionary`` keyed by the asyncio loop:
#  - When a loop is GC'd (process shutdown, ``asyncio.run`` finishing, fork
#    re-creating the loop in a child), its entry vanishes automatically.
#  - On fork the child's new loop becomes the live reference; the parent's
#    stale entries become unreachable and are dropped on the next mutation,
#    so we don't need explicit PID-tracking.
#
# Inner ``dict`` is keyed by ``(pool class, url, options key, index)`` so
# different cache configurations in the same process don't share a pool.

AsyncPoolsRegistry = weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[tuple[Any, ...], Any]]


def _options_key(options: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    """Make a hashable key from pool options."""
    out: list[tuple[str, Any]] = []
    for k in sorted(options):
        v = options[k]
        # Class objects, strings, ints, floats and tuples are all hashable.
        if isinstance(v, (type, str, int, float, bool)) or v is None:
            out.append((k, v))
        else:
            # Fall back to repr for unhashable / opaque values.
            out.append((k, repr(v)))
    return tuple(out)


_VALKEY_ASYNC_POOLS: AsyncPoolsRegistry = weakref.WeakKeyDictionary()

_VALKEY_AVAILABLE = False
try:
    import valkey
    from valkey.asyncio import ConnectionPool as ValkeyAsyncConnectionPool
    from valkey.asyncio import Valkey as ValkeyAsyncClient
    from valkey.asyncio.cluster import ValkeyCluster as AsyncValkeyCluster
    from valkey.asyncio.sentinel import Sentinel as AsyncValkeySentinel
    from valkey.asyncio.sentinel import SentinelConnectionPool as AsyncValkeySentinelConnectionPool
    from valkey.cluster import ValkeyCluster
    from valkey.cluster import key_slot as valkey_key_slot
    from valkey.sentinel import Sentinel as ValkeySentinel
    from valkey.sentinel import SentinelConnectionPool as ValkeySentinelConnectionPool

    _VALKEY_AVAILABLE = True
except ImportError:
    valkey = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]


def _missing_valkey() -> ImportError:
    return ImportError(
        "valkey-py is required for ValkeyPyAdapter (and friends). Install it with: pip install django-cachex[valkey-py]",
    )


class ValkeyPyAdapter(RespAdapterProtocol):
    """Cache adapter implementation for redis-py-shaped clients.

    Implements the full command surface against a redis-py-API-compatible
    client (``valkey-py`` by default; :class:`~django_cachex.adapters.redis_py.RedisPyAdapter`
    swaps in ``redis-py`` via class-slot overrides). Carries the
    pool-construction machinery, the command surface, and the valkey-py
    availability check; the topology subclasses (:class:`ValkeyPySentinelAdapter`,
    :class:`ValkeyPyClusterAdapter`) inherit from this class and add
    Sentinel / Cluster-specific overrides.
    """

    # Class slots — concrete drivers override under their availability flag.
    _lib: Any = None
    _client_class: builtins.type[Any] | None = None
    _pool_class: builtins.type[Any] | None = None
    _async_client_class: builtins.type[Any] | None = None
    _async_pool_class: builtins.type[Any] | None = None

    # Polymorphic availability check — redis-py subclasses override
    # ``_LIB_AVAILABLE`` and ``_missing_lib_error`` (via mixin) to redirect
    # the ImportError without overriding ``__init__``.
    _LIB_AVAILABLE: bool = _VALKEY_AVAILABLE

    _async_pools = _VALKEY_ASYNC_POOLS

    if _VALKEY_AVAILABLE:
        _lib = valkey
        _client_class = valkey.Valkey
        _pool_class = valkey.ConnectionPool
        _async_client_class = ValkeyAsyncClient
        _async_pool_class = ValkeyAsyncConnectionPool

    # Default scan iteration batch size
    _default_scan_itersize: int = 100

    # Options that shouldn't be passed to the connection pool
    _CLIENT_ONLY_OPTIONS = frozenset(
        {
            "compressor",
            "serializer",
            "sentinels",
            "sentinel_kwargs",
            "async_pool_class",
            "reverse_key_function",
            "stampede_prevention",
        },
    )

    @staticmethod
    def _missing_lib_error() -> ImportError:
        return _missing_valkey()

    def __init__(
        self,
        servers: list[str],
        pool_class: str | builtins.type[Any] | None = None,
        parser_class: str | builtins.type[Any] | None = None,
        async_pool_class: str | builtins.type[Any] | None = None,
        **options: Any,
    ) -> None:
        """Initialize the wire-level adapter.

        Encoding (serializer + compressor) is owned by the cache layer
        (``RespCache``); adapter methods take and return raw bytes.
        ``serializer`` / ``compressor`` keys in ``options`` are silently
        ignored here — the cache reads them directly from its own options.
        """
        if not self._LIB_AVAILABLE:
            raise self._missing_lib_error()

        self._servers = servers
        self._options = options
        self._pools: dict[int, Any] = {}
        self._stampede_config: StampedeConfig | None = make_stampede_config(options.get("stampede_prevention"))

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

    # =========================================================================
    # Stampede + replica-index helpers
    # =========================================================================

    def _resolve_stampede(self, stampede_prevention: bool | dict | None = None) -> StampedeConfig | None:
        return resolve_stampede(self._stampede_config, stampede_prevention)

    def _get_timeout_with_buffer(
        self,
        timeout: int | None,
        stampede_prevention: bool | dict | None = None,
    ) -> int | None:
        return get_timeout_with_buffer(timeout, self._stampede_config, stampede_prevention)

    def _get_connection_pool_index(self, *, write: bool) -> int:
        """Get the pool index for read/write operations."""
        # Write to first server, read from any replica
        if write or len(self._servers) == 1:
            return 0
        return random.randint(1, len(self._servers) - 1)  # noqa: S311

    @staticmethod
    def _normalize_ttl(result: int) -> int | None:
        """Normalize Redis TTL/PTTL/EXPIRETIME results.

        -1 (no expiry) → None, -2 (key missing) → -2, positive → as-is.
        """
        if result == -1:
            return None
        return result

    def _get_connection_pool(self, *, write: bool) -> Any:
        """Get a connection pool for the given operation type."""
        index = self._get_connection_pool_index(write=write)
        if index not in self._pools:
            if self._pool_class is None:
                msg = "Subclasses must set _pool_class"
                raise RuntimeError(msg)
            self._pools[index] = self._pool_class.from_url(
                self._servers[index],
                **self._pool_options,
            )
        return self._pools[index]

    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get a client connection."""
        pool = self._get_connection_pool(write=write)
        if self._client_class is None:
            msg = "Subclasses must set _client_class"
            raise RuntimeError(msg)
        return self._client_class(connection_pool=pool)

    # =========================================================================
    # Async Connection Pool Management
    # =========================================================================

    def _get_async_connection_pool(self, *, write: bool) -> Any:
        """Get an async connection pool, cached process-wide per (loop, config).

        The cache lives on the driver-specific class (``_async_pools``)
        rather than the instance: Django's ``asgiref.local.Local`` returns a
        fresh ``BaseCache`` per asyncio task, so an instance-level dict
        would be empty on every request and a brand-new pool would be
        created each time, defeating ``max_connections`` entirely.
        """
        loop = asyncio.get_running_loop()
        index = self._get_connection_pool_index(write=write)

        if self._async_pool_class is None:
            msg = "Async operations require _async_pool_class to be set. Use RedisAdapter or ValkeyAdapter."
            raise RuntimeError(msg)

        # Filter out parser_class — it's sync-specific.
        async_pool_options: dict[str, Any] = {k: v for k, v in self._pool_options.items() if k != "parser_class"}
        # Default cap so concurrent async load can't grow the pool unbounded.
        # Users who need more can set ``max_connections`` in OPTIONS.
        async_pool_options.setdefault("max_connections", 50)

        url = self._servers[index]
        key = (self._async_pool_class, url, _options_key(async_pool_options), index)

        sub = self._async_pools.get(loop)
        if sub is None:
            sub = {}
            self._async_pools[loop] = sub

        pool = sub.get(key)
        if pool is None:
            pool = self._async_pool_class.from_url(url, **async_pool_options)
            sub[key] = pool
        return pool

    def get_async_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get an async client connection."""
        pool = self._get_async_connection_pool(write=write)
        if self._async_client_class is None:
            msg = "Async operations require _async_client_class to be set. Use RedisPyAdapter or ValkeyPyAdapter."
            raise RuntimeError(msg)
        return self._async_client_class(connection_pool=pool)

    def close(self, **kwargs: Any) -> None:
        """No-op. Pools live for the instance's lifetime (matches Django's BaseCache)."""

    async def aclose(self, **kwargs: Any) -> None:
        """No-op. Pools live for the instance's lifetime (matches Django's BaseCache)."""

    # =========================================================================
    # Core Cache Operations
    # =========================================================================

    def add(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool:
        """Set a value only if the key doesn't exist."""
        client = self.get_client(key, write=True)
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            if ret := bool(client.set(key, nvalue, nx=True)):
                client.delete(key)
            return ret
        return bool(client.set(key, nvalue, nx=True, ex=actual_timeout))

    async def aadd(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool:
        """Set a value only if the key doesn't exist, asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            if ret := bool(await client.set(key, nvalue, nx=True)):
                await client.delete(key)
            return ret
        return bool(await client.set(key, nvalue, nx=True, ex=actual_timeout))

    def get(self, key: KeyT, *, stampede_prevention: bool | dict | None = None) -> Any:
        """Fetch a value from the cache."""
        client = self.get_client(key, write=False)
        val = client.get(key)
        if val is None:
            return None
        config = self._resolve_stampede(stampede_prevention)
        if config and isinstance(val, bytes):
            ttl = client.ttl(key)
            if ttl > 0 and should_recompute(ttl, config):
                return None
        return val

    async def aget(self, key: KeyT, *, stampede_prevention: bool | dict | None = None) -> Any:
        """Fetch a value from the cache asynchronously."""
        client = self.get_async_client(key, write=False)
        val = await client.get(key)
        if val is None:
            return None
        config = self._resolve_stampede(stampede_prevention)
        if config and isinstance(val, bytes):
            ttl = await client.ttl(key)
            if ttl > 0 and should_recompute(ttl, config):
                return None
        return val

    def set(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> None:
        """Set a value in the cache (standard Django interface)."""
        client = self.get_client(key, write=True)
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            client.delete(key)
        else:
            client.set(key, nvalue, ex=actual_timeout)

    async def aset(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> None:
        """Set a value in the cache asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            await client.delete(key)
        else:
            await client.set(key, nvalue, ex=actual_timeout)

    def set_with_flags(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        stampede_prevention: bool | dict | None = None,
    ) -> bool | Any:
        """Set a value with nx/xx/get flags.

        Returns bool for nx/xx (success status), or the previous value
        (decoded) when get=True.
        """
        client = self.get_client(key, write=True)
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            if get:
                return None
            return False
        result = client.set(key, nvalue, ex=actual_timeout, nx=nx, xx=xx, get=get)
        if get:
            if result is None:
                return None
            return result
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
        stampede_prevention: bool | dict | None = None,
    ) -> bool | Any:
        """Set a value with nx/xx/get flags asynchronously.

        Returns bool for nx/xx (success status), or the previous value
        (decoded) when get=True.
        """
        client = self.get_async_client(key, write=True)
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            if get:
                return None
            return False
        result = await client.set(key, nvalue, ex=actual_timeout, nx=nx, xx=xx, get=get)
        if get:
            if result is None:
                return None
            return result
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

    def get_many(self, keys: Iterable[KeyT], *, stampede_prevention: bool | dict | None = None) -> dict[KeyT, Any]:
        """Retrieve many keys."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_client(write=False)
        results = client.mget(keys)

        # Collect non-None results
        found = {k: v for k, v in zip(keys, results, strict=False) if v is not None}

        # Stampede filtering: pipeline TTL for found keys with bytes values
        # (integers bypass stampede, matching get() behavior)
        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            stampede_keys = [k for k, v in found.items() if isinstance(v, bytes)]
            if stampede_keys:
                pipe = client.pipeline()
                for k in stampede_keys:
                    pipe.ttl(k)
                ttls = pipe.execute()
                for k, ttl in zip(stampede_keys, ttls, strict=False):
                    if isinstance(ttl, int) and ttl > 0 and should_recompute(ttl, config):
                        del found[k]

        return dict(found.items())

    async def aget_many(
        self,
        keys: Iterable[KeyT],
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> dict[KeyT, Any]:
        """Retrieve many keys asynchronously."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_async_client(write=False)
        results = await client.mget(keys)

        # Collect non-None results
        found = {k: v for k, v in zip(keys, results, strict=False) if v is not None}

        # Stampede filtering: pipeline TTL for found keys with bytes values
        # (integers bypass stampede, matching aget() behavior)
        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            stampede_keys = [k for k, v in found.items() if isinstance(v, bytes)]
            if stampede_keys:
                pipe = client.pipeline()
                for k in stampede_keys:
                    pipe.ttl(k)
                ttls = await pipe.execute()
                for k, ttl in zip(stampede_keys, ttls, strict=False):
                    if isinstance(ttl, int) and ttl > 0 and should_recompute(ttl, config):
                        del found[k]

        return dict(found.items())

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
        """Increment a value."""
        client = self.get_client(key, write=True)
        return client.incr(key, delta)

    async def aincr(self, key: KeyT, delta: int = 1) -> int:
        """Increment a value asynchronously."""
        client = self.get_async_client(key, write=True)
        return await client.incr(key, delta)

    def set_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        """Set multiple values. timeout=0 deletes keys, None sets without expiry."""
        if not data:
            return []

        client = self.get_client(write=True)
        prepared = dict(data.items())
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            client.delete(*prepared.keys())
        elif actual_timeout is None:
            client.mset(prepared)
        else:
            pipe = client.pipeline()
            pipe.mset(prepared)
            for key in prepared:
                pipe.expire(key, actual_timeout)
            pipe.execute()
        return []

    async def aset_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        """Set multiple values asynchronously."""
        if not data:
            return []

        client = self.get_async_client(write=True)
        prepared = dict(data.items())
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            await client.delete(*prepared.keys())
        elif actual_timeout is None:
            await client.mset(prepared)
        else:
            pipe = client.pipeline()
            pipe.mset(prepared)
            for key in prepared:
                pipe.expire(key, actual_timeout)
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

    # =========================================================================
    # Extended Operations (beyond Django's BaseCache)
    # =========================================================================

    def ttl(self, key: KeyT) -> int | None:
        """Get TTL in seconds. Returns None if no expiry, -2 if key doesn't exist."""
        client = self.get_client(key, write=False)
        return self._normalize_ttl(client.ttl(key))

    def pttl(self, key: KeyT) -> int | None:
        """Get TTL in milliseconds. Returns None if no expiry, -2 if key doesn't exist."""
        client = self.get_client(key, write=False)
        return self._normalize_ttl(client.pttl(key))

    def expiretime(self, key: KeyT) -> int | None:
        """Get the absolute Unix timestamp (seconds) when a key will expire.

        Returns None if the key has no expiry, -2 if the key doesn't exist.
        Requires Redis 7.0+ / Valkey 7.2+.
        """
        client = self.get_client(key, write=False)
        return self._normalize_ttl(client.expiretime(key))

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
        return self._normalize_ttl(await client.ttl(key))

    async def apttl(self, key: KeyT) -> int | None:
        """Get TTL in milliseconds asynchronously."""
        client = self.get_async_client(key, write=False)
        return self._normalize_ttl(await client.pttl(key))

    async def aexpiretime(self, key: KeyT) -> int | None:
        """Get the absolute Unix timestamp (seconds) when a key will expire asynchronously.

        Returns None if the key has no expiry, -2 if the key doesn't exist.
        Requires Redis 7.0+ / Valkey 7.2+.
        """
        client = self.get_async_client(key, write=False)
        return self._normalize_ttl(await client.expiretime(key))

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

    async def ascan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        """Perform a single SCAN iteration asynchronously returning (next_cursor, keys)."""
        client = self.get_async_client(write=False)

        if count is None:
            count = self._default_scan_itersize

        next_cursor, keys = await client.scan(
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

        count = 0
        for batch in batched(client.scan_iter(match=pattern, count=itersize), itersize, strict=False):
            count += cast("int", client.delete(*batch))
        return count

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

        count = 0
        batch: list[Any] = []
        async for key in client.scan_iter(match=pattern, count=itersize):
            batch.append(key)
            if len(batch) >= itersize:
                count += cast("int", await client.delete(*batch))
                batch.clear()
        if batch:
            count += cast("int", await client.delete(*batch))
        return count

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

    def alock(
        self,
        key: str,
        timeout: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        """Get an async distributed lock."""
        client = self.get_async_client(key, write=True)
        return client.lock(
            key,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    def pipeline(self, *, transaction: bool = True) -> ValkeyPyPipelineAdapter:
        """Construct a pipeline adapter (raw command queue) for this driver.

        Returns a :class:`ValkeyPyPipelineAdapter`. ``RespCache.pipeline()``
        wraps the result in a :class:`Pipeline` that adds key-prefixing, value
        encoding, and result decoding.
        """
        client = self.get_client(write=True)
        return ValkeyPyPipelineAdapter(client.pipeline(transaction=transaction))

    def apipeline(self, *, transaction: bool = True) -> ValkeyPyAsyncPipelineAdapter:
        """Construct an async pipeline adapter for this driver.

        Returns a :class:`ValkeyPyAsyncPipelineAdapter` wrapping the underlying
        ``redis.asyncio`` / ``valkey.asyncio`` async pipeline. ``RespCache.apipeline()``
        wraps the result in an :class:`AsyncPipeline`.
        """
        client = self.get_async_client(write=True)
        return ValkeyPyAsyncPipelineAdapter(client.pipeline(transaction=transaction))

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
        items: list[Any] | None = None,
    ) -> int:
        """Set hash field(s). Use field/value, mapping, or items (flat key-value pairs)."""
        client = self.get_client(key, write=True)
        nvalue = value if field is not None else None
        nmapping = dict(mapping.items()) if mapping else None
        nitems = list(items) if items else None

        return cast("int", client.hset(key, field, nvalue, mapping=nmapping, items=nitems))

    def hsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        """Set a hash field only if it doesn't exist."""
        client = self.get_client(key, write=True)
        nvalue = value

        return bool(client.hsetnx(key, field, nvalue))

    def hget(self, key: KeyT, field: str) -> Any | None:
        """Get a hash field."""
        client = self.get_client(key, write=False)

        val = client.hget(key, field)
        return val if val is not None else None

    def hmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        """Get multiple hash fields."""
        client = self.get_client(key, write=False)

        values = client.hmget(key, fields)
        return [v if v is not None else None for v in values]

    def hgetall(self, key: KeyT) -> dict[str, Any]:
        """Get all hash fields."""
        client = self.get_client(key, write=False)

        raw = client.hgetall(key)
        return {(f.decode() if isinstance(f, bytes) else f): v for f, v in raw.items()}

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
        return list(values)

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
        items: list[Any] | None = None,
    ) -> int:
        """Set hash field(s) asynchronously. Use field/value, mapping, or items (flat key-value pairs)."""
        client = self.get_async_client(key, write=True)
        nvalue = value if field is not None else None
        nmapping = dict(mapping.items()) if mapping else None
        nitems = list(items) if items else None

        return cast("int", await client.hset(key, field, nvalue, mapping=nmapping, items=nitems))

    async def ahsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        """Set a hash field only if it doesn't exist, asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = value

        return bool(await client.hsetnx(key, field, nvalue))

    async def ahget(self, key: KeyT, field: str) -> Any | None:
        """Get a hash field asynchronously."""
        client = self.get_async_client(key, write=False)

        val = await client.hget(key, field)
        return val if val is not None else None

    async def ahmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        """Get multiple hash fields asynchronously."""
        client = self.get_async_client(key, write=False)

        values = await client.hmget(key, fields)
        return [v if v is not None else None for v in values]

    async def ahgetall(self, key: KeyT) -> dict[str, Any]:
        """Get all hash fields asynchronously."""
        client = self.get_async_client(key, write=False)

        raw = await client.hgetall(key)
        return {(f.decode() if isinstance(f, bytes) else f): v for f, v in raw.items()}

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
        return list(values)

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
        nvalues = list(values)

        return cast("int", client.lpush(key, *nvalues))

    def rpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the right of a list."""
        client = self.get_client(key, write=True)
        nvalues = list(values)

        return cast("int", client.rpush(key, *nvalues))

    def lpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the left of a list."""
        client = self.get_client(key, write=True)

        if count is not None:
            vals = client.lpop(key, count)
            return list(vals) if vals else []
        val = client.lpop(key)
        return val if val is not None else None

    def rpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the right of a list."""
        client = self.get_client(key, write=True)

        if count is not None:
            vals = client.rpop(key, count)
            return list(vals) if vals else []
        val = client.rpop(key)
        return val if val is not None else None

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
        encoded_value = value

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
        return val if val is not None else None

    def lrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        """Get a range of elements from a list."""
        client = self.get_client(key, write=False)

        values = client.lrange(key, start, end)
        return list(values)

    def lindex(self, key: KeyT, index: int) -> Any | None:
        """Get an element from a list by index."""
        client = self.get_client(key, write=False)

        val = client.lindex(key, index)
        return val if val is not None else None

    def lset(self, key: KeyT, index: int, value: Any) -> bool:
        """Set an element in a list by index."""
        client = self.get_client(key, write=True)
        nvalue = value

        client.lset(key, index, nvalue)
        return True

    def lrem(self, key: KeyT, count: int, value: Any) -> int:
        """Remove elements from a list."""
        client = self.get_client(key, write=True)
        nvalue = value

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
        npivot = pivot
        nvalue = value

        return cast("int", client.linsert(key, where, npivot, nvalue))

    def blpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list."""
        client = self.get_client(write=True)

        result = client.blpop(keys, timeout=timeout)
        if result is None:
            return None
        key_bytes, value_bytes = result
        key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
        return (key_str, value_bytes)

    def brpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list."""
        client = self.get_client(write=True)

        result = client.brpop(keys, timeout=timeout)
        if result is None:
            return None
        key_bytes, value_bytes = result
        key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
        return (key_str, value_bytes)

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
        return val if val is not None else None

    async def alpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the left of a list asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalues = list(values)

        return cast("int", await client.lpush(key, *nvalues))

    async def arpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the right of a list asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalues = list(values)

        return cast("int", await client.rpush(key, *nvalues))

    async def alpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the left of a list asynchronously."""
        client = self.get_async_client(key, write=True)

        if count is not None:
            vals = await client.lpop(key, count)
            return list(vals) if vals else []
        val = await client.lpop(key)
        return val if val is not None else None

    async def arpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the right of a list asynchronously."""
        client = self.get_async_client(key, write=True)

        if count is not None:
            vals = await client.rpop(key, count)
            return list(vals) if vals else []
        val = await client.rpop(key)
        return val if val is not None else None

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
        encoded_value = value

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
        return val if val is not None else None

    async def alrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        """Get a range of elements from a list asynchronously."""
        client = self.get_async_client(key, write=False)

        values = await client.lrange(key, start, end)
        return list(values)

    async def alindex(self, key: KeyT, index: int) -> Any | None:
        """Get an element from a list by index asynchronously."""
        client = self.get_async_client(key, write=False)

        val = await client.lindex(key, index)
        return val if val is not None else None

    async def alset(self, key: KeyT, index: int, value: Any) -> bool:
        """Set an element in a list by index asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = value

        await client.lset(key, index, nvalue)
        return True

    async def alrem(self, key: KeyT, count: int, value: Any) -> int:
        """Remove elements from a list asynchronously."""
        client = self.get_async_client(key, write=True)
        nvalue = value

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
        npivot = pivot
        nvalue = value

        return cast("int", await client.linsert(key, where, npivot, nvalue))

    async def ablpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list asynchronously."""
        client = self.get_async_client(write=True)

        result = await client.blpop(keys, timeout=timeout)
        if result is None:
            return None
        key_bytes, value_bytes = result
        key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
        return (key_str, value_bytes)

    async def abrpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list asynchronously."""
        client = self.get_async_client(write=True)

        result = await client.brpop(keys, timeout=timeout)
        if result is None:
            return None
        key_bytes, value_bytes = result
        key_str = key_bytes.decode() if isinstance(key_bytes, bytes) else key_bytes
        return (key_str, value_bytes)

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
        return val if val is not None else None

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(self, key: KeyT, *members: Any) -> int:
        """Add members to a set."""
        client = self.get_client(key, write=True)
        nmembers = list(members)

        return cast("int", client.sadd(key, *nmembers))

    def srem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a set."""
        client = self.get_client(key, write=True)
        nmembers = list(members)

        return cast("int", client.srem(key, *nmembers))

    def smembers(self, key: KeyT) -> _set[Any]:
        """Get all members of a set."""
        client = self.get_client(key, write=False)

        result = client.smembers(key)
        return set(result)

    def sismember(self, key: KeyT, member: Any) -> bool:
        """Check if a value is a member of a set."""
        client = self.get_client(key, write=False)
        nmember = member

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
            return val if val is not None else None
        vals = client.spop(key, count)
        return list(vals) if vals else []

    def srandmember(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Get random member(s) from a set."""
        client = self.get_client(key, write=False)

        if count is None:
            val = client.srandmember(key)
            return val if val is not None else None
        vals = client.srandmember(key, count)
        return list(vals) if vals else []

    def smove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        """Move a member from one set to another."""
        client = self.get_client(write=True)
        nmember = member

        return bool(client.smove(src, dst, nmember))

    def sdiff(self, keys: KeyT | Sequence[KeyT]) -> _set[Any]:
        """Return the difference of sets."""
        client = self.get_client(write=False)

        result = client.sdiff(keys)
        return set(result)

    def sdiffstore(self, dest: KeyT, keys: KeyT | Sequence[KeyT]) -> int:
        """Store the difference of sets."""
        client = self.get_client(write=True)

        return cast("int", client.sdiffstore(dest, keys))

    def sinter(self, keys: KeyT | Sequence[KeyT]) -> _set[Any]:
        """Return the intersection of sets."""
        client = self.get_client(write=False)

        result = client.sinter(keys)
        return set(result)

    def sinterstore(self, dest: KeyT, keys: KeyT | Sequence[KeyT]) -> int:
        """Store the intersection of sets."""
        client = self.get_client(write=True)

        return cast("int", client.sinterstore(dest, keys))

    def sunion(self, keys: KeyT | Sequence[KeyT]) -> _set[Any]:
        """Return the union of sets."""
        client = self.get_client(write=False)

        result = client.sunion(keys)
        return set(result)

    def sunionstore(self, dest: KeyT, keys: KeyT | Sequence[KeyT]) -> int:
        """Store the union of sets."""
        client = self.get_client(write=True)

        return cast("int", client.sunionstore(dest, keys))

    def smismember(self, key: KeyT, *members: Any) -> list[bool]:
        """Check if multiple values are members of a set."""
        client = self.get_client(key, write=False)
        nmembers = list(members)

        result = client.smismember(key, nmembers)
        return [bool(v) for v in result]

    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, _set[Any]]:
        """Incrementally iterate over set members."""
        client = self.get_client(key, write=False)

        next_cursor, members = client.sscan(key, cursor=cursor, match=match, count=count)
        return next_cursor, set(members)

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
    ) -> Iterator[Any]:
        """Iterate over set members using SSCAN."""
        client = self.get_client(key, write=False)

        yield from client.sscan_iter(key, match=match, count=count)

    async def asadd(self, key: KeyT, *members: Any) -> int:
        """Add members to a set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = list(members)

        return cast("int", await client.sadd(key, *nmembers))

    async def asrem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = list(members)

        return cast("int", await client.srem(key, *nmembers))

    async def asmembers(self, key: KeyT) -> _set[Any]:
        """Get all members of a set asynchronously."""
        client = self.get_async_client(key, write=False)

        result = await client.smembers(key)
        return set(result)

    async def asismember(self, key: KeyT, member: Any) -> bool:
        """Check if a value is a member of a set asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = member

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
            return val if val is not None else None
        vals = await client.spop(key, count)
        return list(vals) if vals else []

    async def asrandmember(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Get random member(s) from a set asynchronously."""
        client = self.get_async_client(key, write=False)

        if count is None:
            val = await client.srandmember(key)
            return val if val is not None else None
        vals = await client.srandmember(key, count)
        return list(vals) if vals else []

    async def asmove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        """Move a member from one set to another asynchronously."""
        client = self.get_async_client(write=True)
        nmember = member

        return bool(await client.smove(src, dst, nmember))

    async def asdiff(self, keys: KeyT | Sequence[KeyT]) -> _set[Any]:
        """Return the difference of sets asynchronously."""
        client = self.get_async_client(write=False)

        result = await client.sdiff(keys)
        return set(result)

    async def asdiffstore(self, dest: KeyT, keys: KeyT | Sequence[KeyT]) -> int:
        """Store the difference of sets asynchronously."""
        client = self.get_async_client(write=True)

        return cast("int", await client.sdiffstore(dest, keys))

    async def asinter(self, keys: KeyT | Sequence[KeyT]) -> _set[Any]:
        """Return the intersection of sets asynchronously."""
        client = self.get_async_client(write=False)

        result = await client.sinter(keys)
        return set(result)

    async def asinterstore(self, dest: KeyT, keys: KeyT | Sequence[KeyT]) -> int:
        """Store the intersection of sets asynchronously."""
        client = self.get_async_client(write=True)

        return cast("int", await client.sinterstore(dest, keys))

    async def asunion(self, keys: KeyT | Sequence[KeyT]) -> _set[Any]:
        """Return the union of sets asynchronously."""
        client = self.get_async_client(write=False)

        result = await client.sunion(keys)
        return set(result)

    async def asunionstore(self, dest: KeyT, keys: KeyT | Sequence[KeyT]) -> int:
        """Store the union of sets asynchronously."""
        client = self.get_async_client(write=True)

        return cast("int", await client.sunionstore(dest, keys))

    async def asmismember(self, key: KeyT, *members: Any) -> list[bool]:
        """Check if multiple values are members of a set asynchronously."""
        client = self.get_async_client(key, write=False)
        nmembers = list(members)

        result = await client.smismember(key, nmembers)
        return [bool(v) for v in result]

    async def asscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, _set[Any]]:
        """Incrementally iterate over set members asynchronously."""
        client = self.get_async_client(key, write=False)

        next_cursor, members = await client.sscan(key, cursor=cursor, match=match, count=count)
        return next_cursor, set(members)

    async def asscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
    ) -> AsyncIterator[Any]:
        """Iterate over set members using SSCAN asynchronously."""
        client = self.get_async_client(key, write=False)

        async for member in client.sscan_iter(key, match=match, count=count):
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
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
    ) -> int:
        """Add members to a sorted set."""
        client = self.get_client(key, write=True)
        scored_mapping = dict(mapping.items())

        return cast("int", client.zadd(key, scored_mapping, nx=nx, xx=xx, gt=gt, lt=lt, ch=ch))

    def zrem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a sorted set."""
        client = self.get_client(key, write=True)
        nmembers = list(members)

        return cast("int", client.zrem(key, *nmembers))

    def zscore(self, key: KeyT, member: Any) -> float | None:
        """Get the score of a member."""
        client = self.get_client(key, write=False)
        nmember = member

        result = client.zscore(key, nmember)
        return float(result) if result is not None else None

    def zrank(self, key: KeyT, member: Any) -> int | None:
        """Get the rank of a member (0-based)."""
        client = self.get_client(key, write=False)
        nmember = member

        return client.zrank(key, nmember)

    def zrevrank(self, key: KeyT, member: Any) -> int | None:
        """Get the reverse rank of a member."""
        client = self.get_client(key, write=False)
        nmember = member

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
        nmember = member

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
            return [(m, float(s)) for m, s in result]
        return list(result)

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
            return [(m, float(s)) for m, s in result]
        return list(result)

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
            return [(m, float(s)) for m, s in result]
        return list(result)

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
            return [(m, float(s)) for m, s in result]
        return list(result)

    def zremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        """Remove members by rank range."""
        client = self.get_client(key, write=True)

        return cast("int", client.zremrangebyrank(key, start, end))

    def zremrangebyscore(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Remove members by score range."""
        client = self.get_client(key, write=True)

        return cast("int", client.zremrangebyscore(key, min_score, max_score))

    def zpopmin(self, key: KeyT, count: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        client = self.get_client(key, write=True)

        result = client.zpopmin(key, count)
        return [(m, float(s)) for m, s in result]

    def zpopmax(self, key: KeyT, count: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        client = self.get_client(key, write=True)

        result = client.zpopmax(key, count)
        return [(m, float(s)) for m, s in result]

    def zmscore(self, key: KeyT, *members: Any) -> list[float | None]:
        """Get scores for multiple members."""
        client = self.get_client(key, write=False)
        nmembers = list(members)

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
        scored_mapping = dict(mapping.items())

        return cast("int", await client.zadd(key, scored_mapping, nx=nx, xx=xx, gt=gt, lt=lt, ch=ch))

    async def azrem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a sorted set asynchronously."""
        client = self.get_async_client(key, write=True)
        nmembers = list(members)

        return cast("int", await client.zrem(key, *nmembers))

    async def azscore(self, key: KeyT, member: Any) -> float | None:
        """Get the score of a member asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = member

        result = await client.zscore(key, nmember)
        return float(result) if result is not None else None

    async def azrank(self, key: KeyT, member: Any) -> int | None:
        """Get the rank of a member (0-based) asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = member

        return await client.zrank(key, nmember)

    async def azrevrank(self, key: KeyT, member: Any) -> int | None:
        """Get the reverse rank of a member asynchronously."""
        client = self.get_async_client(key, write=False)
        nmember = member

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
        nmember = member

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
            return [(m, float(s)) for m, s in result]
        return list(result)

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
            return [(m, float(s)) for m, s in result]
        return list(result)

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
            return [(m, float(s)) for m, s in result]
        return list(result)

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
            return [(m, float(s)) for m, s in result]
        return list(result)

    async def azremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        """Remove members by rank range asynchronously."""
        client = self.get_async_client(key, write=True)

        return cast("int", await client.zremrangebyrank(key, start, end))

    async def azremrangebyscore(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Remove members by score range asynchronously."""
        client = self.get_async_client(key, write=True)

        return cast("int", await client.zremrangebyscore(key, min_score, max_score))

    async def azpopmin(self, key: KeyT, count: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores asynchronously."""
        client = self.get_async_client(key, write=True)

        result = await client.zpopmin(key, count)
        return [(m, float(s)) for m, s in result]

    async def azpopmax(self, key: KeyT, count: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores asynchronously."""
        client = self.get_async_client(key, write=True)

        result = await client.zpopmax(key, count)
        return [(m, float(s)) for m, s in result]

    async def azmscore(self, key: KeyT, *members: Any) -> list[float | None]:
        """Get scores for multiple members asynchronously."""
        client = self.get_async_client(key, write=False)
        nmembers = list(members)

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
        encoded_fields = dict(fields.items())

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

    def _decode_stream_entries(
        self,
        results: list[tuple[Any, dict[Any, Any]]],
    ) -> list[tuple[str, dict[str, Any]]]:
        """Decode raw stream entries (entry_id + field dict) from Redis."""
        return [
            (
                entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                {k.decode() if isinstance(k, bytes) else k: v for k, v in fields.items()},
            )
            for entry_id, fields in results
        ]

    def _decode_stream_results(
        self,
        results: list[tuple[Any, list[tuple[Any, dict[Any, Any]]]]],
    ) -> dict[str, list[tuple[str, dict[str, Any]]]]:
        """Decode multi-stream results (xread/xreadgroup) from Redis."""
        return {
            (stream_key.decode() if isinstance(stream_key, bytes) else stream_key): self._decode_stream_entries(
                entries,
            )
            for stream_key, entries in results
        }

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
        return self._decode_stream_entries(results)

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
        return self._decode_stream_entries(results)

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

        return self._decode_stream_results(results)

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

        return self._decode_stream_results(results)

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
        return self._decode_stream_entries(results)

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

        if justid:
            # redis-py returns flat list of claimed IDs (strips next_id/deleted)
            claimed: list[str] = [r.decode() if isinstance(r, bytes) else r for r in result]
            return ("", claimed, [])

        next_id = result[0].decode() if isinstance(result[0], bytes) else result[0]
        deleted = [d.decode() if isinstance(d, bytes) else d for d in result[2]] if len(result) > 2 else []
        return (next_id, self._decode_stream_entries(result[1]), deleted)

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
        encoded_fields = dict(fields.items())

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
        return self._decode_stream_entries(results)

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
        return self._decode_stream_entries(results)

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

        return self._decode_stream_results(results)

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

        return self._decode_stream_results(results)

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
        return self._decode_stream_entries(results)

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

        if justid:
            # redis-py returns flat list of claimed IDs (strips next_id/deleted)
            claimed: list[str] = [r.decode() if isinstance(r, bytes) else r for r in result]
            return ("", claimed, [])

        next_id = result[0].decode() if isinstance(result[0], bytes) else result[0]
        deleted = [d.decode() if isinstance(d, bytes) else d for d in result[2]] if len(result) > 2 else []
        return (next_id, self._decode_stream_entries(result[1]), deleted)

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


class ValkeyPySentinelAdapter(ValkeyPyAdapter):
    """Sentinel cache client with automatic primary/replica discovery via Sentinel."""

    # Annotations widen the inferred narrow types so the redis-py thin layer
    # in ``redis_py.py`` can override class slots without tripping mypy's
    # ``incompatible-override`` check.
    _pool_class: builtins.type[Any] | None
    _sentinel_class: builtins.type[Any] | None = None
    _sentinel_pool_class: builtins.type[Any] | None = None
    _async_sentinel_class: builtins.type[Any] | None = None
    _async_sentinel_pool_class: builtins.type[Any] | None = None

    # Sentinel-managed pools live on ``_async_sentinel_pool_class``; clear the
    # generic ``_async_pool_class`` inherited from ``ValkeyPyAdapter`` so callers
    # that probe it (e.g. tests gating on ``_async_pool_class is None``)
    # correctly identify this adapter as sentinel-shaped.
    _async_pool_class: builtins.type[Any] | None = None

    # Async sentinels: WeakKeyDictionary keyed by event loop
    _async_sentinels: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Any]

    # Options that shouldn't be passed to the connection pool
    _SENTINEL_ONLY_OPTIONS = frozenset({"sentinels", "sentinel_kwargs"})

    if _VALKEY_AVAILABLE:
        _pool_class = ValkeySentinelConnectionPool
        _sentinel_class = ValkeySentinel
        _sentinel_pool_class = ValkeySentinelConnectionPool
        _async_sentinel_class = AsyncValkeySentinel
        _async_sentinel_pool_class = AsyncValkeySentinelConnectionPool

    @override
    def __init__(
        self,
        servers: list[str],
        pool_class: str | type | None = None,
        parser_class: str | type | None = None,
        **options: Any,
    ) -> None:
        # Transform the first URL to add is_master query param for primary/replica
        # This creates two URLs: one for primary (is_master=1) and one for replica (is_master=0)
        if servers:
            servers = self._transform_sentinel_urls(servers[0])

        super().__init__(servers, pool_class, parser_class, **options)

        # Initialize async sentinels dict
        self._async_sentinels = weakref.WeakKeyDictionary()

        # Clean up _pool_options to remove sentinel-specific options
        # These should not be passed to the connection pool
        if hasattr(self, "_pool_options"):
            for key in self._SENTINEL_ONLY_OPTIONS:
                self._pool_options.pop(key, None)

        # Create sentinel instance
        sentinels = self._options.get("sentinels")
        if not sentinels:
            raise ImproperlyConfigured("sentinels must be provided as a list of (host, port) tuples")

        sentinel_kwargs = self._options.get("sentinel_kwargs", {})
        # Use _pool_options from parent class (already cleaned above)
        pool_options = dict(self._pool_options) if hasattr(self, "_pool_options") else {}

        if self._sentinel_class is None:
            msg = "Subclasses must set _sentinel_class"
            raise RuntimeError(msg)
        self._sentinel = self._sentinel_class(
            sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **pool_options,
        )

    def _transform_sentinel_urls(self, server: str) -> list[str]:
        """Transform a single URL into primary and replica URLs."""
        url = urlparse(server)
        primary_query = parse_qs(url.query, keep_blank_values=True)
        replica_query = dict(primary_query)
        primary_query["is_master"] = ["1"]
        replica_query["is_master"] = ["0"]

        def replace_query(parsed_url: Any, query: dict[str, list[str]]) -> str:
            return urlunparse((*parsed_url[:4], urlencode(query, doseq=True), parsed_url[5]))

        return [replace_query(url, q) for q in (primary_query, replica_query)]

    def _parse_sentinel_url(self, index: int) -> tuple[str | None, bool, str]:
        """Parse a sentinel URL into (service_name, is_master, clean_url).

        Extracts the service name from the hostname, the is_master flag from
        query params, and returns a clean URL with is_master stripped.
        """
        parsed = urlparse(self._servers[index])
        query_params = parse_qs(parsed.query)

        service_name = parsed.hostname
        is_master = True
        if "is_master" in query_params:
            is_master = query_params["is_master"][0] in ("1", "true", "True")

        new_query = {k: v for k, v in query_params.items() if k != "is_master"}
        clean_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                urlencode(new_query, doseq=True),
                parsed.fragment,
            ),
        )
        return service_name, is_master, clean_url

    @override
    def _get_connection_pool(self, *, write: bool) -> ConnectionPool:
        """Get a sentinel-managed connection pool."""
        index = self._get_connection_pool_index(write=write)

        if index in self._pools:
            return self._pools[index]

        service_name, is_master, clean_url = self._parse_sentinel_url(index)

        # Use _pool_options from parent class (already cleaned in __init__)
        pool_options: dict[str, Any] = dict(self._pool_options) if hasattr(self, "_pool_options") else {}
        pool_options.update(
            service_name=service_name,
            sentinel_manager=self._sentinel,
            is_master=is_master,
        )

        if self._sentinel_pool_class is None:
            msg = "Subclasses must set _sentinel_pool_class"
            raise RuntimeError(msg)
        pool = self._sentinel_pool_class.from_url(clean_url, **pool_options)
        self._pools[index] = pool

        return pool

    def _get_async_sentinel(self) -> Any:
        """Get or create an async sentinel instance for the current event loop."""
        loop = asyncio.get_running_loop()

        if loop in self._async_sentinels:
            return self._async_sentinels[loop]

        if self._async_sentinel_class is None:
            msg = "Subclasses must set _async_sentinel_class"
            raise RuntimeError(msg)

        sentinels = self._options.get("sentinels")
        sentinel_kwargs = self._options.get("sentinel_kwargs", {})
        # Filter out parser_class - it's sync-specific
        pool_options = (
            {k: v for k, v in self._pool_options.items() if k != "parser_class"}
            if hasattr(self, "_pool_options")
            else {}
        )

        async_sentinel = self._async_sentinel_class(
            sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **pool_options,
        )
        self._async_sentinels[loop] = async_sentinel
        return async_sentinel

    @override
    def _get_async_connection_pool(self, *, write: bool) -> Any:
        """Get an async sentinel-managed connection pool, shared process-wide.

        Uses the same ``_async_pools`` registry (driver-specific class
        attribute) as the non-sentinel client so per-task adapter instances
        share a single pool. See
        ``RespAdapterProtocol._get_async_connection_pool`` for the rationale.
        """
        loop = asyncio.get_running_loop()
        index = self._get_connection_pool_index(write=write)

        if self._async_sentinel_pool_class is None:
            msg = "Subclasses must set _async_sentinel_pool_class"
            raise RuntimeError(msg)

        service_name, is_master, clean_url = self._parse_sentinel_url(index)
        async_sentinel = self._get_async_sentinel()

        # Filter out parser_class - it's sync-specific and causes AttributeError on async connections
        pool_options: dict[str, Any] = (
            {k: v for k, v in self._pool_options.items() if k != "parser_class"}
            if hasattr(self, "_pool_options")
            else {}
        )
        # Default cap so concurrent async load can't grow the pool unbounded.
        pool_options.setdefault("max_connections", 50)
        pool_options.update(
            service_name=service_name,
            sentinel_manager=async_sentinel,
            is_master=is_master,
        )

        # Key on the sentinel-aware fields plus the URL + options. The
        # sentinel manager is per-loop (cached above) so we include its id in
        # the key to avoid sharing a pool across two managers in the same loop.
        key = (
            self._async_sentinel_pool_class,
            clean_url,
            service_name,
            is_master,
            id(async_sentinel),
            _options_key({k: v for k, v in pool_options.items() if k != "sentinel_manager"}),
            index,
        )

        sub = self._async_pools.get(loop)
        if sub is None:
            sub = {}
            self._async_pools[loop] = sub

        pool = sub.get(key)
        if pool is None:
            pool = self._async_sentinel_pool_class.from_url(clean_url, **pool_options)
            sub[key] = pool
        return pool


class ValkeyPyClusterAdapter(ValkeyPyAdapter):
    """Cluster cache client base class.

    Extends ``ValkeyAdapter`` with cluster-specific handling for
    server-side sharding and slot-aware operations.
    """

    # Cluster manages its own pool; clear the generic pool slots inherited
    # from ``ValkeyAdapter`` so callers probing ``_async_pool_class``
    # correctly identify this adapter as cluster-shaped.
    _async_pool_class: builtins.type[Any] | None = None

    # Subclasses must set these
    _cluster_class: builtins.type[Any] | None = None
    _async_cluster_class: builtins.type[Any] | None = None
    _key_slot_func: Any = None  # Function to calculate key slot

    if _VALKEY_AVAILABLE:
        _cluster_class = ValkeyCluster
        _async_cluster_class = AsyncValkeyCluster
        _key_slot_func = staticmethod(valkey_key_slot)

    @override
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Per-instance cluster (cluster manages its own connection pool)
        self._cluster_instance: Any | None = None
        # Per-instance async clusters: WeakKeyDictionary keyed by event loop
        # Keyed by event loop because async clusters are bound to the loop they're
        # created on. The same client instance can see multiple loops (e.g., WSGI
        # thread that calls asyncio.run() — ContextVar copies mean the same cache
        # instance is shared with the async context). WeakKeyDictionary ensures
        # automatic cleanup when a loop is GC'd.
        self._async_cluster_instances: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop,
            Any,
        ] = weakref.WeakKeyDictionary()

    @property
    def _cluster(self) -> builtins.type[Any]:
        """Get the cluster class, asserting it's configured."""
        if self._cluster_class is None:
            msg = "Subclasses must set _cluster_class"
            raise RuntimeError(msg)
        return self._cluster_class

    @property
    def _async_cluster(self) -> builtins.type[Any]:
        """Get the async cluster class, asserting it's configured."""
        if self._async_cluster_class is None:
            msg = "Subclasses must set _async_cluster_class"
            raise RuntimeError(msg)
        return self._async_cluster_class

    @override
    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the Cluster client."""
        if self._cluster_instance is not None:
            return self._cluster_instance

        url = self._servers[0]
        parsed_url = urlparse(url)
        # Pass through options
        cluster_options = {
            key_opt: value for key_opt, value in self._options.items() if key_opt not in self._CLIENT_ONLY_OPTIONS
        }

        if parsed_url.hostname:
            cluster_options["host"] = parsed_url.hostname
        if parsed_url.port:
            cluster_options["port"] = parsed_url.port

        self._cluster_instance = self._cluster(**cluster_options)
        return self._cluster_instance

    @override
    def get_async_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the async Cluster client for the current event loop."""
        loop = asyncio.get_running_loop()

        # Check if we already have an async cluster for this loop
        if loop in self._async_cluster_instances:
            return self._async_cluster_instances[loop]

        url = self._servers[0]
        parsed_url = urlparse(url)
        # Pass through options
        cluster_options = {
            key_opt: value for key_opt, value in self._options.items() if key_opt not in self._CLIENT_ONLY_OPTIONS
        }

        if parsed_url.hostname:
            cluster_options["host"] = parsed_url.hostname
        if parsed_url.port:
            cluster_options["port"] = parsed_url.port

        cluster = self._async_cluster(**cluster_options)
        self._async_cluster_instances[loop] = cluster

        return cluster

    def _group_keys_by_slot(self, keys: Iterable[KeyT]) -> dict[int, list[KeyT]]:
        """Group keys by their cluster slot."""
        slots: dict[int, list[KeyT]] = defaultdict(list)
        for key in keys:
            key_bytes = key.encode() if isinstance(key, str) else key
            slot = self._key_slot_func(key_bytes)
            slots[slot].append(key)
        return dict(slots)

    # Override methods that need cluster-specific handling

    @override
    def get_many(self, keys: Iterable[KeyT], *, stampede_prevention: bool | dict | None = None) -> dict[KeyT, Any]:
        """Retrieve many keys, handling cross-slot keys."""
        keys = list(keys)
        if not keys:
            return {}

        client = self.get_client(write=False)
        # mget_nonatomic handles slot splitting
        results = cast(
            "list[bytes | None]",
            client.mget_nonatomic(keys),
        )

        # Collect non-None results
        found = {k: v for k, v in zip(keys, results, strict=False) if v is not None}

        # Stampede filtering: pipeline TTL for all found keys
        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            pipe = client.pipeline()
            found_keys = list(found.keys())
            for k in found_keys:
                pipe.ttl(k)
            ttls = pipe.execute()
            for k, ttl in zip(found_keys, ttls, strict=False):
                if isinstance(ttl, int) and ttl > 0 and should_recompute(ttl, config):
                    del found[k]

        return dict(found.items())

    @override
    def set_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        """Set multiple values, handling cross-slot keys."""
        if not data:
            return []

        client = self.get_client(write=True)

        prepared_data = dict(data.items())
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            # timeout=0 means "delete immediately" (matches base client behavior)
            for slot_keys in self._group_keys_by_slot(prepared_data.keys()).values():
                client.delete(*slot_keys)
        elif actual_timeout is None:
            # No expiry
            client.mset_nonatomic(prepared_data)
        else:
            # Use SET with PX per key in a pipeline so each key is set
            # atomically with its TTL (no window where keys exist without expiry)
            timeout_ms = int(actual_timeout * 1000)
            pipe = client.pipeline()
            for key, value in prepared_data.items():
                pipe.set(key, value, px=timeout_ms)
            pipe.execute()
        return []

    @override
    def delete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys, grouping by slot."""
        if not keys:
            return 0

        client = self.get_client(write=True)

        # Group keys by slot
        slots = self._group_keys_by_slot(keys)

        total_deleted = 0
        for slot_keys in slots.values():
            total_deleted += cast("int", client.delete(*slot_keys))
        return total_deleted

    @override
    def clear(self) -> bool:
        """Flush all primary nodes in the cluster."""
        client = self.get_client(write=True)

        # Use PRIMARIES constant from the cluster class
        client.flushdb(target_nodes=self._cluster.PRIMARIES)
        return True

    @override
    def keys(self, pattern: str) -> list[str]:
        """Execute KEYS command across all primary nodes (pattern is already prefixed)."""
        client = self.get_client(write=False)

        keys_result = cast(
            "list[bytes]",
            client.keys(pattern, target_nodes=self._cluster.PRIMARIES),
        )
        return [k.decode() for k in keys_result]

    @override
    def iter_keys(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> Iterator[str]:
        """Iterate keys matching pattern across all primary nodes (pattern is already prefixed)."""
        client = self.get_client(write=False)

        if itersize is None:
            itersize = self._default_scan_itersize

        for item in client.scan_iter(
            match=pattern,
            count=itersize,
            target_nodes=self._cluster.PRIMARIES,
        ):
            yield item.decode()

    @override
    def delete_pattern(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> int:
        """Remove all keys matching pattern across all primary nodes (pattern is already prefixed)."""
        client = self.get_client(write=True)

        if itersize is None:
            itersize = self._default_scan_itersize

        total_deleted = 0
        for batch in batched(
            client.scan_iter(
                match=pattern,
                count=itersize,
                target_nodes=self._cluster.PRIMARIES,
            ),
            itersize,
            strict=False,
        ):
            for slot_keys in self._group_keys_by_slot(batch).values():
                total_deleted += cast("int", client.delete(*slot_keys))
        return total_deleted

    @override
    def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        """SCAN is not supported in cluster mode (per-node cursors can't be combined). Use iter_keys() instead."""
        raise NotSupportedError("scan", "cluster")

    @override
    def close(self, **kwargs: Any) -> None:
        """No-op. Cluster lives for the instance's lifetime (matches Django's BaseCache)."""

    # =========================================================================
    # Async Override Methods
    # =========================================================================

    @override
    async def aget_many(
        self,
        keys: Iterable[KeyT],
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> dict[KeyT, Any]:
        """Retrieve many keys asynchronously, handling cross-slot keys."""

        keys = list(keys)
        if not keys:
            return {}

        client = self.get_async_client(write=False)

        # mget_nonatomic handles slot splitting
        results = cast(
            "list[bytes | None]",
            await client.mget_nonatomic(keys),
        )

        # Collect non-None results
        found = {k: v for k, v in zip(keys, results, strict=False) if v is not None}

        # Stampede filtering: pipeline TTL for all found keys
        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            pipe = client.pipeline()
            found_keys = list(found.keys())
            for k in found_keys:
                pipe.ttl(k)
            ttls = await pipe.execute()
            for k, ttl in zip(found_keys, ttls, strict=False):
                if isinstance(ttl, int) and ttl > 0 and should_recompute(ttl, config):
                    del found[k]

        return dict(found.items())

    @override
    async def aset_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        """Set multiple values asynchronously, handling cross-slot keys."""
        if not data:
            return []

        client = self.get_async_client(write=True)

        prepared_data = dict(data.items())
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            # timeout=0 means "delete immediately" (matches base client behavior)
            for slot_keys in self._group_keys_by_slot(prepared_data.keys()).values():
                await client.delete(*slot_keys)
        elif actual_timeout is None:
            # No expiry
            await client.mset_nonatomic(prepared_data)
        else:
            # Use SET with PX per key in a pipeline so each key is set
            # atomically with its TTL (no window where keys exist without expiry)
            timeout_ms = int(actual_timeout * 1000)
            pipe = client.pipeline()
            for key, value in prepared_data.items():
                pipe.set(key, value, px=timeout_ms)
            await pipe.execute()
        return []

    @override
    async def adelete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys asynchronously, grouping by slot."""
        if not keys:
            return 0

        client = self.get_async_client(write=True)

        # Group keys by slot
        slots = self._group_keys_by_slot(keys)

        total_deleted = 0
        for slot_keys in slots.values():
            total_deleted += cast("int", await client.delete(*slot_keys))
        return total_deleted

    @override
    async def aclear(self) -> bool:
        """Flush all primary nodes in the cluster asynchronously."""
        client = self.get_async_client(write=True)

        # Use PRIMARIES constant from the cluster class
        await client.flushdb(target_nodes=self._async_cluster.PRIMARIES)
        return True

    @override
    async def akeys(self, pattern: str) -> list[str]:
        """Execute KEYS command asynchronously across all primary nodes."""
        client = self.get_async_client(write=False)

        keys_result = cast(
            "list[bytes]",
            await client.keys(pattern, target_nodes=self._async_cluster.PRIMARIES),
        )
        return [k.decode() for k in keys_result]

    @override
    async def aiter_keys(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> AsyncIterator[str]:
        """Iterate keys matching pattern asynchronously across all primary nodes."""
        client = self.get_async_client(write=False)

        if itersize is None:
            itersize = self._default_scan_itersize

        async for item in client.scan_iter(
            match=pattern,
            count=itersize,
            target_nodes=self._async_cluster.PRIMARIES,
        ):
            yield item.decode()

    @override
    async def adelete_pattern(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> int:
        """Remove all keys matching pattern asynchronously across all primary nodes."""
        client = self.get_async_client(write=True)

        if itersize is None:
            itersize = self._default_scan_itersize

        total_deleted = 0
        batch: list[Any] = []
        async for key in client.scan_iter(
            match=pattern,
            count=itersize,
            target_nodes=self._async_cluster.PRIMARIES,
        ):
            batch.append(key)
            if len(batch) >= itersize:
                for slot_keys in self._group_keys_by_slot(batch).values():
                    total_deleted += cast("int", await client.delete(*slot_keys))
                batch.clear()
        if batch:
            for slot_keys in self._group_keys_by_slot(batch).values():
                total_deleted += cast("int", await client.delete(*slot_keys))
        return total_deleted

    @override
    async def ascan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        """SCAN is not supported in cluster mode (per-node cursors can't be combined). Use aiter_keys() instead."""
        raise NotSupportedError("scan", "cluster")

    @override
    async def aclose(self, **kwargs: Any) -> None:
        """No-op. Cluster lives for the instance's lifetime (matches Django's BaseCache)."""

    @override
    def pipeline(self, *, transaction: bool = True) -> ValkeyPyPipelineAdapter:
        """Construct a cluster pipeline adapter. Transactions are ignored in cluster mode."""
        client = self.get_client(write=True)
        return ValkeyPyPipelineAdapter(client.pipeline(transaction=False))

    @override
    def apipeline(self, *, transaction: bool = True) -> ValkeyPyAsyncPipelineAdapter:
        """Construct an async cluster pipeline adapter. ``ClusterPipeline`` doesn't use MULTI/EXEC."""
        client = self.get_async_client(write=True)
        return ValkeyPyAsyncPipelineAdapter(client.pipeline(transaction=False))


class ValkeyPyPipelineAdapter(RespPipelineProtocol):
    """Pipeline adapter for the redis-py / valkey-py / cluster driver.

    Forwards each cachex pipeline op to ``self._raw`` — a redis-py-shaped
    ``Pipeline`` (or ``ClusterPipeline``) whose method surface mirrors the
    cachex contract one-for-one. The :class:`Pipeline` wrapper passes
    already-prefixed keys and already-encoded values; raw per-command
    results are decoded by the wrapper in ``execute()``.
    """

    def __init__(self, raw_pipeline: Any) -> None:
        self._raw = raw_pipeline

    # -------------------------------------------------------------------------
    # Core lifecycle
    # -------------------------------------------------------------------------

    def execute(self) -> list[Any]:
        """Run all buffered commands and return their raw results."""
        return cast("list[Any]", self._raw.execute())

    def reset(self) -> None:
        """Discard any buffered commands without executing."""
        self._raw.reset()

    def execute_command(self, *args: Any) -> Any:
        """Queue a raw Redis command (``EVAL``, etc.)."""
        return self._raw.execute_command(*args)

    # -------------------------------------------------------------------------
    # Strings / generic key ops
    # -------------------------------------------------------------------------

    def set(
        self,
        key: Any,
        value: Any,
        *,
        ex: int | timedelta | None = None,
        px: int | timedelta | None = None,
        nx: bool = False,
        xx: bool = False,
        exat: int | datetime | None = None,
        pxat: int | datetime | None = None,
        keepttl: bool = False,
        get: bool = False,
    ) -> Any:
        return self._raw.set(
            key,
            value,
            ex=ex,
            px=px,
            nx=nx,
            xx=xx,
            exat=exat,
            pxat=pxat,
            keepttl=keepttl,
            get=get,
        )

    def get(self, key: Any) -> Any:
        return self._raw.get(key)

    def delete(self, *keys: Any) -> Any:
        return self._raw.delete(*keys)

    def exists(self, *keys: Any) -> Any:
        return self._raw.exists(*keys)

    def expire(self, key: Any, seconds: int | timedelta) -> Any:
        return self._raw.expire(key, seconds)

    def expireat(self, key: Any, when: int | datetime) -> Any:
        return self._raw.expireat(key, when)

    def pexpire(self, key: Any, milliseconds: int | timedelta) -> Any:
        return self._raw.pexpire(key, milliseconds)

    def pexpireat(self, key: Any, when: int | datetime) -> Any:
        return self._raw.pexpireat(key, when)

    def persist(self, key: Any) -> Any:
        return self._raw.persist(key)

    def ttl(self, key: Any) -> Any:
        return self._raw.ttl(key)

    def pttl(self, key: Any) -> Any:
        return self._raw.pttl(key)

    def expiretime(self, key: Any) -> Any:
        return self._raw.expiretime(key)

    def type(self, key: Any) -> Any:
        return self._raw.type(key)

    def rename(self, src: Any, dst: Any) -> Any:
        return self._raw.rename(src, dst)

    def renamenx(self, src: Any, dst: Any) -> Any:
        return self._raw.renamenx(src, dst)

    def incrby(self, key: Any, amount: int) -> Any:
        return self._raw.incrby(key, amount)

    def decrby(self, key: Any, amount: int) -> Any:
        return self._raw.decrby(key, amount)

    # -------------------------------------------------------------------------
    # Lists
    # -------------------------------------------------------------------------

    def lpush(self, key: Any, *values: Any) -> Any:
        return self._raw.lpush(key, *values)

    def rpush(self, key: Any, *values: Any) -> Any:
        return self._raw.rpush(key, *values)

    def lpop(self, key: Any, count: int | None = None) -> Any:
        return self._raw.lpop(key, count=count)

    def rpop(self, key: Any, count: int | None = None) -> Any:
        return self._raw.rpop(key, count=count)

    def lrange(self, key: Any, start: int, end: int) -> Any:
        return self._raw.lrange(key, start, end)

    def lindex(self, key: Any, index: int) -> Any:
        return self._raw.lindex(key, index)

    def llen(self, key: Any) -> Any:
        return self._raw.llen(key)

    def lrem(self, key: Any, count: int, value: Any) -> Any:
        return self._raw.lrem(key, count, value)

    def ltrim(self, key: Any, start: int, end: int) -> Any:
        return self._raw.ltrim(key, start, end)

    def lset(self, key: Any, index: int, value: Any) -> Any:
        return self._raw.lset(key, index, value)

    def linsert(self, key: Any, where: str, pivot: Any, value: Any) -> Any:
        return self._raw.linsert(key, where, pivot, value)

    def lpos(
        self,
        key: Any,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> Any:
        return self._raw.lpos(key, value, rank=rank, count=count, maxlen=maxlen)

    def lmove(self, source: Any, destination: Any, src: str = "LEFT", dest: str = "RIGHT") -> Any:
        return self._raw.lmove(source, destination, src, dest)

    # -------------------------------------------------------------------------
    # Sets
    # -------------------------------------------------------------------------

    def sadd(self, key: Any, *members: Any) -> Any:
        return self._raw.sadd(key, *members)

    def srem(self, key: Any, *members: Any) -> Any:
        return self._raw.srem(key, *members)

    def scard(self, key: Any) -> Any:
        return self._raw.scard(key)

    def sismember(self, key: Any, member: Any) -> Any:
        return self._raw.sismember(key, member)

    def smismember(self, key: Any, *members: Any) -> Any:
        return self._raw.smismember(key, *members)

    def smembers(self, key: Any) -> Any:
        return self._raw.smembers(key)

    def smove(self, src: Any, dst: Any, member: Any) -> Any:
        return self._raw.smove(src, dst, member)

    def spop(self, key: Any, count: int | None = None) -> Any:
        return self._raw.spop(key, count)

    def srandmember(self, key: Any, count: int | None = None) -> Any:
        return self._raw.srandmember(key, count)

    def sdiff(self, *keys: Any) -> Any:
        return self._raw.sdiff(*keys)

    def sinter(self, *keys: Any) -> Any:
        return self._raw.sinter(*keys)

    def sunion(self, *keys: Any) -> Any:
        return self._raw.sunion(*keys)

    def sdiffstore(self, dst: Any, *keys: Any) -> Any:
        return self._raw.sdiffstore(dst, *keys)

    def sinterstore(self, dst: Any, *keys: Any) -> Any:
        return self._raw.sinterstore(dst, *keys)

    def sunionstore(self, dst: Any, *keys: Any) -> Any:
        return self._raw.sunionstore(dst, *keys)

    # -------------------------------------------------------------------------
    # Hashes
    # -------------------------------------------------------------------------

    def hset(
        self,
        key: Any,
        field: Any = None,
        value: Any = None,
        mapping: Mapping[Any, Any] | None = None,
        items: list[Any] | None = None,
    ) -> Any:
        return self._raw.hset(key, field, value, mapping=mapping, items=items)

    def hsetnx(self, key: Any, field: Any, value: Any) -> Any:
        return self._raw.hsetnx(key, field, value)

    def hdel(self, key: Any, *fields: Any) -> Any:
        return self._raw.hdel(key, *fields)

    def hget(self, key: Any, field: Any) -> Any:
        return self._raw.hget(key, field)

    def hgetall(self, key: Any) -> Any:
        return self._raw.hgetall(key)

    def hmget(self, key: Any, fields: Sequence[Any]) -> Any:
        return self._raw.hmget(key, fields)

    def hlen(self, key: Any) -> Any:
        return self._raw.hlen(key)

    def hkeys(self, key: Any) -> Any:
        return self._raw.hkeys(key)

    def hvals(self, key: Any) -> Any:
        return self._raw.hvals(key)

    def hexists(self, key: Any, field: Any) -> Any:
        return self._raw.hexists(key, field)

    def hincrby(self, key: Any, field: Any, amount: int = 1) -> Any:
        return self._raw.hincrby(key, field, amount)

    def hincrbyfloat(self, key: Any, field: Any, amount: float = 1.0) -> Any:
        return self._raw.hincrbyfloat(key, field, amount)

    # -------------------------------------------------------------------------
    # Sorted sets
    # -------------------------------------------------------------------------

    def zadd(
        self,
        key: Any,
        mapping: Mapping[Any, float],
        *,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Any:
        return self._raw.zadd(key, mapping, nx=nx, xx=xx, ch=ch, incr=incr, gt=gt, lt=lt)

    def zcard(self, key: Any) -> Any:
        return self._raw.zcard(key)

    def zscore(self, key: Any, member: Any) -> Any:
        return self._raw.zscore(key, member)

    def zmscore(self, key: Any, members: Sequence[Any]) -> Any:
        return self._raw.zmscore(key, members)

    def zrank(self, key: Any, member: Any) -> Any:
        return self._raw.zrank(key, member)

    def zrevrank(self, key: Any, member: Any) -> Any:
        return self._raw.zrevrank(key, member)

    def zincrby(self, key: Any, amount: float, member: Any) -> Any:
        return self._raw.zincrby(key, amount, member)

    def zcount(self, key: Any, min: Any, max: Any) -> Any:
        return self._raw.zcount(key, min, max)

    def zrem(self, key: Any, *members: Any) -> Any:
        return self._raw.zrem(key, *members)

    def zremrangebyrank(self, key: Any, start: int, end: int) -> Any:
        return self._raw.zremrangebyrank(key, start, end)

    def zremrangebyscore(self, key: Any, min: Any, max: Any) -> Any:
        return self._raw.zremrangebyscore(key, min, max)

    def zpopmin(self, key: Any, count: int | None = None) -> Any:
        return self._raw.zpopmin(key, count)

    def zpopmax(self, key: Any, count: int | None = None) -> Any:
        return self._raw.zpopmax(key, count)

    def zrange(
        self,
        key: Any,
        start: int,
        end: int,
        *,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: Callable[[Any], Any] = float,
    ) -> Any:
        return self._raw.zrange(
            key,
            start,
            end,
            desc=desc,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )

    def zrevrange(
        self,
        key: Any,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        score_cast_func: Callable[[Any], Any] = float,
    ) -> Any:
        return self._raw.zrevrange(
            key,
            start,
            end,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )

    def zrangebyscore(
        self,
        key: Any,
        min: Any,
        max: Any,
        start: int | None = None,
        num: int | None = None,
        *,
        withscores: bool = False,
        score_cast_func: Callable[[Any], Any] = float,
    ) -> Any:
        return self._raw.zrangebyscore(
            key,
            min,
            max,
            start=start,
            num=num,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )

    def zrevrangebyscore(
        self,
        key: Any,
        max: Any,
        min: Any,
        start: int | None = None,
        num: int | None = None,
        *,
        withscores: bool = False,
        score_cast_func: Callable[[Any], Any] = float,
    ) -> Any:
        return self._raw.zrevrangebyscore(
            key,
            max,
            min,
            start=start,
            num=num,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )

    # -------------------------------------------------------------------------
    # Streams
    # -------------------------------------------------------------------------

    def xadd(
        self,
        key: Any,
        fields: Mapping[Any, Any],
        *,
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return self._raw.xadd(
            key,
            fields,
            id=id,
            maxlen=maxlen,
            approximate=approximate,
            nomkstream=nomkstream,
            minid=minid,
            limit=limit,
        )

    def xlen(self, key: Any) -> Any:
        return self._raw.xlen(key)

    def xrange(self, key: Any, min: str = "-", max: str = "+", count: int | None = None) -> Any:
        return self._raw.xrange(key, min=min, max=max, count=count)

    def xrevrange(self, key: Any, max: str = "+", min: str = "-", count: int | None = None) -> Any:
        return self._raw.xrevrange(key, max=max, min=min, count=count)

    def xread(
        self,
        streams: Mapping[Any, Any],
        count: int | None = None,
        block: int | None = None,
    ) -> Any:
        return self._raw.xread(streams, count=count, block=block)

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[Any, Any],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> Any:
        return self._raw.xreadgroup(group, consumer, streams, count=count, block=block, noack=noack)

    def xtrim(
        self,
        key: Any,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return self._raw.xtrim(key, maxlen=maxlen, approximate=approximate, minid=minid, limit=limit)

    def xdel(self, key: Any, *entry_ids: str) -> Any:
        return self._raw.xdel(key, *entry_ids)

    def xinfo_stream(self, key: Any, full: bool = False) -> Any:
        return self._raw.xinfo_stream(key, full=full)

    def xinfo_groups(self, key: Any) -> Any:
        return self._raw.xinfo_groups(key)

    def xinfo_consumers(self, key: Any, group: str) -> Any:
        return self._raw.xinfo_consumers(key, group)

    def xgroup_create(
        self,
        key: Any,
        group: str,
        id: str = "$",
        *,
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> Any:
        return self._raw.xgroup_create(key, group, id, mkstream=mkstream, entries_read=entries_read)

    def xgroup_destroy(self, key: Any, group: str) -> Any:
        return self._raw.xgroup_destroy(key, group)

    def xgroup_setid(
        self,
        key: Any,
        group: str,
        id: str,
        *,
        entries_read: int | None = None,
    ) -> Any:
        return self._raw.xgroup_setid(key, group, id, entries_read=entries_read)

    def xgroup_delconsumer(self, key: Any, group: str, consumer: str) -> Any:
        return self._raw.xgroup_delconsumer(key, group, consumer)

    def xack(self, key: Any, group: str, *entry_ids: str) -> Any:
        return self._raw.xack(key, group, *entry_ids)

    def xpending(self, key: Any, group: str) -> Any:
        return self._raw.xpending(key, group)

    def xpending_range(
        self,
        key: Any,
        group: str,
        *,
        min: str,
        max: str,
        count: int,
        consumername: str | None = None,
        idle: int | None = None,
    ) -> Any:
        kwargs: dict[str, Any] = {}
        if consumername is not None:
            kwargs["consumername"] = consumername
        if idle is not None:
            kwargs["idle"] = idle
        return self._raw.xpending_range(key, group, min=min, max=max, count=count, **kwargs)

    def xclaim(
        self,
        key: Any,
        group: str,
        consumer: str,
        min_idle_time: int,
        message_ids: list[str],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
    ) -> Any:
        return self._raw.xclaim(
            key,
            group,
            consumer,
            min_idle_time,
            message_ids,
            idle=idle,
            time=time,
            retrycount=retrycount,
            force=force,
            justid=justid,
        )

    def xautoclaim(
        self,
        key: Any,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> Any:
        return self._raw.xautoclaim(
            key,
            group,
            consumer,
            min_idle_time,
            start_id=start_id,
            count=count,
            justid=justid,
        )


class ValkeyPyAsyncPipelineAdapter(ValkeyPyPipelineAdapter, RespAsyncPipelineProtocol):
    """Async sibling of :class:`ValkeyPyPipelineAdapter` — wraps an async ``Pipeline``.

    The chainable surface inherited from :class:`ValkeyPyPipelineAdapter` is
    purely sync (queueing only); the underlying ``redis.asyncio`` /
    ``valkey.asyncio`` ``Pipeline`` accepts the same chainable calls and only
    awaits at execution time. We override ``execute()`` and ``reset()`` to
    await the underlying pipeline, since redis-py's async ``Pipeline`` declares
    both as coroutines.
    """

    @override
    async def execute(self) -> list[Any]:  # type: ignore[override]
        """Run all buffered commands asynchronously and return their raw results."""
        return cast("list[Any]", await self._raw.execute())

    @override
    async def reset(self) -> None:  # type: ignore[override]
        """Discard buffered commands. ``redis.asyncio.Pipeline.reset()`` is awaitable."""
        await self._raw.reset()


__all__ = [
    "_VALKEY_AVAILABLE",
    "AsyncPoolsRegistry",
    "ValkeyPyAdapter",
    "ValkeyPyAsyncPipelineAdapter",
    "ValkeyPyClusterAdapter",
    "ValkeyPyPipelineAdapter",
    "ValkeyPySentinelAdapter",
]
