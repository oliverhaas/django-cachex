"""Sentinel cache backend and client for Redis-compatible backends.

This module provides cache backends that use Redis Sentinel for automatic
primary/replica discovery and failover.

Architecture (matching Django's RedisCache structure):
- KeyValueSentinelCacheClient(KeyValueCacheClient): Base class with sentinel pooling
- RedisSentinelCacheClient: Sets class attributes for redis-py
- KeyValueSentinelCache(KeyValueCache): Base cache backend
- RedisSentinelCache: Sets _class = RedisSentinelCacheClient
"""

from __future__ import annotations

import asyncio
import weakref
from typing import TYPE_CHECKING, Any, override
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from django.core.exceptions import ImproperlyConfigured

from django_cachex.client.cache import KeyValueCache, ValkeyCache
from django_cachex.client.default import KeyValueCacheClient, ValkeyCacheClient

if TYPE_CHECKING:
    from redis.connection import ConnectionPool

# Try to import redis-py
_REDIS_AVAILABLE = False
try:
    import redis
    from redis.asyncio import Redis as AsyncRedisClient
    from redis.asyncio.sentinel import Sentinel as AsyncRedisSentinel
    from redis.asyncio.sentinel import SentinelConnectionPool as AsyncRedisSentinelConnectionPool
    from redis.sentinel import Sentinel as RedisSentinel
    from redis.sentinel import SentinelConnectionPool as RedisSentinelConnectionPool

    _REDIS_AVAILABLE = True
except ImportError:
    redis = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]

# Try to import valkey-py
_VALKEY_AVAILABLE = False
try:
    import valkey
    from valkey.sentinel import Sentinel as ValkeySentinel  # noqa: F401
    from valkey.sentinel import SentinelConnectionPool as ValkeySentinelConnectionPool  # noqa: F401

    _VALKEY_AVAILABLE = True
except ImportError:
    valkey = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]


# =============================================================================
# CacheClient Classes (actual Redis operations)
# =============================================================================


class KeyValueSentinelCacheClient(KeyValueCacheClient):
    """Sentinel cache client base class.

    Extends KeyValueCacheClient with Sentinel-specific connection pooling.
    Automatically discovers primary and replica nodes via Sentinel.

    Subclasses must set:
    - _lib: The library module (redis or valkey)
    - _client_class: The client class (e.g., redis.Redis)
    - _pool_class: The connection pool class (typically SentinelConnectionPool)
    - _sentinel_class: The Sentinel class
    - _sentinel_pool_class: The SentinelConnectionPool class
    - _async_sentinel_class: The async Sentinel class (optional, for async support)
    - _async_sentinel_pool_class: The async SentinelConnectionPool class (optional)
    """

    # Subclasses must set these to the appropriate sentinel classes
    _sentinel_class: type[Any] | None = None
    _sentinel_pool_class: type[Any] | None = None
    _async_sentinel_class: type[Any] | None = None
    _async_sentinel_pool_class: type[Any] | None = None

    # Async sentinels: WeakKeyDictionary keyed by event loop
    _async_sentinels: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Any]

    # Options that shouldn't be passed to the connection pool
    _SENTINEL_ONLY_OPTIONS = frozenset({"sentinels", "sentinel_kwargs"})

    @override
    def __init__(
        self,
        servers: list[str],
        serializer: str | list | type | None = None,
        pool_class: str | type | None = None,
        parser_class: str | type | None = None,
        **options: Any,
    ) -> None:
        # Transform the first URL to add is_master query param for primary/replica
        # This creates two URLs: one for primary (is_master=1) and one for replica (is_master=0)
        if servers:
            servers = self._transform_sentinel_urls(servers[0])

        super().__init__(servers, serializer, pool_class, parser_class, **options)

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

        assert self._sentinel_class is not None, "Subclasses must set _sentinel_class"  # noqa: S101
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

    @override
    def _get_connection_pool(self, *, write: bool) -> ConnectionPool:
        """Get a sentinel-managed connection pool."""
        index = self._get_connection_pool_index(write=write)
        url = self._servers[index]
        parsed = urlparse(url)

        if index in self._pools:
            return self._pools[index]

        # Parse service name and is_master from URL
        service_name = parsed.hostname
        query_params = parse_qs(parsed.query)
        is_master = True
        if "is_master" in query_params:
            is_master = query_params["is_master"][0] in ("1", "true", "True")

        # Use _pool_options from parent class (already cleaned in __init__)
        pool_options: dict[str, Any] = dict(self._pool_options) if hasattr(self, "_pool_options") else {}
        pool_options.update(
            service_name=service_name,
            sentinel_manager=self._sentinel,
            is_master=is_master,
        )

        # Create pool (strip is_master from URL for from_url)
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

        assert self._sentinel_pool_class is not None, "Subclasses must set _sentinel_pool_class"  # noqa: S101
        pool = self._sentinel_pool_class.from_url(clean_url, **pool_options)
        self._pools[index] = pool

        return pool

    def _get_async_sentinel(self) -> Any:
        """Get or create an async sentinel instance for the current event loop."""
        loop = asyncio.get_running_loop()

        if loop in self._async_sentinels:
            return self._async_sentinels[loop]

        assert self._async_sentinel_class is not None, "Subclasses must set _async_sentinel_class"  # noqa: S101

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
        """Get an async sentinel-managed connection pool."""
        loop = asyncio.get_running_loop()
        index = self._get_connection_pool_index(write=write)
        url = self._servers[index]
        parsed = urlparse(url)

        # Check if we already have an async pool for this loop
        if loop in self._async_pools and index in self._async_pools[loop]:
            return self._async_pools[loop][index]

        # Parse service name and is_master from URL
        service_name = parsed.hostname
        query_params = parse_qs(parsed.query)
        is_master = True
        if "is_master" in query_params:
            is_master = query_params["is_master"][0] in ("1", "true", "True")

        # Get the async sentinel for this event loop
        async_sentinel = self._get_async_sentinel()

        # Use _pool_options from parent class (already cleaned in __init__)
        # Filter out parser_class - it's sync-specific and causes AttributeError on async connections
        pool_options: dict[str, Any] = (
            {k: v for k, v in self._pool_options.items() if k != "parser_class"}
            if hasattr(self, "_pool_options")
            else {}
        )
        pool_options.update(
            service_name=service_name,
            sentinel_manager=async_sentinel,
            is_master=is_master,
        )

        # Create pool (strip is_master from URL for from_url)
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

        assert self._async_sentinel_pool_class is not None, "Subclasses must set _async_sentinel_pool_class"  # noqa: S101
        pool = self._async_sentinel_pool_class.from_url(clean_url, **pool_options)

        # Cache the pool for this event loop
        if loop not in self._async_pools:
            self._async_pools[loop] = {}
        self._async_pools[loop][index] = pool

        return pool


# =============================================================================
# Cache Classes (extend BaseCache, delegate to CacheClient)
# =============================================================================


class KeyValueSentinelCache(KeyValueCache):
    """Sentinel cache backend base class.

    Extends KeyValueCache for sentinel-specific behavior.
    Subclasses set `_class` class attribute to their specific SentinelCacheClient.
    """

    _class: type[KeyValueSentinelCacheClient] = KeyValueSentinelCacheClient


# =============================================================================
# Concrete Implementations
# =============================================================================

if _REDIS_AVAILABLE:

    class RedisSentinelCacheClient(KeyValueSentinelCacheClient):
        """Redis Sentinel cache client.

        Extends KeyValueSentinelCacheClient with Redis-specific classes.
        Automatically discovers primary and replica nodes via Redis Sentinel.
        """

        _lib = redis
        _client_class = redis.Redis
        _pool_class = RedisSentinelConnectionPool
        _sentinel_class = RedisSentinel
        _sentinel_pool_class = RedisSentinelConnectionPool
        _async_client_class = AsyncRedisClient
        _async_sentinel_class = AsyncRedisSentinel
        _async_sentinel_pool_class = AsyncRedisSentinelConnectionPool

    class RedisSentinelCache(KeyValueSentinelCache):
        """Django cache backend for Redis Sentinel high availability.

        Redis Sentinel provides automatic failover and service discovery for Redis.
        When the primary server fails, Sentinel promotes a replica to primary and
        updates clients automatically.

        Requirements:
            Requires redis-py to be installed::

                pip install redis

        How it Works:
            - Sentinel monitors Redis primary and replica nodes
            - On primary failure, Sentinel elects a new primary
            - The cache backend automatically discovers the current primary
            - Reads can be directed to replicas for better performance

        Example:
            Basic configuration with Sentinel service name::

                CACHES = {
                    "default": {
                        "BACKEND": "django_cachex.client.RedisSentinelCache",
                        "LOCATION": "redis://mymaster/0",  # Service name, not hostname
                        "OPTIONS": {
                            "sentinels": [
                                ("sentinel1.example.com", 26379),
                                ("sentinel2.example.com", 26379),
                                ("sentinel3.example.com", 26379),
                            ],
                        }
                    }
                }

            With authentication and additional options::

                CACHES = {
                    "default": {
                        "BACKEND": "django_cachex.client.RedisSentinelCache",
                        "LOCATION": "redis://mymaster/0",
                        "OPTIONS": {
                            "sentinels": [
                                ("sentinel1.example.com", 26379),
                                ("sentinel2.example.com", 26379),
                            ],
                            "sentinel_kwargs": {
                                "password": "sentinel-password",
                            },
                            "password": "redis-password",
                        }
                    }
                }

        Note:
            The ``LOCATION`` should use the Sentinel service name (e.g., "mymaster")
            as the hostname, not the actual Redis server hostname. Sentinel will
            resolve the service name to the current primary's address.

        See Also:
            - ``RedisCache``: For standalone Redis servers
            - ``RedisClusterCache``: For Redis Cluster horizontal scaling
        """

        _class = RedisSentinelCacheClient

else:

    class RedisSentinelCacheClient(KeyValueCacheClient):  # type: ignore[no-redef]
        """Redis Sentinel cache client (requires redis-py to be installed)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "RedisSentinelCacheClient requires redis-py to be installed. Install it with: pip install redis",
            )

    class RedisSentinelCache(KeyValueCache):  # type: ignore[no-redef]
        """Redis Sentinel cache backend (requires redis-py to be installed)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "RedisSentinelCache requires redis-py to be installed. Install it with: pip install redis",
            )


# NOTE: ValkeySentinelCacheClient is not currently provided due to a bug in valkey-py.
# The valkey-py library's SentinelManagedConnection is missing the `_get_from_local_cache`
# method which causes AttributeError when using Sentinel connections.
# See: https://github.com/valkey-io/valkey-py/issues
# Once the upstream bug is fixed, we can re-enable this.


class ValkeySentinelCacheClient(ValkeyCacheClient):  # ty: ignore[unsupported-base]
    """Valkey Sentinel cache client (currently unavailable due to valkey-py bug)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(
            "ValkeySentinelCacheClient is currently unavailable due to a bug in valkey-py. "
            "The SentinelManagedConnection class is missing the '_get_from_local_cache' method. "
            "Use RedisSentinelCacheClient with a Valkey server instead (protocol compatible), "
            "or wait for an upstream fix in valkey-py.",
        )


class ValkeySentinelCache(ValkeyCache):
    """Valkey Sentinel cache backend (currently unavailable due to valkey-py bug)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(
            "ValkeySentinelCache is currently unavailable due to a bug in valkey-py. "
            "The SentinelManagedConnection class is missing the '_get_from_local_cache' method. "
            "Use RedisSentinelCache with a Valkey server instead (protocol compatible), "
            "or wait for an upstream fix in valkey-py.",
        )


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "KeyValueSentinelCache",
    "KeyValueSentinelCacheClient",
    "RedisSentinelCache",
    "RedisSentinelCacheClient",
    "ValkeySentinelCache",
    "ValkeySentinelCacheClient",
]
