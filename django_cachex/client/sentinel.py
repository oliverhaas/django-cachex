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

from typing import TYPE_CHECKING, Any
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
    from redis.sentinel import Sentinel as RedisSentinel
    from redis.sentinel import SentinelConnectionPool as RedisSentinelConnectionPool

    _REDIS_AVAILABLE = True
except ImportError:
    redis = None  # type: ignore[assignment]

# Try to import valkey-py
_VALKEY_AVAILABLE = False
try:
    import valkey
    from valkey.sentinel import Sentinel as ValkeySentinel
    from valkey.sentinel import SentinelConnectionPool as ValkeySentinelConnectionPool

    _VALKEY_AVAILABLE = True
except ImportError:
    valkey = None  # type: ignore[assignment]


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
    """

    # Subclasses must set these to the appropriate sentinel classes
    _sentinel_class: type[Any] = None  # type: ignore[assignment]
    _sentinel_pool_class: type[Any] = None  # type: ignore[assignment]

    # Options that shouldn't be passed to the connection pool
    _SENTINEL_ONLY_OPTIONS = frozenset({"sentinels", "sentinel_kwargs"})

    def __init__(
        self,
        servers,
        serializer=None,
        pool_class=None,
        parser_class=None,
        **options,
    ) -> None:
        # Transform the first URL to add is_master query param for primary/replica
        # This creates two URLs: one for primary (is_master=1) and one for replica (is_master=0)
        if servers:
            servers = self._transform_sentinel_urls(servers[0])

        super().__init__(servers, serializer, pool_class, parser_class, **options)

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

        def replace_query(parsed_url, query):
            return urlunparse((*parsed_url[:4], urlencode(query, doseq=True), parsed_url[5]))

        return [replace_query(url, q) for q in (primary_query, replica_query)]

    def _get_connection_pool(self, write: bool) -> ConnectionPool:
        """Get a sentinel-managed connection pool."""
        index = self._get_connection_pool_index(write)
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
        pool_options = dict(self._pool_options) if hasattr(self, "_pool_options") else {}
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

        pool = self._sentinel_pool_class.from_url(clean_url, **pool_options)
        self._pools[index] = pool

        return pool


# =============================================================================
# Cache Classes (extend BaseCache, delegate to CacheClient)
# =============================================================================


class KeyValueSentinelCache(KeyValueCache):
    """Sentinel cache backend base class.

    Extends KeyValueCache for sentinel-specific behavior.
    Subclasses set `_class` class attribute to their specific SentinelCacheClient.
    """

    _class: type[KeyValueSentinelCacheClient] = KeyValueSentinelCacheClient  # type: ignore[assignment]


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

    class RedisSentinelCache(KeyValueSentinelCache):
        """Redis Sentinel cache backend.

        Extends KeyValueSentinelCache for Redis Sentinel support.
        Use as: BACKEND = "django_cachex.client.RedisSentinelCache"
        """

        _class = RedisSentinelCacheClient

else:

    class RedisSentinelCacheClient(KeyValueCacheClient):  # type: ignore[no-redef]
        """Redis Sentinel cache client (requires redis-py to be installed)."""

        def __init__(self, *args, **kwargs):
            raise ImportError(
                "RedisSentinelCacheClient requires redis-py to be installed. Install it with: pip install redis",
            )

    class RedisSentinelCache(KeyValueCache):  # type: ignore[no-redef]
        """Redis Sentinel cache backend (requires redis-py to be installed)."""

        def __init__(self, *args, **kwargs):
            raise ImportError(
                "RedisSentinelCache requires redis-py to be installed. Install it with: pip install redis",
            )


# NOTE: ValkeySentinelCacheClient is not currently provided due to a bug in valkey-py.
# The valkey-py library's SentinelManagedConnection is missing the `_get_from_local_cache`
# method which causes AttributeError when using Sentinel connections.
# See: https://github.com/valkey-io/valkey-py/issues
# Once the upstream bug is fixed, we can re-enable this.


class ValkeySentinelCacheClient(ValkeyCacheClient):
    """Valkey Sentinel cache client (currently unavailable due to valkey-py bug)."""

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "ValkeySentinelCacheClient is currently unavailable due to a bug in valkey-py. "
            "The SentinelManagedConnection class is missing the '_get_from_local_cache' method. "
            "Use RedisSentinelCacheClient with a Valkey server instead (protocol compatible), "
            "or wait for an upstream fix in valkey-py.",
        )


class ValkeySentinelCache(ValkeyCache):
    """Valkey Sentinel cache backend (currently unavailable due to valkey-py bug)."""

    def __init__(self, *args, **kwargs):
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
    "KeyValueSentinelCacheClient",
    "KeyValueSentinelCache",
    "RedisSentinelCacheClient",
    "RedisSentinelCache",
    "ValkeySentinelCacheClient",
    "ValkeySentinelCache",
]
