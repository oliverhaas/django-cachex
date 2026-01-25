"""Connection pool factories for Redis backends."""

from typing import TYPE_CHECKING, ClassVar
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from django.conf import settings
from django.utils.module_loading import import_string

if TYPE_CHECKING:
    from redis import Redis
    from redis.cluster import RedisCluster
    from redis.connection import ConnectionPool

# Try to import redis-py
_REDIS_AVAILABLE = False
try:
    from redis import Redis as RedisClient
    from redis.cluster import RedisCluster as RedisClusterClient
    from redis.connection import ConnectionPool as RedisConnectionPool
    from redis.connection import DefaultParser as RedisDefaultParser
    from redis.connection import to_bool as redis_to_bool
    from redis.sentinel import Sentinel as RedisSentinel

    _REDIS_AVAILABLE = True
except ImportError:
    pass

# Try to import valkey-py
_VALKEY_AVAILABLE = False
try:
    from valkey import Valkey as ValkeyClient
    from valkey.cluster import ValkeyCluster as ValkeyClusterClient
    from valkey.connection import ConnectionPool as ValkeyConnectionPool
    from valkey.connection import DefaultParser as ValkeyDefaultParser
    from valkey.connection import to_bool as valkey_to_bool
    from valkey.sentinel import Sentinel as ValkeySentinel

    _VALKEY_AVAILABLE = True

    # If redis-py not available, use valkey classes as fallback
    if not _REDIS_AVAILABLE:
        RedisClient = ValkeyClient  # type: ignore[misc]
        RedisClusterClient = ValkeyClusterClient  # type: ignore[misc]
        RedisConnectionPool = ValkeyConnectionPool  # type: ignore[misc]
        RedisDefaultParser = ValkeyDefaultParser
        redis_to_bool = valkey_to_bool
        RedisSentinel = ValkeySentinel  # type: ignore[misc]
except ImportError:
    pass

# Known options that we handle explicitly (not passed to pool)
_KNOWN_OPTIONS = frozenset(
    {
        "serializer",
        "compressor",
        "client_class",
        "connection_factory",
        "sentinels",
        "sentinel_kwargs",
        "ignore_exceptions",
        "close_connection",
        "reverse_key_function",
    },
)


class ConnectionFactory:
    """Connection factory for standard Redis.

    Creates and caches connection pools. Pools are cached process-globally
    because Django creates new cache client instances for every request.
    """

    _pools: ClassVar[dict[str, "ConnectionPool"]] = {}

    def __init__(self, options: dict):
        self.options = options

        # Pool class - accept class or string path
        pool_class = options.get("pool_class", RedisConnectionPool)
        if isinstance(pool_class, str):
            pool_class = import_string(pool_class)
        self.pool_class = pool_class

        # Parser class - accept class or string path
        parser_class = options.get("parser_class", RedisDefaultParser)
        if isinstance(parser_class, str):
            parser_class = import_string(parser_class)
        self.parser_class = parser_class

    def _get_pool_options(self) -> dict:
        """Get options to pass directly to ConnectionPool.from_url().

        Unknown options are passed through to the pool, matching Django's behavior.
        """
        pool_options = {"parser_class": self.parser_class}

        # Pass through any option that's not in our known set
        for key, value in self.options.items():
            if key not in _KNOWN_OPTIONS and key not in ("pool_class", "parser_class"):
                pool_options[key] = value

        return pool_options

    def connect(self, url: str) -> "Redis":
        """Create a new Redis connection for the given URL."""
        # Handle db option by appending to URL if not already specified
        db = self.options.get("db")
        if db is not None:
            parsed = urlparse(url)
            if not parsed.path or parsed.path == "/":
                url = f"{url.rstrip('/')}/{db}"

        pool = self._get_or_create_pool(url)
        return RedisClient(connection_pool=pool)

    def disconnect(self, connection: "Redis") -> None:
        """Disconnect a Redis connection."""
        connection.connection_pool.disconnect()

    def _get_or_create_pool(self, url: str) -> "ConnectionPool":
        """Get a cached pool or create a new one."""
        if url not in self._pools:
            self._pools[url] = self._create_pool(url)
        return self._pools[url]

    def _create_pool(self, url: str) -> "ConnectionPool":
        """Create a new connection pool."""
        pool_options = self._get_pool_options()
        return self.pool_class.from_url(url, **pool_options)


class SentinelConnectionFactory(ConnectionFactory):
    """Connection factory for Redis Sentinel."""

    def __init__(self, options: dict):
        # Default to SentinelConnectionPool
        options = dict(options)
        options.setdefault("pool_class", "redis.sentinel.SentinelConnectionPool")
        super().__init__(options)

        sentinels = options.get("sentinels")
        if not sentinels:
            msg = "sentinels must be provided as a list of (host, port) tuples"
            raise ValueError(msg)

        # Create sentinel instance with connection options
        sentinel_kwargs = options.get("sentinel_kwargs", {})
        pool_options = self._get_pool_options()

        self._sentinel = RedisSentinel(
            sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **pool_options,
        )

    def _create_pool(self, url: str) -> "ConnectionPool":
        """Create a new sentinel connection pool."""
        parsed = urlparse(url)
        service_name = parsed.hostname

        # Parse is_master from query string
        query_params = parse_qs(parsed.query)
        is_master = True
        if "is_master" in query_params:
            is_master = redis_to_bool(query_params["is_master"][0])
            del query_params["is_master"]

        # Rebuild URL without is_master
        new_query = urlencode(query_params, doseq=True)
        new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

        pool_options = self._get_pool_options()
        pool_options.update(
            service_name=service_name,
            sentinel_manager=self._sentinel,
            is_master=is_master,
        )

        return self.pool_class.from_url(new_url, **pool_options)


class ClusterConnectionFactory:
    """Connection factory for Redis Cluster.

    Redis Cluster manages its own connection pool internally.
    """

    _clusters: ClassVar[dict[str, "RedisCluster"]] = {}

    def __init__(self, options: dict):
        self.options = options

    def _get_cluster_options(self) -> dict:
        """Get options to pass to RedisCluster."""
        cluster_options = {}

        for key, value in self.options.items():
            if key not in _KNOWN_OPTIONS:
                cluster_options[key] = value

        return cluster_options

    def connect(self, url: str) -> "RedisCluster":
        """Connect to a Redis Cluster."""
        if url in self._clusters:
            return self._clusters[url]

        parsed = urlparse(url)
        cluster_options = self._get_cluster_options()

        if parsed.hostname:
            cluster_options["host"] = parsed.hostname
        if parsed.port:
            cluster_options["port"] = parsed.port

        cluster = RedisClusterClient(**cluster_options)
        self._clusters[url] = cluster
        return cluster

    def disconnect(self, connection: "RedisCluster") -> None:
        """Disconnect from Redis Cluster."""
        connection.close()
        # Remove from cache
        for url, cluster in list(self._clusters.items()):
            if cluster is connection:
                del self._clusters[url]


def get_connection_factory(options: dict):
    """Get the appropriate connection factory for the given options."""
    # Check for explicit connection_factory option
    factory_path = options.get("connection_factory")

    # Fall back to global setting
    if not factory_path:
        factory_path = getattr(
            settings,
            "DJANGO_REDIS_CONNECTION_FACTORY",
            "django_cachex.pool.ConnectionFactory",
        )

    if isinstance(factory_path, str):
        factory_class = import_string(factory_path)
    else:
        factory_class = factory_path

    return factory_class(options)
