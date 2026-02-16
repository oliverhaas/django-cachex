"""Cache fixture and configuration builders."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, cast

import pytest
from django.test import override_settings

from django_cachex.cache import KeyValueCache

if TYPE_CHECKING:
    from tests.fixtures.containers import RedisContainerInfo, SentinelContainerInfo

# Available compressors (None means no compression)
COMPRESSORS = {
    None: None,
    "gzip": "django_cachex.compressors.gzip.GzipCompressor",
    "lz4": "django_cachex.compressors.lz4.Lz4Compressor",
    "lzma": "django_cachex.compressors.lzma.LzmaCompressor",
    "zlib": "django_cachex.compressors.zlib.ZlibCompressor",
    "zstd": "django_cachex.compressors.zstd.ZStdCompressor",
}

# Available serializers (None means default pickle)
SERIALIZERS = {
    None: None,
    "json": "django_cachex.serializers.json.JSONSerializer",
    "msgpack": "django_cachex.serializers.msgpack.MessagePackSerializer",
}

# Available cache backends - keyed by (backend_type, client_library)
# These are Cache classes (extend BaseCache) - used as BACKEND in Django settings
BACKENDS = {
    # Valkey backends
    ("default", "valkey"): "django_cachex.cache.ValkeyCache",
    ("sentinel", "valkey"): "django_cachex.cache.ValkeySentinelCache",
    ("cluster", "valkey"): "django_cachex.cache.ValkeyClusterCache",
    # Redis backends
    ("default", "redis"): "django_cachex.cache.RedisCache",
    ("sentinel", "redis"): "django_cachex.cache.RedisSentinelCache",
    ("cluster", "redis"): "django_cachex.cache.RedisClusterCache",
}

# Client library configurations: maps client_library -> (pool_class, parser_class)
# parser_class is the Python parser, native_parser_class is hiredis/libvalkey parser
CLIENT_LIBRARY_CONFIGS = {
    "valkey": {
        "pool_class": "valkey.connection.ConnectionPool",
        "parser_class": "valkey._parsers.resp2._RESP2Parser",  # Python parser
        "native_parser_class": "valkey._parsers.libvalkey._LibvalkeyParser",
    },
    "redis": {
        "pool_class": "redis.connection.ConnectionPool",
        "parser_class": "redis._parsers.resp2._RESP2Parser",  # Python parser
        "native_parser_class": "redis._parsers.hiredis._HiredisParser",
    },
}


# Parametrized fixtures - tests opt-in by requesting these
@pytest.fixture(params=[None, "gzip", "lz4", "lzma", "zlib", "zstd"])
def compressors(request) -> str | None:
    """Parametrized compressor fixture. Request this to test all compressors."""
    return request.param


@pytest.fixture(params=[None, "json", "msgpack"])  # None is default pickle
def serializers(request) -> str | None:
    """Parametrized serializer fixture. Request this to test all serializers."""
    return request.param


@pytest.fixture(params=["default", "cluster"])
def client_class(request) -> str:
    """Parametrized client class fixture."""
    return request.param


@pytest.fixture(params=[False, "sentinel", "sentinel_opts"])
def sentinel_mode(request) -> str | bool:
    """Parametrized sentinel mode fixture."""
    return request.param


@pytest.fixture(params=[False, True], ids=["python-parser", "native-parser"])
def native_parser(request) -> bool:
    """Parametrized native parser fixture.

    When True, uses hiredis for redis-py or libvalkey for valkey-py.
    When False, uses the default Python parser.
    """
    return request.param


def _get_client_library_options(
    client_library: str = "redis",
    native_parser: bool = False,
) -> dict:
    """Get OPTIONS dict entries for the given client library.

    Args:
        client_library: "redis" or "valkey"
        native_parser: If True, use hiredis/libvalkey parser; else use Python parser

    Returns:
        Dict with pool_class and parser_class

    """
    config = CLIENT_LIBRARY_CONFIGS.get(client_library, CLIENT_LIBRARY_CONFIGS["redis"])
    options = {
        "pool_class": config["pool_class"],
    }
    # Always set parser_class explicitly to control which parser is used
    if native_parser:
        options["parser_class"] = config["native_parser_class"]
    else:
        options["parser_class"] = config["parser_class"]
    return options


def build_cache_config(
    redis_host: str,
    redis_port: int,
    *,
    backend: str = "default",
    compressor: str | None = None,
    serializer: str | None = None,
    client_library: str = "redis",
    native_parser: bool = False,
    db: int = 1,
) -> dict:
    """Build a CACHES configuration dict.

    Args:
        redis_host: Redis server host
        redis_port: Redis server port
        backend: django-cachex backend ("default", "sentinel", "cluster")
        compressor: Compressor name (None, "gzip", "lz4", "lzma", "zlib", "zstd")
        serializer: Serializer name (None, "json", "msgpack")
        client_library: Python client library ("redis" or "valkey")
        native_parser: If True, use native parser (hiredis/libvalkey)
        db: Redis database number

    """
    options = _get_client_library_options(client_library, native_parser)

    if compressor and compressor in COMPRESSORS:
        options["compressor"] = COMPRESSORS[compressor]
    if serializer and serializer in SERIALIZERS:
        options["serializer"] = SERIALIZERS[serializer]

    location = f"redis://{redis_host}:{redis_port}?db={db}"
    backend_class = BACKENDS[(backend, client_library)]

    return {
        "default": {
            "BACKEND": backend_class,
            "LOCATION": [location, location],
            "OPTIONS": options,
        },
        "doesnotexist": {
            "BACKEND": backend_class,
            "LOCATION": f"redis://{redis_host}:56379?db={db}",
            "OPTIONS": options.copy(),
        },
        "sample": {
            "BACKEND": backend_class,
            "LOCATION": f"{location},{location}",
            "OPTIONS": options.copy(),
        },
        "with_prefix": {
            "BACKEND": backend_class,
            "LOCATION": location,
            "OPTIONS": options.copy(),
            "KEY_PREFIX": "test-prefix",
        },
    }


def build_sentinel_cache_config(
    sentinel_host: str,
    sentinel_port: int,
    *,
    client_library: str = "redis",
    native_parser: bool = False,
    db: int = 7,
) -> dict:
    """Build a CACHES configuration for Sentinel."""
    sentinels = [(sentinel_host, sentinel_port)]

    base_options = {
        "sentinels": sentinels,
    }
    # Get client library options but exclude pool_class - sentinel uses SentinelConnectionPool by default
    lib_options = _get_client_library_options(client_library, native_parser)
    lib_options.pop("pool_class", None)
    base_options.update(lib_options)

    backend_class = BACKENDS[("sentinel", client_library)]

    # Use appropriate URL scheme based on client library
    scheme = "valkey" if client_library == "valkey" else "redis"

    return {
        "default": {
            "BACKEND": backend_class,
            "LOCATION": [f"{scheme}://mymaster?db={db}"],
            "OPTIONS": base_options.copy(),
        },
        "doesnotexist": {
            "BACKEND": backend_class,
            "LOCATION": f"{scheme}://missing_service?db={db}",
            "OPTIONS": base_options.copy(),
        },
        "sample": {
            "BACKEND": backend_class,
            "LOCATION": f"{scheme}://mymaster?db={db}",
            "OPTIONS": base_options.copy(),
        },
        "with_prefix": {
            "BACKEND": backend_class,
            "LOCATION": f"{scheme}://mymaster?db={db}",
            "OPTIONS": base_options.copy(),
            "KEY_PREFIX": "test-prefix",
        },
    }


def build_cluster_cache_config(
    cluster_host: str,
    cluster_port: int,
    *,
    compressor: str | None = None,
    serializer: str | None = None,
    client_library: str = "redis",
    native_parser: bool = False,
) -> dict:
    """Build a CACHES configuration for Redis Cluster."""
    options = {}
    # For cluster, we only use client library options for non-cluster connections
    # The ClusterCacheClient creates RedisCluster/ValkeyCluster directly
    lib_options = _get_client_library_options(client_library, native_parser)
    # Remove pool class - cluster manages its own connections
    lib_options.pop("pool_class", None)
    options.update(lib_options)

    if compressor and compressor in COMPRESSORS:
        options["compressor"] = COMPRESSORS[compressor]
    if serializer and serializer in SERIALIZERS:
        options["serializer"] = SERIALIZERS[serializer]

    # Cluster doesn't use db numbers - data is sharded across slots
    location = f"redis://{cluster_host}:{cluster_port}"
    backend_class = BACKENDS[("cluster", client_library)]

    return {
        "default": {
            "BACKEND": backend_class,
            "LOCATION": location,
            "OPTIONS": options.copy(),
        },
        "doesnotexist": {
            "BACKEND": backend_class,
            "LOCATION": f"redis://{cluster_host}:56379",  # Non-existent port
            "OPTIONS": options.copy(),
        },
        "sample": {
            "BACKEND": backend_class,
            "LOCATION": location,
            "OPTIONS": options.copy(),
        },
        "with_prefix": {
            "BACKEND": backend_class,
            "LOCATION": location,
            "OPTIONS": options.copy(),
            "KEY_PREFIX": "test-prefix",
        },
    }


def get_db_number(
    backend: str,
    compressor: str | None,
    serializer: str | None,
) -> int:
    """Calculate the db number based on configuration to avoid conflicts."""
    return abs(hash((backend, compressor, serializer))) % 14 + 1


def _make_cache(
    redis_container: "RedisContainerInfo",
    request: pytest.FixtureRequest,
    backend_val: str,
    sentinel_mode_val: str | bool,
    compressor_val: str | None,
    serializer_val: str | None,
    native_parser_val: bool = False,
) -> Iterator[KeyValueCache]:
    """Core cache creation logic shared by all cache fixtures."""
    redis_host = redis_container.host
    redis_port = redis_container.port
    client_library = redis_container.client_library

    # Handle sentinel mode - it overrides other settings
    if sentinel_mode_val:
        sentinel_info: SentinelContainerInfo = request.getfixturevalue("sentinel_container")
        db = 8 if sentinel_mode_val == "sentinel_opts" else 7
        caches = build_sentinel_cache_config(
            sentinel_info.host,
            sentinel_info.port,
            client_library=sentinel_info.client_library,
            native_parser=native_parser_val,
            db=db,
        )

        with override_settings(CACHES=caches):
            from django.core.cache import cache as default_cache

            default_cache.flush_db()  # Flush DB before test
            yield cast("KeyValueCache", default_cache)
            default_cache.flush_db()  # Flush DB after test
        return

    # Handle cluster client - needs cluster_container instead of redis_container
    if backend_val == "cluster":
        cluster_host, cluster_port = request.getfixturevalue("cluster_container")
        caches = build_cluster_cache_config(
            cluster_host,
            cluster_port,
            compressor=compressor_val,
            serializer=serializer_val,
            client_library=client_library,
            native_parser=native_parser_val,
        )
        with override_settings(CACHES=caches):
            from django.core.cache import cache as default_cache

            default_cache.flush_db()  # Flush DB before test
            yield cast("KeyValueCache", default_cache)
            default_cache.flush_db()  # Flush DB after test
        return

    # Build cache config for default client
    db = get_db_number(backend_val, compressor_val, serializer_val)
    caches = build_cache_config(
        redis_host,
        redis_port,
        backend=backend_val,
        compressor=compressor_val,
        serializer=serializer_val,
        client_library=client_library,
        native_parser=native_parser_val,
        db=db,
    )

    with override_settings(CACHES=caches):
        from django.core.cache import cache as default_cache

        default_cache.flush_db()  # Flush DB before test
        yield cast("KeyValueCache", default_cache)
        default_cache.flush_db()  # Flush DB after test


@pytest.fixture
def cache(
    client_class: str,
    sentinel_mode: str | bool,
    redis_container: "RedisContainerInfo",
    request: pytest.FixtureRequest,
) -> Iterator[KeyValueCache]:
    """Django cache fixture parametrized by client_class × sentinel_mode.

    If the test also requests `compressor`, `serializer`, or `native_parser` fixtures,
    those will be used (creating additional Cartesian product).
    Otherwise, defaults (no compression, pickle serializer, Python parser) are used.

    The client library (redis-py vs valkey-py) is automatically determined from
    the redis_container fixture, which is coupled to the server image being tested.
    """

    # Check if test opted into compressor/serializer/native_parser parametrization
    compressor_val = None
    serializer_val = None
    native_parser_val = False

    if "compressors" in request.fixturenames:
        compressor_val = request.getfixturevalue("compressors")
    if "serializers" in request.fixturenames:
        serializer_val = request.getfixturevalue("serializers")
    if "native_parser" in request.fixturenames:
        native_parser_val = request.getfixturevalue("native_parser")

    yield from _make_cache(
        redis_container,
        request,
        client_class,
        sentinel_mode,
        compressor_val,
        serializer_val,
        native_parser_val,
    )
