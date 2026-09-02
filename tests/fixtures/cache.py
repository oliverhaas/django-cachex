"""Cache fixture and configuration builders."""

import zlib
from typing import TYPE_CHECKING, cast

import pytest
from django.test import override_settings

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django_cachex.cache import RespCache
    from tests.fixtures.containers import RedisContainerInfo, SentinelContainerInfo

# Available compressors (None means no compression)
COMPRESSORS = {
    None: None,
    "gzip": "django_cachex.compressors.gzip.GzipCompressor",
    "lz4": "django_cachex.compressors.lz4.Lz4Compressor",
    "lzma": "django_cachex.compressors.lzma.LzmaCompressor",
    "zlib": "django_cachex.compressors.zlib.ZlibCompressor",
    "zstd": "django_cachex.compressors.zstd.ZstdCompressor",
}

# Available serializers (None means default pickle)
SERIALIZERS = {
    None: None,
    "json": "django_cachex.serializers.json.JsonSerializer",
    "msgpack": "django_cachex.serializers.msgpack.MsgpackSerializer",
}

# Available cache backends keyed by (topology, resp_adapter).
#
# ``valkey-glide`` ships standalone + cluster clients only. No Sentinel
# client exists upstream, so its sentinel row is omitted. The matrix-level
# ``cache`` fixture skips the missing combos automatically.
BACKENDS = {
    ("default", "redis-py"): "django_cachex.cache.RedisCache",
    ("sentinel", "redis-py"): "django_cachex.cache.RedisSentinelCache",
    ("cluster", "redis-py"): "django_cachex.cache.RedisClusterCache",
    ("default", "valkey-py"): "django_cachex.cache.ValkeyCache",
    ("sentinel", "valkey-py"): "django_cachex.cache.ValkeySentinelCache",
    ("cluster", "valkey-py"): "django_cachex.cache.ValkeyClusterCache",
    ("default", "valkey-glide"): "django_cachex.cache.ValkeyGlideCache",
    ("cluster", "valkey-glide"): "django_cachex.cache.ValkeyGlideClusterCache",
}

# Per-adapter image + library mapping. ``redis-py`` is paired with the Redis
# image; everything else lives on the Valkey image. Each adapter has exactly
# one home image so we don't multiply the matrix by image.
ADAPTER_IMAGES = {
    "redis-py": ("redis:latest", "redis"),
    "valkey-py": ("valkey/valkey:latest", "valkey"),
    "valkey-glide": ("valkey/valkey:latest", "valkey"),
}

# Adapters that accept ``pool_class`` / ``parser_class``; valkey-glide ignores both.
POOL_OPTION_ADAPTERS = frozenset({"redis-py", "valkey-py"})

# Modules that poke redis-py's own objects; tests/conftest.py skips them on other adapters.
REDIS_PY_INTERNALS_TEST_FILES = frozenset(
    {
        "test_internals.py",
        "test_client.py",
        "test_replica.py",
    },
)

# ``client_class`` and ``sentinel_mode`` derive from the topology so existing skips keep working.
TOPOLOGIES = ("default", "cluster", "sentinel")

# Pool / parser config per Python client library. ``client_library`` is
# derived from ``resp_adapter`` via ``ADAPTER_IMAGES``.
CLIENT_LIBRARY_CONFIGS = {
    "valkey": {
        "pool_class": "valkey.connection.ConnectionPool",
        "parser_class": "valkey._parsers.resp2._RESP2Parser",
        "native_parser_class": "valkey._parsers.libvalkey._LibvalkeyParser",
    },
    "redis": {
        "pool_class": "redis.connection.ConnectionPool",
        "parser_class": "redis._parsers.resp2._RESP2Parser",
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


@pytest.fixture(params=TOPOLOGIES)
def topology(request) -> str:
    """Parametrized topology fixture: standalone, cluster or sentinel."""
    return request.param


@pytest.fixture
def client_class(topology: str) -> str:
    """Cache class family for the active topology ("default" or "cluster")."""
    return "cluster" if topology == "cluster" else "default"


@pytest.fixture
def sentinel_mode(topology: str) -> str | bool:
    """Truthy only on the sentinel topology."""
    return "sentinel" if topology == "sentinel" else False


@pytest.fixture(params=["default", "cluster"])
def stampede_topology(request) -> str:
    """Topologies the stampede cache is built for; it has no sentinel config."""
    return request.param


@pytest.fixture(params=[False, True], ids=["python-parser", "native-parser"])
def native_parser(request) -> bool:
    """Parametrized native parser fixture (only meaningful for redis-py / valkey-py)."""
    return request.param


@pytest.fixture(params=["redis-py", "valkey-py", "valkey-glide"])
def resp_adapter(request) -> str:
    """Parametrized adapter fixture: which RespAdapter implementation to test."""
    return request.param


def _get_client_library_options(
    client_library: str,
    native_parser: bool = False,
) -> dict:
    """Pool/parser options for the redis-py / valkey-py adapters."""
    config = CLIENT_LIBRARY_CONFIGS[client_library]
    options = {"pool_class": config["pool_class"]}
    options["parser_class"] = config["native_parser_class"] if native_parser else config["parser_class"]
    return options


def build_cache_config(
    redis_host: str,
    redis_port: int,
    *,
    backend: str = "default",
    compressor: str | None = None,
    serializer: str | None = None,
    resp_adapter: str = "redis-py",
    native_parser: bool = False,
    db: int = 1,
) -> dict:
    """Build a CACHES configuration dict."""
    if resp_adapter in POOL_OPTION_ADAPTERS:
        client_library = ADAPTER_IMAGES[resp_adapter][1]
        options: dict = _get_client_library_options(client_library, native_parser)
    else:
        # valkey-glide ignores pool/parser options entirely.
        options = {}

    if compressor and compressor in COMPRESSORS:
        options["compressor"] = COMPRESSORS[compressor]
    if serializer and serializer in SERIALIZERS:
        options["serializer"] = SERIALIZERS[serializer]

    location = f"redis://{redis_host}:{redis_port}?db={db}"
    backend_class = BACKENDS[(backend, resp_adapter)]

    return {
        "default": {
            "BACKEND": backend_class,
            "LOCATION": [location, location],
            "OPTIONS": options,
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
    resp_adapter: str = "redis-py",
    native_parser: bool = False,
    db: int = 7,
) -> dict:
    """Build a CACHES configuration for Sentinel."""
    sentinels = [(sentinel_host, sentinel_port)]
    base_options: dict = {"sentinels": sentinels}

    client_library = ADAPTER_IMAGES[resp_adapter][1]
    if resp_adapter in POOL_OPTION_ADAPTERS:
        # Sentinel uses SentinelConnectionPool by default, so drop pool_class.
        lib_options = _get_client_library_options(client_library, native_parser)
        lib_options.pop("pool_class", None)
        base_options.update(lib_options)

    backend_class = BACKENDS[("sentinel", resp_adapter)]
    scheme = "valkey" if client_library == "valkey" else "redis"

    return {
        "default": {
            "BACKEND": backend_class,
            "LOCATION": [f"{scheme}://mymaster?db={db}"],
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
    resp_adapter: str = "redis-py",
    native_parser: bool = False,
) -> dict:
    """Build a CACHES configuration for Redis Cluster."""
    options: dict = {}
    if resp_adapter in POOL_OPTION_ADAPTERS:
        # Cluster manages its own connections; pass parser_class only.
        client_library = ADAPTER_IMAGES[resp_adapter][1]
        lib_options = _get_client_library_options(client_library, native_parser)
        lib_options.pop("pool_class", None)
        options.update(lib_options)

    if compressor and compressor in COMPRESSORS:
        options["compressor"] = COMPRESSORS[compressor]
    if serializer and serializer in SERIALIZERS:
        options["serializer"] = SERIALIZERS[serializer]

    location = f"redis://{cluster_host}:{cluster_port}"
    backend_class = BACKENDS[("cluster", resp_adapter)]

    return {
        "default": {
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
    """Pick one of dbs 1-14 for a cache configuration, stably across processes.

    There are more configurations than dbs, so two of them can land on the
    same db. That is harmless: tests run serially inside a worker and every
    cache fixture flushes its db both when it is set up and when it is torn
    down, so no configuration ever sees another one's keys.
    """
    fingerprint = f"{backend}|{compressor}|{serializer}".encode()
    return zlib.crc32(fingerprint) % 14 + 1


def _yield_default_cache(caches: dict) -> Iterator[RespCache]:
    """Activate ``caches`` and hand out ``caches["default"]``, flushed either side."""
    with override_settings(CACHES=caches):
        from django.core.cache import cache as default_cache

        default_cache.flush_db()
        yield cast("RespCache", default_cache)
        default_cache.flush_db()


def _adapter_library_available(resp_adapter: str) -> bool:
    """Whether the underlying client library for ``resp_adapter`` is importable.

    Glide ships only as an optional extra (and has no wheel on cp314t), so on
    bare envs the adapter constructor raises ``ImportError``. Other adapters
    are always available.
    """
    if resp_adapter == "valkey-glide":
        try:
            import glide  # noqa: F401
            import glide_sync  # noqa: F401
        except ImportError:
            return False
    return True


def _skip_unsupported_combo(resp_adapter: str, topology: str) -> None:
    """Skip cells the adapter doesn't ship a cache class for."""
    if not _adapter_library_available(resp_adapter):
        pytest.skip(f"{resp_adapter} library not installed")
    if (topology, resp_adapter) not in BACKENDS:
        pytest.skip(f"{resp_adapter} has no {topology} cache class")


@pytest.fixture
def cache(
    topology: str,
    resp_adapter: str,
    redis_container: RedisContainerInfo,
    request: pytest.FixtureRequest,
) -> Iterator[RespCache]:
    """Django cache fixture parametrized by topology × resp_adapter."""
    _skip_unsupported_combo(resp_adapter, topology)

    compressor_val = None
    serializer_val = None
    native_parser_val = False

    if "compressors" in request.fixturenames:
        compressor_val = request.getfixturevalue("compressors")
    if "serializers" in request.fixturenames:
        serializer_val = request.getfixturevalue("serializers")
    if "native_parser" in request.fixturenames:
        native_parser_val = request.getfixturevalue("native_parser")

    if topology == "sentinel":
        sentinel_info: SentinelContainerInfo = request.getfixturevalue("sentinel_container")
        caches = build_sentinel_cache_config(
            sentinel_info.host,
            sentinel_info.port,
            resp_adapter=resp_adapter,
            native_parser=native_parser_val,
        )
    elif topology == "cluster":
        cluster_host, cluster_port = request.getfixturevalue("cluster_container")
        caches = build_cluster_cache_config(
            cluster_host,
            cluster_port,
            compressor=compressor_val,
            serializer=serializer_val,
            resp_adapter=resp_adapter,
            native_parser=native_parser_val,
        )
    else:
        caches = build_cache_config(
            redis_container.host,
            redis_container.port,
            compressor=compressor_val,
            serializer=serializer_val,
            resp_adapter=resp_adapter,
            native_parser=native_parser_val,
            db=get_db_number(topology, compressor_val, serializer_val),
        )

    yield from _yield_default_cache(caches)


@pytest.fixture
def stampede_cache(
    stampede_topology: str,
    resp_adapter: str,
    redis_container: RedisContainerInfo,
    request: pytest.FixtureRequest,
) -> Iterator[RespCache]:
    """Django cache fixture with stampede prevention enabled."""
    _skip_unsupported_combo(resp_adapter, stampede_topology)

    if stampede_topology == "cluster":
        cluster_host, cluster_port = request.getfixturevalue("cluster_container")
        caches = build_cluster_cache_config(
            cluster_host,
            cluster_port,
            resp_adapter=resp_adapter,
        )
    else:
        caches = build_cache_config(
            redis_container.host,
            redis_container.port,
            resp_adapter=resp_adapter,
            db=15,
        )

    caches["default"]["OPTIONS"]["stampede_prevention"] = True
    yield from _yield_default_cache(caches)
