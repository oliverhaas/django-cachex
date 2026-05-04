"""Cache fixture and configuration builders."""

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
    "json": "django_cachex.serializers.json.JSONSerializer",
    "msgpack": "django_cachex.serializers.msgpack.MessagePackSerializer",
}

# Available cache backends keyed by (topology, resp_adapter).
#
# ``valkey-glide`` only ships a single-topology cache class — no cluster /
# sentinel variants exist, so its row is restricted to ``"default"``. The
# matrix-level ``cache`` fixture skips the missing combos automatically.
BACKENDS = {
    ("default", "redis-py"): "django_cachex.cache.RedisCache",
    ("sentinel", "redis-py"): "django_cachex.cache.RedisSentinelCache",
    ("cluster", "redis-py"): "django_cachex.cache.RedisClusterCache",
    ("default", "valkey-py"): "django_cachex.cache.ValkeyCache",
    ("sentinel", "valkey-py"): "django_cachex.cache.ValkeySentinelCache",
    ("cluster", "valkey-py"): "django_cachex.cache.ValkeyClusterCache",
    ("default", "valkey-glide"): "django_cachex.cache.ValkeyGlideCache",
    ("default", "redis-rs"): "django_cachex.cache.RedisRsCache",
    ("sentinel", "redis-rs"): "django_cachex.cache.RedisRsSentinelCache",
    ("cluster", "redis-rs"): "django_cachex.cache.RedisRsClusterCache",
}

# Per-adapter image + library mapping. ``redis-py`` is paired with the Redis
# image; everything else lives on the Valkey image. Each adapter has exactly
# one home image so we don't multiply the matrix by image.
ADAPTER_IMAGES = {
    "redis-py": ("redis:latest", "redis"),
    "valkey-py": ("valkey/valkey:latest", "valkey"),
    "valkey-glide": ("valkey/valkey:latest", "valkey"),
    "redis-rs": ("valkey/valkey:latest", "valkey"),
}

# Adapters whose Python connection pool / parser internals are exercised by
# the ``test_internals.py`` / ``test_client.py`` / ``test_replica.py``
# probes. Used by ``conftest.pytest_collection_modifyitems`` to skip those
# files for adapters that don't expose those concepts (redis-rs, valkey-glide).
PY_INTERNAL_ADAPTERS = frozenset({"redis-py", "valkey-py"})

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
    """Parametrized native parser fixture (only meaningful for redis-py / valkey-py)."""
    return request.param


@pytest.fixture(params=["redis-py", "valkey-py", "valkey-glide", "redis-rs"])
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
    if resp_adapter in PY_INTERNAL_ADAPTERS:
        client_library = ADAPTER_IMAGES[resp_adapter][1]
        options: dict = _get_client_library_options(client_library, native_parser)
    else:
        # redis-rs and valkey-glide ignore pool/parser options entirely.
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
    resp_adapter: str = "redis-py",
    native_parser: bool = False,
    db: int = 7,
) -> dict:
    """Build a CACHES configuration for Sentinel."""
    sentinels = [(sentinel_host, sentinel_port)]
    base_options: dict = {"sentinels": sentinels}

    client_library = ADAPTER_IMAGES[resp_adapter][1]
    if resp_adapter in PY_INTERNAL_ADAPTERS:
        # Sentinel uses SentinelConnectionPool by default — drop pool_class.
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
    resp_adapter: str = "redis-py",
    native_parser: bool = False,
) -> dict:
    """Build a CACHES configuration for Redis Cluster."""
    options: dict = {}
    if resp_adapter in PY_INTERNAL_ADAPTERS:
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
        "doesnotexist": {
            "BACKEND": backend_class,
            "LOCATION": f"redis://{cluster_host}:56379",
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
    redis_container: RedisContainerInfo,
    request: pytest.FixtureRequest,
    backend_val: str,
    sentinel_mode_val: str | bool,
    compressor_val: str | None,
    serializer_val: str | None,
    native_parser_val: bool = False,
    resp_adapter_val: str = "redis-py",
) -> Iterator[RespCache]:
    """Core cache creation logic shared by all cache fixtures."""
    redis_host = redis_container.host
    redis_port = redis_container.port

    if sentinel_mode_val:
        sentinel_info: SentinelContainerInfo = request.getfixturevalue("sentinel_container")
        db = 8 if sentinel_mode_val == "sentinel_opts" else 7
        caches = build_sentinel_cache_config(
            sentinel_info.host,
            sentinel_info.port,
            resp_adapter=resp_adapter_val,
            native_parser=native_parser_val,
            db=db,
        )

        with override_settings(CACHES=caches):
            from django.core.cache import cache as default_cache

            default_cache.flush_db()
            yield cast("RespCache", default_cache)
            default_cache.flush_db()
        return

    if backend_val == "cluster":
        cluster_host, cluster_port = request.getfixturevalue("cluster_container")
        caches = build_cluster_cache_config(
            cluster_host,
            cluster_port,
            compressor=compressor_val,
            serializer=serializer_val,
            resp_adapter=resp_adapter_val,
            native_parser=native_parser_val,
        )
        with override_settings(CACHES=caches):
            from django.core.cache import cache as default_cache

            default_cache.flush_db()
            yield cast("RespCache", default_cache)
            default_cache.flush_db()
        return

    db = get_db_number(backend_val, compressor_val, serializer_val)
    caches = build_cache_config(
        redis_host,
        redis_port,
        backend=backend_val,
        compressor=compressor_val,
        serializer=serializer_val,
        resp_adapter=resp_adapter_val,
        native_parser=native_parser_val,
        db=db,
    )

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


def _skip_unsupported_combo(resp_adapter: str, client_class: str, sentinel_mode: str | bool) -> None:
    """Skip combos that the adapter doesn't ship a cache class for.

    ``sentinel_mode`` takes priority over ``client_class`` in ``_make_cache``,
    so the lookup matches that order.
    """
    if not _adapter_library_available(resp_adapter):
        pytest.skip(f"{resp_adapter} library not installed")
    if sentinel_mode:
        if ("sentinel", resp_adapter) not in BACKENDS:
            pytest.skip(f"{resp_adapter} has no sentinel cache class")
    elif (client_class, resp_adapter) not in BACKENDS:
        pytest.skip(f"{resp_adapter} has no {client_class} cache class")


@pytest.fixture
def cache(
    client_class: str,
    sentinel_mode: str | bool,
    resp_adapter: str,
    redis_container: RedisContainerInfo,
    request: pytest.FixtureRequest,
) -> Iterator[RespCache]:
    """Django cache fixture parametrized by client_class × sentinel_mode × resp_adapter."""
    _skip_unsupported_combo(resp_adapter, client_class, sentinel_mode)

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
        resp_adapter,
    )


def _make_stampede_cache(
    redis_container: RedisContainerInfo,
    request: pytest.FixtureRequest,
    backend_val: str,
    resp_adapter_val: str = "redis-py",
) -> Iterator[RespCache]:
    """Create a cache with stampede prevention enabled."""
    redis_host = redis_container.host
    redis_port = redis_container.port

    if backend_val == "cluster":
        cluster_host, cluster_port = request.getfixturevalue("cluster_container")
        caches = build_cluster_cache_config(
            cluster_host,
            cluster_port,
            resp_adapter=resp_adapter_val,
        )
    else:
        db = 15
        caches = build_cache_config(
            redis_host,
            redis_port,
            backend=backend_val,
            resp_adapter=resp_adapter_val,
            db=db,
        )

    caches["default"]["OPTIONS"]["stampede_prevention"] = True

    with override_settings(CACHES=caches):
        from django.core.cache import cache as default_cache

        default_cache.flush_db()
        yield cast("RespCache", default_cache)
        default_cache.flush_db()


@pytest.fixture
def stampede_cache(
    client_class: str,
    resp_adapter: str,
    redis_container: RedisContainerInfo,
    request: pytest.FixtureRequest,
) -> Iterator[RespCache]:
    """Django cache fixture with stampede prevention enabled."""
    _skip_unsupported_combo(resp_adapter, client_class, sentinel_mode=False)

    yield from _make_stampede_cache(
        redis_container,
        request,
        client_class,
        resp_adapter,
    )
