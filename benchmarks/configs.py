"""Driver and serializer configurations enumerated for the benchmark matrix.

Each config is a small dataclass describing how to build a CACHES entry. The
runner takes a (driver, serializer) pair and a server URL and produces the
override_settings payload.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class DriverConfig:
    id: str
    backend: str
    options: dict
    server: str  # "redis" or "valkey" — picks which container URL to use


@dataclass(frozen=True)
class SerializerConfig:
    id: str
    dotted_path: str | None  # None means default pickle


@dataclass(frozen=True)
class CompressorConfig:
    id: str
    dotted_path: str | None  # None means no compression


# Drivers we want to compare. The "server" field decides which container URL
# the runner connects to — we keep redis-py paired with redis-server and
# valkey-py paired with valkey-server because that's the natural pairing.
DRIVER_CONFIGS: tuple[DriverConfig, ...] = (
    DriverConfig(
        id="redis-py",
        backend="django_cachex.cache.RedisCache",
        options={
            "pool_class": "redis.connection.ConnectionPool",
            "parser_class": "redis._parsers.resp2._RESP2Parser",
        },
        server="redis",
    ),
    DriverConfig(
        id="redis-py+hiredis",
        backend="django_cachex.cache.RedisCache",
        options={
            "pool_class": "redis.connection.ConnectionPool",
            "parser_class": "redis._parsers.hiredis._HiredisParser",
        },
        server="redis",
    ),
    DriverConfig(
        id="valkey-py",
        backend="django_cachex.cache.ValkeyCache",
        options={
            "pool_class": "valkey.connection.ConnectionPool",
            "parser_class": "valkey._parsers.resp2._RESP2Parser",
        },
        server="valkey",
    ),
    DriverConfig(
        id="valkey-py+libvalkey",
        backend="django_cachex.cache.ValkeyCache",
        options={
            "pool_class": "valkey.connection.ConnectionPool",
            "parser_class": "valkey._parsers.libvalkey._LibvalkeyParser",
        },
        server="valkey",
    ),
    DriverConfig(
        id="redis-rs",
        backend="django_cachex.cache.RustValkeyCache",
        options={},
        server="valkey",
    ),
    DriverConfig(
        id="valkey-glide",
        backend="django_cachex.cache.glide.ValkeyGlideCache",
        options={},
        server="valkey",
    ),
    # Django's official built-in Redis cache backend (added in Django 4.0,
    # `django.core.cache.backends.redis.RedisCache`). Not to be confused with
    # the third-party `jazzband/django-redis` package — that one ships under
    # `django_redis.cache.RedisCache` and is unrelated.
    #
    # Useful as an external reference point: its `get_client()` instantiates
    # a fresh `redis.Redis` per cache call, sharing only the pool. Makes it
    # an interesting subject for our connection tracking.
    DriverConfig(
        id="django (builtin)",
        backend="django.core.cache.backends.redis.RedisCache",
        options={},
        server="redis",
    ),
)


SERIALIZER_CONFIGS: tuple[SerializerConfig, ...] = (
    SerializerConfig(id="pickle", dotted_path=None),
    SerializerConfig(id="json", dotted_path="django_cachex.serializers.json.JSONSerializer"),
    SerializerConfig(id="msgpack", dotted_path="django_cachex.serializers.msgpack.MessagePackSerializer"),
    SerializerConfig(id="orjson", dotted_path="django_cachex.serializers.orjson.OrjsonSerializer"),
    SerializerConfig(id="ormsgpack", dotted_path="django_cachex.serializers.ormsgpack.OrMessagePackSerializer"),
)


COMPRESSOR_CONFIGS: tuple[CompressorConfig, ...] = (
    CompressorConfig(id="none", dotted_path=None),
    CompressorConfig(id="zlib", dotted_path="django_cachex.compressors.zlib.ZlibCompressor"),
    CompressorConfig(id="gzip", dotted_path="django_cachex.compressors.gzip.GzipCompressor"),
    CompressorConfig(id="lzma", dotted_path="django_cachex.compressors.lzma.LzmaCompressor"),
    CompressorConfig(id="lz4", dotted_path="django_cachex.compressors.lz4.Lz4Compressor"),
    CompressorConfig(id="zstd", dotted_path="django_cachex.compressors.zstd.ZstdCompressor"),
)


DRIVER_BY_ID = {c.id: c for c in DRIVER_CONFIGS}
SERIALIZER_BY_ID = {c.id: c for c in SERIALIZER_CONFIGS}
COMPRESSOR_BY_ID = {c.id: c for c in COMPRESSOR_CONFIGS}
