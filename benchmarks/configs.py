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
        id="rust-valkey",
        backend="django_cachex.cache.RustValkeyCache",
        options={},
        server="valkey",
    ),
)


SERIALIZER_CONFIGS: tuple[SerializerConfig, ...] = (
    SerializerConfig(id="pickle", dotted_path=None),
    SerializerConfig(id="json", dotted_path="django_cachex.serializers.json.JSONSerializer"),
    SerializerConfig(id="msgpack", dotted_path="django_cachex.serializers.msgpack.MessagePackSerializer"),
    SerializerConfig(id="orjson", dotted_path="django_cachex.serializers.orjson.OrjsonSerializer"),
    SerializerConfig(id="ormsgpack", dotted_path="django_cachex.serializers.ormsgpack.OrMessagePackSerializer"),
)


DRIVER_BY_ID = {c.id: c for c in DRIVER_CONFIGS}
SERIALIZER_BY_ID = {c.id: c for c in SERIALIZER_CONFIGS}
