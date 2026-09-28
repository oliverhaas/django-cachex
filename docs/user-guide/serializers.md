# Serializers

The `serializer` option sets how values are encoded before they are sent to Valkey or Redis.

## Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "OPTIONS": {
            "serializer": "django_cachex.serializers.json.JsonSerializer",
        },
    }
}
```

## Available Serializers

| Serializer | Description | Extra |
|------------|-------------|-------|
| `django_cachex.serializers.pickle.PickleSerializer` | Python pickle (default), for nearly all Python types | (stdlib) |
| `django_cachex.serializers.json.JsonSerializer` | JSON via Django's `DjangoJSONEncoder` (broadest Django type coverage of the JSON family) | (stdlib) |
| `django_cachex.serializers.msgpack.MsgpackSerializer` | MessagePack via the `msgpack` package (C extension when its wheel ships one), a compact binary format | `msgpack` |
| `django_cachex.serializers.orjson.OrjsonSerializer` | Rust-backed JSON, with fewer types than `DjangoJSONEncoder` | `orjson` |
| `django_cachex.serializers.ormsgpack.OrmsgpackSerializer` | Rust-backed MessagePack | `ormsgpack` |

Install the optional serializers with their extras:

```console
uv add django-cachex[msgpack]
uv add django-cachex[orjson]
uv add django-cachex[ormsgpack]
```

## Constructor options

`serializer` takes a dotted path, a class or an instance. A dotted path or
class is instantiated with no arguments. To set a constructor option, pass an
instance. Two serializers have one, both keyword-only:

| Serializer | Option | Default |
|------------|--------|---------|
| `PickleSerializer` | `protocol` | `pickle.DEFAULT_PROTOCOL` |
| `JsonSerializer` | `encoder_class` | `DjangoJSONEncoder` |

```python
from django_cachex.serializers.pickle import PickleSerializer

CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "OPTIONS": {
            "serializer": PickleSerializer(protocol=5),
        },
    }
}
```

An instance works in the fallback list too, mixed with dotted paths.

## Type compatibility

A check mark means the value comes back as the same type. A tilde (`~`)
followed by a type means the value comes back as that type, and the caller
converts it on read. A cross means `dumps` raises `SerializerError`.

| Type | pickle | json (Django) | msgpack | orjson | ormsgpack |
|------|:------:|:-------------:|:-------:|:------:|:---------:|
| Throughput vs pickle¹ | 1.00× | 0.88× | 0.99× | 1.05× | 1.05× |
| JSON primitives (`str`, `int`, `float`, `bool`, `None`, `list`, `dict`) | ✓ | ✓ | ✓ | ✓ | ✓ |
| `bytes` | ✓ | ✗ | ✓ | ✗ | ✓ |
| `tuple` | ✓ | ~ list | ~ list | ~ list | ~ list |
| `set` / `frozenset` | ✓ | ✗ | ✗ | ✗ | ✗ |
| `datetime` / `date` / `time` | ✓ | ~ str | ✗ | ~ str | ~ str |
| `timedelta` | ✓ | ~ str | ✗ | ✗ | ✗ |
| `Decimal` | ✓ | ~ str | ✗ | ✗ | ✗ |
| `UUID` | ✓ | ~ str | ✗ | ~ str | ~ str |
| `complex` | ✓ | ✗ | ✗ | ✗ | ✗ |
| `dataclass` instance | ✓ | ✗ | ✗ | ~ dict | ~ dict |
| `Enum` | ✓ | ✗ | ✗ | ~ value | ~ value |

¹ Geometric mean of the `get`, `set`, `mget` and `mset` rates, end to end
through the `valkey-py+libvalkey` adapter to a local Valkey, with a ~150 B
payload. A real network or larger payloads narrow the spread. The
[benchmarks](https://github.com/oliverhaas/django-cachex/tree/main/benchmarks)
harness reproduces it.

`Decimal("1.99")` round-trips through `DjangoJSONEncoder` as the string
`"1.99"`. To get a `Decimal` back, convert on read.

For Django model instances and types the table does not list, use `pickle` or
a custom serializer.

For JSON-compatible values, or with `Decimal` and `datetime` converted to
strings first, `orjson` and `ormsgpack` are the fastest encoders on batch
writes. They are about 45% ahead of `json` on `mset` in the
[benchmarks](../reference/benchmarks.md). Single-key operations are
transport-bound, so the encoder changes them little.

## Fallback for Migration

To migrate between formats, pass a list of serializers. The cache writes with the first and tries each in order on read:

```python
"OPTIONS": {
    "serializer": [
        "django_cachex.serializers.json.JsonSerializer",     # Write with new format
        "django_cachex.serializers.pickle.PickleSerializer", # Read old format
    ],
}
```

## Custom Serializers

Subclass `BaseSerializer` and implement `_dumps` and `_loads`. The base class
wraps any exception they raise in `SerializerError`, which triggers the
fallback chain. It passes plain ints through `loads` unchanged, so `incr()`
results need no decoding:

```python
from django_cachex.serializers.base import BaseSerializer


class MySerializer(BaseSerializer):
    def _dumps(self, obj):
        return my_encode(obj)  # must return bytes

    def _loads(self, data):
        return my_decode(data)
```
