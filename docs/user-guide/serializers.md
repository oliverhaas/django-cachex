# Serializers

The `serializer` option sets how the cache encodes values before it sends them to Valkey or Redis:

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
| `django_cachex.serializers.json.JsonSerializer` | JSON via Django's `DjangoJSONEncoder` | (stdlib) |
| `django_cachex.serializers.msgpack.MsgpackSerializer` | MessagePack via `msgpack` | `msgpack` |
| `django_cachex.serializers.orjson.OrjsonSerializer` | Rust-backed JSON | `orjson` |
| `django_cachex.serializers.ormsgpack.OrmsgpackSerializer` | Rust-backed MessagePack | `ormsgpack` |

Install an optional serializer with its extra:

```console
uv add django-cachex[msgpack]
uv add django-cachex[orjson]
uv add django-cachex[ormsgpack]
```

## Constructor options

`serializer` takes a dotted path, a class or an instance. The cache instantiates a dotted path or class with no arguments, so pass an instance to set an option. Two serializers take a keyword-only option:

| Serializer | Option | Default |
|------------|--------|---------|
| `PickleSerializer` | `protocol` | `pickle.DEFAULT_PROTOCOL` |
| `JsonSerializer` | `encoder_class` | `DjangoJSONEncoder` |

```python
from django_cachex.serializers.pickle import PickleSerializer

"OPTIONS": {
    "serializer": PickleSerializer(protocol=5),
}
```

## Type compatibility

A check mark means the value comes back as the same type. `~ type` means it
comes back as that type, and the caller converts it on read. A cross means
`dumps` raises `SerializerError`.

| Type | pickle | json (Django) | msgpack | orjson | ormsgpack |
|------|:------:|:-------------:|:-------:|:------:|:---------:|
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

For Django model instances and types the table does not list, use `pickle` or
a custom serializer. For JSON-compatible values, `orjson` and `ormsgpack` are
the fastest encoders on batch writes, and `json` is the slowest. The encoder
changes single-key operations little. See the
[serializer benchmark](../reference/benchmarks.md#serializers).

## Fallback for Migration

To migrate between formats, pass a list of serializers. The cache writes with
the first and tries each in order on read. The list can mix instances and
dotted paths:

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
wraps their exceptions in `SerializerError`, so a failed read falls through to
the next serializer in the fallback list:

```python
from django_cachex.serializers.base import BaseSerializer


class MySerializer(BaseSerializer):
    def _dumps(self, obj):
        return my_encode(obj)  # must return bytes

    def _loads(self, data):
        return my_decode(data)
```
