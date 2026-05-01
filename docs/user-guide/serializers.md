# Serializers

django-cachex supports pluggable serializers for data before sending to Valkey/Redis.

## Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "OPTIONS": {
            "serializer": "django_cachex.serializers.json.JSONSerializer",
        }
    }
}
```

## Available Serializers

| Serializer | Description | Extra |
|------------|-------------|-------|
| `django_cachex.serializers.pickle.PickleSerializer` | Python pickle (default) — supports nearly all Python types | — |
| `django_cachex.serializers.json.JSONSerializer` | JSON via Django's `DjangoJSONEncoder` — best Django type coverage of the JSON family | — |
| `django_cachex.serializers.msgpack.MessagePackSerializer` | Pure-Python MessagePack — compact binary format | `msgpack` |
| `django_cachex.serializers.orjson.OrjsonSerializer` | Rust-backed JSON — fastest JSON, fewer types than `DjangoJSONEncoder` | `orjson` |
| `django_cachex.serializers.ormsgpack.OrMessagePackSerializer` | Rust-backed MessagePack — fastest overall in our benchmarks | `ormsgpack` |

Install optional serializers via the matching extra:

```console
uv add django-cachex[msgpack]
uv add django-cachex[orjson]
uv add django-cachex[ormsgpack]
```

## Type compatibility

Round-trip behaviour for common Python types. Legend: **✓** preserved (same
type back), **~** encoded but returns as a different type (caller must convert
on read), **✗** raises `SerializerError` on `dumps`.

| Type | pickle | json (Django) | msgpack | orjson | ormsgpack |
|------|:------:|:-------------:|:-------:|:------:|:---------:|
| **Throughput vs pickle**¹ | **1.00×** | **0.72×** | **1.06×** | **1.10×** | **1.13×** |
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

¹ End-to-end Django cache → `rust-valkey` driver → localhost Valkey,
~150 B payload, geometric mean of `get`/`set`/`mget`/`mset` ops/sec.
Real network or larger payloads dampen the spread. Reproduce with the
[benchmarks](https://github.com/e1plus/django-cachex/tree/main/benchmarks) harness.

Notes:

- The "~" cells are not bugs — they reflect what the underlying format can
  represent. `Decimal("1.99")` round-trips through `DjangoJSONEncoder` as the
  string `"1.99"`; if you need a `Decimal` back, convert on read.
- `orjson` natively encodes `dataclass` and `Enum` values, but loses the
  original type on the way back (becomes a `dict` or the underlying value).
- For arbitrary Django model instances or types not listed above, prefer
  `pickle` or write a custom serializer.
- If you need maximum speed and your values are JSON-compatible (or you
  pre-convert `Decimal`/`datetime` to strings), `orjson` and `ormsgpack` are
  significantly faster than the pure-Python equivalents.

## Fallback for Migration

Specify a list of serializers to safely migrate between formats. The first is used for writing, all are tried for reading:

```python
"OPTIONS": {
    "serializer": [
        "django_cachex.serializers.json.JSONSerializer",     # Write with new format
        "django_cachex.serializers.pickle.PickleSerializer", # Read old format
    ],
}
```

## Custom Serializers

Implement `dumps` and `loads` methods, raising `SerializerError` on failure:

```python
from django_cachex.serializers.base import BaseSerializer
from django_cachex.exceptions import SerializerError

class MySerializer(BaseSerializer):
    def dumps(self, value):
        return my_encode(value)

    def loads(self, value):
        try:
            return my_decode(value)
        except MyDecodeError as e:
            raise SerializerError from e
```
