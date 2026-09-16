# Serializers

django-cachex supports pluggable serializers for data before sending to Valkey/Redis.

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
| `django_cachex.serializers.pickle.PickleSerializer` | Python pickle (default); supports nearly all Python types | (stdlib) |
| `django_cachex.serializers.json.JsonSerializer` | JSON via Django's `DjangoJSONEncoder` (broadest Django type coverage of the JSON family) | (stdlib) |
| `django_cachex.serializers.msgpack.MsgpackSerializer` | MessagePack via the `msgpack` package (C extension when its wheel ships one); compact binary format | `msgpack` |
| `django_cachex.serializers.orjson.OrjsonSerializer` | Rust-backed JSON; fewer types than `DjangoJSONEncoder` | `orjson` |
| `django_cachex.serializers.ormsgpack.OrmsgpackSerializer` | Rust-backed MessagePack | `ormsgpack` |

Install optional serializers via the matching extra:

```console
uv add django-cachex[msgpack]
uv add django-cachex[orjson]
uv add django-cachex[ormsgpack]
```

## Constructor options

`serializer` takes a dotted path, a class or an instance. A dotted path or
class is instantiated with no arguments, so pass an instance to set a
constructor option. Two serializers have one, both keyword-only:

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

Round-trip behaviour for common Python types. A check mark means the value
comes back as the same type; a tilde (`~`) followed by a type means the value
is encoded but comes back as that type, so the caller converts on read; a cross
means `dumps` raises `SerializerError`.

| Type | pickle | json (Django) | msgpack | orjson | ormsgpack |
|------|:------:|:-------------:|:-------:|:------:|:---------:|
| **Throughput vs pickle**¹ | **1.00×** | **0.88×** | **0.99×** | **1.05×** | **1.05×** |
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

¹ End-to-end Django cache → `valkey-py+libvalkey` adapter → localhost Valkey,
~150 B payload, geometric mean of `get`/`set`/`mget`/`mset` ops/sec.
Real network or larger payloads dampen the spread. Reproduce with the
[benchmarks](https://github.com/oliverhaas/django-cachex/tree/main/benchmarks) harness.

Notes:

- The "~" cells are not bugs; they reflect what the underlying format can
  represent. `Decimal("1.99")` round-trips through `DjangoJSONEncoder` as the
  string `"1.99"`; if you need a `Decimal` back, convert on read.
- `orjson` natively encodes `dataclass` and `Enum` values, but loses the
  original type on the way back (becomes a `dict` or the underlying value).
- For arbitrary Django model instances or types not listed above, prefer
  `pickle` or write a custom serializer.
- If your values are JSON-compatible (or you pre-convert `Decimal`/`datetime`
  to strings), `orjson` and `ormsgpack` are the fastest encoders on batch
  writes: about 45% ahead of `json` on `mset` in the
  [benchmarks](../reference/benchmarks.md). Single-key operations are
  transport-bound, so the encoder barely moves them.

## Fallback for Migration

Specify a list of serializers to safely migrate between formats. The first is used for writing, all are tried for reading:

```python
"OPTIONS": {
    "serializer": [
        "django_cachex.serializers.json.JsonSerializer",     # Write with new format
        "django_cachex.serializers.pickle.PickleSerializer", # Read old format
    ],
}
```

## Custom Serializers

Subclass `BaseSerializer` and implement `_dumps` and `_loads`. The base
class wraps any exception they raise in `SerializerError` (which triggers
the fallback chain) and passes plain ints through `loads` unchanged, so
`incr()` results don't need re-decoding:

```python
from django_cachex.serializers.base import BaseSerializer


class MySerializer(BaseSerializer):
    def _dumps(self, obj):
        return my_encode(obj)  # must return bytes

    def _loads(self, data):
        return my_decode(data)
```
