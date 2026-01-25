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

| Serializer | Description |
|------------|-------------|
| `django_cachex.serializers.pickle.PickleSerializer` | Python pickle (default) - supports all Python types |
| `django_cachex.serializers.json.JSONSerializer` | JSON - interoperable but limited to JSON types |
| `django_cachex.serializers.msgpack.MessagePackSerializer` | MessagePack - fast and compact binary format |

MessagePack requires the optional dependency:

```console
uv add django-cachex[msgpack]
```

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
