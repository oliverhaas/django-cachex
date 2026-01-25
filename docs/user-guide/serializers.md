# Serializers

django-cachex supports pluggable serializers for data before sending to Redis.

## Pickle Serializer (Default)

The default serializer uses Python's `pickle` module:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
            # Uses pickle by default
        }
    }
}
```

### Configure Pickle Version

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "PICKLE_VERSION": -1  # Highest protocol available
        }
    }
}
```

## JSON Serializer

For JSON-serializable data only:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
            "SERIALIZER": "django_cachex.serializers.json.JSONSerializer",
        }
    }
}
```

## MsgPack Serializer

Requires the `msgpack` library:

```console
pip install msgpack
```

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
            "SERIALIZER": "django_cachex.serializers.msgpack.MessagePackSerializer",
        }
    }
}
```

## Serializer Fallback (Migration Support)

When migrating from one serializer to another, you can specify a list of serializers.
The first serializer is used for writing new data, while all serializers are tried
in order when reading until one succeeds.

This allows safe migration between serialization formats without data loss:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            # First serializer used for writing, all tried for reading
            "SERIALIZER": [
                "django_cachex.serializers.json.JSONSerializer",  # New format
                "django_cachex.serializers.pickle.PickleSerializer",  # Old format
            ],
        }
    }
}
```

### Migration Example

1. **Before migration** - using pickle:
   ```python
   "SERIALIZER": "django_cachex.serializers.pickle.PickleSerializer"
   ```

2. **During migration** - write JSON, read both:
   ```python
   "SERIALIZER": [
       "django_cachex.serializers.json.JSONSerializer",
       "django_cachex.serializers.pickle.PickleSerializer",
   ]
   ```

3. **After migration** - all data refreshed with JSON:
   ```python
   "SERIALIZER": "django_cachex.serializers.json.JSONSerializer"
   ```

### How It Works

When deserializing, each serializer is tried in order. If deserialization fails,
the next serializer is tried. This continues until one succeeds or all fail.

## Custom Serializer

Create a custom serializer by implementing `dumps` and `loads` methods:

```python
from django_cachex.serializers.base import BaseSerializer
from django_cachex.exceptions import SerializerError

class MySerializer(BaseSerializer):
    def dumps(self, value):
        # Convert value to bytes
        return my_encode(value)

    def loads(self, value):
        # Convert bytes to value
        try:
            return my_decode(value)
        except MyDecodeError as e:
            raise SerializerError from e
```

**Note:** Custom serializers should raise `SerializerError` on deserialization failure
to enable proper fallback behavior when using multiple serializers.

Then configure:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "SERIALIZER": "myapp.serializers.MySerializer",
        }
    }
}
```
