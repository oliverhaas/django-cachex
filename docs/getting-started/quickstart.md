# Quick Start

## Configure as Cache Backend

To start using django-cachex, configure your Django cache settings:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
    }
}
```

For Redis instead of Valkey:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
    }
}
```

## Connection URL Formats

django-cachex uses the valkey-py/redis-py native URL notation for connection strings:

- `valkey://[[username]:[password]]@localhost:6379/0` - Valkey TCP connection
- `redis://[[username]:[password]]@localhost:6379/0` - Redis TCP connection
- `valkeys://[[username]:[password]]@localhost:6379/0` - Valkey SSL/TLS connection
- `rediss://[[username]:[password]]@localhost:6379/0` - Redis SSL/TLS connection
- `unix://[[username]:[password]]@/path/to/socket.sock?db=0` - Unix socket

### Database Selection

There are two ways to specify the database number:

1. Query string: `valkey://localhost?db=0`
2. Path (for `valkey://` or `redis://` scheme): `valkey://localhost/0`

## Configure as Session Backend

Django can use any cache backend as session storage:

```python
SESSION_ENGINE = "django.contrib.sessions.backends.cache"
SESSION_CACHE_ALIAS = "default"
```

## Basic Usage

```python
from django.core.cache import cache

# Set a value
cache.set("key", "value", timeout=300)

# Get a value
value = cache.get("key")

# Delete a key
cache.delete("key")

# Set multiple values
cache.set_many({"key1": "value1", "key2": "value2"})

# Get multiple values
values = cache.get_many(["key1", "key2"])
```

## Raw Client Access

For advanced features not exposed by Django's cache interface:

```python
from django_cachex import get_redis_connection

conn = get_redis_connection("default")
conn.set("raw_key", "raw_value")
```

## Testing

To flush all cache data after tests:

```python
from django_cachex import get_redis_connection

def tearDown(self):
    get_redis_connection("default").flushall()
```
