# Quick Start

## Configure as Cache Backend

To start using django-cachex-ng, configure your Django cache settings:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
        }
    }
}
```

## Connection URL Formats

django-cachex uses the redis-py native URL notation for connection strings:

- `redis://[[username]:[password]]@localhost:6379/0` - TCP connection
- `rediss://[[username]:[password]]@localhost:6379/0` - SSL/TLS connection
- `unix://[[username]:[password]]@/path/to/socket.sock?db=0` - Unix socket

### Database Selection

There are two ways to specify the database number:

1. Query string: `redis://localhost?db=0`
2. Path (for `redis://` scheme): `redis://localhost/0`

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

## Raw Redis Access

For advanced Redis features not exposed by Django's cache interface:

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
