# Configuration

## Authentication

### Using Redis ACLs

When using Redis ACLs, add the username to the URL:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://django@localhost:6379/0",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
            "PASSWORD": "mysecret"
        }
    }
}
```

Or include both in the URL:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://django:mysecret@localhost:6379/0",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
        }
    }
}
```

### Non-URL-Safe Passwords

For passwords with special characters, use the `PASSWORD` option:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
            "PASSWORD": "my$ecret!password"
        }
    }
}
```

!!! note
    The `PASSWORD` option is ignored if a password is already in the URL.

## Timeouts

### Socket Timeouts

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "SOCKET_CONNECT_TIMEOUT": 5,  # Connection timeout in seconds
            "SOCKET_TIMEOUT": 5,          # Read/write timeout in seconds
        }
    }
}
```

### Infinite Timeout

django-cachex supports infinite timeouts:

- `timeout=0` - Expires immediately
- `timeout=None` - Never expires

```python
cache.set("key", "value", timeout=None)
```

## Exception Handling

### Ignore Exceptions (Memcached-like behavior)

To ignore connection exceptions (useful when Redis is optional):

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "IGNORE_EXCEPTIONS": True,
        }
    }
}
```

Or globally:

```python
DJANGO_REDIS_IGNORE_EXCEPTIONS = True
```

### Log Ignored Exceptions

```python
DJANGO_REDIS_LOG_IGNORED_EXCEPTIONS = True
DJANGO_REDIS_LOGGER = "some.specified.logger"  # Optional, defaults to __name__
```

## Connection Pools

### Configure Pool Size

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CONNECTION_POOL_KWARGS": {"max_connections": 100}
        }
    }
}
```

### Additional Pool Options

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CONNECTION_POOL_KWARGS": {
                "max_connections": 100,
                "retry_on_timeout": True
            }
        }
    }
}
```

### Custom Connection Pool Class

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CONNECTION_POOL_CLASS": "myproj.mypool.MyOwnPool",
        }
    }
}
```

### Verify Pool Connections

```python
from django_cachex import get_redis_connection

r = get_redis_connection("default")
connection_pool = r.connection_pool
print(f"Created connections: {connection_pool._created_connections}")
```

## Connection Closing

By default, connections are kept open. To close connections:

```python
# Per-cache setting
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLOSE_CONNECTION": True,
        }
    }
}

# Or globally
DJANGO_REDIS_CLOSE_CONNECTION = True
```

## SSL/TLS

### Basic SSL Connection

Use `rediss://` scheme:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "rediss://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
        }
    }
}
```

### Self-Signed Certificates

To disable certificate verification:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "rediss://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
            "CONNECTION_POOL_KWARGS": {"ssl_cert_reqs": None}
        }
    }
}
```
