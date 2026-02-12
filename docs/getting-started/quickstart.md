# Quick Start

## Configure as Cache Backend

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
    }
}
```

## Backend Classes

| Backend | Description |
|---------|-------------|
| `ValkeyCache` | Standard Valkey connection |
| `RedisCache` | Standard Redis connection |
| `ValkeySentinelCache` | Valkey Sentinel high availability |
| `RedisSentinelCache` | Redis Sentinel high availability |
| `ValkeyClusterCache` | Valkey Cluster sharding |
| `RedisClusterCache` | Redis Cluster sharding |

!!! note "Valkey and Redis Compatibility"
    Valkey and Redis are protocol-compatible, so either backend works with either server.
    Valkey is recommended as it remains fully open source.

## Connection URL Formats

Uses the valkey-py/redis-py native URL notation:

- `valkey://[[username]:[password]]@localhost:6379/0` - Valkey TCP connection
- `redis://[[username]:[password]]@localhost:6379/0` - Redis TCP connection
- `valkeys://[[username]:[password]]@localhost:6379/0` - Valkey SSL/TLS connection
- `rediss://[[username]:[password]]@localhost:6379/0` - Redis SSL/TLS connection
- `unix://[[username]:[password]]@/path/to/socket.sock?db=0` - Unix socket

### Database Selection

Two ways to specify the database number:

1. Query string: `valkey://localhost?db=0`
2. Path (for `valkey://` or `redis://` scheme): `valkey://localhost/0`

## Configure as Session Backend

Use django-cachex as a session backend:

```python
SESSION_ENGINE = "django.contrib.sessions.backends.cache"
SESSION_CACHE_ALIAS = "default"
```

## Basic Usage

```python
from django.core.cache import cache

# Standard Django cache methods
cache.set("key", "value", timeout=300)
value = cache.get("key")

# Extended data structure methods
cache.hset("user:1", "name", "Alice")
cache.zrange("leaderboard", 0, 10)

# Async versions (standard Django methods)
await cache.aget("key")
await cache.aset("key", "value", timeout=300)
```

## Raw Client Access

For operations not exposed by the cache interface:

```python
client = cache.get_client()
client.publish("channel", "message")
```
