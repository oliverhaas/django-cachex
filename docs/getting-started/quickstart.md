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

All backends live in `django_cachex.cache`. The valkey-py and redis-py backends are the default. The [configuration reference](../user-guide/configuration.md#backend-classes) has the full table.

| Backend | Description |
|---------|-------------|
| `ValkeyCache` / `RedisCache` | Standard connection (valkey-py / redis-py) |
| `ValkeySentinelCache` / `RedisSentinelCache` | Sentinel high availability (valkey-py / redis-py) |
| `ValkeyClusterCache` / `RedisClusterCache` | Cluster sharding (valkey-py / redis-py) |
| `ValkeyGlideCache` | Standard connection through valkey-glide (`valkey-glide` extra, experimental) |
| `ValkeyGlideClusterCache` | Cluster sharding through valkey-glide (same extra, experimental) |
| `LocMemCache` | Drop-in replacement for Django's `LocMemCache` |
| `DatabaseCache` | Drop-in replacement for Django's `DatabaseCache` |
| `TrackingCache` | Local read cache over a Redis or Valkey alias, kept coherent by `CLIENT TRACKING` |

Valkey and Redis are protocol-compatible, so either backend works with either server.
Prefer Valkey, which remains fully open source.

## Connection URL Formats

`LOCATION` uses the valkey-py and redis-py URL notation:

| URL | Connection |
|-----|------------|
| `valkey://[[username]:[password]]@localhost:6379/0` | Valkey TCP |
| `redis://[[username]:[password]]@localhost:6379/0` | Redis TCP |
| `valkeys://[[username]:[password]]@localhost:6379/0` | Valkey SSL/TLS |
| `rediss://[[username]:[password]]@localhost:6379/0` | Redis SSL/TLS |
| `unix://[[username]:[password]]@/path/to/socket.sock?db=0` | Unix socket |

### Database Selection

Set the database number in the query string (`valkey://localhost?db=0`) or, on every scheme except `unix://`, in the path (`valkey://localhost/0`).

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

For commands the cache does not wrap, use the driver client from `cache.get_client()`:

```python
client = cache.get_client()
client.publish("channel", "message")
```
