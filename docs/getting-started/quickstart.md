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

All backends live in `django_cachex.cache`. The [configuration reference](../user-guide/configuration.md#backend-classes) has the details.

| Backend | Description |
|---------|-------------|
| `ValkeyCache` / `RedisCache` | Standalone (valkey-py / redis-py) |
| `ValkeySentinelCache` / `RedisSentinelCache` | Sentinel high availability (valkey-py / redis-py) |
| `ValkeyClusterCache` / `RedisClusterCache` | Cluster sharding (valkey-py / redis-py) |
| `ValkeyGlideCache` / `ValkeyGlideClusterCache` | Standalone / cluster through valkey-glide (`valkey-glide` extra, experimental) |
| `LocMemCache` / `DatabaseCache` | Drop-in replacements for Django's backends of the same name |
| `TrackingCache` | Local read cache over a Redis or Valkey alias, kept coherent by `CLIENT TRACKING` |

Valkey and Redis are protocol-compatible, so either backend works with either server. The redis-py backends reject `valkey://` and `valkeys://` URLs, though: the first cache call raises `ValueError`.

## Connection URL Formats

`LOCATION` uses the valkey-py and redis-py URL notation:

| URL | Connection |
|-----|------------|
| `valkey://[[username]:[password]]@localhost:6379/0` | Valkey TCP |
| `redis://[[username]:[password]]@localhost:6379/0` | Redis TCP |
| `valkeys://[[username]:[password]]@localhost:6379/0` | Valkey SSL/TLS |
| `rediss://[[username]:[password]]@localhost:6379/0` | Redis SSL/TLS |
| `unix://[[username]:[password]]@/path/to/socket.sock?db=0` | Unix socket |

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

# The driver client, for commands the cache does not wrap
client = cache.get_client()
client.publish("channel", "message")
```
