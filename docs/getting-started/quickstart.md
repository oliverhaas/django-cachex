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

All backends live in `django_cachex.cache`. See the [configuration reference](../user-guide/configuration.md#backend-classes) for the full table.

**Valkey / Redis (Python driver, default):**

| Backend | Description |
|---------|-------------|
| `ValkeyCache` / `RedisCache` | Standard connection |
| `ValkeySentinelCache` / `RedisSentinelCache` | Sentinel high availability |
| `ValkeyClusterCache` / `RedisClusterCache` | Cluster sharding |

**Other adapters:**

| Backend | Description |
|---------|-------------|
| `RedisRsCache` / `RedisRsSentinelCache` / `RedisRsClusterCache` | Rust driver (opt-in via `redis-rs` extra) |
| `ValkeyGlideCache` | valkey-glide (opt-in via `valkey-glide` extra) |
| `LocMemCache` | Drop-in replacement for Django's `LocMemCache` |
| `DatabaseCache` | Drop-in replacement for Django's `DatabaseCache` |
| `StreamCache` | In-memory cache synchronized via a Redis Stream |
| `TieredCache` | L1/L2 composite with TTL propagation |

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
