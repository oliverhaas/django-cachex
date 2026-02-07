# Async Support

django-cachex provides full async support for all cache operations, enabling efficient use with Django's async views and ASGI servers.

## Overview

Django 4.0+ added async methods to the cache framework with the `a` prefix (e.g., `aget`, `aset`, `adelete`). django-cachex implements these using native async clients from `redis.asyncio` and `valkey.asyncio`, providing true async operations without thread pool overhead.

A single cache backend instance supports both sync and async operations simultaneously. This mirrors Django's approach where `cache.get()` and `await cache.aget()` work on the same backend - no separate "async backend" configuration needed.

## Basic Usage

```python
from django.core.cache import cache

# Async views (ASGI)
async def my_view(request):
    # Read
    value = await cache.aget("key")

    # Write
    await cache.aset("key", "value", timeout=300)

    # Delete
    await cache.adelete("key")

    # Check existence
    exists = await cache.ahas_key("key")

    return JsonResponse({"value": value})
```

## Available Async Methods

### Standard Django Cache Methods

All standard Django cache methods have async equivalents:

| Sync | Async |
|------|-------|
| `get(key)` | `aget(key)` |
| `set(key, value, timeout)` | `aset(key, value, timeout)` |
| `add(key, value, timeout)` | `aadd(key, value, timeout)` |
| `delete(key)` | `adelete(key)` |
| `get_many(keys)` | `aget_many(keys)` |
| `set_many(mapping, timeout)` | `aset_many(mapping, timeout)` |
| `delete_many(keys)` | `adelete_many(keys)` |
| `has_key(key)` | `ahas_key(key)` |
| `incr(key, delta)` | `aincr(key, delta)` |
| `decr(key, delta)` | `adecr(key, delta)` |
| `touch(key, timeout)` | `atouch(key, timeout)` |
| `clear()` | `aclear()` |
| `close()` | `aclose()` |
| `get_or_set(key, default, timeout)` | `aget_or_set(key, default, timeout)` |
| `incr_version(key)` | `aincr_version(key)` |
| `decr_version(key)` | `adecr_version(key)` |

### Extended Methods

django-cachex extended methods also have async versions:

```python
# TTL operations
ttl = await cache.attl("key")           # Get TTL in seconds
pttl = await cache.apttl("key")         # Get TTL in milliseconds
await cache.aexpire("key", timeout=60)  # Set expiration
await cache.apersist("key")             # Remove expiration

# Key operations
keys = await cache.akeys("pattern:*")
await cache.adelete_pattern("session:*")
await cache.arename("old_key", "new_key")

# Iterate keys (memory-efficient)
async for key in cache.aiter_keys("user:*"):
    print(key)
```

### Data Structures

All data structure operations have async equivalents:

```python
# Hashes
await cache.ahset("user:1", "name", "Alice")
name = await cache.ahget("user:1", "name")
user = await cache.ahgetall("user:1")

# Lists
await cache.alpush("queue", "item")
item = await cache.alpop("queue")
items = await cache.alrange("queue", 0, -1)

# Sets
await cache.asadd("tags", "python", "django")
members = await cache.asmembers("tags")
is_member = await cache.asismember("tags", "python")

# Sorted Sets
await cache.azadd("leaderboard", {"alice": 100, "bob": 85})
rank = await cache.azrank("leaderboard", "alice")
top_players = await cache.azrange("leaderboard", 0, 9, withscores=True)

# Streams
entry_id = await cache.axadd("events", {"type": "login", "user": "alice"})
entries = await cache.axrange("events", "-", "+", count=10)
```

## How It Works

### Connection Pool Architecture

django-cachex maintains separate connection pools for sync and async operations:

- **Sync pools**: Standard connection pools (`redis.ConnectionPool` / `valkey.ConnectionPool`), one per server
- **Async pools**: Async connection pools (`redis.asyncio.ConnectionPool` / `valkey.asyncio.ConnectionPool`), cached per event loop

```
┌─────────────────────────────────────────────────────────┐
│                    Cache Backend                         │
├─────────────────────────────────────────────────────────┤
│  Sync Pools                 │  Async Pools              │
│  ────────────               │  ────────────             │
│  pools[0] → server1         │  loop1 → {0: pool, ...}   │
│  pools[1] → server2         │  loop2 → {0: pool, ...}   │
│  ...                        │  ...                      │
└─────────────────────────────────────────────────────────┘
```

### Per-Event-Loop Caching

Async pools are stored in a `WeakKeyDictionary` keyed by event loop. This provides:

1. **Automatic cleanup**: When an event loop is garbage collected, its pools are automatically cleaned up
2. **Thread safety**: Each event loop gets its own set of pools
3. **Connection reuse**: Within the same event loop, connections are efficiently reused

## Performance Considerations

!!! warning "Event Loop Lifecycle"
    Async pool connections are cached **per event loop**. This works efficiently for long-lived event loops but has implications for short-lived ones.

### Efficient: Long-Lived Event Loops

ASGI servers (uvicorn, daphne, hypercorn) maintain long-lived event loops, making async operations very efficient:

```python
# In an ASGI application - efficient!
async def my_view(request):
    # Connections are reused across requests
    value = await cache.aget("key")
    await cache.aset("key", "new_value")
    return JsonResponse({"value": value})
```

### Inefficient: Short-Lived Event Loops

Avoid async cache methods when event loops are frequently created and destroyed:

```python
# BAD: Each asyncio.run() creates a new event loop = new connection pool
def sync_function():
    for i in range(100):
        # Creates 100 connection pools!
        asyncio.run(cache.aget(f"key:{i}"))

# BAD: sync_to_async may create temporary event loops
@sync_to_async
def wrapped_function():
    # May not reuse connections efficiently
    pass
```

### Recommendations

| Context | Recommendation |
|---------|----------------|
| ASGI views (uvicorn, daphne) | Use async methods (`aget`, `aset`, etc.) |
| WSGI views (gunicorn, uwsgi) | Use sync methods (`get`, `set`, etc.) |
| Management commands | Use sync methods |
| Celery tasks | Use sync methods |
| Background tasks with persistent loop | Use async methods |

## Configuration

### Custom Async Pool Class

You can provide a custom async connection pool class:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            # Custom async pool class (import path or class)
            "async_pool_class": "myapp.pools.CustomAsyncConnectionPool",
        }
    }
}
```

### Closing Async Connections

To explicitly close async connections (e.g., during shutdown):

```python
await cache.aclose()
```

This closes the async connection pool for the current event loop. The sync pools remain open.

## Mixed Sync/Async Usage

A single cache backend works for both sync and async code:

```python
from django.core.cache import cache

# Sync code path
def sync_view(request):
    value = cache.get("key")           # Uses sync pool
    cache.set("key", "value")
    return HttpResponse(value)

# Async code path
async def async_view(request):
    value = await cache.aget("key")    # Uses async pool
    await cache.aset("key", "value")
    return JsonResponse({"value": value})
```

Both views can use the same cache backend configured in settings - no separate configuration needed.

## Cluster and Sentinel

Async support is available for all backend types:

```python
# Cluster - async works the same way
async def cluster_example():
    await cache.aset("key", "value")
    await cache.aget_many(["key1", "key2", "key3"])

# Sentinel - async works the same way
async def sentinel_example():
    await cache.aset("key", "value")
    value = await cache.aget("key")
```

## Complete Example

```python
# settings.py
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "TIMEOUT": 300,
        "OPTIONS": {
            "max_connections": 50,
        }
    }
}

# views.py
from django.core.cache import cache
from django.http import JsonResponse

async def user_profile(request, user_id):
    cache_key = f"user:{user_id}:profile"

    # Try cache first
    profile = await cache.aget(cache_key)

    if profile is None:
        # Cache miss - fetch from database
        profile = await get_user_profile_from_db(user_id)
        await cache.aset(cache_key, profile, timeout=3600)

    return JsonResponse(profile)

async def leaderboard(request):
    # Get top 10 from sorted set
    top_players = await cache.azrange(
        "game:leaderboard",
        0, 9,
        desc=True,
        withscores=True
    )

    return JsonResponse({"leaderboard": top_players})
```
