# Async Support

django-cachex implements Django's async cache methods (`aget`, `aset`, `adelete`, etc.) using native async clients from `redis.asyncio` and `valkey.asyncio`, providing true async operations without thread pool overhead.

## Overview

A single cache backend supports both sync and async operations simultaneously -- `cache.get()` and `await cache.aget()` work on the same backend with no separate configuration needed.

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

All standard Django cache methods have async equivalents with the `a` prefix:

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

django-cachex extended methods also have async versions at the client level (`cache._cache`):

```python
client = cache._cache  # Access the cache client for extended async methods

# TTL operations
ttl = await client.attl(key)             # Get TTL in seconds
pttl = await client.apttl(key)           # Get TTL in milliseconds
await client.aexpire(key, timeout=60)    # Set expiration
await client.apersist(key)               # Remove expiration

# Key operations
keys = await client.akeys("pattern:*")
await client.adelete_pattern("session:*")
await client.arename(key, new_key)

# Iterate keys (memory-efficient)
async for key in client.aiter_keys("user:*"):
    print(key)
```

!!! note "Key prefixing"
    Client-level methods operate on raw (already-prefixed) keys. Use `cache.make_and_validate_key()` to prefix keys when calling client methods directly.

### Data Structures

Data structure operations have async equivalents at the client level:

```python
client = cache._cache

# Hashes
await client.ahset(key, "name", "Alice")
name = await client.ahget(key, "name")
user = await client.ahgetall(key)

# Lists
await client.alpush(key, "item")
item = await client.alpop(key)
items = await client.alrange(key, 0, -1)

# Sets
await client.asadd(key, "python", "django")
members = await client.asmembers(key)
is_member = await client.asismember(key, "python")

# Sorted Sets
await client.azadd(key, {"alice": 100, "bob": 85})
rank = await client.azrank(key, "alice")
top_players = await client.azrange(key, 0, 9, withscores=True)
```

## How It Works

### Connection Pool Architecture

Separate connection pools are maintained for sync and async operations:

- **Sync pools**: Standard pools (`redis.ConnectionPool` / `valkey.ConnectionPool`), one per server
- **Async pools**: Async pools (`redis.asyncio.ConnectionPool` / `valkey.asyncio.ConnectionPool`), cached per event loop

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

Async pools are stored in a `WeakKeyDictionary` keyed by event loop, providing automatic cleanup when loops are garbage collected, thread safety (each loop gets its own pools), and connection reuse within the same loop.

## Performance Considerations

!!! warning "Event Loop Lifecycle"
    Async pools are cached **per event loop**. This is efficient for long-lived loops but wasteful for short-lived ones.

### Efficient: Long-Lived Event Loops

ASGI servers (uvicorn, daphne, hypercorn) maintain long-lived event loops where connections are reused across requests:

```python
# In an ASGI application - efficient!
async def my_view(request):
    # Connections are reused across requests
    value = await cache.aget("key")
    await cache.aset("key", "new_value")
    return JsonResponse({"value": value})
```

### Inefficient: Short-Lived Event Loops

Avoid async methods when event loops are frequently created and destroyed:

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

Provide a custom async connection pool class:

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

```python
await cache.aclose()
```

Closes the async connection pool for the current event loop. Sync pools remain open.

## Mixed Sync/Async Usage

A single backend works for both sync and async code:

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

Both views use the same cache backend configured in settings.

## Cluster and Sentinel

Async works identically with Cluster and Sentinel backends:

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
    # Get top 10 from sorted set (descending by score)
    top_players = cache.zrevrange(
        "game:leaderboard",
        0, 9,
        withscores=True,
    )

    return JsonResponse({"leaderboard": top_players})
```
