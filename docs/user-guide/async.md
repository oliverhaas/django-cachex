# Async Support

django-cachex implements Django's async cache methods with native async clients. The redis-py and valkey-py backends use `redis.asyncio` and `valkey.asyncio`, and the valkey-glide backends use glide's own async `glide.GlideClient`. Their async calls do not go through the asgiref thread pool. `cache.get()` and `await cache.aget()` use the same backend, with no separate configuration.

## Basic Usage

```python
from django.core.cache import cache


# Async views (ASGI)
async def my_view(request):
    value = await cache.aget("key")
    await cache.aset("key", "value", timeout=300)
    await cache.adelete("key")
    exists = await cache.ahas_key("key")
    return JsonResponse({"value": value})
```

## Available Async Methods

Each standard Django cache method has an async twin with an `a` prefix, such as `aget()`, `aget_or_set()`, `aincr_version()` and `aclose()`.

### Extended Methods

The django-cachex extensions have async twins on the cache too:

```python
# TTL operations
ttl = await cache.attl(key)  # seconds
pttl = await cache.apttl(key)  # milliseconds
await cache.aexpire(key, timeout=60)
await cache.apersist(key)  # remove the expiry

# Key operations
keys = await cache.akeys("pattern:*")
await cache.adelete_pattern("session:*")
await cache.arename(key, new_key)

# Iterate keys (memory-efficient)
async for key in cache.aiter_keys("user:*"):
    print(key)
```

Methods on `cache.adapter`, such as `await cache.adapter.aget(raw_key)`, skip key prefixing and serialization. They take already-prefixed keys and return raw bytes or values.

### Data Structures

The data structure methods have async twins as well:

```python
# Hashes
await cache.ahset(key, "name", "Alice")
name = await cache.ahget(key, "name")
user = await cache.ahgetall(key)

# Lists
await cache.alpush(key, "item")
item = await cache.alpop(key)
items = await cache.alrange(key, 0, -1)

# Sets
await cache.asadd(key, "python", "django")
members = await cache.asmembers(key)
is_member = await cache.asismember(key, "python")

# Sorted Sets
await cache.azadd(key, {"alice": 100, "bob": 85})
rank = await cache.azrank(key, "alice")
top_players = await cache.azrange(key, 0, 9, withscores=True)
```

### Async Pipelines

An async pipeline sends the queued commands in one round trip:

```python
async with await cache.apipeline() as pipe:
    pipe.set("a", 1)
    pipe.set("b", 2)
    pipe.hset("h", "field", "value")
    results = await pipe.execute()
```

Queueing methods such as `set`, `hset` and `lpush` are synchronous. Only `apipeline()` and `execute()` are awaited. Otherwise the async pipeline behaves like the sync `pipeline()`, with the commands listed under [Pipelines](../reference/api.md#pipelines).

`apipeline()` must be awaited on every backend, because the valkey-glide adapter creates its async client asynchronously.

## How It Works

On the redis-py and valkey-py backends, sync and async calls use separate connection pools. Sync calls use one `redis.ConnectionPool` or `valkey.ConnectionPool` per server. Async calls use a `redis.asyncio.ConnectionPool` or `valkey.asyncio.ConnectionPool` per server and event loop.

Each event loop gets its own async pools and reuses them for every call it makes. A pool cannot outlive its loop, because its connections belong to the loop that opened them. On every pool lookup and on `close()`, django-cachex drops the pools of loops that have closed. The sockets of a dropped pool close when the garbage collector reclaims their connections.

## Performance Considerations

!!! warning "Event Loop Lifecycle"
    Async pools are cached per event loop. That suits long-lived loops and wastes connections on short-lived ones.

### Efficient: Long-Lived Event Loops

ASGI servers such as uvicorn, daphne and hypercorn run long-lived event loops, so connections are reused across requests:

```python
# In an ASGI application, the loop, and so the pool, outlives the request
async def my_view(request):
    value = await cache.aget("key")
    await cache.aset("key", "new_value")
    return JsonResponse({"value": value})
```

### Inefficient: Short-Lived Event Loops

Avoid async methods when event loops are created and closed often:

```python
# Each asyncio.run() starts a new event loop, so a new pool and a new TCP
# connection. The pools do not pile up, because each run sweeps out the
# previous one, but this costs 100 handshakes instead of one.
def sync_function():
    for i in range(100):
        asyncio.run(cache.aget(f"key:{i}"))


# async_to_sync() inside a sync_to_async() body schedules on the outer loop,
# so the pool is reused, but every call hops threads twice. Await
# cache.aget() directly instead.
@sync_to_async
def wrapped_function():
    return async_to_sync(cache.aget)("key")
```

### Recommendations

| Context | Recommendation |
|---------|----------------|
| ASGI views (uvicorn, daphne) | Use async methods (`aget`, `aset`) |
| WSGI views (gunicorn, uwsgi) | Use sync methods (`get`, `set`) |
| Management commands | Use sync methods |
| Celery tasks | Use sync methods |
| Background tasks with persistent loop | Use async methods |

## Configuration

### Custom Async Pool Class

`async_pool_class` sets the async connection pool class, as an import path or a class:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "async_pool_class": "myapp.pools.CustomAsyncConnectionPool",
        },
    }
}
```

### Closing Async Connections

```python
await cache.aclose()
```

`aclose()` disconnects and drops the async pools this cache alias opened on the running loop. The next `await` on the cache opens new ones, so call it when you are done with a loop, not between requests. It also drops the pools of loops that have closed. On a cluster backend, it closes the loop's cluster client. On a Sentinel backend, it also closes the loop's Sentinel manager and the clients it used for discovery.

An alias with a different URL or options keeps its connections. An alias with the same configuration shares the same pools, so it is disconnected too, including connections in use, and reconnects on its next command. If closing one pool fails, the pools not yet closed stay registered.

`close()` leaves the sync pools connected, because Django calls it on every `request_finished` signal and a teardown there would force a reconnect per request. It does drop the async pools of closed loops.

Calling either is optional. A process that runs one loop accumulates nothing. A process that creates loops, through `asyncio.run()` or `async_to_sync()` from a sync thread, drops the pools of closed loops with each new loop.

## Mixed Sync/Async Usage

One cache alias serves sync and async code:

```python
from django.core.cache import cache


# Sync code path
def sync_view(request):
    value = cache.get("key")  # Uses sync pool
    cache.set("key", "value")
    return HttpResponse(value)


# Async code path
async def async_view(request):
    value = await cache.aget("key")  # Uses async pool
    await cache.aset("key", "value")
    return JsonResponse({"value": value})
```

## Other backends

`LocMemCache` and `DatabaseCache` also have the async extensions, such as `alpush()`, `ahset()`, `azadd()`, `attl()` and `aexpire()`:

- `LocMemCache` keeps its data in memory, so each `a*` method calls its sync twin directly, without a thread. It does no I/O, so awaiting it from an event loop is harmless.
- `DatabaseCache` runs database queries, so each `a*` method runs its sync twin through `asgiref.sync.sync_to_async`, as Django's `BaseCache.aget()` does.

Django's own backends (`django.core.cache.backends.*`) and other non-cachex backends get no extensions from django-cachex. The admin marks them "limited" and shows only their configuration, without key browsing. It suggests a cachex `BACKEND` where one exists.

## Cluster and Sentinel

The async methods work the same on Cluster and Sentinel backends:

```python
# Cluster
async def cluster_example():
    await cache.aset("key", "value")
    await cache.aget_many(["key1", "key2", "key3"])


# Sentinel
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
    }
}

# views.py
from django.core.cache import cache
from django.http import JsonResponse


async def user_profile(request, user_id):
    cache_key = f"user:{user_id}:profile"
    profile = await cache.aget(cache_key)

    if profile is None:
        # Cache miss: load from the database
        profile = await get_user_profile_from_db(user_id)
        await cache.aset(cache_key, profile, timeout=3600)

    return JsonResponse(profile)


async def leaderboard(request):
    # Top 10, highest score first
    top_players = await cache.azrevrange(
        "game:leaderboard",
        0,
        9,
        withscores=True,
    )

    return JsonResponse({"leaderboard": top_players})
```
