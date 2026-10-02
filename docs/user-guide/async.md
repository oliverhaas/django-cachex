# Async Support

Django's cache methods and the django-cachex extensions have async twins with an `a` prefix, such as `aget()`, `attl()` and `ahset()`. The exceptions are `get_client()`, `info()`, `slowlog_get()` and `slowlog_len()`. `cache.get()` and `await cache.aget()` use the same alias, with no separate configuration. The API reference covers the twins under [Async Methods](../reference/api.md#async-methods).

```python
from django.core.cache import cache
from django.http import JsonResponse


async def user_profile(request, user_id):
    key = f"user:{user_id}:profile"
    profile = await cache.aget(key)
    if profile is None:
        profile = await get_user_profile_from_db(user_id)
        await cache.aset(key, profile, timeout=3600)
    return JsonResponse(profile)


async def leaderboard(request):
    # Top 10, highest score first
    top_players = await cache.azrevrange("game:leaderboard", 0, 9, withscores=True)
    return JsonResponse({"leaderboard": top_players})
```

## Backend Support

| Backend | The `a*` methods |
|---------|------------------|
| redis-py and valkey-py backends | Run natively on `redis.asyncio` or `valkey.asyncio` |
| valkey-glide backends | Run natively on glide's async `glide.GlideClient` |
| `LocMemCache` | Call the sync twin directly, without a thread. It does no I/O, so awaiting it from an event loop is harmless |
| `DatabaseCache` | Run the sync twin through `asgiref.sync.sync_to_async`, as Django's `BaseCache.aget()` does |
| Django's own backends | Django's methods only, without the django-cachex extensions |

The native clients do not go through the asgiref thread pool. The Cluster and Sentinel variants run their async methods the same way.

## Async Pipelines

```python
async with await cache.apipeline() as pipe:
    pipe.set("a", 1)
    pipe.hset("h", "field", "value")
    results = await pipe.execute()
```

Queueing methods such as `set()` and `hset()` are synchronous. Await only `apipeline()` and `execute()`, on every backend. The async pipeline otherwise behaves like the sync [`pipeline()`](../reference/api.md#pipelines).

## Event Loops and Connection Pools

On the redis-py and valkey-py backends, sync and async calls use separate connection pools. Sync calls use one pool per server, which every thread shares. Async calls use one pool per server and event loop, because connections belong to the loop that opened them. Each loop reuses its pools for every call.

!!! warning "Short-lived event loops"
    Each `asyncio.run()` starts a new event loop, so it opens a new pool and a new TCP connection. Use the sync methods where loops are short-lived.

| Context | Recommendation |
|---------|----------------|
| ASGI views (uvicorn, daphne) | Use async methods (`aget`, `aset`) |
| WSGI views (gunicorn, uwsgi) | Use sync methods (`get`, `set`) |
| Management commands | Use sync methods |
| Celery tasks | Use sync methods |
| Background tasks with persistent loop | Use async methods |

## Custom Async Pool Class

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

## Closing Async Connections

`await cache.aclose()` disconnects the async pools and clients the alias opened on the running loop. The next `await` opens new ones, so call `aclose()` when you are done with a loop, not between requests. Aliases with the same configuration share pools, so `aclose()` disconnects them too, including connections in use. They reconnect on their next command.

`close()` leaves the sync pools connected, because Django calls it on every `request_finished` signal. Calling either method is optional. django-cachex drops the pools of closed loops on every pool lookup and on `close()`.
