# Recipes

## Session Storage

Store Django sessions in Valkey or Redis:

```python
# settings.py
SESSION_ENGINE = "django.contrib.sessions.backends.cache"
SESSION_CACHE_ALIAS = "default"

CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/0",
    }
}
```

To give sessions their own alias and a longer TTL:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/0",
    },
    "sessions": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "TIMEOUT": 86400 * 14,  # 2 weeks
    },
}

SESSION_CACHE_ALIAS = "sessions"
```

## Rate Limiting

A rate limiter on a sorted set:

```python
import time
from django.core.cache import cache


def is_rate_limited(user_id: str, limit: int = 100, window: int = 60) -> bool:
    """Record one request and return True if the user is over the limit."""
    key = f"ratelimit:{user_id}"
    now = time.time()
    window_start = now - window

    with cache.pipeline() as pipe:
        # Remove old entries
        pipe.zremrangebyscore(key, 0, window_start)
        # Add current request
        pipe.zadd(key, {str(now): now})
        # Count requests in window
        pipe.zcard(key)
        # Set expiry
        pipe.expire(key, window)
        results = pipe.execute()

    count = results[2]
    return count > limit
```

## Cache Invalidation Patterns

### Pattern-based deletion

Delete every key that matches a pattern:

```python
from django.core.cache import cache

# Delete all user-related cache entries
cache.delete_pattern("user:*")

# Delete all cached API responses
cache.delete_pattern("api:*:response")
```

### Versioned cache keys

Invalidate a group of keys by incrementing a version counter. The counter is
created with `add()` before the increment. It has `timeout=None`, so it cannot
expire while the data keys it namespaces are alive:

```python
from django.core.cache import cache


def version_key(user_id: int) -> str:
    return f"user:{user_id}:version"


def get_user_cache_version(user_id: int) -> int:
    """Get current cache version for a user."""
    return cache.get(version_key(user_id), 1)


def invalidate_user_cache(user_id: int) -> None:
    """Invalidate all cached data for a user."""
    key = version_key(user_id)
    cache.add(key, 1, timeout=None)
    cache.incr(key)


def get_user_data(user_id: int) -> dict:
    """Get user data with versioned caching."""
    version = get_user_cache_version(user_id)
    key = f"user:{user_id}:data:v{version}"

    data = cache.get(key)
    if data is None:
        data = fetch_user_data_from_db(user_id)
        cache.set(key, data, timeout=3600)
    return data
```

On the Valkey and Redis backends, `incr()` on a missing key creates it at
`delta`. Without the `add()`, the first invalidation would leave the counter at
`1`, the value a missing key reads as, and the stale `v1` data would still be
served. On `LocMemCache` and `DatabaseCache`, `incr()` on a missing key raises
`ValueError`. With the counter created at `1` first, the increment moves it to
`2` on every backend.

## Distributed Locking

A lock keeps a critical section from running concurrently. `lease` is the TTL
of the held lock, so the lock is released if the holder crashes. `timeout` is
the longest time `acquire()` waits:

```python
from django.core.cache import cache

with cache.lock("process-payments", lease=30):
    process_pending_payments()

# Or bound the wait for the lock:
lock = cache.lock("process-payments", lease=30, timeout=5)
if lock.acquire():
    try:
        process_pending_payments()
    finally:
        lock.release()
```

On the redis-py and valkey-py backends, `cache.lock()` returns a wrapper around
the driver's lock. Its `acquire()` takes the driver's `blocking_timeout`
argument, and its failures raise `django_cachex.lock.LockError` with the
driver's error as `__cause__`. `timeout` on `cache.lock()` works on every
backend with locks.

## Gate Memory-Heavy Work With a Weighted Semaphore

A weighted semaphore shares a budget, such as a worker's memory, between tasks of different sizes. Each caller declares its weight and blocks while the budget has no room for it. Admission is FIFO. A waiting large task holds back the smaller tasks queued behind it, even when their weight would fit, so they cannot starve it.

```python
from django.core.cache import cache

# 500 MB budget across all callers. This task uses ~100 MB.
with cache.semaphore("memory-pool", weight=100, capacity=500, lease=300):
    convert_huge_image(...)
```

To bound the wait in `acquire()`, pass `timeout`:

```python
from django_cachex import SemaphoreTimeoutError

try:
    with cache.semaphore(
        "memory-pool",
        weight=100,
        capacity=500,
        lease=300,
        timeout=10,
    ):
        convert(...)
except SemaphoreTimeoutError:
    # Defer to a retry or fall back to a smaller pipeline.
    ...
```

For async tasks, use `cache.asemaphore`:

```python
async with await cache.asemaphore("memory-pool", weight=100, capacity=500, lease=300):
    await convert_async(...)
```

On the RESP backends, `lease` is required. It is the TTL of the held claim, so if a worker crashes mid-task, the next acquirer reclaims the budget after the lease expires. For a task that can run longer than its lease, call `sem.extend(seconds)`.

## Development Without a Server

For local development without a server, use `LocMemCache`:

```python
# settings_dev.py
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.LocMemCache",
        "LOCATION": "dev",
    }
}
```

`django_cachex.cache.LocMemCache` extends Django's built-in `LocMemCache`
with the hash, list, set and sorted-set commands (`hset`, `lpush`, `zadd` and
the rest), the `ttl()` / `expire()` / `persist()` helpers, and admin support.
It has no streams, locks, pipelines or Lua.
[Local backends](user-guide/configuration.md#local-backends) lists what it
supports.

!!! tip "For testing"
    django-cachex runs its test suite with [testcontainers](https://testcontainers.com/).
    Use it in your tests for accurate server behavior.
