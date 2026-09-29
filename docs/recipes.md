# Recipes

## Session Storage

Store Django sessions on a dedicated Valkey or Redis alias:

```python
# settings.py
SESSION_ENGINE = "django.contrib.sessions.backends.cache"
SESSION_CACHE_ALIAS = "sessions"

CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/0",
    },
    "sessions": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
    },
}
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

To delete every key that matches a pattern, call `cache.delete_pattern("user:*")`.

To invalidate a group of keys, increment a version counter in their names:

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

The counter has `timeout=None`, so it cannot expire while its data keys are alive. `add()` creates it at `1` before `incr()`. Without `add()`, `incr()` on the Valkey and Redis backends creates the missing counter at `delta`. The version then stays `1`, and the stale `v1` data stays live. `LocMemCache` and `DatabaseCache` raise `ValueError` instead.

## Distributed Locking

A lock keeps a critical section from running concurrently. `lease` is the TTL of the held lock, so a crashed holder's lock expires. `timeout` is the longest time `acquire()` waits:

```python
from django.core.cache import cache

lock = cache.lock("process-payments", lease=30, timeout=5)
if lock.acquire():
    try:
        process_pending_payments()
    finally:
        lock.release()
```

The lock also works as a context manager. [Lock Interface](reference/api.md#lock-interface) covers the backend differences in `acquire()` and the lock errors.

## Gate Memory-Heavy Work With a Weighted Semaphore

A weighted semaphore shares a budget, such as a worker's memory, between tasks of different sizes. Each caller declares its weight and blocks while the budget has no room for it. Admission is FIFO, so a waiting large task holds back the smaller tasks queued behind it, even when they would fit.

```python
from django.core.cache import cache
from django_cachex import SemaphoreTimeoutError

try:
    # 500 MB budget across all callers. This task uses ~100 MB.
    with cache.semaphore(
        "memory-pool",
        weight=100,
        capacity=500,
        lease=300,
        timeout=10,  # longest wait in acquire()
    ) as sem:
        convert_huge_image(...)
except SemaphoreTimeoutError:
    # Defer to a retry or fall back to a smaller pipeline.
    ...
```

On the Valkey and Redis backends, `lease` is required. It is the TTL of the held claim, so a crashed worker's weight returns to the budget when the lease expires. Call `sem.extend(seconds)` for a task that can run longer than its lease. Async tasks use `async with await cache.asemaphore(...)`.

## Development Without a Server

Use `django_cachex.cache.LocMemCache`:

```python
# settings_dev.py
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.LocMemCache",
        "LOCATION": "dev",
    }
}
```

It extends Django's `LocMemCache` with the hash, list, set and sorted-set commands, the TTL helpers and admin support. It has no streams, locks, pipelines or Lua. [Local backends](user-guide/configuration.md#local-backends) lists what it supports.

!!! tip "For testing"
    Use [testcontainers](https://testcontainers.com/) in your tests for accurate server behavior.
