# Recipes

Practical solutions for common caching scenarios.

## Session Storage

Use Valkey/Redis for Django sessions:

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

For dedicated session storage with longer TTL:

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

Simple rate limiter using sorted sets:

```python
import time
from django.core.cache import cache

def is_rate_limited(user_id: str, limit: int = 100, window: int = 60) -> bool:
    """Check if user has exceeded rate limit.

    Args:
        user_id: Unique identifier for the user
        limit: Maximum requests allowed in window
        window: Time window in seconds

    Returns:
        True if rate limited, False otherwise
    """
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

Delete all keys matching a pattern:

```python
from django.core.cache import cache

# Delete all user-related cache entries
cache.delete_pattern("user:*")

# Delete all cached API responses
cache.delete_pattern("api:*:response")
```

### Versioned cache keys

Invalidate entire cache groups by incrementing version:

```python
from django.core.cache import cache

def get_user_cache_version(user_id: int) -> int:
    """Get current cache version for a user."""
    return cache.get(f"user:{user_id}:version", 1)

def invalidate_user_cache(user_id: int) -> None:
    """Invalidate all cached data for a user."""
    cache.incr(f"user:{user_id}:version")

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

## Distributed Locking

Prevent concurrent execution of critical sections. `lease` is the TTL of
the held lock (auto-released if the holder crashes); pass `timeout` to
`acquire()` for the maximum time to wait before giving up:

```python
from django.core.cache import cache

with cache.lock("process-payments", lease=30):
    process_pending_payments()

# Or, to bound how long we wait for the lock:
lock = cache.lock("process-payments", lease=30)
if lock.acquire(timeout=5):
    try:
        process_pending_payments()
    finally:
        lock.release()
```

## Gate Memory-Heavy Work With a Weighted Semaphore

When a worker pod has a limited memory budget but multiple task types compete for it, a weighted semaphore lets each caller declare how much it intends to consume. Big tasks block when the budget can't accommodate them; small tasks slip through whenever there's room.

```python
from django.core.cache import cache

# Stay under 500 MB across all callers; this task uses ~100 MB.
with cache.semaphore("memory-pool", weight=100, capacity=500, lease=300):
    convert_huge_image(...)
```

If `acquire()` should give up after some bounded wait, pass `timeout`:

```python
from django_cachex import SemaphoreTimeoutError

try:
    with cache.semaphore(
        "memory-pool", weight=100, capacity=500, lease=300, timeout=10,
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

On RESP backends, `lease` is required and acts as a TTL on the held claim: if the worker crashes mid-task, the next acquirer reclaims the budget after the lease expires. Use `sem.extend(seconds)` to bump the TTL for tasks that may legitimately exceed their original lease.

## Development Without a Server

For local development without a running server, use `LocMemCache`:

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
with the full data-structure surface (`hset`, `lpush`, `zadd`, …), TTL
helpers, and admin support. See
[LocMemCache vs fakeredis](development/locmem-vs-fakeredis.md) for the
performance comparison and rationale.

!!! tip "For testing"
    django-cachex uses [testcontainers](https://testcontainers.com/) for its test suite.
    Consider using the same approach for accurate behavior in your tests.
