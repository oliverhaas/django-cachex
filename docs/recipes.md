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

    with cache.client.get_client(write=True) as client:
        pipe = client.pipeline()
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

Prevent concurrent execution of critical sections:

```python
from django.core.cache import cache

with cache.lock("process-payments", timeout=30):
    process_pending_payments()
```

## Development Without a Server

For simple local development without running a server, [fakeredis](https://github.com/cunla/fakeredis-py) provides an in-memory implementation:

```python
# settings_dev.py
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://localhost:6379/0",
        "OPTIONS": {
            "pool_class": "fakeredis.FakeConnectionPool",
        },
    }
}
```

!!! tip "For testing"
    django-cachex uses [testcontainers](https://testcontainers.com/) for its test suite.
    Consider using the same approach for accurate behavior in your tests.
