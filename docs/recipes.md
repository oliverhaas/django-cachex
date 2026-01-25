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

## Multi-Tenant Caching

Isolate cache data per tenant using key prefixes:

```python
# settings.py
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/0",
        "KEY_PREFIX": "",  # We'll handle prefix ourselves
    }
}
```

```python
from django.core.cache import cache
from threading import local

_tenant = local()

def set_current_tenant(tenant_id: str) -> None:
    _tenant.id = tenant_id

def get_current_tenant() -> str:
    return getattr(_tenant, "id", "default")

def tenant_key(key: str) -> str:
    """Prefix key with current tenant."""
    return f"tenant:{get_current_tenant()}:{key}"

# Usage
set_current_tenant("acme-corp")
cache.set(tenant_key("settings"), {"theme": "dark"})
```

## Distributed Locking

Prevent concurrent execution of critical sections:

```python
from django.core.cache import cache
from contextlib import contextmanager

@contextmanager
def distributed_lock(name: str, timeout: int = 30):
    """Acquire a distributed lock.

    Args:
        name: Lock name
        timeout: Lock timeout in seconds

    Raises:
        RuntimeError: If lock cannot be acquired
    """
    lock = cache.lock(name, timeout=timeout, blocking_timeout=5)
    acquired = lock.acquire(blocking=True)
    if not acquired:
        raise RuntimeError(f"Could not acquire lock: {name}")
    try:
        yield
    finally:
        lock.release()

# Usage
with distributed_lock("process-payments"):
    process_pending_payments()
```

## Caching Database Queries

Cache expensive queries with automatic invalidation:

```python
from django.core.cache import cache
from django.db.models.signals import post_save, post_delete
from functools import wraps

def cached_query(timeout: int = 300):
    """Decorator to cache query results."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Build cache key from function name and arguments
            key = f"query:{func.__name__}:{hash((args, tuple(sorted(kwargs.items()))))}"
            result = cache.get(key)
            if result is None:
                result = func(*args, **kwargs)
                cache.set(key, result, timeout=timeout)
            return result
        return wrapper
    return decorator

@cached_query(timeout=600)
def get_active_products(category_id: int):
    return list(Product.objects.filter(
        category_id=category_id,
        is_active=True,
    ).values("id", "name", "price"))

# Invalidate on model changes
def invalidate_product_cache(sender, instance, **kwargs):
    cache.delete_pattern("query:get_active_products:*")

post_save.connect(invalidate_product_cache, sender=Product)
post_delete.connect(invalidate_product_cache, sender=Product)
```

## Development and Testing Without a Server

!!! tip "Recommendation: Use a real container"
    We recommend using a Valkey or Redis container even for development/testing.
    They're lightweight (~10MB memory) and give you accurate behavior:

    ```bash
    docker run -d --name valkey -p 6379:6379 valkey/valkey:8
    ```

### Using fakeredis

For situations where a container isn't practical, [fakeredis](https://github.com/cunla/fakeredis-py)
provides an in-memory fake that implements most Valkey/Redis commands.

Configure via connection pool class:

```python
# settings.py (development/testing)
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

Or for pytest with more control:

```python
# conftest.py
import pytest

@pytest.fixture
def fake_cache_settings(settings):
    """Configure fakeredis for tests."""
    settings.CACHES = {
        "default": {
            "BACKEND": "django_cachex.cache.RedisCache",
            "LOCATION": "redis://localhost:6379/0",
            "OPTIONS": {
                "pool_class": "fakeredis.FakeConnectionPool",
            },
        }
    }
```

### Using Django's LocMemCache

For simple tests that don't need Valkey/Redis-specific features:

```python
# tests/settings.py
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    }
}
```

!!! warning "Limitations"
    `LocMemCache` doesn't support extended operations (lists, sets, hashes, sorted sets),
    pipelines, or distributed locking. Use fakeredis or a real server for those.
