# Redis Sentinel

django-cachex includes built-in support for [Redis Sentinel](https://redis.io/topics/sentinel) for high availability.

## Basic Setup

Enable the Sentinel connection factory:

```python
DJANGO_REDIS_CONNECTION_FACTORY = "django_cachex.pool.SentinelConnectionFactory"

SENTINELS = [
    ("sentinel-1", 26379),
    ("sentinel-2", 26379),
    ("sentinel-3", 26379),
]

CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://service_name/db",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.SentinelClient",
            "SENTINELS": SENTINELS,
        },
    },
}
```

## Full Configuration Example

```python
DJANGO_REDIS_CONNECTION_FACTORY = "django_cachex.pool.SentinelConnectionFactory"

SENTINELS = [
    ("sentinel-1", 26379),
    ("sentinel-2", 26379),
    ("sentinel-3", 26379),
]

CACHES = {
    # Full configuration with SentinelClient
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://service_name/db",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.SentinelClient",
            "SENTINELS": SENTINELS,
            "SENTINEL_KWARGS": {},  # Optional kwargs for Sentinel
            "CONNECTION_POOL_CLASS": "redis.sentinel.SentinelConnectionPool",
        },
    },

    # Minimal example with SentinelClient
    "minimal": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://minimal_service_name/db",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.SentinelClient",
            "SENTINELS": SENTINELS,
        },
    },

    # Using DefaultClient with primary/replica
    "other": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": [
            "redis://other_service_name/db?is_master=1",
            "redis://other_service_name/db?is_master=0",
        ],
        "OPTIONS": {"SENTINELS": SENTINELS},
    },

    # Read-only replicas
    "readonly": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://readonly_service_name/db?is_master=0",
        "OPTIONS": {"SENTINELS": SENTINELS},
    },
}
```

## Mixed Configuration

You can use both Sentinel and non-Sentinel caches:

```python
SENTINELS = [
    ("sentinel-1", 26379),
    ("sentinel-2", 26379),
    ("sentinel-3", 26379),
]

CACHES = {
    # Sentinel-based cache
    "sentinel": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://service_name/db",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.SentinelClient",
            "SENTINELS": SENTINELS,
            "CONNECTION_POOL_CLASS": "redis.sentinel.SentinelConnectionPool",
            "CONNECTION_FACTORY": "django_cachex.pool.SentinelConnectionFactory",
        },
    },

    # Standard Redis cache
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_cachex.client.DefaultClient",
        },
    },
}
```

## URL Parameters

When using Sentinel with the DefaultClient:

- `is_master=1` - Connect to primary
- `is_master=0` - Connect to replica
