# Migration Guide

## From Django's Built-in Cache Backend

```python
# Before (Django's Redis backend)
"BACKEND": "django.core.cache.backends.redis.RedisCache"

# After (Valkey)
"BACKEND": "django_cachex.cache.ValkeyCache"

# Or (Redis)
"BACKEND": "django_cachex.cache.RedisCache"
```

All Django options work unchanged. You gain extended features (data structures, TTL ops, locking, compression, unified Valkey/Redis support).

## From django-valkey

```python
# Before
"BACKEND": "django_valkey.cache.ValkeyCache"
"OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"}

# After
"BACKEND": "django_cachex.cache.ValkeyCache"
```

**Key changes:**

| django-valkey | django-cachex |
|--------------|---------------|
| `CLIENT_CLASS` | Removed - use specific backend class |
| `SERIALIZER` | `serializer` (lowercase) |
| `COMPRESSOR` | `compressor` (lowercase) |
| `CONNECTION_POOL_CLASS` | `pool_class` |
| `get_valkey_connection()` | `cache.get_client()` |

Import paths: `django_valkey.*` → `django_cachex.*`

For Sentinel: Use `django_cachex.cache.ValkeySentinelCache` instead of `CLIENT_CLASS`.

## From django-redis

```python
# Before
"BACKEND": "django_redis.cache.RedisCache"
"OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"}

# After
"BACKEND": "django_cachex.cache.RedisCache"
```

**Key changes:**

| django-redis | django-cachex |
|-------------|---------------|
| `CLIENT_CLASS` | Removed - use specific backend class |
| `SERIALIZER` | `serializer` (lowercase) |
| `COMPRESSOR` | `compressor` (lowercase) |
| `CONNECTION_POOL_CLASS` | `pool_class` |
| `get_redis_connection()` | `cache.get_client()` |

Import paths: `django_redis.*` → `django_cachex.*`

For Sentinel: Use `django_cachex.cache.RedisSentinelCache` instead of `CLIENT_CLASS`.

## New Features

After migrating, you gain:

- **Unified Valkey + Redis support** in one package
- **Multi-serializer/compressor fallback** for safe migrations
- **Extended data structures** (hashes, lists, sets, sorted sets) directly on cache
- **TTL operations** (`ttl()`, `pttl()`, `expire()`, `persist()`)
- **Pattern operations** (`keys()`, `iter_keys()`, `delete_pattern()`)
- **Distributed locking** (`cache.lock()`)
- **Pipelines** (`cache.pipeline()`)
