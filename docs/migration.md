# Migration Guide

## From Django's Built-in Redis Backend

```python
# Before
"BACKEND": "django.core.cache.backends.redis.RedisCache"

# After
"BACKEND": "django_cachex.cache.RedisCache"
```

All Django options work unchanged. You gain extended features (data structures, TTL ops, locking, compression, Valkey support).

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

## From django-valkey

```python
# Before
"BACKEND": "django_valkey.cache.ValkeyCache"
"OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"}

# After
"BACKEND": "django_cachex.cache.ValkeyCache"
```

Same changes as django-redis above. Import paths: `django_valkey.*` → `django_cachex.*`

## New Features

After migrating, you gain:

- **Valkey + Redis support** in one package
- **Multi-serializer/compressor fallback** for safe migrations
- **Extended data structures** (hashes, lists, sets, sorted sets) directly on cache
- **TTL operations** (`ttl()`, `pttl()`, `expire()`, `persist()`)
- **Pattern operations** (`keys()`, `iter_keys()`, `delete_pattern()`)
- **Distributed locking** (`cache.lock()`)
- **Pipelines** (`cache.pipeline()`)
