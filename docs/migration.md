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

All existing Django cache options work unchanged. Two behaviours differ:

- `incr()` and `decr()` on a missing key create it at `delta` on the Valkey/Redis backends (Redis `INCRBY` semantics) where Django's `RedisCache` raises `ValueError`. Code that relied on the `ValueError` to detect an expired counter should check `has_key()` first. `LocMemCache` and `DatabaseCache` keep Django's behaviour.
- `clear()` deletes this alias's keys only (a pattern delete over `KEY_PREFIX` and `VERSION`), not the whole database. `flush_db()` is the `FLUSHDB` equivalent.

## From django-valkey

```python
# Before
"BACKEND": "django_valkey.cache.ValkeyCache"
"OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"}

# After
"BACKEND": "django_cachex.cache.ValkeyCache"
```

Key changes:

| django-valkey | django-cachex |
|--------------|---------------|
| `CLIENT_CLASS` | Removed - use specific backend class |
| `SERIALIZER` | `serializer` (lowercase) |
| `COMPRESSOR` | `compressor` (lowercase) |
| `CONNECTION_POOL_CLASS` | `pool_class` |
| `CONNECTION_POOL_KWARGS` | Flat keys in `OPTIONS`; they are forwarded to the pool's `from_url()` |
| `PARSER_CLASS` | `parser_class` |
| `SENTINELS` / `SENTINEL_KWARGS` | `sentinels` / `sentinel_kwargs` |
| `get_valkey_connection()` | `cache.get_client()` |
| `cache.lock(key, timeout=30)` | `cache.lock(key, lease=30)`, see [Locks](#locks) |
| `django_valkey.serializers.json.JSONSerializer` | `django_cachex.serializers.json.JsonSerializer` |
| `django_valkey.serializers.msgpack.MSGPackSerializer` | `django_cachex.serializers.msgpack.MsgpackSerializer` |
| `django_valkey.compressors.zstd.ZStdCompressor` | `django_cachex.compressors.zstd.ZstdCompressor` |

Import paths: `django_valkey.*` → `django_cachex.*`, with the class-name
changes above. Any uppercase key left in `OPTIONS` is forwarded to the
driver's `from_url()` as is and fails at connect time with a `TypeError`.

For Sentinel: Use `django_cachex.cache.RedisSentinelCache` (or `ValkeySentinelCache`) instead of `CLIENT_CLASS`.

## From django-redis

```python
# Before
"BACKEND": "django_redis.cache.RedisCache"
"OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"}

# After
"BACKEND": "django_cachex.cache.RedisCache"
```

Key changes:

| django-redis | django-cachex |
|-------------|---------------|
| `CLIENT_CLASS` | Removed - use specific backend class |
| `SERIALIZER` | `serializer` (lowercase) |
| `COMPRESSOR` | `compressor` (lowercase) |
| `CONNECTION_POOL_CLASS` | `pool_class` |
| `CONNECTION_POOL_KWARGS` | Flat keys in `OPTIONS`; they are forwarded to the pool's `from_url()` |
| `PARSER_CLASS` | `parser_class` |
| `SENTINELS` / `SENTINEL_KWARGS` | `sentinels` / `sentinel_kwargs` |
| `get_redis_connection()` | `cache.get_client()` |
| `cache.lock(key, timeout=30)` | `cache.lock(key, lease=30)`, see [Locks](#locks) |
| `django_redis.serializers.json.JSONSerializer` | `django_cachex.serializers.json.JsonSerializer` |
| `django_redis.serializers.msgpack.MSGPackSerializer` | `django_cachex.serializers.msgpack.MsgpackSerializer` |
| `django_redis.compressors.zstd.ZStdCompressor` | `django_cachex.compressors.zstd.ZstdCompressor` |

Import paths: `django_redis.*` → `django_cachex.*`, with the class-name
changes above. A path-swapped `"django_cachex.serializers.json.JSONSerializer"`
raises `ImportError` at `caches[alias]`. Any uppercase key left in `OPTIONS` is
forwarded to the driver's `from_url()` as is and fails at connect time with a
`TypeError`.

For Sentinel: Use `django_cachex.cache.RedisSentinelCache` instead of `CLIENT_CLASS`.

## Behaviour differences

django-cachex differs from both libraries in these ways:

- `incr()` on a missing key creates it at `delta` (Redis `INCRBY` semantics) rather than raising.
- `clear()` is a pattern delete over this alias's `KEY_PREFIX` and `VERSION`, not `FLUSHDB`. Call `flush_db()` for `FLUSHDB`.
- `ttl()` of a missing key returns `-2`, of a key without expiry `None`.

### Locks

`cache.lock()` keeps the name but not the meaning of `timeout`. In
django-redis and django-valkey `timeout=30` is the TTL of the held lock,
forwarded to the driver's `client.lock(timeout=...)`. In django-cachex the
lock TTL is `lease`, and `timeout` is the keyword-only maximum time
`acquire()` waits before giving up. A mechanically migrated
`cache.lock(key, timeout=30)` therefore waits up to 30 seconds to acquire and
then holds the lock with no TTL, so a crashed holder blocks every peer until
the key is deleted by hand. Rename the argument:

```python
# django-redis / django-valkey
with cache.lock("job", timeout=30):
    ...

# django-cachex
with cache.lock("job", lease=30):
    ...
```

## New Features

Coming from Django's built-in backend, everything django-cachex adds is new:
data structures, TTL and pattern helpers, locks, semaphores, pipelines and
scripting. Coming from django-redis or django-valkey, which already have TTL
and pattern helpers and `cache.lock()`, you gain:

- Valkey and Redis in one package, redis-py, valkey-py and valkey-glide behind one API.
- Multi-serializer/compressor fallback for safe migrations.
- Pipelines via `cache.pipeline()` and `cache.apipeline()`, with key prefixing and serialization applied.
- Weighted semaphores via `cache.semaphore()`.
- Hash-field TTL (`hexpire()`, `hsetex()`, `hgetex()` and relatives).
- Lua scripting with key-prefixing and encoding hooks (`eval_script()`).
- Stampede prevention (`OPTIONS["stampede_prevention"]`).
