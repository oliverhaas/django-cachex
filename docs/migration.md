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

All Django cache options work unchanged. Two behaviors differ on the Valkey and Redis backends:

- `incr()` and `decr()` on a missing key start it from 0, like Redis `INCRBY`, where Django's `RedisCache` raises `ValueError`. Code that relies on the `ValueError` to detect an expired counter must check `has_key()` first. django-cachex's `LocMemCache` and `DatabaseCache` keep Django's behavior.
- `clear()` deletes only this alias's keys, by pattern over `KEY_PREFIX` and `VERSION`, not the whole database. `flush_db()` runs `FLUSHDB`.

## From django-valkey

```python
# Before
"BACKEND": "django_valkey.cache.ValkeyCache"
"OPTIONS": {"CLIENT_CLASS": "django_valkey.client.DefaultClient"}

# After
"BACKEND": "django_cachex.cache.ValkeyCache"
```

| django-valkey | django-cachex |
|--------------|---------------|
| `CLIENT_CLASS` | Removed. Use the backend class for the topology, such as `RedisSentinelCache` or `ValkeySentinelCache` for Sentinel. |
| `SERIALIZER` | `serializer` |
| `COMPRESSOR` | `compressor` |
| `CONNECTION_POOL_CLASS` | `pool_class` |
| `CONNECTION_POOL_KWARGS` | Flat keys in `OPTIONS`, forwarded to the pool's `from_url()` |
| `PARSER_CLASS` | `parser_class` |
| `SENTINELS` / `SENTINEL_KWARGS` | `sentinels` / `sentinel_kwargs` |
| `get_valkey_connection()` | `cache.get_client(write=True)` |
| `cache.lock(key, timeout=30)` | `cache.lock(key, lease=30)`, see [Differences](#differences-from-django-redis-and-django-valkey) |
| `django_valkey.serializers.json.JSONSerializer` | `django_cachex.serializers.json.JsonSerializer` |
| `django_valkey.serializers.msgpack.MSGPackSerializer` | `django_cachex.serializers.msgpack.MsgpackSerializer` |
| `django_valkey.compressors.zstd.ZStdCompressor` | `django_cachex.compressors.zstd.ZstdCompressor` |

Import paths change from `django_valkey.*` to `django_cachex.*`, with the class names above.

## From django-redis

```python
# Before
"BACKEND": "django_redis.cache.RedisCache"
"OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"}

# After
"BACKEND": "django_cachex.cache.RedisCache"
```

| django-redis | django-cachex |
|-------------|---------------|
| `CLIENT_CLASS` | Removed. Use the backend class for the topology, such as `RedisSentinelCache` for Sentinel. |
| `SERIALIZER` | `serializer` |
| `COMPRESSOR` | `compressor` |
| `CONNECTION_POOL_CLASS` | `pool_class` |
| `CONNECTION_POOL_KWARGS` | Flat keys in `OPTIONS`, forwarded to the pool's `from_url()` |
| `PARSER_CLASS` | `parser_class` |
| `SENTINELS` / `SENTINEL_KWARGS` | `sentinels` / `sentinel_kwargs` |
| `get_redis_connection()` | `cache.get_client(write=True)` |
| `cache.lock(key, timeout=30)` | `cache.lock(key, lease=30)`, see [Differences](#differences-from-django-redis-and-django-valkey) |
| `django_redis.serializers.json.JSONSerializer` | `django_cachex.serializers.json.JsonSerializer` |
| `django_redis.serializers.msgpack.MSGPackSerializer` | `django_cachex.serializers.msgpack.MsgpackSerializer` |
| `django_redis.compressors.zstd.ZStdCompressor` | `django_cachex.compressors.zstd.ZstdCompressor` |

Import paths change from `django_redis.*` to `django_cachex.*`, with the class names above.

## Differences from django-redis and django-valkey

- `incr()`, `decr()` and `clear()` differ in the same way as from [Django's built-in backend](#from-djangos-built-in-cache-backend).
- `ttl()` returns `-2` for a missing key and `None` for a key without expiry.
- `cache.lock()` takes the lock's TTL as `lease`, and its keyword-only `timeout` caps how long `acquire()` waits. An unchanged `cache.lock(key, timeout=30)` therefore holds the lock without a TTL. A crashed holder then blocks every peer until someone deletes the key by hand.

## From django-cachalot

The [ORM cache](user-guide/orm-cache.md) is derived from django-cachalot 2.9.1. Replace the app and uninstall cachalot, because running both patches the ORM twice:

```python
# Before
INSTALLED_APPS = [..., "cachalot"]

# After
INSTALLED_APPS = [..., "django_cachex.orm"]
```

### Settings

The settings move into one `CACHEX_ORM` dict, without the `CACHALOT_` prefix:

```python
# Before
CACHALOT_TIMEOUT = 3600
CACHALOT_UNCACHABLE_TABLES = frozenset(("django_migrations", "django_session"))

# After
CACHEX_ORM = {
    "TIMEOUT": 3600,
    "UNCACHABLE_TABLES": ("django_session",),
}
```

| django-cachalot | django-cachex |
|-----------------|---------------|
| `CACHALOT_ENABLED`, `CACHALOT_CACHE`, `CACHALOT_DATABASES`, `CACHALOT_ONLY_CACHABLE_TABLES`, `CACHALOT_ADDITIONAL_TABLES`, `CACHALOT_FINAL_SQL_CHECK` | The same key without the prefix |
| `CACHALOT_UNCACHABLE_TABLES` | `UNCACHABLE_TABLES`. `django_migrations` is never cached, so it can be left out. |
| `CACHALOT_TIMEOUT` | `TIMEOUT`, which defaults to the cache's default timeout, not `None` |
| `CACHALOT_ONLY_CACHABLE_APPS`, `CACHALOT_UNCACHABLE_APPS` | Removed. List the apps' tables, many-to-many tables included, in `ONLY_CACHABLE_TABLES` or `UNCACHABLE_TABLES`. |
| `CACHALOT_CACHE_RANDOM`, `CACHALOT_CACHE_ITERATORS`, `CACHALOT_INVALIDATE_RAW` | Removed. Random queries and the results of `iterator()` are never cached, and raw SQL writes always invalidate. |
| `CACHALOT_QUERY_KEYGEN`, `CACHALOT_TABLE_KEYGEN` | Removed. Keys go through the cache alias's `KEY_FUNCTION`. If it tells tenants apart, a write to a table they share invalidates only the writing tenant's results, so list shared tables in `UNCACHABLE_TABLES`. |
| `CACHALOT_USE_UNSUPPORTED_DATABASE`, `CACHALOT_ADDITIONAL_SUPPORTED_DATABASES` | Removed. Only PostgreSQL and SQLite are cached. |
| | `LEASE_TIMEOUT` has no cachalot counterpart (see [Failures](user-guide/orm-cache.md#failures)). |

`CACHE` must name a django-cachex Redis or Valkey backend, or a `TrackingCache` over one. Its serializer must keep Python types, as the default pickle serializer does. Other backends cache nothing, apart from `LocMemCache` for tests and single processes (see [Caches and databases](user-guide/orm-cache.md#caches-and-databases)).

### API

Import from `django_cachex.orm.api` instead of `cachalot.api`:

| django-cachalot | django-cachex |
|-----------------|---------------|
| `invalidate()` | `invalidate()`, with the same arguments |
| `cachalot_disabled(all_queries=False)` | `orm_cache_disabled()`, without `all_queries`, which cachalot 2.9.1 ignored |
| `get_last_invalidation(*tables_or_models, cache_alias=None, db_alias=None)` | [`table_generations(*tables_or_models, db_alias="default")`](user-guide/orm-cache.md#table_generations), returning generations instead of a timestamp and needing at least one table |
| `manage.py invalidate_cachalot` | `manage.py invalidate_orm_cache`, with the same arguments and options |
| `cachalot.signals.post_invalidation` | Removed. Cache values derived from tables under their `table_generations()` instead. |
| `cachalot.*` system checks | `cachex_orm.*` |
| The `get_last_invalidation` template tag, the Jinja2 extension and the Django Debug Toolbar panel | Removed |

Cache values under the generations instead of `get_last_invalidation()`. They are `None` when a value computed now must not be cached:

```python
# Before
def order_totals():
    key = f"totals:{get_last_invalidation(Order, OrderLine)}"
    return cache.get_or_set(key, compute_totals, timeout=3600)


# After
def order_totals():
    generations = table_generations(Order, OrderLine)
    if generations is None:
        return compute_totals()
    key = "totals:" + ":".join(generations)
    return cache.get_or_set(key, compute_totals, timeout=3600)
```

### Behavior differences

- Writes need the cache. A write that cannot take its lease raises `InvalidationError`, a `DatabaseError`, unless `ENABLED` is off (see [Failures](user-guide/orm-cache.md#failures)).
- While a write commits, queries on its tables run against the database and leave no stale result behind (see [How invalidation works](user-guide/orm-cache.md#how-invalidation-works)).
- Subqueries count with their tables wherever they sit, and `Now()` anywhere in a query keeps it from being cached.
- A MySQL alias in `DATABASES` is the error `cachex_orm.E006`, and `"supported_only"` leaves out replicas, the aliases with a `TEST["MIRROR"]`.
- With psycopg2 instead of psycopg 3, queries with JSON, binary or range parameters are not cached.
- A `migrate` that applied nothing invalidates nothing. Cachalot invalidated every model after each `migrate`.

### Rolling out

Cachalot and the ORM cache keep separate keys, so while both run, the writes of one do not invalidate what the other cached. A deployment that stops every process first can switch in one go. A rolling deployment takes three steps:

1. Set `CACHALOT_ENABLED = False` and deploy.
2. After every process runs with it, replace cachalot with `django_cachex.orm`, with `"ENABLED": False` in `CACHEX_ORM`, and deploy.
3. After the last cachalot process stops, set `"ENABLED": True` and deploy.

Cachalot stored its entries without expiry by default. If they live in a cache of their own, clear it. Otherwise they stay until the server evicts them, which it never does under a `volatile-*` or `noeviction` policy.
