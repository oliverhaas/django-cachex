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
- An ORM query cache, `django_cachex.orm`, see [ORM Cache](user-guide/orm-cache.md).

## From django-cachalot

The [ORM cache](user-guide/orm-cache.md) started as a copy of django-cachalot 2.9.1, so most of its settings and functions carry over under new names. Invalidation works differently: results are stored under table generations instead of timestamps, and writes take leases, so a query that runs while a write commits can no longer leave a stale result in the cache.

Replace the app and uninstall cachalot; running both would patch the ORM twice:

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
| `CACHALOT_TIMEOUT` | `TIMEOUT`. The default is the cache's default timeout, not `None`. Either way a write leaves its tables' results in the cache until they expire or are evicted, see [Eviction](user-guide/orm-cache.md#eviction). |
| `CACHALOT_ONLY_CACHABLE_APPS`, `CACHALOT_UNCACHABLE_APPS` | Removed. List the apps' tables, many-to-many tables included, in `ONLY_CACHABLE_TABLES` or `UNCACHABLE_TABLES`. |
| `CACHALOT_CACHE_RANDOM`, `CACHALOT_CACHE_ITERATORS`, `CACHALOT_INVALIDATE_RAW` | Removed. Random queries and the results of `iterator()` are never cached, and raw SQL writes always invalidate. |
| `CACHALOT_QUERY_KEYGEN`, `CACHALOT_TABLE_KEYGEN` | Removed. The keys go through the cache alias's `KEY_FUNCTION`, which can tell tenants apart. |
| `CACHALOT_USE_UNSUPPORTED_DATABASE`, `CACHALOT_ADDITIONAL_SUPPORTED_DATABASES` | Removed. Only PostgreSQL and SQLite are cached. |
| | `LEASE_TIMEOUT` is new, see [Failures](user-guide/orm-cache.md#failures). |

`CACHE` must name a django-cachex Redis or Valkey backend, or a `TrackingCache` over one, with a serializer that keeps Python types, like the default pickle one. Other backends cache nothing (`cachex_orm.W001`), apart from `LocMemCache` for tests and single processes. See [Caches and databases](user-guide/orm-cache.md#caches-and-databases).

### API

Import from `django_cachex.orm.api` instead of `cachalot.api`:

| django-cachalot | django-cachex |
|-----------------|---------------|
| `invalidate()` | `invalidate()`, with the same arguments |
| `cachalot_disabled(all_queries=False)` | `orm_cache_disabled()`. The `all_queries` argument is gone; cachalot 2.9.1 ignored it. |
| `get_last_invalidation(*tables_or_models, cache_alias=None, db_alias=None)` | `table_generations(*tables_or_models, db_alias="default")`, returning generations instead of a timestamp and needing at least one table, see [table_generations()](user-guide/orm-cache.md#table_generations) |
| `manage.py invalidate_cachalot` | `manage.py invalidate_orm_cache`, with the same arguments and options. An app label also covers the app's many-to-many tables, and an app without models invalidates nothing instead of every table. |
| `cachalot.signals.post_invalidation` | Removed. Cache values derived from tables under their [table_generations()](user-guide/orm-cache.md#table_generations) instead. |
| `cachalot.*` system checks | `cachex_orm.*`, see [System checks](user-guide/orm-cache.md#system-checks) |
| The `get_last_invalidation` template tag, the Jinja2 extension and the Django Debug Toolbar panel | Removed |

A value cached under `get_last_invalidation()` is cached under the generations instead, which are `None` when a value computed now must not be cached:

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

- A write can no longer leave a stale result behind: while a write holds its lease, queries on its tables are neither served from the cache nor stored in it. See [How invalidation works](user-guide/orm-cache.md#how-invalidation-works).
- Writes need the cache. A write that cannot take its lease raises `InvalidationError`, a `DatabaseError`, before its statement or `COMMIT` runs, unless `ENABLED` is off. See [Failures](user-guide/orm-cache.md#failures).
- Subqueries nested in expressions, in the ordering or in `FilteredRelation` conditions count with their tables, and `Now()` anywhere in a query keeps it from being cached; cachalot looked at the top level of filters and annotations only. Queries calling `Random()`, `UUID4()`, `UUID7()` or `RandomUUID()` are never cached, like those ordered by `"?"`.
- Raw SQL is searched for whole table names, in any case. Cachalot found `shop_order` inside `shop_orderline`, which invalidated more than needed, and missed names with uppercase letters, which left stale results. It also missed the tables of an `extra()` select that `values()` hides and the ordering uses.
- Only PostgreSQL and SQLite are cached. Cachalot also covered MySQL; listing it in `DATABASES` is now the error `cachex_orm.E006`. `"supported_only"` also leaves out replicas, the aliases with a `TEST["MIRROR"]`.
- The results of `iterator()` are not cached; cachalot read them into memory in full and cached them by default.
- With psycopg2 instead of psycopg 3, queries with JSON, binary or range parameters are not cached.
- `migrate` invalidates only when it applied a migration, and then the many-to-many tables too; cachalot invalidated every model after each `migrate` and left out the many-to-many tables.
- Query parameters are keyed by their type and whole value. Cachalot keyed them by `str()`, which psycopg 3 shortens for long JSON and binary values, so two queries differing only there could share a result, as could `Value(1)` and `Value("1")`.

### Rolling out

Cachalot and the ORM cache keep separate keys, so while both run, the writes of one do not invalidate what the other cached. A deployment that stops every process before starting the new ones can switch in one go. A rolling deployment takes three:

1. Set `CACHALOT_ENABLED = False` and deploy. Once every process runs it, nothing is served from cachalot's cache, and its processes still invalidate it.
2. Replace cachalot with `django_cachex.orm`, with `"ENABLED": False` in `CACHEX_ORM`, and deploy. Nothing is served from either cache.
3. Once no cachalot process remains, set `"ENABLED": True` and deploy.

Cachalot stored its entries without expiry by default. If they live in a cache of their own, clear it; otherwise they stay until the server evicts them, which it never does under a `volatile-*` or `noeviction` policy.
