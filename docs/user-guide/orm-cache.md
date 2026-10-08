# ORM Cache

`django_cachex.orm` caches the results of Django ORM queries on PostgreSQL and SQLite. A write to a table invalidates every cached query that read it, in every process. The app is derived from [django-cachalot](https://github.com/noripyt/django-cachalot) 2.9.1. For moving from cachalot, see [Migrating from django-cachalot](../migration.md#from-django-cachalot).

Add the app to `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ...
    "django_cachex.orm",
]

CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
    },
}

CACHEX_ORM = {
    "CACHE": "default",
    "TIMEOUT": 3600,
}
```

Every process that writes to the database must run with the app installed.

## Caches and databases

The ORM cache works with these cache backends:

- The Redis and Valkey backends: standalone, Sentinel and Cluster on redis-py and valkey-py, standalone and Cluster on valkey-glide.
- `TrackingCache` over one of them, which the ORM cache uses as if `CACHE` named its transport. It keeps no local copies of results.
- `LocMemCache`, Django's or django-cachex's. It keeps everything in the process, so a write in one process does not invalidate the others. Use it for tests and single-process setups.

Other backends, such as Memcached, `DatabaseCache` or Django's own `RedisCache`, cache nothing (`cachex_orm.W001`).

The cache's serializer must bring results back with their Python types, such as `Decimal`, `datetime` and tuples. The default pickle serializer does, and the JSON and MsgPack serializers fail the `cachex_orm.E004` check. Any compressor works.

Only PostgreSQL and SQLite databases are cached.

## Settings

All settings live in the `CACHEX_ORM` dict, with upper-case keys. An unknown key is the `cachex_orm.W004` warning.

| Key | Default | Description |
|-----|---------|-------------|
| `ENABLED` | `True` | Serve and store query results. Writes invalidate the cache even while it is `False`. |
| `CACHE` | `"default"` | Cache alias of the ORM cache. |
| `DATABASES` | `"supported_only"` | Aliases whose queries are cached: `"supported_only"` for every PostgreSQL and SQLite alias except replicas (aliases with a `TEST["MIRROR"]`), or a list, tuple or set of aliases. |
| `TIMEOUT` | the cache's default timeout | Seconds a result is kept. `None` keeps it until the cache evicts it, even after a write (see [Eviction](#eviction)). |
| `ONLY_CACHABLE_TABLES` | `()` | If set, only queries whose tables are all listed are cached. |
| `UNCACHABLE_TABLES` | `()` | Queries reading one of these tables are not cached, and writes to them invalidate nothing. `django_migrations` is never cached. |
| `ADDITIONAL_TABLES` | `()` | Tables no model covers, to look for in raw SQL. |
| `FINAL_SQL_CHECK` | `False` | Also search the final SQL of every query for table names in double quotes, such as those in a `Func` template. |
| `QUERY_KEYGEN` | `"django_cachex.orm.utils.readable_query_key"` | Callable, or its dotted path, that builds the key of a query's result (see [Cache keys](#cache-keys)). |
| `TABLE_KEYGEN` | `"django_cachex.orm.utils.readable_table_key"` | Callable, or its dotted path, that builds the key of a table's generation (see [Cache keys](#cache-keys)). |

## What is cached

Queries the ORM compiles, such as querysets, `get()`, `count()`, `aggregate()` and their async forms, are cached when all the tables they read are cachable. A result is cached per database, SQL and parameters. The values of an `__in` filter are sorted before the query runs, so one set of values gets one result in any order, such as the order of the parent rows a `prefetch_related()` passes. `__in` filters in subqueries, annotations and `FilteredRelation()` conditions keep their order.

These are not cached:

- `select_for_update()` and `explain()`.
- Queries ordered by `"?"`.
- Queries calling `Now()`, `TransactionNow()`, `Random()`, `UUID4()`, `UUID7()` or `RandomUUID()` anywhere, subqueries included.
- The results of `iterator()` and `aiterator()`.
- Queries holding an expression without `get_source_expressions()`.
- Queries with a parameter the cache key cannot represent exactly, such as JSON, binary and range parameters under psycopg2.
- Raw SQL through `cursor.execute()` and `Manager.raw()`.
- Inside a transaction, queries reading a table it has written, unless it reads a snapshot (see [Transactions](#transactions)).

The ORM cache does not search SQL you write for functions, so it caches a query with `RawSQL("now()")` or `Func(function="NOW")`. Run such queries inside [`orm_cache_disabled()`](#orm_cache_disabled).

## How invalidation works

Every table has a generation in the cache, and every committed write to the table changes it. A result is stored with the generations of the tables it read and served only while all of them are unchanged.

A write changes the generations of its tables twice: right before the statement under autocommit, or before the `COMMIT` inside a transaction, and again right after it. The first change drops the results stored before the write, and a write whose first change fails does not run (see [Failures](#failures)). The second change drops the results that queries read while the write ran. Between the commit and the second change, about one round trip to the cache, other processes can still be served those results, which show the tables as they were before the write. The write returns, and its `on_commit()` hooks run, only after the second change, so queries that run after the write returns, in the same process or in a task it starts, get the new rows.

A cached read costs one round trip to the cache, an `MGET` of the generations and the result, and a miss two. A write under autocommit, or the commit of a transaction that wrote, also costs two. Reads go to the primary even when the cache alias lists replicas, since a replica can lag behind a write. On a redis-py or valkey-py cluster, `read_from_replicas` or a `load_balancing_strategy` in the alias's `OPTIONS` sends them to replicas, which can serve the old rows until they have caught up with a write.

### Cache keys

A result is stored under `orm:{<database alias>}:q:<query key>:<result type>` and the generation of a table under `orm:{<database alias>}:g:<table key>`, both under the cache alias's `KEY_PREFIX` and `VERSION`. The database alias is the hash tag, so on a cluster all keys of one database live on one shard. The keys name database aliases, not databases. Projects or environments sharing a cache server need distinct `KEY_PREFIX`es or database numbers, or each serves the results the other read.

By default the query key is the sorted names of the tables the query reads, joined with `.`, then `:` and the SHA-1 digest of the database alias, SQL and parameters. Past 100 characters, the names that do not fit are left out and counted, as in `shop_customer.+3more`. The table key is the table name:

```text
orm:{default}:q:shop_customer.shop_order:3f2a9c...:multi
orm:{default}:g:shop_order
```

`QUERY_KEYGEN` and `TABLE_KEYGEN` take a callable, or its dotted path, that builds these keys instead. Both are called with keyword arguments and return a `str`:

- `QUERY_KEYGEN(*, compiler, tables, digest)` returns the query key. `compiler` is the `SQLCompiler` of the query, already compiled, `tables` a `frozenset` of the names of the tables it reads, and `digest` the SHA-1 hex digest of the database alias, SQL and parameters. The SQL is the SQL that runs, with its `__in` values sorted. The ORM cache appends the result type itself.
- `TABLE_KEYGEN(*, db_alias, table)` returns the table key, for reads, writes, `invalidate()` and `table_generations()`.

A custom keygen must follow these rules:

- A table key depends on `db_alias` and `table` only, and is the same in every process and request. A key that also depends on the tenant of a request lets a write by one tenant leave the other tenants' results of a shared table stale.
- Queries with different digests get different keys. Anything a query key adds, such as a tenant id, only splits entries further.
- Tables can share a key. A write to one of them then invalidates the results of all of them.
- `QUERY_KEYGEN` can raise `django_cachex.orm.utils.UncachableQuery` to leave a query uncached. Any other exception from either keygen propagates: a query, `invalidate()` or `table_generations()` fails with it, and a write behaves as one that cannot reach the cache (see [Failures](#failures)).
- While processes run with different table keygens, each misses the writes of the others. Run `invalidate_orm_cache` after they all run the same one.

This table keygen keeps the table keys of django-cachex 0.12.1 and earlier:

```python
# myproject/cache_keys.py
from hashlib import sha1


def hashed_table_key(*, db_alias: str, table: str) -> str:
    return sha1(f"{db_alias}:{table}".encode(), usedforsecurity=False).hexdigest()
```

```python
CACHEX_ORM = {"TABLE_KEYGEN": "myproject.cache_keys.hashed_table_key"}
```

This query keygen adds the django-tenants schema to the query key, so tenants do not share results (see [Limits](#limits)):

```python
# myproject/cache_keys.py
from django_cachex.orm.utils import readable_query_key


def tenant_query_key(*, compiler, tables, digest):
    query_key = readable_query_key(compiler=compiler, tables=tables, digest=digest)
    return f"{compiler.connection.schema_name}:{query_key}"
```

```python
CACHEX_ORM = {"QUERY_KEYGEN": "myproject.cache_keys.tenant_query_key"}
```

The table keys stay the same in every schema, so a write to a table in one schema invalidates that table's results in all of them. Give the ORM cache an alias without a tenant-aware `KEY_FUNCTION` such as `django_tenants.cache.make_key`, which would put the tenant into the table keys too.

### Eviction

A write does not delete the results it invalidates, so they stay in the cache until they expire or are evicted. What a full Redis or Valkey server evicts depends on its `maxmemory-policy`:

- `volatile-*` policies evict only keys with an expiry, which only results with a `TIMEOUT` have. Set a `TIMEOUT` with them, or they act like `noeviction`.
- `allkeys-*` policies also evict generations, which is safe: an evicted generation comes back as a new value, which no stored result matches.
- Under `noeviction`, the default, a full server refuses writes, so every database write to a cached table fails with `InvalidationError` until memory is freed.

## Transactions

`READ COMMITTED`, PostgreSQL's default, and SQLite's default rollback journal let a transaction use the shared cache. Under `REPEATABLE READ` and `SERIALIZABLE`, and on SQLite in WAL mode, a transaction reads a snapshot that can be older than the shared cache. It caches up to 16 MiB of pickled results for itself instead, and drops them when it ends.

The ORM cache reads the isolation level per connection, from the `isolation_level` option or the server, and again after raw SQL changes the session default. Other SQL naming an isolation level, such as `SET TRANSACTION ISOLATION LEVEL`, makes the connection cache per transaction until it reconnects.

## Failures

A write that cannot change the generations of its tables before it runs, for example because the cache server is down, raises `django_cachex.orm.exceptions.InvalidationError` before the statement or `COMMIT` runs. The error is a `DatabaseError`, so `atomic()` rolls the transaction back. To keep writing through a cache outage, set `CACHEX_ORM["ENABLED"] = False`. A write that cannot reach the cache then logs a warning and goes ahead. Run `invalidate_orm_cache` before enabling it again.

If the change after the write fails, or the process dies between the commit and that change, results that other processes read while the write ran can be served until the next write to one of their tables or until they expire. A failed lookup or store, or a failed change after a write, logs a warning to the `django_cachex.orm` logger and does not raise.

## API

```python
from django_cachex.orm.api import invalidate, orm_cache_disabled, table_generations
```

### invalidate()

`invalidate(*tables_or_models, cache_alias=None, db_alias=None)` invalidates the cached queries of the given tables, models or `"app_label.ModelName"` strings, or of every table when none are given. `cache_alias` and `db_alias` narrow it to one cache or database. Call it after changing data the ORM cache cannot see, such as a bulk load through another client.

If a cache cannot be reached, it still invalidates the others, then raises one `InvalidationError` naming the caches and databases that failed.

### orm_cache_disabled()

```python
with orm_cache_disabled():
    orders = list(Order.objects.filter(status="open"))
```

Queries in the block run against the database, and writes in it still invalidate the cache. It applies to the current thread or asyncio task.

### table_generations()

`table_generations(*tables_or_models, db_alias="default")` returns the current generations of the given tables as a tuple of strings. Every committed write to one of the tables changes the tuple, so a value computed from the tables can be cached under it:

```python
from django.core.cache import cache
from django_cachex.orm.api import table_generations


def open_order_total():
    generations = table_generations(Order, OrderLine)
    if generations is None:
        return compute_open_order_total()
    key = "open-order-total:" + ":".join(generations)
    total = cache.get(key)
    if total is None:
        total = compute_open_order_total()
        cache.set(key, total, timeout=3600)
    return total
```

Read the generations before computing the value, and name every table the computation reads. `table_generations()` returns `None` when a value computed now must not be cached:

- When the ORM cache is disabled or would not cache a query of these tables.
- Inside a transaction that has written one of the tables or reads a snapshot.

### Management command

```console
python manage.py invalidate_orm_cache                    # every table
python manage.py invalidate_orm_cache shop               # an app's models, many-to-many tables included
python manage.py invalidate_orm_cache shop.Order --cache default --db default
```

## System checks

| ID | Meaning |
|----|---------|
| `cachex_orm.W001` | The cache backend cannot hold the ORM cache, so nothing is cached. |
| `cachex_orm.W002` | None of the databases are PostgreSQL or SQLite. |
| `cachex_orm.W003` | `DATABASES` is empty. |
| `cachex_orm.W004` | `CACHEX_ORM` has unknown keys. |
| `cachex_orm.W005` | A database listed in `DATABASES` has a `TEST["MIRROR"]`, so it looks like a replica. |
| `cachex_orm.E001` | `DATABASES` names an alias missing from Django's `DATABASES`. |
| `cachex_orm.E002` | `DATABASES` is neither `"supported_only"` nor a list, tuple or set. |
| `cachex_orm.E003` | `CACHE` names an alias missing from `CACHES`. |
| `cachex_orm.E004` | The cache's serializer does not bring query results back unchanged. |
| `cachex_orm.E005` | The cache could not be loaded. |
| `cachex_orm.E006` | A database listed in `DATABASES` is neither PostgreSQL nor SQLite. It is not cached. |
| `cachex_orm.E007` | A table setting is not a list, tuple or set, like `("django_session")` without its comma. The value counts as empty. |

## Limits

- The ORM cache does not see writes by other applications and database clients, triggers and rules, `COPY`, or stored procedures run with `callproc()`. Call `invalidate()` or `invalidate_orm_cache` after them. Deletes follow the foreign keys that Django declares with a database-level `on_delete` (`DB_CASCADE`, `DB_SET_NULL`, `DB_SET_DEFAULT`). `TRUNCATE ... CASCADE` follows every table that references a truncated one.
- Raw SQL writes are recognized by keyword (`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `ALTER`, `CREATE`, `DROP`, `REFRESH`, `REPLACE INTO`, `MERGE INTO`) and their tables by name. Tables without a model are found only when listed in `ADDITIONAL_TABLES`. Transactions opened with a raw `BEGIN` are not seen, so use `atomic()`.
- Results read from a view are cached under the view's name, so writes to the tables behind it do not invalidate them. List views in `UNCACHABLE_TABLES`, or `invalidate()` them after such writes. `REFRESH MATERIALIZED VIEW` invalidates the view it names.
- A result is keyed by the database alias, SQL and parameters. If the same SQL returns other rows depending on session state, such as django-tenants' per-tenant `search_path` or a row-level security policy reading a session setting, one tenant is served another's rows. Add that state to the query key (see [Cache keys](#cache-keys)).
- Leave replicas out of `DATABASES`. Writes to the primary do not invalidate what was cached from a replica's alias.
- A failover of the cache server can lose the latest generation changes, which makes stale results current again. Run `invalidate_orm_cache` after a failover.
- Writes made while the app was uninstalled, a database was left out of `DATABASES` or `CACHE` named another alias did not invalidate the cache. Run `invalidate_orm_cache` before switching back.
- `migrate` invalidates every model after applying a migration, and so does `flush`. Migrations change generations like any write, so they need the cache too, or `ENABLED` off followed by `invalidate_orm_cache`.
