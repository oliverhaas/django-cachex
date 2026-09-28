# ORM Cache

`django_cachex.orm` caches the results of Django ORM queries on PostgreSQL and SQLite. A write to a table invalidates every cached query that read it, in every process. The app is derived from [django-cachalot](https://github.com/noripyt/django-cachalot) 2.9.1. It replaces cachalot's timestamps with table generations and write leases, so a write cannot leave a stale result behind (see [How invalidation works](#how-invalidation-works)). For moving from cachalot, see [Migrating from django-cachalot](../migration.md#from-django-cachalot).

Nothing is cached until the app is installed:

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

Every process that writes to the database must run with the app installed (see [Limits](#limits)).

## Caches and databases

The ORM cache runs Lua scripts on the cache server, or keeps its state in the process. It works with:

- The Redis and Valkey backends: standalone, Sentinel and Cluster on redis-py and valkey-py, standalone and Cluster on valkey-glide.
- `TrackingCache` over one of them (see [TrackingCache](#trackingcache)).
- `LocMemCache`, Django's or django-cachex's: results, generations and leases live in the process, so a write in one process does not invalidate the others. Use it in tests and single-process setups.

Any other backend, such as Memcached, `DatabaseCache` or Django's own `RedisCache`, caches nothing, and the `cachex_orm.W001` check warns about it.

Results must come back from the cache with their Python types, such as `Decimal`, `datetime` and tuples. The default pickle serializer keeps them. The JSON and MsgPack serializers do not, and the `cachex_orm.E004` check rejects them. Any compressor works.

The ORM cache supports PostgreSQL and SQLite, whose transaction isolation it reads to decide what a transaction can share (see [Transactions](#transactions)). An alias of another vendor is never cached, and listing it in `CACHEX_ORM["DATABASES"]` is the error `cachex_orm.E006`.

## Settings

All settings live in the `CACHEX_ORM` dict. Keys are upper case, and an unknown key raises the `cachex_orm.W004` warning.

| Key | Default | Description |
|-----|---------|-------------|
| `ENABLED` | `True` | Serve and store query results. While off, nothing is served from the cache, but writes still invalidate it. |
| `CACHE` | `"default"` | Cache alias holding the results, generations and leases. |
| `DATABASES` | `"supported_only"` | Database aliases whose queries are cached. `"supported_only"` means every PostgreSQL and SQLite alias except replicas, which have a `TEST["MIRROR"]`. A list, tuple or set names the aliases. |
| `TIMEOUT` | the cache's default timeout | Seconds a result is kept. `None` keeps it until the cache evicts it, even after a write (see [Eviction](#eviction)). |
| `LEASE_TIMEOUT` | `60` | Seconds a write's lease lasts if the write cannot release it. Keep it above the longest write (see [Failures](#failures)). |
| `ONLY_CACHABLE_TABLES` | `()` | If set, only queries whose tables are all listed are cached. |
| `UNCACHABLE_TABLES` | `()` | Queries reading one of these tables are not cached, and writes to them invalidate nothing. `django_migrations` is never cached. |
| `ADDITIONAL_TABLES` | `()` | Tables no model covers, to look for in raw SQL. |
| `FINAL_SQL_CHECK` | `False` | Also search the final SQL of every query for table names, to catch the tables custom expressions name in SQL of their own, such as a `Func` template. Queries with `extra()` selects or conditions, or ordered by a subquery, are always searched. |

The settings are read again when a test overrides `CACHEX_ORM`, `DATABASES` or `CACHES`.

## What is cached

Queries the ORM compiles are cached when all the tables they read are cachable. That covers querysets, `get()`, `count()`, `exists()`, `aggregate()` and `values()`, with joins, subqueries and many-to-many tables. A result is cached per database, SQL and parameters. Async queries such as `aget()`, `acount()` and `async for` are cached the same way.

These are not cached:

- `select_for_update()` and `explain()`.
- Queries ordered by `"?"`.
- Queries calling `Now()`, `TransactionNow()`, `Random()`, `UUID4()`, `UUID7()` or `RandomUUID()` anywhere: in a filter, an annotation, the ordering, a subquery or a `FilteredRelation` condition.
- The results of `iterator()` and `aiterator()`, which stream their rows. A result the same query stored without them is still served.
- Queries holding an expression without `get_source_expressions()`, whose SQL the ORM cache cannot look into.
- Queries with a parameter of a type the cache key cannot represent faithfully. The standard scalar, date and time types, `Decimal`, `UUID`, containers of them and psycopg 3's types are fine. With psycopg2, queries with JSON, binary or range parameters are not cached.
- Raw SQL through `cursor.execute()` and `Manager.raw()`.
- Queries reading `django_migrations`, a table in `UNCACHABLE_TABLES` or one outside `ONLY_CACHABLE_TABLES`.
- Inside a transaction, queries reading a table the transaction has written, unless the transaction reads a snapshot (see [Transactions](#transactions)).

SQL you write yourself is not searched for functions, so a query with `RawSQL("now()")` or `Func(function="NOW")` is cached like any other. Run such queries inside [`orm_cache_disabled()`](#orm_cache_disabled).

## How invalidation works

Every table has a generation in the cache, and every committed write to the table changes it. A result is stored with the generations of the tables it read and served only while all of them are unchanged.

A write holds a lease on its tables while it commits: under autocommit around the statement, inside a transaction around the `COMMIT`. While a table is leased, queries on it are neither served from the cache nor stored in it. A query that reads the table during a write therefore cannot cache a result the write makes stale.

Leases expire by the cache server's clock, so the application servers' clocks do not need to agree. A cached read costs one round trip, and a miss one more to store the result. A write under autocommit, or the commit of a transaction that wrote, costs two: one to take the lease and one to release it.

The keys go through the cache alias's key function, so they carry its `KEY_PREFIX` and `VERSION`:

- `orm:{<database alias>}:q:<query key>`: a result and the generations it was stored under, expiring after `TIMEOUT`.
- `orm:{<database alias>}:g:<table key>`: the generation of a table, without expiry.
- `orm:{<database alias>}:l:<table key>`: the leases on a table. The key has no expiry. Each lease expires on its own, and the key is deleted when its last lease is removed.

The database alias is the hash tag, so on a cluster all keys of one database live in one slot, and on one shard. A generation that is evicted or cleared comes back with a new value, which no result stored under the old one matches.

The keys name database aliases, not the databases behind them. Projects or environments sharing a cache server therefore need distinct `KEY_PREFIX`es or database numbers. Otherwise each serves the results the other read.

### Eviction

A write stops its tables' results from being served but does not delete them. They stay in the cache until they expire, are evicted or are replaced by the same query's next result. What a full Redis or Valkey server evicts depends on its `maxmemory-policy`:

- `volatile-lru`, `volatile-lfu`, `volatile-random` and `volatile-ttl` evict only keys with an expiry. Of the ORM cache's keys, only results have one, and only with a `TIMEOUT`. Set a `TIMEOUT` with these policies, because without keys to evict they act like `noeviction`.
- `allkeys-lru`, `allkeys-lfu` and `allkeys-random` also evict generations, which is safe, and leases. A lease evicted during its write acts like one that expired early (see [Failures](#failures)).
- Under `noeviction`, the default, a full server refuses writes, so every database write to a cached table fails with `InvalidationError` until memory is freed.

## Transactions

A transaction's writes are tracked per savepoint, and rolling back to a savepoint forgets the writes made since. Leases are taken at `COMMIT`, so other connections keep using the cache while the transaction runs.

- `READ COMMITTED`, PostgreSQL's default, and SQLite's default rollback journal let each statement see what was committed when it started, like the shared cache. A transaction reads the tables it has not written through the shared cache, and the tables it has written from the database.
- Under `REPEATABLE READ` and `SERIALIZABLE`, and on SQLite in WAL mode, a transaction reads a snapshot that can be older than the shared cache. Its results are cached for the transaction alone, up to 16 MiB of them pickled, and dropped when it ends.

The isolation is read once per database connection, from the `isolation_level` option or from the server. Raw SQL that changes the session default, such as `SET SESSION CHARACTERISTICS`, `default_transaction_isolation` or `PRAGMA journal_mode`, makes the ORM cache read it again. Inside a transaction, it is read again after the transaction ends, when the new default takes effect. Other statements naming an isolation, like `SET TRANSACTION ISOLATION LEVEL`, make the connection cache per transaction until it reconnects.

## Failures

A write that cannot take its lease, for example because the cache server is down, raises `django_cachex.orm.exceptions.InvalidationError` before the statement or the `COMMIT` runs. The error is a `DatabaseError` and a `CachexError`, so `atomic()` rolls the transaction back. Writes fail while the cache is unreachable, so that none is missed. To keep writing through an outage, set `CACHEX_ORM["ENABLED"] = False`. Nothing is then served from the cache, and a write that cannot reach it logs a warning and goes ahead. Run `invalidate_orm_cache` before enabling it again.

A write that cannot release its lease logs a warning. The lease expires after `LEASE_TIMEOUT`, and until then queries on its tables run against the database. The same happens when a process dies holding a lease. If a lookup or a store fails, the query runs against the database and a warning is logged.

`LEASE_TIMEOUT` must outlast the longest write statement and `COMMIT`. A lease that expires while its write still runs lets another process store what it read before the commit. That result is served until the write releases the lease, or until the result expires if the release fails. Keep `LEASE_TIMEOUT` above the database's `statement_timeout`.

Warnings go to the `django_cachex.orm` logger.

## TrackingCache

Over a `TrackingCache`, the results, generations and leases live on its transport. Each process also keeps the results it read in a local LRU, separate from the alias's own local store and bounded by the same `MAX_ENTRIES`. A hit on a local copy still costs a round trip to check it against the server, but the result is not transferred. A local copy expires with the server's copy and is never served after a write. The check does not rely on the tracking listener, so both coherence modes work.

## API

```python
from django_cachex.orm.api import invalidate, orm_cache_disabled, table_generations
```

### invalidate()

`invalidate(*tables_or_models, cache_alias=None, db_alias=None)` invalidates the cached queries of the given tables, models or `"app_label.ModelName"` strings, or of every table when none are given. It covers every cache and database unless `cache_alias` and `db_alias` narrow it. Call it after changing data the ORM cache cannot see, such as a bulk load through another client.

Inside a transaction, it also marks the tables as written. The transaction then reads them as tables it has written (see [Transactions](#transactions)), and its commit invalidates them again.

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

- While a write to one of the tables holds its lease.
- When the ORM cache is disabled or would not cache a query of these tables.
- Inside a transaction that has written one of the tables or reads a snapshot.

### Management command

```console
python manage.py invalidate_orm_cache                    # every table
python manage.py invalidate_orm_cache shop               # an app's models, many-to-many tables included
python manage.py invalidate_orm_cache shop.Order --cache default --db default
```

An unknown label is an error, and an app without models invalidates nothing.

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

- Writes the ORM cache does not see invalidate nothing. These are writes by other applications and database clients, triggers and rules, stored procedures run with `callproc()`, `COPY`, and writes by processes without the app. Call `invalidate()` or `invalidate_orm_cache` after them. Deletes do follow the foreign keys that Django declares with a database-level `on_delete` (`DB_CASCADE`, `DB_SET_NULL`, `DB_SET_DEFAULT`). `TRUNCATE ... CASCADE` follows every table that references a truncated one.
- Raw SQL writes are recognized by keyword (`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `ALTER`, `CREATE`, `DROP`, `REFRESH`, `REPLACE INTO`, `MERGE INTO`) and their tables by name. A statement naming a table anywhere, even in a string, invalidates it. Tables without a model are found only when listed in `ADDITIONAL_TABLES`. Transactions opened with a raw `BEGIN` are not seen, so use `atomic()`.
- Results read from a view are cached under the view's name, not the tables behind it. Writes to those tables do not invalidate them. List views in `UNCACHABLE_TABLES`, or `invalidate()` them after such writes. `REFRESH MATERIALIZED VIEW` invalidates the view it names.
- Leave replicas out of `DATABASES`. Generations are kept per database alias, so writes to the primary do not invalidate what was cached from the replica. The replica also lags behind.
- A failover of the cache server can lose the latest generation changes to asynchronous replication, which makes stale results current again. Run `invalidate_orm_cache` after a failover.
- Writes made while the app was uninstalled, a database was left out of `DATABASES` or `CACHE` named another alias did not invalidate the cache. Run `invalidate_orm_cache` before switching back.
- A `migrate` that applied a migration invalidates every model when it finishes, many-to-many tables included, and so does `flush`. A `migrate` that applied nothing invalidates nothing. Schema changes and data migrations take leases like any write, so migrations need the cache too, or `ENABLED` off followed by `invalidate_orm_cache`.
- `TIMEOUT` bounds how long a result takes up memory, not how stale it can get. Results are current until their tables are written.
