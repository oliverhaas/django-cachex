# ORM Cache

`django_cachex.orm` caches the results of Django ORM queries and invalidates them per table: a write to a table invalidates every cached query that read it, in every process. It is derived from [django-cachalot](https://github.com/noripyt/django-cachalot) 2.9.1, with cachalot's timestamps replaced by table generations and write leases, so a write cannot leave a stale result behind (see [How invalidation works](#how-invalidation-works)). Coming from cachalot, see [Migrating from django-cachalot](../migration.md#from-django-cachalot).

The app is opt-in; nothing changes until it is installed:

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
    "CACHE": "default",  # alias holding the results, generations and leases
    "TIMEOUT": 3600,  # seconds a result is kept; the cache's default timeout if unset
}
```

From then on, ORM queries on PostgreSQL and SQLite databases are served from the cache while none of the tables they read has been written, and run against the database otherwise. Every process that writes to the database must run with the app installed (see [Limits](#limits)).

## Caches and databases

The ORM cache runs Lua scripts on the cache server, or keeps its state in the process:

- The Redis and Valkey backends: standalone, Sentinel and Cluster on redis-py and valkey-py, standalone and Cluster on valkey-glide.
- `TrackingCache`: results live on its transport, and each process also keeps a copy of the results it read (see [TrackingCache](#trackingcache)).
- `LocMemCache`, Django's or django-cachex's: results, generations and leases live in the process, so a write in one process does not invalidate the others. Use it in tests and single-process setups.

Any other backend (Memcached, `DatabaseCache`, Django's own `RedisCache`, ...) caches nothing, and the `cachex_orm.W001` check says so.

Results must come back from the cache with their Python types (`Decimal`, `datetime`, tuples). The default pickle serializer does that; the JSON and MsgPack serializers do not, and the `cachex_orm.E004` check rejects them. Any compressor works.

PostgreSQL and SQLite are supported: the ORM cache reads their transaction isolation to decide what a transaction may share (see [Transactions](#transactions)). By default every PostgreSQL and SQLite alias in `DATABASES` is cached except those with a `TEST["MIRROR"]`, which marks a replica. `CACHEX_ORM["DATABASES"]` lists the aliases explicitly instead. An alias of another vendor can be listed as well: inside transactions it then caches results for the transaction only (`cachex_orm.W003`), and it is untested.

## Settings

All settings live in the `CACHEX_ORM` dict. Keys are upper case; unknown keys raise the `cachex_orm.W005` warning.

| Key | Default | Description |
|-----|---------|-------------|
| `ENABLED` | `True` | Serve and store query results. While off, nothing is served from the cache, but writes still invalidate it. |
| `CACHE` | `"default"` | Cache alias holding the results, generations and leases. |
| `DATABASES` | `"supported_only"` | Database aliases whose queries are cached: every PostgreSQL and SQLite alias except replicas, or a list, tuple or set of aliases. |
| `TIMEOUT` | the cache's default timeout | Seconds a result is kept. A write stops its tables' results from being served without deleting them, so `None` keeps a result until the cache evicts it, see [Eviction](#eviction). |
| `LEASE_TIMEOUT` | `60` | Seconds a write's lease lasts if the write cannot release it. Keep it above the longest write, see [Failures](#failures). |
| `CACHE_RANDOM` | `False` | Cache queries ordered by `"?"` or calling `Random()`, `UUID4()`, `UUID7()` or `RandomUUID()`. |
| `CACHE_ITERATORS` | `True` | Cache the results of `iterator()`, which reads them into memory in full. |
| `INVALIDATE_RAW` | `True` | Invalidate the tables raw SQL writes to (see [Limits](#limits)). |
| `ONLY_CACHABLE_TABLES` | `()` | If set, only queries whose tables are all listed are cached. |
| `ONLY_CACHABLE_APPS` | `()` | Adds the tables of the apps with these labels, many-to-many tables included, to `ONLY_CACHABLE_TABLES`. |
| `UNCACHABLE_TABLES` | `()` | Queries reading one of these tables are not cached, and writes to them invalidate nothing. `django_migrations` is never cached. |
| `UNCACHABLE_APPS` | `()` | Adds the tables of the apps with these labels, many-to-many tables included, to `UNCACHABLE_TABLES`. |
| `ADDITIONAL_TABLES` | `()` | Tables no model covers, to look for in raw SQL. |
| `QUERY_KEYGEN` | `"django_cachex.orm.utils.get_query_cache_key"` | Callable, or its dotted path, building the key of a query from its SQL compiler. |
| `TABLE_KEYGEN` | `"django_cachex.orm.utils.get_table_cache_key"` | Callable, or its dotted path, building the key of a table from a database alias and a table name. |
| `FINAL_SQL_CHECK` | `False` | Also search the final SQL of every query for table names, to catch the tables custom expressions name in SQL of their own, such as a `Func` template. Queries with `extra()` conditions or ordered by a subquery are always searched. |

The settings are read again when a test overrides `CACHEX_ORM`, `DATABASES` or `CACHES`.

## What is cached

Queries the ORM compiles: querysets, `get()`, `count()`, `exists()`, `aggregate()`, `values()` and the like, across joins, subqueries and many-to-many tables, whenever all the tables they read are cachable. A result is cached per database, SQL and parameters. Async queries (`aget()`, `acount()`, `async for`) run the same code in a thread and are cached the same way.

Not cached:

- `select_for_update()` and `explain()`.
- Queries calling `Now()` or `TransactionNow()` anywhere: in a filter, an annotation, the ordering, a subquery or a `FilteredRelation` condition.
- Queries ordered by `"?"` or calling `Random()`, `UUID4()`, `UUID7()` or `RandomUUID()`, unless `CACHE_RANDOM` is on.
- Queries holding an expression that compiles to SQL the ORM cache cannot look into, one without `get_source_expressions()`.
- Queries with a parameter of a type the cache key cannot represent faithfully. The standard scalar, date and time types, `Decimal`, `UUID`, containers of them and the PostgreSQL driver's types are fine.
- Raw SQL: `cursor.execute()` and `Manager.raw()`.
- Queries reading `django_migrations`, a table in `UNCACHABLE_TABLES` or one outside `ONLY_CACHABLE_TABLES`.
- Inside a transaction, queries reading a table the transaction has written.

SQL you write yourself is not inspected for functions: a `RawSQL("now()")` or a `Func(function="NOW")` is cached like any other query. Run such queries inside [`orm_cache_disabled()`](#orm_cache_disabled).

## How invalidation works

Every table has a generation in the cache. A result is stored with the generations of the tables it read, and served only while all of them are unchanged. A write takes a lease on its tables and bumps their generations before it runs, then releases the lease and bumps them again once it is committed: around the statement under autocommit, around `COMMIT` inside a transaction. While a table is leased, queries on it neither read from nor write to the cache, so a result read before the commit is never stored under generations that are current after it.

Each step is one Lua script, and leases expire by the cache server's clock, so the application servers' clocks do not need to agree. A cached read costs one round trip, a miss one more to store the result, and a write under autocommit or the commit of a transaction that wrote two: taking and releasing the lease.

The keys go through the cache alias's key function, so they carry its `KEY_PREFIX` and `VERSION`:

- `orm:{<database alias>}:q:<query key>`: a result and the generations it was stored under, expiring after `TIMEOUT`.
- `orm:{<database alias>}:g:<table key>`: the generation of a table, without expiry.
- `orm:{<database alias>}:l:<table key>`: the leases on a table, without expiry: each lease carries its own, and the key goes when its last lease is removed.

The database alias is the hash tag, so on a cluster all keys of one database live in one slot, and on one shard. A generation that is evicted or cleared comes back derived from the server clock in microseconds, which no result stored under its old value matches.

The keys name database aliases, not the databases behind them, so projects or environments sharing a cache server need distinct `KEY_PREFIX`es or database numbers. Otherwise each serves the results the other read.

### Eviction

A write stops its tables' results from being served but leaves them in the cache until they expire or are evicted, or until the same query stores its result again. What a full Redis or Valkey server evicts depends on its `maxmemory-policy`:

- `volatile-lru`, `volatile-lfu`, `volatile-random` and `volatile-ttl` evict only keys with an expiry. Of the ORM cache's keys only results have one, and only with a `TIMEOUT`. Use one of these policies with a `TIMEOUT`: with nothing left to evict, they act like `noeviction`.
- `allkeys-lru`, `allkeys-lfu` and `allkeys-random` evict generations too, which is safe, and leases: a lease evicted during its write acts like one that expired early (see [Failures](#failures)).
- Under `noeviction`, the default, a full server refuses writes, so every database write to a cached table fails with `InvalidationError` until memory is freed.

## Transactions

What a transaction writes is tracked per savepoint; rolling back to a savepoint forgets what was written since. Leases are taken at `COMMIT`, so other connections keep using the cache while the transaction runs.

- Under PostgreSQL's `READ COMMITTED` (the default) and on SQLite with a rollback journal (the default), each statement sees what was committed when it started, like the shared cache. A transaction reads the tables it has not written through the shared cache, and the tables it has written from the database.
- Under `REPEATABLE READ` and `SERIALIZABLE`, on SQLite in WAL mode and on other vendors, a transaction reads a snapshot that may be older than the shared cache. Its results are cached for the transaction alone and dropped when it ends.

The isolation is read once per database connection, from the `isolation_level` option or from the server. Raw SQL changing the session default (`SET SESSION CHARACTERISTICS`, `default_transaction_isolation`, `PRAGMA journal_mode`) makes the ORM cache read it again. Other statements naming an isolation, like `SET TRANSACTION ISOLATION LEVEL`, make the connection cache per transaction until it reconnects.

## Failures

A write must not be missed, so:

- If a write cannot take its lease, for example because the cache server is down, it raises `django_cachex.orm.exceptions.InvalidationError` before the statement or the `COMMIT` runs. The error is a `DatabaseError` (and a `CachexError`), so `atomic()` rolls the transaction back: writes fail while the cache is unreachable. To keep writing through an outage, set `CACHEX_ORM["ENABLED"] = False`: nothing is then served from the cache, and a write that cannot reach it logs a warning and goes ahead. Run `invalidate_orm_cache` before enabling it again.
- If a write cannot release its lease, it logs a warning. The lease expires after `LEASE_TIMEOUT`, and until then queries on its tables run against the database. The same happens when a process dies holding a lease.
- If a lookup or a store fails, the query runs against the database and a warning is logged.

`LEASE_TIMEOUT` must outlast the longest write statement and `COMMIT`: a lease that expires while its write still runs lets another process store what it read before the commit, and that result is served until the write releases the lease, or until it expires if the release fails. Keep it above the database's `statement_timeout`.

Warnings go to the `django_cachex.orm` logger.

## TrackingCache

Over a `TrackingCache`, the results, generations and leases live on its transport, and each process also keeps the results it read in a local LRU, apart from the alias's own local store and bounded by the same `MAX_ENTRIES`. A lookup sends the generations of the local copy, and while the server holds the result under the same generations it answers without the payload: a hit costs a round trip but not the transfer. A local copy expires with the server's copy and is never served after a write. This does not rely on the tracking listener, so both coherence modes work.

## API

```python
from django_cachex.orm.api import invalidate, orm_cache_disabled, table_generations
```

### invalidate()

`invalidate(*tables_or_models, cache_alias=None, db_alias=None)` invalidates the cached queries of the given tables, models or `"app_label.ModelName"` strings, or of every table when none are given. It covers every cache and database unless `cache_alias` and `db_alias` narrow it. Call it after changing data the ORM cache cannot see, like a bulk load through another client.

Inside a transaction, the tables are also marked as written, so their queries run against the database until the commit, which invalidates them once more.

### orm_cache_disabled()

```python
with orm_cache_disabled():
    orders = list(Order.objects.filter(status="open"))
```

Queries in the block run against the database, and writes in it still invalidate the cache. It applies to the current thread or asyncio task.

### table_generations()

`table_generations(*tables_or_models, db_alias="default")` returns the current generations of the given tables as a tuple of strings. Every committed write to one of them changes it, so a value computed from the tables can be cached under it:

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

Read the generations before computing the value, and name every table the computation reads. It returns `None` when a value computed now must not be cached: while a write to one of the tables holds its lease, when the ORM cache is disabled or would not cache a query of these tables, and inside a transaction that has written one of them or reads a snapshot.

### Management command

```console
python manage.py invalidate_orm_cache                    # every table
python manage.py invalidate_orm_cache shop               # an app's models, many-to-many tables included
python manage.py invalidate_orm_cache shop.Order --cache default --db default
```

An unknown label is an error; an app without models invalidates nothing.

### Signal

`django_cachex.orm.signals.post_invalidation` is sent once per table after its queries were invalidated: after a write under autocommit, when a transaction that wrote the table commits, and by `invalidate()` (at the commit, inside a transaction). The sender is the table name, and `db_alias` names the database. A receiver that raises does not fail the write, which has happened by then: the error is logged to the `django.dispatch` logger.

## System checks

| ID | Meaning |
|----|---------|
| `cachex_orm.W001` | The cache backend cannot hold the ORM cache, so nothing is cached. |
| `cachex_orm.W002` | None of the databases are PostgreSQL or SQLite. |
| `cachex_orm.W003` | A database listed in `DATABASES` is neither PostgreSQL nor SQLite. |
| `cachex_orm.W004` | `DATABASES` is empty. |
| `cachex_orm.W005` | `CACHEX_ORM` has unknown keys. |
| `cachex_orm.W006` | A database listed in `DATABASES` has a `TEST["MIRROR"]`, so it looks like a replica. |
| `cachex_orm.E001` | `DATABASES` names an alias missing from Django's `DATABASES`. |
| `cachex_orm.E002` | `DATABASES` is neither `"supported_only"` nor a list, tuple or set. |
| `cachex_orm.E003` | `CACHE` names an alias missing from `CACHES`. |
| `cachex_orm.E004` | The cache's serializer does not bring query results back unchanged. |
| `cachex_orm.E005` | The cache could not be loaded. |
| `cachex_orm.E006` | `ONLY_CACHABLE_APPS` or `UNCACHABLE_APPS` names a label no installed app has. |
| `cachex_orm.E007` | A table or app setting is not a list, tuple or set, like `("django_session")` without its comma. The value counts as empty. |

## Limits

- Writes the ORM cache does not see invalidate nothing: other applications and database clients, triggers and rules, stored procedures run with `callproc()`, `COPY`, and writes by processes without the app. Call `invalidate()` or `invalidate_orm_cache` after them. Deletes do follow the foreign keys that Django declares with a database-level `on_delete` (`DB_CASCADE`, `DB_SET_NULL`, `DB_SET_DEFAULT`), and `TRUNCATE ... CASCADE` every table that references a truncated one.
- Raw SQL writes are recognized by keyword (`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `ALTER`, `CREATE`, `DROP`, `REPLACE INTO`, `MERGE INTO`) and their tables by name, so a statement naming a table anywhere, even in a string, invalidates it. Tables without a model are found only when listed in `ADDITIONAL_TABLES`. Transactions opened with a raw `BEGIN` are not seen; use `atomic()`.
- Leave replicas out of `DATABASES`. Generations are kept per database alias, so writes to the primary do not invalidate what was cached from the replica, and the replica lags behind anyway.
- A failover of the cache server can lose the latest generation bumps to asynchronous replication, which makes stale results current again. Run `invalidate_orm_cache` after a failover.
- Writes made while the app was not installed, while a database was left out of `DATABASES` or while `CACHE` pointed at another alias invalidated nothing in the cache in question. Run `invalidate_orm_cache` before switching back.
- A `migrate` that applied a migration invalidates every model when it finishes, many-to-many tables included, and so does `flush`; a `migrate` that applied nothing invalidates nothing. Schema changes and data migrations take leases like any write, so migrations need the cache too (or `ENABLED` off, followed by `invalidate_orm_cache`).
- `TIMEOUT` bounds how long a result takes up memory, not how stale it can get: results are current until their tables are written.
