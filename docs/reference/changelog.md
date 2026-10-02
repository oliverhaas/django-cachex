# Changelog

## Unreleased

### Improvements

- The admin decides per cache who can use its keys. Each alias in `CACHES` gets a permission, `django_cachex.access_<alias>`, and listing, opening, adding, editing, deleting, clearing and flushing keys need it next to the existing permissions, on a `TrackingCache` alias together with that of its transport. Flush database, the slow log's arguments, and Clear and Flush on Django's own `RedisCache` and the memcached backends need it for every alias. Before, `view_key` opened every cache, the session cache included, whose key names hold session ids. After upgrading, run `migrate`: staff who are not superusers see no keys until they are granted the caches they need. See [Permissions](../user-guide/admin.md#permissions).
- ORM cache keys show their tables, as in `orm:{default}:q:shop_customer.shop_order:<digest>:multi` for a result and `orm:{default}:g:shop_order` for a generation. The new `QUERY_KEYGEN` and `TABLE_KEYGEN` settings take a callable, or its dotted path, that builds them instead (see [Cache keys](../user-guide/orm-cache.md#cache-keys)). While processes of 0.12.1 or earlier and newer ones run against one cache, neither sees the other's writes, so results can be stale. Stop the old processes before the first new one starts, run `invalidate_orm_cache` after the last old one has stopped, or keep the old table keys with the `hashed_table_key` example from the docs.
- The ORM cache sorts the values of an `__in` filter before it keys a query, so a `prefetch_related()` under another order of its parent rows, or over a set of strings in another process, hits the cache. It compiles a query on an uncachable table once, not twice.
- The ORM cache finds the tables of a query with 10,000 `__in` values in 0.2 ms instead of 8 ms.

### Fixes

- The ORM cache's `invalidate()` and `invalidate_orm_cache` stopped at the first cache they could not reach, so the caches after it in `CACHES`, the ORM cache itself among them, kept serving stale results. They now invalidate every cache they can reach before raising `InvalidationError` for the ones that failed.
- A `CACHEX_ORM["LEASE_TIMEOUT"]` that is not a positive number of seconds went unreported: `0` gave writes leases that expire at once, and `None` made every write raise `InvalidationError`. The new `cachex_orm.E008` check reports it.
- `DatabaseCache.scan()` skipped keys when others were deleted mid-iteration, so a loop that scans and deletes left about half of them. Its cursor is now a position in hash order, like `LocMemCache`'s. The 0.11.0 fix had missed it.
- On valkey-glide, `xreadgroup()` with id `0` raised `TypeError` when a pending entry had been removed by `XDEL` or `XTRIM`. That entry now comes back with an empty field dict, as on redis-py and valkey-py.
- On valkey-glide, each sync blocking call (`blpop()`, `brpop()`, `blmove()`, `xread()` and `xreadgroup()` with `block`, or a pipeline holding one) built a client and kept about 2 KB of it for the life of the process, because glide registers every client in a fork hook. Those calls now reuse idle clients.
- On Redis before 7.0, `set(nx=True, get=True)` and its pipeline form raised the driver's `syntax error`. They now raise `NotSupportedError`, as the [requirements](../getting-started/installation.md#requirements) say.
- On valkey-glide, `flush_db()`, `aflush_db()` and the admin's Flush database button sent `FLUSHDB SYNC`, so a large flush blocked the server and raised `TimeoutError` after `request_timeout` (250 ms by default), though the flush went through. They now send a plain `FLUSHDB`, as redis-py and valkey-py do, and the server's `lazyfree-lazy-user-flush` decides whether to free the keys in the background.
- With stampede prevention on, `set(key, value, nx=True)` wrote nothing while the old value sat in its stampede buffer, where `get()` already returns `None`, so the refill after a miss failed and every request recomputed the value for up to `buffer` seconds. With `get=True`, `set()` returned that old value, and with `xx=True` it overwrote it. `set()` now counts that key as absent with every flag, as `add()` does: `nx=True` writes, `xx=True` writes nothing, and `get=True` returns `None`.
- With stampede prevention on, `get()` and `get_many()` returned a key's old value again in its last half second, once its `TTL` read 0, though they had returned `None` for the rest of its buffer and `add()` counted it as absent. They now return a miss there too, and so does `TrackingCache`.
- A tuple in `OPTIONS["serializer"]` or `OPTIONS["compressor"]` was taken as one codec instead of a fallback chain, so the first write or read raised `AttributeError`. A tuple now works like a list.
- The pipeline's `lpop()`, `rpop()`, `spop()`, `zpopmax()` and `zpopmin()` with a negative `count`, `linsert()` with a position other than `BEFORE` or `AFTER`, and `lpos()` with `rank=0` or a negative `count` or `maxlen` queued the command anyway, so `execute()` ran the commands queued before it and then raised the driver's error. They now raise `ValueError` when queued, like the cache methods.
- `full_encode_pre` encoded an `Encoded(...)` arg as the wrapper, so `get()` returned `Encoded(value=...)` instead of the value, or the JSON serializer raised `SerializerError`. It now encodes the wrapped value.
- The admin showed Add key to users with `add_key` but not `change_key`, then gave them a key page with no way to add the first value. `add_key` now creates a key that does not exist yet; editing an existing key still takes `change_key`.
- The admin gave a user with `add_key` but not `view_key` the Add key form, and its Continue button answered with a 403. That user can now add the first value, which creates the key, and then lands on the admin index, as in Django's admin. Existing keys stay closed to them. The breadcrumbs and Back links of the Add key form and the key page led to the cache list and the key list, which answered that user with a 403 too. A list the user cannot open is now plain text in the breadcrumbs, and the Back link to it is left out. An error such as an unknown alias or an unreachable server also redirected to one of those lists, so the user got a 403 instead of the error message. It now redirects to the admin index when the user cannot open the list. The key list's breadcrumbs and its Cache Details button linked the cache list and the cache info page, which answered a user without `view_cache` or `change_cache` with a 403. For that user, those crumbs are now plain text, and the button is left out.
- The admin's Clear tool said it removes only the current cache version, but on `LocMemCache` and `DatabaseCache` it removes every version, and on `DatabaseCache` the whole table, which other aliases may share. Its confirmation and message now say what goes.
- The admin's key page read and rendered a string value in full, so a 50 MB value blocked Redis and made a 50 MB page. A string over 1 MiB now shows only its size, also when it is stored compressed in less; its TTL and Delete still work. On the Valkey and Redis backends, the page does not read a value stored in over 1 MiB.
- With stampede prevention on, the admin's TTL form shows 0 for a key inside its stampede buffer, and 0 removes the expiry, so saving the form untouched made a key that was about to expire persistent. Saving the form without changing the TTL now leaves the expiry alone.
- One key name that is not valid UTF-8 made `scan()` and `ascan()` raise `UnicodeDecodeError`, so the admin's key list showed an error instead of any key. They now return that name with its bad bytes escaped, as in `bad\xff`, next to the other keys.
- `LocMemCache`'s `zadd()`, `zincrby()`, `zrem()`, `zrank()` and `zrevrank()` raised `ValueError` for a member equal to a stored one with another string form, such as `1` or `True` for a stored `1.0`, and a failed `zrem()` left the member in `zrange()` but not in `zcard()`. They now act on the stored member.
- `LocMemCache.delete()` returned `True` for an expired key, which `get()` and `has_key()` already treated as missing. It now returns `False`, as Redis does, and still removes the key.
- `LocMemCache`'s `aget()`, `aadd()`, `atouch()`, `adelete()`, `aget_or_set()`, `adelete_many()`, `aclear()` and `aclose()` went through `sync_to_async` and a worker thread, although the docs say its async methods call the sync method directly. They now do.
- After a fork, as under gunicorn `--preload` or Celery prefork, the child's first `TrackingCache` write, `clear()` or `shutdown()` could hang for good when a thread of the parent, such as its listener, held the local store's lock at the fork. Writes now switch the child to a fresh store first, as reads already did.
- After a fork, the child's first `TrackingCache` read or write, or a new thread's first use of the cache there, could hang for good when another thread of the parent was setting up its own `TrackingCache` at the fork and held the lock that setup takes. The child now starts with a fresh lock.
- After a fork, the child's first ORM query or write could hang for good when the ORM cache was a `TrackingCache` and another thread of the parent was starting one at the fork, holding the lock that each one takes first. The child now starts with a fresh lock.
- The redis-py and valkey-py backends, Sentinel and cluster included, opened another connection pool once an object in `OPTIONS` changed state, such as a credential provider caching a renewed token or a `Retry` that the asyncio cluster client adds errors to, and the old pool stayed open. Such an object now keys the same pool for as long as it lives.
- With `retry_on_timeout=True`, the redis-py and valkey-py backends, Sentinel included, let the driver add `TimeoutError` to the list in `OPTIONS["retry_on_error"]` (or in `sentinel_kwargs`) for each new connection, so the next cache instance, as in each new thread, opened another connection pool and the old ones stayed open. The drivers now get their own copies of the lists, dicts and sets in `OPTIONS`, which stay as given.
- On the redis-py and valkey-py cluster backends, a thread connecting to a slow or unreachable cluster held a process-wide lock through node discovery, so every other cluster alias waited for it, and each command rebuilt the key of the shared cluster client. Discovery now runs outside the lock, and a cache instance looks its client up once.
- On valkey-glide, a thread or task connecting to a slow or unreachable server held a lock that all aliases share, process-wide for sync calls and per event loop for async ones, so every other alias that had not connected yet waited until it gave up. Each configuration now connects under a lock of its own.
- On redis-py 7, with `OPTIONS["socket_timeout"] = None` on valkey-py, and in valkey-py's sync Sentinel lookups, a connect had no timeout, so an unreachable host held each call for the kernel's TCP timeout of about two minutes. A connect now gives up after 5 seconds unless `socket_connect_timeout` or `socket_timeout` says otherwise in `OPTIONS`, the `LOCATION` query or `sentinel_kwargs`.
- On `ValkeyGlideClusterCache`, the `ImproperlyConfigured` error for seed URLs that disagree on TLS, username or password called the `LOCATION` list a primary and its replicas. It now says that valkey-glide applies one set of these settings to every URL in the list.

### Documentation

- The ORM cache guide says that under django-tenants' per-tenant `search_path` or a row-level security policy, one tenant can be served another's cached rows, and shows a `QUERY_KEYGEN` that adds the tenant to the query key.
- The quickstart and the configuration reference say that the redis-py backends reject `valkey://` and `valkeys://` URLs, with a `ValueError` on the first cache call.
- The API reference, the async guide and the `TrackingCache` docs no longer claim async twins for `info()`, `slowlog_get()` and `slowlog_len()`, which have none.
- The `TrackingCache` guide says which calls open the listener connection. Reads and `info()` do, writes do not.
- The configuration reference says that concurrent `DatabaseCache` writes on SQLite can fail with `database is locked` under Django's default deferred transactions, and that `"transaction_mode": "IMMEDIATE"` in the database's `OPTIONS` makes them wait for the lock.
- The configuration reference lists exclusive score bounds such as `"(5"` among what `LocMemCache` and `DatabaseCache` raise `NotSupportedError` for.
- The configuration reference said every other `OPTIONS` key goes to the driver. It now says that the Valkey/Redis backends reject `decode_responses` and that `MAX_ENTRIES` and `CULL_FREQUENCY` do nothing there.
- The stampede prevention docs say that `{}` turns it off, that unknown dict keys are dropped with a warning, so a dict of misspelled keys means the defaults, and that bad field values raise `TypeError` or `ValueError`, not `ImproperlyConfigured`.
- The stampede prevention docs say that counters and data structures get the buffer too, and how to keep it off them.
- The admin guide's Backend Abilities table said that `LocMemCache` and `DatabaseCache` have no conflict detection on edit. It now says they detect type changes only.
- The API reference said that a pipelined `zadd(..., incr=True)` sends `ZINCRBY`, which takes none of the `nx`, `xx`, `gt` and `lt` flags. It now says that the pipeline sends `ZADD ... INCR`.

## 0.12.1 (October 2026)

### Fixes

- The redis-py and valkey-py backends, Sentinel included, share sync connection pools across the process, as they do async ones. Django builds a cache instance per thread and per asyncio task, so each new thread opened new connections, and under ASGI so did each request that made sync cache calls. `OPTIONS["max_connections"]` now caps one pool that every thread shares, along with every alias with the same `LOCATION` and connection options. redis-py 8 defaults it to 100, so a process with more than 100 cache commands in flight at once gets `Too many connections` unless it raises the cap or uses `BlockingConnectionPool`.

## 0.12.0 (October 2026)

### Improvements

- The admin's Flush action, Clear tool and danger zone are off by default, for superusers too. Set `CACHEX_ADMIN = {"ALLOW_FLUSH": True}` to turn them back on; they still need the `change_cache` permission. See [Flushing Caches](../user-guide/admin.md#flushing-caches).

## 0.11.1 (September 2026)

### Fixes

- `flush_db()` and `aflush_db()` raise `NotSupportedError` on `LocMemCache`, `DatabaseCache` and `TrackingCache`, like `clear_all_versions()`, instead of `AttributeError`.

### Documentation

- The API reference lists `info()`, `slowlog_get()`, `slowlog_len()`, and the `version_src` and `version_dst` arguments of `rename()` and `renamenx()`.
- The semaphore docs say that `release()` after an expired lease logs a warning, and that the semaphore keys survive `clear()` and `clear_all_versions()`.
- The docs say that a pipelined `get()` skips the stampede check, that `scan()` raises `NotSupportedError` on the redis-py and valkey-py cluster backends, and which options a cluster `LOCATION` list takes from its first URL.
- The serializers guide warns that anyone who can write to the cache server can run code through pickle. The admin guide says that Flush on Django's own `RedisCache` runs `FLUSHDB`.

## 0.11.0 (September 2026)

### Breaking changes

- redis-py 7.2 is the oldest supported release (`redis>=7.2,<9`), up from 6.0. Older releases mishandle async pipeline errors and async cluster scripts.
- On valkey-glide, `xread()`, `xreadgroup()` and their async and pipelined forms return `{}` instead of `None` for an empty read. Test with `if not result`.
- `execute()` on `RespPipelineProtocol` and `RespAsyncPipelineProtocol` takes a keyword-only `raise_on_error=True`, which custom adapters must accept. With `False`, errors are returned as results.

### Features

- `django_cachex.orm` is an opt-in ORM query cache derived from django-cachalot 2.9.1. With the app in `INSTALLED_APPS`, it caches PostgreSQL and SQLite query results in a Redis, Valkey, `TrackingCache` or `LocMemCache` alias and invalidates the tables each write touches. A query running while a write commits cannot store a stale result, subqueries count with their tables, and queries using `Now()` are not cached. The API is `invalidate()`, `orm_cache_disabled()`, `table_generations()` and the `invalidate_orm_cache` command. See [ORM Cache](../user-guide/orm-cache.md), and [Migration](../migration.md#from-django-cachalot) for moving from cachalot and for the cachalot settings it drops.

### Improvements

- Admin hash and set pages on the RESP backends transfer only the page shown; stream pages read at most about half the stream.

### Fixes

- On valkey-glide, `blpop()`, `brpop()`, `blmove()`, and `xread()` and `xreadgroup()` with `block` get a short-lived client per call, direct, async or pipelined, instead of stalling every other command. The client waits for the block plus `request_timeout` (250 ms by default); a block of `0` is not cut short.
- `zpopmin()` and `zpopmax()` without `count` raised `ValueError` under `OPTIONS["protocol"] = 3` on redis-py and valkey-py, directly and in a pipeline.
- For a missing key, `Pipeline.rename()` raises `KeyNotFoundError` from `execute()` and `Pipeline.renamenx()` returns `False`, instead of the driver's error.
- With stampede prevention active, `add()` and `aadd()` overwrite a key whose TTL entered the stampede buffer, which `get()` reports as missing.
- `get_or_set()` and `aget_or_set()` with `stampede_prevention=False` still added the stampede buffer to the stored TTL.
- `RedisClusterCache` and `ValkeyClusterCache` seed discovery from every `LOCATION` URL, so they connect while the first node is down. Credentials, TLS and other options still come from the first URL.
- `decode_list_post` keeps a nil element as `None` instead of raising `TypeError`. Serializers and compressors given non-bytes raise `SerializerError` or `CompressorError` instead of `TypeError`.
- The default `scan()` of `LocMemCache` and `DatabaseCache` skipped keys when others were deleted mid-iteration. Keys now come in hash order, sorted per page.
- `LocMemCache` and `DatabaseCache` follow Redis more closely: `persist()` without a TTL returns `False`, `hdel()` and `zrem()` count duplicates once, `zadd()` and `zincrby()` reject NaN scores with `ValueError`, a negative `start` in `zrangebyscore()` or `zrevrangebyscore()` returns `[]`, and `LocMemCache.sadd()` with an unhashable member raises before adding the others.
- `DatabaseCache.sdiff()` and `sinter()` of a single key returned an internal set type that `set()` stored as a set-typed key.
- `DatabaseCache` collection writes that trigger a `MAX_ENTRIES` cull no longer risk deadlocking with a concurrent cull.
- A `TrackingCache` listener that connects after `shutdown()` is closed instead of replacing its successor and clearing the local store. Under `coherence="ttl"`, a forked instance drops the parent's local store.
- An in-process `Semaphore.acquire()` interrupted while waiting (`KeyboardInterrupt`, Celery's soft time limit) leaves the queue instead of blocking every later `acquire()`.
- `RespSemaphore.acquire()` and `aacquire()` reject a NaN `timeout` with `ValueError` instead of waiting forever, and a second interrupt or cancellation while abandoning one no longer makes the next `acquire()` raise `SemaphoreError`.
- The admin renders stream entries with an `items` field, and key-list paging links keep an `items` query parameter.
- The admin hash page keeps leading and trailing spaces in field names instead of stripping them.
- Renaming a sorted-set member in the admin onto an existing member is refused instead of overwriting that member's score and dropping the old one. On the RESP backends, the rename is also refused when the score changed since the page loaded.
- The admin's Flush database text wrongly said a cluster flush only reaches the connected primary, and a quote in a translation broke the confirmation button.
- The wheel and sdist ship `LICENSE.django-redis` (BSD-3-Clause) for the serializer, compressor and exception code derived from django-redis. The README and docs home page say which parts it covers.

### Documentation

- `clear()` was documented as safe on a shared database; it deletes every key under its `KEY_PREFIX` and `VERSION`, so apps need distinct prefixes. The docstring and API reference now say so.
- The README and docs promised automatic key prefixing and value encoding in `eval_script()`, which only applies them through a `pre_hook` such as `keys_only_pre`. The Lua guide's example now passes one.
- The docs list the multi-key commands redis-py and valkey-py cluster pipelines refuse: `rename`, `renamenx`, `smove`, `sdiff`, `sinter`, `sunion` and the `store` variants.
- The configuration guide lists `sscan()`, `sscan_iter()` and `clear_all_versions()` among the methods `LocMemCache` and `DatabaseCache` do not support.
- The LocMemCache vs fakeredis page is gone, the `TrackingCache` guide is half as long, and other pages drop filler.

### Tooling

- CI builds the docs with `mkdocs build --strict` on pull requests.
- CI runs the cache tests over RESP3 and against the oldest supported client libraries (redis-py 7.2.0, valkey-py 6.1.0).
- The release workflow runs the ORM cache tests on SQLite with `LocMemCache` and on PostgreSQL with Redis before tagging.

## 0.10.0 (September 2026)

### Breaking changes

- `StreamCache` and the `django_cachex.cache.stream` module are removed; `TrackingCache` covers the same use case with a coherence guarantee.
- The redis-py and valkey-py cluster backends reject `OPTIONS["parser_class"]`, `["pool_class"]` and `["async_pool_class"]` with `ImproperlyConfigured` at `caches[alias]` instead of silently dropping them. Remove them; the C parser comes from installing `hiredis` or `libvalkey`.
- `OPTIONS["decode_responses"] = True` raises `ImproperlyConfigured` on every RESP backend; it broke every read.
- `TrackingCache` rejects `KEY_FUNCTION`, `VERSION`, `TIMEOUT` and an explicit `KEY_PREFIX: ""` on its own alias, directly or in `OPTIONS`; set them on the transport alias.
- `lock()` and `alock()` on the redis-py and valkey-py backends return a wrapper that raises `django_cachex.lock.LockError` and `LockNotOwnedError`, subclasses of `ValueError`, with the driver error as `__cause__`; only `except redis.exceptions.LockError` needs changing. The wrapper forwards other attributes, including assignment, and supports `copy.copy()`.
- Pipeline parameter names match `RespCache`: `smove(src, dst, member)` (was `source`, `destination`), `sunionstore(dest, keys)` (was `destination`), `member` (was `value`) in `zscore()`, `zrank()`, `zrevrank()` and `zincrby()`, and `*members` (was `*values`) in `sadd()` and `zrem()`. `lmove()` no longer defaults `wherefrom` and `whereto` to `"LEFT"` and `"RIGHT"`.
- `Pipeline.get(key, default=None, version=None)` gained `default`, so `pipe.get("k", 2)` now means default 2, not version 2; pass `version=` by keyword.
- `add()` and `set()` with `nx`, `xx` or `get` and `timeout=0` now use `PXAT` on every RESP backend, closing a window where the key had no TTL. `PXAT` needs Redis 6.2, now the documented minimum.
- Sentinel backends reject a multi-entry `LOCATION` list with `ImproperlyConfigured`; Sentinel nodes go in `OPTIONS["sentinels"]`. `OPTIONS["async_pool_class"]` must be the driver's async `SentinelConnectionPool` or a subclass, and is now used.
- `OPTIONS["stampede_prevention"]` accepts only `bool`, `dict`, `StampedeConfig` or `None`; other values, such as the string `"False"`, raise `ImproperlyConfigured` instead of enabling it. A `StampedeConfig` is used as is.
- `TrackingCache` aliases sharing one `LOCATION` must agree on `transport`, `coherence`, `prefixes`, `local_timeout`, `MAX_ENTRIES`, `poll_timeout`, `health_check_interval` and `reconnect_delay`; a mismatch raises `ImproperlyConfigured`. The four timing options must be positive finite numbers.
- Cluster `incr_version()` and `decr_version()` reject a `KEY_FUNCTION` that puts the version inside the `{...}` hash tag with `NotSupportedError`, not `CROSSSLOT`; the message names both methods.
- `xautoclaim(..., justid=True)` raises `NotSupportedError` in pipelines and on the redis-py / valkey-py cluster backends, where it returned `""` as the cursor; call it without `justid` there.
- `django_cachex.admin.views` exports `cache_detail_view`, `key_add_view` and `key_detail_view`; the underscore-prefixed names are gone. `django_cachex.adapters.redis_py._REDIS_AVAILABLE` is no longer in that module's `__all__`.
- The valkey-glide pipeline raises `AttributeError` for unknown attributes instead of queuing them as commands; use `execute_command(*args)`.
- For custom adapters: `RespAdapterProtocol` gained `memory_usage()`, `amemory_usage()` and `get_async_client()`, `keys()` declares `pattern="*"`, `xadd()` returns `str | None`, and the pipeline protocol gained `memory_usage()`, `zadd(incr=)` and `zrange(desc=)`.

### Features

- `Encoded` and `encoded_pre` for `eval_script()`: wrap only the ARGV entries to encode, a middle ground between `keys_only_pre` and `full_encode_pre`. An `Encoded` reaching the adapter unwrapped, in `keys` or nested raises `TypeError`.
- `memory_usage(key, version=None, *, samples=None)` and `amemory_usage()` return a key's `MEMORY USAGE` in bytes (`None` if missing), on the RESP backends, pipelines and `TrackingCache`. `LocMemCache`, `DatabaseCache` and the new `BaseCachex` defaults raise `NotSupportedError`.
- `largest_keys(pattern="*", count=10, version=None, *, samples=None, itersize=None)` and `alargest_keys()` return the `count` largest keys matching `pattern` as `(key, bytes)` pairs, largest first. `count=0` returns `[]` and a negative count raises `ValueError`.
- `Pipeline` gained `set(..., get=True)`, `zadd(..., incr=True)` and `zrange(..., desc=True)`, decoding as on the cache.
- `django_cachex.script_sha()` is exported from the package root.
- `LocMemCache` semaphores gained `extend()` and `aextend()`, returning whether the claim is still held.
- `LocMemCache` and `DatabaseCache` implement `zrevrangebyscore()` and `azrevrangebyscore()` with Redis semantics: `max_score` then `min_score`, highest first, `LIMIT` applied to the descending order, `withscores` supported.

### Improvements

- `eval_script()` and `aeval_script()` send `EVALSHA`, loading the script on `NOSCRIPT`; valkey-glide does the same with `Script` objects, including its lock release and extend scripts. Pipelines still send `EVAL`.
- Configuration errors surface at `caches[alias]`, not on the first command: a blank or missing `LOCATION` raises `ImproperlyConfigured` with an example URL, and a missing driver, a bad `pool_class` or a rejected Sentinel `LOCATION` fail there too.
- `TrackingCache` under `coherence="ttl"` rolls the stampede dice on the key's remaining server TTL, not the `local_timeout` cap. `has_key()` no longer rolls, and `get_or_set()` reads its own write back without rolling.
- `TrackingCache.delete_pattern()` evicts local copies even under a custom `KEY_FUNCTION` without `REVERSE_KEY_FUNCTION`, and `delete_many()` no longer runs the key function under the store lock.
- `TrackingCache` logs one traceback per listener outage, then a one-line warning per later attempt, instead of a traceback every second.
- Cluster `delete_many()`, `delete_pattern()` and `set_many(timeout=0)` send one `UNLINK` per batch.
- Sentinel async pool lookups compute their registry key once per server; the registry is only mutated under its lock.
- The `CLIENT TRACKING` listener no longer reconnects silently without tracking; `TrackingCache` rebuilds it instead.
- `aclose()` on the redis-py and valkey-py adapters keeps the remaining pools registered when one fails to disconnect, and its docstring covers aliases sharing a pool.
- The admin key list fetches TTL, type and size per `SCAN` batch on redis-py and valkey-py, and hash and set pages fetch less (sets in server order). Values and edit fingerprints are read atomically, the TTL form only appears on backends with `expire()` and `persist()`, and quoted driver errors mask URL credentials.
- A transport's `NotSupportedError` passes through `TrackingCache` unchanged, with the server's reason.
- valkey-glide: a multi-URL `LOCATION` whose URLs differ only in a value `OPTIONS["db"]`, `["username"]` or `["password"]` overrides is accepted.
- CI runs `ruff check --no-fix`; plain `ruff check` fixed the checkout and passed.
- CI tests the Django version each matrix cell names, runs free-threaded tests with `PYTHON_GIL=0`, fails when `glide` cannot be imported, and lints with the full pre-commit hook set.
- `LocMemCache.zremrangebyscore()` runs in O(log N + k) instead of scanning every member.
- `clear_all_versions()` and `aclear_all_versions()` default to `NotSupportedError` on `BaseCachex`, so `LocMemCache`, `DatabaseCache` and `TrackingCache` raise it instead of `AttributeError`.
- CI tests the installed wheel, not the checkout, and runs the cache tests on Redis 6.2 and Valkey 7.2.
- `expiretime()` is documented as needing Redis 7.0+ (every Valkey release has it), and the `NotSupportedError` an older server raises names that release.

### Fixes

- The PyPI `Changelog` link pointed at the nonexistent `reference/changelog/`; it now points at `latest/reference/changelog/`.
- Nil stream entries no longer crash `pipe.execute()` for `xclaim()`, `xautoclaim()`, `xread()` and `xreadgroup()`; they decode to `(id, {})`.
- `zadd()` with an empty mapping returns `0`, and `xadd()` with no fields raises `ValueError`, instead of the driver's `DataError`.
- `KEY_PREFIX` glob escaping covers backslashes; a `\` made `keys()`, `iter_keys()`, `scan()`, `delete_pattern()`, `clear_all_versions()` and `make_pattern()` match a different prefix or nothing.
- `NotSupportedError` and `KeyNotFoundError` survive `pickle`, `copy.copy()` and `copy.deepcopy()` with their message and `operation`, `backend`, `detail` and `key` intact.
- The admin key pages no longer return HTTP 500 for an unreachable backend or overwrite a key whose type changed since the page loaded. Key add rejects types outside the creatable set, and the Help link on an unmodelled type shows the generic text instead of nothing.
- valkey-glide: a `LOCATION` it cannot dial, such as a unix-socket URL, raises `ImproperlyConfigured` instead of connecting to `localhost:6379`. `xtrim()` without `maxlen` or `minid` raises `ValueError`, the pipelined `xread()` returns `None` for an empty read, `aget_many()` raises `CachexError` when its TTL batch fails instead of serving every key as fresh, and `aclose()` no longer leaves per-loop registry entries behind.
- `DatabaseCache.incr()`, `decr()` and the async twins no longer lose concurrent increments or reset the key's timeout; a collection key raises `WrongTypeError`.
- `incr_version(key, 0)` and `decr_version(key, 0)` on `LocMemCache` and `DatabaseCache` leave the key in place instead of deleting it.
- `LocMemCache.ttl()` rounds like Redis `TTL`; a key just written with `timeout=300` read `299`.
- `hset()` with an odd-length `items` list on `LocMemCache` and `DatabaseCache` no longer leaves a half-written hash.
- `zadd()` on `LocMemCache` and `DatabaseCache` rejects conflicting `nx`/`xx`/`gt`/`lt` combinations with `ValueError` instead of silently accepting them.
- `DatabaseCache.info()` with a missing cache table on PostgreSQL no longer poisons the caller's open transaction.
- The semaphore state hash no longer carries an unused `capacity` field.
- `OPTIONS["username"]` and `OPTIONS["password"]` override `LOCATION` URL credentials, in userinfo or query string, on the redis-py and valkey-py standalone, Sentinel and cluster backends, as documented.
- `Pipeline.zadd()` with an empty mapping and `Pipeline.hmget()` with no fields return `0` and `[]` instead of raising `DataError` or failing the batch.
- Sentinel `aclose()` removes the current loop's Sentinel manager under the async registry lock.
- valkey-glide: conflicting `zadd()` flags, `set()` with both `nx` and `xx`, and `zrangebyscore()` or `zrevrangebyscore()` with only one of `start` and `num` raise `ValueError`, in pipelines too, instead of being silently mishandled. Dynamically formatted Lua sources no longer leak memory.
- A multi-server `LOCATION` with a trailing separator or blank entries no longer produces an empty server URL.
- On the RESP backends, invalid `spop(count=...)`, `lpos(rank=..., count=..., maxlen=...)` and `linsert(where=...)` arguments raise `ValueError`, not `ResponseError`.
- A `KeyboardInterrupt` or `asyncio.CancelledError` landing while a semaphore `acquire()` reply is in flight no longer leaks the claim.
- `Semaphore.extend()` and `aextend()` on the RESP backends return `False` after `release()` or when never acquired, instead of raising `SemaphoreError`.
- The admin masks `?password=` and `&password=` query parameters in cache URLs, and the hash field-name input is read-only without the change permission.
- The `RespClusterCache.lock()` and `alock()` docs no longer blame `EVALSHA` for their `NotSupportedError`.
- `LocMemCache` and `DatabaseCache` on Django 6.0: `aincr()`, `adecr()`, `ahas_key()`, `aget_many()`, `aincr_version()` and `adecr_version()` (and `adelete_many()` on `DatabaseCache`) use the sync implementation. `aincr()` no longer resets the TTL, and `ahas_key()`, `aget_many()` and `aincr_version()` no longer raise `WrongTypeError` on collection keys.
- `TrackingCache.has_key()` and `ahas_key()` no longer count as hits in `info()["tracking"]["hits"]` or refresh the entry's LRU position.
- The admin cache changelist no longer returns HTTP 500 when an alias's backend constructor raises; the row shows the masked error, and that alias's other pages redirect to the changelist with a message.
- The admin hash detail page says "No fields on this page." instead of "Hash is empty." when a concurrent delete emptied only the page.
- The admin key add form only offers types the backend can write (no `Stream` on `LocMemCache` and `DatabaseCache`, only `String` on `TrackingCache`), and stock Django backends get no "Add key" link.
- Removing a collection's last member in the admin keeps the user on the key in create mode instead of showing a "does not exist" error.
- The admin key list on a `TrackingCache` alias no longer logs a traceback per container key; their size column stays empty.
- The list form of `LOCATION` is cleaned like the string form: entries are stripped, blanks dropped, non-string entries raise `ImproperlyConfigured`, and a list with no usable entry raises the same error as an empty string.
- `decode_responses` with any value in a `LOCATION` query string is rejected like `OPTIONS["decode_responses"]`.
- `lpop()`, `rpop()`, `zpopmin()`, `zpopmax()` and the async twins on the RESP backends raise `ValueError` for a negative `count`.
- `zadd()` with conflicting flags and `zrangebyscore()` / `zrevrangebyscore()` with only one of `start` and `num` raise `ValueError`, not `DataError`, on redis-py, valkey-py and every pipeline at queue time.
- `cache.lock()` and `alock()` reject a `lease` below one millisecond with `ValueError` instead of failing every acquire.
- `RespSemaphore.acquire()` and `aacquire()`: a second cancellation, interrupt or error during an interrupted acquire's cleanup no longer leaks the claim or leaves the instance held.
- `Semaphore.extend()`, `aextend()` and the RESP semaphore `lease` reject `NaN` and infinity with `ValueError` instead of an unrelated `ValueError` or `OverflowError`; the local `extend()` silently returned `True`.
- `Pipeline.zadd(incr=True)` raises `ValueError` at queue time on every driver unless the mapping holds exactly one member; redis-py and valkey-py raised `DataError`, and valkey-glide failed mid-batch.
- `Pipeline.xadd()` with empty `fields` raises `ValueError` at queue time, like `cache.xadd()`, instead of a driver-specific error.
- `TrackingCache.aget_or_set()` awaits an `async def` default instead of serializing the coroutine object.
- `TrackingCache` rejects Django's legacy lowercase `timeout` key on its own alias or `OPTIONS` with `ImproperlyConfigured` instead of silently ignoring it.
- `LocMemCache` and `DatabaseCache` `zrangebyscore()` / `zrevrangebyscore()` with only one of `start` and `num` raise `ValueError` instead of ignoring the window.
- `LocMemCache` and `DatabaseCache` `hset(items=...)` raise the RESP backends' `ValueError` for an odd-length list.
- `DatabaseCache.zpopmin()` and `zpopmax()` with `count=0` return `[]` without rewriting the row.
- The wheel CI job failed with `No module named 'tests'`.
- `DatabaseCache.keys()` and `delete_pattern()` no longer assume Django's `prefix:version:key` layout; under a custom `KEY_FUNCTION`, `keys()` returned mangled keys and `delete_pattern()` deleted nothing.
- A `TrackingCache` instance created before a fork (gunicorn `--preload`, Celery prefork, warmed at import time) drops the inherited store in the child and starts its own listener without logging `listener thread died, restarting` in every worker.

## 0.9.0 (September 2026)

### Breaking changes

- `InvalidationListenerProtocol.client_ids`, the `(subscriber, tracker)` pair, is now `client_id`, a single `int`. The listener holds one connection.

### Improvements

- The `CLIENT TRACKING BCAST` listener behind `TrackingCache` runs over one RESP3 connection instead of two RESP2 ones.

## 0.8.0 (September 2026)

### Breaking changes

- `TieredCache` is removed; `TrackingCache` with `OPTIONS["coherence"] = "ttl"` replaces it. Point `transport` at the L2 alias, rename `l1_timeout` to `local_timeout`, move `MAX_ENTRIES` to the `TrackingCache` alias and drop the L1 alias. The transport must be a cachex Valkey/Redis backend; a stock Django L2 has no replacement.
- `info()`, `slowlog_get()` and `slowlog_len()` raise `NotSupportedError` on backends that lack them instead of an empty result. `LocMemCache`, `DatabaseCache`, `StreamCache` and `TrackingCache` implement `info()`; only the Valkey/Redis backends have a slow log, and `TrackingCache` delegates it to its transport.
- `Pipeline.zcount()`, `zrangebyscore()`, `zrevrangebyscore()` and `zremrangebyscore()` take `min_score` and `max_score` instead of `min` and `max`, and `start` and `num` are keyword-only on `zrangebyscore()` and `zrevrangebyscore()`, matching `RespCache`. Callers passing the bounds positionally are unaffected.
- The valkey-glide adapter's keywords match `RespAdapterProtocol`: `zcount(key, min_score=..., max_score=...)` (was `mn`/`mx`), `sdiffstore(dest=...)` (was `dst`), `xack(*entry_ids)` (was `*ids`). `zadd()` takes keyword-only `nx`, `xx`, `ch`, `gt` and `lt` instead of silently ignoring unknown `**kwargs`, and the sorted-set range methods take keyword-only `withscores`, `desc`, `start` and `num`.
- The valkey-glide pipeline's stream keywords match the protocol: `xclaim(message_ids=...)`, `xgroup_create(id=...)`, `xgroup_setid(id=...)`, `xdel(*entry_ids)`, `pexpire(milliseconds=...)`, and `xpending_range()` takes keyword-only `min`, `max`, `count`, `consumername` and `idle`.
- `ValkeyGlidePipelineAdapter` no longer has the undocumented `mget()` and `mset()`.
- A `LOCATION` list whose URLs disagree on TLS scheme, username, password or database raises `ImproperlyConfigured` on the valkey-glide backends instead of silently using the first URL's settings.

### Features

- `TrackingCache`: a local read cache over an existing redis-py or valkey-py alias, kept coherent by `CLIENT TRACKING` broadcasts. Nothing is cached while its listener is down; cluster and valkey-glide transports are rejected. With `OPTIONS["coherence"] = "ttl"` no listener runs, `local_timeout` alone bounds staleness, and any transport works. See [Composite backends](../user-guide/composite-backends.md#trackingcache).
- Adapters gained `invalidation_listener(prefixes)`, a `CLIENT TRACKING BCAST` subscription; the cluster and valkey-glide adapters raise `NotSupportedError`.

### Improvements

- Hash field expiration on the redis-py, valkey-py and valkey-glide adapters, with async twins and pipeline support: `hexpire()`, `hpexpire()`, `hexpireat()`, `hpexpireat()`, `httl()`, `hpttl()`, `hexpiretime()` and `hpersist()` set, read and remove per-field TTLs, and `hsetex()`/`hgetex()` write or read fields while setting their TTL. They need Redis 7.4+ or Valkey 9.0+ (`hsetex()`/`hgetex()`: Redis 8.0+ or Valkey 9.0+) and raise `NotSupportedError` on older servers.
- Key deletion sends `UNLINK` instead of `DEL` on every RESP adapter: `delete()`, `delete_many()`, `delete_pattern()`, `set(timeout=0)`, the pipeline `delete()` and the cluster per-slot paths.
- `NotSupportedError` gains a `detail` attribute, and the RESP adapters, cluster included, raise it instead of a driver `ResponseError` for a command the server lacks, with `operation` set to the command name.
- `TrackingCache` delegates `slowlog_get()` and `slowlog_len()` to its transport, alongside `info()`.
- `DatabaseCache.incr_version()`, `decr_version()` and the async twins move the key like Redis `RENAME`: a collection key no longer raises `WrongTypeError`, and the key keeps its remaining TTL instead of the cache default.
- `StampedeConfig` validates its arguments on construction (`buffer` a non-negative `int`, `beta` and `delta` finite non-negative numbers), so a bad `stampede_prevention` option fails at configuration instead of on every timed write.

### Fixes

- A collection command with no members is a no-op on every backend, direct, async and pipelined, instead of a driver error: `sadd()`, `srem()`, `hdel()`, `hset()`, `lpush()`, `rpush()`, `zrem()`, `xdel()` and `xack()` return `0`, `smismember()` and `zmscore()` an empty list. On `LocMemCache` and `DatabaseCache`, `lpush()` and `rpush()` on an existing list used to return its length. On valkey-glide this replaces the 0.7.0 `ValueError` for `hset()` with an empty mapping.
- `hset(key, items=[...])` rejects an odd-length `items` list with `ValueError` instead of sending mis-paired arguments.
- Sentinel backends keep Sentinel discovery and failover when `LOCATION` uses a TLS scheme (`rediss://` or `valkeys://`).
- `xread()` and `xreadgroup()` decode correctly under `OPTIONS = {"protocol": 3}`, both directly and on a pipeline, where they raised `ValueError`.
- Stream reads decode a nil entry (from Redis 6 `XCLAIM`) to an empty field dict instead of raising.
- `aclose()` disconnects only the calling alias's pools, and on cluster its own client, on the running event loop. It used to disconnect every alias's pools, dropping other aliases' in-flight connections.
- Sentinel `aclose()` no longer leaks discovery clients when a different adapter instance runs it, as under asgiref.
- Pipelined `sadd()` rejects an unhashable member like `sadd()` does, instead of storing it and making every later read of the key raise `TypeError`.
- Pipelined hash field commands (`hexpire()`, `hpexpire()`, `hexpireat()`, `hpexpireat()`, `httl()`, `hpttl()`, `hexpiretime()`, `hpersist()`, `hgetex()`) called with no fields return `[]` instead of failing the whole batch.
- `keys()`, `scan()`, `iter_keys()` and `delete_pattern()` on `DatabaseCache` match case-sensitively on SQLite.
- `DatabaseCache` no longer raises `OverflowError` storing a `timeout=None` key under `USE_TZ = True` with a database `TIME_ZONE` east of UTC.
- `DatabaseCache` reports the right TTL and expiry when the database `TIME_ZONE` differs from UTC.
- An empty pattern matches only the empty key on `LocMemCache` and `DatabaseCache`. It used to match every key, so `delete_pattern("")` cleared the cache.
- `LocMemCache` sorted-set values can be pickled, and `copy.deepcopy` no longer duplicates their internal ordering index.
- `LocMemCache.info()["memory"]` counts the ordering index of sorted sets, so a cache holding large sorted sets reports roughly twice the size it did.
- A character-class range written backwards, such as `keys("[z-a]")`, matches the keys Redis matches instead of raising a regex error.
- `lpos()` rejects a negative `count` or `maxlen` with Redis's `COUNT can't be negative` and `MAXLEN can't be negative` instead of searching from the wrong end.
- `linsert()` on `LocMemCache` and `DatabaseCache` raises `ValueError("syntax error")` for a `where` other than `"BEFORE"` or `"AFTER"` instead of inserting before the pivot.
- `spop()` with a negative `count` raises Redis's `value is out of range, must be positive` on the native backends instead of an internal `random.sample` message.
- `TrackingCache` no longer rolls the stampede dice twice for a locally held value. A local hit that triggers recompute returns the default without refetching from the transport, so early recompute follows `beta` and `delta`.
- `TrackingCache` pings its invalidation connection every `health_check_interval` seconds, not only while idle, so a dropped connection no longer goes unnoticed.
- The first `TrackingCache` listener connect from `aget()`, `aget_many()` or `ahas_key()` runs in a worker thread instead of stalling the event loop.
- `TrackingCache.incr_version()`, `decr_version()` and the async twins delegate the rename to the transport and forget local entries for both versions, so a transport alias with `VERSION` is read at the right version.
- `TrackingCache.delete_pattern()` evicts local entries by Redis glob rules (`[^0]` is a negation), matching what the transport deletes.
- `TrackingCache` rejects a non-iterable `OPTIONS["prefixes"]` with `ImproperlyConfigured` instead of a bare `TypeError`.
- A `TrackingCache` listener whose shutdown was abandoned no longer clears the local store that its live replacement keeps coherent.
- `StreamCache` keeps the local value when a broadcast is dropped for lack of publish budget or on a closed executor, and skips the superseded own entry instead of overwriting it. Other pods keep their last value until the key's next write or expiry.
- `StreamCache` no longer leaks own-entry marks when an `XADD` fails or when the stream trims an own entry away.
- `StreamCache` logs the consumer traceback once per transport outage and repeats only the one-line warning while the outage lasts.
- `ValkeyGlideCache` and `ValkeyGlideClusterCache` honor every `LOCATION` URL, not only the first; standalone uses the extra URLs as replicas with `read_from=PREFER_REPLICA`. Repeated URLs collapse to one address.
- valkey-glide: `zadd()`, `azadd()` and the pipeline's `zadd()` without flags no longer store a serialized `bytes` member as its repr.
- valkey-glide: `xadd()` and `xtrim()` raise `ValueError` when `maxlen` and `minid` are given together instead of silently dropping `minid`.
- valkey-glide: an atomic batch the server discarded raises `CachexError` instead of returning an empty result list.
- The cache admin masks connection passwords in `LOCATION` and backend errors; users with only `view_cache` could read them.
- A type-specific admin write on a backend without `type()` (any stock Django backend) returned a 500; the admin now refuses it with a message and keeps Delete and Set TTL available.
- The admin reads values with stampede prevention bypassed; a key past its logical expiry showed `null`, and Update wrote `None` back.
- The admin key list's `type=unknown` filter matched nothing; it now lists the keys it names.
- The admin key list showed Clear and Add key to users without `change_cache` and `add_key`; each now needs its permission.
- Deleting an already-gone key in the admin reported success; the key page now warns "Key not found, nothing was deleted." and the bulk action counts misses separately.
- The admin cache detail page hides the Slow Log section on backends without a slow log instead of showing an error.
- An explicit `timeout=None` passed to a semaphore's `acquire()` or `aacquire()` blocks indefinitely instead of falling back to the semaphore's `timeout`.
- `extend()` and `aextend()` raise `ValueError` when `additional_seconds` is zero or negative instead of returning `True`; `extend(-30)` silently shortened the claim.

## 0.7.1 (September 2026)

### Breaking changes

- `renamenx()` and `arenamenx()` return `False` for a missing source key instead of raising `ValueError`.

### Improvements

- `rename()` and `arename()` raise `KeyNotFoundError` for a missing source key, and both methods document it. The exception subclasses `CachexError` and `ValueError` and carries the key as `.key`.

## 0.7.0 (September 2026)

### Breaking changes

- `LocMemCache.ttl()`, `DatabaseCache.ttl()`, `StreamCache.ttl()` and their `pttl()` twins return `None` for a key with no expiry instead of `-1`. `-2` still means the key is gone.
- `DatabaseCache.get()` raises `WrongTypeError` on a key holding a list, set, hash or sorted set instead of returning the raw tagged container, and `get_many()` omits it.
- `Pipeline.zadd()` no longer takes `incr` and `Pipeline.zrange()` no longer takes `desc`, matching `RespCache`.
- `RespCache.adecr()` is gone as an override. It duplicated `BaseCache.adecr()` line for line, which is what callers get now.
- `RespAdapterProtocol` no longer declares `get_async_client()`; the redis-py and valkey-py adapters keep the method.
- `Pipeline.set()` reports an `nx`/`xx` miss as `False` instead of the driver's `None`.
- `Pipeline.type()` returns `None` for a missing key instead of `"none"`, and `KeyType.UNKNOWN` for an unmodeled server type instead of raising, matching `cache.type()`.
- `aclose()` disconnects and drops the running loop's async pools, so the next await opens fresh ones; on cluster it closes the loop's cluster client, on Sentinel also its Sentinel manager and discovery clients. Call it when a loop is finished, not between requests. `close()` keeps sync pools connected but now sweeps the async registries.
- `pool_class` on a Sentinel backend, previously ignored, selects the Sentinel-managed pool and must be `SentinelConnectionPool` or a subclass, or startup raises `ImproperlyConfigured`.
- `TieredCache` raises `ImproperlyConfigured` when `OPTIONS["l1_timeout"]` is unset and the L1 tier has `TIMEOUT = None`. Set `l1_timeout` on the tiered alias or `TIMEOUT` on the L1 tier.
- `StreamCache.set()` returns `None`, matching Django's `BaseCache` and the other backends. It previously returned `True`.
- `ValkeyGlideAdapter` and `ValkeyGlideClusterAdapter` raise `ImproperlyConfigured` for an empty `LOCATION` instead of an `IndexError`.
- `xpending()` on the valkey-glide backend returns the other backends' summary and range dicts, and rejects a filter without a count. Code that unpacked the raw list replies reads the dict keys instead.
- `hset()` with an empty mapping raises `ValueError` on the valkey-glide backend, direct, async and pipelined. The pipeline used to queue nothing.

### Improvements

- `DatabaseCache.scan(key_type=...)` pushes the type filter into the query instead of checking keys one by one.
- `DatabaseCache.sinter`, `sdiff` and `sunion` read every operand in one query instead of one query per key.
- `LocMemCache` sorted-set range and count queries bisect instead of scanning, and `LocMemCache.get`/`has_key` no longer unpickle a value they only test for presence.
- The admin shows a key whose type it cannot render, including an unmodeled type, read-only: the type is named, the value and operations hidden, and hand-crafted edits refused, but delete and TTL changes go through. Such types are never offered when adding a key.
- The admin key detail page hides mutation controls from users without `change_key` or `delete_key` instead of showing forms that fail with a 403.
- The admin add-key page takes only a key name and a data type; the first operation on the key detail page creates the key.

### Fixes

- Sorted-set score edits in the admin no longer fail on backends without server-side scripting (`LocMemCache`, `DatabaseCache`); the conflict check is offered only where it can run.
- The admin key detail page no longer shows "Could not load value" for a stream with all entries deleted.
- Admin breadcrumbs render styled on Django 6.0, and the Help and List Keys links render there again.
- A cache whose `BACKEND` cannot be imported shows a message on every admin page instead of a 500 on the cache detail, key detail and add-key pages.
- The admin danger zone (clear all versions, FLUSHDB) appears only on backends that implement it; elsewhere it raised `AttributeError`.
- A failed first operation while creating a key in the admin keeps you on the create page instead of bouncing you to the key list with "key does not exist".
- The native backends read Redis's glob dialect, not `fnmatch`'s, in `keys`, `scan` and `delete_pattern`: negation is `[^a]`, not `[!a]`, `!` is a member, `\` escapes, and case no longer folds on Windows.
- `LocMemCache.zpopmin`/`zpopmax` and `DatabaseCache.lpop`/`rpop`/`zpopmin`/`zpopmax` reject a negative count; `zpopmax(key, -2)` popped all but the last two members.
- `LocMemCache.zrangebyscore` honors the Redis rule that a negative `num` means "to the end"; `LIMIT 0 -1` dropped the last member.
- `LocMemCache.zadd` and `zincrby` coerce string scores to `float` like Redis; a stored `"1.5"` sorted as a string and made the next numeric write raise. `zadd` with an invalid score rejects the command instead of applying it halfway.
- `lpos` rejects `rank=0` on both native backends with Redis's own message, and a negative rank applies `maxlen` from the tail instead of the head.
- `srandmember` with a negative count returns exactly `|count|` members, repeats allowed. Both native backends reached `random.sample` and raised `ValueError`.
- `LocMemCache.ascan()` works instead of raising `NotSupportedError`, so the admin's async key browser can page a LocMem cache.
- `DatabaseCache` reports the key in its `WRONGTYPE` messages, matching `LocMemCache`.
- Exclusive score bounds (`(1`) raise `NotSupportedError` naming the backend on `LocMemCache` as well, rather than a bare `ValueError`.
- `StreamCache` polls the stream instead of parking in `XREAD BLOCK` on a valkey-glide transport; with two pods in one process, publishes timed out and mutations went missing on the other pod.
- `StreamCache` shares one consumer thread, publisher thread and pod identity per `LOCATION` within a process. Each ASGI request and WSGI thread used to start an uncollectable consumer and publisher with its own pod id, so sibling consumers treated the process's own writes as remote.
- `StreamCache` pods writing the same key inside the propagation window converge on the last write in stream order instead of diverging permanently. A pod's own `clear` coming back spares keys it wrote after clearing.
- Restarting a `StreamCache` after `shutdown()` no longer leaves two consumers advancing the same stream cursor.
- `StreamCache.delete_pattern()` publishes one broadcast for the whole match instead of one per key, so a large pattern no longer exhausts the publish budget.
- The RESP semaphore's `{name}:state` and `{name}:claims` hashes carry a guard TTL of twice the longest lease, refreshed by acquire, extend and release, so a dead holder no longer strands them.
- Async connection pools of closed event loops are released: every pool lookup and every `close()` drops them. `async_to_sync` callers leaked one pool and one TCP connection per call, and `close()` and `aclose()` were documented no-ops.
- `type()` and `atype()` return `KeyType.UNKNOWN` for a Redis module key (`ReJSON-RL`, `TSDB-TYPE`) instead of raising `ValueError`, which broke any scan reaching one.
- `IntEnum` and `IntegerChoices` members with values 48 to 57 round-trip through msgpack and ormsgpack as plain ints instead of reading back as 0 to 9; pickle still returns the enum member.
- `sadd()` rejects a member the configured serializer turns unhashable, such as a tuple under json, orjson, msgpack or ormsgpack, instead of failing later in `smembers()`.
- `expireat()` and `pexpireat()` with a past deadline delete the key on a stampede cache instead of keeping it for `buffer` seconds.
- `Pipeline.set()` with `nx` or `xx` and an immediate expiry no longer deletes a key it could not write, and `nx` with `xx` is rejected.
- Pipelined `expire()`, `pexpire()`, `expireat()` and `pexpireat()` add the stampede buffer and pipelined `ttl()`, `pttl()` and `expiretime()` subtract it; all seven take keyword-only `stampede_prevention`.
- Pipelined `xpending()` accepts the client's arguments: `count` alone is allowed, and a range without `count` raises `ValueError` instead of returning the summary.
- `SerializerError` and `CompressorError` carry a message naming the codec, the payload and the underlying cause. They were raised bare.
- valkey-glide: a username without a password (a nopass ACL user) no longer fails client construction; credentials are built only with a password.
- valkey-glide: `hmget()` with no fields returns an empty list instead of sending a malformed command to the server.
- valkey-glide: `lpop()` and `rpop()` with a count return `None` for a missing key.
- valkey-glide: pipelined `xpending()` and `xpending_range()` decode their replies like the direct calls instead of returning raw driver output.
- valkey-glide: `xinfo_stream(full=True)` decodes the group and consumer entries nested inside its list values.
- valkey-glide: async clients of closed event loops are closed; one `asyncio.run()` per request leaked a client and its connections each time.
- valkey-glide: `type()` returns `KeyType.UNKNOWN` for an unmodeled server type and `None` for a missing key instead of raising.
- valkey-glide: pipelined `set()` takes `px`, `exat`, `pxat`, `keepttl` and `get`, rejects conflicting expiry flags, and returns the old value for `get=True`.
- valkey-glide: stream, list and server methods use the adapter protocol's parameter names (`entry_id`, `start` and `end`, `slowlog_get(count)`), so keyword calls bind.
- valkey-glide: a blocking lock acquire no longer sleeps past its `blocking_timeout`.
- valkey-glide: cluster pipelines build a `ClusterBatch`, the batch type the cluster client is declared to execute.

### Documentation

- `set_with_flags(get=True)` documents what it returns: the driver's raw previous value, which the cache layer decodes.
- The documented server requirement said "Valkey 7.0+", but Valkey's first release was 7.2; the README, the docs home page and the installation page now say "Valkey 7.2+ or Redis 6.0+".
- The distributed-locking recipe passed `timeout` to `lock.acquire()`, which raised `TypeError` on the redis-py and valkey-py backends; it now sets `timeout` on `cache.lock()`.
- The example projects' READMEs had the wrong Valkey port for the full example, `cd example` for a directory named `simple`, `admin`/`admin` where `run.sh` creates `admin`/`password`, a `../.venv` path one level short, and a cache table without the `cluster`, `sentinel`, `sync` and `stream_transport` aliases. The full example's `run.sh` announced a nonexistent `SyncCache` backend instead of `StreamCache`.

### Tooling

- The release workflow runs `tests/admin/` as well as `tests/cache/` before tagging.
- Dropped the `scripts/**` ruff per-file-ignores entry; the directory it covered was removed in 4acfe2e.
- The cache test matrix is parametrized by topology (`default`, `cluster`, `sentinel`) instead of an independent client class and sentinel flag; `client_class` and `sentinel_mode` derive from the active topology.
- Container fixtures pass their addresses directly instead of through the environment, use `LogMessageWaitStrategy` instead of the deprecated `wait_for_logs`, and pick db numbers with `crc32`, not `hash()`.
- Tests that toggled the never-read `DJANGO_REDIS_SCAN_ITERSIZE` and `DJANGO_REDIS_CLOSE_CONNECTION` settings drive the real `itersize` argument and a real close; pytest-django's `settings` fixture replaces the vendored `SettingsWrapper`.
- The valkey-glide adapter no longer needs its module-wide `ignore_errors` mypy override or its file-wide `ruff: noqa: ERA001`.

## 0.6.0 (August 2026)

### Breaking changes

- `DatabaseCache` stores collections as tagged subclasses (`_List`, `_Set`, `_Hash`, `_ZSet`), like `LocMemCache`. Compound operations on untagged values, including rows older versions wrote, raise `WrongTypeError`. Clear the cache table or re-write those keys before upgrading.
- `type()` returns `None` for a missing key; the `BaseCachex` default answered `STRING`.
- `scan(key_type=...)` filters; the `BaseCachex` default, `StreamCache` and `DatabaseCache` ignored it, so the admin's Type filter returned unfiltered results.
- `get_client()` and `get_async_client()` return one shared client per connection pool on the redis-py and valkey-py adapters; construction dropped from 55 µs to 0.06 µs per call. Mutating it, for example with `set_response_callback`, affects other calls.
- `StreamCache` broadcasts `delete_many` as a list; a key containing `\x00` made remote pods delete the wrong keys. Old and new pods disagree on this message: drain or rotate `stream_key` during the rollout.
- `Pipeline.set()` applies the backend's `TIMEOUT`, not `timeout=None` (no expiry), and normalizes negative and float timeouts like `cache.set()` instead of raising.
- With `stampede_prevention` on, `expire()`, `pexpire()`, `expireat()` and `pexpireat()` add the stampede buffer and `ttl()`, `pttl()` and `expiretime()` subtract it. Each takes a keyword-only `stampede_prevention` argument for the raw value.
- `sadd()` rejects an unhashable member with `TypeError` and adds nothing; such members broke every later read.
- `RespCache`, `RespClusterCache` and `RespSentinelCache` raise `ImproperlyConfigured` when used as a `BACKEND` directly; they bind no driver and exist to be subclassed.
- `xpending()`, its pipeline form and the async twins raise `ValueError` for `start`, `end`, `consumer` or `idle` without `count`; they returned the unfiltered summary.

### Features

- Async admin surface on `TieredCache`: `akeys`, `aiter_keys`, `ascan`, `attl`, `apttl`, `atype`, `apersist`, `aexpire` and `adelete_pattern`.
- `ascan()` on `DatabaseCache`, which had the sync `scan` but inherited the `NotSupportedError` default for the async twin.
- `get_many()` and `incr_version()` on `LocMemCache`, both collection-aware and both reading under a single lock acquisition.
- `expire()` and `pexpire()` accept a `timedelta` on the valkey-glide adapter.

### Fixes

- `hget`/`hgetall`/`hvals` read a field written by `HINCRBYFLOAT` instead of raising `SerializerError`. A float written by `hset` is still not incrementable, as documented on `encode()` and `hincrbyfloat`.
- `aget_or_set()` awaits a default that returns an awaitable, not only a coroutine function.
- `incr_version()` works on a cluster with a hash-tagged `KEY_PREFIX`; `KEY_PREFIX="{app}"` was rejected.
- `CULL_FREQUENCY` and `MAX_ENTRIES` are no longer forwarded to the driver's connection pool.
- `semaphore()` reports a missing `lease` as a `ValueError` naming the argument.
- `LocMemCache.get()` and `incr()` no longer raise `KeyError` under concurrent writes.
- A `LocMemCache` collection rewrite at `MAX_ENTRIES` no longer strips the key's TTL or evicts a sibling; culling runs only on a first write.
- `lpush`/`rpush` with no values return 0 instead of creating an empty, immortal key.
- `lpop`/`rpop` reject a negative count; `rpop(key, -2)` popped from the head.
- `TieredCache` mutates L2 before invalidating L1 in `delete`, `delete_many`, `incr`, `decr`, `expire`, `delete_pattern`, `clear` and the async twins; concurrent reads left L1 stale.
- `TieredCache` async methods call L1's async twins; the sync ones raised `SynchronousOnlyOperation` on a guarded L1.
- `TieredCache.delete_many()` and `clear()` no longer report failure against a non-RESP L2.
- `TieredCache` no longer masks an `AttributeError` raised inside an L2 method as `NotSupportedError`; only a missing method reports it.
- `TieredCache` rejects self-referencing and duplicate tier aliases at construction instead of raising `RecursionError` on the first `get()`.
- RESP semaphores no longer admit far past capacity after a release on an evicted state hash.
- The semaphore queue score comes from the server; a host with a slow clock starved other hosts' waiters.
- Growing a local semaphore's capacity wakes the parked waiters it now fits.
- Releasing a local semaphore no longer raises `RuntimeError` when a waiter's event loop has closed.
- The local semaphore registry drops names nothing references. It held every name forever, so `cache.semaphore(f"job:{id}")` grew it without bound.
- The RESP semaphore deletes its `{name}:state` hash on the last release instead of leaking it.
- `release()` logs a warning when the claim was already reaped, meaning the work ran past its lease unprotected.
- The capacity-change warning points at the caller of a directly constructed `Semaphore(...)`.
- Pipelines on the redis-py and valkey-py adapters raise `WrongTypeError` for `WRONGTYPE`, not the driver's `ResponseError`.
- Count-form `lpop`/`rpop` report a missing key as `None`, like `LocMemCache`, instead of `[]`.
- `hmget(key)` with no fields returns `[]` instead of sending an invalid command.
- RESP3 dict replies (`OPTIONS {"protocol": 3}`) no longer break stream result decoding with `ValueError`.
- Sentinel connections inherit the driver's socket timeouts; the old `sentinel_kwargs` default of `{}` let a blackholing sentinel block discovery.
- Connection pools are keyed stably; an option like a `Retry` instance opened a new pool per cache instance.
- An empty `LOCATION` server list raises `ImproperlyConfigured` naming the backend instead of failing on first use.
- `INFO` parses on a valkey-glide cluster, pinned to a random node; the admin's memory and keyspace panels were blank.
- `set_many()` on valkey-glide cannot leave a key without a TTL when a batch breaks partway.
- The valkey-glide lock's `__enter__` and `__aenter__` raise `LockError`, not `RuntimeError`; `extend()` refuses a lease-less lock instead of making it self-release.
- `aclose()` on the valkey-glide adapter closes the per-loop client instead of leaking its connection.
- The valkey-glide pipeline raises on an empty `hset` mapping; skipping it shifted every later result.
- `xclaim(justid=True)` returns `str` IDs from a pipeline, matching the non-pipeline path.
- `ttl`, `pttl` and `expiretime` normalize -1 to `None` in pipelines, and `rename` returns a `bool` across drivers.
- `DatabaseCache` deletes a collection row when `lrem`, `srem`, `hdel`, `zrem` or `ltrim` removes the last member.
- `DatabaseCache` rejects `(` exclusive score bounds instead of raising a bare `ValueError`, and reads a negative `num` as "to the end".
- On `DatabaseCache`, a `KEY_PREFIX` with a glob character no longer matches sibling prefixes: `KEY_PREFIX="svc?1"` made `keys("*")` return `svcX1` rows.
- `DatabaseCache.info()` reports a real `expires` count; compound operations use one write connection under a routing router; pattern deletes run in `itersize` chunks.
- `StreamCache.get_or_set()` follows Django's semantics: a stored `None` is a hit, and a `None` default is stored.
- `StreamCache.info()["last_read_age"]` stops growing on an idle stream.
- `StreamCache` raises `NotSupportedError` rather than `AttributeError` for the cachex operations it does not implement, and `expire()` accepts a float.
- `StreamCache` joins its consumer thread with a bound at interpreter exit; `close()` stays a no-op.
- Key patterns on `LocMemCache` and `StreamCache` match case-sensitively on Windows, like Redis globs.
- `scan(count=0)` is honored on the `BaseCachex` default, `DatabaseCache` and `StreamCache`, which turned it into 100.
- `OrmsgpackSerializer` accepts non-string mapping keys, like `MsgpackSerializer` with its `strict_map_key=False`.
- `LocMemCache.info()` sizes modules, classes and functions opaquely instead of walking the import graph under the cache's lock; a value holding `sys` cost 11 ms and 2.4 MB.
- A LocMem or Database `BACKEND` no longer imports redis-py or valkey-py; names resolve on first access.
- The key admin no longer localizes scores, TTLs, list indices or page numbers, so edits work under `de` or `USE_THOUSAND_SEPARATOR`.
- Creating a key in the admin requires `add_key`; the create form and actions on missing keys checked only `change_key`.
- An admin TTL of `"00"`, `"+0"` or `"-0"` makes the key persistent instead of deleting it.
- The admin's `lrem` removes one occurrence by default, not every occurrence.
- The admin warns when a `ZADD` changed nothing, counting score updates via `CH`.
- The admin key detail no longer raises on a value the serializer rejects.
- Admin sorted set members that deserialize to a list or dict are read-only; submitting them raised `TypeError`.
- The admin's `xtrim` is exact, and the Help link preserves the current page and type filter.
- The cache and key admins' per-object history and delete routes return 404.

### Documentation

- The `OPTIONS` reference says which backends honor each key, with a valkey-glide section covering `db`, `use_tls`, `username`, `password`, `request_timeout` and `client_name`; glide ignores `ssl_*`.
- The valkey-glide description names `glide_sync.GlideClient` for the sync surface and `glide.GlideClient` for the `a*` methods.
- `cache.lock()`'s documented signature matches the code, including that the second positional argument is `version`, not the lease.
- The admin permission list matches the views: `change_cache` gates cache-wide actions, `change_key` every key detail mutation including TTL and persist.
- The README and `docs/index.md` say streams are RESP-only; `LocMemCache` and `DatabaseCache` carry only the hash, list, set and sorted set operations.
- README screenshots load on PyPI, which does not resolve repository-relative image paths.
- `semaphore()` documents its keys outside the cache's namespace (`{name}:state`, `:claims`, `:queue`), which `clear()`, `keys()` and the admin skip.
- `sscan()` and `sscan_iter()` document that `match` runs server-side against the serialized member unless it is a plain string.
- `incr()` documents where it diverges from `BaseCache.incr` and from `LocMemCache`.

## 0.5.1 (August 2026)

### Improvements

- `TieredCache.get_many` batches its L2 TTL lookups into one pipeline where L2 supports it.
- The sdist no longer ships example and benchmark files.
- Free-threaded CPython is verified in CI again: the cache suite runs on 3.14t.

### Fixes

- `django_cachex.adapters._pipeline_parsers` removed; it was left over from the dropped Rust driver.
- `version` and `PackageNotFoundError` no longer leak into `django_cachex`'s namespace.
- The admin key-size lookup logs its failures instead of silently showing a blank size.
- Admin breadcrumbs render with Django 6.1's markup instead of drawing unstyled.
- Admin object tools (Help, List Keys) sit next to the page title again instead of below it.

### Documentation

- The `username` connection option is documented: an `OPTIONS` key on every adapter that takes precedence over the URL.

## 0.5.0 (August 2026)

### Breaking changes

- The `redis-rs` backends are gone: `RedisRsCache`, `RedisRsSentinelCache`, `RedisRsClusterCache`, `django_cachex.adapters.redis_rs`, the `redis-rs` extra and the `django-cachex-redis-rs` package. The driver never reached PyPI; switch to `ValkeyCache`, `RedisCache` or `ValkeyGlideCache`. A standalone [redis-rs-py](https://github.com/oliverhaas/redis-rs-py) binding is in progress.
- `django_cachex.Lock` and `django_cachex.AsyncLock` removed; they wrapped the Rust driver's lock commands. `cache.lock()`, `LockError` and `LockNotOwnedError` are unchanged.

## 0.4.2 (August 2026)

### Improvements

- Every key admin value input is the same textarea, so push, add and set-field forms accept multi-line JSON.
- Set and sorted set members can be edited; an interrupted rename leaves a duplicate, not a lost member.
- Hash field names can be edited atomically; a rename is refused if the value changed since page load or the name exists.

### Fixes

- Container entries that are not JSON-serializable are read-only; submitting their `repr()` stored the repr string over the real value.
- `xadd` parses its value like every other handler, so a stream entry can hold a number, list or dict.
- The string editor strips surrounding whitespace, so a stray newline no longer changes the stored value.

## 0.4.1 (August 2026)

### Fixes

- The admin's value textarea no longer overflows its container.
- Admin warnings and field errors are readable in dark mode.
- Semaphore `release()` and `extend()` no longer act on a token installed by a racing re-acquire on the same instance.
- RESP semaphores no longer wedge or admit past capacity when Redis evicts their bookkeeping.

## 0.4.0 (August 2026)

!!! note "Historical record"
    Entries below describe 0.4.0 as released. Since 0.5.0 the package is pure
    Python again, without the Rust extension, the `redis-rs` backends or binary
    wheels, so the cibuildwheel and cp314t wheel entries are outdated.

### Breaking changes

- Python 3.14+ required. Dropped support for 3.12 and 3.13. The package now ships on cp314 and cp314t (free-threaded) wheels.
- Django 6.0+ required. Dropped support for Django 5.2.
- `LocMemCache` data structures use tagged subclasses (`_List`, `_Set`, `_Hash`, `_ZSet`), and cross-type access raises `WrongTypeError` instead of silently coercing.
- `LocMemCache` bypasses pickle for tagged collections and drops copy-on-read and copy-on-write: `cache.get()` returns the live structure, not a detached snapshot.
- `StreamCache` wire format changed to the transport's serializer and compressor instead of raw pickle; new pods cannot read older pods' entries, so drain or rotate `stream_key`.
- `hmset` removed. Use `hset(key, mapping=...)` or `hset(key, items=...)` (flat key-value list, matching redis-py/valkey-py).
- `django_cachex.unfold` removed: the django-unfold admin theme, the `[unfold]` extra and `examples/unfold/` are gone. Use `django_cachex.admin`.
- Lock parameters renamed, with no deprecation shim: `cache.lock(timeout=...)` is now `cache.lock(lease=...)` (TTL) and `lock.acquire(blocking_timeout=...)` is now `lock.acquire(timeout=...)` (max wait). `blocking_timeout` raises `TypeError`; the constructor's `timeout=` means max wait, not TTL.
- `ZStdCompressor` renamed to `ZstdCompressor` (`django_cachex.compressors.zstd.ZstdCompressor`). Update `OPTIONS["compressor"]` strings.
- `LzmaCompressor` constructor `preset=` renamed to `level=`, which every compressor now accepts.
- `PickleSerializer` no longer raises `ImproperlyConfigured` for `protocol > pickle.HIGHEST_PROTOCOL`; the first `dumps` raises `SerializerError` instead, with pickle's `ValueError` as `__cause__`.
- `CachexCompat` removed, along with the admin's "wrapped" support tier. Use `django_cachex.cache.LocMemCache` / `DatabaseCache` (drop-in replacements) for full admin support; non-cachex backends show as "limited" (configuration only).
- Cluster `LOCATION` with a non-zero database number raises (`RedisClusterException` / `ValkeyClusterException`) on the redis-py and valkey-py cluster backends. Cluster never honored it; drop it from `LOCATION`. `ValkeyGlideClusterCache` and `RedisRsClusterCache` still ignore it.

### Features

- Rust I/O driver (experimental) in the separate `django-cachex-redis-rs` package, via the `redis-rs` extra: `RedisRsCache`, `RedisRsClusterCache` and `RedisRsSentinelCache`, which raise `ImportError` on first use without the extra.
- `valkey-glide` adapter (experimental) via the `valkey-glide` extra: `ValkeyGlideCache` and `ValkeyGlideClusterCache`. No Sentinel.
- `WrongTypeError` exception. LocMem, redis-py, valkey-py, valkey-glide and the Rust adapter raise `django_cachex.WrongTypeError` (a `TypeError` subclass) for ``WRONGTYPE``.
- Async ext methods on LocMem and Database. The full async data-structure surface works instead of raising `NotSupportedError`.
- `StreamCache` backend. Local in-memory reads, with writes broadcast over a Redis Stream to every pod. Read-heavy, write-light, eventually consistent.
- `TieredCache` backend. Composes two `CACHES` entries as L1 (e.g. LocMem) and L2 (e.g. Redis), with TTL propagation and pull-through reads.
- Cache-stampede prevention. TTL-based XFetch via `OPTIONS["stampede_prevention"]` (or `stampede_prevention=` per call). Configurable buffer/beta/delta.
- `LocMemCache` and `DatabaseCache` extensions: drop-in replacements for the Django builtins with data-structure ops, TTL helpers and admin support. `LocMemCache` compound ops are serialized (#62).
- `orjson` and `ormsgpack` serializer extras.
- Free-threaded CPython (3.14t) support. A cp314t wheel is built, and the Rust driver runs with the GIL disabled.
- PyPI wheels via cibuildwheel. Wheels for Linux x86_64, Linux aarch64, macOS arm64, and Windows amd64, on cp314 and cp314t.
- Async pool sharing. Per-task `Cache` instances share one async connection pool (#83), avoiding the thundering-herd reconnect on cold start.
- Pipeline parity. Stream ops, CAS ops, missing key ops (`persist`, `pttl`, `expireat` and others), context manager, `zpopmin`/`zpopmax` default `count=1`.
- Compressors gain a uniform `level=` parameter (gzip, lz4 and zstd join zlib and lzma), defaulting to each library's own default.
- Serializer/compressor wrappers consolidated. Subclasses implement `_dumps`/`_loads` or `_compress`/`_decompress`; the base classes handle `SerializerError` / `CompressorError` translation and int passthrough.
- Weighted semaphores. `cache.semaphore(name, capacity, *, weight=1, lease=..., timeout=...)` and `cache.asemaphore(...)` on `LocMemCache` and the RESP backends, including cluster, with lease-based crash reclaim on RESP. Sync and async share state per cache instance.

### Performance

- `LocMemCache` sorted sets are O(log N), down from O(N log N) per write. Adds `sortedcontainers>=2.4` as a runtime dependency.
- `LocMemCache` skips pickle for tagged collections and mutates list/set/hash/zset/stream types in place.

### Fixes

- `LocMemCache.lpush`, `sadd`, `hset`, `hincrby`, `zadd` and similar methods no longer lose updates under concurrent threads (#62).
- `delete_pattern` batches deletes to bound peak memory on broad patterns.
- `clear()` is now prefix/version-scoped instead of `FLUSHDB`. The old behavior is available as `flush_db()`.
- Compressor `compress` and `decompress` methods catch all exceptions and re-raise as `CompressorError`.
- Cluster correctness: script loading on replicas, set_many `timeout=0`.
- Reading values small enough to have skipped compression (at or below the compressor's `min_length`) no longer crashes.
- Admin cache/key changelists are compatible with Django 6.1.
- Semaphore waiters abandoned by crashed or cancelled callers are reaped instead of blocking the queue.
- valkey-glide: TLS (`rediss`/`valkeys` or `use_tls`/`ssl`), credentials from the URL or `OPTIONS`, the database index (standalone only), `request_timeout` and `client_name` reach the client, not only host and port. `zadd` forwards `gt`/`lt`; pipelines support stream commands.
- `TieredCache.set` forwards `nx`/`xx` to L2. A stock Django L2 no longer raises `TypeError`: `nx` falls back to `add()`, `xx`/`get` raise `NotSupportedError`, and a plain set drops the flags.
- `set(..., timeout=0)` deletes the key across all backends, matching Django's cache contract.
- `LocMemCache` aliases sharing a `LOCATION` share one store, including tagged collections and semaphore budgets.
- Admin: backend capability probes fail gracefully, and key URLs are quoted so keys with special characters open correctly.
- CI runs the test matrix against Django 6.1 in addition to 6.0.
- Dependabot automerge waits for every workflow run on the PR head to succeed before merging.
- `reverse_key()` handles a `KEY_PREFIX` containing colons, so `keys()`, `iter_keys()`, `scan()` and the blocking list pops return user keys.
- `DatabaseCache` compound ops (`rpush`, `sadd`, `zadd`, `hset`, ...) merge with a concurrent writer's row instead of overwriting it.
- `LocMemCache` and `DatabaseCache` `hincrby`/`hincrbyfloat` reject non-numeric stored values with the same error as the server instead of truncating them.
- `TieredCache` rejects `KEY_PREFIX` in the standard top-level slot as well as in `OPTIONS`; it was silently ignored before.
- Sentinel: async connection pools are keyed by sentinel fleet, so aliases sharing a service name no longer share a pool.
- Semaphores: concurrent `acquire()` on one `RespSemaphore` instance can no longer double-claim and leak a slot until the lease expires.
- Admin: editing a key keeps its TTL and persistence on every backend instead of resetting to the default timeout.
- `StreamCache` broadcasts in write order, so consumers converge on the writer's final value, and `keys()` is scoped to the cache's prefix and version.
- A pipeline reused after `execute()` raises decodes the next batch correctly; `AsyncPipeline` rejects a sync `with` before the block runs.
- The redis-py and valkey-py cluster backends now honor the URL's TLS scheme, credentials and query parameters. Per-task async Sentinel adapters share one pool.
- `encode()` passes through exact `int` values only, so `int` subclasses (`IntEnum`, `IntFlag`) keep their type.
- `touch()`/`atouch()` apply the stampede buffer and accept a per-call `stampede_prevention=`; a touch pushed every reader into a recompute.
- `DatabaseCache` key scans no longer match unrelated rows when a `KEY_PREFIX` or pattern contains `%`, `_` or a backslash.
- `DatabaseCache.zadd`/`zincrby` reject a non-numeric score with `ValueError` instead of storing a value that breaks later range queries.
- `MAX_ENTRIES` culling covers the whole store: `LocMemCache` counts and evicts tagged collections, and `DatabaseCache` compound ops cull on insert like `set()`.
- `LocMemCache` collection edge cases: `keys()` scopes to the requested version and skips expired entries, `incr()` on a collection raises `WrongTypeError`, not `KeyError`, and `sadd`/`hset`/`zadd` adding nothing leave no empty key.
- `rpop(count=0)` on `LocMemCache` and `DatabaseCache`, and `zpopmax(count=0)` on `LocMemCache`, return an empty list instead of draining the whole collection.

---

## 0.3.0 (February 2026)

- `expiretime()` and `set(get=True)` support: New cache methods for retrieving absolute expiry timestamps and atomic get-and-set operations.
- Atomic CAS operations in admin: Key detail edits use compare-and-swap via Lua-computed SHA1 fingerprints to prevent concurrent edit conflicts.
- Key detail pagination: Collection types (list, hash, set, zset, stream) are paginated at 100 items per page with `?page=N` navigation.
- Keys in admin sidebar: The key list is now a sidebar entry with a cache filter for switching between configured caches.
- Simplified Lua script execution: `eval_script()` replaces the `register_script`/`LuaScript` registry with direct `EVAL` calls; redis-py handles script caching.
- Async data structure methods: All hash, list, set, and sorted set operations now have async counterparts on `RespCache` (e.g. `ahset`, `alpush`, `asadd`, `azadd`).
- Stream operations: Full sync and async support for Redis streams (`xadd`, `xread`, `xrange`, `xlen`, `xdel`, `xtrim`, `xinfo_stream`, `xgroup_create`, `xreadgroup`, `xack`, `xpending`, `xclaim`, `xautoclaim`, and more).
- Safe `clear()`: `clear()` now uses `delete_pattern("*")` to only remove keys for the current cache version and prefix, instead of `FLUSHDB`. Use `flush_db()` for the old behavior.
- Danger zone in admin: Cache detail view has a "Danger Zone" section with "Clear all versions" and "Flush database" actions. Key list view has a "Clear" button for safe prefix-scoped clearing.
- `hset` items param: `hset()` now accepts an `items` parameter (flat key-value list), matching the redis-py/valkey-py signature. `hmset` is removed.
- `delete_pattern` batched deletes: Deletes are now batched to prevent OOM on broad patterns.
- Multi-key params standardized: Set operations such as `sdiff`, `sinter` and `sunion` accept `KeyT | Sequence[KeyT]` consistently.

---

## 0.2.0 (February 2026)

- Django permissions enforced: The admin now uses Django's built-in permission system for granular access control. Staff users need explicit permissions; superusers are unaffected.

---

## 0.1.0 (February 2026)

Initial stable release of django-cachex.

### Features

- Valkey and Redis support in one package.
- Session backend support via Django's cache sessions.
- Pluggable clients: Default, Sentinel, Cluster.
- Pluggable serializers: Pickle, JSON, MsgPack.
- Pluggable compressors: Zlib, Gzip, LZMA, LZ4, Zstandard.
- Multi-serializer/compressor fallback for safe migrations.
- Connection pooling with configurable options.
- Primary/replica replication support.
- Valkey/Redis Sentinel support for high availability.
- Valkey/Redis Cluster support with automatic slot handling.
- Distributed locks compatible with `threading.Lock`.
- TTL operations: `ttl()`, `pttl()`, `expire()`, `persist()`.
- Pattern operations: `keys()`, `iter_keys()`, `delete_pattern()`.
- Pipelines for batched operations.
- Lua script interface with automatic key prefixing and value encoding/decoding.
- Django Cache Admin for cache inspection and management:
  - Browse, search, edit, and delete cache keys.
  - View server info, memory statistics, and slowlog.
  - Key type filter sidebar.
  - Support for Django builtin backends (LocMemCache, DatabaseCache, FileBasedCache) via wrappers.
  - Django Unfold theme support (`django_cachex.unfold`).
- Async support for all extended methods.

### Data Structure Operations

- Hash operations: `hset`, `hdel`, `hexists`, `hget`, `hgetall`, `hincrby`, `hincrbyfloat`, `hkeys`, `hlen`, `hmget`, `hmset`, `hsetnx`, `hvals`
- Sorted set operations: `zadd`, `zcard`, `zcount`, `zincrby`, `zrange`, `zrevrange`, `zrangebyscore`, `zrevrangebyscore`, `zrank`, `zrevrank`, `zrem`, `zremrangebyrank`, `zremrangebyscore`, `zscore`, `zmscore`, `zpopmin`, `zpopmax`
- List operations: `llen`, `lpush`, `rpush`, `lpop`, `rpop`, `lindex`, `lrange`, `lset`, `ltrim`, `lrem`, `lpos`, `linsert`, `lmove`, `blpop`, `brpop`, `blmove`
- Set operations: `sadd`, `srem`, `smembers`, `sismember`, `smismember`, `scard`, `spop`, `srandmember`, `smove`, `sdiff`, `sdiffstore`, `sinter`, `sinterstore`, `sunion`, `sunionstore`, `sscan`, `sscan_iter`

### Requirements

- Python 3.12+
- Django 5.2+
- valkey-py 6.1+ or redis-py 6+

---

## Pre-release History

### 0.1.0b6 (February 2026)

#### New Features

- Key type filter: Filter keys by type (string, list, set, hash, zset, stream) in the admin key list sidebar
- LocMemCache data structure operations: List, set, and hash operations now work with LocMemCache wrappers
- LocMemCache type detection: Automatically detects stored Python types (list, set, dict) and maps them to Redis equivalents
- `KeyType` StrEnum: Centralized enum for Redis key types, replacing scattered string literals

#### Improvements

- Admin refactoring: replaced service layer with helpers module, simplified views, restructured templates
- Unified admin views between classic Django admin and Unfold theme
- Added `_cachex_support` ClassVar to `CacheProtocol` for standardized support level detection
- Mixin-based class patching for cache wrappers (replacing intermediate extension classes)
- Dead code cleanup across the codebase

#### Bug Fixes

- Unfold templates match the classic admin
- `key_type` variable usage in the unfold key detail template
- mypy and ty type-checking errors
- `!r` format spec for `KeyT` in error messages

### 0.1.0b5 (February 2026)

#### New Features

- Expanded cache backend support: The admin interface now supports Django's builtin cache backends through wrapper classes
  - `LocMemCache`: Full support including key listing, TTL inspection, and memory statistics
  - `DatabaseCache`: Key listing, TTL inspection, and database statistics
  - `FileBasedCache`: File listing (as MD5 hashes) and disk usage statistics
  - `Memcached`: Basic stats when available
  - Django's `RedisCache`: Basic support (full features require django-cachex backends)

#### Improvements

- Standardized `info()` output format across all wrapped cache backends
- Added TTL support (`ttl()`, `expire()`, `persist()`) for LocMemCache
- Cache admin UX: unsupported operations fail gracefully instead of hiding UI elements

#### Bug Fixes

- LocMemCache keys no longer show "not found" when clicked in admin
- The key search form preserves the cache query parameter
- Editing works for wrapped cache backends

### 0.1.0b4 (January 2026)

#### New Features

- Django Cache Admin: Built-in admin interface for cache management
  - Browse all configured caches
  - Search keys with wildcard patterns
  - View and edit cache values (strings, hashes, lists, sets, sorted sets)
  - Inspect TTL and modify expiration
  - View server info and memory statistics
  - Flush individual caches
  - Bulk delete keys

- Django Unfold Theme Support: Alternative admin styling for django-unfold users
  - Use `django_cachex.unfold` instead of `django_cachex.admin`
  - Consistent styling with Unfold's modern admin theme

- Example Projects: Added example projects demonstrating various configurations
  - `examples/simple/` - Basic setup with ValkeyCache and LocMemCache
  - `examples/full/` - Multiple backends including Sentinel and Cluster
  - `examples/unfold/` - Django Unfold theme integration

### 0.1.0b3 (January 2026)

#### New Features

- Lua Script Interface: High-level API for registering and executing Lua scripts with automatic key prefixing and value encoding/decoding
  - `cache.register_script()` to register scripts with pre/post processing hooks
  - `cache.eval_script()` and `cache.aeval_script()` for sync/async execution
  - `pipe.eval_script()` for pipeline support
  - Pre-built helpers: `keys_only_pre`, `full_encode_pre`, `decode_single_post`, `decode_list_post`
  - `ScriptHelpers` class exposes `make_key`, `encode`, `decode` for custom hooks
  - Automatic SHA caching with NOSCRIPT fallback
