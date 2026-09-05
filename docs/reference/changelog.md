# Changelog

## Unreleased

### Breaking changes

- `renamenx()` and `arenamenx()` return `False` for a missing source key instead of raising. The method already answers "did the rename happen" with a bool when the destination is taken, and a missing source is the same answer on the claim-a-key path; callers had to wrap the call in `except ValueError` to get one.

### Improvements

- `rename()` and `arename()` raise `KeyNotFoundError` for a missing source key, and both methods document it. The exception subclasses `CachexError` and `ValueError`, so `except CachexError` now covers it and existing `except ValueError` handlers keep working without swallowing unrelated errors; the missing key is available as `.key`.

## 0.7.0 (September 2026)

### Breaking changes

- `LocMemCache.ttl()`, `DatabaseCache.ttl()` and `StreamCache.ttl()` return `None` for a key with no expiry, and so do their `pttl()` twins. They returned Redis's raw `-1`, while the RESP adapters normalize that to `None`, so a caller checking `ttl(key) is None` got different answers from different backends. `-2` still means the key is gone.
- `DatabaseCache.get()` raises `WrongTypeError` on a key holding a list, set, hash or sorted set, and `get_many()` omits it. A collection came back as the raw tagged container, so `get()` handed out a `_List` that compared equal to a plain list and `get_many()` mixed structures into a string read. `LocMemCache` has behaved this way since the tags were introduced.
- `Pipeline.zadd()` no longer takes `incr` and `Pipeline.zrange()` no longer takes `desc`. Neither argument exists on `RespCache`, and the pipeline is meant to queue the same calls the client answers directly.
- `RespCache.adecr()` is gone as an override. It duplicated `BaseCache.adecr()` line for line, which is what callers get now.
- `RespAdapterProtocol` no longer declares `get_async_client()`. The redis-py and valkey-py adapters keep the method; the protocol only promises what every adapter, glide included, provides.
- `Pipeline.set()` reports an `nx`/`xx` miss as `False`. It surfaced the driver's `None`, which is what the client-side `set()` never returned.
- `Pipeline.type()` returns `None` for a missing key and `KeyType.UNKNOWN` for a server type the package does not model, matching `cache.type()`. It used to hand back the raw string `"none"` and raise on module types such as ReJSON-RL.
- `aclose()` disconnects the async pools of the loop it runs on and drops them from the registry, so the next await opens fresh ones. Call it when a loop is finished, not between requests. On cluster it closes that loop's cluster client; on Sentinel it also closes the loop's Sentinel manager and the clients it discovered with. `close()` still leaves the sync pools connected, since Django fires it on every `request_finished`, but it now sweeps the async registries.
- `pool_class` on a Sentinel backend selects the Sentinel-managed pool and must be `SentinelConnectionPool` or a subclass. It was accepted and ignored; anything else now raises `ImproperlyConfigured` at startup, because a plain connection pool takes none of the primary/replica discovery arguments.
- `TieredCache` raises `ImproperlyConfigured` when `OPTIONS["l1_timeout"]` is unset and the L1 tier has `TIMEOUT = None`. The TTL cap is the only thing that evicts an L1 entry this process did not write, so without one a key another process changes in L2 is served stale from L1 forever. Set `l1_timeout` on the tiered alias or `TIMEOUT` on the L1 tier.
- `StreamCache.set()` returns `None`, matching Django's `BaseCache` and the other backends. It previously returned `True`.
- `ValkeyGlideAdapter` and `ValkeyGlideClusterAdapter` raise `ImproperlyConfigured` for an empty `LOCATION` instead of failing with an `IndexError` while building the client config.
- `xpending()` on the valkey-glide backend returns the same summary and range dicts as the other backends, and rejects a filter given without a count. Code that unpacked the raw list replies reads the dict keys instead.
- `hset()` with an empty mapping raises `ValueError` on the valkey-glide backend in every form, direct, async and pipelined. The pipeline used to queue nothing.

### Improvements

- `DatabaseCache.scan(key_type=...)` pushes the type filter into the query. The tagged class's name appears verbatim in the pickled row, so a `LIKE` pre-filter over its three base64 alignments narrows the result set in SQL; surviving rows are still decoded and confirmed, so the filter stays exact while the per-key round trips disappear.
- `DatabaseCache.sinter`, `sdiff` and `sunion` read every operand in one query instead of one query per key.
- `LocMemCache` sorted-set range and count queries bisect to the low bound instead of scanning the whole set, and `LocMemCache.get`/`has_key` no longer unpickle a value they only test for presence.
- The admin shows a key whose server-side type it cannot render, including a type the package does not model, as read-only: the type is named, the value is not shown and no operation is offered, and a hand-crafted edit request is refused with a message, though delete and TTL changes still go through. Such a type is never offered when adding a key.
- The admin key detail page hides every mutation control from users without `change_key` or `delete_key` instead of showing forms that fail with a 403 on submit.
- The admin add-key page takes only a key name and a data type; the key is created by the first operation on the key detail page.

### Fixes

- Sorted-set score edits in the admin no longer fail on backends without server-side scripting (`LocMemCache`, `DatabaseCache`). The conflict check is offered only where it can run.
- The admin key detail page no longer shows "Could not load value" for a stream whose entries have all been deleted.
- Admin breadcrumbs render styled on Django 6.0 as well as 6.1, and the Help and List Keys links on the cache detail, key detail and add-key pages render on Django 6.0 again; they were dropped entirely there.
- A cache whose `BACKEND` cannot be imported shows a message on every admin page instead of returning a 500 on the cache detail, key detail and add-key pages.
- The admin danger zone (clear all versions, FLUSHDB) is shown only on backends that implement it, instead of failing with an `AttributeError` when the button is pressed.
- A failed first operation while creating a key in the admin keeps you on the create page instead of bouncing you to the key list with "key does not exist".
- The native backends read Redis's glob dialect, not `fnmatch`'s. `keys`, `scan` and `delete_pattern` on `LocMemCache` and `DatabaseCache` spelled negation `[!a]` instead of `[^a]`, treated `!` as negation rather than a member, ignored `\` as an escape, and ran the pattern through `os.path.normcase`, which folds case on Windows. One translator now serves both backends and both the regex and SQL `LIKE` forms.
- `LocMemCache.zpopmin`/`zpopmax` and `DatabaseCache.lpop`/`rpop`/`zpopmin`/`zpopmax` reject a negative count. It sliced from the opposite end, so `zpopmax(key, -2)` popped all but the last two members instead of raising.
- `LocMemCache.zrangebyscore` honors the Redis rule that a negative `num` means "to the end". The idiomatic `LIMIT 0 -1` sliced `[0:-1]` and dropped the last member. `DatabaseCache` was fixed in 0.6.0; the two now agree.
- `LocMemCache.zadd` and `zincrby` coerce string scores to `float` the way Redis does. A stored `"1.5"` sorted as a string and made the next numeric write raise from inside `sortedcontainers`. `zadd` parses the whole mapping before it takes the lock, so an invalid score rejects the command instead of applying it halfway.
- `lpos` rejects `rank=0` on both native backends, with Redis's own message, and a negative rank applies `maxlen` from the tail. `maxlen` truncated the head of the list even when the rank asked for a tail scan, so the matches it was supposed to bound were never examined.
- `srandmember` with a negative count returns exactly `|count|` members, repeats allowed. Both native backends reached `random.sample` and raised `ValueError`.
- `LocMemCache.ascan()` works. It was left at the `BaseCachex` default and raised `NotSupportedError` even though `keys()`, which the default paginates over, is implemented; the admin's async key browser could not page a LocMem cache.
- `DatabaseCache` reports the key in its `WRONGTYPE` messages, matching `LocMemCache`. The message named only the expected type, so a failed compound operation gave no way to tell which key was wrong.
- Exclusive score bounds (`(1`) raise `NotSupportedError` naming the backend on `LocMemCache` as well, rather than a bare `ValueError` out of `float()`.
- `StreamCache` polls the stream instead of parking in `XREAD BLOCK` when its transport is a valkey-glide backend. Glide carries every command of a client over one connection, so the blocking read held up each publish behind it: with two pods in one process the publishes queued past glide's request timeout and mutations went missing on the other pod.
- `StreamCache` shares one consumer thread, one publisher thread and one pod identity per `LOCATION` within a process. Django hands out one cache instance per thread and per async context, so every ASGI request and every WSGI worker thread used to start its own consumer and publisher, neither of them collectable, and mint its own pod id, which made sibling consumers treat this process's own writes as remote.
- `StreamCache` pods converge when two of them write the same key inside the propagation window. A pod applies its own stream entries as well, so both land on the last entry in stream order instead of permanently holding each other's value. A pod still never reads back a value a later local write superseded, and its own `clear` coming back spares the keys it wrote after clearing.
- Restarting a `StreamCache` after `shutdown()` no longer leaves two consumers advancing the same stream cursor.
- `StreamCache.delete_pattern()` publishes one broadcast for the whole match instead of one per key, so a large pattern no longer exhausts the publish budget.
- The RESP semaphore's `{name}:state` and `{name}:claims` hashes carry a guard TTL of twice the longest lease, refreshed by acquire, extend and release. A holder that died without releasing left two keys per semaphore name behind indefinitely.
- Async connection pools of closed event loops are released. The per-loop registry used weak keys, but every open connection holds a transport that holds its loop, so a key never expired: a sync process awaiting the cache through `async_to_sync` leaked one pool and one TCP connection per call, and `close()` and `aclose()` were documented no-ops. Every pool lookup and every `close()` now drops the entry of each loop that has closed. Measured on a WSGI-shaped loop, 201 calls went from 201 registry entries and 202 server-side connections to 1 and 2.
- `type()` and `atype()` return `KeyType.UNKNOWN` for a key created by a Redis module. `KeyType(result)` raised `ValueError` on `ReJSON-RL`, `TSDB-TYPE` and friends, so a single module key took down any scan that reached it. A missing key still reads as `None`.
- `IntEnum` and `IntegerChoices` members with values 48 to 57 round-trip through the msgpack and ormsgpack serializers. msgpack packs a small int subclass as a single fixint byte, and that byte is an ASCII digit, so `decode()`'s int fast path read the member back as 0 to 9. A value that reads back as a number is now stored as a plain int; pickle still returns the enum member.
- `sadd()` rejects a member that the configured serializer turns unhashable. A tuple is hashable, but json, orjson, msgpack and ormsgpack all return it as a list, so the 0.6.0 hash check passed at the write and `smembers()` failed on the read. The check now runs on the round-tripped value.
- `expireat()` and `pexpireat()` with a deadline in the past delete the key on a stampede cache. The buffer was added to past deadlines too, so a key meant to expire immediately lived on for `buffer` seconds.
- `Pipeline.set()` with `nx` or `xx` and an immediate expiry no longer deletes a key it was not allowed to write. The zero-timeout branch queued an unconditional `DEL`; it now queues `EXISTS` for `nx` and a conditional `DEL` for `xx`, and `nx` together with `xx` reaches the driver, which rejects it.
- Pipelined `expire()`, `pexpire()`, `expireat()` and `pexpireat()` add the stampede buffer, and pipelined `ttl()`, `pttl()` and `expiretime()` subtract it, matching the client-side methods 0.6.0 fixed. All seven take a keyword-only `stampede_prevention` argument.
- Pipelined `xpending()` accepts the same arguments as the client. `count` alone is allowed, and a range without `count` raises `ValueError` instead of falling back to the summary form.
- `SerializerError` and `CompressorError` carry a message naming the codec, the payload and the underlying cause. They were raised bare.
- valkey-glide: a username configured without a password, which is what a nopass ACL user has, no longer fails client construction. Credentials are built only when there is a password.
- valkey-glide: `hmget()` with no fields returns an empty list instead of sending a malformed command to the server.
- valkey-glide: `lpop()` and `rpop()` with a count return `None` for a missing key, so a miss is still distinguishable from an empty pop.
- valkey-glide: pipelined `xpending()` and `xpending_range()` decode their replies the way the direct calls do, instead of handing back raw driver output.
- valkey-glide: `xinfo_stream(full=True)` decodes the group and consumer entries nested inside its list values.
- valkey-glide: async clients belonging to closed event loops are closed and dropped from the per-loop registry. A glide client holds its loop, so the weak key never expired and a process running one `asyncio.run()` per request leaked a client and its connections every time.
- valkey-glide: `type()` returns `KeyType.UNKNOWN` for a server type the enum does not model and `None` for a missing key, instead of raising.
- valkey-glide: pipelined `set()` takes the full option set (`px`, `exat`, `pxat`, `keepttl`, `get`), rejects conflicting expiry flags, and returns the old value for `get=True`.
- valkey-glide: stream, list and server methods use the parameter names the adapter protocol declares (`entry_id`, `start` and `end`, `slowlog_get(count)`), so keyword calls from the cache and pipeline layers bind.
- valkey-glide: a blocking lock acquire no longer sleeps past its `blocking_timeout`.
- valkey-glide: cluster pipelines build a `ClusterBatch`, the batch type the cluster client is declared to execute.

### Documentation

- `set_with_flags(get=True)` documents what it returns: the driver's raw previous value, which the cache layer decodes.
- The documented server requirement said "Valkey 7.0+". Valkey's first release was 7.2, so the README, the docs home page and the installation page now say "Valkey 7.2+ or Redis 6.0+".
- The distributed-locking recipe passed `timeout` to `lock.acquire()`. On the redis-py and valkey-py backends `cache.lock()` returns the driver's own `Lock`, whose `acquire()` takes `blocking_timeout`, so the snippet raised `TypeError`. The recipe sets `timeout` on `cache.lock()` instead, which every backend accepts.
- The example projects' READMEs contradicted the code next to them: the wrong Valkey port for the full example, a `cd example` for a directory named `simple`, an `admin`/`admin` login where `run.sh` creates `admin`/`password`, a `../.venv` path one level short, and a cache table missing the `cluster`, `sentinel`, `sync` and `stream_transport` aliases. The full example's `run.sh` also announced a `SyncCache` backend that does not exist; the alias is `StreamCache`.

### Tooling

- The release workflow runs `tests/admin/` as well as `tests/cache/` before tagging.
- Dropped the `scripts/**` ruff per-file-ignores entry; the directory it covered was removed in 4acfe2e.
- The cache test matrix is parametrized by topology (`default`, `cluster`, `sentinel`) instead of an independent client class and sentinel flag. The old pair produced six cells of which two were duplicates and one differed only by db number; `client_class` and `sentinel_mode` are now derived from the active topology.
- Container fixtures hand their addresses to the cache fixtures directly instead of exporting them into the process environment, attach a `LogMessageWaitStrategy` instead of calling testcontainers' deprecated `wait_for_logs`, and pick test db numbers with `crc32` rather than the per-process randomized `hash()`.
- Tests that toggled `DJANGO_REDIS_SCAN_ITERSIZE` and `DJANGO_REDIS_CLOSE_CONNECTION`, settings the package has never read, now drive the real `itersize` argument and a real close. The vendored `SettingsWrapper` is gone in favour of pytest-django's `settings` fixture.
- The valkey-glide adapter no longer needs its module-wide `ignore_errors` mypy override or its file-wide `ruff: noqa: ERA001`. It type-checks and lints with the rest of the package.

## 0.6.0 (August 2026)

### Breaking changes

- `DatabaseCache` stores its collections as tagged subclasses. Lists, sets, hashes and sorted sets are written as `_List` / `_Set` / `_Hash` / `_ZSet`, the same tagging `LocMemCache` has used since 0.4.0, and a compound operation on an untagged value raises `WrongTypeError`. Rows written by an older version hold plain containers, so `lpush` on a row a previous release wrote will now raise. Clear the cache table, or re-write those keys, before upgrading. Without the tags a plain `set("k", ["a"])` was indistinguishable from a list key, and `type()` had to guess from the value's shape.
- `type()` returns `None` for a key that does not exist. The `BaseCachex` default answered `STRING` for every key including missing ones, which contradicted its own `KeyType | None` annotation and made the admin's type column claim a type for keys that had expired between the scan and the read.
- `scan(key_type=...)` filters. The argument was accepted and silently ignored by the `BaseCachex` default, by `StreamCache` and by `DatabaseCache`, so the admin's Type filter returned unfiltered results on every backend that did not override `scan`. `DatabaseCache` pushes the filter into the query; the others apply it per key.
- `get_client()` and `get_async_client()` return a shared client. The redis-py and valkey-py adapters built a new driver client for every single cache operation; there is now one client per connection pool, parked on the pool so it dies with it. Client construction went from 55 µs to 0.06 µs per call and the cyclic garbage each operation left behind (23 objects) is gone. Callers that mutated the returned client, for example with `set_response_callback`, now mutate an object other calls share.
- `StreamCache` broadcasts `delete_many` as a list. The keys used to travel as a single `\x00`-joined string, and `\x00` is legal in a Django cache key, so a key containing one split into fragments and remote pods deleted the wrong keys. Pods on the old and new code disagree about this one message type; drain or rotate `stream_key` during the rollout.
- `Pipeline.set()` applies the backend's `TIMEOUT`. It defaulted to `timeout=None` and stored the key forever, while `cache.set()` on the same backend applied the configured default. The signature now takes the same `DEFAULT_TIMEOUT` sentinel, and negative and float timeouts are normalized the way `cache.set()` normalizes them instead of reaching the driver and raising.
- The whole TTL surface honors the stampede buffer. With `stampede_prevention` on, `set()` stored keys at `timeout + buffer` but `expire()`, `pexpire()`, `expireat()` and `pexpireat()` wrote the bare timeout, so one `expire()` call silently stripped the buffer off a stampede-managed key; `ttl()`, `pttl()` and `expiretime()` reported the raw value, which read as `buffer` seconds longer than the key's logical life. All seven now add or subtract the buffer, and all seven take a keyword-only `stampede_prevention` argument to reach the raw value. With prevention off the buffer is 0 and nothing changes.
- `sadd()` rejects an unhashable member. Every set reader returns a Python `set`, so a member that cannot be hashed was stored happily and then took down the whole key on the next read. It raises `TypeError` at the write now, and rejects the whole call rather than adding part of it.
- `RespCache`, `RespClusterCache` and `RespSentinelCache` raise `ImproperlyConfigured` when used as a `BACKEND` directly. They bind no driver and exist to be subclassed; naming one used to fail later with an obscure attribute error.
- `xpending()` rejects filters given without `count`. Passing `start`, `end`, `consumer` or `idle` without a `count` silently fell back to the unfiltered summary form and returned a dict where a per-message list was asked for. It raises `ValueError` now, on the client, the pipeline and both async twins.

### New features

- Async admin surface on `TieredCache`: `akeys`, `aiter_keys`, `ascan`, `attl`, `apttl`, `atype`, `apersist`, `aexpire` and `adelete_pattern`.
- `ascan()` on `DatabaseCache`, which had the sync `scan` but inherited the `NotSupportedError` default for the async twin.
- `get_many()` and `incr_version()` on `LocMemCache`, both collection-aware and both reading under a single lock acquisition.
- `expire()` and `pexpire()` accept a `timedelta` on the valkey-glide adapter, matching the `int | timedelta` the public `RespCache` API declares. Glide renders arguments with `str()`, so a `timedelta` previously reached the server as `EXPIRE k 0:05:00`.

### Fixes

- A hash field written by `HINCRBYFLOAT` can be read back. `decode()` short-circuited on `int()` only, so a float came back through `hget`/`hgetall`/`hvals` as a `SerializerError` that took the whole hash down with it. Note the reverse direction still does not hold: `hset` serializes a float, so it is not incrementable, because raw float encoding breaks wire compatibility with Django's own `RedisCache`, whose deserializer tries `int()` then `pickle.loads()`. This is documented on `encode()` and on `hincrbyfloat`.
- `aget_or_set()` awaits a default that returns an awaitable. It tested the callable with `iscoroutinefunction`, which answers `False` for an object with an `async def __call__` and for a sync function returning a coroutine; it calls the default and tests the result now.
- `incr_version()` works on a cluster with a hash-tagged `KEY_PREFIX`. It checked the user key for a hash tag rather than the made key, so `KEY_PREFIX="{app}"` was rejected even though every key it produces is colocated.
- `CULL_FREQUENCY` and `MAX_ENTRIES` are no longer forwarded to the driver. They are Django's generic cache parameters, read by `BaseCache.__init__`, and passing them on to a connection pool that has never heard of them is at best ignored.
- `semaphore()` reports a missing `lease` as a `ValueError` naming the argument, before it builds a key or reaches the adapter.
- `LocMemCache.get()` and `incr()` no longer raise `KeyError` under concurrent writes. Both inherited Django's implementation, which takes the non-reentrant `_lock` once to check expiry and again to read the value. A `lpush` landing in between registered the key in `_collections` with no expiry, so the inherited `get()` passed the expiry check and then hit a bare `self._cache[key]`. Both now do the whole read under one acquisition.
- A `LocMemCache` rewrite at capacity no longer loses the key it is writing. Every collection write ran the cull check, so a rewrite at `MAX_ENTRIES` could evict the very key being written; the write then re-added it without its TTL and evicted a sibling for nothing. Culling now runs only on a first write.
- `lpush`/`rpush` with no values return 0 instead of creating an empty, immortal key. Redis never creates an empty list, and the key this made had no TTL and no list operation that could reach or reap it.
- `lpop`/`rpop` reject a negative count. It sliced from the opposite end, so `rpop(key, -2)` popped from the head. Redis rejects the argument outright.
- `TieredCache` mutates L2 before invalidating L1. In `delete`, `delete_many`, `incr`, `decr`, `expire`, `delete_pattern`, `clear` and their async twins, the other order let a concurrent read repopulate L1 from the pre-mutation L2 value.
- `TieredCache` reaches L1 through its async methods from async code. Every async path called L1's sync method, which raises `SynchronousOnlyOperation` on an L1 that guards against it.
- `TieredCache.delete_many()` and `clear()` report success against a non-RESP L2. Stock Django backends return `None` from those calls, and `bool(None)` / `None or 0` turned a successful clear into a failure and two successful deletes into zero.
- `TieredCache` no longer masks an `AttributeError` raised inside an L2 method as "operation not supported". The capability probe now uses `getattr`, so only a genuinely absent method reports `NotSupportedError`; `_wrap_iter` rewraps the one raised lazily out of a generator.
- `TieredCache` rejects a tier alias pointing at itself, and duplicate aliases, at construction instead of recursing until `RecursionError` on the first `get()`.
- Semaphore `RELEASE` no longer invents a `used` counter when the state hash has been evicted. It wrote `max(0, 0 - weight)`, and `ACQUIRE` only re-derives the counter from the live claims when it is absent, so the invented zero was trusted on the fast path and admitted far past capacity. `RELEASE` leaves the counter absent instead.
- The semaphore queue score comes from the server. It was `int(time.time() * 1000)` read on the client, and admission is strictly head-of-queue, so a host with a slow clock sorted ahead of every other host's waiters and starved them for as long as it kept enqueueing. `ACQUIRE_LUA` reads `TIME` itself and no longer accepts a timestamp.
- Growing a local semaphore's capacity wakes the parked waiters it now fits. They slept out their full timeout, or until an unrelated release, before being admitted.
- Releasing a local semaphore no longer raises when a waiter's event loop has closed. `call_soon_threadsafe` raised `RuntimeError` out of `release()`, nobody else was notified, and the dead waiter stayed at the head so every later `release()` raised again.
- The local semaphore registry drops names nothing references. It held every name forever, so `cache.semaphore(f"job:{id}")` grew it without bound.
- The RESP semaphore deletes its `{name}:state` hash on the last release. The claims hash and the queue zset already self-delete when emptied, so that key was the one un-expiring leak per name.
- `release()` warns when the claim was already reaped. `RELEASE_LUA` answers `not_owned`, meaning the work ran past its lease unprotected, and that was discarded silently. It logs a warning rather than raising, because `release()` is what `__exit__` calls and raising would chain over whatever the body was already reporting.
- The capacity-change warning points at the caller. `stacklevel` was hardcoded for the `cache.semaphore()` call chain, so constructing a `Semaphore(...)` directly blamed the frame above the real call site.
- Pipelines translate `WRONGTYPE`. The redis-py and valkey-py adapters patch `execute_command` on the client instance, and a pipeline is a fresh object with its own unpatched method, so a batched type error escaped as the driver's raw `ResponseError` instead of `WrongTypeError`.
- Count-form `lpop`/`rpop` report a missing key as `None`. The driver's nil reply collapsed to `[]`, so a caller checking `is None` never saw the miss that `LocMemCache` reports.
- `hmget(key)` with no fields returns `[]` instead of sending an invalid command.
- RESP3 dict replies no longer break stream result decoding. `OPTIONS {"protocol": 3}` made the driver return a mapping, which unpacked as bytes keys and raised `ValueError`.
- Sentinel connections inherit the driver's socket timeouts. `sentinel_kwargs` defaulted to `{}`, which suppressed redis-py's fallback, so a blackholing sentinel blocked discovery instead of timing out.
- Connection pools are keyed stably. `_options_key` fell back to `repr()`, which embeds `id()` for a plain object, so an option like a `Retry` instance opened a brand-new pool for every cache instance. Values are now reduced structurally, falling back to `(type, repr)` only at the leaves.
- An empty `LOCATION` server list raises `ImproperlyConfigured` naming the backend, instead of reaching `random.randint(1, -1)` on reads and `_servers[0]` on writes.
- `INFO` parses on a valkey-glide cluster. An unrouted `INFO` takes glide's all-primaries default and answers `{node: payload}`, which the string parser choked on; the cluster adapter now pins it to a random node, the way valkey-py's `default-node` flag does. The admin's memory and keyspace panels were blank on cluster.
- `set_many()` on valkey-glide cannot leave a key without a TTL. It ran `MSET` plus N `EXPIRE`s, so a batch that broke partway left keys resident forever. It issues per-key `SET ... PX` in one batch now.
- The valkey-glide lock raises `LockError`, not `RuntimeError`, from `__enter__` and `__aenter__`, and `extend()` refuses a lease-less lock. `PTTL` is -1 without a lease and the Lua clamped it to 0, so extending turned a lock that never expires into one that self-releases.
- `aclose()` on the valkey-glide adapter closes the per-loop client. Glide clients define no `__del__`, so a client dropped from the registry never released its connection.
- The valkey-glide pipeline raises on an empty `hset` mapping instead of skipping the enqueue. `Pipeline.execute` zips results against decoders with `strict=True`, so a queueing method that returns without queueing shifts every later result.
- `xclaim(justid=True)` returns `str` IDs from a pipeline, matching the non-pipeline path.
- `ttl`, `pttl` and `expiretime` normalize -1 to `None` in pipelines, and `rename` returns a `bool` across drivers. The raw sentinel and the raw `"OK"` leaked through, so a persistent key looked nearly expired and `results[i] is True` held on redis-py but not on glide.
- `DatabaseCache` deletes a collection row when the last member goes. `lrem`, `srem`, `hdel`, `zrem` and `ltrim` left an empty container behind, which Redis never does.
- `DatabaseCache` rejects the `(` exclusive score bound before the read rather than raising a bare `ValueError` from `float("(1")`, and honors the Redis rule that a negative `num` means "to the end".
- A `KEY_PREFIX` containing a glob character no longer matches sibling prefixes. The prefix and version portion of the key was translated into SQL wildcards along with the user pattern, so `KEY_PREFIX="svc?1"` made `keys("*")` return rows belonging to `svcX1`.
- `DatabaseCache.info()` reports a real `expires` count, resolves the write alias once per compound operation so the cursor and the transaction cannot land on different connections under a routing router, and deletes patterns in `itersize` chunks instead of one unbounded `IN (...)`.
- `StreamCache.get_or_set()` follows Django's semantics: a stored `None` is a hit, and a `None` default is stored. Keying off `val is None` re-invoked the callable and re-published a broadcast on every call for a key holding `None`.
- `StreamCache.info()["last_read_age"]` stops growing on an idle stream. `_last_read_time` was stamped only when a message arrived.
- `StreamCache` raises `NotSupportedError` rather than `AttributeError` for the cachex operations it does not implement, and `expire()` accepts a float.
- `StreamCache` joins its consumer thread with a bound at interpreter exit. The join now runs through `threading._register_atexit`, which fires before `concurrent.futures`' own unbounded join. `close()` stays a no-op deliberately: Django fires it on every `request_finished`.
- Key patterns match case-sensitively on Windows. `LocMemCache` and `StreamCache` used `fnmatch.fnmatch`, which normcases both sides; Redis globs never do.
- `scan(count=0)` is honored. `count = count or 100` turned an explicit zero into 100 on the `BaseCachex` default, on `DatabaseCache` and on `StreamCache`.
- `OrmsgpackSerializer` accepts non-string mapping keys, like `MsgpackSerializer` with its `strict_map_key=False`. `OPT_NON_STR_KEYS` is needed on the way out as well as in, so swapping serializers no longer changes what is cacheable.
- `LocMemCache.info()` no longer walks the import graph. `_deep_getsizeof` recursed into a cached value's module references, and one value holding `sys` cost 11 ms and 2.4 MB, all under the cache's lock. Modules, classes and functions are now sized opaquely: 14 µs and 2.3 KB for the same value.
- A LocMem or Database `BACKEND` no longer imports a driver. `django_cachex/__init__.py`, `django_cachex/adapters/__init__.py`, `django_cachex/cache/__init__.py` and `django_cachex.exceptions` all pulled redis-py and valkey-py eagerly, so every install paid both import times whether or not it talked to a server. Names resolve on first access now.
- The key admin localizes nothing it feeds back to the server. Sorted set scores, TTL inputs, list indices and pagination page numbers rendered through Django's number formatting, so under `de` or `USE_THOUSAND_SEPARATOR` a score of `1234.5` submitted as `1.234,5` and the edit failed.
- Creating a key in the admin requires `add_key`. A materializing action on a key that does not exist, and the create-mode form itself, were gated only on `change_key`.
- Setting an admin TTL to `"00"`, `"+0"` or `"-0"` makes the key persistent instead of deleting it. The check compared the raw string, so those spellings reached `expire(key, 0)`.
- The admin's `lrem` removes one occurrence by default. The count defaulted to 0, which is LREM's "remove every occurrence", while the confirmation dialog described removing an item.
- The admin reports a `ZADD` that changed nothing. It read the added-count, which is 0 for an existing member whether or not the score moved; it passes `CH` now and reports a no-op as a warning.
- The admin key detail survives a value it cannot read. Only `WrongTypeError` was caught around `cache.get`, so a key holding a payload the serializer rejects raised out of the view.
- Sorted set members that deserialize to a list or dict are marked read-only in the admin instead of raising `TypeError` when submitted back.
- The admin's `xtrim` is exact, matching what its confirmation dialog promises, and the Help link preserves the current page and type filter.
- The admin's per-object history and delete routes return 404. `ModelAdmin` registers them for every model, and the cache and key admins have no objects for them to act on.

### Documentation

- The `OPTIONS` reference says which backends honor each key. valkey-glide takes a different configuration surface and silently ignores the rest, which the reference did not mention; there is now a scope table, a valkey-glide section covering `db`, `use_tls`, `username`, `password`, `request_timeout` and `client_name`, and a note that `ssl_*` does not reach it.
- The valkey-glide description says what it actually does: `glide_sync.GlideClient` for the sync surface and `glide.GlideClient` for the `a*` methods, not an async client wrapped transparently.
- `cache.lock()`'s documented signature matches the code, including that the second positional argument is `version`, not the lease.
- The admin permission list matches what the views enforce: `change_cache` gates cache-wide actions, `change_key` gates every key detail mutation including TTL and persist.
- The README and `docs/index.md` no longer claim `LocMemCache` and `DatabaseCache` carry the same data-structure operations as the RESP backends. They carry the hash, list, set and sorted set operations; streams are RESP-only.
- README screenshots load on PyPI, which does not resolve repository-relative image paths.
- `semaphore()` documents the keys it keeps outside the cache's namespace (`{name}:state`, `:claims`, `:queue`), which survive `clear()` and do not show up in `keys()` or the admin, and why they have to.
- `sscan()` and `sscan_iter()` document that `match` is applied by the server to the stored member, which is the serialized form unless the member is a plain string.
- `incr()` documents where it diverges from `BaseCache.incr` and from `LocMemCache`.

## 0.5.1 (August 2026)

### Improvements

- `TieredCache.get_many` batches its L2 TTL lookups. Repopulating L1 issued one `TTL` call per key on top of the `MGET`, so a 100-key miss cost 101 round trips against the very tier it exists to spare. The TTLs now go through a single pipeline, falling back to the per-key path for an L2 that can't pipeline (stock Django backends, LocMem).
- The sdist no longer ships example and benchmark files. `README.md` and `pyproject.toml` were unanchored globs in the hatch config, so they matched at any depth and pulled in `examples/*/README.md`, `examples/full/pyproject.toml` and `benchmarks/README.md`.
- Free-threaded CPython is verified in CI again. The only cp314t job went out with the redis-rs wheels, leaving the free-threading classifier and the README's support claim unchecked. The cache suite now runs on 3.14t.

### Fixes

- `django_cachex.adapters._pipeline_parsers` removed. Nothing imported it; it existed so the Rust pipeline could resolve the parsers by name, and it kept shipping in the wheel after that driver was dropped.
- `version` and `PackageNotFoundError` no longer leak into `django_cachex`'s namespace. They were reachable as `django_cachex.version` purely because of how `__version__` is computed.
- The admin key-size lookup logs its failures. It swallowed every exception and returned `None`, unlike the sibling helpers that log, so a broken key showed a blank size with nothing in the log to explain it.
- Admin breadcrumbs render with Django 6.1's markup. Django 6.1 moved from `<div class="breadcrumbs">` to `<ol class="breadcrumbs">` with `<li>` items and rescoped every rule in `admin/css/base.css` to `ol.breadcrumbs`. The four templates overriding the block still emitted the old `div`, so the cache detail, key list, key detail and key add breadcrumbs drew unstyled.
- Admin object tools sit next to the page title again. The cache detail, key detail and key add templates emitted their `<ul class="object-tools">` inside `{% block content %}`, but Django's `base.html` positions that list from `{% block object-tools %}` in the flex row beside the `<h1>`. The Help and List Keys buttons landed below the title, left-aligned.

### Documentation

- The `username` connection option is documented. It works as an `OPTIONS` key on every adapter and takes precedence over the URL, which matters when an ACL user name would need URL-escaping.

## 0.5.0 (August 2026)

### Breaking changes

- The `redis-rs` backends are gone. `RedisRsCache`, `RedisRsSentinelCache` and `RedisRsClusterCache`, the `django_cachex.adapters.redis_rs` module, the `redis-rs` extra and the `django-cachex-redis-rs` companion package have all been removed. The Rust driver was never published to PyPI, so no released install can break; switch to `ValkeyCache`, `RedisCache` or `ValkeyGlideCache`. A standalone [redis-rs-py](https://github.com/oliverhaas/redis-rs-py) binding is in progress and cachex may grow an adapter for it once that package stands on its own.
- `django_cachex.Lock` and `django_cachex.AsyncLock` removed. They existed only to wrap the Rust driver's raw lock commands. `cache.lock()` is unchanged on every remaining backend, and `LockError` / `LockNotOwnedError` are still the exceptions it raises.

## 0.4.2 (August 2026)

### Improvements

- Every value input in the key admin is the same textarea. Push, add and set-field forms were single-line `<input type="text">` fields 100 to 150px wide, so a multi-line JSON value could not be typed into them at all. They are now four-row textareas laid out like the string editor. Item, field and member rows use a two-row version of the same field, with their buttons stacked beside it so a row stays compact. The field is one Django template partial (`{% partialdef value-input %}`), and the fieldsets carry the admin's own `monospace` class instead of five ad-hoc font declarations.
- Sorted set members and set members can be edited. Both rendered as static `<code>`, so a typo in a member meant remove-and-re-add by hand. Renaming adds the new member before dropping the old one, so an interrupted request leaves a visible duplicate rather than losing the member.
- Hash field names can be edited. They rendered as static `<code>` for the same reason. Redis has no `HRENAME`, so the rename runs `HSET` plus `HDEL` inside the existing compare-and-swap script: one round trip, atomic, and still refused if the value changed since page load. It also refuses to overwrite a field name that is already in use.

### Fixes

- Container entries that are not JSON-serializable are read-only. They display as `repr()`, and nothing stopped that text from being submitted back, which stored the repr string over the real value. Their Update and Remove buttons are now disabled, matching the guard the string editor already had.
- `xadd` parses its value like every other handler. It was the one action that stored the submitted text raw, so a stream entry could not hold a number, list or dict the way a list item or hash field can.
- The string editor strips surrounding whitespace. Every container handler already did; the string path did not, so a stray newline changed the stored value.

## 0.4.1 (August 2026)

### Fixes

- The admin's value textarea no longer overflows its container. It was sized `width: 100%` without `box-sizing: border-box`, so the admin's 1px border and 8px side padding landed outside that width and clipped the right edge by 18px.
- Admin warnings and field errors are readable in dark mode. The key detail page's complex-value notice inlined light-theme colors and set no foreground, so dark mode drew `.help`'s near-white text on pale yellow. It now uses the admin's own `messagelist` warning styling, which follows the active theme. The add form's client-side field errors hardcoded the light-theme error red for the same reason and now take `--error-fg`.
- Semaphore `release()` and `extend()` no longer act on a token they don't own. Both re-read `self._token` after their "not held" guard, so a racing re-acquire on the same instance could install a new token in between; the command then ran against that live claim, and `release()` cleared the field to `None`. The token is now snapshotted under the same lock `_claim()` uses, and `release()` clears it only if it is still the one it entered with.
- RESP semaphores no longer wedge when Redis evicts their bookkeeping. The reaper only ever adjusted the `used` counter by a delta, so losing the claims hash to `maxmemory` eviction left `used` pinned at capacity with nothing left to subtract and every later acquire failing forever. Losing the state hash instead made `used` read zero and admitted past capacity. `used` is now derived from the surviving claims during the walk the reaper already performs, which costs no extra round trip and self-heals both directions.

## 0.4.0 (August 2026)

!!! note "Historical record"
    Entries below describe 0.4.0 as released. The Rust extension and the
    `redis-rs` backends were removed in 0.5.0, and with them the binary wheels:
    the package is pure Python again and builds a single wheel with hatchling,
    so the cibuildwheel and cp314t wheel entries no longer describe the current
    release.

### Breaking changes

- Python 3.14+ required. Dropped support for 3.12 and 3.13. The package now ships on cp314 and cp314t (free-threaded) wheels.
- Django 6.0+ required. Dropped support for Django 5.2.
- `LocMemCache` data structures use tagged subclasses. Lists, sets, hashes, and sorted sets are stored as dedicated subclasses (`_List`, `_Set`, `_Hash`, `_ZSet`) rather than plain Python types, and cross-type access raises `WrongTypeError` instead of silently coercing, matching real Valkey/Redis ``WRONGTYPE`` semantics.
- `LocMemCache` bypasses pickle for tagged collections. Mutations happen in place; the prior copy-on-read/copy-on-write contract no longer holds. Code that relied on getting a detached snapshot from `cache.get()` for these types now sees the live structure.
- `StreamCache` wire format changed. Stream entries now flow through the transport's serializer + compressor pipeline instead of raw pickle. Pods running the new code cannot read entries written by older pods on the same stream; coordinate the rollout (drain or rotate `stream_key`).
- `hmset` removed. Use `hset(key, mapping=...)` or `hset(key, items=...)` (flat key-value list, matching redis-py/valkey-py).
- `django_cachex.unfold` removed. The django-unfold theme variant of the admin is gone, along with the `[unfold]` extra and `examples/unfold/`. Plain `django_cachex.admin` remains. Unfold support may return as a thin theme override once the core admin app stabilises.
- Lock parameters renamed. `cache.lock(timeout=...)` is now `cache.lock(lease=...)` (TTL of the held lock); `lock.acquire(blocking_timeout=...)` is now `lock.acquire(timeout=...)` (max wait). The old `blocking_timeout` kwarg raises `TypeError`; the constructor's new `timeout=` kwarg means "max wait" rather than "TTL". No deprecation shim. This aligns the lock API with the upcoming `cache.semaphore(...)` primitive.
- `ZStdCompressor` renamed to `ZstdCompressor` (`django_cachex.compressors.zstd.ZstdCompressor`). Update `OPTIONS["compressor"]` strings.
- `LzmaCompressor` constructor `preset=` renamed to `level=` for consistency with the other compressors. All compressors now accept `level=` (mapped to the underlying library's native parameter).
- `PickleSerializer` no longer raises `ImproperlyConfigured` for `protocol > pickle.HIGHEST_PROTOCOL`. Pickle's own `ValueError` is now surfaced at the first `dumps` call, wrapped as `SerializerError` (with the pickle exception as `__cause__`).
- `CachexCompat` removed. The mixin class that emulated the cachex ext surface on top of an arbitrary `BaseCache` is gone, along with the admin's "wrapped" support tier. Django's `BaseCache` and the stock backends (`LocMemCache`, `RedisCache`, `DatabaseCache`, `FileBasedCache`, `MemcachedCache`, `DummyCache`) deliberately don't expose key listing, so the wrap couldn't drive the admin's browse views meaningfully. Use `django_cachex.cache.LocMemCache` / `DatabaseCache` (drop-in replacements) for full admin support; non-cachex backends now show as "limited" (configuration only).
- Cluster `LOCATION` with a database number now raises on the redis-py and valkey-py cluster backends. Those two are built with the driver's `from_url()`, which rejects a non-zero `db` in the URL path or query (`RedisClusterException` / `ValkeyClusterException`). The old code read only host and port off the URL, so `redis://host:6379/1` connected to db 0 without complaint. Cluster has no `SELECT`, so the number was never honored; drop it from `LOCATION`. `ValkeyGlideClusterCache` and `RedisRsClusterCache` still ignore it silently.

### New features

- Rust I/O driver (experimental). Optional native driver built on PyO3 + tokio + redis-rs, shipped as a separate `django-cachex-redis-rs` package. Interfaces and behavior may change, and it has seen less production testing than the redis-py/valkey-py paths. Opt in via the `redis-rs` extra (`pip install django-cachex[redis-rs]`); without it, only the pure-Python backends are pulled in and the `RedisRsCache` classes raise a clean `ImportError` on first use. Set `BACKEND` to one of `RedisRsCache`, `RedisRsClusterCache`, or `RedisRsSentinelCache`. Sync and async share one tokio runtime; async dodges the threadpool round-trip.
- `valkey-glide` adapter (experimental). Optional Rust-cored client from the Valkey project. Interfaces and behavior may change, and it has seen less production testing than the redis-py/valkey-py paths. Opt in via the `valkey-glide` extra. Standalone (`ValkeyGlideCache`) and cluster (`ValkeyGlideClusterCache`) topologies are exposed; Sentinel is not (`valkey-glide` itself does not ship a Sentinel client).
- `WrongTypeError` exception. Backends now translate Redis ``WRONGTYPE`` responses into a single `django_cachex.WrongTypeError` (subclass of `TypeError`) so user code can catch one exception across LocMem, redis-py, valkey-py, valkey-glide, and the Rust adapter.
- Async ext methods on LocMem and Database. The full async data-structure surface (`alpush`, `ahset`, `azadd`, `attl`, `aexpire`, ...) is now available on `LocMemCache` (direct sync calls; in-memory, so no I/O to offload) and `DatabaseCache` (via ``sync_to_async``, the same path Django uses for ``BaseCache.aget``). They no longer raise `NotSupportedError` from async views.
- `StreamCache` backend. Stream-synchronized in-memory cache: reads are local, writes broadcast over a Redis Stream, a daemon thread on each pod consumes the stream and applies remote changes. Read-heavy, write-light, eventually consistent.
- `TieredCache` backend. Composes two existing `CACHES` entries as L1 (fast, e.g. LocMem) and L2 (durable, e.g. Redis), with TTL propagation and pull-through reads.
- Cache-stampede prevention. TTL-based XFetch via `OPTIONS["stampede_prevention"]` (or `stampede_prevention=` per call). Configurable buffer/beta/delta.
- `LocMemCache` and `DatabaseCache` extensions. Drop-in replacements for the Django builtins, adding data-structure ops, TTL helpers, and admin support. Compound read-modify-write ops on `LocMemCache` are serialized via a per-backend `RLock` (#62).
- `orjson` and `ormsgpack` serializer extras.
- Free-threaded CPython (3.14t) support. A cp314t wheel is built; `_redis_rs` works with the GIL disabled. The Rust driver also runs on the free-threaded build.
- PyPI wheels via cibuildwheel. Wheels for Linux x86_64, Linux aarch64, macOS arm64, and Windows amd64, on cp314 and cp314t.
- Async pool sharing. A single async connection pool is shared across per-task `Cache` instances (#83), avoiding the thundering-herd reconnect on cold start.
- Pipeline parity. Stream ops, CAS ops, missing key ops (`persist`/`pttl`/`expireat`/etc.), context manager, `zpopmin`/`zpopmax` default `count=1` aligned with the cache API.
- Compressors gain a uniform `level=` parameter (gzip, lz4, zstd join zlib/lzma in exposing it). Defaults match each library's own default.
- Serializer/compressor wrappers consolidated. Subclasses now implement `_dumps`/`_loads` (serializers) or `_compress`/`_decompress` (compressors); the base classes wrap the boilerplate (`SerializerError` / `CompressorError` translation, int-passthrough on loads).
- Weighted semaphores. New `cache.semaphore(name, capacity, *, weight=1, lease=..., timeout=...)` and `cache.asemaphore(...)` for gating concurrent access by a budget (counting or weighted). Backed by an in-process FIFO deque on `LocMemCache` and by Lua scripts on the RESP backends (redis-py, redis-rs, valkey-py, valkey-glide). Cluster mode is supported via `{name}` hash-tag colocation. Sync and async APIs share state per cache instance; lease-based crash reclaim on the RESP backend (no heartbeat). See `docs/recipes.md` for examples.

### Performance

- `LocMemCache` sorted sets are O(log N). Sorted-set operations now back the underlying dict with a `sortedcontainers.SortedList` sidecar for O(log N) insertion, deletion, and rank queries; previous implementation was O(N log N) per write. Adds `sortedcontainers>=2.4` as a runtime dependency.
- `LocMemCache` skips pickle for tagged collections. Tagged subclasses are mutated in place; reads and writes no longer round-trip through pickle for list/set/hash/zset/stream types.

### Fixes

- `LocMemCache.lpush`/`sadd`/`hset`/`hincrby`/`zadd`/etc. no longer lose updates under concurrent threads (#62).
- `delete_pattern` batches deletes to bound peak memory on broad patterns.
- `clear()` is now prefix/version-scoped instead of `FLUSHDB`. The old behavior is available as `flush_db()`.
- Compressor `compress` and `decompress` methods catch all exceptions and re-raise as `CompressorError`.
- Several cluster correctness fixes (script loading on replicas, set_many `timeout=0`).
- Fixed a crash when reading values small enough to have skipped compression (at or below the compressor's `min_length`).
- Admin cache/key changelists are compatible with Django 6.1.
- Semaphore waiters abandoned by crashed or cancelled callers are reaped instead of blocking the queue.
- valkey-glide: connection options reach the client instead of being reduced to host and port. The TLS scheme (`rediss`/`valkeys`) or `use_tls`/`ssl`, credentials from the URL or `OPTIONS`, the database index (standalone only), `request_timeout`, and `client_name` are all applied; `zadd` forwards the `gt`/`lt` flags, and pipelines support the stream commands.
- `TieredCache.set` forwards `nx`/`xx` to L2, and an L2 that is a stock Django backend no longer raises `TypeError`: `nx` falls back to `add()`, `xx`/`get` raise `NotSupportedError`, and a plain set drops the flags.
- `set(..., timeout=0)` deletes the key across all backends, matching Django's cache contract.
- `LocMemCache` aliases sharing a `LOCATION` share one store, including the tagged collections and the semaphore budgets, matching Django's builtin behavior.
- Admin: backend capability probes fail gracefully, and key URLs are quoted so keys with special characters open correctly.
- CI runs the test matrix against Django 6.1 in addition to 6.0.
- Dependabot automerge waits for every workflow run on the PR head to succeed before merging.
- `reverse_key()` handles a `KEY_PREFIX` containing colons, so `keys()`, `iter_keys()`, `scan()`, and the blocking list pops return user keys instead of raw internal ones.
- `DatabaseCache` compound ops (`rpush`, `sadd`, `zadd`, `hset`, ...) that lose the insert race against a concurrent writer now merge with the committed row instead of overwriting it.
- `LocMemCache` and `DatabaseCache` `hincrby`/`hincrbyfloat` reject non-numeric stored values with the same error as the server instead of truncating them.
- `TieredCache` rejects `KEY_PREFIX` in the standard top-level slot as well as in `OPTIONS`; it was silently ignored before.
- Sentinel: async connection pools are keyed by sentinel fleet, so two aliases sharing a service name no longer alias onto one pool.
- Semaphores: concurrent `acquire()` on one `RespSemaphore` instance can no longer double-claim and leak a slot until the lease expires.
- Admin: editing a key preserves its TTL and persistence instead of resetting it to the default timeout. Covers every backend, including those that report no-expiry as `-1` rather than `None` (`StreamCache`) and those without `pexpire` (`StreamCache`, `TieredCache`).
- `StreamCache` enqueues each broadcast while still holding the local write lock, so a pod's stream entries carry the order its writes were applied and replaying consumers converge on the writer's final value instead of an older one. `keys()` is scoped to the cache's own prefix and version.
- Pipelines discard their queued decoders when `execute()` raises, so a reused pipeline no longer decodes the next batch against a stale, misaligned decoder list. `AsyncPipeline` rejects a sync `with` at entry rather than after the block has run.
- The redis-py and valkey-py cluster backends are built from the full server URL through the driver's `from_url()`, so the TLS scheme, credentials, and query parameters survive; only the host and port were read before. The async Sentinel pool cache is also keyed on the sentinel fleet rather than the manager's `id()`, so the per-task adapters asgiref creates share one pool instead of each opening its own.
- `encode()` passes through exact `int` values only. `int` subclasses (`IntEnum`, `IntFlag`) now go through the serializer, so they come back as their own type instead of as plain ints.
- `touch()`/`atouch()` apply the stampede buffer to the TTL they write and accept a per-call `stampede_prevention=`. Touching a key under stampede prevention no longer strips the buffer and pushes every reader into a recompute.
- `DatabaseCache` key scans escape SQL `LIKE` metacharacters per database vendor, so a `KEY_PREFIX` or pattern containing `%`, `_`, or a backslash no longer matches unrelated rows.
- `DatabaseCache.zadd`/`zincrby` reject a non-numeric score with `ValueError` before writing, matching the server, instead of storing a value that breaks later range queries.
- `MAX_ENTRIES` culling covers the whole store: `LocMemCache` counts its tagged collections alongside the pickled entries and evicts them, and `DatabaseCache` compound ops (`rpush`, `sadd`, `hset`, ...) run the same cull check as a plain `set()` when they insert a new row.
- `LocMemCache` collection edge cases: `keys()` scopes to the requested version and skips expired-but-not-yet-culled entries, `incr()` on a collection key raises `WrongTypeError` instead of `KeyError`, and `sadd`/`hset`/`zadd` no longer leave an empty key behind when the call adds nothing (`zadd` where `nx`/`xx` skip every member, `sadd`/`hset` called with no members or fields).
- `rpop(count=0)` on `LocMemCache` and `DatabaseCache`, and `zpopmax(count=0)` on `LocMemCache`, return an empty list instead of draining the whole collection.

---

## 0.3.0 (February 2026)

- `expiretime()` and `set(get=True)` support: New cache methods for retrieving absolute expiry timestamps and atomic get-and-set operations.
- Atomic CAS operations in admin: Key detail edits use compare-and-swap via Lua-computed SHA1 fingerprints to prevent concurrent edit conflicts.
- Key detail pagination: Collection types (list, hash, set, zset, stream) are paginated at 100 items per page with `?page=N` navigation.
- Keys in admin sidebar: The key list is now a first-class sidebar entry with a cache filter for switching between configured caches.
- Simplified Lua script execution: `eval_script()` replaces the `register_script`/`LuaScript` registry with direct `EVAL` calls; redis-py handles script caching.
- Async data structure methods: All hash, list, set, and sorted set operations now have async counterparts on `RespCache` (e.g. `ahset`, `alpush`, `asadd`, `azadd`).
- Stream operations: Full sync and async support for Redis streams (`xadd`, `xread`, `xrange`, `xlen`, `xdel`, `xtrim`, `xinfo_stream`, `xgroup_create`, `xreadgroup`, `xack`, `xpending`, `xclaim`, `xautoclaim`, and more).
- Safe `clear()`: `clear()` now uses `delete_pattern("*")` to only remove keys for the current cache version and prefix, instead of `FLUSHDB`. Use `flush_db()` for the old behavior.
- Danger zone in admin: Cache detail view has a "Danger Zone" section with "Clear all versions" and "Flush database" actions. Key list view has a "Clear" button for safe prefix-scoped clearing.
- `hset` items param: `hset()` now accepts an `items` parameter (flat key-value list), matching the redis-py/valkey-py signature. `hmset` is removed.
- `delete_pattern` batched deletes: Deletes are now batched to prevent OOM on broad patterns.
- Multi-key params standardized: Set operations (`sdiff`, `sinter`, `sunion`, etc.) accept `KeyT | Sequence[KeyT]` consistently.

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

- Major admin refactoring: replaced service layer with helpers module, simplified views, restructured templates
- Unified admin views between classic Django admin and Unfold theme
- Added `_cachex_support` ClassVar to `CacheProtocol` for standardized support level detection
- Mixin-based class patching for cache wrappers (replacing intermediate extension classes)
- Extensive dead code cleanup across the codebase

#### Bug Fixes

- Fixed unfold template differences with classic admin
- Fixed `key_type` variable usage in unfold key detail template
- Fixed mypy and ty type-checking errors
- Fixed `!r` format spec for `KeyT` in error messages

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
- Improved cache admin UX: operations that aren't supported now fail gracefully instead of hiding UI elements

#### Bug Fixes

- Fixed LocMemCache keys showing "not found" when clicked in admin
- Fixed cache query parameter preservation in key search form
- Fixed editing for wrapped cache backends

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
