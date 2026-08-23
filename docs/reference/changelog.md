# Changelog

## 0.5.1 (August 2026)

### Improvements

- **`TieredCache.get_many` batches its L2 TTL lookups.** Repopulating L1 issued one `TTL` call per key on top of the `MGET`, so a 100-key miss cost 101 round trips against the very tier it exists to spare. The TTLs now go through a single pipeline, falling back to the per-key path for an L2 that can't pipeline (stock Django backends, LocMem).
- **The sdist no longer ships example and benchmark files.** `README.md` and `pyproject.toml` were unanchored globs in the hatch config, so they matched at any depth and pulled in `examples/*/README.md`, `examples/full/pyproject.toml` and `benchmarks/README.md`.
- **The `username` connection option is documented.** It works as an `OPTIONS` key on every adapter and takes precedence over the URL, which matters when an ACL user name would need URL-escaping.
- **Free-threaded CPython is verified in CI again.** The only cp314t job went out with the redis-rs wheels, leaving the free-threading classifier and the README's support claim unchecked. The cache suite now runs on 3.14t.

### Fixes

- **`django_cachex.adapters._pipeline_parsers` removed.** Nothing imported it; it existed so the Rust pipeline could resolve the parsers by name, and it kept shipping in the wheel after that driver was dropped.
- **`version` and `PackageNotFoundError` no longer leak into `django_cachex`'s namespace.** They were reachable as `django_cachex.version` purely because of how `__version__` is computed.
- **The admin key-size lookup logs its failures.** It swallowed every exception and returned `None`, unlike the sibling helpers that log, so a broken key showed a blank size with nothing in the log to explain it.

## 0.5.0 (August 2026)

### Breaking changes

- **The `redis-rs` backends are gone.** `RedisRsCache`, `RedisRsSentinelCache` and `RedisRsClusterCache`, the `django_cachex.adapters.redis_rs` module, the `redis-rs` extra and the `django-cachex-redis-rs` companion package have all been removed. The Rust driver was never published to PyPI, so no released install can break; switch to `ValkeyCache`, `RedisCache` or `ValkeyGlideCache`. A standalone [redis-rs-py](https://github.com/oliverhaas/redis-rs-py) binding is in progress and cachex may grow an adapter for it once that package stands on its own.
- **`django_cachex.Lock` and `django_cachex.AsyncLock` removed.** They existed only to wrap the Rust driver's raw lock commands. `cache.lock()` is unchanged on every remaining backend, and `LockError` / `LockNotOwnedError` are still the exceptions it raises.

## 0.4.2 (August 2026)

### Improvements

- **Every value input in the key admin is the same textarea.** Push, add and set-field forms were single-line `<input type="text">` fields 100 to 150px wide, so a multi-line JSON value could not be typed into them at all. They are now four-row textareas laid out like the string editor. Item, field and member rows use a two-row version of the same field, with their buttons stacked beside it so a row stays compact. The field is one Django template partial (`{% partialdef value-input %}`), and the fieldsets carry the admin's own `monospace` class instead of five ad-hoc font declarations.
- **Sorted set members and set members can be edited.** Both rendered as static `<code>`, so a typo in a member meant remove-and-re-add by hand. Renaming adds the new member before dropping the old one, so an interrupted request leaves a visible duplicate rather than losing the member.
- **Hash field names can be edited.** They rendered as static `<code>` for the same reason. Redis has no `HRENAME`, so the rename runs `HSET` plus `HDEL` inside the existing compare-and-swap script: one round trip, atomic, and still refused if the value changed since page load. It also refuses to overwrite a field name that is already in use.

### Fixes

- **Container entries that are not JSON-serializable are read-only.** They display as `repr()`, and nothing stopped that text from being submitted back, which stored the repr string over the real value. Their Update and Remove buttons are now disabled, matching the guard the string editor already had.
- **`xadd` parses its value like every other handler.** It was the one action that stored the submitted text raw, so a stream entry could not hold a number, list or dict the way a list item or hash field can.
- **The string editor strips surrounding whitespace.** Every container handler already did; the string path did not, so a stray newline changed the stored value.

## 0.4.1 (August 2026)

### Fixes

- **The admin's value textarea no longer overflows its container.** It was sized `width: 100%` without `box-sizing: border-box`, so the admin's 1px border and 8px side padding landed outside that width and clipped the right edge by 18px.
- **Admin warnings and field errors are readable in dark mode.** The key detail page's complex-value notice inlined light-theme colors and set no foreground, so dark mode drew `.help`'s near-white text on pale yellow. It now uses the admin's own `messagelist` warning styling, which follows the active theme. The add form's client-side field errors hardcoded the light-theme error red for the same reason and now take `--error-fg`.
- **Semaphore `release()` and `extend()` no longer act on a token they don't own.** Both re-read `self._token` after their "not held" guard, so a racing re-acquire on the same instance could install a new token in between; the command then ran against that live claim, and `release()` cleared the field to `None`. The token is now snapshotted under the same lock `_claim()` uses, and `release()` clears it only if it is still the one it entered with.
- **RESP semaphores no longer wedge when Redis evicts their bookkeeping.** The reaper only ever adjusted the `used` counter by a delta, so losing the claims hash to `maxmemory` eviction left `used` pinned at capacity with nothing left to subtract and every later acquire failing forever. Losing the state hash instead made `used` read zero and admitted past capacity. `used` is now derived from the surviving claims during the walk the reaper already performs, which costs no extra round trip and self-heals both directions.

## 0.4.0 (August 2026)

### Breaking changes

- **Python 3.14+ required.** Dropped support for 3.12 and 3.13. The package now ships on cp314 and cp314t (free-threaded) wheels.
- **Django 6.0+ required.** Dropped support for Django 5.2.
- **`LocMemCache` data structures use tagged subclasses.** Lists, sets, hashes, and sorted sets are stored as dedicated subclasses (`_List`, `_Set`, `_Hash`, `_ZSet`) rather than plain Python types, and cross-type access raises `WrongTypeError` instead of silently coercing, matching real Valkey/Redis ``WRONGTYPE`` semantics.
- **`LocMemCache` bypasses pickle for tagged collections.** Mutations happen in place; the prior copy-on-read/copy-on-write contract no longer holds. Code that relied on getting a detached snapshot from `cache.get()` for these types now sees the live structure.
- **`StreamCache` wire format changed.** Stream entries now flow through the transport's serializer + compressor pipeline instead of raw pickle. Pods running the new code cannot read entries written by older pods on the same stream; coordinate the rollout (drain or rotate `stream_key`).
- **`hmset` removed.** Use `hset(key, mapping=...)` or `hset(key, items=...)` (flat key-value list, matching redis-py/valkey-py).
- **`django_cachex.unfold` removed.** The django-unfold theme variant of the admin is gone, along with the `[unfold]` extra and `examples/unfold/`. Plain `django_cachex.admin` remains. Unfold support may return as a thin theme override once the core admin app stabilises.
- **Lock parameters renamed.** `cache.lock(timeout=...)` is now `cache.lock(lease=...)` (TTL of the held lock); `lock.acquire(blocking_timeout=...)` is now `lock.acquire(timeout=...)` (max wait). The old `blocking_timeout` kwarg raises `TypeError`; the constructor's new `timeout=` kwarg means "max wait" rather than "TTL". No deprecation shim. This aligns the lock API with the upcoming `cache.semaphore(...)` primitive.
- **`ZStdCompressor` renamed to `ZstdCompressor`** (`django_cachex.compressors.zstd.ZstdCompressor`). Update `OPTIONS["compressor"]` strings.
- **`LzmaCompressor` constructor `preset=` renamed to `level=`** for consistency with the other compressors. All compressors now accept `level=` (mapped to the underlying library's native parameter).
- **`PickleSerializer` no longer raises `ImproperlyConfigured` for `protocol > pickle.HIGHEST_PROTOCOL`**. Pickle's own `ValueError` is now surfaced at the first `dumps` call, wrapped as `SerializerError` (with the pickle exception as `__cause__`).
- **`CachexCompat` removed.** The mixin class that emulated the cachex ext surface on top of an arbitrary `BaseCache` is gone, along with the admin's "wrapped" support tier. Django's `BaseCache` and the stock backends (`LocMemCache`, `RedisCache`, `DatabaseCache`, `FileBasedCache`, `MemcachedCache`, `DummyCache`) deliberately don't expose key listing, so the wrap couldn't drive the admin's browse views meaningfully. Use `django_cachex.cache.LocMemCache` / `DatabaseCache` (drop-in replacements) for full admin support; non-cachex backends now show as "limited" (configuration only).
- **Cluster `LOCATION` with a database number now raises on the redis-py and valkey-py cluster backends.** Those two are built with the driver's `from_url()`, which rejects a non-zero `db` in the URL path or query (`RedisClusterException` / `ValkeyClusterException`). The old code read only host and port off the URL, so `redis://host:6379/1` connected to db 0 without complaint. Cluster has no `SELECT`, so the number was never honored; drop it from `LOCATION`. `ValkeyGlideClusterCache` and `RedisRsClusterCache` still ignore it silently.

### New features

- **Rust I/O driver (experimental).** Optional native driver built on PyO3 + tokio + redis-rs, shipped as a separate `django-cachex-redis-rs` package. Interfaces and behavior may change, and it has seen less production testing than the redis-py/valkey-py paths. Opt in via the `redis-rs` extra (`pip install django-cachex[redis-rs]`); without it, only the pure-Python backends are pulled in and the `RedisRsCache` classes raise a clean `ImportError` on first use. Set `BACKEND` to one of `RedisRsCache`, `RedisRsClusterCache`, or `RedisRsSentinelCache`. Sync and async share one tokio runtime; async dodges the threadpool round-trip.
- **`valkey-glide` adapter (experimental).** Optional Rust-cored client from the Valkey project. Interfaces and behavior may change, and it has seen less production testing than the redis-py/valkey-py paths. Opt in via the `valkey-glide` extra. Standalone (`ValkeyGlideCache`) and cluster (`ValkeyGlideClusterCache`) topologies are exposed; Sentinel is not (`valkey-glide` itself does not ship a Sentinel client).
- **`WrongTypeError` exception.** Backends now translate Redis ``WRONGTYPE`` responses into a single `django_cachex.WrongTypeError` (subclass of `TypeError`) so user code can catch one exception across LocMem, redis-py, valkey-py, valkey-glide, and the Rust adapter.
- **Async ext methods on LocMem and Database.** The full async data-structure surface (`alpush`, `ahset`, `azadd`, `attl`, `aexpire`, ...) is now available on `LocMemCache` (direct sync calls; in-memory, so no I/O to offload) and `DatabaseCache` (via ``sync_to_async``, the same path Django uses for ``BaseCache.aget``). They no longer raise `NotSupportedError` from async views.
- **`StreamCache` backend.** Stream-synchronized in-memory cache: reads are local, writes broadcast over a Redis Stream, a daemon thread on each pod consumes the stream and applies remote changes. Read-heavy, write-light, eventually consistent.
- **`TieredCache` backend.** Composes two existing `CACHES` entries as L1 (fast, e.g. LocMem) and L2 (durable, e.g. Redis), with TTL propagation and pull-through reads.
- **Cache-stampede prevention.** TTL-based XFetch via `OPTIONS["stampede_prevention"]` (or `stampede_prevention=` per call). Configurable buffer/beta/delta.
- **`LocMemCache` and `DatabaseCache` extensions.** Drop-in replacements for the Django builtins, adding data-structure ops, TTL helpers, and admin support. Compound read-modify-write ops on `LocMemCache` are serialized via a per-backend `RLock` (#62).
- **`orjson` and `ormsgpack` serializer extras.**
- **Free-threaded CPython (3.14t) support.** A cp314t wheel is built; `_redis_rs` works with the GIL disabled. The Rust driver also runs on the free-threaded build.
- **PyPI wheels via cibuildwheel.** Wheels for Linux x86_64, Linux aarch64, macOS arm64, and Windows amd64, on cp314 and cp314t.
- **Async pool sharing.** A single async connection pool is shared across per-task `Cache` instances (#83), avoiding the thundering-herd reconnect on cold start.
- **Pipeline parity.** Stream ops, CAS ops, missing key ops (`persist`/`pttl`/`expireat`/etc.), context manager, `zpopmin`/`zpopmax` default `count=1` aligned with the cache API.
- **Compressors gain a uniform `level=` parameter** (gzip, lz4, zstd join zlib/lzma in exposing it). Defaults match each library's own default.
- **Serializer/compressor wrappers consolidated.** Subclasses now implement `_dumps`/`_loads` (serializers) or `_compress`/`_decompress` (compressors); the base classes wrap the boilerplate (`SerializerError` / `CompressorError` translation, int-passthrough on loads).
- **Weighted semaphores.** New `cache.semaphore(name, capacity, *, weight=1, lease=..., timeout=...)` and `cache.asemaphore(...)` for gating concurrent access by a budget (counting or weighted). Backed by an in-process FIFO deque on `LocMemCache` and by Lua scripts on the RESP backends (redis-py, redis-rs, valkey-py, valkey-glide). Cluster mode is supported via `{name}` hash-tag colocation. Sync and async APIs share state per cache instance; lease-based crash reclaim on the RESP backend (no heartbeat). See `docs/recipes.md` for examples.

### Performance

- **`LocMemCache` sorted sets are O(log N).** Sorted-set operations now back the underlying dict with a `sortedcontainers.SortedList` sidecar for O(log N) insertion, deletion, and rank queries; previous implementation was O(N log N) per write. Adds `sortedcontainers>=2.4` as a runtime dependency.
- **`LocMemCache` skips pickle for tagged collections.** Tagged subclasses are mutated in place; reads and writes no longer round-trip through pickle for list/set/hash/zset/stream types.

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

- **`expiretime()` and `set(get=True)` support**: New cache methods for retrieving absolute expiry timestamps and atomic get-and-set operations.
- **Atomic CAS operations in admin**: Key detail edits use compare-and-swap via Lua-computed SHA1 fingerprints to prevent concurrent edit conflicts.
- **Key detail pagination**: Collection types (list, hash, set, zset, stream) are paginated at 100 items per page with `?page=N` navigation.
- **Keys in admin sidebar**: The key list is now a first-class sidebar entry with a cache filter for switching between configured caches.
- **Simplified Lua script execution**: `eval_script()` replaces the `register_script`/`LuaScript` registry with direct `EVAL` calls; redis-py handles script caching.
- **Async data structure methods**: All hash, list, set, and sorted set operations now have async counterparts on `RespCache` (e.g. `ahset`, `alpush`, `asadd`, `azadd`).
- **Stream operations**: Full sync and async support for Redis streams (`xadd`, `xread`, `xrange`, `xlen`, `xdel`, `xtrim`, `xinfo_stream`, `xgroup_create`, `xreadgroup`, `xack`, `xpending`, `xclaim`, `xautoclaim`, and more).
- **Safe `clear()`**: `clear()` now uses `delete_pattern("*")` to only remove keys for the current cache version and prefix, instead of `FLUSHDB`. Use `flush_db()` for the old behavior.
- **Danger zone in admin**: Cache detail view has a "Danger Zone" section with "Clear all versions" and "Flush database" actions. Key list view has a "Clear" button for safe prefix-scoped clearing.
- **`hset` items param**: `hset()` now accepts an `items` parameter (flat key-value list), matching the redis-py/valkey-py signature. `hmset` is removed.
- **`delete_pattern` batched deletes**: Deletes are now batched to prevent OOM on broad patterns.
- **Multi-key params standardized**: Set operations (`sdiff`, `sinter`, `sunion`, etc.) accept `KeyT | Sequence[KeyT]` consistently.

---

## 0.2.0 (February 2026)

- **Django permissions enforced**: The admin now uses Django's built-in permission system for granular access control. Staff users need explicit permissions; superusers are unaffected.

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

- **Hash operations**: `hset`, `hdel`, `hexists`, `hget`, `hgetall`, `hincrby`, `hincrbyfloat`, `hkeys`, `hlen`, `hmget`, `hmset`, `hsetnx`, `hvals`
- **Sorted set operations**: `zadd`, `zcard`, `zcount`, `zincrby`, `zrange`, `zrevrange`, `zrangebyscore`, `zrevrangebyscore`, `zrank`, `zrevrank`, `zrem`, `zremrangebyrank`, `zremrangebyscore`, `zscore`, `zmscore`, `zpopmin`, `zpopmax`
- **List operations**: `llen`, `lpush`, `rpush`, `lpop`, `rpop`, `lindex`, `lrange`, `lset`, `ltrim`, `lrem`, `lpos`, `linsert`, `lmove`, `blpop`, `brpop`, `blmove`
- **Set operations**: `sadd`, `srem`, `smembers`, `sismember`, `smismember`, `scard`, `spop`, `srandmember`, `smove`, `sdiff`, `sdiffstore`, `sinter`, `sinterstore`, `sunion`, `sunionstore`, `sscan`, `sscan_iter`

### Requirements

- Python 3.12+
- Django 5.2+
- valkey-py 6.1+ or redis-py 6+

---

## Pre-release History

### 0.1.0b6 (February 2026)

#### New Features

- **Key type filter**: Filter keys by type (string, list, set, hash, zset, stream) in the admin key list sidebar
- **LocMemCache data structure operations**: List, set, and hash operations now work with LocMemCache wrappers
- **LocMemCache type detection**: Automatically detects stored Python types (list, set, dict) and maps them to Redis equivalents
- **`KeyType` StrEnum**: Centralized enum for Redis key types, replacing scattered string literals

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

- **Expanded cache backend support**: The admin interface now supports Django's builtin cache backends through wrapper classes
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

- **Django Cache Admin**: Built-in admin interface for cache management
  - Browse all configured caches
  - Search keys with wildcard patterns
  - View and edit cache values (strings, hashes, lists, sets, sorted sets)
  - Inspect TTL and modify expiration
  - View server info and memory statistics
  - Flush individual caches
  - Bulk delete keys

- **Django Unfold Theme Support**: Alternative admin styling for django-unfold users
  - Use `django_cachex.unfold` instead of `django_cachex.admin`
  - Consistent styling with Unfold's modern admin theme

- **Example Projects**: Added example projects demonstrating various configurations
  - `examples/simple/` - Basic setup with ValkeyCache and LocMemCache
  - `examples/full/` - Multiple backends including Sentinel and Cluster
  - `examples/unfold/` - Django Unfold theme integration

### 0.1.0b3 (January 2026)

#### New Features

- **Lua Script Interface**: High-level API for registering and executing Lua scripts with automatic key prefixing and value encoding/decoding
  - `cache.register_script()` to register scripts with pre/post processing hooks
  - `cache.eval_script()` and `cache.aeval_script()` for sync/async execution
  - `pipe.eval_script()` for pipeline support
  - Pre-built helpers: `keys_only_pre`, `full_encode_pre`, `decode_single_post`, `decode_list_post`
  - `ScriptHelpers` class exposes `make_key`, `encode`, `decode` for custom hooks
  - Automatic SHA caching with NOSCRIPT fallback
