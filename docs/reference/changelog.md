# Changelog

## Unreleased

### Breaking changes

- **Python 3.14+ required.** Dropped support for 3.12 and 3.13. The package now ships on cp314 and cp314t (free-threaded) wheels.
- **Django 6.0+ required.** Dropped support for Django 5.2.
- **`SyncCache` wire format changed.** Stream entries now flow through the transport's serializer + compressor pipeline instead of raw pickle. Pods running the new code cannot read entries written by older pods on the same stream — coordinate the rollout (drain or rotate `STREAM_KEY`).
- **`hmset` removed.** Use `hset(key, mapping=...)` or `hset(key, items=...)` (flat key-value list, matching redis-py/valkey-py).
- **`django_cachex.unfold` removed.** The django-unfold theme variant of the admin is gone, along with the `[unfold]` extra and `examples/unfold/`. Plain `django_cachex.admin` remains. Unfold support may return as a thin theme override once the core admin app stabilises.

### New features

- **Rust I/O driver.** Optional native driver built on PyO3 + tokio + redis-rs, shipped as a separate `django-cachex-rust` package. Opt in via the `redis-rs` extra (`pip install django-cachex[redis-rs]`); without it, only the pure-Python backends are pulled in and `RustValkeyCache` / `RustRedisCache` raise a clean `ImportError` on first use. Set `BACKEND` to one of `RustRedisCache`, `RustValkeyCache`, `RustRedisClusterCache`, `RustValkeyClusterCache`, `RustRedisSentinelCache`, `RustValkeySentinelCache`. Sync and async share one tokio runtime; async dodges the threadpool round-trip.
- **`SyncCache` backend.** Stream-synchronized in-memory cache: reads are local, writes broadcast over a Redis Stream, a daemon thread on each pod consumes the stream and applies remote changes. Read-heavy, write-light, eventually consistent.
- **`TieredCache` backend.** Composes two existing `CACHES` entries as L1 (fast, e.g. LocMem) and L2 (durable, e.g. Redis), with TTL propagation and pull-through reads.
- **Cache-stampede prevention.** TTL-based XFetch via `OPTIONS["stampede_prevention"]` (or `stampede_prevention=` per call). Configurable buffer/beta/delta.
- **`LocMemCache` and `DatabaseCache` extensions.** Drop-in replacements for the Django builtins, adding data-structure ops, TTL helpers, and admin support. Compound read-modify-write ops on `LocMemCache` are serialized via a per-backend `RLock` (#62).
- **`orjson` and `ormsgpack` serializer extras.**
- **Free-threaded CPython (3.14t) support.** A cp314t wheel is built; `_driver` works with the GIL disabled. The Rust driver also runs on the free-threaded build.
- **PyPI wheels via cibuildwheel.** Manylinux x86_64 wheels for cp314 and cp314t.
- **Async pool sharing.** A single async connection pool is shared across per-task `Cache` instances (#83), avoiding the thundering-herd reconnect on cold start.
- **Pipeline parity.** Stream ops, CAS ops, missing key ops (`persist`/`pttl`/`expire_at`/etc.), context manager, `zpopmin`/`zpopmax` default `count=1` aligned with the cache API.

### Fixes

- `LocMemCache.lpush`/`sadd`/`hset`/`hincrby`/`zadd`/etc. no longer lose updates under concurrent threads (#62).
- `delete_pattern` batches deletes to bound peak memory on broad patterns.
- `clear()` is now prefix/version-scoped instead of `FLUSHDB`. The old behavior is available as `flush_db()`.
- Compressor `decompress` methods catch all exceptions and re-raise as `CompressorError`.
- Several cluster correctness fixes (script loading on replicas, set_many `timeout=0`).

---

## 0.3.0 (February 2026)

- **`expiretime()` and `set(get=True)` support**: New cache methods for retrieving absolute expiry timestamps and atomic get-and-set operations.
- **Atomic CAS operations in admin**: Key detail edits use compare-and-swap via Lua-computed SHA1 fingerprints to prevent concurrent edit conflicts.
- **Key detail pagination**: Collection types (list, hash, set, zset, stream) are paginated at 100 items per page with `?page=N` navigation.
- **Keys in admin sidebar**: The key list is now a first-class sidebar entry with a cache filter for switching between configured caches.
- **Simplified Lua script execution**: `eval_script()` replaces the `register_script`/`LuaScript` registry with direct `EVAL` calls; redis-py handles script caching.
- **Async data structure methods**: All hash, list, set, and sorted set operations now have async counterparts on `KeyValueCache` (e.g. `ahset`, `alpush`, `asadd`, `azadd`).
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

- **Unified Valkey + Redis support** in one package
- **Full-featured cache backend** for Django
- **Session backend support** via Django's cache sessions
- **Pluggable clients** (Default, Sentinel, Cluster)
- **Pluggable serializers** (Pickle, JSON, MsgPack)
- **Pluggable compressors** (Zlib, Gzip, LZMA, LZ4, Zstandard)
- **Multi-serializer/compressor fallback** for safe migrations
- **Connection pooling** with configurable options
- **Primary/replica replication** support
- **Valkey/Redis Sentinel support** for high availability
- **Valkey/Redis Cluster support** with automatic slot handling
- **Distributed locks** compatible with `threading.Lock`
- **TTL operations** (`ttl()`, `pttl()`, `expire()`, `persist()`)
- **Pattern operations** (`keys()`, `iter_keys()`, `delete_pattern()`)
- **Pipelines** for batched operations
- **Lua script interface** with automatic key prefixing and value encoding/decoding
- **Django Cache Admin** for cache inspection and management
  - Browse, search, edit, and delete cache keys
  - View server info, memory statistics, and slowlog
  - Key type filter sidebar
  - Support for Django builtin backends (LocMemCache, DatabaseCache, FileBasedCache) via wrappers
  - Django Unfold theme support (`django_cachex.unfold`)
- **Async support** for all extended methods

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
