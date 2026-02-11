# Changelog

## 0.2.0 (February 2026)

### Breaking Changes

- **Django permissions enforced**: The admin now uses Django's built-in permission system. Staff users without explicit permissions will get 403 errors. Superusers are unaffected. Grant `django_cachex.view_cache`, `django_cachex.change_cache`, `django_cachex.view_key`, `django_cachex.add_key`, `django_cachex.change_key`, and `django_cachex.delete_key` as needed.

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

- **Hash operations**: `hset`, `hdel`, `hexists`, `hget`, `hgetall`, `hincrby`, `hincrbyfloat`, `hlen`, `hmget`, `hmset`, `hsetnx`, `hvals`
- **Sorted set operations**: `zadd`, `zcard`, `zcount`, `zincrby`, `zrange`, `zrangebyscore`, `zrank`, `zrevrank`, `zrem`, `zremrangebyrank`, `zscore`, `zmscore`
- **List operations**: `llen`, `lpush`, `rpush`, `lpop`, `rpop`, `lindex`, `lrange`, `lset`, `ltrim`, `lpos`, `lmove`
- **Set operations**: `sadd`, `srem`, `smembers`, `sismember`, `smismember`, `scard`, `spop`, `srandmember`, `sdiff`, `sinter`, `sunion`

### Requirements

- Python 3.12+
- Django 5.2+
- valkey-py 6+ or redis-py 6+

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
