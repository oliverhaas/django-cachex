# Changelog

## 0.1.0b3 (January 2026)

### New Features

- **Lua Script Interface**: High-level API for registering and executing Lua scripts with automatic key prefixing and value encoding/decoding
  - `cache.register_script()` to register scripts with pre/post processing hooks
  - `cache.eval_script()` and `cache.aeval_script()` for sync/async execution
  - `pipe.eval_script()` for pipeline support
  - Pre-built helpers: `keys_only_pre`, `full_encode_pre`, `decode_single_post`, `decode_list_post`
  - `ScriptHelpers` class exposes `make_key`, `encode`, `decode` for custom hooks
  - Automatic SHA caching with NOSCRIPT fallback

### New Classes

- `LuaScript` - Dataclass for registered scripts with metadata
- `ScriptHelpers` - Helper functions passed to pre/post processing hooks
- `ScriptNotRegisteredError` - Exception for unregistered script names

## 0.1.0 (January 2026)

Initial release of django-cachex.

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

### Data Structure Operations

- **Hash operations**: `hset`, `hdel`, `hexists`, `hget`, `hgetall`, `hincrby`, `hincrbyfloat`, `hlen`, `hmget`, `hmset`, `hsetnx`, `hvals`
- **Sorted set operations**: `zadd`, `zcard`, `zcount`, `zincrby`, `zrange`, `zrangebyscore`, `zrank`, `zrevrank`, `zrem`, `zremrangebyrank`, `zscore`, `zmscore`
- **List operations**: `llen`, `lpush`, `rpush`, `lpop`, `rpop`, `lindex`, `lrange`, `lset`, `ltrim`, `lpos`, `lmove`

### Requirements

- Python 3.12+
- Django 5.2+
- valkey-py 6+ or redis-py 6+
