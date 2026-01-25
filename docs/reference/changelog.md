# Changelog

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
