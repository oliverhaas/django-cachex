# django-cachex TODO

Features and changes we want to make.

---

## Compatibility Policy

**THE ONLY COMPATIBILITY WE CARE ABOUT:**
- **Django's official cache backend** (`django.core.cache.backends.redis`)
- We want to be a **drop-in replacement** for it
- Then provide **extended features on top**

This is a fresh/alpha package. Break whatever needs to break. We've not released yet.

---

## Target Versions

- Python 3.12, 3.13, 3.14
- Django 4.2.8+, 5.2+
- Valkey server (valkey-py 6.0+) or Redis server 6.x/7.x (redis-py 5.0+)

Both backends are opt-in:
- `pip install django-cachex[valkey]`
- `pip install django-cachex[redis]`

---

## Architecture (Django-aligned Two-Class Design)

**Goal**: Mirror Django's official `django.core.cache.backends.redis` architecture exactly,
then layer our extended features on top.

### Current State

The two-class architecture is mostly implemented but needs cleanup to ensure alignment with Django's structure.

### Design Principles

1. **Copy Django's two-class design** - `Cache` (backend) + `CacheClient` (operations)
2. **Use generics** - Type-safe implementations for Valkey/Redis/RedisCluster/Sentinel/ValkeyCluster/...
3. **Pool handling identical to Django** - Pools cached by index, `pool_class.from_url()`
4. **Stay close to Django** - Users familiar with Django's Redis backend should feel at home, including internals as much as possible

### Class Hierarchy

**Cache Backend Classes** (extend Django's `BaseCache`):
```
KeyValueCache[ClientT]              # Generic base, mirrors Django's RedisCache
├── ValkeyCache                     # _class = ValkeyCacheClient
├── RedisCache                      # _class = RedisCacheClient
├── ValkeyClusterCache              # _class = ValkeyClusterCacheClient
├── RedisClusterCache               # _class = RedisClusterCacheClient
├── ValkeySentinelCache             # _class = ValkeySentinelCacheClient
└── RedisSentinelCache              # _class = RedisSentinelCacheClient
```

**Cache Client Classes** (actual Valkey/Redis operations):
```
KeyValueCacheClient[ClientT]        # Generic base, mirrors Django's RedisCacheClient
├── ValkeyCacheClient               # Uses valkey-py
├── RedisCacheClient                # Uses redis-py
├── ValkeyClusterCacheClient        # Uses valkey-py cluster
├── RedisClusterCacheClient         # Uses redis-py cluster
├── ValkeySentinelCacheClient       # Uses valkey-py sentinel
└── RedisSentinelCacheClient        # Uses redis-py sentinel
```

### Current File Structure

```
django_cachex/
├── cache.py                    # Re-exports for convenience
├── client/
│   ├── __init__.py             # Public exports
│   ├── cache.py                # KeyValueCache + concrete Cache classes
│   ├── default.py              # KeyValueCacheClient + concrete Client classes
│   ├── cluster.py              # Cluster-specific Cache + Client classes
│   ├── sentinel.py             # Sentinel-specific Cache + Client classes
│   └── pipeline.py             # Pipeline for batched operations
├── serializers/
├── compressors/
├── pool.py
└── types.py
```

### Remaining Architecture Tasks

- [ ] Review and align with Django's `RedisCache` / `RedisCacheClient` structure
- [ ] Ensure pool handling matches Django exactly (pools cached by server index)
- [ ] Clean up any remaining cruft from old architecture
- [ ] Consider removing `pool.py` if pool logic can be inline in CacheClient (like Django)

---

## Planned Features

- [ ] Add async Django cache interface (official Django async cache API)
- [ ] Add async API for all extended methods

## Additional Data Structure Operations

Extended operations are implemented directly on the CacheClient class:
- Hashes: hset, hdel, hlen, hkeys, hexists, hget, hgetall, hmget, hincrby, hincrbyfloat, hsetnx, hvals
- Lists: lpush, rpush, lpop, rpop, lrange, lindex, llen, lrem, ltrim, lset, linsert, lpos, lmove, blpop, brpop, blmove
- Sets: sadd, scard, sdiff, sdiffstore, sinter, sinterstore, sismember, smembers, smove, spop, srandmember, srem, sscan, sscan_iter, sunion, sunionstore
- Sorted Sets: zadd, zcard, zcount, zincrby, zpopmax, zpopmin, zrange, zrangebyscore, zrank, zrem, zremrangebyscore, zrevrange, zrevrangebyscore, zscore, zrevrank, zmscore, zremrangebyrank

Pipeline support exists for all these operations in `client/pipeline/`.

### Future Data Structure Features

- [ ] Investigate Streams support (xadd, xread, xrange, consumer groups)
- [ ] Investigate Lua scripting support (eval, evalsha, script_load)

---

## Compression & Serialization

Current features:
- Multiple compressor fallback for safe migration between formats
- Multiple serializer fallback for safe migration between formats
- Conditional compression based on value size (`min_length = 256`)
- No compression when `compressor` option is not set or `None`

---

## Testing

- [ ] Integration tests with real multi-node cluster

---

## Code Quality

- [ ] Review `[[tool.mypy.overrides]]` for optional dependencies as libraries improve
- [ ] Add docstrings incrementally (start with public API)
- [ ] Enable more ruff rules (review ignored rules and fix violations)

