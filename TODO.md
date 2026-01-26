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

The two-class architecture is implemented and aligned with Django's `RedisCache` / `RedisCacheClient` structure.

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
│   └── pipeline/               # Pipeline for batched operations
├── serializers/
├── compressors/
└── types.py
```

---

## Planned Features

### Async Support

Django 4.0+ added async methods to `BaseCache` with the `a` prefix (e.g., `aget`, `aset`, `adelete`).
We provide true async using native async clients (`redis.asyncio` / `valkey.asyncio`).

**Implementation Status:**

✅ **Completed:**
- Async connection pool management with per-event-loop caching
- `WeakKeyDictionary` keyed by event loop for automatic cleanup when loops are GC'd
- All Django official async cache methods for standard clients:
  - `aadd`, `aget`, `aset`, `atouch`, `adelete`
  - `aget_many`, `aset_many`, `adelete_many`
  - `ahas_key`, `aincr`, `adecr`, `aclear`, `aclose`
- Async wrapper methods in `KeyValueCache` with key prefixing/versioning
- Async versions of extended methods:
  - TTL/expiry: `attl`, `apttl`, `aexpire`, `apexpire`, `aexpireat`, `apexpireat`, `apersist`
  - Keys: `akeys`, `aiter_keys`, `adelete_pattern`, `arename`, `arenamenx`
  - Hashes: `ahset`, `ahsetnx`, `ahget`, `ahmset`, `ahmget`, `ahgetall`, `ahdel`, `ahexists`, `ahlen`, `ahkeys`, `ahvals`, `ahincrby`, `ahincrbyfloat`
  - Lists: `alpush`, `arpush`, `alpop`, `arpop`, `allen`, `alpos`, `almove`, `alrange`, `alindex`, `alset`, `alrem`, `altrim`, `alinsert`, `ablpop`, `abrpop`, `ablmove`
  - Sets: `asadd`, `asrem`, `asmembers`, `asismember`, `ascard`, `aspop`, `asrandmember`, `asmove`, `asdiff`, `asdiffstore`, `asinter`, `asinterstore`, `asunion`, `asunionstore`, `asmismember`, `asscan`, `asscan_iter`
  - Sorted sets: `azadd`, `azrem`, `azscore`, `azrank`, `azrevrank`, `azcard`, `azcount`, `azincrby`, `azrange`, `azrevrange`, `azrangebyscore`, `azrevrangebyscore`, `azremrangebyrank`, `azremrangebyscore`, `azpopmin`, `azpopmax`, `azmscore`
- `get_or_set`, `aget_or_set`, `aincr_version`, `adecr_version`
- Async support for cluster clients (`RedisClusterCacheClient`, `ValkeyClusterCacheClient`)
  - Async cluster management with per-event-loop caching
  - Cluster-specific async methods: `aget_many`, `aset_many`, `adelete_many`, `aclear`, `akeys`, `aiter_keys`, `adelete_pattern`, `aclose`
- Async support for sentinel clients (`RedisSentinelCacheClient`)
  - Async sentinel pool management with per-event-loop caching
- Tests in `tests/test_cache_async.py`

⏳ **Pending:**
- (none currently - all async methods implemented)

**Architecture:**

```python
# In KeyValueCacheClient:
_pools: dict[int, SyncConnectionPool]           # Sync pools (keyed by server index)
_async_pools: WeakKeyDictionary[EventLoop, dict[int, AsyncPool]]  # Per-loop async pools
```

Async pools use `WeakKeyDictionary` with the event loop as key. This ensures pools are
automatically cleaned up when their event loop is garbage collected.

**Configuration:**

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            # Optional: custom async pool class
            "async_pool_class": "myapp.pools.CustomAsyncConnectionPool",
        }
    }
}
```

**Documentation Required:**

Add a dedicated "Async Support" section to the docs covering:

1. **Combined Sync/Async Architecture**
   - A single cache backend instance supports both sync and async operations simultaneously
   - This mirrors Django's approach where `cache.get()` and `await cache.aget()` work on the same backend
   - No separate "async backend" class needed - just use `RedisCache` or `ValkeyCache`

2. **How It Works**
   - Sync operations use sync connection pools (redis.ConnectionPool / valkey.ConnectionPool)
   - Async operations use async connection pools (redis.asyncio.ConnectionPool / valkey.asyncio.ConnectionPool)
   - Async pools are created per event loop and cached with weak references
   - Pools are automatically cleaned up when their event loop is garbage collected

3. **Important Performance Caveat** ⚠️
   - Async pool connections are cached **per event loop**
   - This is efficient for long-lived event loops (e.g., ASGI servers like uvicorn, daphne)
   - This is **NOT efficient** for short-lived or "small" event loops
   - Specifically, avoid using async methods when:
     - Running `sync_to_async` wrappers that create temporary event loops
     - Using `asyncio.run()` repeatedly in sync code (each call = new loop = new pool)
     - Any pattern where event loops are frequently created and destroyed
   - In these cases, each new event loop creates a fresh connection pool, losing connection reuse benefits
   - This is a fundamental constraint of redis-py and valkey-py's asyncio implementations

4. **Usage Examples**
   ```python
   # In async views (ASGI) - efficient, pools are reused
   async def my_view(request):
       value = await cache.aget("key")
       await cache.aset("key", "value", timeout=300)

   # In sync views (WSGI) - use sync methods
   def my_view(request):
       value = cache.get("key")
       cache.set("key", "value", timeout=300)

   # Mixed usage in same backend - both work
   cache.set("sync_key", "value")           # Uses sync pool
   await cache.aget("async_key")            # Uses async pool for current event loop
   ```

5. **Configuration**
   - Document `async_pool_class` option for custom async pools
   - Show example of providing custom pool class via import string

## Additional Data Structure Operations

Extended operations are implemented directly on the CacheClient class:
- Hashes: hset, hdel, hlen, hkeys, hexists, hget, hgetall, hmget, hincrby, hincrbyfloat, hsetnx, hvals
- Lists: lpush, rpush, lpop, rpop, lrange, lindex, llen, lrem, ltrim, lset, linsert, lpos, lmove, blpop, brpop, blmove
- Sets: sadd, scard, sdiff, sdiffstore, sinter, sinterstore, sismember, smembers, smove, spop, srandmember, srem, sscan, sscan_iter, sunion, sunionstore
- Sorted Sets: zadd, zcard, zcount, zincrby, zpopmax, zpopmin, zrange, zrangebyscore, zrank, zrem, zremrangebyscore, zrevrange, zrevrangebyscore, zscore, zrevrank, zmscore, zremrangebyrank
- Streams (sync + async):
  - Basic: xadd, xlen, xrange, xrevrange, xread, xtrim, xdel
  - Info: xinfo_stream, xinfo_groups, xinfo_consumers
  - Consumer groups: xgroup_create, xgroup_destroy, xgroup_setid, xgroup_delconsumer, xreadgroup, xack, xpending, xclaim, xautoclaim
- Lua scripting (sync + async): eval, evalsha, script_load, script_exists, script_flush, script_kill

Pipeline support exists for all these operations in `client/pipeline/`.

---

## Compression & Serialization

Current features:
- Multiple compressor fallback for safe migration between formats
- Multiple serializer fallback for safe migration between formats
- Conditional compression based on value size (`min_length = 256`)
- No compression when `compressor` option is not set or `None`

---

## Testing

- [x] Integration tests with real multi-node cluster (master-replica setup using bitnami/redis)

---

## Code Quality

- [ ] Review `[[tool.mypy.overrides]]` for optional dependencies as libraries improve
- [ ] Add docstrings incrementally (start with public API)
- [ ] Enable more ruff rules (review ignored rules and fix violations)
