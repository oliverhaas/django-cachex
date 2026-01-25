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
The default implementations just wrap sync methods or re-implement using `aget`/`aset`, losing native
Redis/Valkey async support. We want true async using native async clients.

**Architecture Overview:**

The async implementation requires separate connection pools because:
- Sync clients (redis.Redis / valkey.Valkey) use blocking sockets
- Async clients (redis.asyncio.Redis / valkey.asyncio.Valkey) use asyncio sockets
- Each event loop needs its own async connection pool (asyncio objects can't be shared across loops)

**Connection Pool Strategy:**

```python
# In KeyValueCacheClient:
_pools: dict[int, SyncConnectionPool]           # Sync pools (keyed by server index)
_async_pools: WeakValueDictionary[int, dict]    # Async pools (keyed by event loop id -> {index: pool})
```

The async pools use a `WeakValueDictionary` keyed by `id(asyncio.get_running_loop())`.
This ensures pools are automatically cleaned up when their event loop is garbage collected.

**Configuration Options:**

Users can configure async pool class via Django settings:
```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            # Optional: custom async pool class (defaults to library's async pool)
            "async_pool_class": "myapp.pools.CustomAsyncConnectionPool",
        }
    }
}
```

**Implementation Plan:**

1. **Add async client class attributes and configuration to KeyValueCacheClient**
   ```python
   # Class attributes - subclasses override these
   _async_client_class: type = None  # e.g., redis.asyncio.Redis
   _async_pool_class: type = None    # e.g., redis.asyncio.ConnectionPool

   # Add to _CLIENT_ONLY_OPTIONS
   _CLIENT_ONLY_OPTIONS = frozenset({
       ...,
       "async_pool_class",  # Don't pass to sync pool
   })

   def __init__(self, servers, ..., async_pool_class=None, **options):
       ...
       # Set up async pool class (can be overridden via argument)
       if isinstance(async_pool_class, str):
           async_pool_class = import_string(async_pool_class)
       self._async_pool_class = async_pool_class or self.__class__._async_pool_class
   ```

2. **Add async pool management methods**
   ```python
   def _get_async_connection_pool(self, write: bool) -> AsyncConnectionPool:
       """Get async pool for current event loop, creating if needed."""
       loop_id = id(asyncio.get_running_loop())
       # Lazily create pool dict for this loop
       # Store in WeakValueDictionary for automatic cleanup

   async def get_async_client(self, key=None, *, write=False) -> AsyncClient:
       """Get an async client connection."""
   ```

3. **Implement Django's official async cache methods in KeyValueCacheClient**
   - `aadd(key, value, timeout)` - async add
   - `aget(key, default=None)` - async get
   - `aset(key, value, timeout)` - async set
   - `atouch(key, timeout)` - async touch
   - `adelete(key)` - async delete
   - `aget_many(keys)` - async get_many
   - `ahas_key(key)` - async has_key
   - `aincr(key, delta=1)` - async incr
   - `adecr(key, delta=1)` - async decr (calls aincr with -delta)
   - `aset_many(data, timeout)` - async set_many
   - `adelete_many(keys)` - async delete_many
   - `aclear()` - async clear
   - `aclose()` - async close (cleanup async pools for current loop)

4. **Implement async wrappers in KeyValueCache (backend class)**
   - Same methods as above, but with key prefixing/versioning
   - `aget_or_set(key, default, timeout, version)` - async get_or_set
   - `aincr_version(key, delta, version)` - async incr_version

5. **Add async versions of all extended methods**
   - TTL: `attl`, `apttl`
   - Expiry: `aexpire`, `apexpire`, `aexpireat`, `apexpireat`, `apersist`
   - Keys: `akeys`, `aiter_keys`, `adelete_pattern`
   - Hashes: `ahset`, `ahget`, `ahgetall`, `ahmget`, `ahdel`, `ahexists`, `ahlen`, `ahkeys`, `ahvals`, `ahincrby`, `ahincrbyfloat`, `ahsetnx`
   - Lists: `alpush`, `arpush`, `alpop`, `arpop`, `alrange`, `alindex`, `allen`, `alpos`, `almove`, `alrem`, `altrim`, `alset`, `alinsert`, `ablpop`, `abrpop`, `ablmove`
   - Sets: `asadd`, `asrem`, `asmembers`, `asismember`, `ascard`, `aspop`, `asrandmember`, `asmove`, `asdiff`, `asdiffstore`, `asinter`, `asinterstore`, `asunion`, `asunionstore`, `asmismember`, `asscan`, `asscan_iter`
   - Sorted Sets: `azadd`, `azrem`, `azscore`, `azrank`, `azrevrank`, `azcard`, `azcount`, `azincrby`, `azrange`, `azrevrange`, `azrangebyscore`, `azrevrangebyscore`, `azremrangebyrank`, `azremrangebyscore`, `azpopmin`, `azpopmax`, `azmscore`

6. **Update close() to also cleanup async pools**
   ```python
   def close(self, **kwargs):
       # ... existing sync pool cleanup ...
       # Also cleanup async pools for all tracked event loops
       self._async_pools.clear()

   async def aclose(self, **kwargs):
       """Async close - disconnect async pools for current event loop."""
       loop_id = id(asyncio.get_running_loop())
       if loop_id in self._async_pools:
           for pool in self._async_pools[loop_id].values():
               await pool.disconnect()
           del self._async_pools[loop_id]
   ```

7. **Handle finalization / cleanup**
   - Use `weakref.finalize` or `__del__` to ensure async pools are cleaned up
   - Note: Can't await in finalizers, so just call `pool.disconnect()` synchronously
     or rely on WeakValueDictionary garbage collection

**Concrete Client Classes:**

```python
# In RedisCacheClient:
if _REDIS_AVAILABLE:
    from redis.asyncio import Redis as AsyncRedis
    from redis.asyncio import ConnectionPool as AsyncConnectionPool

    class RedisCacheClient(KeyValueCacheClient):
        _lib = redis
        _client_class = redis.Redis
        _pool_class = redis.ConnectionPool
        _async_client_class = AsyncRedis
        _async_pool_class = AsyncConnectionPool

# Similarly for ValkeyCacheClient with valkey.asyncio
```

**Testing Considerations:**

- Test async methods work correctly with pytest-asyncio
- Test that each event loop gets its own pool
- Test that pools are cleaned up when event loops are garbage collected
- Test concurrent async operations from multiple coroutines
- Test mixing sync and async operations

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

