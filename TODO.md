# django-cachex-ng TODO

Features and changes we want to make.

---

## ⚠️ CRITICAL COMPATIBILITY POLICY ⚠️

**WE DO NOT CARE ABOUT BACKWARDS COMPATIBILITY WITH:**
- Our old implementation
- django-cachex (jazzband)
- django-valkey

**THE ONLY COMPATIBILITY WE CARE ABOUT:**
- **Django's official redis cache backend** (`django.core.cache.backends.redis`)
- We want to be a **drop-in replacement** for it
- Then provide **extended features on top**

This is a fresh package. Break whatever needs to break.

---

## Architecture Refactor (Align with Django Official)

**Goal**: Mirror Django's official `django.core.cache.backends.redis` architecture exactly,
then layer our extended features on top. No backwards compatibility needed - this is a fresh package.

### Design Principles

1. **Copy Django's two-class design** - `Cache` (backend) + `CacheClient` (operations)
2. **Use generics extensively** - Type-safe implementations for Redis/Valkey/Cluster/Sentinel
3. **Pool handling identical to Django** - Pools cached by index, `pool_class.from_url()`
4. **Stay close to Django** - Users familiar with Django's Redis backend should feel at home

### Class Hierarchy

**Cache Backend Classes** (extend Django's `BaseCache`):
```
KeyValueCache[ClientT]              # Generic base, mirrors Django's RedisCache
├── RedisCache                      # _class = RedisCacheClient
├── ValkeyCache                     # _class = ValkeyCacheClient
├── RedisClusterCache               # _class = RedisClusterCacheClient
├── ValkeyClusterCache              # _class = ValkeyClusterCacheClient
├── RedisSentinelCache              # _class = RedisSentinelCacheClient
└── ValkeySentinelCache             # _class = ValkeySentinelCacheClient
```

Responsibilities:
- Extend `BaseCache`, handle Django cache interface
- Key versioning via `make_and_validate_key()`
- Timeout conversion via `get_backend_timeout()`
- Lazily instantiate `CacheClient` via `@cached_property`

**Cache Client Classes** (actual Redis/Valkey operations):
```
KeyValueCacheClient[ClientT]        # Generic base, mirrors Django's RedisCacheClient
├── RedisCacheClient                # Uses redis-py Redis
├── ValkeyCacheClient               # Uses valkey-py Valkey
├── RedisClusterCacheClient         # Uses redis-py RedisCluster
├── ValkeyClusterCacheClient        # Uses valkey-py ValkeyCluster
├── RedisSentinelCacheClient        # Uses redis-py Sentinel
└── ValkeySentinelCacheClient       # Uses valkey-py Sentinel
```

Responsibilities:
- Manage connection pools (cached by server index, like Django)
- Handle serialization/compression
- Read/write routing (primary vs replicas)
- All the actual Redis commands

### Pool Handling (match Django exactly)

```python
# Pools cached by server INDEX (not URL string)
self._pools: dict[int, ConnectionPool] = {}

def _get_connection_pool(self, write: bool) -> ConnectionPool:
    index = self._get_connection_pool_index(write)
    if index not in self._pools:
        self._pools[index] = self._pool_class.from_url(
            self._servers[index],
            **self._pool_options,
        )
    return self._pools[index]
```

### File Structure After Refactor

```
django_cachex/
├── cache.py                    # Re-exports for convenience
├── client/
│   ├── __init__.py             # Public exports
│   ├── cache.py                # KeyValueCache + concrete Cache classes
│   ├── client.py               # KeyValueCacheClient + concrete Client classes
│   ├── cluster.py              # Cluster-specific Cache + Client classes
│   ├── sentinel.py             # Sentinel-specific Cache + Client classes
│   └── mixins/                 # Extended operations (lists, sets, hashes, etc.)
├── serializers/                # (unchanged)
├── compressors/                # (unchanged)
└── types.py                    # (unchanged)
```

### Implementation Steps

- [ ] Step 1: Create `KeyValueCache` base class mirroring Django's `RedisCache`
  - Copy Django's RedisCache structure exactly
  - Add our extensions (compressor, extended methods)
  - Generic over ClientT

- [ ] Step 2: Create `KeyValueCacheClient` base class mirroring Django's `RedisCacheClient`
  - Copy Django's RedisCacheClient structure exactly
  - Pool handling by index (not URL)
  - Add our serializer/compressor support
  - Generic over the raw client type

- [ ] Step 3: Create concrete Redis implementations
  - `RedisCache` and `RedisCacheClient`
  - Test against redis-py

- [ ] Step 4: Create concrete Valkey implementations
  - `ValkeyCache` and `ValkeyCacheClient`
  - Test against valkey-py

- [ ] Step 5: Create Cluster implementations
  - `RedisClusterCache` + `RedisClusterCacheClient`
  - `ValkeyClusterCache` + `ValkeyClusterCacheClient`
  - Handle slot-aware operations

- [ ] Step 6: Create Sentinel implementations
  - `RedisSentinelCache` + `RedisSentinelCacheClient`
  - `ValkeySentinelCache` + `ValkeySentinelCacheClient` (if valkey-py bug fixed)

- [ ] Step 7: Migrate mixins to work with new architecture
  - HashMixin, ListMixin, SetMixin, SortedSetMixin
  - Extended methods (keys, iter_keys, delete_pattern, lock, etc.)

- [ ] Step 8: Remove old code
  - Delete `pool.py` (pool logic now in CacheClient)
  - Clean up old client/default.py

- [ ] Step 9: Update all tests

- [ ] Step 10: Update documentation

### Key Differences from Current Architecture

| Current | New (Django-aligned) |
|---------|---------------------|
| `KeyValueCacheClient` extends `BaseCache` | `KeyValueCache` extends `BaseCache`, delegates to `KeyValueCacheClient` |
| Pools cached by URL string | Pools cached by server index |
| Single class does everything | Two-class separation (Cache + CacheClient) |
| `ConnectionFactory` classes | Pool logic inline in CacheClient (like Django) |

---

## Compatibility

- Redis server 6.x and 7.x (redis-py 5.x)
- Python 3.12, 3.13, 3.14
- Django 5.2, 6.0

## Planned Features

- [x] Support Valkey client (valkey-py)
  - Branches: `research/valkey-py`, `research/valkey-glide`
  - Added valkey-py as optional dependency `[valkey]` and `[libvalkey]`
  - Test fixtures couple server images with client libraries (redis→redis-py, valkey→valkey-py)
  - Added native parser support (hiredis for redis-py, libvalkey for valkey-py)
- [ ] Add async Django cache interface (official Django async cache API)
  - Branch: `feat/async-support`
- [ ] Add async API for all other methods
- [ ] Full compatibility with Django's builtin Redis backend (django.core.cache.backends.redis)
  - See "Architecture Refactor" section - this is the main goal
  - Drop-in replacement with extended functionality
  - Match configuration options where applicable
- [x] Add more Redis method support via mixins
  - ListMixin: lpush, rpush, lpop, rpop, lrange, lindex, llen, lrem, ltrim, lset, linsert, lpos, lmove
  - SetMixin: sadd, scard, sdiff, sdiffstore, sinter, sinterstore, sismember, smembers, smove, spop, srandmember, srem, sscan, sscan_iter, sunion, sunionstore
  - HashMixin: hset, hdel, hlen, hkeys, hexists, hget, hgetall, hmget, hmset, hincrby, hincrbyfloat, hsetnx, hvals
  - SortedSetMixin: zadd, zcard, zcount, zincrby, zpopmax, zpopmin, zrange, zrangebyscore, zrank, zrem, zremrangebyscore, zrevrange, zrevrangebyscore, zscore, zrevrank, zmscore, zremrangebyrank
  - Fixed hash method parameter semantics (key/field vs name/key)
- [x] Use Python 3.14 builtin zstd when available (with backport fallback)
  - Uses `compression.zstd` on Python 3.14+, `backports.zstd` on older versions

## Compression & Serialization

- [x] Multiple compressor fallback for backwards compatibility on read
  - List-based COMPRESSOR config: `["path.to.ZstdCompressor", "path.to.GzipCompressor"]`
  - First compressor used for writing, all tried for reading
  - Exception-based fallback: tries each compressor until one succeeds
- [x] Multiple serializer fallback for backwards compatibility on read
  - List-based SERIALIZER config: `["path.to.JSONSerializer", "path.to.PickleSerializer"]`
  - First serializer used for writing, all tried for reading
  - Exception-based fallback: tries each serializer until one succeeds
- [x] Conditional compression based on value size
  - `BaseCompressor.min_length = 256` - values below this size are not compressed
  - Subclasses can override if needed
- [ ] Consider removing IdentityCompressor
  - `_decompress()` already returns raw value when all compressors fail
  - May still be useful as explicit "no compression" config option
- [x] Investigate Django's builtin serializer base class
  - Finding: Django does NOT have a serializer base class
  - `django.core.cache.backends.redis.RedisSerializer` is a concrete pickle implementation, not an abstract base
  - Django's interface is just duck-typed: any object with `dumps`/`loads` methods works
  - We already support Django's serializer via `create_serializer()` fallback to no-arg init
  - Conclusion: Keep our `BaseSerializer` for our serializers; users can use Django's `RedisSerializer` directly if preferred
- [x] Benchmarks for compression/serialization overhead
  - Compression exception handling: ~0.5µs (vs ~0.06µs for magic byte check)
  - Serialization: pickle ~0.28µs, JSON ~1.03µs, exception overhead ~60% for JSON
  - Conclusion: exception-based fallback is fast enough, simpler code wins

## Client Architecture

**Note**: See "Architecture Refactor" section above for the major restructuring plan.

- [x] Make redis-py truly optional (valkey-only installations)
  - Redis is no longer open source (switched to dual SSPL/RSALv2 license)
  - Users can install `django-cachex-ng[valkey]` without redis-py
  - All files now use conditional imports for redis-py/valkey-py
  - Exception tuples built dynamically based on available libraries
- [x] Define our own type aliases instead of importing from redis-py/valkey-py
  - Defined in `django_cachex/types.py`: `KeyT`, `EncodableT`, `ExpiryT`, `AbsExpiryT`, `PatternT`
  - 100% compatible with redis-py and valkey-py definitions
- [x] Add ClusterClient for Redis Cluster support (will be refactored)
- [x] Remove ShardClient (obsolete client-side sharding from pre-Cluster era)
- [x] Remove HerdClient (thundering herd protection)

## Naming / API

- [ ] Rename package to `django-xcache`
  - "Extended cache" - implies improvement on Django's builtin cache
  - Avoids sounding like a django-cachex fork
  - General enough for key-value caches (Redis, Valkey, etc.)
  - Module name: `django_xcache`
  - Do this after architecture refactor is complete
  - Repository setup:
    - Un-fork from jazzband/django-cachex (create fresh repo)
    - Set up new PyPI project and publishing workflow
    - Update all imports and references
    - Start versioning fresh (0.1.0 for development, 1.0.0 when stable)
- [x] Remove Redis-specific option names
  - Removed `redis_client_class` option entirely (not needed with new architecture)
  - Users should use the appropriate backend class instead (RedisCacheClient, ValkeyCacheClient, etc.)
- [ ] API stability policy
  - Document what is public API vs internal
  - Semantic versioning guarantees
  - Deprecation policy (warnings for N versions before removal)

## Additional Redis Features

- [ ] Investigate Pub/Sub support
  - `publish()`, `subscribe()`, channel patterns
  - May need async support first
- [ ] Investigate Redis Streams support
  - `xadd()`, `xread()`, `xrange()`, `xlen()`, `xdel()`, `xtrim()`
  - Consumer groups: `xgroup_create()`, `xreadgroup()`, `xack()`
- [ ] Investigate Lua scripting support
  - `eval()`, `evalsha()`, `script_load()`
  - Useful for atomic operations
- [x] Pipeline/transaction support
  - `pipeline()` context manager for batching commands
  - `MULTI`/`EXEC` transaction support via `transaction=True` (default)
  - Full support for lists, sets, hashes, sorted sets operations
  - Proper key prefixing and value encoding/decoding
  - Deferred decoder pattern for batch result processing
  - Note: Redis Cluster requires hash tags for multi-key transactions
- [x] Blocking list operations
  - `blpop()`, `brpop()`, `blmove()` with timeout handling
- [x] Add `blocking` parameter to `cache.lock()` (like django-cachex has)
  - Made signature explicit: sleep, blocking, blocking_timeout, thread_local

## Testing

- [x] Use testcontainers with Redis and Valkey images instead of Docker Compose
  - Parametrized session fixture for Redis/Valkey/redis-stack-server
- [ ] Fake cache backend (locmem-style or fakeredis) for testing without Redis
- [ ] Performance/benchmark tests
  - Measure serialization overhead
  - Measure compression overhead
  - Compare with Django builtin redis backend
  - Compare with django-cachex
- [ ] Edge case coverage review
  - Large values (> 512MB)
  - Unicode edge cases
  - Connection failure scenarios
  - Timeout edge cases
- [ ] Integration tests with real Redis Cluster (multi-node)
  - Currently using single-container cluster image

## Code Quality

- [x] Full type annotations up to Django layer (user-facing typing support)
  - Added cast() for redis-py type annotations
  - Created types.py with type aliases (KeyT, TimeoutT, EncodedT, protocols)
  - Proper type signatures on cache.py methods
- [x] Enable more ruff rules (disable/exclude instead of explicit enable, like django-nested-values)
  - Using `select = ["ALL"]` with minimal ignores
  - Per-file ignores for source code and tests
  - Fixed code quality issues: ClassVar, __slots__, dict comprehensions, etc.
- [x] Stricter mypy configuration
  - Enabled error codes: return-value, union-attr, operator, misc
  - Still disabled due to redis-py: arg-type (key types), assignment (Redis/RedisCluster), type-var (zadd)
  - Enabled `warn_unused_ignores = true` to catch stale type ignores
- [x] Configure ty type checker with separate ignores
  - ty ignores `# type: ignore` comments (for mypy only)
  - ty only respects `# ty: ignore[rule]` comments
  - `unused-ignore-comment = "error"` catches stale ty ignores
  - Errors requiring both: `code  # type: ignore[x]  # ty: ignore[y]`
- [ ] Review `[[tool.mypy.overrides]]` for optional dependencies
  - Currently using `ignore_missing_imports = true` for: lz4, xdist, backports, compression, msgpack, redis, valkey
  - As these libraries improve their type annotations, we may be able to remove some overrides
- [ ] Clean up config as changes are made
- [x] Remove CacheKey (dead code)
  - Django's official redis backend does NOT use a marker class
  - Our CacheKey had no `isinstance` checks - didn't prevent double-prefixing
  - `original_key()` method was never called anywhere
  - Removed: `make_key()` now returns `str` directly
- [ ] Add docstrings incrementally
  - Currently using `"D"` ignore in ruff for docstrings
  - Start with public API methods
  - Use Google-style docstrings
- [x] Verify py.typed marker is present for PEP 561
  - Added `django_cachex/py.typed` marker file
  - Allows type checkers to use our type hints

## Tooling/Infrastructure

- [x] Migrate to pyproject.toml with hatchling
- [x] Switch to UV for package management
- [x] MkDocs documentation with Material theme
- [x] Modern CI/CD with auto-tagging and publishing
- [x] Update CI matrix for Python 3.12-3.14 and Django 5.2-6.0
- [x] ~~Add SECURITY.md~~ - Not needed for this project
- [ ] Document release process
  - Versioning strategy (SemVer)
  - Changelog maintenance
  - PyPI publishing workflow

## Documentation

- [ ] Migration guide from django-cachex
  - Configuration differences
  - API differences
  - Feature comparison
- [ ] Migration guide from Django's builtin redis backend
  - What you gain by switching
  - Configuration mapping
- [ ] Compare features with competing packages
  - django-cachex (jazzband): https://github.com/jazzband/django-cachex
  - django-valkey: https://github.com/django-commons/django-valkey
  - Django builtin: django.core.cache.backends.redis
  - Document what we have that others don't, and vice versa
- [ ] Performance tuning guide
  - Connection pool sizing
  - Serializer selection
  - Compression tradeoffs
  - Replica routing
- [ ] Troubleshooting guide
  - Common errors and solutions
  - Connection issues
  - Serialization issues
- [ ] More examples/recipes
  - Session storage setup
  - Rate limiting pattern
  - Cache invalidation patterns
  - Multi-tenant caching
- [ ] Review API reference completeness
  - Ensure all public methods documented
  - Add examples for each method
  - Document all OPTIONS parameters
- [ ] Document all configuration options in one place
  - Currently spread across multiple doc pages
  - Single reference page with all OPTIONS
