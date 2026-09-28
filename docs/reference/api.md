# API Reference

## Cache Methods

### Standard Django Cache Methods

| Method | Description |
|--------|-------------|
| `get(key, default=None)` | Get a value |
| `set(key, value, timeout=DEFAULT)` | Set a value |
| `add(key, value, timeout=DEFAULT)` | Set only if key doesn't exist |
| `delete(key)` | Delete a key |
| `touch(key, timeout=DEFAULT)` | Update timeout on a key |
| `get_many(keys)` | Get multiple values |
| `set_many(data, timeout=DEFAULT)` | Set multiple values |
| `delete_many(keys)` | Delete multiple keys |
| `get_or_set(key, default, timeout=DEFAULT)` | Get value or set default |
| `clear()` | Clear the cache |
| `has_key(key)` | Check if key exists |
| `incr(key, delta=1)` | Increment a value |
| `decr(key, delta=1)` | Decrement a value |
| `incr_version(key, delta=1)` | Increment key version |
| `decr_version(key, delta=1)` | Decrement key version |
| `close()` | Close connections |

### Extended Methods

| Method | Description |
|--------|-------------|
| `ttl(key)` | Get TTL in seconds (`None` = no expiry, `-2` = not found) |
| `pttl(key)` | Get TTL in milliseconds (`None` = no expiry, `-2` = not found) |
| `expire(key, timeout)` | Set expiration in seconds |
| `pexpire(key, timeout)` | Set expiration in milliseconds |
| `expireat(key, when)` | Set expiration at datetime |
| `pexpireat(key, when)` | Set expiration at datetime (ms precision) |
| `expiretime(key)` | Absolute Unix timestamp (seconds) when the key expires. Redis 7.0+ (any supported Valkey); an older server raises `NotSupportedError` |
| `persist(key)` | Remove expiration |
| `type(key)` | Get the data type of a key |
| `memory_usage(key, version=None, *, samples=None)` | Bytes the key and its value take on the server (`MEMORY USAGE`), or `None` for a missing key. `samples` bounds how many elements of a container are sampled (server default 5, `0` = all). Valkey/Redis backends only; other backends raise `NotSupportedError` |
| `largest_keys(pattern="*", count=10, version=None, *, samples=None, itersize=None)` | The `count` largest keys matching `pattern` as `(key, bytes)` pairs, largest first. Scans with `iter_keys()` and pipelines `MEMORY USAGE` in batches of 100. `count=0` returns `[]` without scanning, and a negative `count` raises `ValueError`. Valkey/Redis backends only; other backends raise `NotSupportedError` |
| `clear_all_versions(itersize=None)` | Delete every key under this cache's `KEY_PREFIX` in all versions, matched by the glob `key_func("*", prefix, "*")`, and return the number deleted. `delete_pattern()` adds the prefix and one version to its pattern, so it cannot match all versions. `clear()` removes the current version only. Valkey/Redis backends only; other backends raise `NotSupportedError` |
| `lock(key, ...)` | Get a distributed lock. Valkey/Redis backends only: `LocMemCache`, `DatabaseCache` and `TrackingCache` raise `NotSupportedError` |
| `keys(pattern)` | Get keys matching pattern |
| `iter_keys(pattern)` | Iterate keys matching pattern |
| `scan(cursor=0, pattern="*", count=None, version=None, key_type=None)` | Single SCAN iteration. `key_type` keeps only keys of that RESP type. `version` is the fourth positional parameter, so pass `key_type` by keyword |
| `delete_pattern(pattern)` | Delete keys matching pattern |
| `rename(src, dst)` | Rename a key; raises `KeyNotFoundError` when `src` does not exist |
| `renamenx(src, dst)` | Rename a key only if `dst` does not exist; `False` when `dst` exists or `src` does not |

#### Key patterns

`keys()`, `iter_keys()`, `scan()` and `delete_pattern()` take a Redis glob on every backend. The glob supports `*`, `?`, `[abc]`, `[a-z]`, `[^abc]` for negation, and `\` to escape any of them. Matching is case-sensitive, also on `LocMemCache` and on `DatabaseCache` with SQLite. A reversed range such as `[z-a]` matches like `[a-z]`, as in Redis.

The empty pattern matches only the empty key, as in Redis, so `delete_pattern("")` deletes at most one key. Use `"*"` to match every key.

### Data-structure calls with no arguments

Called with no members, fields, values or entry ids, `sadd`, `srem`, `hdel`, `hset`, `lpush`, `rpush`, `zrem`, `xdel` and `xack` return `0`. So does `zadd` with an empty mapping. `smismember` and `zmscore` return an empty list. No command reaches the server, so no key is created. Every backend that supports the method answers the same way, and so do the async twins. `xadd` raises `ValueError` for empty `fields`, because an entry needs at least one field.

The hash-field TTL commands (`hexpire`, `hpexpire`, `hexpireat`, `hpexpireat`, `httl`, `hpttl`, `hexpiretime`, `hpersist` and `hgetex`) called with no fields return `[]` and send nothing.

In a pipeline, such a call queues no command. It still adds the same `0` or `[]` to the `execute()` result, so the results line up with the calls. Async pipelines behave the same way.

### Hash Methods

| Method | Description |
|--------|-------------|
| `hset(key, field=None, value=None, mapping=None, items=None)` | Set hash field(s); pass `field`/`value`, a `mapping` dict, or a flat `items` list |
| `hdel(key, *fields)` | Delete hash field(s) |
| `hexists(key, field)` | Check if hash field exists |
| `hget(key, field)` | Get a hash field value |
| `hgetall(key)` | Get all fields and values in a hash |
| `hkeys(key)` | Get all field names in a hash |
| `hincrby(key, field, amount=1)` | Increment hash field by integer |
| `hincrbyfloat(key, field, amount=1.0)` | Increment hash field by float |
| `hlen(key)` | Get number of fields in hash |
| `hmget(key, *fields)` | Get multiple hash field values |
| `hsetnx(key, field, value)` | Set hash field only if it doesn't exist |
| `hvals(key)` | Get all values in a hash |
| `hexpire(key, timeout, *fields, nx=False, xx=False, gt=False, lt=False)` | Set a TTL in seconds (or a `timedelta`) on hash fields. Returns one code per field: `2` deleted, `1` set, `0` condition unmet, `-2` no such field |
| `hpexpire(key, timeout, *fields, ...)` | Same as `hexpire` with millisecond precision |
| `hexpireat(key, when, *fields, ...)` | Expire hash fields at a Unix timestamp or `datetime` |
| `hpexpireat(key, when, *fields, ...)` | Same as `hexpireat` with millisecond precision |
| `httl(key, *fields)` | Get TTL in seconds per field (`None` = no expiry, `-2` = not found) |
| `hpttl(key, *fields)` | Same as `httl` in milliseconds |
| `hexpiretime(key, *fields)` | Absolute Unix timestamp (seconds) per field (`None` = no expiry, `-2` = not found) |
| `hpersist(key, *fields)` | Remove field expirations; `1` removed, `-1` had none, `-2` no such field |
| `hsetex(key, field=None, value=None, timeout=DEFAULT_TIMEOUT, mapping=None, items=None, fnx=False, fxx=False, keepttl=False)` | Set hash field(s) and their TTL in one command; `False` when `fnx`/`fxx` blocks the write |
| `hgetex(key, *fields, timeout=None, persist=False)` | Get hash field(s) and set (`timeout`) or remove (`persist`) their TTL in the same command |

Field expiration needs Redis 7.4+ or Valkey 9.0+, and `hsetex` and `hgetex` need Redis 8.0+ or Valkey 9.0+. An older server raises `NotSupportedError`.

`hset()` and `hsetex()` raise `ValueError("items must hold field/value pairs")` for an odd-length `items` list. In a pipeline the error comes when the call is queued, so nothing in the batch is sent.

### Set Methods

| Method | Description |
|--------|-------------|
| `sadd(key, *members)` | Add member(s) to set |
| `srem(key, *members)` | Remove member(s) from set |
| `smembers(key)` | Get all members of set |
| `sismember(key, member)` | Check if member exists in set |
| `smismember(key, *members)` | Check if multiple members exist |
| `scard(key)` | Get number of members |
| `spop(key, count=None)` | Remove and return random member(s) |
| `srandmember(key, count=None)` | Get random member(s) without removing |
| `smove(src, dst, member)` | Move member between sets |
| `sdiff(keys)` | Get difference of sets |
| `sdiffstore(dest, keys)` | Store difference of sets |
| `sinter(keys)` | Get intersection of sets |
| `sinterstore(dest, keys)` | Store intersection of sets |
| `sunion(keys)` | Get union of sets |
| `sunionstore(dest, keys)` | Store union of sets |
| `sscan(key, cursor=0, ...)` | Incrementally iterate set members |
| `sscan_iter(key, ...)` | Iterate over set members using SSCAN |

The multi-key set operations take `keys` as one key or a sequence. Their second positional parameter is `version`, so write `sdiff(["a", "b"])`, not `sdiff("a", "b")`.

`spop` raises `ValueError` for a negative `count`, with Redis's message `value is out of range, must be positive`.

Members must be hashable before and after a round trip through the configured serializer, and `sadd` raises `TypeError` for a member that is not. The JSON and MessagePack serializers, for example, return a tuple as a list. The set readers (`smembers`, `sdiff`, `sinter`, `sunion`, `spop`, `sscan`) return a Python `set`, so Python equality decides membership. `1`, `True` and `1.0` are three members on the server, and `scard` counts three. In the returned `set` they are one entry, as in `{1, True, 1.0}`.

### Sorted Set Methods

| Method | Description |
|--------|-------------|
| `zadd(key, mapping, *, nx, xx, ch, gt, lt)` | Add member(s) with scores |
| `zcard(key)` | Get number of members |
| `zcount(key, min_score, max_score)` | Count members with scores in range |
| `zincrby(key, amount, member)` | Increment member's score |
| `zrange(key, start, end, ...)` | Get members by index range |
| `zrevrange(key, start, end, ...)` | Get members by index range (descending) |
| `zrangebyscore(key, min_score, max_score, ...)` | Get members by score range |
| `zrevrangebyscore(key, max_score, min_score, ...)` | Get members by score range (descending) |
| `zrank(key, member)` | Get member's rank (ascending) |
| `zrevrank(key, member)` | Get member's rank (descending) |
| `zrem(key, *members)` | Remove member(s) |
| `zremrangebyrank(key, start, end)` | Remove members by rank range |
| `zremrangebyscore(key, min_score, max_score)` | Remove members by score range |
| `zscore(key, member)` | Get member's score |
| `zmscore(key, *members)` | Get multiple members' scores |
| `zpopmin(key, count=None)` | Remove and return members with lowest scores |
| `zpopmax(key, count=None)` | Remove and return members with highest scores |

### List Methods

| Method | Description |
|--------|-------------|
| `llen(key)` | Get list length |
| `lpush(key, *values)` | Prepend value(s) to list |
| `rpush(key, *values)` | Append value(s) to list |
| `lpop(key, count=None)` | Remove and return the first element, or a list of the first `count` elements |
| `rpop(key, count=None)` | Remove and return the last element, or a list of the last `count` elements |
| `lindex(key, index)` | Get element by index |
| `lrange(key, start, end)` | Get elements in range |
| `lset(key, index, value)` | Set element at index |
| `ltrim(key, start, end)` | Trim list to range |
| `lrem(key, count, value)` | Remove elements equal to value |
| `lpos(key, value, ...)` | Find element position in list |
| `linsert(key, where, pivot, value)` | Insert value before or after pivot |
| `lmove(src, dst, wherefrom, whereto)` | Atomically move element between lists |
| `blpop(keys, timeout=0)` | Blocking pop from head of list |
| `brpop(keys, timeout=0)` | Blocking pop from tail of list |
| `blmove(src, dst, timeout, ...)` | Blocking move between lists |

`blpop` and `brpop` take `keys` as one key or a sequence, followed by `timeout`.

`lpos` rejects `rank=0` and a negative `count` or `maxlen`, and `linsert` rejects a `where` other than `"BEFORE"` or `"AFTER"`. Both raise `ValueError` with Redis's own message.

### Stream Methods

| Method | Description |
|--------|-------------|
| `xadd(key, fields, entry_id="*", maxlen=None, ..., nomkstream=False)` | Append an entry, returning its ID; `None` when `nomkstream=True` and the stream does not exist. Empty `fields` raise `ValueError` |
| `xlen(key)` | Number of entries in the stream |
| `xrange(key, start="-", end="+", count=None)` | Range of entries (forward) |
| `xrevrange(key, end="+", start="-", count=None)` | Range of entries (reverse) |
| `xread(streams, count=None, block=None)` | Read new entries from one or more streams |
| `xtrim(key, maxlen=None, approximate=True, minid=None, ...)` | Cap stream length |
| `xdel(key, *entry_ids)` | Delete entries by ID |
| `xinfo_stream(key, full=False)` | Stream metadata |
| `xinfo_groups(key)` | Consumer group metadata |
| `xinfo_consumers(key, group)` | Per-consumer metadata for a group |
| `xgroup_create(key, group, entry_id="$", mkstream=False)` | Create a consumer group |
| `xgroup_destroy(key, group)` | Drop a consumer group |
| `xgroup_setid(key, group, entry_id)` | Re-anchor a consumer group's read position |
| `xgroup_delconsumer(key, group, consumer)` | Drop a consumer from a group |
| `xreadgroup(group, consumer, streams, count=None, block=None)` | Read entries as a group consumer |
| `xack(key, group, *entry_ids)` | Acknowledge processed entries |
| `xpending(key, group, ...)` | Inspect pending (unacked) entries |
| `xclaim(key, group, consumer, min_idle_time, entry_ids, ...)` | Claim pending entries |
| `xautoclaim(key, group, consumer, min_idle_time, ...)` | Auto-claim entries idle longer than threshold |

### Lua Script Methods

`eval_script()` sends `EVALSHA` with the SHA-1 of the script. On a `NOSCRIPT` reply it loads the script with `SCRIPT LOAD` and retries, so a `SCRIPT FLUSH` or a server restart costs one extra round trip. A pipeline sends queued scripts as plain `EVAL`, because it cannot retry after a `NOSCRIPT` reply and cluster pipelines reject `EVALSHA`. `django_cachex.script_sha(script)` returns the digest, for code that calls `SCRIPT EXISTS` or `EVALSHA` through `get_client()`.

#### eval_script / aeval_script

```python
result = cache.eval_script(
    script,  # Lua source
    keys=(),  # KEYS
    args=(),  # ARGV
    pre_hook=None,  # (helpers, keys, args) -> (keys, args)
    post_hook=None,  # (helpers, result) -> result; None returns the result unchanged
    version=None,  # key version for prefixing
)
```

#### Pre-built Hooks

| Hook | Description |
|------|-------------|
| `encoded_pre` | Prefix keys, encode the args wrapped in `Encoded(...)`, pass the rest through |
| `keys_only_pre` | Prefix keys, leave args unchanged |
| `full_encode_pre` | Prefix keys and encode all args |
| `decode_single_post` | Decode a single returned value |
| `decode_list_post` | Decode a list of returned values |

`Encoded(value)` marks one ARGV entry for `encoded_pre` to encode. An `Encoded` in `keys`, a nested `Encoded(Encoded(...))`, and an `Encoded` that no `pre_hook` unwraps all raise `TypeError`.

#### ScriptHelpers

The `helpers` argument of both hooks:

| Attribute/Method | Description |
|------------------|-------------|
| `make_key(key, version)` | Apply cache key prefix |
| `make_keys(keys)` | Prefix multiple keys |
| `encode(value)` | Encode a value (serialize + compress) |
| `encode_values(values)` | Encode multiple values |
| `decode(value)` | Decode a value |
| `decode_values(values)` | Decode multiple values |
| `version` | Current key version |

### Set Method Options

```python
cache.set(key, value, timeout=300, nx=False, xx=False, get=False)
```

| Parameter | Description |
|-----------|-------------|
| `timeout` | Expiration in seconds (`None` = never, `0` = immediate) |
| `nx` | Only set if key doesn't exist (SETNX) |
| `xx` | Only set if key exists |
| `get` | Return the previous value (atomic get-and-set) |

The Valkey/Redis backends and `LocMemCache` take all three flags. `DatabaseCache` takes `nx` and raises `NotSupportedError` for `xx` and `get`. `TrackingCache` passes the flags to its transport.

## Async Methods

Every cache method on this page except `get_client()` has an async twin with an `a` prefix, such as `aget()` or `ahset()`. The twins apply the same key prefix, serializer and compressor:

```python
# Sync
value = cache.get("key")

# Async
value = await cache.aget("key")
```

```python
# Sync
cache.hset("hash", "field", "value")

# Async
await cache.ahset("hash", "field", "value")
```

`alock()`, `asemaphore()` and `apipeline()` are `async def`, so `async with` needs an extra `await`, as in `async with await cache.alock("k"):`. For raw access without prefixing or serialization, use `cache.adapter`, for example `await cache.adapter.aget(prefixed_key)`.

## Raw Client Access

```python
client = cache.get_client(key=None, *, write=False)
```

| Parameter | Description |
|-----------|-------------|
| `key` | Ignored by every adapter. Accepted for compatibility with django-redis |
| `write` | `True` returns a client for the primary. With a multi-URL `LOCATION` or Sentinel, `False` (the default) picks a replica pool when one exists. Cluster ignores it |

The return type depends on the backend:

| Backend | `get_client()` returns |
|---------|------------------------|
| `ValkeyCache` (`valkey-py`) | `valkey.Valkey` |
| `RedisCache` (`redis-py`) | `redis.Redis` |
| `ValkeyGlideCache` (`valkey-glide`) | a proxy wrapping `glide_sync.GlideClient`; it forwards every attribute and translates server errors, but `isinstance(client, GlideClient)` is `False` |

The three clients have similar commands but different types, so code that relies on one client's type or vendor-specific calls fails on the others. `get_client()` also bypasses the key prefix, version, serializer and compressor. Use the cache API for portable code.

The cache has no `get_async_client()`. Get the async client from the adapter: `await cache.adapter.get_async_client()`.

## Lock Interface

```python
lock = cache.lock(key, version=None, lease=None, sleep=0.1, *, blocking=True, timeout=None, thread_local=True)
```

| Parameter | Description |
|-----------|-------------|
| `key` | Lock name |
| `version` | Cache version of the lock key |
| `lease` | Seconds until a held lock expires and releases itself. `None` means no expiry. A value under 1 ms raises `ValueError` |
| `sleep` | Seconds between acquire attempts |
| `blocking` | Wait if the lock is held |
| `timeout` | Longest time `acquire()` waits before it gives up. `None` means no limit |
| `thread_local` | Keep the ownership token in thread-local storage, so one thread cannot release another's hold |

`version` comes before `lease`, so `cache.lock("k", 30)` sets `version=30` and leaves the lock without a TTL. Pass `lease` by keyword. `alock()` takes the same parameters.

The `acquire()` signature depends on the backend. The redis-py and valkey-py backends return a thin wrapper around the driver's lock, with `acquire(sleep=None, blocking=None, blocking_timeout=None, token=None)`. The async `acquire()` has no `sleep`. The wrapper forwards attribute reads and writes, so `lock.blocking_timeout = 1` reaches the driver lock. `copy.copy()` works, and pickling fails as it does for the bare driver lock. Passing `timeout=` to this `acquire()` raises `TypeError`.

The valkey-glide backend returns the django-cachex lock, whose `acquire()` is keyword-only: `acquire(*, blocking=None, timeout=None)`.

```python
lock = cache.lock("mylock", lease=30)
if lock.acquire(blocking_timeout=5):  # redis-py / valkey-py: wait up to 5s
    ...
```

For code that runs on every Valkey/Redis backend, set the wait on `cache.lock()`:

```python
lock = cache.lock("mylock", lease=30, timeout=5)
if lock.acquire():
    ...
```

Lock failures raise `django_cachex.lock.LockError` on every Valkey/Redis backend. A lock lost before `release()` or `extend()` raises its subclass `LockNotOwnedError`. On redis-py and valkey-py, the driver's own `LockError` is kept as `__cause__`. Both classes subclass `ValueError` like the driver lock errors, whereas `threading.Lock` raises `RuntimeError` on a bad release.

```python
from django_cachex.lock import LockError, LockNotOwnedError


def guarded_work():
    lock = cache.lock("mylock", lease=30, timeout=5)
    try:
        if not lock.acquire():
            return  # another holder kept it for 5 seconds
        do_work()
        lock.release()
    except LockNotOwnedError:
        ...  # the lease ran out during do_work()
    except LockError:
        ...  # released twice or by another owner
```

The lock supports the `threading.Lock` usage patterns:

```python
# Context manager
with cache.lock("mylock"):
    do_work()

# Manual acquire/release
lock = cache.lock("mylock")
if lock.acquire():
    try:
        do_work()
    finally:
        lock.release()
```

### Async lock

```python
# Context manager
async with await cache.alock("mylock"):
    await do_work()

# Manual acquire/release
lock = await cache.alock("mylock")
if await lock.acquire():
    try:
        await do_work()
    finally:
        await lock.release()
```

On the cluster backends, `lock()` and `alock()` raise `NotSupportedError`, because the driver locks are not cluster-aware. Use `semaphore()` instead.

## Semaphore Interface

```python
sem = cache.semaphore(key, capacity, *, weight=1, version=None, lease=None, timeout=None)
```

`semaphore()` returns a weighted semaphore. Use it as a context manager:

```python
with cache.semaphore("image-convert", capacity=4, lease=60):
    # Up to 4 callers can hold this semaphore at once.
    convert(...)

# Weighted: claim 100 of a 500 budget.
with cache.semaphore("memory-heavy", weight=100, capacity=500, lease=300):
    convert_huge(...)
```

| Parameter | Description |
|-----------|-------------|
| `key` | Semaphore name. Callers with the same name share the budget. |
| `capacity` | Total budget. The first caller sets it. A later caller that passes a different value updates it on every backend, and `LocMemCache` also emits a `RuntimeWarning`. |
| `weight` | How much of the capacity this caller claims. The default `1` makes a counting semaphore. |
| `version` | Cache version of the semaphore keys. |
| `lease` | Seconds until a held claim expires, which frees the claim of a crashed holder. Required on the RESP backends, ignored on `LocMemCache`. |
| `timeout` | Default wait for `acquire()` before it raises `SemaphoreTimeoutError`. `None` means no limit. |

`acquire()` and `aacquire()` take `blocking` and `timeout` to override the defaults of the semaphore object. Without a `timeout` argument, the value from `cache.semaphore()` applies. An explicit `timeout=None` waits without limit, whatever the default is.

`release()` returns the claim. On the RESP backends, `extend(additional_seconds)` and `aextend(additional_seconds)` add `additional_seconds` to the remaining lease. They raise `ValueError` unless `additional_seconds` is a positive finite number. They return `False` when the claim was already released or reaped. `LocMemCache` has no lease, so there they return `True` while the claim is held and `False` otherwise.

On the RESP backends, a semaphore keeps three keys: `{name}:state`, `{name}:claims` and `{name}:queue`. Every operation renews the TTL of the keys it touches to twice the longest lease seen. When the last holder dies without releasing, the keys expire on their own.

```python
sem = cache.semaphore("mysem", capacity=4, lease=30)
sem.acquire(timeout=5)  # raises SemaphoreTimeoutError after 5 seconds
try:
    do_work()
finally:
    sem.release()
```

### Async semaphore

```python
# Context manager
async with await cache.asemaphore("mysem", capacity=4, lease=30):
    await do_work()

# Manual acquire/release
sem = await cache.asemaphore("mysem", capacity=4, lease=30)
await sem.aacquire()
try:
    await do_work()
finally:
    await sem.arelease()
```

`aacquire()` and `arelease()` are the awaitable methods. The same object also has the sync `acquire()` and `release()`, so `await sem.acquire()` runs the sync acquire and then raises `TypeError` on the returned `bool`. The claim stays held until its lease expires.

Sync and async callers on the same cache instance share state for a given name.

`LocMemCache` serves waiters in strict FIFO order within the process. The RESP backends run Lua scripts, and their FIFO order across processes is best-effort, with a head-of-queue check and jittered polling. Semaphores work on the cluster backends, because all keys of one semaphore carry the `{name}` hash tag and share a slot.

## Pipelines

Queueing methods (`set`, `hset`, `lpush`, ...) are synchronous in both wrappers. Only `execute()` sends the commands.

```python
pipe = cache.pipeline(*, transaction=True, version=None)
pipe = await cache.apipeline(*, transaction=True, version=None)
```

`transaction=True`, the default on the standalone and Sentinel backends, wraps the batch in `MULTI`/`EXEC`. `transaction=False` sends a plain pipeline. The cluster backends default to `transaction=False` and raise `NotSupportedError` for `transaction=True`, because `MULTI`/`EXEC` cannot span slots. `version` overrides the cache's `VERSION` for every key queued on the pipeline.

`pipeline()` returns a `django_cachex.Pipeline`. `apipeline()` returns its subclass `django_cachex.AsyncPipeline`, with the same queueing methods, an awaitable `execute()` and `async with` support. A plain `with` on an `AsyncPipeline` raises `TypeError` at `__enter__`. Leaving either context manager discards the commands still queued.

### Sync

```python
with cache.pipeline() as pipe:
    pipe.set("key1", "value1")
    pipe.set("key2", "value2")
    pipe.hset("hash", "field", "value")
    results = pipe.execute()
```

### Async

```python
async with await cache.apipeline() as pipe:
    pipe.set("key1", "value1")
    pipe.hset("hash", "field", "value")
    results = await pipe.execute()
```

A pipeline queues `get`, `set`, `delete`, `exists`, `incr`, `decr`, `type`, `rename`, `renamenx`, `memory_usage` and `eval_script`. It also queues the TTL commands (`ttl`, `pttl`, `expire`, `pexpire`, `expireat`, `pexpireat`, `expiretime`, `persist`) and the hash, list, set, sorted-set and stream commands. The exceptions are `sscan`, `sscan_iter` and the blocking `blpop`, `brpop` and `blmove`. The pipeline has no `set_many`, `get_many`, `delete_many`, `get_or_set`, `add`, `touch`, `has_key`, `keys`, `scan`, `delete_pattern` or `clear`. Queue their underlying commands instead. `execute()` returns the results as a list in queue order.

On `RedisClusterCache` and `ValkeyClusterCache`, the driver's cluster pipeline refuses the multi-key commands `rename`, `renamenx`, `smove`, `sdiff`, `sinter`, `sunion`, `sdiffstore`, `sinterstore` and `sunionstore`. Queueing one raises `RedisClusterException` or `ValkeyClusterException`. Call them on the cache instead, with keys that share a hash tag (see [Cluster](../user-guide/cluster.md#hash-tags)).

The queueing methods take the same parameters as the cache methods, including `min_score`, `max_score`, `start` and `num` on the score-range methods:

```python
pipe.zcount("z", min_score, max_score)
pipe.zrangebyscore("z", min_score, max_score, withscores=False, start=None, num=None)
pipe.zrevrangebyscore("z", max_score, min_score, withscores=False, start=None, num=None)
pipe.zremrangebyscore("z", min_score, max_score)
```

The flags work as on the cache methods and decide what a step adds to the results:

- `set(key, value, timeout=DEFAULT, version=None, *, nx=False, xx=False, get=False)` adds `True`, or `False` when `nx` or `xx` blocks the write. With `get=True` it adds the previous value or `None` instead, also with `nx`, `xx` or an immediate timeout. `SET ... GET` needs Redis 6.2+, and `nx` together with `get` needs Redis 7.0+.
- `get(key, default=None, version=None)` adds `default` for a missing key. `version` is the third positional argument.
- `zadd(key, mapping, *, ..., incr=False)`: `incr=True` exists on the pipeline only. It turns the single pair in `mapping` into `ZINCRBY` with the other flags applied. The step adds the member's new score, or `None` when `nx`, `xx`, `gt` or `lt` blocked it.
- `zrange(key, start, end, *, withscores=False, desc=False)`: `desc=True` exists on the pipeline only. It sends `ZRANGE ... REV`, which gives the same result as `zrevrange`.
- `xautoclaim(..., justid=True)` raises `NotSupportedError` when queued, because the driver drops the cursor from a pipelined `JUSTID` reply. Use `justid=False`, or call `cache.xautoclaim()`.
- `memory_usage(key, version=None, *, samples=None)` adds the byte count, or `None` for a missing key.

## Clearing keys

| Method | Description |
|--------|-------------|
| `clear()` | Delete only this cache's keys (`KEY_PREFIX` + `VERSION`), with `delete_pattern("*")`. |
| `flush_db()` | Run `FLUSHDB`, which deletes every key in the Redis/Valkey database, whatever its prefix. |

On a shared database, `clear()` spares the keys of other caches only when each cache has its own `KEY_PREFIX`. Two caches on the default empty prefix both write `:1:*` keys, so `clear()` on either deletes the keys of both. Use `flush_db()` only to empty the whole database.

## Settings Reference

### Cache OPTIONS

| Option | Backends | Description |
|--------|----------|-------------|
| `serializer` | all Valkey/Redis | Serializer class or list for fallback |
| `compressor` | all Valkey/Redis | Compressor class or list for fallback |
| `stampede_prevention` | all Valkey/Redis | `True` / `False` / `None` / dict (`buffer`, `beta`, `delta`) / a `StampedeConfig`; any other value raises `ImproperlyConfigured`. See [`StampedeConfig`](#stampedeconfig) |
| `username` | all Valkey/Redis | ACL user name; takes precedence over the URL |
| `password` | all Valkey/Redis | Server password; takes precedence over the URL |
| `socket_connect_timeout` | redis-py, valkey-py | Connection timeout |
| `socket_timeout` | redis-py, valkey-py | Read/write timeout |
| `pool_class` | redis-py, valkey-py | Custom connection pool class (sync). The cluster backends reject it with `ImproperlyConfigured` |
| `async_pool_class` | redis-py, valkey-py | Custom connection pool class (async). The cluster backends reject it with `ImproperlyConfigured` |
| `parser_class` | redis-py, valkey-py | Custom RESP parser class. The cluster backends reject it with `ImproperlyConfigured`, because their client picks its own parser |
| `sentinels` | redis-py, valkey-py | Sentinel server list (for Sentinel backends) |
| `sentinel_kwargs` | redis-py, valkey-py | Sentinel configuration |
| `db` | valkey-glide | Database index (standalone only); on redis-py/valkey-py it goes through the URL or `from_url()` |
| `use_tls` / `ssl` | valkey-glide | Force TLS on or off, overriding the URL scheme |
| `request_timeout` | valkey-glide | Per-request timeout, in milliseconds |
| `client_name` | valkey-glide | Client name reported to the server |

The redis-py and valkey-py adapters pass every other key to the driver's `from_url()`, so driver options such as `retry_on_timeout`, `ssl_ca_certs` and `socket_keepalive` work there. The valkey-glide adapter reads only the keys marked "all Valkey/Redis" or "valkey-glide" and ignores the rest. `LocMemCache`, `DatabaseCache` and `TrackingCache` take their own `OPTIONS`, described in [Configuration](../user-guide/configuration.md) and [TrackingCache](../user-guide/composite-backends.md).

### StampedeConfig

`django_cachex.StampedeConfig(buffer=60, beta=1.0, delta=1.0)` is a frozen dataclass that tunes the TTL-based XFetch stampede prevention. Pass it to `OPTIONS["stampede_prevention"]` for the whole cache, or to the `stampede_prevention=` keyword for one call. `get`, `set`, `get_many`, `set_many`, `add`, `touch`, `get_or_set`, `ttl`, `pttl`, `expire`, `expireat`, `pexpire`, `pexpireat`, `expiretime` and their async twins take the keyword. Its value is `True`, `False`, `None` or a `StampedeConfig`, and any other value raises `TypeError`. The dict form works in `OPTIONS` only. On `touch`, the keyword decides whether the refreshed TTL gets the buffer added back.

| Field | Default | Description |
|-------|---------|-------------|
| `buffer` | `60`  | Seconds added to TTL on writes; defines the early-recompute window. |
| `beta`   | `1.0` | Multiplier on the recompute probability; higher = recompute earlier. |
| `delta`  | `1.0` | Recompute-cost estimate (seconds); larger = recompute earlier. |

Only the Valkey/Redis backends implement stampede prevention. `LocMemCache` and `DatabaseCache` ignore the `OPTIONS` key, and their methods raise `TypeError` for the `stampede_prevention=` keyword. `TrackingCache` follows the setting of its transport and has no per-call keyword either.

## Exceptions

All exceptions below are importable from `django_cachex` and subclass `CachexError`, so `except CachexError` catches any of them.

| Exception | Description |
|-----------|-------------|
| `CachexError` | Base class of every django-cachex exception. |
| `WrongTypeError` | A command hit a key that holds another RESP type, as Redis `WRONGTYPE` reports. Subclass of `TypeError`. Raised by `LocMemCache`, `DatabaseCache`, redis-py, valkey-py and valkey-glide alike. |
| `KeyNotFoundError` | An operation needed a key that does not exist, as Redis `ERR no such key` reports. `rename()` raises it for a missing source. The missing key is in `key`. Subclass of `ValueError`. |
| `CompressorError` | Compression or decompression failed. Triggers the configured compressor fallback chain. |
| `SerializerError` | Serialization or deserialization failed. Triggers the serializer fallback chain. |
| `NotSupportedError` | The backend does not support the operation (e.g. `lpush` on `TrackingCache`), or the connected server does not (e.g. `hexpire` on Redis 7.2). `operation` names the method or command, and `backend` the cache class (`None` when the server rejected the command). `detail` gives the reason, including the server release that adds a missing command. |
| `LockError` | A lock operation failed, such as a `with` block that could not acquire the lock or a release of an unlocked lock. Raised by every Valkey/Redis backend. Where the driver's own lock raised, that error is the `__cause__`. Subclass of `ValueError`. |
| `LockNotOwnedError` | Releasing or extending a lock the caller no longer owns (expired or stolen). Subclass of `LockError`. |
| `SemaphoreError` | A semaphore operation failed (e.g. re-acquiring before release). |
| `SemaphoreTimeoutError` | `timeout` elapsed before the semaphore could be acquired. Subclass of `SemaphoreError`. |

The [ORM cache](../user-guide/orm-cache.md) raises `django_cachex.orm.exceptions.InvalidationError` when a write cannot invalidate the cache. It subclasses `CachexError` and Django's `DatabaseError`, so `atomic()` rolls the transaction back. See [Failures](../user-guide/orm-cache.md#failures).
