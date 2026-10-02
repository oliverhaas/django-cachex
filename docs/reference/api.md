# API Reference

## Cache Methods

A method that the backend or the connected server does not support raises `NotSupportedError`. [Local backends](../user-guide/configuration.md#local-backends) lists what `LocMemCache` and `DatabaseCache` lack.

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
| `clear()` | Clear the cache, see [Clearing keys](#clearing-keys) |
| `has_key(key)` | Check if key exists |
| `incr(key, delta=1)` | Increment a value |
| `decr(key, delta=1)` | Decrement a value |
| `incr_version(key, delta=1)` | Increment key version |
| `decr_version(key, delta=1)` | Decrement key version |
| `close()` | Close connections |

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

The Valkey/Redis backends and `LocMemCache` take all three flags, and `TrackingCache` passes them to its transport. `DatabaseCache` takes only `nx`. `nx` together with `get` needs Redis 7.0+.

### Extended Methods

| Method | Description |
|--------|-------------|
| `ttl(key)` | Get TTL in seconds (`None` = no expiry, `-2` = not found) |
| `pttl(key)` | Same as `ttl` in milliseconds |
| `expire(key, timeout)` | Set expiration in seconds |
| `pexpire(key, timeout)` | Set expiration in milliseconds |
| `expireat(key, when)` | Set expiration at datetime |
| `pexpireat(key, when)` | Set expiration at datetime (ms precision) |
| `expiretime(key)` | Unix timestamp (seconds) when the key expires. Needs Redis 7.0+ (any supported Valkey) |
| `persist(key)` | Remove expiration |
| `type(key)` | Get the data type of a key |
| `memory_usage(key, version=None, *, samples=None)` | Bytes the key and its value take on the server, or `None` for a missing key. `samples` sets the `MEMORY USAGE` sample count for containers (`0` = all) |
| `largest_keys(pattern="*", count=10, version=None, *, samples=None, itersize=None)` | The `count` largest keys matching `pattern` as `(key, bytes)` pairs, largest first. Runs `MEMORY USAGE` on every matching key |
| `info(section=None)` | Server statistics from `INFO` as a dict. On the Valkey/Redis backends, `section`, such as `"memory"`, returns one section |
| `slowlog_get(count=10)` | Up to `count` of the newest slow log entries |
| `slowlog_len()` | Number of entries in the slow log |
| `lock(key, ...)` | Get a distributed lock, see [Lock Interface](#lock-interface) |
| `keys(pattern)` | Get keys matching pattern |
| `iter_keys(pattern)` | Iterate keys matching pattern |
| `scan(cursor=0, pattern="*", count=None, version=None, key_type=None)` | Single SCAN iteration. `key_type` keeps only keys of that type |
| `delete_pattern(pattern)` | Delete keys matching pattern |
| `rename(src, dst, version=None, version_src=None, version_dst=None)` | Rename a key; raises `KeyNotFoundError` when `src` does not exist. `version_src` and `version_dst` override `version` for one side |
| `renamenx(src, dst, version=None, version_src=None, version_dst=None)` | Rename a key only if `dst` does not exist; `False` when `dst` exists or `src` does not |

#### Key patterns

`keys()`, `iter_keys()`, `scan()` and `delete_pattern()` take a case-sensitive Redis glob on every backend. The glob supports `*`, `?`, `[abc]`, `[a-z]`, `[^abc]`, and `\` to escape any of them. The empty pattern matches only the empty key, so `delete_pattern("")` deletes at most one key. Use `"*"` to match every key.

### Data-structure calls with no arguments

`sadd`, `srem`, `hdel`, `hset`, `lpush`, `rpush`, `zrem`, `xdel` and `xack` return `0` when called with no members, fields, values or entry ids. So does `zadd` with an empty mapping. `smismember`, `zmscore`, `hgetex` and the field-TTL methods from `hexpire` to `hpersist` return `[]`. These calls send no command and create no key. In a pipeline, such a call still adds its `0` or `[]` to the `execute()` result, so the results line up with the calls.

### Hash Methods

| Method | Description |
|--------|-------------|
| `hset(key, field=None, value=None, version=None, mapping=None, items=None)` | Set hash field(s); pass `field`/`value`, a `mapping` dict, or a flat `items` list |
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
| `hexpire(key, timeout, *fields, nx=False, xx=False, gt=False, lt=False)` | Set a TTL in seconds (or a `timedelta`) on hash fields. Returns the `HEXPIRE` reply code per field |
| `hpexpire(key, timeout, *fields, ...)` | Same as `hexpire` with millisecond precision |
| `hexpireat(key, when, *fields, ...)` | Expire hash fields at a Unix timestamp or `datetime` |
| `hpexpireat(key, when, *fields, ...)` | Same as `hexpireat` with millisecond precision |
| `httl(key, *fields)` | Get TTL in seconds per field (`None` = no expiry, `-2` = not found) |
| `hpttl(key, *fields)` | Same as `httl` in milliseconds |
| `hexpiretime(key, *fields)` | Absolute Unix timestamp (seconds) per field (`None` = no expiry, `-2` = not found) |
| `hpersist(key, *fields)` | Remove field expirations. Returns the `HPERSIST` reply code per field |
| `hsetex(key, field=None, value=None, timeout=DEFAULT_TIMEOUT, version=None, mapping=None, items=None, *, fnx=False, fxx=False, keepttl=False)` | Set hash field(s) and their TTL in one command; `False` when `fnx`/`fxx` blocks the write |
| `hgetex(key, *fields, timeout=None, persist=False)` | Get hash field(s) and set (`timeout`) or remove (`persist`) their TTL in the same command |

Field expiration needs Redis 7.4+ or Valkey 9.0+, and `hsetex` and `hgetex` need Redis 8.0+ or Valkey 9.0+.

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

`sdiff`, `sinter`, `sunion` and their `*store` forms take `keys` as one key or a sequence. Write `sdiff(["a", "b"])`, because `sdiff("a", "b")` passes `"b"` as `version`.

Members must stay hashable after a round trip through the serializer. The JSON and MessagePack serializers return a tuple as a list, so `sadd` rejects tuples there. `smembers`, `sdiff`, `sinter`, `sunion`, `spop` and `sscan` return a Python `set`. `1`, `True` and `1.0` are one entry in it but three members on the server, and `scard` counts three.

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
| `blpop(keys, timeout=0)` | Blocking pop from head of list; `keys` is one key or a sequence |
| `brpop(keys, timeout=0)` | Blocking pop from tail of list; `keys` is one key or a sequence |
| `blmove(src, dst, timeout, ...)` | Blocking move between lists |

### Stream Methods

| Method | Description |
|--------|-------------|
| `xadd(key, fields, entry_id="*", maxlen=None, ..., nomkstream=False)` | Append an entry, returning its ID; `None` when `nomkstream=True` and the stream does not exist |
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

`eval_script()` sends `EVALSHA` and, after a `NOSCRIPT` reply, loads the script and retries. A pipeline sends queued scripts as plain `EVAL`. `django_cachex.script_sha(script)` returns the SHA-1 digest. [Lua Scripts](../user-guide/advanced.md#lua-scripts) has examples.

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

`Encoded(value)` marks one ARGV entry for `encoded_pre` to encode.

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

## Async Methods

Every cache method on this page except `get_client()`, `info()`, `slowlog_get()` and `slowlog_len()` has an async twin with an `a` prefix, such as `aget()` or `ahset()`. `alock()`, `asemaphore()` and `apipeline()` are `async def`, so `async with` needs an extra `await`, as in `async with await cache.alock("k"):`. See [Async Support](../user-guide/async.md).

## Raw Client Access

```python
client = cache.get_client(key=None, *, write=False)
```

| Parameter | Description |
|-----------|-------------|
| `key` | Ignored. Accepted for compatibility with django-redis |
| `write` | `True` returns a client for the primary. With a multi-URL `LOCATION` or Sentinel, `False` (the default) picks a replica pool when one exists. Cluster and valkey-glide ignore it |

`get_client()` bypasses the key prefix, version, serializer and compressor. The client type depends on the backend:

| Backend | `get_client()` returns |
|---------|------------------------|
| `ValkeyCache` (`valkey-py`) | `valkey.Valkey` |
| `RedisCache` (`redis-py`) | `redis.Redis` |
| `ValkeyGlideCache` (`valkey-glide`) | a proxy for `glide_sync.GlideClient` that forwards every attribute and translates server errors; `isinstance(client, GlideClient)` is `False` |

Use the cache API for portable code. `cache.adapter` also skips prefixing and serialization, as in `await cache.adapter.aget(prefixed_key)`. For the async client, call `await cache.adapter.get_async_client()`.

## Lock Interface

```python
lock = cache.lock(key, version=None, lease=None, sleep=0.1, *, blocking=True, timeout=None, thread_local=True)
```

| Parameter | Description |
|-----------|-------------|
| `key` | Lock name |
| `version` | Cache version of the lock key |
| `lease` | Seconds until a held lock expires and releases itself. `None` means no expiry |
| `sleep` | Seconds between acquire attempts |
| `blocking` | Wait if the lock is held |
| `timeout` | Longest time `acquire()` waits before it returns `False`. `None` means no limit |
| `thread_local` | Keep the ownership token in thread-local storage, so one thread cannot release another's hold |

Pass `lease` by keyword, because `cache.lock("k", 30)` sets `version=30` and leaves the lock without a TTL. `alock()` takes the same parameters. Locks work on the standalone and Sentinel Valkey/Redis backends. The cluster backends raise `NotSupportedError`, because the driver locks are not cluster-aware. Use `semaphore()` there.

On redis-py and valkey-py, the lock wraps the driver's lock and forwards every attribute. Its signature is `acquire(sleep=None, blocking=None, blocking_timeout=None, token=None)`, and the async one has no `sleep`. The valkey-glide lock has `acquire(*, blocking=None, timeout=None)`. Portable code passes `timeout` to `cache.lock()`.

Lock failures raise `django_cachex.lock.LockError` or its subclass `LockNotOwnedError`:

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

A lock also works in a `with` block, and `alock()` returns the async form:

```python
with cache.lock("mylock"):
    do_work()

async with await cache.alock("mylock"):
    await do_work()

# Manual acquire/release, async
lock = await cache.alock("mylock")
if await lock.acquire():
    try:
        await do_work()
    finally:
        await lock.release()
```

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
| `key` | Semaphore name. Sync and async callers with the same name share the budget. |
| `capacity` | Total budget, set by the first caller. A later caller with a different value replaces it, and `LocMemCache` also emits a `RuntimeWarning`. |
| `weight` | How much of the capacity this caller claims. The default `1` makes a counting semaphore. |
| `version` | Cache version of the semaphore keys. |
| `lease` | Seconds until a held claim expires, which frees the claim of a crashed holder. Required on the Valkey/Redis backends, ignored on `LocMemCache`. |
| `timeout` | Default wait for `acquire()` before it raises `SemaphoreTimeoutError`. `None` means no limit. |

Semaphores work on every Valkey/Redis backend, cluster included, and on `LocMemCache`. `LocMemCache` serves waiters in strict FIFO order within the process. The Valkey/Redis backends poll the server, and their FIFO order across processes is best-effort.

`acquire()` and `aacquire()` take keyword-only `blocking=True` and `timeout`. A blocking call returns `True` or raises `SemaphoreTimeoutError`, and only `blocking=False` can return `False`. Without a `timeout` argument, the value from `cache.semaphore()` applies. An explicit `timeout=None` waits without limit.

```python
sem = cache.semaphore("mysem", capacity=4, lease=30)
sem.acquire(timeout=5)  # raises SemaphoreTimeoutError after 5 seconds
try:
    do_work()
finally:
    sem.release()
```

`release()` frees the claim. `extend(additional_seconds)` adds to the remaining lease and returns `False` when the claim was already released or reaped. `LocMemCache` has no lease, so there `extend()` returns whether the claim is held.

On the Valkey/Redis backends, another caller can reclaim the weight of a claim whose lease expired while its holder still works. The holder's `release()` then finds no claim and logs a warning to the `django_cachex.semaphore` logger instead of raising.

The Valkey/Redis backends keep a semaphore in keys that start with `{<key>}:`, where `<key>` carries the `KEY_PREFIX` and `VERSION`. The braces make `<key>` a hash tag, so on a cluster all of them share a slot. `clear()`, `clear_all_versions()`, `keys()` and the admin key browser skip these keys. They go away when the last claim is released with nobody waiting, and a guard TTL expires them when the last holder dies without releasing.

Async code awaits `aacquire()`, `arelease()` and `aextend()`:

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

`await sem.acquire()` runs the sync acquire, then raises `TypeError` on the returned `bool` and leaves the claim held until its lease expires.

## Pipelines

```python
pipe = cache.pipeline(*, transaction=True, version=None)
pipe = await cache.apipeline(*, transaction=True, version=None)
```

`transaction=True`, the default on the standalone and Sentinel backends, wraps the batch in `MULTI`/`EXEC`. The cluster backends default to `transaction=False` and raise `NotSupportedError` for `transaction=True`. `version` overrides the cache's `VERSION` for every key queued on the pipeline.

`pipeline()` returns a `django_cachex.Pipeline`. `apipeline()` returns its subclass `django_cachex.AsyncPipeline`, which takes `async with` and an awaited `execute()`. The queueing methods are synchronous in both, and only `execute()` sends the commands. Leaving the `with` block discards the commands still queued.

```python
with cache.pipeline() as pipe:
    pipe.set("key1", "value1")
    pipe.hset("hash", "field", "value")
    results = pipe.execute()

async with await cache.apipeline() as pipe:
    pipe.set("key1", "value1")
    pipe.hset("hash", "field", "value")
    results = await pipe.execute()
```

A pipeline queues `get`, `set`, `delete`, `exists`, `incr`, `decr`, `type`, `rename`, `renamenx`, `memory_usage`, `eval_script` and the TTL methods from `ttl` to `persist`. It also queues the hash, list, set, sorted-set and stream methods, except `sscan`, `sscan_iter` and the blocking `blpop`, `brpop` and `blmove`. Other methods, such as `set_many`, `add` or `delete_pattern`, have no pipeline form, so queue their underlying commands. `execute()` returns the results as a list in queue order.

The queueing methods take the parameters of the cache methods. Their flags decide what a step adds to the results:

- `set()` adds `True`, or `False` when `nx` or `xx` blocks the write. With `get=True` it adds the previous value or `None` instead.
- `zadd(key, mapping, *, ..., incr=False)` takes `incr=True` on the pipeline only. It sends `ZINCRBY` for the single pair in `mapping` and adds the new score, or `None` when `nx`, `xx`, `gt` or `lt` blocked it.
- `zrange(key, start, end, *, withscores=False, desc=False)` takes `desc=True` on the pipeline only, which gives the result of `zrevrange`.
- `xautoclaim(..., justid=True)` raises `NotSupportedError` when queued. Use `justid=False`, or call `cache.xautoclaim()`.

The redis-py and valkey-py cluster pipelines refuse some multi-key commands, as [Cluster](../user-guide/cluster.md) lists.

## Clearing keys

| Method | Description |
|--------|-------------|
| `clear()` | Delete only this cache's keys (`KEY_PREFIX` + `VERSION`), with `delete_pattern("*")`. |
| `clear_all_versions(itersize=None)` | Delete this cache's keys (`KEY_PREFIX`) in every version and return the number deleted. |
| `flush_db()` | Run `FLUSHDB`, which deletes every key in the Redis/Valkey database, whatever its prefix. |

The table describes the Valkey/Redis backends. Only they support `clear_all_versions()` and `flush_db()`, and the other backends raise `NotSupportedError`. Neither `clear()` nor `clear_all_versions()` deletes [semaphore](#semaphore-interface) state.

On a shared database, `clear()` spares the keys of other caches only when each cache has its own `KEY_PREFIX`. Two caches on the default empty prefix both write `:1:*` keys, so `clear()` on either deletes the keys of both. Use `flush_db()` only to empty the whole database.

## Settings Reference

[OPTIONS Reference](../user-guide/configuration.md#options-reference) lists the `OPTIONS` keys and the backends that honor them.

### StampedeConfig

`django_cachex.StampedeConfig(buffer=60, beta=1.0, delta=1.0)` is a frozen dataclass that tunes [stampede prevention](../user-guide/configuration.md#cache-stampede-prevention) on the Valkey/Redis backends. Pass it to `OPTIONS["stampede_prevention"]`, or to the `stampede_prevention=` keyword of one call. The keyword also takes `True`, `False` or `None`, and the dict form works in `OPTIONS` only.

| Field | Default | Description |
|-------|---------|-------------|
| `buffer` | `60`  | Seconds added to TTL on writes; defines the early-recompute window. |
| `beta`   | `1.0` | Multiplier on the recompute probability; higher = recompute earlier. |
| `delta`  | `1.0` | Recompute-cost estimate (seconds); larger = recompute earlier. |

## Exceptions

All exceptions below are importable from `django_cachex` and subclass `CachexError`.

| Exception | Description |
|-----------|-------------|
| `CachexError` | Base class of every django-cachex exception. |
| `WrongTypeError` | A command hit a key of another type, as Redis `WRONGTYPE` reports. Subclass of `TypeError`, raised by every backend. |
| `KeyNotFoundError` | `rename()` found no source key. The missing key is in `key`. Subclass of `ValueError`. |
| `CompressorError` | Compression or decompression failed. Triggers the configured compressor fallback chain. |
| `SerializerError` | Serialization or deserialization failed. Triggers the serializer fallback chain. |
| `NotSupportedError` | The backend or the connected server does not support the operation. It carries `operation`, `backend` (`None` when the server rejected the command) and `detail`. |
| `LockError` | A lock operation failed, such as a `with` block that could not acquire the lock or a release of an unlocked lock. A driver lock error is kept as `__cause__`. Subclass of `ValueError`. |
| `LockNotOwnedError` | Releasing or extending a lock the caller no longer owns (expired or stolen). Subclass of `LockError`. |
| `SemaphoreError` | A semaphore operation failed, such as re-acquiring before release. |
| `SemaphoreTimeoutError` | `timeout` elapsed before the semaphore could be acquired. Subclass of `SemaphoreError`. |

The [ORM cache](../user-guide/orm-cache.md#failures) raises `django_cachex.orm.exceptions.InvalidationError` when a write cannot invalidate the cache. It subclasses `CachexError` and Django's `DatabaseError`, so `atomic()` rolls the transaction back.
