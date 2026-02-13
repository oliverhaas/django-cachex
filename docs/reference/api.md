# API Reference

## Cache Methods

### Standard Django Cache Methods

All standard Django cache methods are supported:

| Method | Description |
|--------|-------------|
| `get(key, default=None)` | Get a value |
| `set(key, value, timeout=DEFAULT)` | Set a value |
| `add(key, value, timeout=DEFAULT)` | Set only if key doesn't exist |
| `delete(key)` | Delete a key |
| `touch(key, timeout=DEFAULT)` | Update timeout on a key |
| `get_many(keys)` | Get multiple values |
| `set_many(mapping, timeout=DEFAULT)` | Set multiple values |
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

django-cachex adds these extended methods:

| Method | Description |
|--------|-------------|
| `ttl(key)` | Get TTL in seconds (`None` = no expiry, `-2` = not found) |
| `pttl(key)` | Get TTL in milliseconds (`None` = no expiry, `-2` = not found) |
| `expire(key, timeout)` | Set expiration in seconds |
| `pexpire(key, timeout)` | Set expiration in milliseconds |
| `expire_at(key, when)` | Set expiration at datetime |
| `pexpire_at(key, when)` | Set expiration at datetime (ms precision) |
| `persist(key)` | Remove expiration |
| `type(key)` | Get the data type of a key |
| `lock(key, ...)` | Get a distributed lock |
| `keys(pattern)` | Get keys matching pattern |
| `iter_keys(pattern)` | Iterate keys matching pattern |
| `scan(cursor, pattern, count)` | Single SCAN iteration |
| `delete_pattern(pattern)` | Delete keys matching pattern |
| `rename(src, dst)` | Rename a key |
| `renamenx(src, dst)` | Rename key only if dest doesn't exist |

### Hash Methods

Hash operations for field-value data structures:

| Method | Description |
|--------|-------------|
| `hset(key, field, value)` | Set a hash field value |
| `hdel(key, *fields)` | Delete hash field(s) |
| `hexists(key, field)` | Check if hash field exists |
| `hget(key, field)` | Get a hash field value |
| `hgetall(key)` | Get all fields and values in a hash |
| `hkeys(key)` | Get all field names in a hash |
| `hincrby(key, field, amount=1)` | Increment hash field by integer |
| `hincrbyfloat(key, field, amount=1.0)` | Increment hash field by float |
| `hlen(key)` | Get number of fields in hash |
| `hmget(key, *fields)` | Get multiple hash field values |
| `hmset(key, mapping)` | Set multiple hash fields |
| `hsetnx(key, field, value)` | Set hash field only if it doesn't exist |
| `hvals(key)` | Get all values in a hash |

### Set Methods

Set operations for unordered collections of unique elements:

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
| `sdiff(*keys)` | Get difference of sets |
| `sdiffstore(dest, *keys)` | Store difference of sets |
| `sinter(*keys)` | Get intersection of sets |
| `sinterstore(dest, *keys)` | Store intersection of sets |
| `sunion(*keys)` | Get union of sets |
| `sunionstore(dest, *keys)` | Store union of sets |
| `sscan(key, cursor=0, ...)` | Incrementally iterate set members |
| `sscan_iter(key, ...)` | Iterate over set members using SSCAN |

### Sorted Set Methods

Sorted set operations for scored, ordered collections:

| Method | Description |
|--------|-------------|
| `zadd(key, mapping, *, nx, xx, ch, gt, lt)` | Add member(s) with scores |
| `zcard(key)` | Get number of members |
| `zcount(key, min, max)` | Count members with scores in range |
| `zincrby(key, amount, member)` | Increment member's score |
| `zrange(key, start, end, ...)` | Get members by index range |
| `zrevrange(key, start, end, ...)` | Get members by index range (descending) |
| `zrangebyscore(key, min, max, ...)` | Get members by score range |
| `zrevrangebyscore(key, max, min, ...)` | Get members by score range (descending) |
| `zrank(key, member)` | Get member's rank (ascending) |
| `zrevrank(key, member)` | Get member's rank (descending) |
| `zrem(key, *members)` | Remove member(s) |
| `zremrangebyrank(key, start, end)` | Remove members by rank range |
| `zremrangebyscore(key, min, max)` | Remove members by score range |
| `zscore(key, member)` | Get member's score |
| `zmscore(key, *members)` | Get multiple members' scores |
| `zpopmin(key, count=1)` | Remove and return members with lowest scores |
| `zpopmax(key, count=1)` | Remove and return members with highest scores |

### List Methods

List operations for ordered, indexable collections:

| Method | Description |
|--------|-------------|
| `llen(key)` | Get list length |
| `lpush(key, *values)` | Prepend value(s) to list |
| `rpush(key, *values)` | Append value(s) to list |
| `lpop(key)` | Remove and return first element |
| `rpop(key)` | Remove and return last element |
| `lindex(key, index)` | Get element by index |
| `lrange(key, start, end)` | Get elements in range |
| `lset(key, index, value)` | Set element at index |
| `ltrim(key, start, end)` | Trim list to range |
| `lrem(key, count, value)` | Remove elements equal to value |
| `lpos(key, value, ...)` | Find element position in list |
| `linsert(key, where, pivot, value)` | Insert value before or after pivot |
| `lmove(src, dst, wherefrom, whereto)` | Atomically move element between lists |
| `blpop(*keys, timeout=0)` | Blocking pop from head of list |
| `brpop(*keys, timeout=0)` | Blocking pop from tail of list |
| `blmove(src, dst, timeout, ...)` | Blocking move between lists |

### Lua Script Methods

Execute Lua scripts with optional key prefixing and value encoding/decoding:

| Method | Description |
|--------|-------------|
| `eval_script(script, *, keys, args, ...)` | Execute a Lua script |
| `aeval_script(script, *, keys, args, ...)` | Execute a Lua script (async) |

#### eval_script / aeval_script

```python
result = cache.eval_script(
    script,           # Lua script source code
    keys=(),          # KEYS to pass to script
    args=(),          # ARGV to pass to script
    pre_hook=None,    # Pre-processing hook: (helpers, keys, args) -> (keys, args)
    post_hook=None,   # Post-processing hook: (helpers, result) -> result
    version=None,     # Key version for prefixing
)
```

#### Pre-built Hooks

| Hook | Description |
|------|-------------|
| `keys_only_pre` | Prefix keys, leave args unchanged |
| `full_encode_pre` | Prefix keys AND encode all args |
| `decode_single_post` | Decode a single returned value |
| `decode_list_post` | Decode a list of returned values |
| `decode_list_or_none_post` | Decode list or return None |
| `noop_post` | Return result unchanged |

#### ScriptHelpers

The helpers object passed to pre/post hooks:

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
cache.set(key, value, timeout=300, nx=False, xx=False)
```

| Parameter | Description |
|-----------|-------------|
| `timeout` | Expiration in seconds (`None` = never, `0` = immediate) |
| `nx` | Only set if key doesn't exist (SETNX) |
| `xx` | Only set if key exists |

## Async Methods

Standard Django cache methods have async versions on the cache object:

```python
# Sync
value = cache.get("key")

# Async
value = await cache.aget("key")
```

- `aadd`, `aget`, `aset`, `adelete`, `atouch`, `aget_many`, `aset_many`, `adelete_many`
- `ahas_key`, `aincr`, `adecr`, `aget_or_set`, `aclear`, `aclose`
- `aincr_version`, `adecr_version`
- `aeval_script`

Extended methods (data structures, TTL, patterns) have async versions on the cache client, accessible via `cache._cache`:

```python
# Sync (on cache object directly)
cache.hset("hash", "field", "value")

# Async (on cache client -- note: client methods use raw/prefixed keys)
key = cache.make_and_validate_key("hash")
await cache._cache.ahset(key, "field", "value")
```

- `attl`, `apttl`, `aexpire`, `apexpire`, `aexpireat`, `apexpireat`, `apersist`
- `akeys`, `aiter_keys`, `adelete_pattern`
- `ahset`, `ahdel`, `ahexists`, `ahget`, `ahgetall`, `ahincrby`, `ahincrbyfloat`, `ahkeys`, `ahlen`, `ahmget`, `ahmset`, `ahsetnx`, `ahvals`
- `asadd`, `asrem`, `asmembers`, `asismember`, `asmismember`, `ascard`, `aspop`, `asrandmember`, `asmove`, `asdiff`, `asdiffstore`, `asinter`, `asinterstore`, `asunion`, `asunionstore`
- `azadd`, `azcard`, `azcount`, `azincrby`, `azrange`, `azrevrange`, `azrangebyscore`, `azrevrangebyscore`, `azrank`, `azrevrank`, `azrem`, `azremrangebyrank`, `azremrangebyscore`, `azscore`, `azmscore`, `azpopmin`, `azpopmax`
- `allen`, `alpush`, `arpush`, `alpop`, `arpop`, `alindex`, `alrange`, `alset`, `altrim`, `alrem`, `alpos`, `almove`, `alinsert`, `ablpop`, `abrpop`, `ablmove`

## Raw Client Access

```python
client = cache.get_client(write=True)
```

| Parameter | Description |
|-----------|-------------|
| `write` | Get write connection for primary (default: `False`) |

Returns the underlying `valkey.Valkey` or `redis.Redis` client instance.

## Lock Interface

```python
lock = cache.lock(key, timeout=None, sleep=0.1, blocking=True, blocking_timeout=None)
```

| Parameter | Description |
|-----------|-------------|
| `key` | Lock name |
| `timeout` | Lock auto-release timeout |
| `sleep` | Time between acquire attempts |
| `blocking` | Wait for lock if held |
| `blocking_timeout` | Max wait time for lock |

Compatible with `threading.Lock`:

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

## Pipelines

Batch multiple operations for efficiency:

```python
pipe = cache.pipeline()
pipe.set("key1", "value1")
pipe.set("key2", "value2")
pipe.hset("hash", "field", "value")
results = pipe.execute()
```

All cache methods are available on the pipeline. Results are returned as a list in the same order as the commands.

## Settings Reference

### Cache OPTIONS

| Option | Description |
|--------|-------------|
| `serializer` | Serializer class or list for fallback |
| `compressor` | Compressor class or list for fallback |
| `password` | Server password |
| `socket_connect_timeout` | Connection timeout |
| `socket_timeout` | Read/write timeout |
| `ignore_exceptions` | Ignore connection errors |
| `log_ignored_exceptions` | Log ignored exceptions |
| `pool_class` | Custom connection pool class |
| `max_connections` | Maximum pool connections |
| `sentinels` | Sentinel server list (for Sentinel backends) |
| `sentinel_kwargs` | Sentinel configuration |
