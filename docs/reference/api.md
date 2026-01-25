# API Reference

## Cache Methods

### Standard Django Cache Methods

All standard Django cache methods are supported:

| Method | Description |
|--------|-------------|
| `get(key, default=None)` | Get a value |
| `set(key, value, timeout=DEFAULT)` | Set a value |
| `delete(key)` | Delete a key |
| `get_many(keys)` | Get multiple values |
| `set_many(mapping, timeout=DEFAULT)` | Set multiple values |
| `delete_many(keys)` | Delete multiple keys |
| `clear()` | Clear the cache |
| `has_key(key)` | Check if key exists |
| `incr(key, delta=1)` | Increment a value |
| `decr(key, delta=1)` | Decrement a value |
| `close()` | Close connections |

### Extended Methods

django-cachex adds these Redis-specific methods:

| Method | Description |
|--------|-------------|
| `ttl(key)` | Get TTL in seconds |
| `pttl(key)` | Get TTL in milliseconds |
| `expire(key, timeout)` | Set expiration in seconds |
| `pexpire(key, timeout)` | Set expiration in milliseconds |
| `expire_at(key, when)` | Set expiration at datetime |
| `pexpire_at(key, when)` | Set expiration at datetime (ms precision) |
| `persist(key)` | Remove expiration |
| `lock(key, ...)` | Get a distributed lock |
| `keys(pattern)` | Get keys matching pattern |
| `iter_keys(pattern)` | Iterate keys matching pattern |
| `delete_pattern(pattern)` | Delete keys matching pattern |

### Hash Methods

Redis hash operations for field-value data structures:

| Method | Description |
|--------|-------------|
| `hset(key, field, value)` | Set a hash field value |
| `hdel(key, *fields)` | Delete hash field(s) |
| `hexists(key, field)` | Check if hash field exists |
| `hget(key, field)` | Get a hash field value |
| `hgetall(key)` | Get all fields and values in a hash |
| `hincrby(key, field, amount=1)` | Increment hash field by integer |
| `hincrbyfloat(key, field, amount=1.0)` | Increment hash field by float |
| `hlen(key)` | Get number of fields in hash |
| `hmget(key, *fields)` | Get multiple hash field values |
| `hmset(key, mapping)` | Set multiple hash fields |
| `hsetnx(key, field, value)` | Set hash field only if it doesn't exist |
| `hvals(key)` | Get all values in a hash |

### Sorted Set Methods

Redis sorted set operations for scored, ordered collections:

| Method | Description |
|--------|-------------|
| `zadd(key, *args, **kwargs)` | Add member(s) with scores |
| `zcard(key)` | Get number of members |
| `zcount(key, min, max)` | Count members with scores in range |
| `zincrby(key, amount, member)` | Increment member's score |
| `zrange(key, start, end, ...)` | Get members by index range |
| `zrangebyscore(key, min, max, ...)` | Get members by score range |
| `zrank(key, member)` | Get member's rank (ascending) |
| `zrevrank(key, member)` | Get member's rank (descending) |
| `zrem(key, *members)` | Remove member(s) |
| `zremrangebyrank(key, start, end)` | Remove members by rank range |
| `zscore(key, member)` | Get member's score |
| `zmscore(key, *members)` | Get multiple members' scores |

### List Methods

Redis list operations for ordered, indexable collections:

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
| `lpos(key, element, ...)` | Find element position in list |
| `lmove(src, dst, src_side, dst_side)` | Atomically move element between lists |

### Set Method Options

```python
cache.set(key, value, timeout=300, nx=False, xx=False)
```

| Parameter | Description |
|-----------|-------------|
| `timeout` | Expiration in seconds (`None` = never, `0` = immediate) |
| `nx` | Only set if key doesn't exist (SETNX) |
| `xx` | Only set if key exists |

## Helper Functions

### get_redis_connection

```python
from django_cachex import get_redis_connection

conn = get_redis_connection(alias="default", write=True)
```

| Parameter | Description |
|-----------|-------------|
| `alias` | Cache alias from settings (default: `"default"`) |
| `write` | Get write connection for primary (default: `True`) |

Returns the underlying `redis.Redis` client instance.

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

## Settings Reference

### Cache OPTIONS

| Option | Description |
|--------|-------------|
| `CLIENT_CLASS` | Client implementation class |
| `SERIALIZER` | Serializer class |
| `COMPRESSOR` | Compressor class |
| `PASSWORD` | Redis password |
| `SOCKET_CONNECT_TIMEOUT` | Connection timeout |
| `SOCKET_TIMEOUT` | Read/write timeout |
| `IGNORE_EXCEPTIONS` | Ignore connection errors |
| `PICKLE_VERSION` | Pickle protocol version |
| `CONNECTION_POOL_CLASS` | Custom pool class |
| `CONNECTION_POOL_KWARGS` | Pool configuration |
| `CLOSE_CONNECTION` | Close connections on cache close |
| `SENTINELS` | Sentinel server list |
| `SENTINEL_KWARGS` | Sentinel configuration |

### Global Settings

| Setting | Description |
|---------|-------------|
| `DJANGO_REDIS_CONNECTION_FACTORY` | Connection factory class |
| `DJANGO_REDIS_IGNORE_EXCEPTIONS` | Global exception handling |
| `DJANGO_REDIS_LOG_IGNORED_EXCEPTIONS` | Log ignored exceptions |
| `DJANGO_REDIS_LOGGER` | Logger name for exceptions |
| `DJANGO_REDIS_CLOSE_CONNECTION` | Global close behavior |
| `DJANGO_REDIS_SCAN_ITERSIZE` | Default scan batch size |
