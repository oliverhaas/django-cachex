# Advanced Usage

## Serializer

The default serializer is `PickleSerializer` with `pickle.DEFAULT_PROTOCOL`.
To use another one, name it in the `serializer` option:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "OPTIONS": {
            "serializer": "django_cachex.serializers.json.JsonSerializer",
        },
    }
}
```

cachex instantiates a dotted path with no arguments. To set a constructor
option such as the pickle protocol, pass an instance:

```python
from django_cachex.serializers.pickle import PickleSerializer

"OPTIONS": {
    "serializer": PickleSerializer(protocol=5),
}
```

See [Serializers](serializers.md#constructor-options) for the options each serializer takes.

## TTL Operations

### Get TTL

```python
from django.core.cache import cache

cache.set("foo", "value", timeout=25)
cache.ttl("foo")  # Returns 25
cache.ttl("missing")  # Returns -2 (key doesn't exist)
```

`ttl()` returns the seconds until expiry, `None` for a key without expiry (set
with `timeout=None`), and `-2` for a missing or expired key.

### Get TTL in Milliseconds

```python
cache.set("foo", "value", timeout=25)
cache.pttl("foo")  # Returns 25000
```

## Expire & Persist

### Set Expiration

```python
cache.set("foo", "bar", timeout=22)
cache.expire("foo", timeout=5)
cache.ttl("foo")  # Returns 5
```

### Set Expiration in Milliseconds

```python
cache.set("foo", "bar", timeout=22)
cache.pexpire("foo", timeout=5500)
cache.pttl("foo")  # Returns 5500
```

### Expire at Specific Time

```python
from datetime import datetime, timedelta

cache.set("foo", "bar", timeout=22)
cache.expireat("foo", datetime.now() + timedelta(hours=1))
cache.ttl("foo")  # Returns ~3600
```

### Expire at Specific Time in Milliseconds

```python
cache.set("foo", "bar", timeout=22)
cache.pexpireat("foo", datetime.now() + timedelta(milliseconds=900, hours=1))
cache.pttl("foo")  # Returns ~3600900
```

### Remove Expiration

```python
cache.set("foo", "bar", timeout=22)
cache.persist("foo")
cache.ttl("foo")  # Returns None (no expiration)
```

## Locks

`lock()` returns a distributed lock with the `threading.Lock` interface:

```python
from django.core.cache import cache

with cache.lock("somekey"):
    do_some_thing()
```

## Bulk Operations

### Search Keys

```python
from django.core.cache import cache

# Get all matching keys (not recommended for large datasets)
cache.keys("foo_*")  # Returns ["foo_1", "foo_2"]
```

### Iterate Keys

For large datasets, iterate with server-side cursors:

```python
# Returns a generator
for key in cache.iter_keys("foo_*"):
    print(key)
```

### Delete by Pattern

```python
cache.delete_pattern("foo_*")
```

When many keys match, a larger `itersize` needs fewer round trips:

```python
cache.delete_pattern("foo_*", itersize=100_000)
```

The pattern is a case-sensitive Redis glob on every backend. An empty pattern
matches only the empty key, so `delete_pattern("")` deletes at most one key.
`"*"` deletes every key under the cache's prefix and version. See
[Key patterns](../reference/api.md#key-patterns).

## Atomic Operations

### SETNX (Set if Not Exists)

```python
cache.set("key", "value1", nx=True)  # Returns True
cache.set("key", "value2", nx=True)  # Returns False
cache.get("key")  # Returns "value1"
```

### Increment/Decrement

```python
cache.set("counter", 0)
cache.incr("counter")  # Returns 1
cache.incr("counter", delta=5)  # Returns 6
cache.decr("counter")  # Returns 5
```

## Data Structures

### Hashes

```python
from django.core.cache import cache

# Set a single field
cache.hset("user:1", "name", "Alice")

# Set multiple fields at once
cache.hset("user:1", mapping={"email": "alice@example.com", "age": 30})

# Get a single field
name = cache.hget("user:1", "name")  # "Alice"

# Get multiple fields
values = cache.hmget("user:1", "name", "email")  # ["Alice", "alice@example.com"]

# Get all fields and values
user = cache.hgetall("user:1")  # {"name": "Alice", "email": "...", "age": 30}

# Increment a numeric field
cache.hincrby("user:1", "age", 1)  # 31
cache.hincrbyfloat("user:1", "score", 0.5)  # For floating point

# Check if field exists
cache.hexists("user:1", "name")  # True

# Delete fields
cache.hdel("user:1", "age")

# Get count of fields
cache.hlen("user:1")  # 3

# Get all values
cache.hvals("user:1")  # ["Alice", "alice@example.com", 0.5]
```

#### Field Expiration

Hash fields can have their own TTL. This needs Redis 7.4+ or Valkey 9.0+, and
`hsetex` and `hgetex` need Redis 8.0+ or Valkey 9.0+. An older server raises
`NotSupportedError`.

```python
from datetime import datetime, timedelta

cache.hset("session:42", mapping={"token": "abc", "csrf": "xyz", "theme": "dark"})

# Expire fields; one reply code per field
cache.hexpire("session:42", 300, "token", "csrf")  # [1, 1]
cache.httl("session:42", "token", "theme", "nope")  # [300, None, -2]

# Only lengthen an existing TTL (nx/xx/gt/lt mirror EXPIRE's options)
cache.hexpire("session:42", timedelta(hours=1), "token", gt=True)  # [1]

# Absolute deadlines and millisecond precision
cache.hexpireat("session:42", datetime.now() + timedelta(days=1), "csrf")
cache.hpexpire("session:42", 1500, "theme")
cache.hpttl("session:42", "theme")  # [1500]

# Set fields and their TTL in one round trip; fnx/fxx guard the write
cache.hsetex("session:42", "token", "def", timeout=300)  # True
cache.hsetex("session:42", mapping={"a": 1, "b": 2}, timeout=60, fnx=True)  # False if any exist
cache.hsetex("session:42", "token", "ghi", keepttl=True)  # rewrite, keep its TTL

# Read fields and refresh (or drop) their TTL in one round trip
cache.hgetex("session:42", "token", timeout=600)  # ["ghi"]
cache.hgetex("session:42", "token", persist=True)  # ["ghi"], TTL removed

cache.hpersist("session:42", "csrf")  # [1]
```

Rewriting a field with `hset`, or with `hsetex` without `keepttl=True`, clears
that field's TTL. `hsetex()` treats `timeout` the way `set()` does. The default
uses the backend's `TIMEOUT`, `None` means no expiry, and `timeout=0` deletes
the fields immediately. Stampede prevention pads key-level timeouts only and
sends field TTLs as given.

### Sorted Sets

```python
from django.core.cache import cache

# Add members with scores
cache.zadd("leaderboard", {"alice": 100, "bob": 85, "charlie": 92})

# Get rank (0-indexed, ascending by score)
cache.zrank("leaderboard", "alice")  # 2 (highest score = last)
cache.zrevrank("leaderboard", "alice")  # 0 (highest score = first)

# Get score
cache.zscore("leaderboard", "bob")  # 85.0

# Get multiple scores
cache.zmscore("leaderboard", "alice", "bob")  # [100.0, 85.0]

# Increment score
cache.zincrby("leaderboard", 10, "bob")  # 95.0

# Get range by rank (ascending)
cache.zrange("leaderboard", 0, -1)  # All members sorted by score

# Get range by rank with scores
cache.zrange("leaderboard", 0, -1, withscores=True)

# Get range by score
cache.zrangebyscore("leaderboard", 80, 100)

# Count members in score range
cache.zcount("leaderboard", 80, 100)  # 3

# Remove members
cache.zrem("leaderboard", "charlie")

# Remove by rank range
cache.zremrangebyrank("leaderboard", 0, 1)  # Remove lowest 2

# Get total count
cache.zcard("leaderboard")
```

### Lists

```python
from django.core.cache import cache

# Push elements
cache.lpush("queue", "first")  # Prepend (left)
cache.rpush("queue", "last")  # Append (right)

# Pop elements
cache.lpop("queue")  # Remove and return first
cache.rpop("queue")  # Remove and return last

# Get element by index
cache.lindex("queue", 0)  # First element

# Get range of elements
cache.lrange("queue", 0, -1)  # All elements

# Set element at index
cache.lset("queue", 0, "new_first")

# Trim to range
cache.ltrim("queue", 0, 99)  # Keep first 100 elements

# Get length
cache.llen("queue")

# Find element position
cache.lpos("queue", "target")  # Returns index or None

# Move element between lists atomically
cache.lmove("source", "dest", "LEFT", "RIGHT")  # LPOP source, RPUSH dest
```

## Raw Client Access

`get_client()` returns the underlying valkey-py or redis-py client:

```python
client = cache.get_client()
client.publish("channel", "message")
```

## Lua Scripts

`eval_script()` runs a Lua script and sends its keys and args as given. A
`pre_hook` such as `keys_only_pre` adds the cache's key prefix and version, and
the [hooks](#prepost-processing-hooks) also encode and decode values.

### Basic Usage

```python
from django.core.cache import cache
from django_cachex import keys_only_pre

# Simple script
result = cache.eval_script("return 42")

# With keys and args; keys_only_pre applies the cache's key prefix and version
count = cache.eval_script(
    "return redis.call('INCR', KEYS[1])",
    keys=["counter"],
    args=[],
    pre_hook=keys_only_pre,
)
```

`eval_script()` sends the script's SHA-1 digest with `EVALSHA`. After a
`NOSCRIPT` reply, it loads the script with `SCRIPT LOAD` and retries. Each
server receives the full source once per script, and later calls send only the
40-byte digest. A pipeline sends the full source with `EVAL`, because it cannot
retry a `NOSCRIPT` reply.

### Pre/Post Processing Hooks

`pre_hook` transforms the keys and args before the script runs, and
`post_hook` transforms the result.

#### Built-in Helpers

```python
from django_cachex import (
    Encoded,  # Marks an ARGV entry for encoded_pre
    encoded_pre,  # Prefix keys, encode only the args wrapped in Encoded(...)
    keys_only_pre,  # Prefix keys, leave args unchanged
    full_encode_pre,  # Prefix keys AND encode args (serialize values)
    decode_single_post,  # Decode a single returned value
    decode_list_post,  # Decode a list of returned values
)
```

With `post_hook=None`, the default, the result comes back unchanged.

#### Mixed Values and Scalars

A script's ARGV often mixes two kinds of arguments. Values that a later `get()`
reads back must go through the serializer and compressor. Scalars that Lua
consumes with `tonumber` or a string compare must not. Wrap the values in
`Encoded` and use `encoded_pre`:

```python
from django_cachex import Encoded, encoded_pre

SETEXPIRE = """
local value = ARGV[1]
local ex    = tonumber(ARGV[2])
local nx    = ARGV[3] == "1"
if nx and redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
redis.call('SET', KEYS[1], value, 'EX', ex)
return 1
"""

cache.eval_script(
    SETEXPIRE,
    keys=["session:abc"],
    args=[Encoded({"user_id": 123}), 300, "1"],
    pre_hook=encoded_pre,
)
```

`Encoded` works at any position, for example in a variadic tail such as
`[str(score), "0", *map(Encoded, members)]` or in alternating field/value pairs
such as `[field, Encoded(value), ...]`. With nothing wrapped, `encoded_pre` behaves
like `keys_only_pre`, and with everything wrapped, like `full_encode_pre`.

An `Encoded` in `args` that no `pre_hook` unwraps raises `TypeError` before
anything reaches the server. So do an `Encoded` in `keys` and
`Encoded(Encoded(...))`.

#### Key Prefixing

```python
from django_cachex import keys_only_pre

count = cache.eval_script(
    """
    local current = redis.call('INCR', KEYS[1])
    if current == 1 then
        redis.call('EXPIRE', KEYS[1], ARGV[1])
    end
    return current
    """,
    keys=["user:123:requests"],
    args=[60],
    pre_hook=keys_only_pre,
)
```

#### Encoding Values

```python
from django_cachex import full_encode_pre, decode_single_post

# Works with any serializable Python object
old_session = cache.eval_script(
    """
    local old = redis.call('GET', KEYS[1])
    redis.call('SET', KEYS[1], ARGV[1])
    return old
    """,
    keys=["session:abc"],
    args=[{"user_id": 123, "permissions": ["read", "write"]}],
    pre_hook=full_encode_pre,
    post_hook=decode_single_post,
)
```

### Custom Processing Hooks

Custom hooks receive a `ScriptHelpers` instance:

```python
from django_cachex import ScriptHelpers


def my_pre(helpers: ScriptHelpers, keys, args):
    # First arg is a secondary key, rest are values
    processed_args = [helpers.make_key(args[0], helpers.version)]
    processed_args.extend(helpers.encode_values(args[1:]))
    return helpers.make_keys(keys), processed_args


def my_post(helpers: ScriptHelpers, result):
    # Result is [count, list_of_values]
    return {
        "count": result[0],
        "values": helpers.decode_values(result[1]) if result[1] else [],
    }


result = cache.eval_script(
    "...",
    keys=["primary"],
    args=["secondary", value_a, value_b],
    pre_hook=my_pre,
    post_hook=my_post,
)
```

### Pipeline Support

Scripts can be queued in pipelines:

```python
from django_cachex import keys_only_pre

pipe = cache.pipeline()
pipe.set("key1", "value1")
pipe.eval_script(
    "return redis.call('INCR', KEYS[1])",
    keys=["user:1"],
    pre_hook=keys_only_pre,
)
results = pipe.execute()  # [True, 1]
```

### Async Support

Use `aeval_script()` for async execution:

```python
from django_cachex import keys_only_pre

count = await cache.aeval_script(
    """
    local current = redis.call('INCR', KEYS[1])
    if current == 1 then
        redis.call('EXPIRE', KEYS[1], ARGV[1])
    end
    return current
    """,
    keys=["user:123:requests"],
    args=[60],
    pre_hook=keys_only_pre,
)
```
