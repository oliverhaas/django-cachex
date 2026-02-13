# Advanced Usage

## Serializer

By default, `PickleSerializer` is used with `pickle.DEFAULT_PROTOCOL`. To use a different serializer or configure pickle options, use the `serializer` option:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "OPTIONS": {
            "serializer": "django_cachex.serializers.json.JSONSerializer",
        }
    }
}
```

## TTL Operations

### Get TTL

```python
from django.core.cache import cache

cache.set("foo", "value", timeout=25)
cache.ttl("foo")      # Returns 25
cache.ttl("missing")  # Returns 0 (key doesn't exist)
```

Returns:

- `0` - Key doesn't exist or already expired
- `None` - Key exists but has no expiration
- `int` - Seconds until expiration

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
cache.expire_at("foo", datetime.now() + timedelta(hours=1))
cache.ttl("foo")  # Returns ~3600
```

### Expire at Specific Time (milliseconds precision)

```python
cache.set("foo", "bar", timeout=22)
cache.pexpire_at("foo", datetime.now() + timedelta(milliseconds=900, hours=1))
cache.pttl("foo")  # Returns ~3600900
```

### Remove Expiration

```python
cache.set("foo", "bar", timeout=22)
cache.persist("foo")
cache.ttl("foo")  # Returns None (no expiration)
```

## Locks

Distributed locks with the `threading.Lock` interface:

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

### Iterate Keys (Recommended)

For large datasets, use server-side cursors:

```python
# Returns a generator
for key in cache.iter_keys("foo_*"):
    print(key)
```

### Delete by Pattern

```python
cache.delete_pattern("foo_*")
```

For better performance with many keys:

```python
cache.delete_pattern("foo_*", itersize=100_000)
```

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

Direct access to Valkey/Redis data structures through the cache interface.

### Hashes

Maps of field-value pairs:

```python
from django.core.cache import cache

# Set a single field
cache.hset("user:1", "name", "Alice")

# Set multiple fields at once
cache.hmset("user:1", {"email": "alice@example.com", "age": 30})

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
cache.hlen("user:1")  # 2

# Get all values
cache.hvals("user:1")  # ["Alice", "alice@example.com"]
```

### Sorted Sets

Unique members with scores, automatically sorted:

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

Ordered collections of elements:

```python
from django.core.cache import cache

# Push elements
cache.lpush("queue", "first")  # Prepend (left)
cache.rpush("queue", "last")   # Append (right)

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

Access the underlying valkey-py/redis-py client directly:

```python
client = cache.get_client()
client.publish("channel", "message")
```

## Lua Scripts

Execute Lua scripts with automatic key prefixing and value encoding/decoding.

### Basic Usage

```python
from django.core.cache import cache

# Simple script
result = cache.eval_script("return 42")

# With keys and args
count = cache.eval_script(
    "return redis.call('INCR', KEYS[1])",
    keys=["counter"],
    args=[],
)
```

### Pre/Post Processing Hooks

Scripts support `pre_hook` (transform keys/args before execution) and `post_hook` (transform results after execution).

#### Built-in Helpers

```python
from django_cachex import (
    keys_only_pre,      # Prefix keys, leave args unchanged
    full_encode_pre,    # Prefix keys AND encode args (serialize values)
    decode_single_post, # Decode a single returned value
    decode_list_post,   # Decode a list of returned values
    noop_post,          # Return result unchanged
)
```

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

Create custom hooks with `ScriptHelpers`:

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

result = cache.eval_script("...", pre_hook=my_pre, post_hook=my_post)
```

### Pipeline Support

Scripts can be queued in pipelines:

```python
from django_cachex import keys_only_pre

with cache.pipeline() as pipe:
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
