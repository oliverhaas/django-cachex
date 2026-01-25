# Valkey/Redis Cluster

django-cachex includes built-in support for [Valkey Cluster](https://valkey.io/topics/cluster-tutorial/) and [Redis Cluster](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/) with server-side sharding across multiple nodes.

## Basic Setup

Use `ValkeyClusterCacheClient` (or `RedisClusterCacheClient` for Redis):

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.client.ValkeyClusterCacheClient",
        "LOCATION": "valkey://127.0.0.1:7000",  # Any cluster node
    }
}
```

## Full Configuration Example

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.client.ValkeyClusterCacheClient",
        "LOCATION": "valkey://127.0.0.1:7000",
        "OPTIONS": {
            # Connection options (passed to ValkeyCluster)
            "socket_timeout": 5,
            "socket_connect_timeout": 3,
        }
    }
}
```

For Redis Cluster:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.client.RedisClusterCacheClient",
        "LOCATION": "redis://127.0.0.1:7000",
    }
}
```

## Understanding Cluster Slot Handling

Valkey/Redis Cluster distributes keys across 16384 hash slots. When using multi-key operations, the server requires all keys to be on the same slot for atomicity. django-cachex handles this automatically for standard Django cache operations, but you need to understand the distinction between two types of methods:

### Django Cache Interface Methods (Automatic Handling)

These methods are part of the standard Django cache interface and are **cluster-aware**. They automatically handle cross-slot operations by grouping keys appropriately or querying all nodes:

| Method | Cluster Behavior |
|--------|-----------------|
| `get_many()` | Automatically splits keys by slot using `mget_nonatomic` |
| `set_many()` | Automatically splits keys by slot using `mset_nonatomic` |
| `delete_many()` | Groups keys by slot and performs multiple DEL operations |
| `keys()` | Queries all primary nodes with `target_nodes=PRIMARIES` |
| `iter_keys()` | Scans all primary nodes with `target_nodes=PRIMARIES` |
| `delete_pattern()` | Scans all primaries and groups deletions by slot |
| `clear()` | Flushes all primary nodes in the cluster |

!!! info "Non-Atomic Operations"
    Operations like `get_many` and `set_many` use valkey-py/redis-py's `_nonatomic` variants which split keys across slots. This means these operations are **not atomic** across the entire key set, but each slot group is processed atomically.

### Direct Method Wrappers (Pass-Through)

Methods from the data structure mixins (sets, lists, hashes, sorted sets) are **direct wrappers** around commands. They pass through to the server without special cluster handling:

| Category | Methods |
|----------|---------|
| **Sets** | `sadd`, `srem`, `smembers`, `sismember`, `scard`, `sdiff`, `sinter`, `sunion`, `smove`, etc. |
| **Lists** | `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `lindex`, `llen`, `lmove`, etc. |
| **Hashes** | `hset`, `hget`, `hmset`, `hmget`, `hdel`, `hgetall`, `hkeys`, `hvals`, etc. |
| **Sorted Sets** | `zadd`, `zrem`, `zrange`, `zscore`, `zcard`, `zrangebyscore`, etc. |

!!! warning "Multi-Key Commands Require Same Slot"
    Multi-key commands like `sdiff`, `sinter`, `sunion`, `smove`, and `lmove` require **all keys to be on the same slot**. Use hash tags to ensure this (see below).

## Hash Tags for Slot Co-location

Valkey/Redis Cluster uses hash tags to force keys to the same slot. A hash tag is the substring between the first `{` and the following `}` in a key:

```python
# These keys will be on the SAME slot (hash tag is "user:123")
cache.sadd("{user:123}:followers", "alice", "bob")
cache.sadd("{user:123}:following", "charlie")

# Now multi-key operations work
followers_not_following = cache.sdiff(
    "{user:123}:followers",
    "{user:123}:following"
)
```

### When to Use Hash Tags

Use hash tags when you need to:

1. **Perform set operations across keys**: `sdiff`, `sinter`, `sunion`, `smove`
2. **Move items between lists**: `lmove`
3. **Use transactions (MULTI/EXEC)** across multiple keys
4. **Ensure atomicity** for related keys

```python
# Shopping cart example - all cart data on same slot
cart_key = "{cart:user42}:items"
cart_meta = "{cart:user42}:metadata"

cache.sadd(cart_key, "item1", "item2")
cache.hset(cart_meta, "total", "29.99")
```

### Hash Tag Best Practices

!!! tip "Keep Hash Tags Consistent"
    Use a logical grouping for your hash tags that matches your access patterns.

```python
# Good: Group by user
"{user:123}:profile"
"{user:123}:settings"
"{user:123}:sessions"

# Good: Group by entity
"{order:456}:items"
"{order:456}:status"

# Avoid: Overly broad tags that create hot spots
"{app}:user:123"  # All users on one slot!
```

## Example: Set Operations in Cluster

```python
from django.core.cache import cache

# Without hash tags - keys may be on different slots
# These individual operations work fine:
cache.sadd("set1", "a", "b", "c")
cache.sadd("set2", "b", "c", "d")

# But this will FAIL with CROSSSLOT error:
# cache.sinter("set1", "set2")  # Error!

# With hash tags - keys are guaranteed on same slot:
cache.sadd("{mysets}:set1", "a", "b", "c")
cache.sadd("{mysets}:set2", "b", "c", "d")

# Now this works:
common = cache.sinter("{mysets}:set1", "{mysets}:set2")
# Returns: {"b", "c"}
```

## Example: List Operations in Cluster

```python
from django.core.cache import cache

# Moving items between lists requires same slot
cache.rpush("{queue}:pending", "task1", "task2", "task3")
cache.rpush("{queue}:processing", "task0")

# Move task from pending to processing
task = cache.lmove("{queue}:pending", "{queue}:processing", "LEFT", "RIGHT")
# Returns: "task1"
```

## Design Philosophy

The cluster client follows these principles:

1. **Django cache interface should "just work"**: Methods like `get_many()`, `set_many()`, `clear()`, etc. should work without users needing to think about cluster topology or slot distribution.

2. **Method wrappers stay true to the protocol**: Direct commands (set operations, list operations, etc.) behave exactly as they would with a regular client. This means no hidden magic, but also no hidden safety nets.

3. **Explicit over implicit for advanced operations**: When using Valkey/Redis-specific features in cluster mode, you're expected to understand cluster constraints and use hash tags appropriately.

This design ensures that:

- Simple Django caching works seamlessly with clusters
- Power users get full functionality without unexpected behavior
- There's no confusion about which methods handle slots and which don't
