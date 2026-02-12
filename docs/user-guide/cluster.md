# Valkey/Redis Cluster

For basic cluster setup, see [Configuration](configuration.md#cluster-configuration).

## Slot Handling

Valkey/Redis Cluster distributes keys across 16,384 hash slots. django-cachex handles Django cache methods and direct commands differently:

**Django cache methods** (`get_many`, `set_many`, `delete_many`, `keys`, `clear`) are cluster-aware and handle cross-slot operations automatically.

**Direct commands** (sets, lists, hashes, sorted sets) pass through to the server. Multi-key commands (`sdiff`, `sinter`, `sunion`, `lmove`) require all keys on the same slot.

## Hash Tags

Force keys to the same slot using hash tags (the substring between `{` and `}`):

```python
# Same slot (hash tag is "user:123")
cache.sadd("{user:123}:followers", "alice", "bob")
cache.sadd("{user:123}:following", "charlie")

# Multi-key operations now work
cache.sdiff("{user:123}:followers", "{user:123}:following")
```

Use hash tags when you need:

- Multi-key set operations: `sdiff`, `sinter`, `sunion`
- List moves: `lmove`
- Transactions across multiple keys

```python
# Group related keys
"{user:123}:profile"
"{user:123}:settings"
"{order:456}:items"
"{order:456}:status"
```

!!! warning "Avoid Hot Spots"
    Don't use overly broad hash tags like `{app}:user:123` which puts all users on one slot.
