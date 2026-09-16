# Valkey/Redis Cluster

For basic cluster setup, see [Configuration](configuration.md#cluster-configuration).

## Slot Handling

Valkey/Redis Cluster distributes keys across 16,384 hash slots. django-cachex handles Django cache methods and direct commands differently:

The Django cache methods (`get_many`, `set_many`, `delete_many`, `keys`, `clear`) are cluster-aware and handle cross-slot operations automatically. `delete_many()`, `delete_pattern()` and `set_many(timeout=0)` issue one `UNLINK` per batch and let the driver split it by slot.

Direct commands (sets, lists, hashes, sorted sets) pass through to the server. Multi-key commands (`sdiff`, `sinter`, `sunion`, `lmove`) require all keys on the same slot.

Not available on cluster:

- `pipeline(transaction=True)` and `apipeline(transaction=True)` raise `NotSupportedError`; cluster pipelines default to `transaction=False`, since `MULTI`/`EXEC` cannot span slots.
- `lock()` and `alock()` raise `NotSupportedError`; use `semaphore()`, whose keys share a `{name}` hash tag.
- `incr_version()` and `decr_version()` rename `prefix:V:key` to `prefix:V+1:key`, which only stays in one slot when both made keys carry the same `{...}` hash tag. Without one (or with a `KEY_FUNCTION` that puts the version inside the tag) they raise `NotSupportedError` up front instead of a `CROSSSLOT` error from the server. A `KEY_PREFIX` of the form `"{app}"` colocates every key it produces and satisfies the rule.
- `xautoclaim(justid=True)` and `axautoclaim(justid=True)` raise `NotSupportedError` on the redis-py and valkey-py cluster backends: the driver's `JUSTID` reply drops the cursor. Call `xautoclaim()` without `justid`. The valkey-glide cluster backend is unaffected.
- Key browsing in the [admin](admin.md#browsing-a-cluster-alias): cluster `SCAN` returns one cursor per node, so the key list stays empty with a message. The key detail page still opens a key by name.
- `pool_class`, `async_pool_class` and `parser_class` in `OPTIONS` raise `ImproperlyConfigured`; the cluster client owns its per-node pools and picks its own parser.

## Hash Tags

Force keys to the same slot using hash tags (the substring between `{` and `}`):

```python
# Same slot (hash tag is "user:123")
cache.sadd("{user:123}:followers", "alice", "bob")
cache.sadd("{user:123}:following", "charlie")

# Multi-key operations now work
cache.sdiff(["{user:123}:followers", "{user:123}:following"])
```

Use hash tags when you need:

- Multi-key set operations: `sdiff`, `sinter`, `sunion`
- List moves: `lmove`
- `incr_version` / `decr_version` (see above)

```python
# Group related keys: one hash tag per slot
user_keys = ["{user:123}:profile", "{user:123}:settings"]
order_keys = ["{order:456}:items", "{order:456}:status"]
```

!!! warning "Avoid Hot Spots"
    Don't use overly broad hash tags like `{app}:user:123` which puts all users on one slot.
