# Valkey/Redis Cluster

For the basic setup, see [Configuration](configuration.md#cluster-configuration).

## Slot Handling

Valkey/Redis Cluster distributes keys across 16,384 hash slots. The cache
methods `get_many()`, `set_many()`, `delete_many()`, `keys()` and `clear()`
work across slots. `delete_many()`, `delete_pattern()` and
`set_many(timeout=0)` send one `UNLINK` per batch, and the driver splits it by
slot.

The set, list, hash and sorted-set commands go to the server unchanged. A
multi-key command such as `sdiff`, `sinter`, `sunion` or `lmove` therefore
needs all its keys in one slot.

Not available on a cluster:

- `pipeline(transaction=True)` and `apipeline(transaction=True)` raise
  `NotSupportedError`, because `MULTI`/`EXEC` cannot span slots. Cluster
  pipelines default to `transaction=False`.
- `lock()` and `alock()` raise `NotSupportedError`. Use `semaphore()`, whose
  keys share a `{name}` hash tag.
- `incr_version()` and `decr_version()` rename `prefix:V:key` to
  `prefix:V+1:key`, and both keys stay in one slot only when they carry the
  same `{...}` hash tag. Without one, or with a `KEY_FUNCTION` that puts the
  version inside the tag, they raise `NotSupportedError` before the server can
  reply with `CROSSSLOT`. A `KEY_PREFIX` of the form `"{app}"` puts every key
  it produces in one slot and satisfies the rule.
- On the redis-py and valkey-py cluster backends, the driver's cluster
  pipeline refuses `rename`, `renamenx`, `smove`, `sdiff`, `sinter`, `sunion`,
  `sdiffstore`, `sinterstore` and `sunionstore`. Queueing one raises the
  driver's `RedisClusterException` or `ValkeyClusterException`. Call these
  commands on the cache instead, with hash-tagged keys.
- On the redis-py and valkey-py cluster backends, `xautoclaim(justid=True)`
  and `axautoclaim(justid=True)` raise `NotSupportedError`, because the
  driver's `JUSTID` reply drops the cursor. Call `xautoclaim()` without
  `justid`. The valkey-glide cluster backend is unaffected.
- Key browsing in the [admin](admin.md#browsing-a-cluster-alias). Cluster
  `SCAN` returns one cursor per node, so the key list stays empty and shows a
  message. The key detail page still opens a key by name.
- `pool_class`, `async_pool_class` and `parser_class` in `OPTIONS` raise
  `ImproperlyConfigured`. The cluster client owns its per-node pools and picks
  its own parser.

## Hash Tags

Keys with the same hash tag, the part between `{` and `}`, land in the same
slot:

```python
# Same slot (hash tag is "user:123")
cache.sadd("{user:123}:followers", "alice", "bob")
cache.sadd("{user:123}:following", "charlie")

# Multi-key operations work
cache.sdiff(["{user:123}:followers", "{user:123}:following"])
```

The multi-key set commands (`sdiff`, `sinter`, `sunion`), `lmove`,
`incr_version` and `decr_version` need hash tags.

```python
# Group related keys: one hash tag per slot
user_keys = ["{user:123}:profile", "{user:123}:settings"]
order_keys = ["{order:456}:items", "{order:456}:status"]
```

!!! warning "Avoid Hot Spots"
    Keep hash tags narrow. A broad tag such as the `{app}` in
    `{app}:user:123` puts all users on one slot.
