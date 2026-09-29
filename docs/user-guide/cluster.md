# Valkey/Redis Cluster

For the basic setup, see [Configuration](configuration.md#cluster-configuration).

## Slot Handling

Valkey/Redis Cluster distributes keys across 16,384 hash slots. `get_many()`,
`set_many()`, `delete_many()`, `delete_pattern()`, `keys()` and `clear()` work
across slots. The set, list, hash and sorted-set commands go to the server
unchanged. A multi-key command such as `sdiff`, `sinter`, `sunion` or `lmove`
therefore needs all its keys in one slot.

A cluster has these restrictions, which cover the `a*` twins too:

- `pipeline(transaction=True)` raises `NotSupportedError`, because
  `MULTI`/`EXEC` cannot span slots. Cluster pipelines default to
  `transaction=False`.
- `lock()` raises `NotSupportedError`. Use `semaphore()`, whose keys share a
  `{name}` hash tag.
- `incr_version()` and `decr_version()` raise `NotSupportedError` unless the old
  and new key carry the same `{...}` hash tag. A `KEY_PREFIX` such as `"{app}"`
  satisfies the rule and puts every key in one slot.
- The redis-py and valkey-py cluster pipelines refuse `rename`, `renamenx`,
  `smove`, `sdiff`, `sinter`, `sunion`, `sdiffstore`, `sinterstore` and
  `sunionstore`. Call these commands on the cache, with hash-tagged keys.
- On the redis-py and valkey-py cluster backends, `xautoclaim(justid=True)`
  raises `NotSupportedError`. Call `xautoclaim()` without `justid`.
- The [admin](admin.md#browsing-a-cluster-alias) cannot list keys, because
  cluster `SCAN` returns one cursor per node. Its key detail page still opens a
  key by name.
- The redis-py and valkey-py cluster backends reject `pool_class`,
  `async_pool_class` and `parser_class` in `OPTIONS`.

## Hash Tags

Keys with the same hash tag, the part between `{` and `}`, land in the same
slot:

```python
# Same slot (hash tag is "user:123"), so multi-key commands work
cache.sadd("{user:123}:followers", "alice", "bob")
cache.sadd("{user:123}:following", "charlie")
cache.sdiff(["{user:123}:followers", "{user:123}:following"])
```

!!! warning "Avoid Hot Spots"
    Keep hash tags narrow. A broad tag such as the `{app}` in
    `{app}:user:123` puts all users on one slot.
