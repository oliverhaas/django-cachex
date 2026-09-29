# TrackingCache

`TrackingCache` keeps local copies of the values it reads from another alias in `CACHES`, the transport. Reads check the in-process store first, and writes go to the transport and evict the local copy. A listener thread per process receives the server's `CLIENT TRACKING` invalidations, so a write from any client evicts the local copies in every process.

```python
CACHES = {
    "redis": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/0",
        "KEY_PREFIX": "app",
    },
    "default": {
        "BACKEND": "django_cachex.cache.TrackingCache",
        "OPTIONS": {
            "transport": "redis",  # a redis-py or valkey-py alias, standalone or Sentinel
            "coherence": "tracking",  # or "ttl", see below
            "MAX_ENTRIES": 1000,  # size of the local LRU store
            "local_timeout": None,  # longest time in seconds a value stays local
            "prefixes": None,  # tracked key prefixes, derived from the transport by default
            "poll_timeout": 1.0,  # seconds the listener blocks before it checks for shutdown
            "health_check_interval": 15.0,
            "reconnect_delay": 1.0,
        },
    },
}
```

Set `KEY_PREFIX`, `KEY_FUNCTION`, `VERSION` and `TIMEOUT` on the transport. The `TrackingCache` alias rejects them.

## Coherence

- A write from another client evicts the local copy one network round trip after the server applies it.
- A local copy expires with the key's TTL on the server, and `local_timeout` shortens that further.
- `FLUSHDB`, `FLUSHALL`, `clear()` and a lost listener connection empty the local store. While the listener is disconnected, every read goes to the transport.
- The listener pings the server every `health_check_interval` seconds. On a Sentinel transport it also checks that the primary has not moved. A failed check empties the local store. `TrackingCache` thus serves a stale copy for at most `health_check_interval + poll_timeout` seconds, plus 5 seconds for the ping reply.
- The transport's stampede prevention also applies to local hits.

With `"coherence": "ttl"`, no listener runs and a write from another client stays invisible until the local copy expires, so this mode requires `local_timeout`. This mode needs no extra connection and works with every transport, cluster and valkey-glide included.

## Tracked prefixes

The server sends invalidations per key prefix. With Django's default `KEY_FUNCTION`, `TrackingCache` tracks the transport's `KEY_PREFIX` followed by a colon. With a custom `KEY_FUNCTION`, it tracks every key in the database. `OPTIONS["prefixes"]` overrides both. The prefixes must not overlap and must cover every key.

## Supported operations

`TrackingCache` supports the standard Django cache API and the `nx`, `xx` and `get` flags of `set()`. It passes `keys`, `iter_keys`, `scan`, `ttl`, `pttl`, `type`, `expire`, `persist`, `delete_pattern`, `memory_usage`, `largest_keys`, `slowlog_get` and `slowlog_len` to the transport, and each has its async twin. `delete_pattern()`, `incr_version()` and `decr_version()` also evict the matching local copies. Other methods, such as the hash, list, set, sorted-set and stream commands, `lock()` and `pipeline()`, raise `NotSupportedError`; call them on the transport alias.

`info()` adds a `tracking` section with the listener state, the store size and the hit, miss, invalidation and flush counters. The admin lists a `TrackingCache` alias as limited, without key browsing. Browse its keys through the transport alias.

## Operation

- Tracking coherence needs a redis-py or valkey-py transport, standalone or Sentinel.
- Each process opens one extra RESP3 connection for the listener, outside the transport's pool. The first cache operation opens it, and the listener thread retries a failed connect in the background.
- Aliases with the same `LOCATION` (by default the transport alias) share one local store and listener, and must have the same `OPTIONS`.
- `close()` leaves the listener running between requests, and `shutdown()` stops it.
- A local miss costs one pipelined `GET` and `PTTL`. `get_many()` fetches all missing keys in one pipeline.
