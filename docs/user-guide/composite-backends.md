# Composite Backends

Two backend classes don't talk to a server directly. They compose other entries in your `CACHES` setting.

| Backend | Reads served from | Consistency | Best for |
|---------|-------------------|-------------|----------|
| `StreamCache` | Local in-memory dict | Eventually consistent (last-writer-wins) | Read-heavy data shared across pods (config, feature flags) |
| `TrackingCache` | Local store, falling through to the transport | Coherent within one round trip (server-pushed invalidations), or bounded staleness with `coherence: "ttl"` | Hot reads where the transport round trip dominates |

## StreamCache

In-memory store with cross-pod synchronization via a Redis or Valkey stream.

Each pod keeps a local dict (inherited from `LocMemCache`). Writes update the local dict and publish to a shared `XADD` stream. A daemon thread on each pod consumes the stream via `XREAD BLOCK` and applies changes from other pods.

```python
CACHES = {
    "redis": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/0",
    },
    "default": {
        "BACKEND": "django_cachex.cache.StreamCache",
        "OPTIONS": {
            "transport": "redis",  # alias of any cachex Valkey/Redis backend
            "stream_key": "cache:sync",
            "maxlen": 10000,  # approximate trim
            "block_timeout": 1000,  # XREAD BLOCK timeout, ms
            "replay": 0,  # entries to replay on startup; 0 disables
            "max_pending_publishes": 1000,  # publish backlog cap; excess is dropped with a warning
            "publish_shutdown_timeout": 5.0,  # seconds to wait for queued publishes on shutdown
        },
    },
}
```

### What's not supported

- `add()`, `incr()`, `decr()` raise `NotSupportedError`. Their semantics (atomic check-and-set, atomic increment) can't be honoured under eventual consistency. Use the transport cache directly when you need them.
- `set()` with `nx=True`, `xx=True` or `get=True` raises `NotSupportedError` for the same reason. Plain `set()` works.

### Convergence

Every pod applies stream entries in stream order, its own included, and each entry carries the final value rather than a delta. Two pods that write the same key inside the propagation window therefore both end on whichever entry the stream ordered last, instead of each ending up holding the other's value. A pod skips one of its own entries where a later local write to that key (or a local `clear()`) has already replaced it, and where the broadcast for that later write was dropped and the older entry would otherwise undo it. Either way a writer never reads back a value it has moved past.

Broadcasts stay best-effort: one is dropped when the publish backlog is full, when the publisher has been shut down, or when the `XADD` errors. The write still applies locally and stays readable on the pod that made it; the other pods keep the value they last saw until the next write to that key or its expiry. The stream is a replication feed, not a durable log.

### Operational notes

- All pods sharing a `stream_key` must use the same transport `BACKEND` and `OPTIONS` so their serializer/compressor agree on the wire format.
- The consumer thread, the publisher thread and the pod identity are shared per `LOCATION` within a process, not per backend instance. Django hands out one cache instance per thread and per async context, so per-instance state would mean one consumer per ASGI request. Two `StreamCache` aliases sharing a `stream_key` but not a `LOCATION` act as two independent pods, which is how the test suite simulates a cluster in one process.
- The consumer thread is restarted automatically if it dies; check `info()["sync"]` for consumer health, last-read age, and stream position.
- On a valkey-glide transport the consumer polls instead of blocking: glide carries every command of a client over one connection, so a parked `XREAD BLOCK` would hold up each publish behind it. `block_timeout` is ignored there and the poll runs every 25 ms.
- Set `replay` above 0 (up to `maxlen`) so a restarting pod replays the last N mutations and doesn't start with an empty cache.
- Publishes are queued to a background thread; when more than `max_pending_publishes` are outstanding, new publishes are dropped with a warning instead of blocking the caller. A dropped publish costs the other pods that one update, not the local write: this pod keeps the value it wrote.

## TrackingCache

Local read cache over an existing Redis or Valkey alias, kept coherent by the server's `CLIENT TRACKING` broadcast mode.

Reads are served from a bounded in-process store and fall through to the transport on a miss. Writes go to the transport and evict the local copy. One listener thread per process receives the server's invalidation messages for the transport's key prefix, so a write by any client of that database evicts the local copies everywhere. Nothing is cached while the listener is disconnected.

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
            "transport": "redis",  # alias of a redis-py or valkey-py backend (standalone or Sentinel)
            "coherence": "tracking",  # "ttl" runs no listener; see below
            "MAX_ENTRIES": 1000,  # local store bound, LRU
            "local_timeout": None,  # extra cap on how long a value stays local, in seconds
            "prefixes": None,  # tracked key prefixes; derived from the transport by default
            "poll_timeout": 1.0,  # seconds the listener waits for a message before checking for shutdown
            "health_check_interval": 15.0,  # wall-clock seconds between listener pings
            "reconnect_delay": 1.0,  # seconds between reconnection attempts
        },
    },
}
```

### Coherence

- A write is visible locally one network round trip after the server applies it. A read in flight when the invalidation arrives is served but not kept.
- A local copy never outlives its key: it expires with the key's remaining TTL, minus the stampede buffer if the transport uses stampede prevention, and `local_timeout` caps that further.
- `FLUSHDB`, `FLUSHALL`, `clear()` and a lost listener connection flush the local store. The listener reconnects after `reconnect_delay`; until then every read goes to the transport.
- The listener pings its tracking connection every `health_check_interval` seconds of wall clock, busy or idle. A connection the server drops silently is therefore noticed within `health_check_interval + reconnect_delay` plus the reconnect itself, and the local store is flushed at that point, so that sum bounds how long a stale local copy can be served.
- The transport's stampede prevention applies to local hits too, so early recomputes stay spread across processes. A local hit whose XFetch roll fires returns the default; it is not refetched from the transport and rolled a second time.

### Without a listener

`"coherence": "ttl"` runs no listener, so nothing evicts a local copy before it expires: a write elsewhere stays invisible until then, and `local_timeout` is required as the bound. In exchange there is no thread and no extra connection, `prefixes` is not needed, and any transport works, cluster and valkey-glide included.

### Prefixes

`CLIENT TRACKING BCAST` subscribes to key prefixes. With Django's default `KEY_FUNCTION` the tracked prefix is the transport's `KEY_PREFIX` plus a colon; with a custom `KEY_FUNCTION` every key in the database is tracked. `OPTIONS["prefixes"]` overrides that. Prefixes must not overlap, and a key outside every tracked prefix raises `ImproperlyConfigured`, since writes to it would never be seen.

### What's supported

The standard Django cache interface, the `nx`/`xx`/`get` flags on `set`, and the key metadata helpers delegated to the transport (`keys`, `iter_keys`, `scan`, `ttl`, `pttl`, `type`, `info`, `slowlog_get`, `slowlog_len`, `persist`, `expire`, `delete_pattern`), all with async counterparts. Data-structure ops (`lpush`, `hset`, `zadd`, ...) raise `NotSupportedError`; use the transport alias for them. `info()` adds a `tracking` section with the listener state, the store size and the hit, miss, invalidation and flush counters.

`delete_pattern` takes a Redis glob on both sides: the same pattern picks the local entries to evict and the keys the transport deletes, so `[^0]` negates the way it does on the server.

`KEY_PREFIX` is not accepted on a `TrackingCache` alias, in either slot: keys are made by the transport, so set it there. `TIMEOUT` and `VERSION` on the alias are ignored for the same reason. Key versions come from the transport, and `incr_version` / `decr_version` (with their async twins) honor that: they delegate the rename to the transport and forget the local copies of both versions. `VERSION` on the transport alias works.

In the admin a `TrackingCache` alias is badged limited and offers no key browsing, because its keys live on the transport; browse and edit through the transport alias.

### Operational notes

- With tracking coherence the transport must be a redis-py or valkey-py backend, standalone or Sentinel. Cluster (tracking is per node) and valkey-glide (cannot receive invalidations) transports raise `ImproperlyConfigured` on first use. Behind Sentinel, the health check reconnects the listener after a failover.
- Each process holds two extra connections: a subscriber to `__redis__:invalidate` and the connection that enables tracking. The first operation opens them; a transport that is down at that moment is retried in the background.
- The local store, listener thread and counters are shared per `LOCATION` (defaulting to the transport alias) within a process, like `StreamCache`. Two aliases with different `LOCATION`s over one transport act as two independent pods.
- `close()` is a no-op so the listener outlives requests; `shutdown()` stops it. A dead listener thread is restarted on the next operation.
- A local miss costs one pipelined `GET` plus `PTTL`; `get_many` fetches all missing keys in one pipeline.

## Choosing between them

|  | `StreamCache` | `TrackingCache` |
|---|---|---|
| Source of truth | Distributed (every pod has the data) | The transport (the local store is just a cache) |
| Eviction | LRU on each pod (`MAX_ENTRIES`) | LRU on each pod (`MAX_ENTRIES`); under tracking coherence every write evicts too |
| Network on read | Never (after warmup) | Only on local miss |
| Network on write | One `XADD` per write | One write to the transport |
| Failure mode | Stale until consumer recovers | Strict (nothing is cached while the listener is down); with `coherence: "ttl"` local copies are served until they expire |

If reads can be served from process memory and writes are infrequent, `StreamCache`. Otherwise `TrackingCache`: with tracking coherence a write is visible everywhere within a round trip on a single server or Sentinel group; with `coherence: "ttl"` staleness stays within `local_timeout` on any transport.
