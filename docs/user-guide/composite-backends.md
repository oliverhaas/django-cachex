# Composite Backends

Three backend classes don't talk to a server directly. They compose other entries in your `CACHES` setting.

| Backend | Reads served from | Consistency | Best for |
|---------|-------------------|-------------|----------|
| `StreamCache` | Local in-memory dict | Eventually consistent (last-writer-wins) | Read-heavy data shared across pods (config, feature flags) |
| `TieredCache` | L1 (typically `LocMemCache`), falling through to L2 | Bounded staleness (L1 may lag L2 by up to `l1_timeout`) | Hot reads where L2 round-trip cost dominates |
| `TrackingCache` | Local store, falling through to the transport | Coherent: the server invalidates local copies on every write (`CLIENT TRACKING`) | Read-heavy keys whose writes must be visible within a round trip |

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

### Convergence

Every pod applies stream entries in stream order, its own included, and each entry carries the final value rather than a delta. Two pods that write the same key inside the propagation window therefore both end on whichever entry the stream ordered last, instead of each ending up holding the other's value. A pod skips one of its own entries only where a later local write to that key (or a local `clear()`) has already replaced it, so a writer never reads back a value it has moved past.

Broadcasts stay best-effort: an entry is dropped when the publish backlog is full or the transport errors. The stream is a replication feed, not a durable log.

### Operational notes

- All pods sharing a `stream_key` must use the same transport `BACKEND` and `OPTIONS` so their serializer/compressor agree on the wire format.
- The consumer thread, the publisher thread and the pod identity are shared per `LOCATION` within a process, not per backend instance. Django hands out one cache instance per thread and per async context, so per-instance state would mean one consumer per ASGI request. Two `StreamCache` aliases sharing a `stream_key` but not a `LOCATION` act as two independent pods, which is how the test suite simulates a cluster in one process.
- The consumer thread is restarted automatically if it dies; check `info()["sync"]` for consumer health, last-read age, and stream position.
- On a valkey-glide transport the consumer polls instead of blocking: glide carries every command of a client over one connection, so a parked `XREAD BLOCK` would hold up each publish behind it. `block_timeout` is ignored there and the poll runs every 25 ms.
- Set `replay` above 0 (up to `maxlen`) so a restarting pod replays the last N mutations and doesn't start with an empty cache.
- Publishes are queued to a background thread; when more than `max_pending_publishes` are outstanding, new publishes are dropped with a warning instead of blocking the caller.

## TieredCache

Two-tier cache referencing two existing `CACHES` entries.

```python
CACHES = {
    "l1": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "OPTIONS": {"MAX_ENTRIES": 1000},
    },
    "l2": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/0",
    },
    "default": {
        "BACKEND": "django_cachex.cache.TieredCache",
        "OPTIONS": {
            "tiers": ["l1", "l2"],
            "l1_timeout": 5,  # cap for how long entries live in L1
        },
    },
}
```

### TTL bounding

L1 TTL is `min(l1_timeout, L2's remaining TTL)`. An L1 entry can never outlive its L2 entry: if you `set(key, value, timeout=60)`, L1 won't keep that entry past 60 seconds, even if `l1_timeout` is larger.

If `l1_timeout` is omitted, the cap falls back to L1's own `TIMEOUT` setting. One of the two is required. With neither (`l1_timeout` unset and `TIMEOUT: None` on the L1 alias) an L1 entry would never expire, so the first operation raises `ImproperlyConfigured` rather than caching indefinitely.

The bound is on TTL, not on cross-process visibility. Writes and deletes through a `TieredCache` update both of its tiers, but when another process changes a key in L2, this process keeps serving its L1 copy until that entry expires, so a read can lag L2 by up to `l1_timeout`.

The same bound applies within a process. Every mutation writes L2 before touching L1, which keeps a concurrent read from repopulating L1 out of the pre-mutation L2 value, but it does not close the window: a read that has already fetched from L2 when the invalidation runs still writes the old value into L1 afterwards. L1 then serves the superseded value for up to `l1_timeout`. Address the tiers directly if a read has to observe a write immediately.

### What's supported

`TieredCache` exposes the standard Django cache interface (`get`, `set`, `add`, `delete`, `get_many`, `set_many`, ...) plus key metadata helpers delegated to L2 (`keys`, `iter_keys`, `scan`, `ttl`, `pttl`, `type`, `info`, `persist`, `expire`, `delete_pattern`), which is what drives the admin. Data-structure ops (`lpush`, `hset`, `zadd`, ...) raise `NotSupportedError`; for those, pipelines, or scripts, address the tier caches directly via `caches["l1"]` or `caches["l2"]`.

`KEY_PREFIX` is not accepted on a `TieredCache` alias, in either the top-level slot or `OPTIONS`, because keys are passed through to the tiers unprefixed. Set `KEY_PREFIX` on the tier aliases instead; configuring it on the tiered alias raises `ImproperlyConfigured`.

## TrackingCache

Read-through local cache over an existing Redis or Valkey alias, kept coherent by the server's `CLIENT TRACKING` broadcast mode.

Reads are served from a bounded in-process store and fall through to the transport on a miss. Writes go to the transport and evict the local copy. One listener thread per process subscribes to the server's invalidation messages for the transport's key prefix, so a write on any pod, or by any other client of the same database, evicts the local copies everywhere. The transport is the only place data is stored: the local store only ever holds what the server just returned, and while the listener is not connected nothing is cached at all.

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
            "MAX_ENTRIES": 1000,  # local store bound, LRU
            "local_timeout": None,  # extra cap on how long a value stays local, in seconds
            "prefixes": None,  # tracked key prefixes; derived from the transport by default
            "poll_timeout": 1.0,  # seconds the listener waits for a message before checking for shutdown
            "health_check_interval": 15.0,  # idle seconds before the listener pings its connections
            "reconnect_delay": 1.0,  # seconds between reconnection attempts
        },
    },
}
```

### Coherence

- A write through the transport, from any process, reaches the listener as an invalidation message and evicts the local copy. The window between the server applying the write and the eviction is one network round trip; a local read inside it may still return the old value. A read that was in flight when the invalidation arrived is served but not kept.
- A local copy never outlives its key. Its local expiry is bounded by the key's remaining TTL, minus the stampede buffer when the transport has stampede prevention on, and `local_timeout` caps it further.
- `FLUSHDB` or `FLUSHALL` on the server, a lost listener connection and `clear()` all flush the local store. The listener reconnects on its own after `reconnect_delay` and every read goes to the transport until it does.
- The invalidation for this process's own write is delivered asynchronously too, so a read that immediately follows a write may be fetched from the transport twice. That costs a round trip, never coherence.
- The transport's stampede prevention is honoured: a key inside its early-recompute window reads as a miss here as well and is not kept locally, and every local hit rolls the same dice, so early recomputes stay spread across processes.

### Prefixes

`CLIENT TRACKING BCAST` subscribes to key prefixes. With Django's default `KEY_FUNCTION` keys look like `prefix:version:key`, so the tracked prefix is the transport's `KEY_PREFIX` followed by a colon. A custom `KEY_FUNCTION` on the transport makes the cache track every key in the database instead. Set `OPTIONS["prefixes"]` to narrow that down. Prefixes must not overlap, which the server rejects, and a read whose key lies outside every tracked prefix raises `ImproperlyConfigured`, because writes to it would never be seen.

### What's supported

`TrackingCache` exposes the standard Django cache interface, with the `nx`/`xx`/`get` flags on `set`, plus the key metadata helpers delegated to the transport (`keys`, `iter_keys`, `scan`, `ttl`, `pttl`, `type`, `info`, `persist`, `expire`, `delete_pattern`) and their async counterparts. Data-structure ops (`lpush`, `hset`, `zadd`, ...) raise `NotSupportedError`; use the transport alias for them. `info()` adds a `tracking` section with the listener state (`connected`, `listener_alive`, `reconnects`, `last_message_age_seconds`), the store size (`entries`), the tracked `prefixes` and the counters `hits`, `misses`, `invalidations` and `flushes`.

`KEY_PREFIX` is not accepted on a `TrackingCache` alias, in either slot: keys are made by the transport, so set it there. `TIMEOUT` and `VERSION` on the alias are ignored for the same reason; the transport's apply.

### Operational notes

- The transport must be a redis-py or valkey-py backend, standalone or Sentinel. Cluster backends are rejected, because `CLIENT TRACKING REDIRECT` is per node, and so is valkey-glide, which cannot receive invalidation messages. Both fail with `ImproperlyConfigured` on first use. Through Sentinel, the health check also confirms the listener still sits on the current primary and reconnects after a failover.
- Broadcast tracking keeps no per-client state on the server. Each process holds two extra connections: a subscriber to the `__redis__:invalidate` channel and the connection that enables tracking, redirected to it. The first operation in a process opens them synchronously; a transport that is down at that moment is logged and retried in the background.
- The local store, the listener thread and the counters are shared per `LOCATION` (defaulting to the transport alias) within a process, not per backend instance, for the same reason as `StreamCache`. Two aliases with different `LOCATION`s over one transport act as two independent pods.
- `close()` is a no-op so the listener outlives requests; `shutdown()` stops it. A dead listener thread is restarted on the next operation.
- A local miss costs one pipelined `GET` plus `PTTL`; `get_many` fetches all missing keys in one pipeline.

## Choosing between them

|  | `StreamCache` | `TieredCache` | `TrackingCache` |
|---|---|---|---|
| Source of truth | Distributed (every pod has the data) | L2 (L1 is just a hot cache) | The transport (the local store is just a cache) |
| Eviction | LRU on each pod (`MAX_ENTRIES`) | LRU on L1; L2 governs survival | LRU on each pod (`MAX_ENTRIES`); the server evicts on every write |
| Network on read | Never (after warmup) | Only on L1 miss | Only on local miss |
| Network on write | One `XADD` per write | One write to each tier | One write to the transport |
| Failure mode | Stale until consumer recovers | Strict (falls through to L2) | Strict (nothing is cached while the listener is down) |

If reads can be served from process memory and writes are infrequent, `StreamCache`. If writes are common and staleness must stay within a known bound, `TieredCache`. If a read must not lag a write by more than a round trip and the transport is a single Redis or Valkey server or a Sentinel group, `TrackingCache`.
