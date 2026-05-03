# Composite Backends

Two backend classes don't talk to a server directly — they compose other entries in your `CACHES` setting.

| Backend | Reads served from | Consistency | Best for |
|---------|-------------------|-------------|----------|
| `StreamCache` | Local in-memory dict | Eventually consistent (last-writer-wins) | Read-heavy data shared across pods (config, feature flags) |
| `TieredCache` | L1 (typically `LocMemCache`), falling through to L2 | Strong (L1 TTL is bounded) | Hot reads where L2 round-trip cost dominates |

## StreamCache

In-memory store with cross-pod synchronization via a Redis Stream.

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
            "TRANSPORT": "redis",   # alias of any cachex RespCache subclass
            "STREAM_KEY": "cache:sync",
            "MAXLEN": 10000,        # approximate trim
            "BLOCK_TIMEOUT": 1000,  # XREAD BLOCK timeout, ms
            "REPLAY": 0,            # entries to replay on startup; 0 disables
        },
    },
}
```

### What's not supported

- `add()`, `incr()`, `decr()` raise `NotSupportedError`. Their semantics (atomic check-and-set, atomic increment) can't be honoured under eventual consistency. Use the transport cache directly when you need them.

### Operational notes

- All pods sharing a `STREAM_KEY` must use the same transport `BACKEND` and `OPTIONS` so their serializer/compressor agree on the wire format.
- The consumer thread is restarted automatically if it dies; check `info()["sync"]` for consumer health, last-read age, and stream position.
- Set `REPLAY > 0` (up to `MAXLEN`) so a restarting pod replays the last N mutations and doesn't start with an empty cache.

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
            "TIERS": ["l1", "l2"],
            "L1_TIMEOUT": 5,    # cap for how long entries live in L1
        },
    },
}
```

### TTL bounding

L1 TTL is `min(L1_TIMEOUT, L2's remaining TTL)`. This prevents serving stale data: if you `set(key, value, timeout=60)` on L2, L1 won't keep that entry past 60 seconds, even if `L1_TIMEOUT` is larger.

If `L1_TIMEOUT` is omitted, the cap falls back to L1's own `TIMEOUT` setting.

### What's supported

`TieredCache` exposes only the standard Django cache interface (`get`, `set`, `add`, `delete`, `get_many`, `set_many`, ...). For data structures, pipelines, scripts, or any other extended feature, address the tier caches directly via `caches["l1"]` or `caches["l2"]`.

## Choosing between them

|  | `StreamCache` | `TieredCache` |
|---|---|---|
| Source of truth | Distributed (every pod has the data) | L2 (L1 is just a hot cache) |
| Eviction | LRU on each pod (`MAX_ENTRIES`) | LRU on L1; L2 governs survival |
| Network on read | Never (after warmup) | Only on L1 miss |
| Network on write | One `XADD` per write | One write to each tier |
| Failure mode | Stale until consumer recovers | Strict (falls through to L2) |

If reads can be served from process memory and writes are infrequent, `StreamCache`. If writes are common and you need consistency, `TieredCache`.
