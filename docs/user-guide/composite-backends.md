# TrackingCache

A local read cache over an existing Redis or Valkey alias, kept coherent by the server's `CLIENT TRACKING` broadcast mode. It does not talk to a server directly; it composes another entry in your `CACHES` setting.

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

`local_timeout`, `poll_timeout`, `health_check_interval` and `reconnect_delay` must be positive finite numbers (numeric strings are accepted; `local_timeout: None` keeps meaning no cap). Anything else raises `ImproperlyConfigured` at `caches[alias]`.

## Coherence

- A write is visible locally one network round trip after the server applies it. A read in flight when the invalidation arrives is served but not kept.
- A local copy never outlives its key: it expires with the key's remaining TTL, minus the stampede buffer if the transport uses stampede prevention, and `local_timeout` caps that further.
- `FLUSHDB`, `FLUSHALL`, `clear()` and a lost listener connection flush the local store. The listener reconnects after `reconnect_delay`; until then every read goes to the transport.
- The listener pings its tracking connection every `health_check_interval` seconds of wall clock, busy or idle. A connection the server drops silently is therefore noticed within `health_check_interval + reconnect_delay` plus the reconnect itself, and the local store is flushed at that point, so that sum bounds how long a stale local copy can be served.
- The transport's stampede prevention applies to local hits too, so early recomputes stay spread across processes. The XFetch roll uses the key's remaining server TTL, not the local copy's age, so a `local_timeout` cap does not make local hits return the default early, and a local copy of a key without a TTL never rolls. A local hit whose roll fires returns the default; it is not refetched from the transport and rolled a second time. `has_key()` never rolls, and `get_or_set()` reads the value it just wrote back without a roll, so short-timeout keys still warm the local store.

## Without a listener

`"coherence": "ttl"` runs no listener, so nothing evicts a local copy before it expires: a write elsewhere stays invisible until then, and `local_timeout` is required as the bound. In exchange there is no thread and no extra connection, `prefixes` is not needed, and any transport works, cluster and valkey-glide included.

## Prefixes

`CLIENT TRACKING BCAST` subscribes to key prefixes. With Django's default `KEY_FUNCTION` the tracked prefix is the transport's `KEY_PREFIX` plus a colon; with a custom `KEY_FUNCTION` every key in the database is tracked. `OPTIONS["prefixes"]` overrides that. Prefixes must not overlap, and a key outside every tracked prefix raises `ImproperlyConfigured`, since writes to it would never be seen.

## What's supported

The standard Django cache interface, the `nx`/`xx`/`get` flags on `set`, and the key metadata helpers delegated to the transport (`keys`, `iter_keys`, `scan`, `ttl`, `pttl`, `type`, `memory_usage`, `largest_keys`, `info`, `slowlog_get`, `slowlog_len`, `persist`, `expire`, `delete_pattern`), all with async counterparts (`akeys`, `aiter_keys`, `ascan`, `attl`, `apttl`, `atype`, `amemory_usage`, `alargest_keys`, `apersist`, `aexpire`, `adelete_pattern`). A `NotSupportedError` the transport raises for a delegated call propagates unchanged, so it still names the server's reason. Data-structure ops (`lpush`, `hset`, `zadd`, ...) raise `NotSupportedError`; use the transport alias for them. `info()` adds a `tracking` section with the listener state, the store size and the hit, miss, invalidation and flush counters.

`delete_pattern` takes a Redis glob on both sides: the same pattern picks the local entries to evict and the keys the transport deletes, so `[^0]` negates the way it does on the server. Local copies are matched by their made key against the transport's `make_pattern()` glob, so eviction also works under a custom `KEY_FUNCTION` without a `REVERSE_KEY_FUNCTION`.

`KEY_PREFIX` is not accepted on a `TrackingCache` alias, in either slot: keys are made by the transport, so set it there. `TIMEOUT` and `VERSION` on the alias are ignored for the same reason. Key versions come from the transport, and `incr_version` / `decr_version` (with their async twins) honor that: they delegate the rename to the transport and forget the local copies of both versions. `VERSION` on the transport alias works.

In the admin a `TrackingCache` alias is badged limited and offers no key browsing, because its keys live on the transport; browse and edit through the transport alias.

## Operational notes

- With tracking coherence the transport must be a redis-py or valkey-py backend, standalone or Sentinel. Cluster (tracking is per node) and valkey-glide (cannot receive invalidations) transports raise `ImproperlyConfigured` on first use. Behind Sentinel, the health check reconnects the listener after a failover.
- Each process holds one extra connection, opened outside the pool's accounting. It speaks RESP3, so the server pushes invalidations on the tracking connection itself. The first operation opens it; a transport that is down at that moment is retried in the background.
- The listener always parses with its driver's pure-Python RESP3 parser, whatever `parser_class` the transport is configured with. `hiredis` and `libvalkey` stay on the data path; only the listener's own handful of messages is parsed in Python.
- The local store, listener thread and counters are shared per `LOCATION` (defaulting to the transport alias) within a process. Two aliases with different `LOCATION`s over one transport act as two independent pods. Aliases sharing one `LOCATION` must agree on `transport`, `coherence`, `prefixes`, `local_timeout`, `MAX_ENTRIES`, `poll_timeout`, `health_check_interval` and `reconnect_delay`; a mismatch raises `ImproperlyConfigured` naming the differing options.
- `close()` is a no-op so the listener outlives requests; `shutdown()` stops it. A dead listener thread is restarted on the next operation. When the tracking socket drops, the listener fails fast rather than letting the driver reconnect without `CLIENT TRACKING`; `TrackingCache` rebuilds it. An outage logs one traceback, then a one-line warning per attempt.
- A local miss costs one pipelined `GET` plus `PTTL`; `get_many` fetches all missing keys in one pipeline.
