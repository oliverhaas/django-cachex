# Client Tracking

With `OPTIONS["tracking"]`, a Redis or Valkey cache keeps a local copy of each string value a process reads, and of each key it finds missing. The server tells every process when one of those keys changes. A read of a kept copy sends no command. The alias, its methods and their results stay the same.

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/0",
        "KEY_PREFIX": "app",
        "OPTIONS": {"tracking": True},
    },
}
```

`RedisCache`, `ValkeyCache`, `RedisSentinelCache` and `ValkeySentinelCache` take the option. The cluster and valkey-glide backends raise `ImproperlyConfigured`.

## Settings

`True` takes the defaults below, and `False`, the default, turns tracking off. A dict sets any of these keys:

| Key | Default | Meaning |
|---|---|---|
| `prefix` | `f"{KEY_PREFIX}:"` | Keys whose name on the server, after `KEY_FUNCTION` and `VERSION`, starts with it get local copies. Other keys are read from the server. A prefix that matches none of the alias's keys keeps no copies and raises no error. Required with a custom `KEY_FUNCTION`. |
| `max_staleness` | `1.0` | Seconds. Copies are served only while the server confirmed within this window that every invalidation reached the process. |
| `max_bytes` | `33554432` (32 MiB) | Size of the local copies per process, counted as value bytes plus key characters plus 100 per entry. An entry above an eighth of it is not kept. |

```python
"OPTIONS": {
    "sentinels": [("sentinel-1", 26379), ("sentinel-2", 26379)],
    "tracking": {"max_staleness": 0.5, "max_bytes": 64 * 1024 * 1024},
}
```

The default prefix covers every version of every key of the alias, so a per-deploy `VERSION` and `incr_version()` need nothing. Tracking cannot be combined with the driver's own client-side caching options, such as redis-py's `cache_config` or valkey-py's `cache_enabled`, and needs the default UTF-8 `encoding`.

## What reads see

- **Own writes.** After a write through the cache returns, every `get()`, `get_many()` and `has_key()` in the process, from any thread, returns its value or a newer one. This covers sync and async methods, pipelines at `execute()`, and the [ORM cache](orm-cache.md).
- **Other processes' writes.** The server sends every process an invalidation for each write under the prefix, and each read first processes the invalidations that have arrived. Another process's write shows after about one network delay.
- **At most `max_staleness` behind.** A read never returns a copy older than a write that completed more than `max_staleness` before the read started. The bound holds whatever failed in between: a connection that died without a FIN, a stalled thread or a paused server. On Linux, it also holds across a system suspend. A Sentinel failover, `SWAPDB` and the cases at the end of [Operation](#operation) are the exceptions.
- **TTLs.** A copy never outlives its key on the server.
- **Failover.** After a Sentinel failover, the copies from the old primary are dropped when its connection closes, or at the latest at the next check of the primary, which runs every `max_staleness`.

Only sync `get()`, `get_many()` and `has_key()` of keys under the prefix, `get_or_set()` through them, and the ORM cache's lookups use copies. Async reads, hash, list, set, sorted set and stream reads, and all other commands go to the server.

A key the server does not have is kept as a copy too, so a repeated miss sends no command, and `entries` counts it. The alias's [stampede prevention](configuration.md#cache-stampede-prevention) applies to hits on copies as it does to reads from the server.

These writes reach the writing process like another process's writes, after about one network delay:

- Writes through `get_client()`, a raw driver client, or an alias without tracking.
- The writes of a lock's `acquire()`, `extend()` and `release()`.
- Lua scripts and functions that build key names inside the script. Key names passed in `keys` or `args` are covered.

`SWAPDB` sends no invalidation, so copies of the swapped databases stay until their TTL runs out or the store evicts them. Don't use it with tracking.

## Costs

- One extra connection and one thread, `django-cachex-tracking`, per process and configuration. Aliases with the same driver, servers, options and tracking settings share them. Each configuration keeps its connection, thread and copies until the process exits. A test that reads the cache under a new `KEY_PREFIX`, set through `override_settings`, adds one of each.
- An invalidation to every process for every write under the prefix, from any client.
- About two `PING`s per `max_staleness` for each tracking connection.
- One non-blocking socket read per tracked read, on top of the lookup.
- `max_bytes` caps the counted size of the copies per process. Real memory is up to about twice that for values under a few hundred bytes.
- A read without a copy sends `GET` and `PTTL` in one pipeline, and `get_many()` fetches all keys without copies in one pipeline. `has_key()` without a copy sends `EXISTS`, or `PTTL` under stampede prevention, and keeps no copy.
- A read without a copy goes to the primary. On a Sentinel alias, or with replicas in `LOCATION`, other reads go to the replicas.

## Operation

- The first tracked read in a process starts the tracking thread, and the thread opens the connection. Writes do not. `close()`, which Django calls after each request, leaves both running.
- Until the connection is up, and whenever it is down, reads go to the server. The thread reconnects with backoff from 0.1 s up to 5 s and logs one warning per outage to the `django_cachex.tracking` logger.
- If the tracking thread cannot start, as under a thread limit or in a subinterpreter, reads go to the server, and one warning is logged. The first read after 5 s tries again.
- A `PING` answered with an error, as while a script runs too long (`-BUSY`) or a server loads its data (`-LOADING`), confirms nothing, so reads go to the server after `max_staleness`.
- `FLUSHDB` and `FLUSHALL` of any database on the server drop every copy.
- A forked child, as under gunicorn `--preload` or Celery prefork, drops the parent's copies and opens its own tracking connection at its first tracked read.
- Python 3.12 and later emit a `DeprecationWarning` when `os.fork()` runs in a process with threads, and the tracking thread is one. A fork while that thread opens its connection can also leave the new socket open in the child. So make the first tracked read after the fork. Under gunicorn `--preload`, don't read the cache or run cached ORM queries at import time.
- uWSGI needs `enable-threads`. Without it the thread never runs, and every read goes to the server. gevent and eventlet are untested.
- The connection's user needs `CLIENT SETNAME`, `CLIENT TRACKING` and `PING`, and `SELECT` when the database is not 0. `HELLO` runs for every user, and a refused `CLIENT SETINFO` does no harm. Without `CLIENT SETNAME`, `CLIENT TRACKING` or `SELECT`, the connection does not open, and reads go to the server with one warning in the log. Without `PING`, the connection stays up, but its error replies confirm nothing, so reads go to the server after `max_staleness` and `fresh` stays false. Redis 8.10 and later leave the keys that user cannot access out of its invalidations, so it needs access to the whole prefix.
- On a Sentinel alias, the Sentinels must let the user that `sentinel_kwargs` names, or the default user when it names none, run `SENTINEL MASTERS`. The connection asks for the primary with that command when it opens, and the thread asks again every `max_staleness`. When that check fails, the thread logs one warning and keeps the connection.
- A Sentinel that accepts the connection and then stops answering blocks the thread, which logs nothing, until the driver stops waiting for it. No `PING` goes out meanwhile, so after about `max_staleness` every read goes to the server. Unless the settings give the Sentinel connections a `socket_timeout`, valkey-py and redis-py 7 wait until the Sentinel answers, and redis-py 8 waits about a minute. To bound the wait, set `socket_timeout` in `sentinel_kwargs`, or in `OPTIONS` when `sentinel_kwargs` is unset. After the wait, the driver asks the next Sentinel, and the thread logs a warning when none answers.
- With `io-threads` on Redis 8.2 or later or on Valkey 8.0 or later, a read can return a copy for about one server event-loop iteration longer than `max_staleness` allows. Module commands that write from a thread or a timer can do the same.

Called without a section, `info()` adds a `tracking` section: `connected`, `fresh`, `prefix`, `max_staleness`, `entries`, `bytes`, `max_bytes`, `hits`, `misses`, `invalidations`, `flushes`, `reconnects` and `last_confirmed_age`. `fresh` is true while reads can use copies. Before the first tracked read it shows `connected` false and zero counters. `info("tracking")` passes the section name to the server and returns its answer.
