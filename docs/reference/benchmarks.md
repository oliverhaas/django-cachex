# Benchmarks

Numbers from the [`benchmarks/`][bench-src] harness. They shift from run to
run, so trust the ordering more than the exact values.

[bench-src]: https://github.com/oliverhaas/django-cachex/tree/main/benchmarks

## Setup

- AMD Ryzen 9 5950X (16C/32T) · 32 GiB RAM · Ubuntu 24.04 · Linux 7.0
- CPython 3.14.2 (GIL build), Django 6.1.1
- Redis 8 / Valkey 9 in local Docker, paired natively per adapter
  (`redis-py` → redis, `valkey-py` / `valkey-glide` → valkey, `django (builtin)` → redis)
- Every round trip 250 µs longer than on the local Docker network, as within
  a cloud availability zone

Each column from `get` to `delete` is a phase of 1,000 calls, or 100 calls of
10 keys for `mget` and `mset`. Throughput is in calls per second. A phase has
10 timed runs after a warmup pass. The cached value pickles to ~150 B, except
in the compressor benchmarks.

## Sync direct

Cache calls such as `cache.get(...)`, with no Django request and no asyncio.
The round trip takes most of each call, so the django-cachex adapters land
within 7% of each other. Django's built-in `RedisCache` builds a `redis.Redis`
client for every call, about 40 µs, and its `incr` makes two round trips
(`EXISTS`, then `INCRBY`).

| Adapter | get | get-miss | set | mget | mset | incr | delete | py-mem KiB |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|-----------:|
| redis-py            | 2,789 | 2,896 | 2,782 | 2,362 | 2,213 | 2,887 | 2,879 | 64 |
| redis-py+hiredis    | 2,783 | 2,887 | 2,777 | 2,391 | 2,215 | 2,901 | 2,905 | 36 |
| valkey-py           | 2,869 | 2,946 | 2,883 | 2,396 | 2,301 | 2,953 | 2,953 | 91 |
| valkey-py+libvalkey | 2,920 | 2,990 | 2,922 | 2,535 | 2,383 | 2,989 | 2,979 | 31 |
| valkey-glide        | 2,733 | 2,837 | 2,743 | 2,337 | 2,297 | 2,840 | 2,834 | 29 |
| django (builtin)    | 2,511 | 2,535 | 2,458 | 2,227 | 2,032 | 1,366 | 2,526 | 50 |

## Serializers

The `valkey-py+libvalkey` adapter with each serializer.

| Serializer | get | get-miss | set | mget | mset | incr | delete |
|------------|----:|---------:|----:|-----:|-----:|-----:|-------:|
| pickle    | 2,906 | 2,976 | 2,924 | 2,499 | 2,383 | 2,984 | 2,980 |
| json      | 2,883 | 2,967 | 2,861 | 2,395 | 2,215 | 2,963 | 2,962 |
| msgpack   | 2,917 | 2,982 | 2,914 | 2,551 | 2,392 | 2,981 | 2,990 |
| orjson    | 2,923 | 2,984 | 2,936 | 2,573 | 2,456 | 2,987 | 2,980 |
| ormsgpack | 2,921 | 2,956 | 2,897 | 2,549 | 2,368 | 2,966 | 2,957 |

## Compressors (macro)

Cache calls through `valkey-py+libvalkey` with `pickle`, on a 14 KiB
queryset-shaped payload.

| Compressor | get | get-miss | set | mget | mset | incr | delete | srv-mem KiB |
|------------|----:|---------:|----:|-----:|-----:|-----:|-------:|------------:|
| none | 2,372 | 2,976 | 2,711 |   918 | 1,480 | 2,988 | 2,982 | 1,269 |
| zlib | 2,495 | 2,983 | 2,195 | 1,105 |   680 | 2,971 | 2,981 |   169 |
| gzip | 2,471 | 2,988 | 2,099 | 1,027 |   608 | 2,980 | 2,978 |   169 |
| lzma | 2,384 | 2,985 |   682 |   920 |    88 | 2,988 | 2,982 |   167 |
| lz4  | 2,551 | 2,987 | 2,677 | 1,181 | 1,420 | 2,986 | 2,978 |   238 |
| zstd | 2,532 | 2,984 | 2,563 | 1,153 | 1,190 | 2,987 | 2,984 |   169 |

## Compressors (micro)

`compress` and `decompress` alone on the same 14 KiB payload, with no
adapter and no network.

| Compressor | output ratio | compress (MB/s) | decompress (MB/s) |
|------------|-------------:|----------------:|------------------:|
| zlib |  11.9% |   155.4 | 1,074.4 |
| gzip |  11.8% |   128.4 |   766.2 |
| lzma |  11.4% |    12.2 |   419.3 |
| lz4  |  16.6% | 2,511.6 | 6,163.5 |
| zstd |  11.4% |   676.4 | 1,695.5 |

## Django request cycle

The sync direct workload, with every cache call inside `Client().get(url)`
(URL resolve → `CommonMiddleware` → view → `request_finished`). The gap to
sync direct is Django's per-request overhead, about 115 µs here.

| Adapter | get | get-miss | set | mget | mset | incr | delete |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|
| redis-py            | 2,105 | 2,168 | 2,090 | 1,854 | 1,738 | 2,010 | 2,081 |
| redis-py+hiredis    | 2,150 | 2,215 | 2,142 | 1,900 | 1,806 | 2,058 | 2,097 |
| valkey-py           | 2,184 | 2,235 | 2,191 | 1,902 | 1,803 | 2,072 | 2,246 |
| valkey-py+libvalkey | 2,201 | 2,257 | 2,205 | 1,963 | 1,862 | 2,084 | 2,236 |
| valkey-glide        | 2,079 | 2,126 | 2,054 | 1,823 | 1,791 | 1,974 | 2,106 |
| django (builtin)    | 1,955 | 1,987 | 1,934 | 1,778 | 1,647 | 1,126 | 1,875 |

## Async serial

One awaited call at a time, such as `await cache.aget(...)`. The gap to sync
direct is asyncio loop overhead. Django's built-in `RedisCache` has no native
async path, so it also pays for `sync_to_async`.

| Adapter | get | get-miss | set | mget | mset | incr | delete |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|
| redis-py            | 2,709 | 2,805 | 2,666 | 2,316 | 1,924 | 2,793 | 2,787 |
| redis-py+hiredis    | 2,684 | 2,774 | 2,662 | 2,335 | 1,889 | 2,776 | 2,790 |
| valkey-py           | 2,678 | 2,768 | 2,675 | 2,318 | 1,997 | 2,756 | 2,746 |
| valkey-py+libvalkey | 2,670 | 2,762 | 2,671 | 2,318 | 2,006 | 2,766 | 2,759 |
| valkey-glide        | 2,615 | 2,699 | 2,619 | 2,247 | 2,202 | 2,695 | 2,680 |
| django (builtin)    | 2,005 | 2,031 | 1,999 | 1,826 | 1,689 | 1,205 | 2,021 |

## Async concurrent (50 in flight)

`asyncio.gather` of 50 calls at a time. redis-py and valkey-py open a
connection per call in flight, and valkey-glide sends them all over one.
Django's built-in `RedisCache` runs every call through `sync_to_async` on one
thread, so it serves 50 in flight about as fast as one at a time. Connection
counts, which include the harness's own, stay flat between phases on every
adapter (`Δ = 0`).

| Adapter | get | get-miss | set | mget | mset | incr | delete | conns peak |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|-----------:|
| redis-py            | 16,411 | 18,362 | 15,447 |  8,937 |  5,466 | 18,488 | 18,481 | 51 |
| redis-py+hiredis    | 16,885 | 18,484 | 15,476 |  8,678 |  5,383 | 18,569 | 18,543 | 51 |
| valkey-py           | 20,514 | 22,735 | 20,088 | 10,133 |  6,427 | 22,510 | 22,838 | 51 |
| valkey-py+libvalkey | 20,541 | 22,882 | 20,318 | 10,108 |  6,430 | 22,563 | 23,058 | 51 |
| valkey-glide        | 43,804 | 47,758 | 33,133 | 13,296 | 10,043 | 50,684 | 49,981 |  2 |
| django (builtin)    |  2,244 |  2,281 |  2,220 |  2,000 |  1,863 |  1,292 |  2,188 |  2 |

## ASGI full-stack

`granian` (4 workers) and `httpx` (100 concurrent clients, 20 s) against a
view that makes six async cache calls per request. The harness samples server
RSS and `connected_clients` every 5 s, and the connection counts include about
5 of its own. The req/s column is noisy, so read it in rough buckets: 500 to
600 for the django-cachex adapters, 350 for Django's built-in `RedisCache`.

Under ASGI, Django gives every request its own instance of a cache backend.
The built-in `RedisCache` builds a connection pool for each instance, and its
`close()` leaves the pool open, so its connections climb until the garbage
collector frees the pools.

| Adapter | req/s | avg ms | p99 ms | RSS peak (MiB) | conns peak | conns settled |
|---------|------:|-------:|-------:|---------------:|-----------:|--------------:|
| redis-py            | 487 | 204.4 | 1,522.8 | 364 |   105 | 105 |
| redis-py+hiredis    | 597 | 167.1 | 1,270.4 | 368 |   105 | 105 |
| valkey-py           | 613 | 162.6 | 1,317.0 | 369 |   104 | 104 |
| valkey-py+libvalkey | 563 | 177.0 | 1,383.5 | 368 |   104 | 104 |
| valkey-glide        | 500 | 199.2 | 1,451.1 | 425 |     8 |   8 |
| django (builtin)    | 355 | 279.6 | 1,769.4 | 459 | 1,371 | 180 |

## ORM cache vs django-cachalot

The [ORM cache](../user-guide/orm-cache.md) derives from django-cachalot 2.9.1.
The contenders are the database alone (`none`), `cachalot`, the ORM cache
(`cachex`), and both over a
[`TrackingCache`](../user-guide/composite-backends.md) (`+tracking`), which
keeps local copies of what each process read.

- The machine above, with CPython 3.14.2 (free-threaded build, with the GIL
  that libvalkey turns back on), Django 6.1.1, psycopg 3.3.6
- PostgreSQL 18 with `fsync=off` and Valkey 9 in local Docker, both libraries
  on the `valkey-py+libvalkey` backend
- The same 250 µs added to every round trip, except in the clock skew runs

### Correctness

Twelve cases where a cached result can differ from the database, each checked
against an uncached read. They are the known differences between the two
libraries, not a sample of typical queries. Cachalot got all 12 wrong, the ORM
cache none:

- A read while an autocommit `UPDATE` runs, and one by a process that an
  `on_commit()` hook hands the new row to. Cachalot invalidates before an
  autocommit statement runs, and in `atomic()` only after every hook has run.
- A subquery or `Now()` nested in a `filter()` expression, and
  `order_by(Random())`. Cachalot looks for subqueries and `Now()` only as a
  filter's direct operands, and refuses `order_by("?")` but not `Random()`.
- Two JSON filter values of the same length that agree in their first 35
  characters. Cachalot's cache key holds each parameter's `str()`, which
  psycopg 3 shortens to those characters and the length.
- `DB_CASCADE` deletes, raw `TRUNCATE`, raw `REFRESH MATERIALIZED VIEW`, and
  raw writes to a mixed-case or unmanaged table. Cachalot doesn't invalidate
  every table these change.
- Reads in a `REPEATABLE READ` transaction from its old snapshot, which
  cachalot stores in the shared cache at the commit.

`CACHALOT_FINAL_SQL_CHECK = True` and listing the unmanaged table in
`CACHALOT_ADDITIONAL_TABLES` fix 2 of the 12.

### Races

Four reader processes read a counter row while a writer updates it 500 times,
at random times 10 ms apart on average. A read is stale if it returns a
version older than one committed before the read began. Stale reads per
1,000, where the ORM cache column covers it with and without a
`TrackingCache`:

| Scenario | cachalot | cachalot+tracking | ORM cache |
|----------|---------:|------------------:|----------:|
| Autocommit                                    | 132 | 504 | 0 |
| Autocommit, commits 2 ms slower               | 755 | 602 | 0 |
| `atomic()`                                    | 8.0 | 512 | 0 |
| `atomic()`, 1 ms of later `on_commit()` hooks | 117 | 567 | 0 |

- Under autocommit, cachalot invalidates before the statement runs, so a read
  before the commit stores the old row as newer than the write. The cache
  serves it until the next write: cachalot hid 146 in 1,000 writes that way,
  and with slower commits all of them. Writes in quick succession, as in a
  loop of updates, make it worse, since a raced read can store its old row
  after the next write's invalidation too.
- In `atomic()`, cachalot invalidates only after all `on_commit()` hooks have
  run, so a process that a hook hands the new row to, say through a task
  queue, can get the old one. It also invalidates in two steps a round trip
  apart, and until the second it can serve a row read before the commit.
- `cachalot+tracking` serves hits from the process's memory. The thread that
  evicts a copy after a write needs the GIL, and readers that never pause
  make it wait up to 5 ms. At a read every 1 ms, it served 40 stale reads per
  1,000.
- The ORM cache sends a table's queries to the database while a write to it
  runs. That costs it 6 to 7 points of hit ratio against cachalot, and 20 to
  22 with slower commits.

### Clock skew

Cachalot stamps results and invalidations with the `time.time()` of the
process that makes them. If a reader's clock runs ahead by Δ, a result it
stores right after a write outlives every write in the next Δ. With writes at
random times and λ = Δ / mean time between writes, that hides λ / (1 + λ) of
the writes. Runs with Δ from 2 to 60 ms came within 34 per 1,000 of that.
Hidden writes per 1,000, by how far the reader's clock runs ahead and how
often the table is written:

| Skew | Write every 1 s | 1 min | 15 min | 1 h |
|-----:|----------------:|------:|-------:|----:|
| 10 ms  |   9.9 | 0.17 | 0.011 | 0.003 |
| 100 ms |    91 |  1.7 |  0.11 | 0.028 |
| 1 s    |   500 |   16 |   1.1 |  0.28 |
| 10 s   |   909 |  143 |    11 |   2.8 |

A hidden write stays hidden until the table's next write after the skew has
passed. A writer's clock ahead costs only hits, keeping e^(−λ) of them, which
the runs matched within 1.3 points: 1 s against a write every minute loses
under 2%. The ORM cache keeps a generation per table on the cache server and
reads no clock. With either clock 40 ms ahead, it hid no write.

### Speed

Median latency in µs over 2,000 calls. For `none`, the hit and the miss are
the query alone.

| Contender | Hit | Miss | Autocommit write | `atomic()` write |
|-----------|----:|-----:|-----------------:|-----------------:|
| none              | 566 |   612 |   563 | 1,244 |
| cachalot          | 500 | 1,453 |   990 | 2,072 |
| cachex            | 541 | 1,489 | 1,354 | 2,057 |
| cachalot+tracking | 136 | 1,508 | 1,010 | 2,128 |
| cachex+tracking   | 531 | 1,516 | 1,377 | 2,082 |

- A hit takes a round trip, as the query does, so it saves only the
  database's work. A hit on one row takes about 90% of the query's time, on
  100 rows about half, and on 1,000 rows about a quarter.
- The ORM cache's hits take about 40 µs longer than cachalot's, for the Lua
  script that checks leases and generations. Its autocommit writes make two
  round trips to Valkey, a lease before the statement and its release after,
  where cachalot makes one.
- `cachalot+tracking` serves hits from memory in about a quarter of the time,
  which is also what makes its reads stale. The ORM cache over a
  `TrackingCache` still asks the server whether its copy is current.
- In a mixed workload of 100-row reads by one of 20 queries, the caches serve
  about 1.5 times the database's throughput at 1% writes, and
  `cachalot+tracking` twice. At 10% writes they serve about a fifth less,
  since every write makes the cached queries on its table miss, and a miss
  takes three round trips.
- A `migrate` with nothing to apply emptied cachalot's cache of 100 queries
  and left the ORM cache's intact.

### Network delay

The same runs with no added delay and with 1 ms. Without delay, a round trip
takes about 60 µs to Valkey and 80 µs to PostgreSQL. A hit saves the
database's work but not the round trip, so with delay it saves less: at 1 ms,
cachalot's one-row hit took 95% of the query's time, against 82% without
delay. A race lasts a few round trips, so with delay more reads fall in one.
Cachalot's results, and the ORM cache's hit ratio in the last row:

| Result | No delay | 250 µs | 1 ms |
|--------|---------:|-------:|-----:|
| Throughput at 1% writes, × the database's  | 1.9  | 1.5  | 1.2  |
| Throughput at 10% writes, × the database's | 0.94 | 0.79 | 0.62 |
| Stale reads per 1,000, autocommit          | 104  | 132  | 247  |
| Stale reads per 1,000, `atomic()`          | 0.19 | 8.0  | 88   |
| Hit ratio at a read every 1 ms             | 94%  | 90%  | 83%  |
| Hit ratio at a read every 1 ms, ORM cache  | 92%  | 83%  | 60%  |

The ORM cache's throughput stayed within 4% of cachalot's, and it served no
stale read at any delay. Its hit ratio falls faster, since its lease sends a
table's reads to the database for as long as a write runs, and writes take
longer with delay.

## Reproducing

The harness starts its own Redis, Valkey and PostgreSQL containers, so a
Docker daemon is the only host requirement:

```console
# Everything, with 250 µs added to every round trip
BENCH_NET_DELAY_US=250 uv run pytest benchmarks/ -c benchmarks/pytest.ini

# A single slice
uv run pytest benchmarks/test_throughput.py::test_adapters_sync \
  -c benchmarks/pytest.ini

# The ORM cache comparison alone
BENCH_NET_DELAY_US=250 uv run pytest benchmarks/test_orm.py \
  -c benchmarks/pytest.ini
```

`benchmarks/README.md` lists the slices, the knobs (`N_OPS`, `K_RUNS`,
`WARMUP_KEYS`, `MGET_BATCH`) and the full methodology. `BENCH_NET_DELAY_US`
delays everything the server containers send, using `tc netem` from a helper
container, so it needs no host privileges.
