# Benchmarks

Numbers from the [`benchmarks/`][bench-src] harness. They shift from run to
run, so trust the ordering more than the exact values.

[bench-src]: https://github.com/oliverhaas/django-cachex/tree/main/benchmarks

## Setup

- AMD Ryzen 9 5950X (16C/32T) · 32 GiB RAM · Ubuntu 24.04 · Linux 6.17
- CPython 3.14.2 (GIL build), Django 6.0
- Redis 8 / Valkey 8 in local Docker, paired natively per adapter
  (`redis-py` → redis, `valkey-py` / `valkey-glide` → valkey, `django (builtin)` → redis)

Each column from `get` to `delete` is a phase of 1,000 calls, or 100 calls of
10 keys for `mget` and `mset`. Throughput is in calls per second. A phase has
10 timed runs after a warmup pass. The cached value pickles to ~150 B, except
in the compressor benchmarks.

## Sync direct

Cache calls such as `cache.get(...)`, with no Django request and no asyncio.

| Adapter | get | get-miss | set | mget | mset | incr | delete | py-mem KiB |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|-----------:|
| redis-py            | 2,179 |  2,327 |  2,150 | 1,368 | 1,226 |  2,345 | 1,132 | 111 |
| redis-py+hiredis    | 2,235 |  2,365 |  2,176 | 1,448 | 1,289 |  2,374 | 1,147 |  51 |
| valkey-py           | 2,639 |  2,823 |  2,603 | 1,513 | 1,347 |  2,873 | 1,374 | 109 |
| valkey-py+libvalkey | 2,707 |  2,865 |  2,613 | 1,617 | 1,421 |  2,887 | 1,394 |  48 |
| valkey-glide        | 7,110 |  8,821 |  6,887 | 1,928 | 1,844 |  9,076 | 3,980 |  29 |
| django (builtin)    | 2,218 |  2,360 |  2,205 | 1,416 | 1,290 |  1,855 | 1,143 |  51 |

## Serializers

The `valkey-py+libvalkey` adapter with each serializer.

| Serializer | get | get-miss | set | mget | mset | incr | delete |
|------------|----:|---------:|----:|-----:|-----:|-----:|-------:|
| pickle    | 2,502 | 2,732 | 2,578 | 1,565 | 1,249 | 2,771 | 1,328 |
| json      | 2,464 | 2,783 | 2,361 | 1,419 |   898 | 2,846 | 1,348 |
| msgpack   | 2,459 | 2,726 | 2,609 | 1,653 | 1,165 | 2,831 | 1,344 |
| orjson    | 2,542 | 2,757 | 2,647 | 1,750 | 1,301 | 2,857 | 1,362 |
| ormsgpack | 2,550 | 2,792 | 2,632 | 1,742 | 1,290 | 2,884 | 1,360 |

## Compressors (macro)

Cache calls through `valkey-py+libvalkey` with `pickle`, on a 14 KiB
queryset-shaped payload.

| Compressor | get | get-miss | set | mget | mset | incr | delete | srv-mem KiB |
|------------|----:|---------:|----:|-----:|-----:|-----:|-------:|------------:|
| none | 1,577 | 2,711 | 2,342 | 291 | 990 | 2,785 | 1,349 | 1,268 |
| zlib | 1,544 | 2,770 | 1,902 | 286 | 531 | 2,841 | 1,347 |   166 |
| gzip | 1,479 | 2,753 | 1,825 | 272 | 486 | 2,864 | 1,354 |   166 |
| lzma | 1,466 | 2,765 |   646 | 265 |  83 | 2,878 | 1,362 |   163 |
| lz4  | 1,571 | 2,773 | 2,338 | 290 | 958 | 2,879 | 1,380 |   233 |
| zstd | 1,564 | 2,782 | 2,248 | 285 | 833 | 2,889 | 1,400 |   166 |

## Compressors (micro)

`compress` and `decompress` alone on the same 14 KiB payload, with no
adapter and no network.

| Compressor | output ratio | compress (MB/s) | decompress (MB/s) |
|------------|-------------:|----------------:|------------------:|
| zlib |  11.9% |   181.0 | 1,217.2 |
| gzip |  11.8% |   147.8 | 1,090.0 |
| lzma |  11.4% |    13.2 |   484.2 |
| lz4  |  16.6% | 2,945.5 | 6,089.9 |
| zstd |  11.4% |   780.6 | 2,077.5 |

## Django request cycle

The sync direct workload, with every cache call inside `Client().get(url)`
(URL resolve → `CommonMiddleware` → view → `request_finished`). The gap to
sync direct is Django's per-request overhead.

| Adapter | get | get-miss | set | mget | mset | incr | delete |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|
| redis-py            | 1,058 | 1,123 | 1,083 |   795 |   745 | 1,099 | 1,077 |
| redis-py+hiredis    | 1,007 | 1,132 | 1,083 |   832 |   778 | 1,116 | 1,122 |
| valkey-py           | 1,035 | 1,230 | 1,183 |   844 |   789 | 1,206 | 1,200 |
| valkey-py+libvalkey | 1,003 | 1,243 | 1,180 |   879 |   825 | 1,215 | 1,238 |
| valkey-glide        | 1,150 | 1,749 | 1,668 |   983 |   940 | 1,740 | 1,745 |
| django (builtin)    |   799 | 1,104 | 1,062 |   812 |   765 |   955 | 1,106 |

## Async serial

One awaited call at a time, such as `await cache.aget(...)`. The gap to sync
direct is asyncio loop overhead. Django's built-in `RedisCache` has no native
async path, so it also pays for `sync_to_async`.

| Adapter | get | get-miss | set | mget | mset | incr | delete |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|
| redis-py            | 1,785 | 1,863 | 1,677 | 1,170 |   830 | 1,857 |   891 |
| redis-py+hiredis    | 1,776 | 1,850 | 1,686 | 1,163 |   840 | 1,842 |   894 |
| valkey-py           | 2,012 | 2,107 | 1,976 | 1,288 |   836 | 2,138 | 1,020 |
| valkey-py+libvalkey | 2,031 | 2,138 | 1,976 | 1,294 |   836 | 2,135 | 1,026 |
| valkey-glide        | 3,251 | 3,634 | 3,291 | 1,640 | 1,634 | 3,680 | 1,735 |
| django (builtin)    | 1,903 | 2,016 | 1,879 |   193 |   189 |   971 |   970 |

## Async concurrent (50 in flight)

`asyncio.gather` of 50 calls at a time. Connection counts stay flat between
phases on every adapter (`Δ = 0`).

| Adapter | get | get-miss | set | mget | mset | incr | delete | conns peak |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|-----------:|
| redis-py            |  2,076 |  2,199 |  2,007 | 1,323 |   910 |  2,114 |   967 |  56 |
| redis-py+hiredis    |  2,074 |  2,198 |  1,998 | 1,318 |   897 |  2,110 |   961 | 106 |
| valkey-py           |  2,434 |  2,530 |  2,290 | 1,186 |   898 |  2,515 | 1,114 |  58 |
| valkey-py+libvalkey |  2,421 |  2,540 |  2,292 | 1,199 |   918 |  2,522 | 1,108 | 108 |
| valkey-glide        |  9,903 | 12,208 |  9,770 | 1,949 | 2,541 | 11,950 | 2,588 | 109 |
| django (builtin)    |  2,007 |  2,170 |  2,058 |   208 |   206 |  1,058 |   991 | 107 |

## ASGI full-stack

`granian` (4 workers) and `httpx` (100 concurrent clients, 20 s) against a
view that makes six async cache calls per request. The harness samples server
RSS and `connected_clients` every 5 s. The req/s column is noisy, so read it
in rough buckets (~600, ~400, ~200).

| Adapter | req/s | avg ms | p99 ms | RSS peak (MiB) | conns peak | conns settled |
|---------|------:|-------:|-------:|---------------:|-----------:|--------------:|
| redis-py            | 413 | 240.6 | 1,507.2 | 435 | 209 | 209 |
| redis-py+hiredis    | 586 | 170.1 | 2,421.7 | 427 | 209 | 209 |
| valkey-py           | 380 | 261.8 | 1,467.0 | 434 | 220 | 220 |
| valkey-py+libvalkey | 626 | 159.2 | 1,180.8 | 434 | 216 | 216 |
| valkey-glide        | 324 | 306.0 | 1,681.9 | 438 | 115 | 115 |
| django (builtin)    | 200 | 494.0 | 2,421.7 | 523 | 316 | 316 |

## ORM cache vs django-cachalot

The [ORM cache](../user-guide/orm-cache.md) derives from django-cachalot 2.9.1.
The contenders are the database alone (`none`), `cachalot`, the ORM cache
(`cachex`), and both over a
[`TrackingCache`](../user-guide/composite-backends.md) (`+tracking`), which
keeps local copies of what each process read.

- The machine above on Linux 7.0, CPython 3.14.2 (free-threaded build, with
  the GIL that libvalkey turns back on), Django 6.1.1, psycopg 3.3.6
- PostgreSQL 18 with `fsync=off` and Valkey 9 in local Docker, both libraries
  on the `valkey-py+libvalkey` backend
- Every round trip 250 µs longer than on the local Docker network, as within
  a cloud availability zone, except in the clock skew runs

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
uv run pytest benchmarks/ -c benchmarks/pytest.ini

# A single slice
uv run pytest benchmarks/test_throughput.py::test_adapters_sync \
  -c benchmarks/pytest.ini

# The ORM cache comparison, with 250 µs added to every round trip
BENCH_NET_DELAY_US=250 uv run pytest benchmarks/test_orm.py \
  -c benchmarks/pytest.ini
```

`benchmarks/README.md` lists the slices, the knobs (`N_OPS`, `K_RUNS`,
`WARMUP_KEYS`, `MGET_BATCH`) and the full methodology. `BENCH_NET_DELAY_US`
delays everything the server containers send, using `tc netem` from a helper
container, so it needs no host privileges.
