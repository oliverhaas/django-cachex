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
These benchmarks check whether each one serves what the database holds, under
races and clock skew, and then measure how fast each one is.

- AMD Ryzen 9 5950X (16C/32T) · 32 GiB RAM · Ubuntu 24.04 · Linux 7.0
- CPython 3.14.2 (free-threaded build, with the GIL that libvalkey turns back
  on), Django 6.1.1, psycopg 3.3.6, django-cachalot 2.9.1
- PostgreSQL 18 and Valkey 9 in local Docker, with both libraries on the
  `valkey-py+libvalkey` backend. PostgreSQL runs with `fsync=off`, since the
  time a commit waits for the disk varies from run to run. The races that
  need slow commits get a fixed delay instead.
- Every round trip to Valkey and PostgreSQL takes 250 µs longer than on the
  local Docker network, as within a cloud availability zone, except in the
  clock skew runs. [Network delay](#network-delay) compares the results with
  no delay and with 1 ms.
- Each contender runs in worker processes of its own.

The contenders are the database alone (`none`), `cachalot`, the ORM cache
(`cachex`), and both of them over a
[`TrackingCache`](../user-guide/composite-backends.md) (`+tracking`), which
keeps local copies of what each process read.

### Correctness

Each case is a way a cached result can differ from the database, and the
check compares what the contender serves with an uncached read. The cases are
the known differences between the two libraries, not a sample of typical
queries, so the score says which patterns to look for in a project, not how
often cachalot serves wrong rows. The database alone passes all 12.

| Case | cachalot | ORM cache |
|------|----------|-----------|
| Another process reads while an autocommit `UPDATE` runs | stale | correct |
| An `on_commit()` hook hands the new row to another process | stale | correct |
| Subquery nested in a `filter()` expression | stale | correct |
| `Now()` nested in a `filter()` expression | stale | correct |
| `order_by(Random())` | same rows every time | correct |
| JSON filter values that differ after 35 characters | another query's rows | correct |
| Rows the database deletes in cascade (`DB_CASCADE`) | stale | correct |
| Raw `TRUNCATE` | stale | correct |
| Raw `REFRESH MATERIALIZED VIEW` | stale | correct |
| Raw `UPDATE` of a table named in mixed case | stale | correct |
| Raw `INSERT` into a table Django does not manage | stale | correct |
| A `REPEATABLE READ` transaction reads a row changed after its snapshot | stale | correct |
| **Correct** | **0 of 12** | **12 of 12** |

Why cachalot fails them:

- It invalidates before an autocommit statement runs, so a read between the
  invalidation and the commit stores the old row as newer than the write. In
  `atomic()`, it invalidates only after every `on_commit()` hook has run.
- It looks for subqueries and `Now()` only as the direct operands of a filter,
  not inside expressions such as `Coalesce()` or `Now() + timedelta(...)`. It
  refuses `order_by("?")` but caches `order_by(Random())`.
- Its cache key holds the `str()` of each parameter. psycopg 3 shortens that
  of a long JSON value to its first 35 characters and its length, so two
  values of the same length that agree in those characters share a key.
- The database deletes the `DB_CASCADE` children itself, and cachalot
  invalidates only the parent's table.
- After raw SQL, it invalidates only if the SQL contains `update`, `insert`,
  `delete`, `alter`, `create` or `drop`, and only the tables of managed
  models and those in `CACHALOT_ADDITIONAL_TABLES`, matched against the
  lowercased SQL.
- When a transaction commits, it stores what the transaction read in the
  shared cache, even what a `REPEATABLE READ` transaction read from its old
  snapshot.

`CACHALOT_FINAL_SQL_CHECK = True` fixes the nested subquery, and listing the
unmanaged table in `CACHALOT_ADDITIONAL_TABLES` fixes the raw `INSERT`, for 2
of 12. Listing the mixed-case table does not help, since cachalot compares
the name as written with the lowercased SQL.

### Races

Four reader processes read a counter row while a writer process updates it
500 times, at random times 10 ms apart on average. A write that falls due
while the one before it still runs starts right after it. The writer
announces each version after it is committed: right after the statement under
autocommit, and from an `on_commit()` hook in `atomic()`.

- A read is stale if it returns a version older than one announced before the
  read began.
- A write is hidden if, just before the next write, the cache still serves
  the version it overwrote. Reads that raced a write can store their results
  for a few milliseconds after it, so the writer checks only the writes that
  the next one follows by 5 ms or more, 160 to 260 of the 500 depending on
  how long a write takes.
- The hit ratio is the share of reads that sent no query.

The scenarios vary the transaction mode, the commit time (a deferred trigger
adds 2 ms, like a commit waiting for a disk), how long the `on_commit()`
hooks after the announcing one take, and whether the readers read back to
back or once per millisecond.

Stale reads per 1,000 reads:

| Scenario | cachalot | cachex | cachalot+tracking | cachex+tracking |
|----------|---------:|-------:|------------------:|----------------:|
| Autocommit                      |  132 | 0 |  504 | 0 |
| Autocommit, 2 ms commits        |  755 | 0 |  602 | 0 |
| `atomic()`                      |  8.0 | 0 |  512 | 0 |
| `atomic()`, 2 ms commits        | 13.7 | 0 |  565 | 0 |
| `atomic()`, 1 ms of later hooks |  117 | 0 |  567 | 0 |
| `atomic()`, a read every 1 ms   | 12.1 | 0 | 40.4 | 0 |

Hidden writes per 1,000 checked, none of them in `atomic()`:

| Scenario | cachalot | cachex | cachalot+tracking | cachex+tracking |
|----------|---------:|-------:|------------------:|----------------:|
| Autocommit               |   146 | 0 |  62 | 0 |
| Autocommit, 2 ms commits | 1,000 | 0 | 227 | 0 |

Hit ratio:

| Scenario | cachalot | cachex | cachalot+tracking | cachex+tracking |
|----------|---------:|-------:|------------------:|----------------:|
| Autocommit                      | 95.0% | 88.7% | 99.3% | 88.6% |
| Autocommit, 2 ms commits        | 94.2% | 74.4% | 99.2% | 74.5% |
| `atomic()`                      | 94.2% | 87.7% | 99.2% | 88.0% |
| `atomic()`, 2 ms commits        | 94.2% | 72.5% | 99.2% | 72.7% |
| `atomic()`, 1 ms of later hooks | 94.2% | 87.1% | 99.2% | 87.2% |
| `atomic()`, a read every 1 ms   | 90.0% | 82.9% | 90.0% | 82.9% |

Cachalot's counts vary from run to run. In four runs, it served 110 to 154
stale reads per 1,000 under autocommit and 7.9 to 9.6 in `atomic()`, and hid
122 to 156 writes per 1,000 under autocommit. One of the four hid a write in
`atomic()`, 1 of 223 checked.

- Under autocommit, cachalot invalidates before the statement runs. A read
  until the commit stores the old row as newer than the write, and the cache
  serves it until the next write. The share of writes it hides grows with the
  commit time: 146 in 1,000 with fast commits, all of them with 2 ms commits.
- When a write follows the one before it straight on, its invalidation can
  come before a read that raced the first one stores the old row, which then
  outlives the second write too. The races thus pile up where writes to a
  table come close together, as in a loop of updates.
- In `atomic()`, cachalot invalidates after the commit, but only after all
  `on_commit()` hooks have run. A process that a hook hands the new version
  to, say through a task queue, is served the old one until then.
- It also invalidates in two steps a round trip apart, first with the time
  the statement started and then with the current time. If a write just
  before made the readers miss, a read that queried before the commit but was
  stamped after the statement started passes the first step, and the cache
  serves its old row until the second.
- `cachalot+tracking` serves hits from the process's memory, without a round
  trip. A listener thread evicts a copy when the server reports a write, but
  it needs the GIL, and in a process busy with reads it can wait up to the
  5 ms switch interval for it. Readers going back to back thus read a stale
  row about half the time, in `atomic()` too. It hides fewer writes than
  cachalot alone because its readers often learn of a write only after the
  commit, when the database already returns the new row.
- The ORM cache served no stale read and hid no write. While a write to a
  table runs or commits, a lease sends the table's queries to the database.
  That costs hit ratio in proportion to the time writes take: 6 to 7 points
  with fast commits, 20 to 22 with 2 ms commits. Over a `TrackingCache`, it
  checks every local copy with the server, so it never serves one after a
  write.
- Back to back, a faster read makes more reads per write and so a higher hit
  ratio. The last row compares hit ratios at the same read rate.

### Clock skew

Cachalot stamps each result it stores, and each invalidation, with the
`time.time()` of the process that makes it, and serves a result stamped
later than the last invalidation of its tables. If a reader's clock runs
ahead by Δ, a result it stores right after a write outlives every write in
the next Δ, and the cache hides them. If the writer's clock runs ahead,
results stored in the Δ after a write count as older than it and are not
served.

One reader starts a read every millisecond, against a writer that commits
`atomic()` blocks at random times 20 ms apart on average, or 60 ms in two
rows. The models assume writes at random times. With λ = skew / mean gap
between writes, a reader ahead hides λ / (1 + λ) of the writes, and a writer
ahead keeps e^(−λ) of the hit ratio it has in sync. Each row has 500 writes,
of which the writer checks those the next write follows by 5 ms or more, as in
the races, so a measured share is good to about ±25 per 1,000. These runs add
no network delay, so even a miss takes less than the millisecond between
reads. [Network delay](#network-delay) shows what changes with it.

Reader ahead, hidden writes per 1,000 checked:

| Skew | Mean gap | λ | Measured | Model |
|-----:|---------:|--:|---------:|------:|
| 0     | 20 ms | 0    |   0 |   0 |
| 2 ms  | 20 ms | 0.1  | 103 |  91 |
| 5 ms  | 20 ms | 0.25 | 215 | 200 |
| 10 ms | 20 ms | 0.5  | 335 | 333 |
| 20 ms | 20 ms | 1    | 500 | 500 |
| 40 ms | 20 ms | 2    | 633 | 667 |
| 15 ms | 60 ms | 0.25 | 187 | 200 |
| 60 ms | 60 ms | 1    | 492 | 500 |

At small skews the reader hides a few more writes than the model predicts. A
write that falls due while the one before it still runs commits right after
it, often just after the reader's miss has queried, and so lands in the next
Δ more often than a write at a random time.

Writer ahead, hit ratio:

| Skew | λ | Measured | Model |
|-----:|--:|---------:|------:|
| 0     | 0    | 95.1% | 95.1% |
| 2 ms  | 0.1  | 87.3% | 86.1% |
| 5 ms  | 0.25 | 75.3% | 74.1% |
| 10 ms | 0.5  | 59.0% | 57.7% |
| 20 ms | 1    | 36.3% | 35.0% |
| 40 ms | 2    | 12.6% | 12.9% |

The ORM cache keeps a generation per table on the cache server and reads no
client's clock. With either clock 40 ms ahead, it hid no write and kept its
in-sync hit ratio of 93.8%, within 0.2 points.

The share of hidden writes depends only on the skew relative to how often a
table is written, so the model carries over to real clocks and write rates.
Hidden writes per 1,000, by how far a reader's clock runs ahead and how often
the table is written on average:

| Skew | Write every 1 s | 1 min | 15 min | 1 h |
|-----:|----------------:|------:|-------:|----:|
| 10 ms  |   9.9 | 0.17 | 0.011 | 0.003 |
| 100 ms |    91 |  1.7 |  0.11 | 0.028 |
| 1 s    |   500 |   16 |   1.1 |  0.28 |
| 10 s   |   909 |  143 |    11 |   2.8 |

So a process whose clock runs 1 s ahead, reading a table written every 15
minutes or so, hides about 1 write in 1,000. A hidden write does not pass
quickly: readers get the old rows until the table's next write after the
skew has passed, for about as long as the table goes between writes. A
writer's clock ahead costs only hits: 1 s against a write every minute loses
under 2% of them.

### Speed

Latency in µs, as the median with the p95 in brackets, over 2,000 calls each.
For `none`, the hit and the miss are the query alone.

| Contender | Hit | Miss | Autocommit write | `atomic()` write |
|-----------|----:|-----:|-----------------:|-----------------:|
| none              | 566 (647) |   612 (676)   |   563 (643)   | 1,244 (1,313) |
| cachalot          | 500 (528) | 1,453 (1,549) |   990 (1,102) | 2,072 (2,286) |
| cachex            | 541 (628) | 1,489 (1,615) | 1,354 (1,450) | 2,057 (2,220) |
| cachalot+tracking | 136 (148) | 1,508 (1,632) | 1,010 (1,117) | 2,128 (2,259) |
| cachex+tracking   | 531 (592) | 1,516 (1,611) | 1,377 (1,456) | 2,082 (2,199) |

Hits by the number of rows they return, median in µs:

| Contender | 1 row | 10 rows | 100 rows | 1,000 rows |
|-----------|------:|--------:|---------:|-----------:|
| none              | 577 | 681 | 1,670 | 12,258 |
| cachalot          | 521 | 539 |   784 |  3,237 |
| cachex            | 530 | 566 |   843 |  3,341 |
| cachalot+tracking | 143 | 170 |   413 |  2,817 |
| cachex+tracking   | 544 | 565 |   826 |  3,216 |

A mixed workload of 5,000 operations reads 100 rows by one of 20 queries, or
for 1% or 10% of the operations updates a random row of the table instead.
Every cache hit 84.5% of the reads at 1% writes and 29.4% at 10%. Operations
per second:

| Contender | 1% writes | 10% writes |
|-----------|----------:|-----------:|
| none              |   582 | 618 |
| cachalot          |   892 | 486 |
| cachex            |   868 | 473 |
| cachalot+tracking | 1,198 | 496 |
| cachex+tracking   |   866 | 473 |

Of 100 cached queries, a `migrate` with nothing to apply left none cached
with cachalot and all 100 with the ORM cache.

- Both libraries make one round trip to Valkey for a hit, two for a miss and
  two for the commit of a transaction that wrote. For an autocommit write,
  cachalot makes one and the ORM cache two, a lease before the statement and
  its release after, so the ORM cache's takes about 360 µs longer.
- The ORM cache checks the leases and generations in a Lua script on the
  server, and its hits and misses take about 40 µs longer than cachalot's.
- `cachalot+tracking` serves hits from the process's memory without a round
  trip, in about a quarter of the time, which is also what causes its stale
  reads. The ORM cache over a `TrackingCache` still asks the server whether
  its local copy is current, so it only saves the transfer of the result,
  which shows at 1,000 rows.
- A hit on one row takes a round trip, as the query does, so it saves only
  the database's work and takes about 90% of the query's time. The cache pays
  off with larger results: a hit on 100 rows takes about half the query's
  time, and one on 1,000 rows about a quarter.
- Both invalidate whole tables, so each write makes every cached query on its
  table miss. At 1% writes, the caches serve about 1.5 times the database's
  throughput, and `cachalot+tracking` twice. At 10% writes, where a miss takes
  three round trips to the query's one, they serve about a fifth less.
- Cachalot invalidates every table after each `migrate`, so a deploy that runs
  one empties the cache. With the ORM cache, a `migrate` that applies nothing
  invalidates nothing.

### Network delay

Without added delay, a round trip takes about 60 µs to Valkey and 80 µs to
PostgreSQL. Adding 250 µs made each round trip about 265 µs longer, and
adding 1 ms about 1,050 µs, and each operation took its time without delay
plus that much per round trip. Median latency in µs:

| Operation | Contender | Round trips | No delay | 250 µs | 1 ms |
|-----------|-----------|------------:|---------:|-------:|-----:|
| Hit              | none (the query)  | 1 | 296 |   566 | 1,365 |
| Hit              | cachalot          | 1 | 238 |   500 | 1,278 |
| Hit              | cachex            | 1 | 274 |   541 | 1,296 |
| Hit              | cachalot+tracking | 0 | 135 |   136 |   137 |
| Miss             | cachalot          | 3 | 672 | 1,453 | 3,823 |
| Miss             | cachex            | 3 | 729 | 1,489 | 3,855 |
| Autocommit write | none              | 1 | 295 |   563 | 1,336 |
| Autocommit write | cachalot          | 2 | 458 |   990 | 2,528 |
| Autocommit write | cachex            | 3 | 560 | 1,354 | 3,732 |
| `atomic()` write | none              | 3 | 443 | 1,244 | 3,622 |
| `atomic()` write | cachalot          | 5 | 750 | 2,072 | 6,001 |
| `atomic()` write | cachex            | 5 | 756 | 2,057 | 6,009 |

A hit saves the database's work but not the round trip, so the slower the
round trip, the less a hit on a small result saves. At 1 ms, a hit on one row
took 95% of the query's time and one on 1,000 rows a third, against 82% and
a quarter without delay. The mixed workload, in operations per second at 1%
writes, and at 10% in brackets:

| Contender | No delay | 250 µs | 1 ms |
|-----------|---------:|-------:|-----:|
| none              |   682 (734) |   582 (618) | 392 (410) |
| cachalot          | 1,270 (688) |   892 (486) | 464 (255) |
| cachex            | 1,223 (682) |   868 (473) | 451 (246) |
| cachalot+tracking | 1,410 (678) | 1,198 (496) | 812 (269) |
| cachex+tracking   | 1,243 (686) |   866 (473) | 455 (246) |

At 1% writes, cachalot served 1.9 times the database's throughput without
delay, 1.5 times at 250 µs and 1.2 times at 1 ms, and the ORM cache 3 to 4%
less than cachalot. `cachalot+tracking` served twice the database's at every
delay. At 10% writes, the caches served 6 to 8% less than the database
without delay, 20 to 24% less at 250 µs and 34 to 40% less at 1 ms.

A race lasts a round trip or a few, so the longer the round trips, the more
reads fall in one. Cachalot's stale reads per 1,000 reads, where the ORM
cache served none at any delay:

| Scenario | No delay | 250 µs | 1 ms |
|----------|---------:|-------:|-----:|
| Autocommit                      |  104 |  132 | 247 |
| Autocommit, 2 ms commits        |  770 |  755 | 708 |
| `atomic()`                      | 0.19 |  8.0 |  88 |
| `atomic()`, 2 ms commits        | 0.56 | 13.7 | 111 |
| `atomic()`, 1 ms of later hooks |  100 |  117 | 233 |
| `atomic()`, a read every 1 ms   | 0.10 | 12.1 |  90 |

Under autocommit, cachalot hid 108, 146 and 275 writes per 1,000 checked,
and with 2 ms commits nearly all of them at every delay.
`cachalot+tracking` read a stale row about half the time back to back, up to
three times in four at 1 ms, and at a read every 1 ms 18, 40 and 222 times
per 1,000.

The ORM cache sends a table's reads to the database for as long as a write to
it runs, and the longer the round trips, the longer a write runs. Cachalot in
`atomic()` serves them from the cache until after the commit, stale ones
included. So the ORM cache's hit ratio falls faster with delay. At a read
every 1 ms:

| Contender | No delay | 250 µs | 1 ms |
|-----------|---------:|-------:|-----:|
| cachalot          | 94.1% | 90.0% | 83.2% |
| cachex            | 92.1% | 82.9% | 60.4% |
| cachalot+tracking | 91.5% | 90.0% | 90.2% |
| cachex+tracking   | 91.4% | 82.9% | 60.5% |

At 1 ms a hit takes longer than the millisecond between reads, so there the
readers of all but `cachalot+tracking` read back to back, the ORM cache's
about a fifth less often than cachalot's.

The clock skew runs with a reader ahead, in hidden writes per 1,000 checked,
with writes 20 ms apart on average:

| Skew | Model | No delay | 250 µs | 1 ms |
|-----:|------:|---------:|-------:|-----:|
| 2 ms  |  91 | 103 | 128 |   0 |
| 5 ms  | 200 | 215 | 251 | 268 |
| 10 ms | 333 | 335 | 357 | 375 |
| 20 ms | 500 | 500 | 524 | 498 |
| 40 ms | 667 | 633 | 656 | 645 |

A write takes about 2 ms at 250 µs and 6 ms at 1 ms, so more writes fall due
while the one before still runs, and a reader 10 ms ahead or less hid up to
68 per 1,000 more writes than the model, for the reason given under
[Clock skew](#clock-skew). At 1 ms, even a write that follows straight on
commits too late for a 2 ms lead, and that reader hid none. With writes 60 ms
apart, all three delays came within 13 of the model.

A writer ahead kept more hits than the model predicts: with λ = 1, it kept
52.7% at 250 µs and 55.9% at 1 ms, where the model gives 35.0% and 34.1%.
Every read in the Δ after a write misses, and with delay a miss takes longer
than the millisecond between reads, so fewer reads fall in that time. The ORM
cache hid no write at any delay with either clock 40 ms ahead, and kept its
in-sync hit ratio within 0.2 points.

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
