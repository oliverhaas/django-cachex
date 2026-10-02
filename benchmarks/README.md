# Benchmarks

Throughput and memory comparison across cache adapter/parser/serializer/compressor
combos, and the ORM cache against django-cachalot.

Not part of the regular test suite. It runs separately because it spins up
its own Redis, Valkey and PostgreSQL containers and is slow on purpose (timing
accuracy depends on letting workloads run).

## What gets compared

**Adapters** (with default pickle serializer):

- `redis-py`: pure-Python parser
- `redis-py+hiredis`: C parser
- `valkey-py`: pure-Python parser
- `valkey-py+libvalkey`: C parser
- `valkey-glide`: `valkey/valkey-glide` Python wheels, Rust-cored with a
  thread-pool transport. Only available on cp314 GIL. No cp314t
  (free-threaded) wheels yet.
- `django (builtin)`: Django's official built-in `django.core.cache.backends.redis.RedisCache`
  (since 4.0). Not the third-party `jazzband/django-redis` package, which is
  unrelated. Included as an external reference point.

**Serializers** (with the `valkey-py+libvalkey` adapter, the fastest one that
is always installed):

- `pickle`: stdlib default
- `json`: Django's `DjangoJSONEncoder`
- `msgpack`: pure-Python `msgpack`
- `orjson`: Rust-backed JSON
- `ormsgpack`: Rust-backed MessagePack

**Compressors** get two views, both with `valkey-py+libvalkey` + `pickle`:

- *Macro* (`test_compressors_macro`): end-to-end Django cache ops on a
  ~14 KiB queryset-shaped payload. Captures the cost of compress/decompress
  against the savings from sending fewer bytes over the wire.
- *Micro* (`test_compressors_micro`): pure compress/decompress in a tight
  loop. Reports output ratio and MB/s. No adapter, no container.

Compressor candidates: `none`, `zlib`, `gzip`, `lzma`, `lz4`, `zstd`.

**Request cycle** (`test_adapters_request_cycle`) runs the same workload as
`test_adapters_sync`, but each cache op runs inside a real Django request cycle:
`Client().get(url)` → URL resolve → `CommonMiddleware` → view function →
response → `request_finished` signal. The view in [urls.py](urls.py) does
exactly one cache op per request, so ops/sec is on the same scale as
`test_adapters_sync` and you can read off the per-op overhead Django adds. Adapter
ids are suffixed with `#req` in the final summary so the request-cycle rows
sit next to their direct counterparts.

**ASGI** (`test_adapters_asgi`) is a full-stack benchmark in the shape of
[`django-vcache`'s `bench_compare.py`](https://gitlab.com/glitchtip/django-vcache/-/blob/main/bench_compare.py):

- Spawns a real **`granian`** ASGI server (4 workers) per adapter
- Drives load with **`httpx.AsyncClient`** (100 concurrent connections,
  20 second duration by default; bump `ASGI_CONCURRENCY` /
  `ASGI_DURATION_S` in `test_throughput.py` for hero numbers)
- Each request hits `/bench/mixed/`, which does six async cache ops:
  `aget`, `aget_many`(3 keys), `aset`, `aset` (large, ~2.5 KiB to trigger
  compression), `aincr`, `aget` (large)
- Samples server RSS and Valkey/Redis `connected_clients` every 5 seconds
  during the run; reports init / peak / final / settled (post-cooldown)

This is the only benchmark that reliably surfaces connection-pool growth
under realistic load. The sync direct, async direct, and request-cycle tests
all show stable connection counts because the workload is too well-behaved
to stress the pool. The ASGI benchmark hits the pool from four worker
processes simultaneously, which is enough to expose any per-call client
pattern.

To match django-vcache's exact methodology (which also adds simulated
network latency to amplify connection-lifetime issues), run the script
inside a Docker container with `--cap-add NET_ADMIN` and apply
`tc qdisc add dev eth0 root netem delay 1ms` against the cache server's
interface. Without latency the directional ranking is the same; with it,
the gaps widen.

**Async** gets two views via `aget` / `aset` / `aget_many` / etc.:

- *Serial* (`test_adapters_async_serial`): `await cache.aget(...)` one op at
  a time. Direct comparison with sync; the gap reveals asyncio-loop
  overhead and, for backends without native async, the cost of Django's
  `sync_to_async` fallback. Ids suffixed with `#async`.
- *Concurrent* (`test_adapters_async_concurrent`): `asyncio.gather` of
  `ASYNC_CONCURRENCY` (default 50) ops in flight. Stresses the connection
  pool: peak connections jump to roughly the concurrency level for backends
  with native async + per-op pool checkout. The intended use is also to
  hunt for connection leaks (peak should plateau and `Δ` should stay 0;
  if `Δ` grows phase over phase, the backend is leaking). Ids suffixed
  with `#asyncN` where N is the concurrency level.

## What gets measured

Adapter / serializer / compressor-macro / request-cycle tests run a
seven-phase workload: `get`, `get-miss`, `set`, `mget` (10-key batch),
`mset` (10-key batch), `incr`, `delete`. Each phase runs `N_OPS=1000`
operations, repeated `K_RUNS=10` times.

Per-phase timings are reported as median ms and ops/sec across runs. Per-run
metrics include Python peak memory (`tracemalloc`) and server memory delta
(`INFO memory.used_memory`). Connections are sampled before the workload
(baseline) and after every phase across every run; the summary reports peak
and `Δ` (peak − baseline).

Compressor-micro tests measure `compress(payload)` and
`decompress(compressed)` in a tight loop on a fixed payload, reporting ratio
and MB/s.

Knobs in [runner.py](runner.py): `N_OPS`, `K_RUNS`, `WARMUP_KEYS`, `MGET_BATCH`.

## Running

```console
# Full matrix (adapters + serializers + compressors + ORM cache)
uv run pytest benchmarks/ -c benchmarks/pytest.ini

# Just one slice
uv run pytest benchmarks/test_throughput.py::test_adapters_sync                  -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_serializers              -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_compressors_macro        -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_compressors_micro        -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_adapters_request_cycle    -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_adapters_async_serial     -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_adapters_async_concurrent -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_adapters_asgi             -c benchmarks/pytest.ini

# A single config
uv run pytest 'benchmarks/test_throughput.py::test_adapters_sync[valkey-glide]' -c benchmarks/pytest.ini

# Every round trip to the servers 250 µs longer (see Notes)
BENCH_NET_DELAY_US=250 uv run pytest benchmarks/ -c benchmarks/pytest.ini
```

`test_compressors_micro` is the only test that doesn't need Docker, which
makes it useful for quick algorithm comparisons on a laptop without
containers running.

A summary table prints at the end of the session. Reference results from a
full run, and the machine they come from, are in
[docs/reference/benchmarks.md](../docs/reference/benchmarks.md).

## Notes

- **No xdist.** Parallel runs make timings noisy; benchmarks run sequentially.
- **Redis vs Valkey.** Each adapter is paired with its natural server
  (redis-py → redis, valkey-py / valkey-glide → valkey,
  django (builtin) → redis). Cross-pairings are intentionally not in the
  matrix: both servers speak the same protocol, so the comparison is
  mostly a wash.
- **Warmup.** Each phase runs an untimed pass before the timed runs to prime
  connections, server keyspace, and lazy serializer state.
- **Memory caveat.** `used_memory` is whole-server, so concurrent activity on
  the same container distorts the delta. Run alone for clean numbers.
- **Network delay.** A round trip to a local container takes about 60 µs to
  Valkey and 80 µs to PostgreSQL, where one within a cloud availability zone
  takes roughly 0.1 to 0.5 ms. `BENCH_NET_DELAY_US` delays everything the
  Redis, Valkey and PostgreSQL containers send by that many microseconds,
  which adds as much to each round trip. A short-lived `alpine:3` container
  joins each server's network namespace with `NET_ADMIN`, installs
  `iproute2-tc` and adds a `netem` delay. That needs no host privileges, but
  the install needs network access.

## ORM cache vs django-cachalot (`test_orm.py`)

Compares the ORM cache with django-cachalot 2.9.1, the release it derives
from. Both run on the `valkey-py+libvalkey` backend against Valkey 9, so only
the ORM layer differs, and on a PostgreSQL 18 container the session starts.
PostgreSQL runs with `fsync=off`, because a durable commit waits for the disk
by an amount that changes from run to run. The races that need a slower
commit get a fixed one.

Each contender runs in worker processes of its own
([orm_worker.py](orm_worker.py)), since both patch the ORM. Separate
processes also give each one its own clock and connections.

- `none`: the database alone.
- `cachalot` and `cachex`.
- `cachalot+tracking` and `cachex+tracking`: the same over a
  `TrackingCache`, which keeps local copies of the values each process read.

**Correctness** (`test_scorecard`) runs the 12 cases of
[ormbench/scorecard.py](ormbench/scorecard.py). Each is a way a cached result
can differ from the database, and the check compares what the contender serves
with an uncached read.

**Races** (`test_races`) run 4 reader processes against a writer process
that updates a counter row 500 times, at Poisson-distributed times 10 ms
apart on average. A write that falls due while the one before it still runs
starts right after it. The writer announces each version through shared memory
after it is committed, inside `atomic()` from an `on_commit()` hook. The
scenarios vary the transaction mode, the commit time (a deferred trigger
sleeps 2 ms, like a commit waiting for a disk), how long later `on_commit()`
hooks run, and whether readers read back to back or once per millisecond.

- A read is stale if it returns a version older than one announced before it
  started.
- A write is hidden if the cache still serves the version it overwrote just
  before the next write. For 5 ms after a write, reads that raced it can
  still store the old version or the new one, so the writer checks only the
  writes that the next one follows by at least that, and hidden writes count
  per 1,000 checked. The check goes through the cache only: a query the cache
  cannot serve raises instead of running, so the check stores nothing.
- The hit ratio is the share of reads that sent no query.

**Clock skew** (`test_clock_skew`) runs one reader, starting a read every
millisecond, against a writer committing `atomic()` blocks 20 ms apart on
average, with `time.time()` shifted in one of them. With λ = skew / mean gap
between writes, cachalot hides λ / (1 + λ) of the writes when the reader's
clock is ahead, and keeps e^(−λ) of its in-sync hit ratio when the writer's
clock is ahead. The table prints these models next to the measurements. The
ORM cache reads no client clock, so its rows only check that.

**Speed:**

- `test_latency`: median and p95 of a hit, a miss, an autocommit write and a
  one-statement `atomic()` block, 2,000 calls each after 200 warmup calls.
- `test_sizes`: hits returning 1, 10, 100 and 1,000 rows.
- `test_mixed`: reads of 100 rows by one of 20 queries, with 1% or 10% of the
  operations updating a random row of the table instead.
- `test_migrate_noop`: how many of 100 cached queries a `migrate` with
  nothing to apply leaves cached.

```console
uv run pytest benchmarks/test_orm.py -c benchmarks/pytest.ini

# One slice
uv run pytest benchmarks/test_orm.py::test_clock_skew -c benchmarks/pytest.ini

# Every round trip 250 µs longer, as in the reference results
BENCH_NET_DELAY_US=250 uv run pytest benchmarks/test_orm.py -c benchmarks/pytest.ini
```

The whole file takes about 8 minutes, 9 with `BENCH_NET_DELAY_US=250` and 12
with `BENCH_NET_DELAY_US=1000`. The results and what they show are in
[docs/reference/benchmarks.md](../docs/reference/benchmarks.md#orm-cache-vs-django-cachalot).
