# Benchmarks

Throughput and memory comparison across cache driver/parser/serializer/compressor
combos.

Not part of the regular test suite — runs separately because it spins up its
own Redis and Valkey containers and is slow on purpose (timing accuracy
depends on letting workloads run).

## What gets compared

**Drivers** (with default pickle serializer):

- `redis-py` — pure-Python parser
- `redis-py+hiredis` — C parser
- `valkey-py` — pure-Python parser
- `valkey-py+libvalkey` — C parser
- `rust-valkey` — our Rust extension driver
- `django (builtin)` — Django's official built-in `django.core.cache.backends.redis.RedisCache`
  (since 4.0). Not the third-party `jazzband/django-redis` package — that one is
  unrelated. Included as an external reference point.

**Serializers** (with `rust-valkey` driver, since driver overhead is smallest there):

- `pickle` — stdlib default
- `json` — Django's `DjangoJSONEncoder`
- `msgpack` — pure-Python `msgpack`
- `orjson` — Rust-backed JSON
- `ormsgpack` — Rust-backed MessagePack

**Compressors** — two views, both with `rust-valkey` + `pickle`:

- *Macro* (`test_compressors_macro`) — end-to-end Django cache ops on a
  ~14 KiB queryset-shaped payload. Captures the cost of compress/decompress
  against the savings from sending fewer bytes over the wire.
- *Micro* (`test_compressors_micro`) — pure compress/decompress in a tight
  loop. Reports output ratio and MB/s. No driver, no container.

Compressor candidates: `none`, `zlib`, `gzip`, `lzma`, `lz4`, `zstd`.

**Request cycle** (`test_adapters_request_cycle`) — same workload as
`test_adapters_sync`, but each cache op runs inside a real Django request cycle:
`Client().get(url)` → URL resolve → `CommonMiddleware` → view function →
response → `request_finished` signal. The view in [urls.py](urls.py) does
exactly one cache op per request, so ops/sec is on the same scale as
`test_adapters_sync` and you can read off the per-op overhead Django adds. Driver
ids are suffixed with `#req` in the final summary so the request-cycle rows
sit next to their direct counterparts.

**ASGI** (`test_adapters_asgi`) — full-stack benchmark in the shape of
[`django-vcache`'s `bench_compare.py`](https://gitlab.com/glitchtip/django-vcache/-/blob/main/bench_compare.py):

- Spawns a real **`granian`** ASGI server (4 workers) per driver
- Drives load with **`httpx.AsyncClient`** (100 concurrent connections,
  20 second duration by default; bump `ASGI_CONCURRENCY` /
  `ASGI_DURATION_S` in `test_throughput.py` for hero numbers)
- Each request hits `/bench/mixed/`, which does six async cache ops —
  `aget`, `aget_many`(3 keys), `aset`, `aset` (large, ~2.5 KiB to trigger
  compression), `aincr`, `aget` (large)
- Samples server RSS and Valkey/Redis `connected_clients` every 5 seconds
  during the run; reports init / peak / final / settled (post-cooldown)

This is the only benchmark that reliably surfaces connection-pool growth
under realistic load — sync direct, async direct, and request-cycle tests
all show stable connection counts because the workload is too well-behaved
to stress the pool. The ASGI benchmark hits the pool from four worker
processes simultaneously, which is enough to expose any per-call client
pattern.

To match django-vcache's exact methodology (which also adds simulated
network latency to amplify connection-lifetime issues), run the script
inside a Docker container with `--cap-add NET_ADMIN` and apply
`tc qdisc add dev eth0 root netem delay 1ms` against the cache server's
interface. Without latency the directional ranking is the same; with it,
the magnitude grows dramatically.

**Async** — two views via `aget` / `aset` / `aget_many` / etc.:

- *Serial* (`test_adapters_async_serial`) — `await cache.aget(...)` one op at
  a time. Direct comparison with sync; the gap reveals asyncio-loop
  overhead and, for backends without native async, the cost of Django's
  `sync_to_async` fallback. Ids suffixed with `#async`.
- *Concurrent* (`test_adapters_async_concurrent`) — `asyncio.gather` of
  `ASYNC_CONCURRENCY` (default 50) ops in flight. Stresses the connection
  pool: peak connections jump to roughly the concurrency level for backends
  with native async + per-op pool checkout. The intended use is also to
  hunt for connection leaks (peak should plateau and `Δ` should stay 0;
  if `Δ` grows phase over phase, the backend is leaking). Ids suffixed
  with `#asyncN` where N is the concurrency level.

## What gets measured

Driver / serializer / compressor-macro / request-cycle tests run a
seven-phase workload — `get`, `get-miss`, `set`, `mget` (10-key batch),
`mset` (10-key batch), `incr`, `delete` — `N_OPS=1000` operations per phase,
repeated `K_RUNS=10` times.

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
# Full matrix (drivers + serializers + compressors)
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
uv run pytest 'benchmarks/test_throughput.py::test_adapters_sync[rust-valkey]' -c benchmarks/pytest.ini
```

`test_compressors_micro` is the only test that doesn't need Docker — useful
for quick algorithm comparisons on a laptop without containers running.

A summary table prints at the end of the session.

## Notes

- **No xdist.** Parallel runs make timings noisy; benchmarks run sequentially.
- **Redis vs Valkey.** Each driver is paired with its natural server (redis-py
  → redis, valkey-py → valkey, rust → valkey). Cross-pairings are intentionally
  not in the matrix — both servers speak the same protocol, so the comparison
  is mostly a wash.
- **Warmup.** Each phase runs an untimed pass before the timed runs to prime
  connections, server keyspace, and lazy serializer state.
- **Memory caveat.** `used_memory` is whole-server, so concurrent activity on
  the same container distorts the delta. Run alone for clean numbers.

## Reference results

Snapshot of the driver matrix on a Ryzen 9 5950X / 32 GiB / Linux 6.17,
Python 3.14, Django 6.0, all servers in local Docker. Numbers shift run to
run; ordering is what matters.

### Sync direct (`test_adapters_sync`) — ops/sec

| Driver               |    get |    set |  mget |  mset |   incr | py-mem KiB |
| -------------------- | -----: | -----: | ----: | ----: | -----: | ---------: |
| redis-py             |  2,363 |  2,315 | 1,426 | 1,309 |  2,516 |        115 |
| redis-py+hiredis     |  2,412 |  2,338 | 1,540 | 1,382 |  2,570 |         57 |
| valkey-py            |  2,760 |  2,729 | 1,589 | 1,442 |  3,030 |        109 |
| valkey-py+libvalkey  |  2,782 |  2,735 | 1,684 | 1,530 |  3,038 |         51 |
| **rust-valkey**      |  9,268 | 10,844 | 2,730 | 4,461 | 12,046 |         24 |
| django (builtin)     |  2,312 |  2,295 | 1,496 | 1,364 |  1,911 |         56 |

### Django request cycle (`test_adapters_request_cycle`, `#req`) — ops/sec

One cache op per request through the full middleware/URL/view path.

| Driver               |   get |   set |  incr |
| -------------------- | ----: | ----: | ----: |
| redis-py             | 1,055 | 1,054 | 1,066 |
| valkey-py+libvalkey  | 1,198 | 1,192 | 1,227 |
| **rust-valkey**      | 1,847 | 1,864 | 1,890 |
| django (builtin)     | 1,108 | 1,078 |   966 |

### Async serial (`test_adapters_async_serial`, `#async`) — ops/sec

One `await` at a time. The Python↔Rust crossing per op doesn't amortize, so
Rust falls behind the C-parser Python drivers on this shape only.

| Driver               |   get |   set |  mget |  mset |  incr |
| -------------------- | ----: | ----: | ----: | ----: | ----: |
| redis-py+hiredis     | 1,801 | 1,734 | 1,172 |   861 | 1,894 |
| valkey-py+libvalkey  | 2,042 | 2,083 | 1,305 |   874 | 2,200 |
| **rust-valkey**      | 1,394 | 1,384 |   880 | 1,134 | 1,372 |
| django (builtin)     | 1,687 | 1,780 |   184 |   186 |   912 |

### Async concurrent at 50 (`test_adapters_async_concurrent`, `#async50`) — ops/sec

`asyncio.gather` of 50 ops in flight — the workload most Django ASGI apps
actually generate.

| Driver               |    get |    set |  mget |  mset |   incr | conns peak |
| -------------------- | -----: | -----: | ----: | ----: | -----: | ---------: |
| redis-py+hiredis     |  2,091 |  2,021 | 1,307 |   922 |  2,192 |        110 |
| valkey-py+libvalkey  |  2,419 |  2,331 | 1,428 |   909 |  2,528 |        108 |
| **rust-valkey**      | 18,801 | 25,515 | 3,175 | 6,072 | 28,567 |        108 |
| django (builtin)     |  2,029 |  2,042 |   201 |   204 |  1,025 |        111 |

### ASGI full-stack (`test_adapters_asgi`)

`granian` (4 workers) + `httpx` (100 concurrent, 20 s). Each request runs
six async cache ops — the shape closest to real production load.

| Driver               | req/s | avg ms | p99 ms | RSS peak (MiB) | conns peak | conns settled |
| -------------------- | ----: | -----: | -----: | -------------: | ---------: | ------------: |
| redis-py             |   155 |    637 |  2,946 |            145 |        128 |           128 |
| redis-py+hiredis     |   227 |    437 |  2,542 |            179 |        122 |           122 |
| valkey-py            |   159 |    618 |  2,674 |            149 |        111 |           111 |
| valkey-py+libvalkey  |   149 |    663 |  3,124 |            147 |        134 |           134 |
| **rust-valkey**      |   251 |    395 |  2,314 |            135 |          2 |             2 |
| django (builtin)     |   157 |    631 |  1,138 |            417 |      1,840 |         1,798 |

### Takeaways

- Under realistic concurrent load, `rust-valkey` is **3–4×** faster than the
  fastest Python driver on sync direct, **8–12×** faster on `async50`, and
  ~10% ahead on full ASGI throughput while using **50× fewer Valkey
  connections** (multiplexed Tokio transport — 2 vs 100+).
- The Django built-in `RedisCache` shows the connection-growth pattern
  vcache flagged: 1,840 connections peak, settled at 1,798, 3× the RSS of
  the cachex backends. The cachex async path keeps Δ at 0 across phases on
  every driver.
- The one shape where `rust-valkey` trails is async serial — one `await` at
  a time. With no concurrency to multiplex over, the Python↔Rust crossing
  costs ~30% versus `valkey-py+libvalkey`. Real Django ASGI apps don't sit
  in that shape; production code that does should batch via `aget_many` /
  `asyncio.gather`.
