# Benchmarks

Throughput and memory comparison across cache adapter/parser/serializer/compressor
combos.

Not part of the regular test suite — runs separately because it spins up its
own Redis and Valkey containers and is slow on purpose (timing accuracy
depends on letting workloads run).

## What gets compared

**Adapters** (with default pickle serializer):

- `redis-py` — pure-Python parser
- `redis-py+hiredis` — C parser
- `valkey-py` — pure-Python parser
- `valkey-py+libvalkey` — C parser
- `redis-rs` — our Rust extension adapter (PyO3 bindings around the
  `redis-rs` crate, multiplexed Tokio transport)
- `valkey-glide` — `valkey/valkey-glide` Python wheels (also Rust-cored,
  but with a thread-pool transport instead of multiplexed). Only available
  on cp314 GIL — no cp314t (free-threaded) wheels yet.
- `django (builtin)` — Django's official built-in `django.core.cache.backends.redis.RedisCache`
  (since 4.0). Not the third-party `jazzband/django-redis` package — that one is
  unrelated. Included as an external reference point.

**Serializers** (with `redis-rs` adapter, since adapter overhead is smallest there):

- `pickle` — stdlib default
- `json` — Django's `DjangoJSONEncoder`
- `msgpack` — pure-Python `msgpack`
- `orjson` — Rust-backed JSON
- `ormsgpack` — Rust-backed MessagePack

**Compressors** — two views, both with `redis-rs` + `pickle`:

- *Macro* (`test_compressors_macro`) — end-to-end Django cache ops on a
  ~14 KiB queryset-shaped payload. Captures the cost of compress/decompress
  against the savings from sending fewer bytes over the wire.
- *Micro* (`test_compressors_micro`) — pure compress/decompress in a tight
  loop. Reports output ratio and MB/s. No adapter, no container.

Compressor candidates: `none`, `zlib`, `gzip`, `lzma`, `lz4`, `zstd`.

**Request cycle** (`test_adapters_request_cycle`) — same workload as
`test_adapters_sync`, but each cache op runs inside a real Django request cycle:
`Client().get(url)` → URL resolve → `CommonMiddleware` → view function →
response → `request_finished` signal. The view in [urls.py](urls.py) does
exactly one cache op per request, so ops/sec is on the same scale as
`test_adapters_sync` and you can read off the per-op overhead Django adds. Adapter
ids are suffixed with `#req` in the final summary so the request-cycle rows
sit next to their direct counterparts.

**ASGI** (`test_adapters_asgi`) — full-stack benchmark in the shape of
[`django-vcache`'s `bench_compare.py`](https://gitlab.com/glitchtip/django-vcache/-/blob/main/bench_compare.py):

- Spawns a real **`granian`** ASGI server (4 workers) per adapter
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

Adapter / serializer / compressor-macro / request-cycle tests run a
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
# Full matrix (adapters + serializers + compressors)
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
uv run pytest 'benchmarks/test_throughput.py::test_adapters_sync[redis-rs]' -c benchmarks/pytest.ini
```

`test_compressors_micro` is the only test that doesn't need Docker — useful
for quick algorithm comparisons on a laptop without containers running.

A summary table prints at the end of the session.

## Notes

- **No xdist.** Parallel runs make timings noisy; benchmarks run sequentially.
- **Redis vs Valkey.** Each adapter is paired with its natural server
  (redis-py → redis, valkey-py / redis-rs / valkey-glide → valkey,
  django (builtin) → redis). Cross-pairings are intentionally not in the
  matrix — both servers speak the same protocol, so the comparison is
  mostly a wash.
- **Warmup.** Each phase runs an untimed pass before the timed runs to prime
  connections, server keyspace, and lazy serializer state.
- **Memory caveat.** `used_memory` is whole-server, so concurrent activity on
  the same container distorts the delta. Run alone for clean numbers.

## Reference results

Snapshot of the adapter matrix on a Ryzen 9 5950X / 32 GiB / Linux 6.17,
cp314 GIL, Django 6.0, all servers in local Docker. Numbers shift run to
run; ordering is what matters.

### Sync direct (`test_adapters_sync`) — ops/sec

| Adapter              |    get | get-miss |    set |  mget |  mset |   incr | delete | py-mem KiB |
| -------------------- | -----: | -------: | -----: | ----: | ----: | -----: | -----: | ---------: |
| redis-py             |  2,301 |    2,439 |  2,251 | 1,439 | 1,285 |  2,463 |  1,189 |        111 |
| redis-py+hiredis     |  2,332 |    2,480 |  2,281 | 1,515 | 1,345 |  2,506 |  1,209 |         51 |
| valkey-py            |  2,783 |    2,956 |  2,695 | 1,595 | 1,418 |  2,978 |  1,437 |        109 |
| valkey-py+libvalkey  |  2,821 |    2,973 |  2,741 | 1,695 | 1,508 |  3,040 |  1,465 |         48 |
| **redis-rs**         |  9,651 |   13,396 | 11,151 | 2,967 | 4,520 | 13,226 |  5,894 |         29 |
| **valkey-glide**     |  6,982 |    8,783 |  6,982 | 2,030 | 1,916 |  8,901 |  3,994 |         29 |
| django (builtin)     |  2,307 |    2,459 |  2,269 | 1,501 | 1,347 |  1,947 |  1,199 |         51 |

### Django request cycle (`test_adapters_request_cycle`, `#req`) — ops/sec

One cache op per request through the full middleware/URL/view path.

| Adapter              |   get | get-miss |   set |  mget |  mset |  incr | delete |
| -------------------- | ----: | -------: | ----: | ----: | ----: | ----: | -----: |
| redis-py             | 1,106 |    1,180 | 1,129 |   835 |   778 | 1,153 |  1,152 |
| redis-py+hiredis     | 1,065 |    1,181 | 1,131 |   866 |   807 | 1,168 |  1,173 |
| valkey-py            | 1,089 |    1,299 | 1,241 |   891 |   836 | 1,276 |  1,282 |
| valkey-py+libvalkey  | 1,053 |    1,306 | 1,248 |   924 |   861 | 1,290 |  1,300 |
| **redis-rs**         | 1,382 |    2,044 | 2,001 | 1,265 | 1,490 | 1,995 |  2,005 |
| **valkey-glide**     | 1,237 |    1,863 | 1,753 | 1,023 |   994 | 1,816 |  1,838 |
| django (builtin)     |   861 |    1,177 | 1,133 |   859 |   806 | 1,016 |  1,169 |

### Async serial (`test_adapters_async_serial`, `#async`) — ops/sec

One `await` at a time.

| Adapter              |   get | get-miss |   set |  mget |  mset |  incr | delete |
| -------------------- | ----: | -------: | ----: | ----: | ----: | ----: | -----: |
| redis-py             | 1,820 |    1,911 | 1,724 | 1,195 |   849 | 1,903 |    917 |
| redis-py+hiredis     | 1,815 |    1,913 | 1,729 | 1,199 |   849 | 1,911 |    915 |
| valkey-py            | 2,080 |    2,184 | 2,039 | 1,331 |   854 | 2,190 |  1,055 |
| valkey-py+libvalkey  | 2,075 |    2,187 | 2,029 | 1,329 |   852 | 2,176 |  1,049 |
| **redis-rs**         | 6,862 |    8,238 | 7,517 | 2,474 | 3,560 | 8,265 |  3,952 |
| **valkey-glide**     | 3,300 |    3,738 | 3,297 | 1,725 | 1,678 | 3,693 |  1,735 |
| django (builtin)     | 1,948 |    1,995 | 1,929 |   199 |   197 |   989 |    992 |

### Async concurrent at 50 (`test_adapters_async_concurrent`, `#async50`) — ops/sec

`asyncio.gather` of 50 ops in flight — the workload most Django ASGI apps
actually generate.

| Adapter              |    get | get-miss |    set |  mget |  mset |   incr | delete | conns peak |
| -------------------- | -----: | -------: | -----: | ----: | ----: | -----: | -----: | ---------: |
| redis-py             |  2,109 |    2,254 |  2,043 | 1,343 |   925 |  2,148 |    971 |         56 |
| redis-py+hiredis     |  2,121 |    2,254 |  2,047 | 1,347 |   931 |  2,149 |    979 |        106 |
| valkey-py            |  2,465 |    2,591 |  2,327 | 1,229 |   920 |  2,569 |  1,130 |         58 |
| valkey-py+libvalkey  |  2,467 |    2,588 |  2,329 | 1,223 |   917 |  2,564 |  1,129 |        108 |
| **redis-rs**         | 22,335 |   35,848 | 28,528 | 2,508 | 6,533 | 39,406 |  5,543 |        108 |
| **valkey-glide**     | 10,149 |   12,575 | 10,045 | 2,074 | 2,607 | 12,232 |  2,627 |        109 |
| django (builtin)     |  2,078 |    2,177 |  2,076 |   212 |   210 |  1,051 |  1,018 |        107 |

### ASGI full-stack (`test_adapters_asgi`)

`granian` (4 workers) + `httpx` (100 concurrent, 20 s). Each request runs
six async cache ops — the shape closest to real production load.

| Adapter              | req/s | avg ms | p99 ms | RSS peak (MiB) | conns peak | conns settled |
| -------------------- | ----: | -----: | -----: | -------------: | ---------: | ------------: |
| redis-py             |   424 |    235 |  1,486 |            413 |        216 |           216 |
| redis-py+hiredis     |   467 |    213 |  1,509 |            414 |        209 |           209 |
| valkey-py            |   519 |    192 |  1,375 |            420 |        211 |           211 |
| valkey-py+libvalkey  |   498 |    200 |  1,599 |            432 |        211 |           211 |
| **redis-rs**         |   607 |    164 |  1,152 |            430 |        115 |           115 |
| **valkey-glide**     |   553 |    180 |  1,395 |            446 |        172 |           172 |
| django (builtin)     |   156 |    634 |  3,117 |            466 |        337 |           337 |

### Takeaways

- **Sync direct.** `redis-rs` leads on every phase — **~10k get / 11k set
  / 13k incr / 6k delete** ops/sec, ~4× the fastest pure-Python adapter.
  `valkey-glide` is second on single-key ops (`get`/`set`/`incr` ~7–9k)
  but trails `redis-rs` on multi-key and `delete`. Both Rust adapters
  use ~4× less Python memory than the pure-Python adapters.
- **Django request cycle.** Both Rust adapters lead the table —
  `redis-rs` ~1.4–2.1k ops/sec, `valkey-glide` ~1.2–1.9k. Earlier
  versions of these adapters failed this benchmark entirely because
  ``close()`` was tearing down the connection on every
  ``request_finished`` signal; now they outperform the Python adapters.
- **Async serial.** `redis-rs` runs **~3–4× faster** than the C-parser
  Python adapters on every phase (6.9k get, 8.3k incr). `valkey-glide`
  is also faster than Python (~1.5–1.8×) but trails `redis-rs`.
- **Async concurrent (50 in flight).** `redis-rs` peaks at **22k get /
  36k get-miss / 39k incr** ops/sec — **9–18×** the fastest Python
  adapter. `valkey-glide` is half that rate but still **~4×** ahead of
  the Python adapters.
- **ASGI full-stack** (granian × 4 workers, httpx × 100 concurrent).
  Both Rust adapters lead on req/s — `redis-rs` 607 (164 ms avg, 1.15 s
  p99), `valkey-glide` 553 — and use **fewer connections** than every
  Python adapter (`redis-rs` 115, `valkey-glide` 172, vs Python ~210).
  Process-wide client sharing (added after this benchmark first
  surfaced 1,111 / 4,577 conn counts) brought the Rust adapters in
  line with the multiplexed-transport claim.
- **Connection stability.** Across every shape the cachex path keeps
  `Δ` at 0 between phases — no per-phase connection leaks on any
  adapter.
