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
| redis-py             |  2,326 |    2,461 |  2,265 | 1,425 | 1,280 |  2,465 |  1,182 |        111 |
| redis-py+hiredis     |  2,338 |    2,466 |  2,271 | 1,509 | 1,328 |  2,487 |  1,195 |         51 |
| valkey-py            |  2,775 |    2,962 |  2,717 | 1,601 | 1,408 |  2,954 |  1,436 |        109 |
| valkey-py+libvalkey  |  2,823 |    2,992 |  2,728 | 1,704 | 1,504 |  3,055 |  1,463 |         48 |
| **redis-rs**         |  5,324 |    6,794 |  5,942 | 2,239 | 3,047 |  6,925 |  3,124 |         29 |
| **valkey-glide**     |  7,339 |    9,150 |  7,036 | 2,058 | 1,943 |  9,433 |  4,044 |         29 |
| django (builtin)     |  2,349 |    2,499 |  2,309 | 1,514 | 1,373 |  1,966 |  1,212 |         51 |

### Django request cycle (`test_adapters_request_cycle`, `#req`) — ops/sec

One cache op per request through the full middleware/URL/view path.

| Adapter              |   get |   set |  incr |
| -------------------- | ----: | ----: | ----: |
| redis-py             | 1,096 | 1,120 | 1,150 |
| redis-py+hiredis     | 1,060 | 1,135 | 1,166 |
| valkey-py            | 1,099 | 1,251 | 1,287 |
| valkey-py+libvalkey  | 1,059 | 1,253 | 1,295 |
| django (builtin)     |   913 | 1,131 | 1,019 |

`redis-rs` and `valkey-glide` rows are missing from this snapshot.
Both adapters fail under sustained sync request-cycle pressure
(~2k sequential `cache.get()` through Django's view layer): `redis-rs`
sees `Connection reset by peer (os error 104)` from the valkey
container, `valkey-glide` sees a `glide_shared.exceptions.ClosingError`
timeout. The pure-Python adapters complete the same workload without
issues, so this is a real connection-handling difference between the
sync paths of the two Rust adapters, not an environmental flake. Worth
investigating separately.

### Async serial (`test_adapters_async_serial`, `#async`) — ops/sec

One `await` at a time.

| Adapter              |   get |   set |  mget |  mset |  incr |
| -------------------- | ----: | ----: | ----: | ----: | ----: |
| redis-py             | 1,812 | 1,726 | 1,209 |   857 | 1,896 |
| redis-py+hiredis     | 1,827 | 1,731 | 1,210 |   859 | 1,910 |
| valkey-py            | 2,074 | 2,026 | 1,334 |   853 | 2,175 |
| valkey-py+libvalkey  | 2,089 | 2,042 | 1,334 |   857 | 2,195 |
| **redis-rs**         | 3,802 | 4,033 | 1,818 | 2,385 | 4,335 |
| **valkey-glide**     | 3,383 | 3,341 | 1,731 | 1,676 | 3,719 |
| django (builtin)     | 1,961 | 1,958 |   200 |   198 | 1,000 |

### Async concurrent at 50 (`test_adapters_async_concurrent`, `#async50`) — ops/sec

`asyncio.gather` of 50 ops in flight — the workload most Django ASGI apps
actually generate.

| Adapter              |    get | get-miss |    set |  mget |  mset |   incr | delete | conns peak |
| -------------------- | -----: | -------: | -----: | ----: | ----: | -----: | -----: | ---------: |
| redis-py             |  2,127 |    2,260 |  2,055 | 1,356 |   931 |  2,159 |    982 |         56 |
| redis-py+hiredis     |  2,115 |    2,274 |  2,068 | 1,358 |   938 |  2,174 |    985 |        106 |
| valkey-py            |  2,483 |    2,593 |  2,340 | 1,243 |   920 |  2,574 |  1,129 |         56 |
| valkey-py+libvalkey  |  2,483 |    2,600 |  2,336 | 1,237 |   922 |  2,565 |  1,122 |        106 |
| **redis-rs**         | 16,170 |   25,711 | 19,783 | 2,293 | 5,524 | 29,685 |  3,062 |        107 |
| **valkey-glide**     |  9,874 |   12,137 |  9,831 | 2,038 | 2,539 | 12,064 |  2,513 |        107 |
| django (builtin)     |  2,046 |    2,147 |  2,037 |   209 |   208 |  1,051 |  1,015 |        107 |

### ASGI full-stack (`test_adapters_asgi`)

`granian` (4 workers) + `httpx` (100 concurrent, 20 s). Each request runs
six async cache ops — the shape closest to real production load.

| Adapter              | req/s | avg ms | p99 ms | RSS peak (MiB) | conns peak | conns settled |
| -------------------- | ----: | -----: | -----: | -------------: | ---------: | ------------: |
| redis-py             |   643 |    155 |  1,144 |            439 |        209 |           209 |
| redis-py+hiredis     |   451 |    221 |  1,371 |            433 |        209 |           209 |
| valkey-py            |   494 |    202 |  1,302 |            430 |        209 |           209 |
| valkey-py+libvalkey  |   466 |    212 |  1,565 |            439 |        212 |           212 |
| **redis-rs**         |   467 |    214 |  1,415 |            490 |      1,111 |         1,111 |
| **valkey-glide**     |   171 |    578 |  3,130 |            631 |      3,606 |         3,606 |
| django (builtin)     |   172 |    575 |  2,658 |            506 |        410 |           410 |

### Takeaways

- **Sync direct.** `valkey-glide` leads single-key ops (`get`/`set`/`incr`)
  by ~30% over `redis-rs`. `redis-rs` wins multi-key (`mget`/`mset`) and
  `delete` thanks to native pipelining. Both Rust adapters use ~4× less
  Python memory than the pure-Python adapters.
- **Async concurrent (50 in flight)** is where `redis-rs` shines: 16k+
  `get` ops/sec, ~30k `incr` ops/sec — **6–7×** the fastest Python adapter.
  `valkey-glide` is roughly half that rate but still **~4×** ahead of the
  Python adapters.
- **Async serial.** One `await` at a time used to penalize the Python↔Rust
  crossing, but both Rust adapters now beat the C-parser Python adapters
  on this shape too — `redis-rs` leads at ~3.8k ops/sec.
- **ASGI full-stack.** Under 4 granian workers + 100 concurrent `httpx`
  clients the picture is more mixed: `redis-py` (no parser) tops `req/s`
  at 643; `redis-rs` at 467 is roughly tied with `valkey-py`. Both Rust
  adapters open many more Valkey connections under this workload than
  earlier reference runs suggested (`redis-rs` 1,111, `valkey-glide` 3,606
  vs the older multiplexed-Tokio measurement of 2). Worth investigating
  whether granian's per-worker loop cycling defeats the per-loop client
  cache. Django's built-in `RedisCache` opens the most connections still
  (410) and uses 506 MiB peak RSS.
- **Connection stability.** Across `test_adapters_async_concurrent` and
  every other shape, the cachex async path keeps `Δ` at 0 between phases
  on every adapter — no per-phase connection leaks.
