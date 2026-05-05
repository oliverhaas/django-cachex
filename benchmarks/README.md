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
| redis-py             |  2,298 |    2,441 |  2,266 | 1,435 | 1,287 |  2,460 |  1,195 |        111 |
| redis-py+hiredis     |  2,357 |    2,488 |  2,296 | 1,517 | 1,355 |  2,512 |  1,213 |         51 |
| valkey-py            |  2,796 |    2,981 |  2,742 | 1,612 | 1,427 |  3,008 |  1,452 |        109 |
| valkey-py+libvalkey  |  2,865 |    3,038 |  2,761 | 1,714 | 1,514 |  3,067 |  1,469 |         48 |
| **redis-rs**         |  9,821 |   13,743 | 12,050 | 3,014 | 4,528 | 13,523 |  6,267 |         29 |
| **valkey-glide**     |  7,023 |    9,267 |  7,203 | 2,044 | 1,904 |  9,364 |  3,980 |         29 |
| django (builtin)     |  2,334 |    2,469 |  2,297 | 1,495 | 1,363 |  1,944 |  1,212 |         51 |

### Django request cycle (`test_adapters_request_cycle`, `#req`) — ops/sec

One cache op per request through the full middleware/URL/view path.

| Adapter              |   get | get-miss |   set |  mget |  mset |  incr | delete |
| -------------------- | ----: | -------: | ----: | ----: | ----: | ----: | -----: |
| redis-py             | 1,109 |    1,178 | 1,127 |   837 |   777 | 1,160 |  1,148 |
| redis-py+hiredis     | 1,055 |    1,182 | 1,137 |   870 |   807 | 1,170 |  1,174 |
| valkey-py            | 1,081 |    1,295 | 1,232 |   891 |   822 | 1,277 |  1,279 |
| valkey-py+libvalkey  | 1,051 |    1,314 | 1,256 |   932 |   868 | 1,296 |  1,295 |
| **redis-rs**         | 1,409 |    2,094 | 1,998 | 1,258 | 1,512 | 2,040 |  2,033 |
| **valkey-glide**     | 1,262 |    1,892 | 1,778 | 1,028 | 1,003 | 1,843 |  1,838 |
| django (builtin)     |   866 |    1,183 | 1,138 |   859 |   810 | 1,016 |  1,178 |

### Async serial (`test_adapters_async_serial`, `#async`) — ops/sec

One `await` at a time.

| Adapter              |   get | get-miss |   set |  mget |  mset |  incr | delete |
| -------------------- | ----: | -------: | ----: | ----: | ----: | ----: | -----: |
| redis-py             | 1,832 |    1,908 | 1,729 | 1,205 |   856 | 1,899 |    915 |
| redis-py+hiredis     | 1,816 |    1,914 | 1,724 | 1,209 |   859 | 1,892 |    913 |
| valkey-py            | 2,069 |    2,170 | 2,035 | 1,328 |   852 | 2,173 |  1,045 |
| valkey-py+libvalkey  | 2,075 |    2,181 | 2,030 | 1,325 |   852 | 2,185 |  1,049 |
| **redis-rs**         | 6,922 |    8,607 | 7,801 | 2,520 | 3,668 | 8,764 |  4,274 |
| **valkey-glide**     | 3,328 |    3,631 | 3,282 | 1,737 | 1,686 | 3,713 |  1,768 |
| django (builtin)     | 1,915 |    2,048 | 1,945 |   198 |   195 |   995 |    992 |

### Async concurrent at 50 (`test_adapters_async_concurrent`, `#async50`) — ops/sec

`asyncio.gather` of 50 ops in flight — the workload most Django ASGI apps
actually generate.

| Adapter              |    get | get-miss |    set |  mget |  mset |   incr | delete | conns peak |
| -------------------- | -----: | -------: | -----: | ----: | ----: | -----: | -----: | ---------: |
| redis-py             |  2,119 |    2,258 |  2,059 | 1,353 |   928 |  2,161 |    981 |         56 |
| redis-py+hiredis     |  2,116 |    2,258 |  2,055 | 1,358 |   930 |  2,161 |    979 |        106 |
| valkey-py            |  2,471 |    2,580 |  2,327 | 1,233 |   919 |  2,559 |  1,126 |         58 |
| valkey-py+libvalkey  |  2,479 |    2,592 |  2,333 | 1,237 |   920 |  2,561 |  1,127 |        108 |
| **redis-rs**         | 22,419 |   36,201 | 28,862 | 2,498 | 6,548 | 39,650 |  6,272 |        109 |
| **valkey-glide**     | 10,184 |   12,512 | 10,032 | 2,073 | 2,629 | 12,314 |  2,649 |        109 |
| django (builtin)     |  2,060 |    2,218 |  2,120 |   210 |   211 |  1,056 |  1,018 |        107 |

### ASGI full-stack (`test_adapters_asgi`)

`granian` (4 workers) + `httpx` (100 concurrent, 20 s). Each request runs
six async cache ops — the shape closest to real production load.

| Adapter              | req/s | avg ms | p99 ms | RSS peak (MiB) | conns peak | conns settled |
| -------------------- | ----: | -----: | -----: | -------------: | ---------: | ------------: |
| redis-py             |   503 |    198 |  1,340 |            418 |        209 |           209 |
| redis-py+hiredis     |   339 |    294 |  1,569 |            417 |        209 |           209 |
| valkey-py            |   263 |    378 |  1,876 |            411 |        227 |           227 |
| valkey-py+libvalkey  |   514 |    194 |  1,325 |            450 |        215 |           215 |
| **redis-rs**         |   286 |    346 |  1,985 |            433 |        592 |           592 |
| **valkey-glide**     |   219 |    452 |  2,094 |            666 |      4,577 |         4,577 |
| django (builtin)     |   174 |    567 |  2,816 |            536 |        387 |           387 |

### Takeaways

- **Sync direct.** `redis-rs` leads on every phase — **~10k get / 12k set
  / 13.5k incr / 6.3k delete** ops/sec, ~4× the fastest pure-Python
  adapter. `valkey-glide` is second on single-key ops (`get`/`set`/`incr`
  ~7–9k) but trails `redis-rs` on multi-key and `delete`. Both Rust
  adapters use ~4× less Python memory than the pure-Python adapters.
- **Django request cycle.** With `close()` correctly treated as a no-op,
  both Rust adapters now lead the table — `redis-rs` ~1.4–2.1k ops/sec,
  `valkey-glide` ~1.3–1.9k. Pre-fix this benchmark failed entirely on
  both Rust adapters because `request_finished` was tearing down the
  connection on every request.
- **Async serial.** `redis-rs` runs **~3–4× faster** than the C-parser
  Python adapters on every phase (6.9k get, 8.7k incr). `valkey-glide`
  is also faster than Python (~1.5–1.8×) but trails `redis-rs`.
- **Async concurrent (50 in flight).** `redis-rs` peaks at **22k get /
  36k get-miss / 39k incr** ops/sec — **9–18×** the fastest Python
  adapter. `valkey-glide` is half that rate but still **~4×** ahead of
  the Python adapters.
- **ASGI full-stack** (granian × 4 workers, httpx × 100 concurrent) is
  noisy run-to-run. Best-case Python adapters land in the 300–500 req/s
  range; Rust adapters in the 200–290 req/s range here. The connection
  count for `valkey-glide` is still anomalously high (4,577) — likely
  granian spawning many short-lived event loops, each getting a fresh
  per-loop async client; worth a follow-up. `redis-rs` improved from
  1,111 → 592 thanks to the close fix but is still elevated vs the
  Python adapters' ~210.
- **Connection stability.** Across every shape the cachex path keeps
  `Δ` at 0 between phases — no per-phase connection leaks on any
  adapter.
