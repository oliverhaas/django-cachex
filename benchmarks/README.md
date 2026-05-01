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

**Request cycle** (`test_drivers_request_cycle`) — same workload as
`test_drivers`, but each cache op runs inside a real Django request cycle:
`Client().get(url)` → URL resolve → `CommonMiddleware` → view function →
response → `request_finished` signal. The view in [urls.py](urls.py) does
exactly one cache op per request, so ops/sec is on the same scale as
`test_drivers` and you can read off the per-op overhead Django adds. Driver
ids are suffixed with `#req` in the final summary so the request-cycle rows
sit next to their direct counterparts.

**ASGI** (`test_drivers_asgi`) — full-stack benchmark in the shape of
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

- *Serial* (`test_drivers_async_serial`) — `await cache.aget(...)` one op at
  a time. Direct comparison with sync; the gap reveals asyncio-loop
  overhead and, for backends without native async, the cost of Django's
  `sync_to_async` fallback. Ids suffixed with `#async`.
- *Concurrent* (`test_drivers_async_concurrent`) — `asyncio.gather` of
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
uv run pytest benchmarks/test_throughput.py::test_drivers                  -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_serializers              -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_compressors_macro        -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_compressors_micro        -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_drivers_request_cycle    -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_drivers_async_serial     -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_drivers_async_concurrent -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_drivers_asgi             -c benchmarks/pytest.ini

# A single config
uv run pytest 'benchmarks/test_throughput.py::test_drivers[rust-valkey]' -c benchmarks/pytest.ini
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
