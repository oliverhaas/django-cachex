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

## What gets measured

Driver / serializer / compressor-macro tests run a seven-phase workload —
`get`, `get-miss`, `set`, `mget` (10-key batch), `mset` (10-key batch),
`incr`, `delete` — `N_OPS=1000` operations per phase, repeated `K_RUNS=10`
times.

Per-phase timings are reported as median ms and ops/sec across runs. Per-run
metrics include Python peak memory (`tracemalloc`) and server memory delta
(`INFO memory.used_memory`). Connection count is sampled once at the end.

Compressor-micro tests measure `compress(payload)` and
`decompress(compressed)` in a tight loop on a fixed payload, reporting ratio
and MB/s.

Knobs in [runner.py](runner.py): `N_OPS`, `K_RUNS`, `WARMUP_KEYS`, `MGET_BATCH`.

## Running

```console
# Full matrix (drivers + serializers + compressors)
uv run pytest benchmarks/ -c benchmarks/pytest.ini

# Just one slice
uv run pytest benchmarks/test_throughput.py::test_drivers           -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_serializers       -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_compressors_macro -c benchmarks/pytest.ini
uv run pytest benchmarks/test_throughput.py::test_compressors_micro -c benchmarks/pytest.ini

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
