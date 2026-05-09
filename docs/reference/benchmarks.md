# Benchmarks

Reference numbers for adapter, serializer, and compressor combinations
under the workloads in [`benchmarks/`][bench-src] (the README there has
the full methodology). Results shift run-to-run; ordering is the part
to trust.

[bench-src]: https://github.com/oliverhaas/django-cachex/tree/main/benchmarks

## Run environment

- AMD Ryzen 9 5950X (16C/32T) · 32 GiB RAM · Ubuntu 24.04 · Linux 6.17
- CPython 3.14.2 (GIL build)
- Django 6.0
- Redis 8 / Valkey 8 in local Docker, paired natively per adapter
  (`redis-py` → redis, `valkey-py` / `redis-rs` / `valkey-glide` → valkey, `django (builtin)` → redis)

## What the workload does

- Sync, async, and request-cycle benchmarks all run the same seven
  phases: `get`, `get-miss`, `set`, `mget`/`mset` (10-key batches,
  reported per-key), `incr`, `delete`. 1,000 ops per phase, 10 timed
  runs after a warmup pass.
- ASGI benchmark spawns `granian` (4 workers), drives 100 concurrent
  `httpx` clients for 20 s against a 6-op view, and samples server RSS
  and `connected_clients` every 5 s.
- Compressor micro skips the cache path entirely — pure
  `compress` / `decompress` on a 14 KiB pickle of queryset-shaped data.

## Sync direct

`cache.get(...)` etc. against the configured backend, no Django
request, no asyncio.

| Adapter | get | get-miss | set | mget | mset | incr | delete | py-mem KiB |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|-----------:|
| redis-py            | 2,179 |  2,327 |  2,150 | 1,368 | 1,226 |  2,345 | 1,132 | 111 |
| redis-py+hiredis    | 2,235 |  2,365 |  2,176 | 1,448 | 1,289 |  2,374 | 1,147 |  51 |
| valkey-py           | 2,639 |  2,823 |  2,603 | 1,513 | 1,347 |  2,873 | 1,374 | 109 |
| valkey-py+libvalkey | 2,707 |  2,865 |  2,613 | 1,617 | 1,421 |  2,887 | 1,394 |  48 |
| **redis-rs**        | **9,366** | **13,631** | **11,354** | **2,852** | **4,322** | **13,875** | **6,232** | **29** |
| **valkey-glide**    | 7,110 |  8,821 |  6,887 | 1,928 | 1,844 |  9,076 | 3,980 |  29 |
| django (builtin)    | 2,218 |  2,360 |  2,205 | 1,416 | 1,290 |  1,855 | 1,143 |  51 |

`redis-rs` is ~4× the fastest pure-Python adapter on single-key ops and
uses ~2-4× less Python memory. `valkey-glide` lands second on
single-key phases and matches `redis-rs` on memory. The C parsers
(`hiredis`, `libvalkey`) shave Python memory in half on their
respective adapters but only move throughput by a few percent.

## Serializers

`redis-rs` adapter, varying the serializer (adapter overhead is smallest
there, so serializer cost dominates).

| Serializer | get | get-miss | set | mget | mset | incr | delete |
|------------|----:|---------:|----:|-----:|-----:|-----:|-------:|
| pickle    |  9,618 | 13,776 | 11,228 | 2,859 | 4,373 | 13,218 | 6,129 |
| json      |  9,420 | 14,004 |  7,910 | 2,454 | 1,867 | 13,970 | 6,021 |
| msgpack   |  9,985 | 13,951 | 11,535 | 3,251 | 4,873 | 13,465 | 6,163 |
| orjson    | 10,178 | 14,119 | 12,328 | 3,513 | 5,245 | 14,080 | 6,194 |
| ormsgpack | 10,780 | 14,474 | 12,417 | 3,476 | 5,276 | 13,680 | 6,261 |

`ormsgpack` and `orjson` (both Rust-cored) edge `pickle` on most
phases — smaller wire payload outweighs the encoder cost. The
pure-Python `json` collapses on `set`/`mset` because the encoder is the
bottleneck there.

## Compressors (macro)

`redis-rs` + `pickle` on a 14 KiB queryset-shaped payload — end-to-end
cache ops.

| Compressor | get | get-miss | set | mget | mset | incr | delete | srv-mem KiB |
|------------|----:|---------:|----:|-----:|-----:|-----:|-------:|------------:|
| none | 2,808 | 12,825 | 7,504 | 312 | 1,802 | 13,928 | 6,238 | 1,266 |
| zlib | 2,725 | 13,188 | 4,428 | 299 |   722 | 13,282 | 6,049 |   167 |
| gzip | 2,618 | 13,531 | 4,084 | 288 |   638 | 13,893 | 6,200 |   165 |
| lzma | 2,526 | 13,359 |   807 | 281 |    89 | 12,977 | 6,022 |   167 |
| lz4  | 2,829 | 13,415 | 7,581 | 310 | 1,763 | 13,342 | 6,218 |   234 |
| zstd | 2,765 | 13,035 | 6,398 | 302 | 1,399 | 14,050 | 6,196 |   165 |

Server-memory tells the trade-off: `lz4` keeps `set` throughput in the
same ballpark as `none` while shrinking the working set ~5×; `zstd` is
the densest of the cheap options. `lzma` is too slow for hot paths;
reach for it only on rarely-written, memory-constrained data.

## Compressors (micro)

Pure compress/decompress on the same 14 KiB payload. No adapter, no
network.

| Compressor | output ratio | compress (MB/s) | decompress (MB/s) |
|------------|-------------:|----------------:|------------------:|
| zlib |  11.9% |   181.0 | 1,217.2 |
| gzip |  11.8% |   147.8 | 1,090.0 |
| lzma |  11.4% |    13.2 |   484.2 |
| lz4  |  16.6% | 2,945.5 | 6,089.9 |
| zstd |  11.4% |   780.6 | 2,077.5 |

`lz4` is in a different league on raw throughput, at the cost of ~5
percentage points of ratio. `zstd` matches `zlib`'s ratio at ~4× the
compress speed.

## Django request cycle

Same workload as sync direct, but every cache op runs inside
`Client().get(url)` (URL resolve → `CommonMiddleware` → view →
`request_finished`). The gap to sync direct is the per-request
overhead Django itself adds.

| Adapter | get | get-miss | set | mget | mset | incr | delete |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|
| redis-py            | 1,058 | 1,123 | 1,083 |   795 |   745 | 1,099 | 1,077 |
| redis-py+hiredis    | 1,007 | 1,132 | 1,083 |   832 |   778 | 1,116 | 1,122 |
| valkey-py           | 1,035 | 1,230 | 1,183 |   844 |   789 | 1,206 | 1,200 |
| valkey-py+libvalkey | 1,003 | 1,243 | 1,180 |   879 |   825 | 1,215 | 1,238 |
| **redis-rs**        | **1,343** | **1,960** | **1,887** | **1,206** | **1,417** | **1,905** | **1,928** |
| **valkey-glide**    | 1,150 | 1,749 | 1,668 |   983 |   940 | 1,740 | 1,745 |
| django (builtin)    |   799 | 1,104 | 1,062 |   812 |   765 |   955 | 1,106 |

Django itself caps throughput at ~1k req/s for pure-Python adapters —
the cache stops being the bottleneck. The Rust-cored adapters land
~1.7-2× the Python adapters because their per-op overhead is small
enough that Django's per-request work doesn't fully mask it.

## Async serial

One `await cache.aget(...)` at a time, no `gather`. Direct comparison
with sync; the gap is asyncio loop overhead (and `sync_to_async` for
backends without native async).

| Adapter | get | get-miss | set | mget | mset | incr | delete |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|
| redis-py            | 1,785 | 1,863 | 1,677 | 1,170 |   830 | 1,857 |   891 |
| redis-py+hiredis    | 1,776 | 1,850 | 1,686 | 1,163 |   840 | 1,842 |   894 |
| valkey-py           | 2,012 | 2,107 | 1,976 | 1,288 |   836 | 2,138 | 1,020 |
| valkey-py+libvalkey | 2,031 | 2,138 | 1,976 | 1,294 |   836 | 2,135 | 1,026 |
| **redis-rs**        | **6,510** | **8,099** | **7,310** | **2,362** | **3,503** | **8,187** | **3,772** |
| valkey-glide        | 3,251 | 3,634 | 3,291 | 1,640 | 1,634 | 3,680 | 1,735 |
| django (builtin)    | 1,903 | 2,016 | 1,879 |   193 |   189 |   971 |   970 |

`redis-rs` runs ~3-4× the C-parser Python adapters on every phase
because its async path skips the `sync_to_async` round-trip the Python
adapters make on top of their sync transports. `valkey-glide` is also
native-async but the Python wrapper is heavier per call. Django's
built-in `RedisCache` `mget`/`mset` collapse to ~190 ops/s under
`sync_to_async`.

## Async concurrent (50 in flight)

`asyncio.gather` of 50 ops at a time — closer to what an ASGI app
under load actually generates.

| Adapter | get | get-miss | set | mget | mset | incr | delete | conns peak |
|---------|----:|---------:|----:|-----:|-----:|-----:|-------:|-----------:|
| redis-py            |  2,076 |  2,199 |  2,007 | 1,323 |   910 |  2,114 |   967 |  56 |
| redis-py+hiredis    |  2,074 |  2,198 |  1,998 | 1,318 |   897 |  2,110 |   961 | 106 |
| valkey-py           |  2,434 |  2,530 |  2,290 | 1,186 |   898 |  2,515 | 1,114 |  58 |
| valkey-py+libvalkey |  2,421 |  2,540 |  2,292 | 1,199 |   918 |  2,522 | 1,108 | 108 |
| **redis-rs**        | **21,354** | **35,524** | **28,350** | **2,372** | **6,347** | **37,786** | **5,764** | 108 |
| **valkey-glide**    |  9,903 | 12,208 |  9,770 | 1,949 | 2,541 | 11,950 | 2,588 | 109 |
| django (builtin)    |  2,007 |  2,170 |  2,058 |   208 |   206 |  1,058 |   991 | 107 |

This is where the multiplexed Rust transports pay off — `redis-rs`
peaks at ~10× the Python adapters on single-key phases, `valkey-glide`
at ~4×. Connection counts plateau (`Δ = 0` across phases on every
adapter), so this also serves as the connection-leak smoke test.

## ASGI full-stack

Granian (4 workers) + httpx (100 concurrent, 20 s) hitting a view that
does six async cache ops per request. Closest shape to a real
production load.

| Adapter | req/s | avg ms | p99 ms | RSS peak (MiB) | conns peak | conns settled |
|---------|------:|-------:|-------:|---------------:|-----------:|--------------:|
| redis-py            | 413 | 240.6 | 1,507.2 | 435 | 209 | 209 |
| redis-py+hiredis    | 586 | 170.1 | 2,421.7 | 427 | 209 | 209 |
| valkey-py           | 380 | 261.8 | 1,467.0 | 434 | 220 | 220 |
| valkey-py+libvalkey | 626 | 159.2 | 1,180.8 | 434 | 216 | 216 |
| redis-rs            | 423 | 235.6 | 1,752.0 | 428 | 115 | 115 |
| valkey-glide        | 324 | 306.0 | 1,681.9 | 438 | 115 | 115 |
| django (builtin)    | 200 | 494.0 | 2,421.7 | 523 | 316 | 316 |

req/s is noisy run-to-run on this benchmark; treat the rough buckets
(~600 / ~400 / ~200) as the signal, not exact ranks. The clearer
takeaways: both Rust adapters keep the connection count to ~half the
Python adapters, and Django's built-in `RedisCache` opens 316
connections — it instantiates a fresh `redis.Redis` per cache call —
and pays the highest avg latency on the smallest server.

## Reproducing

The benchmarks live in [`benchmarks/`][bench-src] and spin up their
own Redis + Valkey containers via `testcontainers`, so a Docker daemon
is the only host requirement:

```console
uv run pytest benchmarks/ -c benchmarks/pytest.ini
```

Single slice:

```console
uv run pytest benchmarks/test_throughput.py::test_adapters_sync \
  -c benchmarks/pytest.ini
```

`benchmarks/README.md` has the full list of slices, knobs (`N_OPS`,
`K_RUNS`, `WARMUP_KEYS`, `MGET_BATCH`), and notes on running with
simulated network latency to reproduce upstream connection-leak
claims.
