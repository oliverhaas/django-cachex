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

## Reproducing

The harness starts its own Redis and Valkey containers, so a Docker daemon is
the only host requirement:

```console
uv run pytest benchmarks/ -c benchmarks/pytest.ini

# A single slice
uv run pytest benchmarks/test_throughput.py::test_adapters_sync \
  -c benchmarks/pytest.ini
```

`benchmarks/README.md` lists the slices, the knobs (`N_OPS`, `K_RUNS`,
`WARMUP_KEYS`, `MGET_BATCH`) and the full methodology.
