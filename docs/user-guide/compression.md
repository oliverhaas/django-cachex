# Compression

django-cachex supports pluggable compression to reduce memory usage. Compression is only applied to values larger than 256 bytes by default.

## Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "OPTIONS": {
            "compressor": "django_cachex.compressors.zstd.ZstdCompressor",
        }
    }
}
```

## Available Compressors

| Compressor | Extra |
|------------|-------|
| `django_cachex.compressors.zlib.ZlibCompressor` | — (stdlib) |
| `django_cachex.compressors.gzip.GzipCompressor` | — (stdlib) |
| `django_cachex.compressors.lzma.LzmaCompressor` | — (stdlib) |
| `django_cachex.compressors.lz4.Lz4Compressor` | `lz4` |
| `django_cachex.compressors.zstd.ZstdCompressor` | — (stdlib on 3.14+) |

Install optional dependencies:

```console
uv add django-cachex[lz4]
```

## Performance

Two views; pick whichever matches the decision you're making.

### Micro: algorithm in isolation

Pure compress/decompress in a tight loop, no driver, no network. Output
size is a percentage of the input (i.e. the no-compression baseline);
compress/decompress are absolute MB/s on the benchmark host.

| Compressor | Output size¹ | Compress² | Decompress² |
|------------|:-----------:|:--------:|:----------:|
| `zlib`     | 12%         | ~190 MB/s   | ~1.3 GB/s   |
| `gzip`     | 12%         | ~150 MB/s   | ~1.2 GB/s   |
| `lzma`     | 11%         | **~13 MB/s** | ~490 MB/s  |
| `lz4`      | 17%         | **~3.1 GB/s** | **~7.7 GB/s** |
| `zstd`     | 11%         | ~820 MB/s   | ~2.2 GB/s   |

### Macro: end-to-end via Django cache

Same compressors, but measured through `cache.get()` / `cache.set()` /
`cache.get_many()` / `cache.set_many()` against a real Valkey server.
Throughput factor is normalized to **no compression**, so the table reads
"this is what enabling each compressor costs you."

| Compressor | Throughput vs no-compression³ |
|------------|:-----------------------------:|
| no compression | 1.00×                     |
| `zlib`         | 0.70×                     |
| `gzip`         | 0.65×                     |
| `lzma`         | **0.26×**                 |
| `lz4`          | **0.99×**                 |
| `zstd`         | **0.90×**                 |

Picking guide:

- `zstd`: same ratio as `lzma` at ~60× the compress speed, and only ~10 % slower end-to-end than no compression at all. Default choice if available.
- `lz4`: pick when CPU is the bottleneck and a ~40 % larger payload is acceptable. End-to-end throughput is statistically indistinguishable from no compression while still cutting payload size 6×.
- `lzma`: pick only when output size matters more than write latency. Even then, `zstd` is usually a better trade now (same ratio, ~3× faster end-to-end).
- `zlib` / `gzip`: nearly identical. Pick `zlib` unless you need gzip's framing for an external consumer.
- No compression: sets the throughput ceiling but stores ~8× more server memory. Outside narrow latency-critical paths, compression almost always wins.

¹ Compressed size as a percentage of the input (~14 KiB pickled queryset-shaped payload).
² Absolute compress/decompress throughput in a tight loop (200 ops × 20 runs, median, single core). Numbers are hardware-dependent; use the ratios between rows, not the absolute values. Real-world impact also depends on payload compressibility (text/JSON compresses ~10×; already-compressed bytes barely shrink).
³ Geometric mean of `get`/`set`/`mget`/`mset` ops/sec end-to-end via Django cache → `redis-rs` adapter → localhost Valkey, normalized to running without a compressor. Reproduce with the [benchmarks](https://github.com/e1plus/django-cachex/tree/main/benchmarks) harness.

## Fallback for Migration

Specify a list of compressors to safely migrate between formats. The first is used for writing, all are tried for reading:

```python
"OPTIONS": {
    "compressor": [
        "django_cachex.compressors.zstd.ZstdCompressor",  # Write with new format
        "django_cachex.compressors.gzip.GzipCompressor",  # Read old format
    ],
}
```
