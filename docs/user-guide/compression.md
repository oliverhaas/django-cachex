# Compression

Compressors shrink cached values to save server memory. By default, only values larger than 256 bytes are compressed.

## Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "OPTIONS": {
            "compressor": "django_cachex.compressors.zstd.ZstdCompressor",
        },
    }
}
```

## Minimum Size

`min_length` is a constructor argument of the compressor, not a key in
`OPTIONS`. It defaults to `256`, so payloads of 256 bytes or less are stored
uncompressed. To change the threshold, configure a compressor instance instead
of a dotted path:

```python
from django_cachex.compressors.zstd import ZstdCompressor

CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "OPTIONS": {
            "compressor": ZstdCompressor(min_length=1024),
        },
    }
}
```

A subclass works too, and stays configurable by dotted path:

```python
class LargeOnlyZstdCompressor(ZstdCompressor):
    min_length = 1024
```

## Compression level

Every compressor also takes a keyword-only `level`, passed the same way
(`ZstdCompressor(level=10)`) or set as a class attribute on a subclass. The
defaults are zlib `6`, gzip `9`, lzma preset `4`, lz4 `0` (fast mode) and
zstd `3`.

## Available Compressors

| Compressor | Extra |
|------------|-------|
| `django_cachex.compressors.zlib.ZlibCompressor` | (stdlib) |
| `django_cachex.compressors.gzip.GzipCompressor` | (stdlib) |
| `django_cachex.compressors.lzma.LzmaCompressor` | (stdlib) |
| `django_cachex.compressors.lz4.Lz4Compressor` | `lz4` |
| `django_cachex.compressors.zstd.ZstdCompressor` | (stdlib on 3.14+) |

Install the `lz4` extra for `Lz4Compressor`:

```console
uv add django-cachex[lz4]
```

## Performance

### Micro: algorithm in isolation

This benchmark compresses and decompresses a ~14 KiB pickled
queryset-shaped payload in a tight loop, with no driver and no network.
Output size is the compressed size as a percentage of the input. Compress
and decompress speeds are the median of 20 runs of 200 operations on a single
core. They depend on the hardware, so compare the ratios between rows, not
the absolute values. Real payloads vary: text and JSON compress about 10×,
and already-compressed bytes barely shrink.

| Compressor | Output size | Compress | Decompress |
|------------|:-----------:|:--------:|:----------:|
| `zlib`     | 12%         | ~190 MB/s   | ~1.3 GB/s   |
| `gzip`     | 12%         | ~150 MB/s   | ~1.2 GB/s   |
| `lzma`     | 11%         | ~13 MB/s    | ~490 MB/s   |
| `lz4`      | 17%         | ~3.1 GB/s   | ~7.7 GB/s   |
| `zstd`     | 11%         | ~820 MB/s   | ~2.2 GB/s   |

### Macro: end-to-end via Django cache

This benchmark runs `cache.get()`, `cache.set()`, `cache.get_many()` and
`cache.set_many()` through the `valkey-py+libvalkey` adapter against a local
Valkey server. The figure is the geometric mean of the four rates, relative
to running without a compressor. The
[benchmarks](https://github.com/oliverhaas/django-cachex/tree/main/benchmarks)
harness reproduces it.

| Compressor | Throughput vs no compression |
|------------|:----------------------------:|
| no compression | 1.00×                    |
| `zlib`         | 0.81×                    |
| `gzip`         | 0.76×                    |
| `lzma`         | 0.37×                    |
| `lz4`          | 0.99×                    |
| `zstd`         | 0.94×                    |

- `zstd` has the same ratio as `lzma` at ~60× the compress speed, and runs ~6% slower end to end than no compression. Pick it by default.
- `lz4` suits workloads where CPU is the bottleneck and a ~40% larger payload is acceptable. Its end-to-end throughput is within 1% of no compression, and it still cuts payload size ~5×.
- `lzma` fits only when output size matters more than write latency. Even then, `zstd` has the same ratio and runs ~2.5× faster end to end.
- `zlib` and `gzip` are nearly identical. Pick `zlib` unless an external consumer needs gzip's framing.
- No compression sets the throughput ceiling, but uses ~8× more server memory.

## Fallback for Migration

To migrate between formats, pass a list of compressors. The cache writes with the first and tries each in order on read:

```python
"OPTIONS": {
    "compressor": [
        "django_cachex.compressors.zstd.ZstdCompressor",  # Write with new format
        "django_cachex.compressors.gzip.GzipCompressor",  # Read old format
    ],
}
```
