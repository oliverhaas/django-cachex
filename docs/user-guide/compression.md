# Compression

The `compressor` option compresses cached values to save server memory:

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

## Available Compressors

| Compressor | Extra | Default `level` |
|------------|-------|-----------------|
| `django_cachex.compressors.zlib.ZlibCompressor` | (stdlib) | `6` |
| `django_cachex.compressors.gzip.GzipCompressor` | (stdlib) | `9` |
| `django_cachex.compressors.lzma.LzmaCompressor` | (stdlib) | preset `4` |
| `django_cachex.compressors.lz4.Lz4Compressor` | `lz4` | `0` (fast mode) |
| `django_cachex.compressors.zstd.ZstdCompressor` | (stdlib on 3.14+) | `3` |

Install the `lz4` extra for `Lz4Compressor`:

```console
uv add django-cachex[lz4]
```

## Minimum size and level

`min_length` and `level` are keyword-only constructor arguments, not keys in
`OPTIONS`. A compressor stores payloads of `min_length` bytes or less
uncompressed, and `min_length` defaults to `256`. To change either argument,
configure an instance or a subclass instead of a dotted path:

```python
from django_cachex.compressors.zstd import ZstdCompressor

"OPTIONS": {
    "compressor": ZstdCompressor(min_length=1024, level=10),
}


# A subclass stays configurable by dotted path.
class LargeOnlyZstdCompressor(ZstdCompressor):
    min_length = 1024
    level = 10
```

## Choosing a compressor

- `zstd` matches the `lzma` output size at a much higher speed and costs little end-to-end throughput. Pick it by default.
- `lz4` is the fastest and suits workloads where CPU is the bottleneck. Its output is the largest of the five, and its end-to-end throughput is close to no compression.
- `lzma` fits only when output size matters more than write latency. Even then, `zstd` reaches the same size faster.
- `zlib` and `gzip` are nearly identical. Pick `zlib` unless an external consumer needs gzip's framing.
- No compression gives the highest throughput and uses the most server memory.

Ratios depend on the payload, and already-compressed bytes barely shrink.
[Benchmarks](../reference/benchmarks.md#compressors-macro) has the numbers.

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
