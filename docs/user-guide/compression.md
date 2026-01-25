# Compression

django-cachex supports pluggable compression to reduce memory usage. Compression is only applied to values larger than 256 bytes by default.

## Configuration

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379/1",
        "OPTIONS": {
            "compressor": "django_cachex.compressors.zstd.ZStdCompressor",
        }
    }
}
```

## Available Compressors

| Compressor | Speed | Ratio | Dependency |
|------------|-------|-------|------------|
| `django_cachex.compressors.zlib.ZlibCompressor` | Medium | Good | Built-in |
| `django_cachex.compressors.gzip.GzipCompressor` | Medium | Good | Built-in |
| `django_cachex.compressors.lzma.LzmaCompressor` | Slow | Best | Built-in |
| `django_cachex.compressors.lz4.Lz4Compressor` | Fast | Moderate | `django-cachex[lz4]` |
| `django_cachex.compressors.zstd.ZStdCompressor` | Fast | Good | `django-cachex[zstd]` (Python < 3.14) |

Install optional dependencies:

```console
uv add django-cachex[lz4]   # For LZ4
uv add django-cachex[zstd]  # For Zstandard (Python < 3.14 only)
```

Zstandard uses the built-in `compression.zstd` module on Python 3.14+.

## Fallback for Migration

Specify a list of compressors to safely migrate between formats. The first is used for writing, all are tried for reading:

```python
"OPTIONS": {
    "compressor": [
        "django_cachex.compressors.zstd.ZStdCompressor",  # Write with new format
        "django_cachex.compressors.gzip.GzipCompressor",  # Read old format
    ],
}
```
