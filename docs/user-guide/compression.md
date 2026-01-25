# Compression

django-cachex supports several compression backends to reduce memory usage.

## Zlib Compression

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "COMPRESSOR": "django_cachex.compressors.zlib.ZlibCompressor",
        }
    }
}
```

## Gzip Compression

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "COMPRESSOR": "django_cachex.compressors.gzip.GzipCompressor",
        }
    }
}
```

## LZMA Compression

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "COMPRESSOR": "django_cachex.compressors.lzma.LzmaCompressor",
        }
    }
}
```

## LZ4 Compression

Requires the `lz4` library:

```console
pip install lz4
```

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "COMPRESSOR": "django_cachex.compressors.lz4.Lz4Compressor",
        }
    }
}
```

## Zstandard (zstd) Compression

On Python 3.14+, zstd compression uses the built-in `compression.zstd` module.
On older Python versions, it falls back to `backports-zstd`.

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "COMPRESSOR": "django_cachex.compressors.zstd.ZStdCompressor",
        }
    }
}
```

## Compressor Fallback (Migration Support)

When migrating from one compressor to another, you can specify a list of compressors.
The first compressor is used for writing new data, while all compressors are tried
in order when reading until one succeeds.

This allows safe migration between compression formats without data loss:

```python
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            # First compressor used for writing, all tried for reading
            "COMPRESSOR": [
                "django_cachex.compressors.zstd.ZStdCompressor",  # New format
                "django_cachex.compressors.gzip.GzipCompressor",  # Old format
            ],
        }
    }
}
```

### Migration Example

1. **Before migration** - using gzip:
   ```python
   "COMPRESSOR": "django_cachex.compressors.gzip.GzipCompressor"
   ```

2. **During migration** - write zstd, read both:
   ```python
   "COMPRESSOR": [
       "django_cachex.compressors.zstd.ZStdCompressor",
       "django_cachex.compressors.gzip.GzipCompressor",
   ]
   ```

3. **After migration** - all data refreshed with zstd:
   ```python
   "COMPRESSOR": "django_cachex.compressors.zstd.ZStdCompressor"
   ```

### How It Works

When decompressing, each compressor is tried in order. If decompression fails,
the next compressor is tried. This continues until one succeeds or all fail.
If all compressors fail, the raw value is returned (allowing uncompressed data to pass through).

## Compression Comparison

| Compressor | Speed | Ratio | Dependencies |
|------------|-------|-------|--------------|
| Zlib | Medium | Good | Built-in |
| Gzip | Medium | Good | Built-in |
| LZMA | Slow | Best | Built-in |
| LZ4 | Fast | Moderate | `lz4` |
| Zstandard | Fast | Good | `pyzstd` |
