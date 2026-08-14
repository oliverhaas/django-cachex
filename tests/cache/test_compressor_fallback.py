"""Tests for compressor fallback functionality."""

import gzip
import zlib
from typing import Any

import pytest

from django_cachex.exceptions import SerializerError


class TestDefaultClientCompressorConfig:
    """Tests for DefaultClient compressor configuration handling."""

    def test_single_string_config_backwards_compatible(self, redis_container):
        from django.test import override_settings

        host, port = redis_container.host, redis_container.port

        caches = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}?db=10",
                "OPTIONS": {
                    "compressor": "django_cachex.compressors.gzip.GzipCompressor",
                },
            },
        }

        with override_settings(CACHES=caches):
            from django.core.cache import cache

            cache.set("test_key", "test_value" * 100)
            assert cache.get("test_key") == "test_value" * 100
            cache.delete("test_key")

    def test_list_config_with_fallback(self, redis_container):
        from django.test import override_settings

        host, port = redis_container.host, redis_container.port

        caches = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}?db=11",
                "OPTIONS": {
                    "compressor": [
                        "django_cachex.compressors.gzip.GzipCompressor",
                        "django_cachex.compressors.zlib.ZlibCompressor",
                    ],
                },
            },
        }

        with override_settings(CACHES=caches):
            from django.core.cache import cache

            # Write with gzip
            cache.set("test_key", "test_value" * 100)
            assert cache.get("test_key") == "test_value" * 100
            cache.delete("test_key")

    def test_small_and_large_value_roundtrip(self, redis_container):
        """Values at or below min_length are stored uncompressed and must still read back.

        Regression: _decompress used to raise CompressorError for any payload
        the configured compressor couldn't decompress, which broke every
        small value when a compressor was configured.
        """
        from django.test import override_settings

        host, port = redis_container.host, redis_container.port

        caches = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}?db=13",
                "OPTIONS": {
                    "compressor": "django_cachex.compressors.zlib.ZlibCompressor",
                },
            },
        }

        with override_settings(CACHES=caches):
            from django.core.cache import cache

            # Serialized form is below the 256-byte min_length: stored raw.
            cache.set("small_key", "tiny")
            assert cache.get("small_key") == "tiny"

            # Above min_length: stored compressed.
            cache.set("large_key", "test_value" * 100)
            assert cache.get("large_key") == "test_value" * 100

            cache.delete("small_key")
            cache.delete("large_key")

    def test_migration_scenario(self, redis_container):
        from django.test import override_settings

        host, port = redis_container.host, redis_container.port

        # Step 1: Write with zlib
        caches_zlib = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}?db=12",
                "OPTIONS": {
                    "compressor": "django_cachex.compressors.zlib.ZlibCompressor",
                },
            },
        }

        with override_settings(CACHES=caches_zlib):
            from django.core.cache import cache

            cache.set("old_key", "old_value" * 100)

        # Step 2: Switch to gzip with zlib fallback
        caches_gzip_fallback = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}?db=12",
                "OPTIONS": {
                    "compressor": [
                        "django_cachex.compressors.gzip.GzipCompressor",
                        "django_cachex.compressors.zlib.ZlibCompressor",
                    ],
                },
            },
        }

        with override_settings(CACHES=caches_gzip_fallback):
            from django.core.cache import cache

            # Should read old zlib-compressed data via fallback
            assert cache.get("old_key") == "old_value" * 100

            # Write new data with gzip
            cache.set("new_key", "new_value" * 100)
            assert cache.get("new_key") == "new_value" * 100

            cache.delete("old_key")
            cache.delete("new_key")


def _make_cache(*, compressor: Any = None, serializer: Any = None) -> Any:
    """Construct a :class:`RedisCache` purely to exercise the encoding stack.

    ``_decompress`` / ``_deserialize`` / ``encode`` / ``decode`` live on the
    cache layer; the adapter is wire-only. The cache's ``adapter`` property
    is lazy, so we never actually open a connection.
    """
    from django_cachex.cache import RedisCache

    options: dict[str, Any] = {}
    if compressor is not None:
        options["compressor"] = compressor
    if serializer is not None:
        options["serializer"] = serializer
    return RedisCache(server="redis://localhost:6379", params={"OPTIONS": options})


class TestDecompressFallback:
    """Tests for the _decompress fallback logic on the cache layer."""

    def test_decompress_gzip_with_multiple_compressors(self):
        cache = _make_cache(
            compressor=[
                "django_cachex.compressors.gzip.GzipCompressor",
                "django_cachex.compressors.zlib.ZlibCompressor",
            ],
        )
        data = b"Test data for compression! " * 50
        gzip_data = gzip.compress(data)
        assert cache._decompress(gzip_data) == data

    def test_decompress_zlib_with_fallback(self):
        """Test that _decompress falls back to zlib for zlib-compressed data."""
        cache = _make_cache(
            compressor=[
                "django_cachex.compressors.gzip.GzipCompressor",
                "django_cachex.compressors.zlib.ZlibCompressor",
            ],
        )
        data = b"Test data for compression! " * 50
        zlib_data = zlib.compress(data)
        # gzip will fail, zlib should succeed
        assert cache._decompress(zlib_data) == data

    def test_decompress_returns_raw_when_all_compressors_fail(self):
        """When every configured compressor fails, _decompress returns the raw bytes.

        compress() stores payloads at or below min_length uncompressed, so
        the read path must hand raw bytes through to the deserializer
        instead of raising CompressorError.
        """
        cache = _make_cache(compressor=["django_cachex.compressors.gzip.GzipCompressor"])
        # Plain data that isn't gzip; the only configured compressor fails.
        data = b"Plain uncompressed data"
        assert cache._decompress(data) == data

    def test_decompress_returns_raw_after_full_chain_fails(self):
        """_decompress walks the full chain and falls back to raw bytes when none succeed."""
        cache = _make_cache(
            compressor=[
                "django_cachex.compressors.gzip.GzipCompressor",
                "django_cachex.compressors.zlib.ZlibCompressor",
            ],
        )
        # Looks like gzip (magic bytes) but isn't valid; zlib also fails.
        fake_gzip = b"\x1f\x8bNot actually valid gzip data"
        assert cache._decompress(fake_gzip) == fake_gzip

    def test_decode_corrupt_payload_raises_serializer_error(self):
        """A genuinely corrupt payload still fails on read, from the deserializer.

        The raw-bytes fallback in _decompress must not turn corruption into
        a silent success: bytes that no compressor and no serializer accept
        surface as SerializerError.
        """
        cache = _make_cache(compressor=["django_cachex.compressors.gzip.GzipCompressor"])
        # Gzip magic bytes with an invalid stream, well above min_length;
        # not valid pickle either.
        corrupt = b"\x1f\x8b" + b"\xff" * 300
        with pytest.raises(SerializerError):
            cache.decode(corrupt)

    def test_decompress_with_no_compressors_returns_raw(self):
        cache = _make_cache(compressor=None)
        data = b"Plain uncompressed data"
        assert cache._decompress(data) == data

    def test_empty_compressor_list_means_no_compression(self):
        cache = _make_cache(compressor=[])
        assert cache._compressors == []
        data = b"Plain uncompressed data"
        assert cache._decompress(data) == data
