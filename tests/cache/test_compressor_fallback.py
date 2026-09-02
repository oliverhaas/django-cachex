"""Tests for compressor fallback functionality."""

import gzip
import zlib

import pytest
from django.core.cache import cache
from django.test import override_settings

from django_cachex.exceptions import CompressorError, SerializerError
from tests.cache.support import make_cache

GZIP_THEN_ZLIB = [
    "django_cachex.compressors.gzip.GzipCompressor",
    "django_cachex.compressors.zlib.ZlibCompressor",
]


def _caches(host: str, port: int, compressor, db: int) -> dict:
    return {
        "default": {
            "BACKEND": "django_cachex.cache.RedisCache",
            "LOCATION": f"redis://{host}:{port}?db={db}",
            "OPTIONS": {"compressor": compressor},
        },
    }


class TestCompressorConfig:
    def test_single_string_config_backwards_compatible(self, redis_container):
        caches = _caches(
            redis_container.host,
            redis_container.port,
            "django_cachex.compressors.gzip.GzipCompressor",
            db=10,
        )

        with override_settings(CACHES=caches):
            cache.set("test_key", "test_value" * 100)
            assert cache.get("test_key") == "test_value" * 100
            cache.delete("test_key")

    def test_list_config_writes_with_the_first_compressor(self, redis_container):
        caches = _caches(redis_container.host, redis_container.port, GZIP_THEN_ZLIB, db=11)

        with override_settings(CACHES=caches):
            cache.set("test_key", "test_value" * 100)
            assert [type(c).__name__ for c in cache._compressors] == ["GzipCompressor", "ZlibCompressor"]
            assert cache.get("test_key") == "test_value" * 100
            cache.delete("test_key")

    def test_small_and_large_value_roundtrip(self, redis_container):
        """Values at or below min_length are stored uncompressed and must still read back."""
        caches = _caches(
            redis_container.host,
            redis_container.port,
            "django_cachex.compressors.zlib.ZlibCompressor",
            db=13,
        )

        with override_settings(CACHES=caches):
            # Serialized form is below the 256-byte min_length: stored raw.
            cache.set("small_key", "tiny")
            assert cache.get("small_key") == "tiny"

            # Above min_length: stored compressed.
            cache.set("large_key", "test_value" * 100)
            assert cache.get("large_key") == "test_value" * 100

            cache.delete("small_key")
            cache.delete("large_key")

    def test_migration_scenario(self, redis_container):
        host, port = redis_container.host, redis_container.port

        caches_zlib = _caches(host, port, "django_cachex.compressors.zlib.ZlibCompressor", db=12)
        with override_settings(CACHES=caches_zlib):
            cache.set("old_key", "old_value" * 100)

        caches_gzip_fallback = _caches(host, port, GZIP_THEN_ZLIB, db=12)
        with override_settings(CACHES=caches_gzip_fallback):
            assert cache.get("old_key") == "old_value" * 100

            cache.set("new_key", "new_value" * 100)
            assert cache.get("new_key") == "new_value" * 100

            cache.delete("old_key")
            cache.delete("new_key")


class TestDecompressFallback:
    """Tests for the _decompress fallback logic on the cache layer."""

    def test_decompress_gzip_with_multiple_compressors(self):
        cache_obj = make_cache(compressor=GZIP_THEN_ZLIB)
        data = b"Test data for compression! " * 50
        assert cache_obj._decompress(gzip.compress(data)) == data

    def test_decompress_zlib_with_fallback(self):
        cache_obj = make_cache(compressor=GZIP_THEN_ZLIB)
        data = b"Test data for compression! " * 50
        assert cache_obj._decompress(zlib.compress(data)) == data

    def test_decompress_returns_raw_when_all_compressors_fail(self):
        """When every configured compressor fails, _decompress returns the raw bytes."""
        cache_obj = make_cache(compressor=["django_cachex.compressors.gzip.GzipCompressor"])
        data = b"Plain uncompressed data"
        assert cache_obj._decompress(data) == data

    def test_decompress_returns_raw_after_full_chain_fails(self):
        cache_obj = make_cache(compressor=GZIP_THEN_ZLIB)
        # Looks like gzip (magic bytes) but isn't valid; zlib also fails.
        fake_gzip = b"\x1f\x8bNot actually valid gzip data"
        assert cache_obj._decompress(fake_gzip) == fake_gzip

    def test_decode_corrupt_payload_raises_serializer_error(self):
        """A genuinely corrupt payload still fails on read, from the deserializer."""
        cache_obj = make_cache(compressor=["django_cachex.compressors.gzip.GzipCompressor"])
        # Gzip magic bytes with an invalid stream, well above min_length;
        # not valid pickle either.
        corrupt = b"\x1f\x8b" + b"\xff" * 300
        with pytest.raises(SerializerError):
            cache_obj.decode(corrupt)

    def test_decompress_with_no_compressors_returns_raw(self):
        cache_obj = make_cache(compressor=None)
        data = b"Plain uncompressed data"
        assert cache_obj._decompress(data) == data

    def test_empty_compressor_list_means_no_compression(self):
        cache_obj = make_cache(compressor=[])
        assert cache_obj._compressors == []
        data = b"Plain uncompressed data"
        assert cache_obj._decompress(data) == data


class TestCompressorError:
    def test_decompress_error_names_the_compressor_and_the_cause(self):
        compressor = make_cache(compressor=GZIP_THEN_ZLIB)._compressors[0]
        with pytest.raises(CompressorError, match="GzipCompressor could not decompress 6 bytes") as excinfo:
            compressor.decompress(b"nogzip")
        assert excinfo.value.__cause__ is not None
