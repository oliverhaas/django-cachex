"""Tests for compressor fallback functionality."""

import gzip
import zlib


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


class TestDecompressFallback:
    """Tests for the _decompress fallback logic."""

    def test_decompress_gzip_with_multiple_compressors(self):
        from django_cachex.adapter import RedisAdapter

        client = RedisAdapter(
            servers=["redis://localhost:6379"],
            compressor=[
                "django_cachex.compressors.gzip.GzipCompressor",
                "django_cachex.compressors.zlib.ZlibCompressor",
            ],
        )

        data = b"Test data for compression! " * 50
        gzip_data = gzip.compress(data)

        assert client._decompress(gzip_data) == data

    def test_decompress_zlib_with_fallback(self):
        """Test that _decompress falls back to zlib for zlib-compressed data."""
        from django_cachex.adapter import RedisAdapter

        client = RedisAdapter(
            servers=["redis://localhost:6379"],
            compressor=[
                "django_cachex.compressors.gzip.GzipCompressor",
                "django_cachex.compressors.zlib.ZlibCompressor",
            ],
        )

        data = b"Test data for compression! " * 50
        zlib_data = zlib.compress(data)

        # gzip will fail, zlib should succeed
        assert client._decompress(zlib_data) == data

    def test_decompress_raises_when_all_compressors_fail(self):
        """When every configured compressor fails, _decompress raises CompressorError.

        Symmetric with _deserialize, which also raises on all-fail. Callers
        that want to tolerate raw payloads should keep the previously-used
        compressor at the end of the fallback chain (or omit compressors).
        """
        import pytest

        from django_cachex.adapter import RedisAdapter
        from django_cachex.exceptions import CompressorError

        client = RedisAdapter(
            servers=["redis://localhost:6379"],
            compressor=[
                "django_cachex.compressors.gzip.GzipCompressor",
            ],
        )

        # Plain data that isn't gzip — the only configured compressor fails.
        data = b"Plain uncompressed data"
        with pytest.raises(CompressorError):
            client._decompress(data)

    def test_decompress_raises_after_full_chain_fails(self):
        """_decompress walks the full chain before raising; raises when none succeed."""
        import pytest

        from django_cachex.adapter import RedisAdapter
        from django_cachex.exceptions import CompressorError

        client = RedisAdapter(
            servers=["redis://localhost:6379"],
            compressor=[
                "django_cachex.compressors.gzip.GzipCompressor",
                "django_cachex.compressors.zlib.ZlibCompressor",
            ],
        )

        # Looks like gzip (magic bytes) but isn't valid; zlib also fails.
        fake_gzip = b"\x1f\x8bNot actually valid gzip data"
        with pytest.raises(CompressorError):
            client._decompress(fake_gzip)

    def test_decompress_with_no_compressors_returns_raw(self):
        from django_cachex.adapter import RedisAdapter

        client = RedisAdapter(
            servers=["redis://localhost:6379"],
            compressor=None,
        )

        data = b"Plain uncompressed data"
        assert client._decompress(data) == data
