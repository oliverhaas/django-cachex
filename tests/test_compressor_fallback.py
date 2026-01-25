"""Tests for compressor fallback functionality."""

import gzip
import zlib


class TestDefaultClientCompressorConfig:
    """Tests for DefaultClient compressor configuration handling."""

    def test_single_string_config_backwards_compatible(self, redis_container):
        """Test that single string compressor config still works."""
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
        """Test that list compressor config with fallback works."""
        from django.test import override_settings

        host, port = redis_container.host, redis_container.port

        caches = {
            "default": {
                "BACKEND": "django_cachex.cache.RedisCache",
                "LOCATION": f"redis://{host}:{port}?db=11",
                "OPTIONS": {
                    "compressor": [
                        "django_cachex.compressors.gzip.GzipCompressor",
                        None,  # Identity compressor
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
        """Test migrating from one compressor to another."""
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
                        None,
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


class TestHasCompressionEnabled:
    """Tests for _has_compression_enabled() with list config."""

    def test_string_identity_returns_false(self):
        """Test that identity compressor string returns False."""
        from django_cachex.client import RedisCacheClient

        client = RedisCacheClient(
            servers=["redis://localhost:6379"],
            options={
                "compressor": "django_cachex.compressors.identity.IdentityCompressor",
            },
        )

        assert client._has_compression_enabled() is False

    def test_string_gzip_returns_true(self):
        """Test that gzip compressor string returns True."""
        from django_cachex.client import RedisCacheClient

        client = RedisCacheClient(
            servers=["redis://localhost:6379"],
            options={
                "compressor": "django_cachex.compressors.gzip.GzipCompressor",
            },
        )

        assert client._has_compression_enabled() is True

    def test_list_with_identity_first_returns_false(self):
        """Test that list with identity first returns False."""
        from django_cachex.client import RedisCacheClient

        client = RedisCacheClient(
            servers=["redis://localhost:6379"],
            options={
                "compressor": [
                        "django_cachex.compressors.identity.IdentityCompressor",
                        "django_cachex.compressors.gzip.GzipCompressor",
                    ],
            },
        )

        assert client._has_compression_enabled() is False

    def test_list_with_none_first_returns_false(self):
        """Test that list with None first returns False."""
        from django_cachex.client import RedisCacheClient

        client = RedisCacheClient(
            servers=["redis://localhost:6379"],
            options={
                "compressor": [
                        None,
                        "django_cachex.compressors.gzip.GzipCompressor",
                    ],
            },
        )

        assert client._has_compression_enabled() is False

    def test_list_with_gzip_first_returns_true(self):
        """Test that list with gzip first returns True."""
        from django_cachex.client import RedisCacheClient

        client = RedisCacheClient(
            servers=["redis://localhost:6379"],
            options={
                "compressor": [
                        "django_cachex.compressors.gzip.GzipCompressor",
                        None,
                    ],
            },
        )

        assert client._has_compression_enabled() is True


class TestDecompressFallback:
    """Tests for the _decompress fallback logic."""

    def test_decompress_gzip_with_multiple_compressors(self):
        """Test that _decompress correctly decompresses gzip data."""
        from django_cachex.client import RedisCacheClient

        client = RedisCacheClient(
            servers=["redis://localhost:6379"],
            options={
                "compressor": [
                        "django_cachex.compressors.gzip.GzipCompressor",
                        "django_cachex.compressors.zlib.ZlibCompressor",
                        None,
                    ],
            },
        )

        data = b"Test data for compression! " * 50
        gzip_data = gzip.compress(data)

        assert client._decompress(gzip_data) == data

    def test_decompress_zlib_with_fallback(self):
        """Test that _decompress falls back to zlib for zlib-compressed data."""
        from django_cachex.client import RedisCacheClient

        client = RedisCacheClient(
            servers=["redis://localhost:6379"],
            options={
                "compressor": [
                        "django_cachex.compressors.gzip.GzipCompressor",
                        "django_cachex.compressors.zlib.ZlibCompressor",
                        None,
                    ],
            },
        )

        data = b"Test data for compression! " * 50
        zlib_data = zlib.compress(data)

        # gzip will fail, zlib should succeed
        assert client._decompress(zlib_data) == data

    def test_decompress_returns_raw_when_all_fail(self):
        """Test that _decompress returns raw bytes when all compressors fail."""
        from django_cachex.client import RedisCacheClient

        # Only gzip, no identity fallback
        client = RedisCacheClient(
            servers=["redis://localhost:6379"],
            options={
                "compressor": [
                        "django_cachex.compressors.gzip.GzipCompressor",
                    ],
            },
        )

        # Plain data that isn't gzip
        data = b"Plain uncompressed data"
        assert client._decompress(data) == data

    def test_decompress_with_identity_catches_all(self):
        """Test that identity compressor at end catches uncompressed data."""
        from django_cachex.client import RedisCacheClient

        client = RedisCacheClient(
            servers=["redis://localhost:6379"],
            options={
                "compressor": [
                        "django_cachex.compressors.gzip.GzipCompressor",
                        None,  # Identity
                    ],
            },
        )

        data = b"Plain uncompressed data"
        # gzip fails, identity returns as-is
        assert client._decompress(data) == data

    def test_decompress_continues_on_failure(self):
        """Test that _decompress continues to next compressor on failure."""
        from django_cachex.client import RedisCacheClient

        client = RedisCacheClient(
            servers=["redis://localhost:6379"],
            options={
                "compressor": [
                        "django_cachex.compressors.gzip.GzipCompressor",
                        None,
                    ],
            },
        )

        # Data that looks like it could be gzip but isn't valid
        fake_gzip = b"\x1f\x8bNot actually valid gzip data"
        # gzip fails, falls through to identity
        assert client._decompress(fake_gzip) == fake_gzip
