"""Tests to verify client library parametrization works correctly."""

from django_cachex.cache import KeyValueCache


class TestClientLibraries:
    """Test that different client libraries work correctly."""

    def test_basic_set_get_with_all_images(self, cache: KeyValueCache, redis_images: tuple[str, str]):
        """Verify basic operations work with all Redis images and their coupled client libraries.

        By including `redis_images` as a direct parameter, pytest will parametrize
        the test across all configured images (redis, redis-stack, valkey).
        """
        image, client_library = redis_images

        # Verify we're using the expected client library
        cache.set("test_key", "hello")
        result = cache.get("test_key")
        assert result == "hello", f"Failed with image={image}, client_library={client_library}"

        # Verify integer handling
        cache.set("int_key", 42)
        result = cache.get("int_key")
        assert result == 42, f"Integer handling failed with image={image}, client_library={client_library}"

    def test_with_native_parser(
        self,
        cache: KeyValueCache,
        redis_images: tuple[str, str],
        native_parser: bool,
    ):
        """Verify native parser (hiredis/libvalkey) works correctly.

        By including `native_parser` as a direct parameter, pytest will parametrize
        the test across both parser values (True/False).
        """
        image, client_library = redis_images
        parser_type = "native" if native_parser else "python"

        # Basic operations should work regardless of parser
        cache.set("test_key", {"nested": {"data": [1, 2, 3]}})
        result = cache.get("test_key")
        assert result == {"nested": {"data": [1, 2, 3]}}, (
            f"Failed with image={image}, client={client_library}, parser={parser_type}"
        )
