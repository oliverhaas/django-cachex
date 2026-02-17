"""Tests for key operations: version, delete_pattern, iter_keys, etc."""

from collections.abc import Iterable
from contextlib import suppress

import pytest
from django.core.cache import caches
from django.test import override_settings

from django_cachex.cache import KeyValueCache
from tests.settings_wrapper import SettingsWrapper


@pytest.fixture
def patch_itersize_setting() -> Iterable[None]:
    # destroy cache to force recreation with overriden settings
    with suppress(AttributeError):
        del caches["default"]
    with override_settings(DJANGO_REDIS_SCAN_ITERSIZE=30):
        yield
    # destroy cache to force recreation with original settings
    with suppress(AttributeError):
        del caches["default"]


class TestVersionOperations:
    def test_version(self, cache: KeyValueCache):
        cache.set("keytest", 2, version=2)
        res = cache.get("keytest")
        assert res is None

        res = cache.get("keytest", version=2)
        assert res == 2

    def test_incr_version(self, cache: KeyValueCache):
        # Use hash tag so versioned keys stay in same cluster slot
        cache.set("{keytest}", 2)
        cache.incr_version("{keytest}")

        res = cache.get("{keytest}")
        assert res is None

        res = cache.get("{keytest}", version=2)
        assert res == 2

    def test_ttl_incr_version_no_timeout(self, cache: KeyValueCache):
        # Use hash tag so versioned keys stay in same cluster slot
        cache.set("{my_key}", "hello world!", timeout=None)

        cache.incr_version("{my_key}")

        my_value = cache.get("{my_key}", version=2)

        assert my_value == "hello world!"


class TestRenameOperations:
    # Note: Use {slot}: prefix to ensure both keys hash to same cluster slot

    def test_rename(self, cache: KeyValueCache):
        cache.set("{slot}:src", "value1")
        cache.rename("{slot}:src", "{slot}:dest")

        assert cache.get("{slot}:src") is None
        assert cache.get("{slot}:dest") == "value1"

    def test_rename_overwrites_existing(self, cache: KeyValueCache):
        cache.set("{slot2}:src", "src_value")
        cache.set("{slot2}:dst", "dst_value")
        cache.rename("{slot2}:src", "{slot2}:dst")

        assert cache.get("{slot2}:src") is None
        assert cache.get("{slot2}:dst") == "src_value"

    def test_rename_preserves_ttl(self, cache: KeyValueCache):
        cache.set("{slot3}:key", "value", timeout=3600)
        cache.rename("{slot3}:key", "{slot3}:dest")

        ttl = cache.ttl("{slot3}:dest")
        assert ttl is not None
        assert ttl > 3500  # Should be close to 3600

    def test_rename_nonexistent_raises(self, cache: KeyValueCache):
        with pytest.raises(ValueError, match="not found"):
            cache.rename("{slot4}:nonexistent", "{slot4}:dest")

    def test_renamenx(self, cache: KeyValueCache):
        cache.set("{slot5}:src", "value")
        result = cache.renamenx("{slot5}:src", "{slot5}:dest")

        assert result is True
        assert cache.get("{slot5}:src") is None
        assert cache.get("{slot5}:dest") == "value"

    def test_renamenx_fails_if_dest_exists(self, cache: KeyValueCache):
        cache.set("{slot6}:src", "src_value")
        cache.set("{slot6}:dest", "existing_value")
        result = cache.renamenx("{slot6}:src", "{slot6}:dest")

        assert result is False
        assert cache.get("{slot6}:src") == "src_value"
        assert cache.get("{slot6}:dest") == "existing_value"

    def test_rename_version_src_dst(self, cache: KeyValueCache):
        """rename with different source and destination versions."""
        cache.set("{vs}:rsrc", "value", version=1)

        cache.rename("{vs}:rsrc", "{vs}:rdst", version_src=1, version_dst=2)
        assert cache.get("{vs}:rsrc", version=1) is None
        assert cache.get("{vs}:rdst", version=2) == "value"

    def test_renamenx_version_src_dst(self, cache: KeyValueCache):
        """renamenx with different source and destination versions."""
        cache.set("{vs}:rnxsrc", "value", version=1)

        result = cache.renamenx("{vs}:rnxsrc", "{vs}:rnxdst", version_src=1, version_dst=2)
        assert result is True
        assert cache.get("{vs}:rnxsrc", version=1) is None
        assert cache.get("{vs}:rnxdst", version=2) == "value"


class TestDeletePatternOperations:
    def test_delete_pattern(self, cache: KeyValueCache):
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")

        res = cache.delete_pattern("*foo-a*")
        assert bool(res) is True

        keys = cache.keys("foo*")
        assert set(keys) == {"foo-bb", "foo-bc"}

        res = cache.delete_pattern("*foo-a*")
        assert bool(res) is False

    def test_delete_pattern_with_custom_count(self, cache: KeyValueCache):
        """Test delete_pattern with custom itersize."""
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")

        # Test that custom itersize works (we can't easily verify the internal itersize,
        # but we can verify the result is correct)
        res = cache.delete_pattern("*foo-a*", itersize=2)
        assert bool(res) is True

        keys = cache.keys("foo*")
        assert set(keys) == {"foo-bb", "foo-bc"}

    def test_delete_pattern_with_settings_default_scan_count(
        self,
        patch_itersize_setting,
        cache: KeyValueCache,
        settings: SettingsWrapper,
    ):
        """Test delete_pattern uses settings for default itersize."""
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")

        # Verify the setting is applied correctly
        assert settings.DJANGO_REDIS_SCAN_ITERSIZE == 30

        res = cache.delete_pattern("*foo-a*")
        assert bool(res) is True

        keys = cache.keys("foo*")
        assert set(keys) == {"foo-bb", "foo-bc"}


class TestIterKeysOperations:
    def test_iter_keys(self, cache: KeyValueCache):
        cache.set("foo1", 1)
        cache.set("foo2", 1)
        cache.set("foo3", 1)

        # Test simple result
        result = set(cache.iter_keys("foo*"))
        assert result == {"foo1", "foo2", "foo3"}

    def test_iter_keys_itersize(self, cache: KeyValueCache):
        cache.set("foo1", 1)
        cache.set("foo2", 1)
        cache.set("foo3", 1)

        # Test limited result
        result = list(cache.iter_keys("foo*", itersize=2))
        assert len(result) == 3

    def test_iter_keys_generator(self, cache: KeyValueCache):
        cache.set("foo1", 1)
        cache.set("foo2", 1)
        cache.set("foo3", 1)

        # Test generator object
        result = cache.iter_keys("foo*")
        next_value = next(result)
        assert next_value is not None
