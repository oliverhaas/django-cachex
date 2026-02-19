"""Tests for miscellaneous cache operations: scan, decr_version, clear_all_versions, flush_db."""

import pytest

from django_cachex.cache import KeyValueCache
from django_cachex.exceptions import NotSupportedError


class TestScanOperations:
    def _is_cluster(self, client_class: str, sentinel_mode: str | bool) -> bool:
        """Check if using a real cluster client (sentinel mode overrides cluster)."""
        return client_class == "cluster" and not sentinel_mode

    def test_scan_returns_keys(self, cache: KeyValueCache, client_class: str, sentinel_mode: str | bool):
        if self._is_cluster(client_class, sentinel_mode):
            with pytest.raises(NotSupportedError):
                cache.scan(pattern="scantest_*")
            return

        cache.set("scantest_a", 1)
        cache.set("scantest_b", 2)

        cursor, keys = cache.scan(pattern="scantest_*")
        assert isinstance(keys, list)
        all_keys = set(keys)
        while cursor != 0:
            cursor, keys = cache.scan(cursor=cursor, pattern="scantest_*")
            all_keys.update(keys)
        assert all_keys == {"scantest_a", "scantest_b"}

    def test_scan_empty(self, cache: KeyValueCache, client_class: str, sentinel_mode: str | bool):
        if self._is_cluster(client_class, sentinel_mode):
            with pytest.raises(NotSupportedError):
                cache.scan(pattern="nonexistent_pattern_xyz_*")
            return

        _cursor, keys = cache.scan(pattern="nonexistent_pattern_xyz_*")
        assert keys == []


class TestDecrVersionOperations:
    def test_decr_version(self, cache: KeyValueCache):
        # Use hash tag so versioned keys stay in same cluster slot
        cache.set("{dv}:key", "hello", version=2)
        new_version = cache.decr_version("{dv}:key", version=2)

        assert new_version == 1
        assert cache.get("{dv}:key", version=2) is None
        assert cache.get("{dv}:key", version=1) == "hello"

    def test_decr_version_default(self, cache: KeyValueCache):
        # Set at default version (1), decrement to version 0
        cache.set("{dv2}:key", "value")
        new_version = cache.decr_version("{dv2}:key")

        assert new_version == 0
        assert cache.get("{dv2}:key") is None
        assert cache.get("{dv2}:key", version=0) == "value"


class TestClearAllVersions:
    def test_clear_all_versions(self, cache: KeyValueCache):
        cache.set("cav_key1", "v1", version=1)
        cache.set("cav_key2", "v2", version=2)

        count = cache.clear_all_versions()
        assert count >= 2

        assert cache.get("cav_key1", version=1) is None
        assert cache.get("cav_key2", version=2) is None


class TestFlushDb:
    def test_flush_db(self, cache: KeyValueCache):
        cache.set("flush_key", "value")
        assert cache.get("flush_key") == "value"

        result = cache.flush_db()
        assert result is True
        assert cache.get("flush_key") is None
