"""Tests for compatibility with Django's core RedisCache backend.

These tests verify that django-cachex produces identical behavior to
Django's builtin django.core.cache.backends.redis.RedisCache.
Both caches point to the same Redis instance to verify data interoperability.
"""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from django.core.cache.backends.redis import RedisCache as DjangoCoreRedisCache

    from django_cachex.cache import RedisCache as CachexRedisCache
    from tests.fixtures.containers import RedisContainerInfo


# Dedicated fixtures (not using the parametrized `cache` fixture) because:
# 1. We need TWO caches per test: Django core's and django-cachex's
# 2. Both must use matching serialization (default pickle) for cross-read compatibility
# 3. Both point to the same Redis db so values are shared


@pytest.fixture
def django_core_cache(
    redis_container: "RedisContainerInfo",
) -> "DjangoCoreRedisCache":
    """Create Django's core RedisCache pointing to the test Redis."""
    from django.core.cache.backends.redis import RedisCache

    host = redis_container.host
    port = redis_container.port
    location = f"redis://{host}:{port}/15"  # Use db 15 for compat tests
    cache = RedisCache(location, {"OPTIONS": {}})
    cache.clear()
    return cache


@pytest.fixture
def cachex_cache(redis_container: "RedisContainerInfo") -> "CachexRedisCache":
    """Create django-cachex cache pointing to the same Redis."""
    from django_cachex.cache import RedisCache

    host = redis_container.host
    port = redis_container.port
    location = f"redis://{host}:{port}/15"  # Same db as django_core_cache
    return RedisCache(location, {"OPTIONS": {}})


class TestDjangoCoreRedisCompatibility:
    """Test compatibility with Django's core RedisCache backend.

    These tests verify that django-cachex produces identical behavior to
    Django's builtin django.core.cache.backends.redis.RedisCache.
    Both caches point to the same Redis instance to verify data interoperability.
    """

    def test_set_get_cross_read(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """Values set by Django core can be read by django-cachex and vice versa."""
        # Django sets, cachex reads
        django_core_cache.set("django_key", "django_value")
        assert cachex_cache.get("django_key") == "django_value"

        # Cachex sets, Django reads
        cachex_cache.set("cachex_key", "cachex_value")
        assert django_core_cache.get("cachex_key") == "cachex_value"

        # Cleanup
        django_core_cache.delete("django_key")
        cachex_cache.delete("cachex_key")

    def test_set_get_same_result(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """Both caches return the same result for get operations."""
        test_values = [
            ("string_val", "hello world"),
            ("int_val", 42),
            ("float_val", 3.14159),
            ("list_val", [1, 2, 3, "four"]),
            ("dict_val", {"a": 1, "b": "two"}),
            ("none_val", None),
            ("bool_true", True),
            ("bool_false", False),
        ]

        for key, value in test_values:
            django_core_cache.set(key, value)
            cachex_cache.set(f"{key}_cx", value)

            # Both should return the same value
            django_result = django_core_cache.get(key)
            cachex_result = cachex_cache.get(f"{key}_cx")
            assert django_result == cachex_result, f"Mismatch for {key}: {django_result} != {cachex_result}"

            # Cross-read should also work
            assert cachex_cache.get(key) == value
            assert django_core_cache.get(f"{key}_cx") == value

        # Cleanup
        for key, _ in test_values:
            django_core_cache.delete(key)
            cachex_cache.delete(f"{key}_cx")

    def test_add_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """add() behaves identically - only sets if key doesn't exist."""
        # Both should succeed on new key
        django_result = django_core_cache.add("add_test_dj", "first")
        cachex_result = cachex_cache.add("add_test_cx", "first")
        assert django_result is True
        assert cachex_result is True

        # Both should fail on existing key
        django_result = django_core_cache.add("add_test_dj", "second")
        cachex_result = cachex_cache.add("add_test_cx", "second")
        assert django_result is False
        assert cachex_result is False

        # Original values should be preserved
        assert django_core_cache.get("add_test_dj") == "first"
        assert cachex_cache.get("add_test_cx") == "first"

        # Cleanup
        django_core_cache.delete("add_test_dj")
        cachex_cache.delete("add_test_cx")

    def test_delete_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """delete() behaves identically."""
        # Set values
        django_core_cache.set("del_test_dj", "value")
        cachex_cache.set("del_test_cx", "value")

        # Delete should return True for existing keys
        django_result = django_core_cache.delete("del_test_dj")
        cachex_result = cachex_cache.delete("del_test_cx")
        assert django_result is True
        assert cachex_result is True

        # Values should be gone
        assert django_core_cache.get("del_test_dj") is None
        assert cachex_cache.get("del_test_cx") is None

        # Delete of non-existent key should return False
        django_result = django_core_cache.delete("del_test_dj")
        cachex_result = cachex_cache.delete("del_test_cx")
        assert django_result is False
        assert cachex_result is False

    def test_has_key_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """has_key() behaves identically."""
        django_core_cache.set("has_key_dj", "value")
        cachex_cache.set("has_key_cx", "value")

        # Existing keys
        assert django_core_cache.has_key("has_key_dj") is True
        assert cachex_cache.has_key("has_key_cx") is True

        # Cross-check
        assert cachex_cache.has_key("has_key_dj") is True
        assert django_core_cache.has_key("has_key_cx") is True

        # Non-existent keys
        assert django_core_cache.has_key("nonexistent") is False
        assert cachex_cache.has_key("nonexistent") is False

        # Cleanup
        django_core_cache.delete("has_key_dj")
        cachex_cache.delete("has_key_cx")

    def test_get_many_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """get_many() behaves identically."""
        # Set some values with both caches
        django_core_cache.set("many_1", "v1")
        django_core_cache.set("many_2", "v2")
        cachex_cache.set("many_3", "v3")

        keys = ["many_1", "many_2", "many_3", "many_missing"]

        django_result = django_core_cache.get_many(keys)
        cachex_result = cachex_cache.get_many(keys)

        # Both should return same dict (excluding missing keys)
        assert django_result == cachex_result
        assert django_result == {"many_1": "v1", "many_2": "v2", "many_3": "v3"}

        # Cleanup
        django_core_cache.delete("many_1")
        django_core_cache.delete("many_2")
        cachex_cache.delete("many_3")

    def test_set_many_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """set_many() behaves identically."""
        data = {"sm_a": "val_a", "sm_b": "val_b", "sm_c": 123}

        # Both set_many calls
        django_core_cache.set_many(data)

        # Verify both can read what Django set
        for key, expected in data.items():
            assert django_core_cache.get(key) == expected
            assert cachex_cache.get(key) == expected

        # Clear and test cachex setting
        for key in data:
            django_core_cache.delete(key)

        cachex_cache.set_many(data)

        # Verify both can read what cachex set
        for key, expected in data.items():
            assert cachex_cache.get(key) == expected
            assert django_core_cache.get(key) == expected

        # Cleanup
        for key in data:
            cachex_cache.delete(key)

    def test_delete_many_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """delete_many() behaves identically."""
        # Set values
        django_core_cache.set("dm_1", "v1")
        django_core_cache.set("dm_2", "v2")
        cachex_cache.set("dm_3", "v3")

        # Delete with django
        django_core_cache.delete_many(["dm_1", "dm_2"])
        assert django_core_cache.get("dm_1") is None
        assert cachex_cache.get("dm_1") is None
        assert django_core_cache.get("dm_2") is None

        # dm_3 should still exist
        assert cachex_cache.get("dm_3") == "v3"

        # Delete with cachex
        cachex_cache.delete_many(["dm_3"])
        assert cachex_cache.get("dm_3") is None
        assert django_core_cache.get("dm_3") is None

    def test_incr_decr_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """incr() and decr() behave identically."""
        # Set initial values
        django_core_cache.set("counter_dj", 10)
        cachex_cache.set("counter_cx", 10)

        # Increment
        dj_result = django_core_cache.incr("counter_dj")
        cx_result = cachex_cache.incr("counter_cx")
        assert dj_result == 11
        assert cx_result == 11

        # Increment by delta
        dj_result = django_core_cache.incr("counter_dj", 5)
        cx_result = cachex_cache.incr("counter_cx", 5)
        assert dj_result == 16
        assert cx_result == 16

        # Decrement
        dj_result = django_core_cache.decr("counter_dj")
        cx_result = cachex_cache.decr("counter_cx")
        assert dj_result == 15
        assert cx_result == 15

        # Decrement by delta
        dj_result = django_core_cache.decr("counter_dj", 3)
        cx_result = cachex_cache.decr("counter_cx", 3)
        assert dj_result == 12
        assert cx_result == 12

        # Cross-read: both should see the same final values
        assert django_core_cache.get("counter_dj") == cachex_cache.get("counter_dj")
        assert django_core_cache.get("counter_cx") == cachex_cache.get("counter_cx")

        # Cleanup
        django_core_cache.delete("counter_dj")
        cachex_cache.delete("counter_cx")

    def test_touch_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """touch() behaves identically."""
        django_core_cache.set("touch_dj", "value", timeout=100)
        cachex_cache.set("touch_cx", "value", timeout=100)

        # Touch should return True for existing keys
        dj_result = django_core_cache.touch("touch_dj", timeout=200)
        cx_result = cachex_cache.touch("touch_cx", timeout=200)
        assert dj_result is True
        assert cx_result is True

        # Touch non-existent should return False
        dj_result = django_core_cache.touch("nonexistent_touch")
        cx_result = cachex_cache.touch("nonexistent_touch")
        assert dj_result is False
        assert cx_result is False

        # Cleanup
        django_core_cache.delete("touch_dj")
        cachex_cache.delete("touch_cx")

    def test_clear_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """clear() behaves identically - clears all keys."""
        # Set some values
        django_core_cache.set("clear_1", "v1")
        cachex_cache.set("clear_2", "v2")

        # Clear using django cache
        django_core_cache.clear()

        # All keys should be gone
        assert django_core_cache.get("clear_1") is None
        assert cachex_cache.get("clear_1") is None
        assert django_core_cache.get("clear_2") is None
        assert cachex_cache.get("clear_2") is None

    def test_get_or_set_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """get_or_set() behaves identically."""
        # Non-existent key - should set and return default
        dj_result = django_core_cache.get_or_set("gos_dj", "default_val")
        cx_result = cachex_cache.get_or_set("gos_cx", "default_val")
        assert dj_result == "default_val"
        assert cx_result == "default_val"

        # Key now exists - should return existing value
        dj_result = django_core_cache.get_or_set("gos_dj", "new_val")
        cx_result = cachex_cache.get_or_set("gos_cx", "new_val")
        assert dj_result == "default_val"
        assert cx_result == "default_val"

        # Cross-read
        assert cachex_cache.get("gos_dj") == "default_val"
        assert django_core_cache.get("gos_cx") == "default_val"

        # Cleanup
        django_core_cache.delete("gos_dj")
        cachex_cache.delete("gos_cx")

    def test_get_or_set_with_callable(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """get_or_set() with callable default behaves identically."""
        call_count = {"dj": 0, "cx": 0}

        def dj_factory() -> str:
            call_count["dj"] += 1
            return f"computed_{call_count['dj']}"

        def cx_factory() -> str:
            call_count["cx"] += 1
            return f"computed_{call_count['cx']}"

        # First call - callable should be invoked
        dj_result = django_core_cache.get_or_set("gos_callable_dj", dj_factory)
        cx_result = cachex_cache.get_or_set("gos_callable_cx", cx_factory)
        assert dj_result == "computed_1"
        assert cx_result == "computed_1"
        assert call_count["dj"] == 1
        assert call_count["cx"] == 1

        # Second call - callable should NOT be invoked
        dj_result = django_core_cache.get_or_set("gos_callable_dj", dj_factory)
        cx_result = cachex_cache.get_or_set("gos_callable_cx", cx_factory)
        assert dj_result == "computed_1"
        assert cx_result == "computed_1"
        assert call_count["dj"] == 1  # Not incremented
        assert call_count["cx"] == 1  # Not incremented

        # Cleanup
        django_core_cache.delete("gos_callable_dj")
        cachex_cache.delete("gos_callable_cx")

    def test_default_on_missing_key(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """get() with default parameter behaves identically."""
        dj_result = django_core_cache.get("missing_key", default="fallback")
        cx_result = cachex_cache.get("missing_key", default="fallback")
        assert dj_result == "fallback"
        assert cx_result == "fallback"

        # Without default, should return None
        dj_result = django_core_cache.get("missing_key")
        cx_result = cachex_cache.get("missing_key")
        assert dj_result is None
        assert cx_result is None

    def test_set_with_timeout(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """set() with timeout behaves identically."""
        import time

        # Set with very short timeout
        django_core_cache.set("timeout_dj", "value", timeout=1)
        cachex_cache.set("timeout_cx", "value", timeout=1)

        # Should exist immediately
        assert django_core_cache.get("timeout_dj") == "value"
        assert cachex_cache.get("timeout_cx") == "value"

        # Wait for expiration
        time.sleep(1.5)

        # Should be expired
        assert django_core_cache.get("timeout_dj") is None
        assert cachex_cache.get("timeout_cx") is None

    def test_set_with_none_timeout(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """set() with timeout=None uses default timeout."""
        # This tests that None timeout behaves consistently
        django_core_cache.set("none_timeout_dj", "value", timeout=None)
        cachex_cache.set("none_timeout_cx", "value", timeout=None)

        # Both should have the value
        assert django_core_cache.get("none_timeout_dj") == "value"
        assert cachex_cache.get("none_timeout_cx") == "value"

        # Cleanup
        django_core_cache.delete("none_timeout_dj")
        cachex_cache.delete("none_timeout_cx")

    def test_close_behavior(
        self,
        django_core_cache: "DjangoCoreRedisCache",
        cachex_cache: "CachexRedisCache",
    ) -> None:
        """close() should work without error on both."""
        # Set a value first
        django_core_cache.set("close_test", "value")

        # Close should not raise
        django_core_cache.close()
        cachex_cache.close()

        # After close, a new connection should be established on next use
        # (implementation detail, but behavior should be consistent)
