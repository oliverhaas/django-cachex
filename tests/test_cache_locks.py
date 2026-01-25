"""Tests for lock operations."""

import threading

from django_cachex.cache import KeyValueCache


class TestBasicLockOperations:
    """Tests for basic lock acquisition and release."""

    def test_acquire_and_release_blocking_lock(self, cache: KeyValueCache):
        resource_lock = cache.lock("resource_a")
        assert resource_lock.acquire(blocking=True) is True
        assert cache.has_key("resource_a") is True
        resource_lock.release()
        assert cache.has_key("resource_a") is False

    def test_nonblocking_lock_prevents_double_acquire(self, cache: KeyValueCache):
        first_lock = cache.lock("resource_b")
        assert first_lock.acquire(blocking=False) is True

        second_lock = cache.lock("resource_b")
        assert second_lock.acquire(blocking=False) is False

        assert cache.has_key("resource_b") is True
        first_lock.release()
        assert cache.has_key("resource_b") is False


class TestCrossThreadLockRelease:
    """Tests for releasing locks from different threads."""

    def test_release_lock_from_different_thread(self, cache: KeyValueCache):
        shared_lock = cache.lock("shared_resource", thread_local=False)
        assert shared_lock.acquire(blocking=True) is True

        def background_release(lock_obj):
            lock_obj.release()

        release_thread = threading.Thread(target=background_release, args=[shared_lock])
        release_thread.start()
        release_thread.join()

        assert cache.has_key("shared_resource") is False
