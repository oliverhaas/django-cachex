"""Tests for lock operations."""

import threading

from django_cachex.cache import RedisCache


class TestLockOperations:
    def test_lock(self, cache: RedisCache):
        lock = cache.lock("foobar")
        assert lock.acquire(blocking=True)

        assert cache.has_key("foobar")
        lock.release()
        assert not cache.has_key("foobar")

    def test_lock_not_blocking(self, cache: RedisCache):
        lock = cache.lock("foobar")
        assert lock.acquire(blocking=False)

        lock2 = cache.lock("foobar")

        assert not lock2.acquire(blocking=False)

        assert cache.has_key("foobar")
        lock.release()
        assert not cache.has_key("foobar")

    def test_lock_released_by_thread(self, cache: RedisCache):
        lock = cache.lock("foobar", thread_local=False)
        assert lock.acquire(blocking=True)

        def release_lock(lock_):
            lock_.release()

        t = threading.Thread(target=release_lock, args=[lock])
        t.start()
        t.join()

        assert not cache.has_key("foobar")
