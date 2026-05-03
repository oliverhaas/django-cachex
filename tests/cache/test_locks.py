"""Tests for lock operations."""

import threading
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


class TestBasicLockOperations:
    """Tests for basic lock acquisition and release."""

    def test_acquire_and_release_blocking_lock(self, cache: RespCache):
        resource_lock = cache.lock("resource_a")
        assert resource_lock.acquire(blocking=True) is True
        assert cache.has_key("resource_a") is True
        resource_lock.release()
        assert cache.has_key("resource_a") is False

    def test_nonblocking_lock_prevents_double_acquire(self, cache: RespCache):
        first_lock = cache.lock("resource_b")
        assert first_lock.acquire(blocking=False) is True

        second_lock = cache.lock("resource_b")
        assert second_lock.acquire(blocking=False) is False

        assert cache.has_key("resource_b") is True
        first_lock.release()
        assert cache.has_key("resource_b") is False

    def test_lock_context_manager(self, cache: RespCache):
        lock = cache.lock("ctx_resource", timeout=5)
        with lock:
            assert cache.has_key("ctx_resource") is True
        assert cache.has_key("ctx_resource") is False


class TestLockExtend:
    """Tests for extending the TTL of a held lock."""

    def test_extend_increases_ttl(self, cache: RespCache):
        lock = cache.lock("extend_resource", timeout=2)
        assert lock.acquire() is True
        try:
            before = cache.pttl("extend_resource")
            assert lock.extend(20) is True
            after = cache.pttl("extend_resource")
            # extend(20s) on top of the remaining ~2s ≈ 22s; assert it grew
            # well past the original timeout to rule out a no-op.
            assert after is not None and before is not None
            assert after > before
            assert after > 5_000
        finally:
            lock.release()


class TestLockRelease:
    """Tests for the release contract."""

    def test_double_release_raises(self, cache: RespCache):
        from redis.exceptions import LockError as RedisLockError

        from django_cachex.lock import LockError

        lock = cache.lock("dbl_release_resource", timeout=5)
        lock.acquire()
        lock.release()
        # The py driver raises ``redis.exceptions.LockError`` while the Rust
        # driver raises ``django_cachex.lock.LockError`` — both fulfill the
        # "double release is loud" contract.
        with pytest.raises((LockError, RedisLockError)):
            lock.release()


class TestCrossThreadLockRelease:
    """Tests for releasing locks from different threads."""

    def test_release_lock_from_different_thread(self, cache: RespCache):
        shared_lock = cache.lock("shared_resource", thread_local=False)
        assert shared_lock.acquire(blocking=True) is True

        def background_release(lock_obj):
            lock_obj.release()

        release_thread = threading.Thread(target=background_release, args=[shared_lock])
        release_thread.start()
        release_thread.join()

        assert cache.has_key("shared_resource") is False
