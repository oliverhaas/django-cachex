"""Tests for the semaphore primitive (local + RESP backends)."""


def test_semaphore_module_exports():
    from django_cachex.semaphore import (
        AsyncSemaphore,
        Semaphore,
        SemaphoreError,
        SemaphoreTimeoutError,
    )

    assert issubclass(SemaphoreTimeoutError, SemaphoreError)
    # AsyncSemaphore is a sibling of Semaphore, not a subclass, mirroring
    # Lock and AsyncLock.
    assert Semaphore is not AsyncSemaphore


class TestLocalCountingSemaphore:
    def test_acquire_within_capacity(self):
        from django_cachex.semaphore import Semaphore

        sem = Semaphore("counting", capacity=2)
        assert sem.acquire(blocking=False) is True
        assert sem.acquire(blocking=False) is True
        # capacity exhausted
        assert sem.acquire(blocking=False) is False

    def test_release_returns_capacity(self):
        from django_cachex.semaphore import Semaphore

        sem = Semaphore("counting2", capacity=1)
        assert sem.acquire(blocking=False) is True
        sem.release()
        assert sem.acquire(blocking=False) is True
