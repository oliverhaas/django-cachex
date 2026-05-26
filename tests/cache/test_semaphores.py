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


class TestLocalWeightedSemaphore:
    def test_weight_consumes_capacity(self):
        from django_cachex.semaphore import Semaphore

        sem_a = Semaphore("weighted", capacity=10, weight=7)
        assert sem_a.acquire(blocking=False) is True

        sem_b = Semaphore("weighted", capacity=10, weight=4)
        # 7 + 4 > 10, so this should not fit.
        assert sem_b.acquire(blocking=False) is False

        sem_c = Semaphore("weighted", capacity=10, weight=3)
        # 7 + 3 == 10, exact fit.
        assert sem_c.acquire(blocking=False) is True

    def test_weight_exceeds_capacity_rejected(self):
        import pytest

        from django_cachex.semaphore import Semaphore

        with pytest.raises(ValueError, match="exceeds capacity"):
            Semaphore("bad_weight", capacity=5, weight=6)
