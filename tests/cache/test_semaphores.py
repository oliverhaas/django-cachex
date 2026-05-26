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


class TestLocalBlockingAcquire:
    def test_blocking_acquire_waits_for_release(self):
        import threading
        import time

        from django_cachex.semaphore import Semaphore

        sem_holder = Semaphore("block", capacity=1)
        assert sem_holder.acquire(blocking=False) is True

        result = {}
        sem_waiter = Semaphore("block", capacity=1)

        def waiter():
            t0 = time.monotonic()
            ok = sem_waiter.acquire(blocking=True, timeout=2)
            result["ok"] = ok
            result["elapsed"] = time.monotonic() - t0

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.1)
        sem_holder.release()
        t.join(timeout=3)

        assert result["ok"] is True
        assert result["elapsed"] >= 0.1
        sem_waiter.release()

    def test_blocking_acquire_timeout_raises(self):
        import pytest

        from django_cachex.semaphore import Semaphore, SemaphoreTimeoutError

        sem_holder = Semaphore("block_to", capacity=1)
        sem_holder.acquire(blocking=False)

        sem_waiter = Semaphore("block_to", capacity=1)
        with pytest.raises(SemaphoreTimeoutError):
            sem_waiter.acquire(blocking=True, timeout=0.1)

        sem_holder.release()
