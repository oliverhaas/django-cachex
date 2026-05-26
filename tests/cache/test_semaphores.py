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


class TestLocalFifoFairness:
    def test_big_weight_not_starved_by_small(self):
        """Big waiter at head of queue blocks smaller waiters behind it."""
        import threading
        import time

        from django_cachex.semaphore import Semaphore

        # Capacity 10, occupied by a weight-6 holder.
        holder = Semaphore("fifo", capacity=10, weight=6)
        assert holder.acquire(blocking=False) is True

        order = []
        big = Semaphore("fifo", capacity=10, weight=6)
        small = Semaphore("fifo", capacity=10, weight=2)

        def big_acquire():
            big.acquire(blocking=True, timeout=2)
            order.append("big")

        def small_acquire():
            small.acquire(blocking=True, timeout=2)
            order.append("small")

        t_big = threading.Thread(target=big_acquire)
        t_big.start()
        time.sleep(0.05)  # ensure big enqueues first
        t_small = threading.Thread(target=small_acquire)
        t_small.start()
        time.sleep(0.05)

        # Release the holder; big should win because it is at the head.
        holder.release()
        t_big.join(timeout=2)
        # Now release big; small should win.
        big.release()
        t_small.join(timeout=2)
        small.release()

        assert order == ["big", "small"]
