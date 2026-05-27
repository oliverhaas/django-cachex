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


class TestLocalAsyncSemaphore:
    def test_async_acquire_within_capacity(self):
        import asyncio

        from django_cachex.semaphore import AsyncSemaphore

        async def run():
            sem = AsyncSemaphore("async", capacity=2)
            assert await sem.acquire(blocking=False) is True
            assert await sem.acquire(blocking=False) is True
            assert await sem.acquire(blocking=False) is False
            await sem.release()
            assert await sem.acquire(blocking=False) is True

        asyncio.run(run())

    def test_async_blocking_waits_for_release(self):
        import asyncio

        from django_cachex.semaphore import AsyncSemaphore

        async def run():
            holder = AsyncSemaphore("async_block", capacity=1)
            await holder.acquire(blocking=False)

            waiter = AsyncSemaphore("async_block", capacity=1)

            async def release_soon():
                await asyncio.sleep(0.05)
                await holder.release()

            async def wait():
                return await waiter.acquire(blocking=True, timeout=2)

            results = await asyncio.gather(release_soon(), wait())
            assert results[1] is True
            await waiter.release()

        asyncio.run(run())

    def test_async_blocking_timeout_raises(self):
        import asyncio

        import pytest

        from django_cachex.semaphore import AsyncSemaphore, SemaphoreTimeoutError

        async def run():
            holder = AsyncSemaphore("async_to", capacity=1)
            await holder.acquire(blocking=False)

            waiter = AsyncSemaphore("async_to", capacity=1)
            with pytest.raises(SemaphoreTimeoutError):
                await waiter.acquire(blocking=True, timeout=0.1)

            await holder.release()

        asyncio.run(run())


class TestLocalCrossContext:
    def test_sync_release_wakes_async_waiter_on_other_thread(self):
        import asyncio
        import threading
        import time

        from django_cachex.semaphore import AsyncSemaphore, Semaphore

        # Capacity 1, sync holder on the main thread.
        holder = Semaphore("xthread", capacity=1)
        holder.acquire(blocking=False)

        wake_result: dict[str, bool] = {}

        def async_waiter_thread() -> None:
            async def run() -> None:
                waiter = AsyncSemaphore("xthread", capacity=1)
                ok = await waiter.acquire(blocking=True, timeout=2)
                wake_result["ok"] = ok
                await waiter.release()

            asyncio.run(run())

        t = threading.Thread(target=async_waiter_thread)
        t.start()
        time.sleep(0.1)  # let the async waiter enqueue
        holder.release()
        t.join(timeout=3)

        assert wake_result == {"ok": True}


class TestLocalCapacityChange:
    def test_capacity_mismatch_warns_and_updates(self):
        import warnings

        from django_cachex.semaphore import Semaphore

        # Use a unique name so this test doesn't interfere with the shared
        # process-wide registry between test runs / other tests.
        Semaphore("capchange_a", capacity=10)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sem_b = Semaphore("capchange_a", capacity=20)

        assert len(w) == 1
        assert issubclass(w[0].category, RuntimeWarning)
        assert "capacity" in str(w[0].message).lower()
        # New capacity took effect.
        assert sem_b._state.capacity == 20

    def test_capacity_unchanged_no_warning(self):
        import warnings

        from django_cachex.semaphore import Semaphore

        Semaphore("capchange_b", capacity=5)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Semaphore("capchange_b", capacity=5)

        assert len(w) == 0


class TestLocMemSemaphoreIntegration:
    def test_locmem_semaphore_via_cache_factory(self):
        from django_cachex.cache import LocMemCache
        from django_cachex.semaphore import Semaphore

        cache = LocMemCache("test-loc-factory", {})
        sem = cache.semaphore("img", capacity=3, weight=1)
        assert isinstance(sem, Semaphore)
        with sem:
            assert sem._held is True
        assert sem._held is False

    def test_locmem_semaphore_scoped_per_cache_instance(self):
        """Two LocMemCache instances do not share semaphore state."""
        from django_cachex.cache import LocMemCache

        cache_a = LocMemCache("loc-a", {})
        cache_b = LocMemCache("loc-b", {})

        sem_a = cache_a.semaphore("shared_name", capacity=1)
        sem_b = cache_b.semaphore("shared_name", capacity=1)

        assert sem_a.acquire(blocking=False) is True
        # Same name on a different cache instance should NOT see used budget.
        assert sem_b.acquire(blocking=False) is True

        sem_a.release()
        sem_b.release()

    def test_locmem_asemaphore_via_cache_factory(self):
        import asyncio

        from django_cachex.cache import LocMemCache
        from django_cachex.semaphore import AsyncSemaphore

        cache = LocMemCache("test-aloc-factory", {})

        async def run() -> None:
            sem = await cache.asemaphore("img", capacity=2)
            assert isinstance(sem, AsyncSemaphore)
            async with sem:
                assert sem._held is True
            assert sem._held is False

        asyncio.run(run())
