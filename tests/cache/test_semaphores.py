"""Tests for the semaphore primitive (local + RESP backends)."""


def test_semaphore_module_exports():
    from django_cachex.semaphore import (
        RespSemaphore,
        Semaphore,
        SemaphoreError,
        SemaphoreTimeoutError,
    )

    assert issubclass(SemaphoreTimeoutError, SemaphoreError)
    # Semaphore (local) and RespSemaphore (RESP) are distinct classes; both
    # expose paired sync/async methods.
    assert Semaphore is not RespSemaphore


class TestLocalCountingSemaphore:
    def test_acquire_within_capacity(self):
        from django_cachex.semaphore import Semaphore

        sem_a = Semaphore("counting", capacity=2)
        sem_b = Semaphore("counting", capacity=2)
        sem_c = Semaphore("counting", capacity=2)
        assert sem_a.acquire(blocking=False) is True
        assert sem_b.acquire(blocking=False) is True
        # capacity exhausted
        assert sem_c.acquire(blocking=False) is False

    def test_release_returns_capacity(self):
        from django_cachex.semaphore import Semaphore

        sem_a = Semaphore("counting2", capacity=1)
        sem_b = Semaphore("counting2", capacity=1)
        assert sem_a.acquire(blocking=False) is True
        sem_a.release()
        assert sem_b.acquire(blocking=False) is True

    def test_double_acquire_raises(self):
        """One instance holds at most one claim; second acquire raises."""
        import pytest

        from django_cachex.semaphore import Semaphore, SemaphoreError

        sem = Semaphore("nonreentrant", capacity=2)
        assert sem.acquire(blocking=False) is True
        with pytest.raises(SemaphoreError, match="already held"):
            sem.acquire(blocking=False)
        sem.release()

    def test_lease_accepted_and_ignored(self):
        """Local backend accepts lease for API parity; it has no effect."""
        import time

        from django_cachex.semaphore import Semaphore

        sem = Semaphore("lease_noop", capacity=1, lease=0.001)
        other = Semaphore("lease_noop", capacity=1)
        assert sem.acquire(blocking=False) is True
        # Wait past the would-be lease expiry; budget must still be held.
        time.sleep(0.05)
        assert other.acquire(blocking=False) is False
        sem.release()
        assert other.acquire(blocking=False) is True
        other.release()


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


class TestLocalCascadeWake:
    def test_one_release_wakes_every_fitting_waiter(self):
        """A single release that frees capacity for several queued waiters
        wakes all of them promptly via cascade, not just the head.

        Regression: a weight-3 holder releasing into three unit waiters used to
        wake only the head; the trailing waiters then sat parked until their own
        acquire timeout elapsed, at which point the retry loop happened to find
        free capacity and admitted them anyway. The boolean outcome is therefore
        identical with or without the fix, so this test asserts on the cascade
        *latency*: without the fix the trailing waiters admit only at their
        multi-second timeout; with it they admit within milliseconds.
        """
        import threading
        import time

        from django_cachex.semaphore import Semaphore, SemaphoreTimeoutError

        waiter_timeout = 5.0
        holder = Semaphore("cascade", capacity=3, weight=3)
        assert holder.acquire(blocking=False) is True

        results: dict[int, bool] = {}
        waiters = [Semaphore("cascade", capacity=3, weight=1) for _ in range(3)]

        def acquire(idx: int, sem: Semaphore) -> None:
            try:
                results[idx] = sem.acquire(blocking=True, timeout=waiter_timeout)
            except SemaphoreTimeoutError:
                results[idx] = False

        threads = [threading.Thread(target=acquire, args=(i, w)) for i, w in enumerate(waiters)]
        for t in threads:
            t.start()
        time.sleep(0.1)  # let all three enqueue before capacity frees

        # Releasing the weight-3 holder frees all three units at once.
        t0 = time.monotonic()
        holder.release()
        for t in threads:
            t.join(timeout=waiter_timeout + 1)
        elapsed = time.monotonic() - t0

        # All three fit in the freed capacity and must be admitted...
        assert results == {0: True, 1: True, 2: True}
        # ...by cascade, near-instantly. Without the fix the trailing waiters
        # admit only at their ``waiter_timeout`` expiry, so bounding the gap well
        # below that distinguishes a real cascade from a retry-on-timeout.
        assert elapsed < 1.5, f"waiters not cascaded promptly: {elapsed:.2f}s"
        for w in waiters:
            w.release()


class TestLocalAsyncSemaphore:
    """The same ``Semaphore`` class now exposes paired sync/async methods."""

    def test_aacquire_within_capacity(self):
        import asyncio

        from django_cachex.semaphore import Semaphore

        async def run():
            sem_a = Semaphore("async", capacity=2)
            sem_b = Semaphore("async", capacity=2)
            sem_c = Semaphore("async", capacity=2)
            assert await sem_a.aacquire(blocking=False) is True
            assert await sem_b.aacquire(blocking=False) is True
            assert await sem_c.aacquire(blocking=False) is False
            await sem_a.arelease()
            assert await sem_c.aacquire(blocking=False) is True

        asyncio.run(run())

    def test_aacquire_blocking_waits_for_release(self):
        import asyncio

        from django_cachex.semaphore import Semaphore

        async def run():
            holder = Semaphore("async_block", capacity=1)
            await holder.aacquire(blocking=False)

            waiter = Semaphore("async_block", capacity=1)

            async def release_soon():
                await asyncio.sleep(0.05)
                await holder.arelease()

            async def wait():
                return await waiter.aacquire(blocking=True, timeout=2)

            results = await asyncio.gather(release_soon(), wait())
            assert results[1] is True
            await waiter.arelease()

        asyncio.run(run())

    def test_aacquire_blocking_timeout_raises(self):
        import asyncio

        import pytest

        from django_cachex.semaphore import Semaphore, SemaphoreTimeoutError

        async def run():
            holder = Semaphore("async_to", capacity=1)
            await holder.aacquire(blocking=False)

            waiter = Semaphore("async_to", capacity=1)
            with pytest.raises(SemaphoreTimeoutError):
                await waiter.aacquire(blocking=True, timeout=0.1)

            await holder.arelease()

        asyncio.run(run())


class TestLocalCrossContext:
    def test_sync_release_wakes_async_waiter_on_other_thread(self):
        import asyncio
        import threading
        import time

        from django_cachex.semaphore import Semaphore

        # Capacity 1, sync holder on the main thread.
        holder = Semaphore("xthread", capacity=1)
        holder.acquire(blocking=False)

        wake_result: dict[str, bool] = {}

        def async_waiter_thread() -> None:
            async def run() -> None:
                waiter = Semaphore("xthread", capacity=1)
                ok = await waiter.aacquire(blocking=True, timeout=2)
                wake_result["ok"] = ok
                await waiter.arelease()

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

        # Distinct names per test avoid cross-test interference in the
        # default process-wide registry. Cache-attached registries already
        # isolate by instance.
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


class TestLocalAsyncCancellation:
    def test_cancelled_waiter_does_not_block_queue(self):
        """A cancelled async acquire must remove itself from the wait queue."""
        import asyncio
        import contextlib

        from django_cachex.semaphore import Semaphore

        async def run() -> None:
            holder = Semaphore("cancel_test", capacity=1)
            await holder.aacquire(blocking=False)

            # Park an async waiter, then cancel it.
            waiter = Semaphore("cancel_test", capacity=1)
            task = asyncio.create_task(waiter.aacquire(blocking=True, timeout=10))
            await asyncio.sleep(0.05)  # let it enqueue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

            # The phantom must be gone; a fresh acquire after release should succeed quickly.
            fresh = Semaphore("cancel_test", capacity=1)
            await holder.arelease()
            ok = await fresh.aacquire(blocking=True, timeout=1)
            assert ok is True
            await fresh.arelease()

        asyncio.run(run())


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
        from django_cachex.semaphore import Semaphore

        cache = LocMemCache("test-aloc-factory", {})

        async def run() -> None:
            sem = await cache.asemaphore("img", capacity=2)
            assert isinstance(sem, Semaphore)
            async with sem:
                assert sem._held is True
            assert sem._held is False

        asyncio.run(run())


class TestRespSemaphoreNonBlocking:
    def test_resp_acquire_within_capacity(self, cache):
        import contextlib

        from django_cachex.semaphore import SemaphoreError

        sem_a = cache.semaphore("resp_nb_a", capacity=2, lease=10)
        sem_b = cache.semaphore("resp_nb_a", capacity=2, lease=10)
        sem_c = cache.semaphore("resp_nb_a", capacity=2, lease=10)
        try:
            assert sem_a.acquire(blocking=False) is True
            assert sem_b.acquire(blocking=False) is True
            assert sem_c.acquire(blocking=False) is False
        finally:
            with contextlib.suppress(SemaphoreError):
                sem_a.release()
            with contextlib.suppress(SemaphoreError):
                sem_b.release()

    def test_resp_lease_required(self, cache):
        import pytest

        with pytest.raises((TypeError, ValueError)):
            cache.semaphore("resp_missing_lease", capacity=2)

    def test_resp_weight(self, cache):
        import contextlib

        from django_cachex.semaphore import SemaphoreError

        sem_a = cache.semaphore("resp_w", capacity=10, weight=7, lease=10)
        sem_b = cache.semaphore("resp_w", capacity=10, weight=4, lease=10)
        sem_c = cache.semaphore("resp_w", capacity=10, weight=3, lease=10)
        try:
            assert sem_a.acquire(blocking=False) is True
            assert sem_b.acquire(blocking=False) is False  # 7+4 > 10
            assert sem_c.acquire(blocking=False) is True  # 7+3 == 10
        finally:
            with contextlib.suppress(SemaphoreError):
                sem_a.release()
            with contextlib.suppress(SemaphoreError):
                sem_c.release()

    def test_resp_double_acquire_raises(self, cache):
        """One RespSemaphore instance is non-reentrant."""
        import pytest

        from django_cachex.semaphore import SemaphoreError

        sem = cache.semaphore("resp_nonreentrant", capacity=2, lease=10)
        assert sem.acquire(blocking=False) is True
        try:
            with pytest.raises(SemaphoreError, match="already held"):
                sem.acquire(blocking=False)
        finally:
            sem.release()


class TestRespSemaphoreBlocking:
    def test_resp_blocking_acquire_waits(self, cache):
        import threading
        import time

        holder = cache.semaphore("resp_blk", capacity=1, lease=10)
        holder.acquire(blocking=False)

        result: dict[str, object] = {}

        def waiter_thread() -> None:
            waiter = cache.semaphore("resp_blk", capacity=1, lease=10, timeout=3)
            t0 = time.monotonic()
            ok = waiter.acquire(blocking=True)
            result["ok"] = ok
            result["elapsed"] = time.monotonic() - t0
            waiter.release()

        t = threading.Thread(target=waiter_thread)
        t.start()
        time.sleep(0.3)
        holder.release()
        t.join(timeout=5)

        assert result["ok"] is True
        assert result["elapsed"] >= 0.3

    def test_resp_blocking_timeout_raises(self, cache):
        import pytest

        from django_cachex.semaphore import SemaphoreTimeoutError

        holder = cache.semaphore("resp_blk_to", capacity=1, lease=10)
        holder.acquire(blocking=False)
        try:
            waiter = cache.semaphore("resp_blk_to", capacity=1, lease=10, timeout=0.3)
            with pytest.raises(SemaphoreTimeoutError):
                waiter.acquire(blocking=True)
        finally:
            holder.release()


class TestRespLeaseReclaim:
    def test_expired_lease_is_reclaimed_on_next_acquire(self, cache):
        """A holder that exits without releasing has its budget reclaimed
        when the next acquirer hits the Lua reap loop."""
        import time

        # Acquire with a short lease, then DO NOT release (simulates crash).
        crashed_holder = cache.semaphore("resp_reclaim", capacity=1, lease=0.3)
        assert crashed_holder.acquire(blocking=False) is True

        # Wait past the lease.
        time.sleep(0.5)

        # Fresh acquirer should succeed because the expired claim is reaped.
        fresh = cache.semaphore("resp_reclaim", capacity=1, lease=10)
        assert fresh.acquire(blocking=False) is True
        fresh.release()

    def test_expired_lease_reclaimed_under_weight(self, cache):
        """Weighted budget is correctly restored on reap."""
        import contextlib
        import time

        # Capacity 10, weight-6 holder crashes; capacity should fully free up.
        crashed = cache.semaphore("resp_reclaim_w", capacity=10, weight=6, lease=0.3)
        assert crashed.acquire(blocking=False) is True

        time.sleep(0.5)

        # Two new acquirers totaling 10 should both fit now that the 6 is reclaimed.
        a = cache.semaphore("resp_reclaim_w", capacity=10, weight=6, lease=10)
        b = cache.semaphore("resp_reclaim_w", capacity=10, weight=4, lease=10)
        try:
            assert a.acquire(blocking=False) is True
            assert b.acquire(blocking=False) is True
        finally:
            with contextlib.suppress(Exception):
                a.release()
            with contextlib.suppress(Exception):
                b.release()


class TestRespExtend:
    def test_extend_increases_claim_ttl(self, cache):
        """Calling extend() bumps the claim TTL key."""
        holder = cache.semaphore("resp_extend", capacity=1, lease=2)
        holder.acquire(blocking=False)
        try:
            # The Lua script writes raw Redis keys; the semaphore name is
            # already the prefixed cache key (set by cache.semaphore()), so
            # the claim TTL key is "{<full_name>}:state:claim:<token>".
            # Use cache.adapter.pttl to bypass the cache-layer key prefixing.
            full_name = cache.make_and_validate_key("resp_extend")
            claim_key = "{" + full_name + "}:state:claim:" + holder._token
            before = cache.adapter.pttl(claim_key)
            assert holder.extend(10) is True
            after = cache.adapter.pttl(claim_key)
            assert before is not None and after is not None
            assert after > before
            assert after > 5_000  # original 2s + 10s extend, well past 5s
        finally:
            holder.release()

    def test_extend_on_unowned_returns_false(self, cache):
        """extend() raises if no token is held."""
        import pytest

        from django_cachex.semaphore import SemaphoreError

        sem = cache.semaphore("resp_extend_unowned", capacity=1, lease=10)
        with pytest.raises(SemaphoreError):
            sem.extend(5)


class TestRespAsyncSemaphore:
    """``cache.asemaphore`` returns the same ``RespSemaphore`` instance; use
    its ``aacquire``/``arelease``/``aextend`` methods from async code."""

    def test_resp_aacquire(self, cache):
        import asyncio

        async def run() -> None:
            sem = await cache.asemaphore("aresp_a", capacity=2, lease=10)
            async with sem:
                pass  # held via context manager

        asyncio.run(run())

    def test_resp_aacquire_non_blocking(self, cache):
        import asyncio
        import contextlib

        async def run() -> None:
            a = await cache.asemaphore("aresp_b", capacity=1, lease=10)
            b = await cache.asemaphore("aresp_b", capacity=1, lease=10)
            try:
                assert await a.aacquire(blocking=False) is True
                assert await b.aacquire(blocking=False) is False
            finally:
                from django_cachex.semaphore import SemaphoreError

                with contextlib.suppress(SemaphoreError):
                    await a.arelease()

        asyncio.run(run())

    def test_resp_aacquire_blocking_with_timeout(self, cache):
        import asyncio

        import pytest

        from django_cachex.semaphore import SemaphoreTimeoutError

        async def run() -> None:
            holder = await cache.asemaphore("aresp_to", capacity=1, lease=10)
            await holder.aacquire(blocking=False)
            try:
                waiter = await cache.asemaphore(
                    "aresp_to",
                    capacity=1,
                    lease=10,
                    timeout=0.3,
                )
                with pytest.raises(SemaphoreTimeoutError):
                    await waiter.aacquire(blocking=True)
            finally:
                await holder.arelease()

        asyncio.run(run())

    def test_resp_aextend(self, cache):
        import asyncio

        async def run() -> None:
            holder = await cache.asemaphore("aresp_ext", capacity=1, lease=2)
            await holder.aacquire(blocking=False)
            try:
                full_name = cache.make_and_validate_key("aresp_ext")
                claim_key = "{" + full_name + "}:state:claim:" + holder._token
                before = cache.adapter.pttl(claim_key)
                assert await holder.aextend(10) is True
                after = cache.adapter.pttl(claim_key)
                assert before is not None and after is not None
                assert after > before
                assert after > 5_000
            finally:
                await holder.arelease()

        asyncio.run(run())


def test_top_level_semaphore_exports():
    """Public names are reachable from the package root."""
    from django_cachex import (
        Semaphore,
        SemaphoreError,
        SemaphoreTimeoutError,
    )

    assert Semaphore is not None
    assert issubclass(SemaphoreTimeoutError, SemaphoreError)
