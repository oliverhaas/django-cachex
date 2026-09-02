"""Tests for the semaphore primitive (local + RESP backends)."""

import asyncio
import contextlib
import gc
import logging
import secrets
import threading
import time
import warnings
from typing import TYPE_CHECKING, Any

import pytest

import django_cachex
from django_cachex.cache import LocMemCache
from django_cachex.cache._semaphore_lua import ACQUIRE_LUA, RELEASE_LUA
from django_cachex.semaphore import (
    RespSemaphore,
    Semaphore,
    SemaphoreError,
    SemaphoreTimeoutError,
    _decode_status,
    _SemaphoreRegistry,
    _Waiter,
)

if TYPE_CHECKING:
    from collections.abc import Callable


def test_semaphore_module_exports():
    assert issubclass(SemaphoreTimeoutError, SemaphoreError)
    assert Semaphore is not RespSemaphore


class TestLocalCountingSemaphore:
    def test_acquire_within_capacity(self):
        sem_a = Semaphore("counting", capacity=2)
        sem_b = Semaphore("counting", capacity=2)
        sem_c = Semaphore("counting", capacity=2)
        assert sem_a.acquire(blocking=False) is True
        assert sem_b.acquire(blocking=False) is True
        assert sem_c.acquire(blocking=False) is False

    def test_release_returns_capacity(self):
        sem_a = Semaphore("counting2", capacity=1)
        sem_b = Semaphore("counting2", capacity=1)
        assert sem_a.acquire(blocking=False) is True
        sem_a.release()
        assert sem_b.acquire(blocking=False) is True

    def test_double_acquire_raises(self):
        """One instance holds at most one claim; second acquire raises."""
        sem = Semaphore("nonreentrant", capacity=2)
        assert sem.acquire(blocking=False) is True
        with pytest.raises(SemaphoreError, match="already held"):
            sem.acquire(blocking=False)
        sem.release()

    def test_lease_accepted_and_ignored(self):
        """Local backend accepts lease for API parity; it has no effect."""
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
        sem_a = Semaphore("weighted", capacity=10, weight=7)
        assert sem_a.acquire(blocking=False) is True

        sem_b = Semaphore("weighted", capacity=10, weight=4)
        assert sem_b.acquire(blocking=False) is False

        sem_c = Semaphore("weighted", capacity=10, weight=3)
        # 7 + 3 == 10, exact fit.
        assert sem_c.acquire(blocking=False) is True

    def test_weight_exceeds_capacity_rejected(self):
        with pytest.raises(ValueError, match="exceeds capacity"):
            Semaphore("bad_weight", capacity=5, weight=6)


class TestLocalBlockingAcquire:
    def test_blocking_acquire_waits_for_release(self):
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
        sem_holder = Semaphore("block_to", capacity=1)
        sem_holder.acquire(blocking=False)

        sem_waiter = Semaphore("block_to", capacity=1)
        with pytest.raises(SemaphoreTimeoutError):
            sem_waiter.acquire(blocking=True, timeout=0.1)

        sem_holder.release()


class TestLocalFifoFairness:
    def test_big_weight_not_starved_by_small(self):
        """Big waiter at head of queue blocks smaller waiters behind it."""
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

        holder.release()
        t_big.join(timeout=2)
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

        t0 = time.monotonic()
        holder.release()
        for t in threads:
            t.join(timeout=waiter_timeout + 1)
        elapsed = time.monotonic() - t0

        assert results == {0: True, 1: True, 2: True}
        # Without the cascade the trailing waiters admit only at their
        # ``waiter_timeout``, well above this bound.
        assert elapsed < 1.5, f"waiters not cascaded promptly: {elapsed:.2f}s"
        for w in waiters:
            w.release()


class TestLocalAsyncSemaphore:
    """The same ``Semaphore`` class now exposes paired sync/async methods."""

    def test_aacquire_within_capacity(self):
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
        # The first instance is kept alive on purpose: state is held weakly,
        # so dropping it would reclaim the name and leave nothing to mismatch.
        sem_a = Semaphore("capchange_a", capacity=10)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sem_b = Semaphore("capchange_a", capacity=20)

        assert len(w) == 1
        assert issubclass(w[0].category, RuntimeWarning)
        assert "capacity" in str(w[0].message).lower()
        assert sem_b._state.capacity == 20
        assert sem_a._state is sem_b._state

    def test_capacity_unchanged_no_warning(self):
        sem_a = Semaphore("capchange_b", capacity=5)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sem_b = Semaphore("capchange_b", capacity=5)

        assert len(w) == 0
        assert sem_a._state is sem_b._state

    def test_capacity_warning_is_attributed_to_the_caller(self):
        # Regression: stacklevel was hardcoded for the cache.semaphore() chain,
        # so a direct Semaphore(...) blamed the frame above the real call site.

        keep_alive = Semaphore("capchange_stack", capacity=1)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Semaphore("capchange_stack", capacity=2)

        assert len(w) == 1
        assert w[0].filename == __file__
        assert keep_alive._state.capacity == 2

    def test_capacity_increase_wakes_a_parked_waiter(self):
        holder = Semaphore("capgrow", capacity=1)
        assert holder.acquire(blocking=False) is True

        result: dict[str, object] = {}

        def park() -> None:
            waiter = Semaphore("capgrow", capacity=1)
            result["ok"] = waiter.acquire(blocking=True, timeout=5)
            waiter.release()

        t = threading.Thread(target=park)
        t.start()
        time.sleep(0.1)  # let the waiter park

        started = time.monotonic()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            Semaphore("capgrow", capacity=2)
        t.join(timeout=5)
        elapsed = time.monotonic() - started

        assert result["ok"] is True
        # Without the wake the waiter admits only near its 5 s timeout.
        assert elapsed < 1.0, f"waiter not woken by the capacity bump: {elapsed:.2f}s"
        holder.release()


class TestLocalRegistryReclamation:
    def test_unreferenced_names_are_reclaimed(self):
        registry = _SemaphoreRegistry()
        for i in range(50):
            sem = Semaphore(f"job:{i}", capacity=1, _registry=registry)
            assert sem.acquire(blocking=False) is True
            sem.release()
            del sem
        gc.collect()

        assert len(registry._states) == 0

    def test_live_instances_keep_their_state(self):
        """Reclamation must never split a name's budget across instances."""
        registry = _SemaphoreRegistry()
        holder = Semaphore("kept", capacity=1, _registry=registry)
        assert holder.acquire(blocking=False) is True
        gc.collect()

        other = Semaphore("kept", capacity=1, _registry=registry)
        assert other._state is holder._state
        assert other.acquire(blocking=False) is False
        holder.release()


class TestLocalDeadWaiter:
    def test_closed_loop_waiter_does_not_wedge_release(self):
        holder = Semaphore("dead_loop", capacity=1)
        assert holder.acquire(blocking=False) is True

        loop = asyncio.new_event_loop()

        async def make_waiter() -> _Waiter:
            return _Waiter(async_=True)

        dead = loop.run_until_complete(make_waiter())
        loop.close()

        state = holder._state
        with state.lock:
            state.waiters[dead] = 1

        holder.release()  # must not raise

        assert dead not in state.waiters
        fresh = Semaphore("dead_loop", capacity=1)
        assert fresh.acquire(blocking=False) is True
        fresh.release()


class TestLocalAsyncCancellation:
    def test_cancelled_waiter_does_not_block_queue(self):
        """A cancelled async acquire must remove itself from the wait queue."""

        async def run() -> None:
            holder = Semaphore("cancel_test", capacity=1)
            await holder.aacquire(blocking=False)

            waiter = Semaphore("cancel_test", capacity=1)
            task = asyncio.create_task(waiter.aacquire(blocking=True, timeout=10))
            await asyncio.sleep(0.05)  # let it enqueue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

            fresh = Semaphore("cancel_test", capacity=1)
            await holder.arelease()
            ok = await fresh.aacquire(blocking=True, timeout=1)
            assert ok is True
            await fresh.arelease()

        asyncio.run(run())


class _RendezvousLock:
    """Stand-in for ``_LocalState.lock`` holding the first ``parties`` entries
    until all of them arrive, so every thread is provably past any check that
    sits outside the lock. A barrier in the test body cannot do this: under the
    GIL the first thread through it finishes the whole method uninterrupted."""

    def __init__(self, real: Any, parties: int = 2):
        self._real = real
        self._barrier = threading.Barrier(parties)
        self._remaining = parties
        self._guard = threading.Lock()

    def __enter__(self) -> Any:
        with self._guard:
            wait = self._remaining > 0
            self._remaining -= 1
        if wait:
            self._barrier.wait(timeout=10)
        return self._real.__enter__()

    def __exit__(self, *exc_info: object) -> Any:
        return self._real.__exit__(*exc_info)


def _race(target: Callable[[], bool], parties: int = 2) -> list[bool]:
    """Run ``target`` in ``parties`` threads, recording True for each caller
    that was let through and False for each that was rejected."""

    outcomes: list[bool] = []
    lock = threading.Lock()

    def attempt() -> None:
        try:
            result = target()
        except SemaphoreError:
            result = False
        with lock:
            outcomes.append(bool(result))

    threads = [threading.Thread(target=attempt) for _ in range(parties)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    return outcomes


class TestLocalConcurrentMisuse:
    """Sharing one instance across threads is misuse, but it must corrupt
    nothing: the shared budget stays consistent and the loser raises."""

    def test_concurrent_release_releases_once(self):
        # Regression: the held check ran outside the state lock, so racing
        # release() calls double-decremented the budget.

        sem = Semaphore("race_release", capacity=1)
        assert sem.acquire(blocking=False) is True
        sem._state.lock = _RendezvousLock(sem._state.lock)

        def release() -> bool:
            sem.release()
            return True

        outcomes = _race(release)
        assert outcomes.count(True) == 1, outcomes
        assert sem._state.used == 0

    def test_concurrent_acquire_admits_once(self):
        # Regression: the held check ran outside the state lock, so racing
        # acquire() calls both admitted and leaked budget.

        sem = Semaphore("race_acquire", capacity=2)
        sem._state.lock = _RendezvousLock(sem._state.lock)

        outcomes = _race(lambda: sem.acquire(blocking=False))
        assert outcomes.count(True) == 1, outcomes
        assert sem._state.used == 1
        sem.release()
        assert sem._state.used == 0

    def test_double_aacquire_raises(self):
        """The held check relocated into the locked loop still rejects
        re-acquiring a held instance on the async path."""

        async def run() -> None:
            sem = Semaphore("race_aacquire", capacity=2)
            assert await sem.aacquire(blocking=False) is True
            with pytest.raises(SemaphoreError, match="already held"):
                await sem.aacquire(blocking=False)
            await sem.arelease()

        asyncio.run(run())


class TestLocMemSemaphoreIntegration:
    def test_locmem_semaphore_via_cache_factory(self):
        cache = LocMemCache("test-loc-factory", {})
        sem = cache.semaphore("img", capacity=3, weight=1)
        assert isinstance(sem, Semaphore)
        with sem:
            assert sem._held is True
        assert sem._held is False

    def test_locmem_semaphore_scoped_per_cache_instance(self):
        cache_a = LocMemCache("loc-a", {})
        cache_b = LocMemCache("loc-b", {})

        sem_a = cache_a.semaphore("shared_name", capacity=1)
        sem_b = cache_b.semaphore("shared_name", capacity=1)

        assert sem_a.acquire(blocking=False) is True
        assert sem_b.acquire(blocking=False) is True

        sem_a.release()
        sem_b.release()

    def test_locmem_asemaphore_via_cache_factory(self):
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
        with pytest.raises(ValueError, match="requires a positive 'lease'"):
            cache.semaphore("resp_missing_lease", capacity=2)

    def test_resp_weight(self, cache):
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
        sem = cache.semaphore("resp_nonreentrant", capacity=2, lease=10)
        assert sem.acquire(blocking=False) is True
        try:
            with pytest.raises(SemaphoreError, match="already held"):
                sem.acquire(blocking=False)
        finally:
            sem.release()


class TestRespSemaphoreBlocking:
    def test_resp_blocking_acquire_waits(self, cache):
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

        crashed_holder = cache.semaphore("resp_reclaim", capacity=1, lease=0.3)
        assert crashed_holder.acquire(blocking=False) is True

        time.sleep(0.5)

        fresh = cache.semaphore("resp_reclaim", capacity=1, lease=10)
        assert fresh.acquire(blocking=False) is True
        fresh.release()

    def test_expired_lease_reclaimed_under_weight(self, cache):
        crashed = cache.semaphore("resp_reclaim_w", capacity=10, weight=6, lease=0.3)
        assert crashed.acquire(blocking=False) is True

        time.sleep(0.5)

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


class TestRespQueueReap:
    def test_stale_queue_entry_reaped_on_acquire(self, cache):
        # Regression: a crashed waiter's queue entry sat at the head forever.
        full_name = cache.make_and_validate_key("resp_queue_reap")
        queue_key = "{" + full_name + "}:queue"
        cache.adapter.zadd(queue_key, {b"dead-token": 1.0})

        fresh = cache.semaphore("resp_queue_reap", capacity=1, lease=10)
        assert fresh.acquire(blocking=False) is True
        fresh.release()
        assert cache.adapter.zscore(queue_key, b"dead-token") is None

    @pytest.mark.asyncio
    async def test_stale_queue_entry_reaped_on_aacquire(self, cache):
        full_name = cache.make_and_validate_key("aresp_queue_reap")
        queue_key = "{" + full_name + "}:queue"
        cache.adapter.zadd(queue_key, {b"dead-token": 1.0})

        fresh = await cache.asemaphore("aresp_queue_reap", capacity=1, lease=10)
        assert await fresh.aacquire(blocking=False) is True
        await fresh.arelease()
        assert cache.adapter.zscore(queue_key, b"dead-token") is None

    def test_live_waiter_not_reaped(self, cache):
        """A queued waiter that polls (refreshing its liveness key) keeps its
        queue position; a later non-blocking acquirer is not admitted past it."""

        holder = cache.semaphore("resp_live_waiter", capacity=1, lease=10)
        assert holder.acquire(blocking=False) is True

        result: dict[str, object] = {}
        may_release = threading.Event()

        def waiter_thread() -> None:
            waiter = cache.semaphore("resp_live_waiter", capacity=1, lease=10, timeout=5)
            result["ok"] = waiter.acquire(blocking=True)
            # Releasing here would hand the jumper a legitimately free slot
            # whenever the waiter's poll lands before the assert.
            may_release.wait(10)
            waiter.release()

        t = threading.Thread(target=waiter_thread)
        t.start()
        time.sleep(0.3)  # let the waiter enqueue and heartbeat

        jumper = cache.semaphore("resp_live_waiter", capacity=1, lease=10)
        holder.release()
        assert jumper.acquire(blocking=False) is False

        may_release.set()
        t.join(timeout=10)
        assert result["ok"] is True


class TestRespAcquireInterrupted:
    def test_interrupted_blocking_acquire_dequeues(self, cache):
        # Regression: KeyboardInterrupt between enqueue and admit leaked the
        # queue entry, blocking later acquirers on a full semaphore.

        class InterruptSecondAcquire:
            """Delegate to the real adapter, but raise on the second ACQUIRE
            poll (after the first has enqueued the token)."""

            def __init__(self, inner) -> None:
                self._inner = inner
                self._acquire_calls = 0

            def eval(self, script, numkeys, *args):
                if script == ACQUIRE_LUA:
                    self._acquire_calls += 1
                    if self._acquire_calls == 2:
                        raise KeyboardInterrupt
                return self._inner.eval(script, numkeys, *args)

        holder = cache.semaphore("resp_interrupt", capacity=1, lease=10)
        assert holder.acquire(blocking=False) is True

        waiter = cache.semaphore("resp_interrupt", capacity=1, lease=10)
        waiter._adapter = InterruptSecondAcquire(cache.adapter)
        with pytest.raises(KeyboardInterrupt):
            waiter.acquire(blocking=True, timeout=5)

        assert waiter._token is None
        full_name = cache.make_and_validate_key("resp_interrupt")
        queue_key = "{" + full_name + "}:queue"
        assert cache.adapter.zcard(queue_key) == 0

        holder.release()
        fresh = cache.semaphore("resp_interrupt", capacity=1, lease=10)
        assert fresh.acquire(blocking=False) is True
        fresh.release()


class TestRespConcurrentMisuse:
    """Sharing one RespSemaphore across threads is misuse, but it must not
    double-claim the server-side budget: the loser raises."""

    def test_concurrent_acquire_claims_once(self, monkeypatch):
        # Regression: the held check ran outside any lock, so two threads both
        # minted a token and one claim leaked.

        class StubAdapter:
            """Emulates the acquire/release Lua fast path with local accounting."""

            def __init__(self, capacity):
                self.capacity = capacity
                self.used = 0
                self.claims = {}
                self.lock = threading.Lock()

            def eval(self, script, numkeys, *args):
                if script not in (ACQUIRE_LUA, RELEASE_LUA):
                    return 0
                token = args[3]
                with self.lock:
                    if script == RELEASE_LUA:
                        self.used -= self.claims.pop(token, 0)
                        return [b"released", self.used, 0]
                    weight = int(args[4])
                    if self.used + weight <= self.capacity:
                        self.used += weight
                        self.claims[token] = weight
                        return [b"acquired", self.used, self.capacity]
                    return [b"queued", self.used, self.capacity]

        adapter = StubAdapter(capacity=2)
        sem = RespSemaphore(adapter, "resp_race", capacity=2, lease=10)

        barrier = threading.Barrier(2)
        real_token_hex = secrets.token_hex

        def barrier_token_hex(nbytes=None):
            # Both arrive only if both got past the held check; once the claim
            # is locked the second never mints and this times out.
            with contextlib.suppress(threading.BrokenBarrierError):
                barrier.wait(timeout=0.25)
            return real_token_hex(nbytes)

        monkeypatch.setattr(secrets, "token_hex", barrier_token_hex)

        outcomes = []

        def attempt():
            try:
                outcomes.append(sem.acquire(blocking=False))
            except SemaphoreError:
                outcomes.append(False)

        first = threading.Thread(target=attempt)
        second = threading.Thread(target=attempt)
        first.start()
        second.start()
        first.join()
        second.join()

        assert outcomes.count(True) == 1, outcomes
        assert adapter.used == 1
        assert len(adapter.claims) == 1
        sem.release()
        assert adapter.used == 0
        assert adapter.claims == {}

    @pytest.mark.asyncio
    async def test_double_aacquire_raises(self, cache):
        """The claim lock is shared with the async path, which still rejects
        re-acquiring a held instance."""

        sem = await cache.asemaphore("resp_race_aacquire", capacity=2, lease=10)
        assert await sem.aacquire(blocking=False) is True
        try:
            with pytest.raises(SemaphoreError, match="already held"):
                await sem.aacquire(blocking=False)
        finally:
            await sem.arelease()


class TestRespTokenSnapshot:
    """release()/extend() must act on the token they entered with."""

    def test_release_spares_a_racing_reacquire(self):
        # Regression: release() re-read self._token after its guard, so a racing
        # re-acquire's live claim was released instead.

        seen: list[str] = []

        class SwappingAdapter:
            """Re-acquires while the RELEASE call is in flight."""

            def __init__(self) -> None:
                self.sem: Any = None

            def eval(self, script, numkeys, *args):
                seen.append(args[3])
                self.sem._token = "racing-token"
                return [b"released", 0, 0]

        adapter = SwappingAdapter()
        sem = RespSemaphore(adapter, "resp_token_race", capacity=1, lease=10)
        adapter.sem = sem
        sem._token = "original-token"

        sem.release()

        assert seen == ["original-token"]
        assert sem._token == "racing-token"

    def test_extend_uses_entry_token(self):
        seen: list[str] = []

        class SwappingAdapter:
            def __init__(self) -> None:
                self.sem: Any = None

            def eval(self, script, numkeys, *args):
                seen.append(args[2])
                self.sem._token = "racing-token"
                return 1

        adapter = SwappingAdapter()
        sem = RespSemaphore(adapter, "resp_token_race_extend", capacity=1, lease=10)
        adapter.sem = sem
        sem._token = "original-token"

        assert sem.extend(1) is True
        assert seen == ["original-token"]


class TestRespEvictedState:
    def test_evicted_claims_hash_does_not_wedge(self, cache):
        # Regression: 'used' moved only by delta, so losing the claims hash to
        # eviction pinned it at capacity with nothing left to reap.
        holder = cache.semaphore("resp_evicted", capacity=1, lease=60)
        assert holder.acquire(blocking=False) is True

        full_name = cache.make_and_validate_key("resp_evicted")
        prefix = "{" + full_name + "}"
        cache.adapter.delete(f"{prefix}:claims")
        cache.adapter.delete(f"{prefix}:state:claim:{holder._token}")

        fresh = cache.semaphore("resp_evicted", capacity=1, lease=60)
        assert fresh.acquire(blocking=False) is True
        fresh.release()

    def test_evicted_state_hash_does_not_over_admit(self, cache):
        # Mirror case: losing the counter reads 'used' as 0, admitting past
        # capacity until the live claims are summed back up.
        holder = cache.semaphore("resp_evicted_state", capacity=1, lease=60)
        assert holder.acquire(blocking=False) is True

        full_name = cache.make_and_validate_key("resp_evicted_state")
        cache.adapter.delete("{" + full_name + "}:state")

        fresh = cache.semaphore("resp_evicted_state", capacity=1, lease=60)
        assert fresh.acquire(blocking=False) is False
        holder.release()

    def test_release_after_state_eviction_does_not_over_admit(self, cache):
        # Regression: RELEASE resurrected 'used' as 0 from a missing state hash.
        # ACQUIRE only re-derives when the counter is absent, so it trusted it.
        a = cache.semaphore("resp_release_evicted", capacity=500, weight=200, lease=60)
        b = cache.semaphore("resp_release_evicted", capacity=500, weight=200, lease=60)
        assert a.acquire(blocking=False) is True
        assert b.acquire(blocking=False) is True

        full_name = cache.make_and_validate_key("resp_release_evicted")
        cache.adapter.delete("{" + full_name + "}:state")

        a.release()

        c = cache.semaphore("resp_release_evicted", capacity=500, weight=500, lease=60)
        assert c.acquire(blocking=False) is False
        d = cache.semaphore("resp_release_evicted", capacity=500, weight=300, lease=60)
        assert d.acquire(blocking=False) is True
        d.release()
        b.release()

    def test_last_release_drops_the_state_hash(self, cache):
        full_name = cache.make_and_validate_key("resp_state_gc")
        prefix = "{" + full_name + "}"

        sem = cache.semaphore("resp_state_gc", capacity=1, lease=60)
        assert sem.acquire(blocking=False) is True
        assert cache.adapter.hlen(f"{prefix}:state") > 0

        sem.release()

        assert cache.adapter.hlen(f"{prefix}:state") == 0
        assert cache.adapter.hlen(f"{prefix}:claims") == 0

    def test_state_hash_survives_while_another_claim_is_live(self, cache):
        """The drop is last-holder-out only, never mid-flight."""
        full_name = cache.make_and_validate_key("resp_state_gc_partial")
        state_key = "{" + full_name + "}:state"

        a = cache.semaphore("resp_state_gc_partial", capacity=2, lease=60)
        b = cache.semaphore("resp_state_gc_partial", capacity=2, lease=60)
        assert a.acquire(blocking=False) is True
        assert b.acquire(blocking=False) is True

        a.release()
        assert cache.adapter.hlen(state_key) > 0

        b.release()
        assert cache.adapter.hlen(state_key) == 0


class TestRespGuardTtl:
    """The shared keys expire on their own when a holder dies without releasing."""

    def test_shared_keys_carry_a_guard_ttl(self, cache):
        prefix = "{" + cache.make_and_validate_key("resp_guard_ttl") + "}"
        sem = cache.semaphore("resp_guard_ttl", capacity=2, lease=60)
        assert sem.acquire(blocking=False) is True
        try:
            for key in (f"{prefix}:state", f"{prefix}:claims"):
                pttl = cache.adapter.pttl(key)
                assert pttl is not None
                assert 60_000 < pttl <= 120_000, key
        finally:
            sem.release()

    def test_guard_ttl_is_never_shortened_by_a_briefer_lease(self, cache):
        prefix = "{" + cache.make_and_validate_key("resp_guard_min") + "}"
        long_holder = cache.semaphore("resp_guard_min", capacity=2, lease=600)
        short_holder = cache.semaphore("resp_guard_min", capacity=2, lease=5)
        assert long_holder.acquire(blocking=False) is True
        assert short_holder.acquire(blocking=False) is True
        try:
            # The guard has to outlive the longest live claim.
            assert cache.adapter.pttl(f"{prefix}:state") > 600_000
            assert cache.adapter.pttl(f"{prefix}:claims") > 600_000
        finally:
            short_holder.release()
            long_holder.release()

    def test_extend_pushes_the_guard_ttl_out(self, cache):
        prefix = "{" + cache.make_and_validate_key("resp_guard_extend") + "}"
        sem = cache.semaphore("resp_guard_extend", capacity=1, lease=10)
        assert sem.acquire(blocking=False) is True
        try:
            assert cache.adapter.pttl(f"{prefix}:state") <= 20_000
            assert sem.extend(600) is True
            assert cache.adapter.pttl(f"{prefix}:state") > 600_000
            assert cache.adapter.pttl(f"{prefix}:claims") > 600_000
        finally:
            sem.release()

    def test_partial_release_keeps_the_guard_ttl(self, cache):
        prefix = "{" + cache.make_and_validate_key("resp_guard_release") + "}"
        a = cache.semaphore("resp_guard_release", capacity=2, lease=60)
        b = cache.semaphore("resp_guard_release", capacity=2, lease=60)
        assert a.acquire(blocking=False) is True
        assert b.acquire(blocking=False) is True
        a.release()
        try:
            assert cache.adapter.pttl(f"{prefix}:state") > 60_000
            assert cache.adapter.pttl(f"{prefix}:claims") > 60_000
        finally:
            b.release()


class TestRespQueueScore:
    def test_queue_score_comes_from_the_server_clock(self, cache):
        server_now = "local t = redis.call('TIME') return tostring(t[1] * 1000 + t[2] / 1000)"

        def server_ms(key: str) -> float:
            raw = cache.adapter.eval(server_now, 1, key)
            return float(raw.decode() if isinstance(raw, bytes) else raw)

        holder = cache.semaphore("resp_qscore", capacity=1, lease=60)
        assert holder.acquire(blocking=False) is True
        try:
            full_name = cache.make_and_validate_key("resp_qscore")
            prefix = "{" + full_name + "}"
            queue_key = f"{prefix}:queue"

            before = server_ms(queue_key)
            result = cache.adapter.eval(
                ACQUIRE_LUA,
                3,
                f"{prefix}:state",
                f"{prefix}:claims",
                queue_key,
                "probe-token",
                "1",
                "1",
                "60000",
                "5000",
            )
            after = server_ms(queue_key)
            assert _decode_status(result) == "queued"

            score = cache.adapter.zscore(queue_key, b"probe-token")
            assert score is not None
            # Bracketed by two reads of the SERVER clock taken milliseconds
            # apart; nothing the caller passes could land in that window.
            assert before <= float(score) <= after
        finally:
            holder.release()


class TestRespReleaseNotOwned:
    def test_release_reports_an_expired_lease(self, caplog):
        # 'not_owned' means the claim was already reaped, so the work ran past
        # its lease unprotected. That used to be discarded silently.

        class NotOwnedAdapter:
            def eval(self, script, numkeys, *args):
                return [b"not_owned", 0]

        sem = RespSemaphore(NotOwnedAdapter(), "resp_not_owned", capacity=1, lease=5)
        sem._token = "reaped-token"

        with caplog.at_level(logging.WARNING, logger="django_cachex.semaphore"):
            sem.release()

        assert "release found no claim" in caplog.text
        assert sem._token is None

    def test_release_is_quiet_on_success(self, cache, caplog):
        sem = cache.semaphore("resp_quiet_release", capacity=1, lease=60)
        assert sem.acquire(blocking=False) is True
        with caplog.at_level(logging.WARNING, logger="django_cachex.semaphore"):
            sem.release()
        assert caplog.text == ""


class TestRespExtend:
    def test_extend_increases_claim_ttl(self, cache):
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

    def test_extend_without_a_claim_raises(self, cache):
        sem = cache.semaphore("resp_extend_unowned", capacity=1, lease=10)
        with pytest.raises(SemaphoreError):
            sem.extend(5)


class TestRespAsyncSemaphore:
    """``cache.asemaphore`` returns the same ``RespSemaphore`` instance; use
    its ``aacquire``/``arelease``/``aextend`` methods from async code."""

    @pytest.mark.asyncio
    async def test_resp_aacquire(self, cache):
        sem = await cache.asemaphore("aresp_a", capacity=2, lease=10)
        async with sem:
            pass  # held via context manager

    @pytest.mark.asyncio
    async def test_resp_aacquire_non_blocking(self, cache):
        a = await cache.asemaphore("aresp_b", capacity=1, lease=10)
        b = await cache.asemaphore("aresp_b", capacity=1, lease=10)
        try:
            assert await a.aacquire(blocking=False) is True
            assert await b.aacquire(blocking=False) is False
        finally:
            with contextlib.suppress(SemaphoreError):
                await a.arelease()

    @pytest.mark.asyncio
    async def test_resp_aacquire_blocking_with_timeout(self, cache):
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

    @pytest.mark.asyncio
    async def test_resp_aextend(self, cache):
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


def test_top_level_semaphore_exports():
    """Public names are reachable from the package root."""
    assert django_cachex.Semaphore is Semaphore
    assert django_cachex.SemaphoreError is SemaphoreError
    assert django_cachex.SemaphoreTimeoutError is SemaphoreTimeoutError
