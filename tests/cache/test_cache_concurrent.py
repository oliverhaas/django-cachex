"""Tests for concurrent/thread-safe cache operations.

Verifies that cache operations produce correct results under concurrent
multi-threaded access. Relevant for Python 3.14t (free-threaded, no GIL).
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django_cachex.cache import KeyValueCache

NUM_THREADS = 10
KEYS_PER_THREAD = 20
INCRS_PER_THREAD = 50


def _run_threads(fn, *, num_threads=NUM_THREADS):
    """Run fn(thread_index, barrier) in NUM_THREADS threads, return results."""
    barrier = threading.Barrier(num_threads)
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(fn, t, barrier) for t in range(num_threads)]
        return [f.result() for f in futures]


class TestConcurrentReadWrite:
    def test_concurrent_unique_key_writes(self, cache: KeyValueCache):
        def worker(t, barrier):
            barrier.wait()
            for i in range(KEYS_PER_THREAD):
                cache.set(f"conc_rw_{t}_{i}", f"val_{t}_{i}")

        _run_threads(worker)

        for t in range(NUM_THREADS):
            for i in range(KEYS_PER_THREAD):
                assert cache.get(f"conc_rw_{t}_{i}") == f"val_{t}_{i}"

    def test_concurrent_overwrite_converges(self, cache: KeyValueCache):
        valid_values = {f"thread_{t}" for t in range(NUM_THREADS)}

        def worker(t, barrier):
            barrier.wait()
            cache.set("conc_ow", f"thread_{t}")

        _run_threads(worker)

        result = cache.get("conc_ow")
        assert result in valid_values


class TestConcurrentAtomicOps:
    def test_concurrent_incr(self, cache: KeyValueCache):
        cache.set("conc_incr", 0)

        def worker(t, barrier):
            barrier.wait()
            for _ in range(INCRS_PER_THREAD):
                cache.incr("conc_incr")

        _run_threads(worker)

        assert cache.get("conc_incr") == NUM_THREADS * INCRS_PER_THREAD

    def test_concurrent_incr_decr_net_zero(self, cache: KeyValueCache):
        cache.set("conc_net", 0)
        half = NUM_THREADS // 2

        def worker(t, barrier):
            barrier.wait()
            for _ in range(INCRS_PER_THREAD):
                if t < half:
                    cache.incr("conc_net")
                else:
                    cache.decr("conc_net")

        _run_threads(worker)

        assert cache.get("conc_net") == 0


class TestConcurrentBulkOps:
    def test_concurrent_set_many_get_many(self, cache: KeyValueCache):
        keys_per_thread = 10

        def worker(t, barrier):
            barrier.wait()
            data = {f"conc_bulk_{t}_{i}": t * 100 + i for i in range(keys_per_thread)}
            cache.set_many(data)

        _run_threads(worker)

        all_keys = [f"conc_bulk_{t}_{i}" for t in range(NUM_THREADS) for i in range(keys_per_thread)]
        result = cache.get_many(all_keys)
        assert len(result) == NUM_THREADS * keys_per_thread
        for t in range(NUM_THREADS):
            for i in range(keys_per_thread):
                assert result[f"conc_bulk_{t}_{i}"] == t * 100 + i


class TestConcurrentPipeline:
    def test_concurrent_pipeline_isolation(self, cache: KeyValueCache):
        def worker(t, barrier):
            barrier.wait()
            with cache.pipeline() as pipe:
                pipe.set(f"conc_pipe_{t}", f"val_{t}")
                pipe.get(f"conc_pipe_{t}")
                return pipe.execute()

        all_results = _run_threads(worker)

        for t, results in enumerate(all_results):
            assert results[0] is True
            assert results[1] == f"val_{t}"

    def test_concurrent_pipeline_incr(self, cache: KeyValueCache):
        cache.set("conc_pipe_incr", 0)
        incrs_per_pipe = 10

        def worker(t, barrier):
            barrier.wait()
            with cache.pipeline() as pipe:
                for _ in range(incrs_per_pipe):
                    pipe.incr("conc_pipe_incr")
                pipe.execute()

        _run_threads(worker)

        assert cache.get("conc_pipe_incr") == NUM_THREADS * incrs_per_pipe


class TestConcurrentPoolInit:
    def test_concurrent_get_client_shares_pool(
        self,
        cache: KeyValueCache,
        client_class: str,
        sentinel_mode: str | bool,
    ):
        def worker(t, barrier):
            barrier.wait()
            return cache.get_client(write=True)

        clients = _run_threads(worker)

        if client_class == "cluster" and not sentinel_mode:
            # Cluster reuses the same cluster instance
            ids = {id(c) for c in clients}
            assert len(ids) == 1
        else:
            # Default/sentinel: each get_client() returns a new client object
            # wrapping a shared connection pool
            pool_ids = {id(c.connection_pool) for c in clients}
            assert len(pool_ids) == 1


class TestConcurrentAsyncIsolation:
    def test_multi_event_loop_isolation(self, cache: KeyValueCache):
        num_loops = 2

        async def coro(t):
            await cache.aset(f"conc_async_{t}", f"async_val_{t}")
            return await cache.aget(f"conc_async_{t}")

        def worker(t, barrier):
            barrier.wait()
            return asyncio.run(coro(t))

        results = _run_threads(worker, num_threads=num_loops)

        for t in range(num_loops):
            assert results[t] == f"async_val_{t}"
            assert cache.get(f"conc_async_{t}") == f"async_val_{t}"


class TestConcurrentGetOrSet:
    def test_concurrent_get_or_set_same_key(self, cache: KeyValueCache):
        cache.delete("conc_gos")

        def worker(t, barrier):
            barrier.wait()
            return cache.get_or_set("conc_gos", lambda: f"by_{t}", timeout=300)

        results = _run_threads(worker)

        # All threads must converge on the same value
        assert len(set(results)) == 1
        assert cache.get("conc_gos") == results[0]
