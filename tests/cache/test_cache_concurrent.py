"""Concurrent thread-safety tests for cache compound data-structure ops.

Targets the read-modify-write race in ``CachexMixin`` (#62): operations like
``lpush``/``sadd``/``hset``/``hincrby``/``zadd``/``zincrby`` are implemented as
GET-then-SET, so two concurrent calls on the same key can lose data.

The ``fast_thread_switching`` fixture lowers ``sys.setswitchinterval`` so the
interpreter switches threads far more aggressively, dramatically increasing
the chance of catching the race within the test's iteration budget.

Backends covered:

- ``LocMemCache`` (``CachexMixin``) — RED until #62 is fixed.
- Redis-backed via the existing ``cache`` fixture — uses native atomic
  Redis commands, so these tests serve as cross-backend smoke coverage.
"""

from __future__ import annotations

import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import pytest

from django_cachex.cache.locmem import LocMemCache

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from django_cachex.cache import KeyValueCache


N_THREADS = 8
OPS_PER_THREAD = 50


@pytest.fixture
def fast_thread_switching() -> Iterator[None]:
    """Lower the GIL switch interval to provoke thread interleavings.

    Default is 5ms; we drop to 1µs so the interpreter switches threads far
    more aggressively. This dramatically increases the chance of catching
    GET-then-SET races within the iteration budget. No-op on free-threaded
    Python (3.14t) — but ops still interleave naturally there.
    """
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        yield
    finally:
        sys.setswitchinterval(old)


@pytest.fixture
def locmem_cache() -> Iterator[LocMemCache]:
    """Fresh ``LocMemCache`` with a unique name (no shared storage)."""
    cache = LocMemCache(name=f"test-concurrent-{uuid.uuid4().hex}", params={})
    try:
        yield cache
    finally:
        cache.clear()


def _run_in_threads(worker: Callable[[int], None], n_threads: int = N_THREADS) -> None:
    """Run ``worker(tid)`` in ``n_threads`` threads, all released by a barrier."""
    barrier = threading.Barrier(n_threads)

    def runner(tid: int) -> None:
        barrier.wait()
        worker(tid)

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        for f in [pool.submit(runner, t) for t in range(n_threads)]:
            f.result()


# =============================================================================
# CachexMixin race tests (RED on main, GREEN once #62 is fixed)
# =============================================================================


def test_locmem_lpush_concurrent_no_data_loss(
    locmem_cache: LocMemCache,
    fast_thread_switching: None,
) -> None:
    """Every lpush'd value must survive — no GET-then-SET overwrites."""
    key = "lpush-race"

    def worker(tid: int) -> None:
        for i in range(OPS_PER_THREAD):
            locmem_cache.lpush(key, f"t{tid}-i{i}")

    _run_in_threads(worker)

    result = locmem_cache.lrange(key, 0, -1)
    assert len(result) == N_THREADS * OPS_PER_THREAD
    assert len(set(result)) == N_THREADS * OPS_PER_THREAD


def test_locmem_sadd_concurrent_no_data_loss(
    locmem_cache: LocMemCache,
    fast_thread_switching: None,
) -> None:
    """Every sadd'd member must survive."""
    key = "sadd-race"

    def worker(tid: int) -> None:
        for i in range(OPS_PER_THREAD):
            locmem_cache.sadd(key, f"t{tid}-i{i}")

    _run_in_threads(worker)

    members = locmem_cache.smembers(key)
    assert len(members) == N_THREADS * OPS_PER_THREAD


def test_locmem_hincrby_concurrent_conservation(
    locmem_cache: LocMemCache,
    fast_thread_switching: None,
) -> None:
    """Total increments on one field must equal N_THREADS * OPS_PER_THREAD."""
    key = "hincrby-race"
    field = "counter"

    def worker(tid: int) -> None:
        for _ in range(OPS_PER_THREAD):
            locmem_cache.hincrby(key, field, 1)

    _run_in_threads(worker)

    assert locmem_cache.hget(key, field) == N_THREADS * OPS_PER_THREAD


def test_locmem_zincrby_concurrent_conservation(
    locmem_cache: LocMemCache,
    fast_thread_switching: None,
) -> None:
    """Total increments on one member's score must equal N_THREADS * OPS_PER_THREAD."""
    key = "zincrby-race"
    member = "winner"

    def worker(tid: int) -> None:
        for _ in range(OPS_PER_THREAD):
            locmem_cache.zincrby(key, 1.0, member)

    _run_in_threads(worker)

    assert locmem_cache.zscore(key, member) == float(N_THREADS * OPS_PER_THREAD)


# =============================================================================
# Redis backend smoke tests — same shape on the existing `cache` fixture
# (default / cluster / sentinel / sentinel_opts x py / rust drivers).
# These exercise native server-side atomic commands and should always pass.
# =============================================================================


def test_redis_lpush_concurrent_no_data_loss(
    cache: KeyValueCache,
    fast_thread_switching: None,
) -> None:
    key = "concurrent-lpush"
    cache.delete(key)

    def worker(tid: int) -> None:
        for i in range(OPS_PER_THREAD):
            cache.lpush(key, f"t{tid}-i{i}")

    _run_in_threads(worker)

    result = cache.lrange(key, 0, -1)
    assert len(result) == N_THREADS * OPS_PER_THREAD
    assert len(set(result)) == N_THREADS * OPS_PER_THREAD


def test_redis_hincrby_concurrent_conservation(
    cache: KeyValueCache,
    fast_thread_switching: None,
) -> None:
    key = "concurrent-hincrby"
    field = "counter"
    cache.delete(key)

    def worker(tid: int) -> None:
        for _ in range(OPS_PER_THREAD):
            cache.hincrby(key, field, 1)

    _run_in_threads(worker)

    assert cache.hget(key, field) == N_THREADS * OPS_PER_THREAD
