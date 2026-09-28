"""Tests for lock operations."""

import copy
import pickle
import threading
from typing import TYPE_CHECKING

import pytest

from django_cachex.exceptions import CachexError, NotSupportedError
from django_cachex.lock import LockError, LockNotOwnedError

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


@pytest.fixture(autouse=True)
def _skip_cluster_lock_tests(request: pytest.FixtureRequest) -> None:
    """Skip cluster from existing lock tests.

    Cluster mode rejects ``lock``/``alock`` outright (the driver locks are
    not cluster-aware; see ``RespClusterCache.lock``). A test in this module
    opts back in by requesting ``_cluster_supported``, as the cluster-rejection
    tests do.
    """
    if "_cluster_supported" in request.fixturenames:
        return
    if "cache" not in request.fixturenames:
        return
    client_class = request.getfixturevalue("client_class")
    sentinel_mode = request.getfixturevalue("sentinel_mode")
    if client_class == "cluster" and not sentinel_mode:
        pytest.skip("RespClusterCache rejects lock/alock; see test_cluster_lock_raises_not_supported")


@pytest.fixture
def _cluster_supported() -> None:
    """Keep ``_skip_cluster_lock_tests`` from skipping the cluster topology."""


def test_lock_error_hierarchy():
    assert issubclass(LockError, CachexError)
    assert issubclass(LockError, ValueError)
    assert issubclass(LockNotOwnedError, LockError)


def test_acquire_and_release_blocking_lock(cache: RespCache):
    resource_lock = cache.lock("resource_a")
    assert resource_lock.acquire(blocking=True) is True
    assert cache.has_key("resource_a") is True
    resource_lock.release()
    assert cache.has_key("resource_a") is False


def test_nonblocking_lock_prevents_double_acquire(cache: RespCache):
    first_lock = cache.lock("resource_b")
    assert first_lock.acquire(blocking=False) is True

    second_lock = cache.lock("resource_b")
    assert second_lock.acquire(blocking=False) is False

    assert cache.has_key("resource_b") is True
    first_lock.release()
    assert cache.has_key("resource_b") is False


def test_lock_context_manager(cache: RespCache):
    lock = cache.lock("ctx_resource", lease=5)
    with lock:
        assert cache.has_key("ctx_resource") is True
    assert cache.has_key("ctx_resource") is False


def test_lock_acquire_rejects_old_blocking_timeout(cache: RespCache, resp_adapter: str):
    """blocking_timeout was renamed to timeout; the old name must error.

    Only enforced for adapters that wrap the returned Lock in our own
    class (valkey-glide). The redis-py / valkey-py adapters
    return the upstream library's Lock object directly, which still
    accepts its native ``blocking_timeout`` kwarg.
    """
    if resp_adapter in {"redis-py", "valkey-py"}:
        pytest.skip("Passthrough adapter exposes upstream library Lock with native names")
    with pytest.raises(TypeError, match="unexpected keyword argument 'blocking_timeout'"):
        cache.lock("rename_check").acquire(blocking_timeout=1)


def test_extend_increases_ttl(cache: RespCache):
    lock = cache.lock("extend_resource", lease=2)
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


# Below 1 ms the drivers send PX 0 (glide a bare PX) and the server rejects every acquire.
@pytest.mark.parametrize("lease", [0, 0.0005, -1])
def test_sub_millisecond_lease_is_rejected(cache: RespCache, lease: float):
    with pytest.raises(ValueError, match="lease must be at least 1 ms"):
        cache.lock("tiny_lease", lease=lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("lease", [0, 0.0005, -1])
async def test_async_sub_millisecond_lease_is_rejected(cache: RespCache, lease: float):
    with pytest.raises(ValueError, match="lease must be at least 1 ms"):
        await cache.alock("tiny_lease", lease=lease)


def test_one_millisecond_lease_is_accepted(cache: RespCache):
    lock = cache.lock("ms_lease", lease=0.001)
    assert lock.acquire(blocking=False) is True


def test_double_release_raises(cache: RespCache):
    lock = cache.lock("dbl_release_resource", lease=5)
    lock.acquire()
    lock.release()
    with pytest.raises(LockError):
        lock.release()


# redis-py and valkey-py locks raise their own LockError; the adapter translates it so app code need not import them.
def test_release_unacquired_raises_lock_error(cache: RespCache):
    lock = cache.lock("unacquired_release", lease=5)
    with pytest.raises(LockError) as excinfo:
        lock.release()
    assert not isinstance(excinfo.value, LockNotOwnedError)


def test_release_after_expiry_raises_not_owned(cache: RespCache):
    lock = cache.lock("expired_release", lease=30)
    assert lock.acquire(blocking=False) is True
    cache.delete("expired_release")
    with pytest.raises(LockNotOwnedError):
        lock.release()


def test_extend_after_expiry_raises_not_owned(cache: RespCache):
    lock = cache.lock("expired_extend", lease=30)
    assert lock.acquire(blocking=False) is True
    cache.delete("expired_extend")
    with pytest.raises(LockNotOwnedError):
        lock.extend(10)


def test_extend_without_lease_raises_lock_error(cache: RespCache):
    lock = cache.lock("leaseless_extend")
    assert lock.acquire(blocking=False) is True
    try:
        with pytest.raises(LockError):
            lock.extend(10)
    finally:
        lock.release()


def test_context_manager_raises_lock_error_when_held(cache: RespCache):
    holder = cache.lock("held_ctx", lease=30)
    assert holder.acquire(blocking=False) is True
    try:
        with pytest.raises(LockError), cache.lock("held_ctx", lease=30, blocking=False):
            pass
    finally:
        holder.release()


def test_translated_error_keeps_the_driver_error_as_cause(cache: RespCache, resp_adapter: str):
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide's lock raises the cachex classes directly")
    lock = cache.lock("cause_check", lease=30)
    with pytest.raises(LockError) as excinfo:
        lock.release()
    assert excinfo.value.__cause__ is not None
    assert type(excinfo.value.__cause__).__name__ == "LockError"


def test_native_lock_attributes_pass_through(cache: RespCache, resp_adapter: str):
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide's lock has no driver lock underneath")
    lock = cache.lock("attr_check", lease=30, timeout=5)
    assert lock.timeout == 30
    assert lock.blocking_timeout == 5
    assert lock.acquire(blocking=False) is True
    try:
        assert lock.locked() is True
        assert lock.owned() is True
    finally:
        lock.release()


def test_native_lock_attributes_can_be_assigned(cache: RespCache, resp_adapter: str):
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide's lock has no driver lock underneath")
    lock = cache.lock("attr_assign", lease=30, timeout=5)

    lock.blocking_timeout = 1
    lock.timeout = 10

    assert lock.blocking_timeout == 1
    assert lock._lock.blocking_timeout == 1
    assert lock.acquire(blocking=False) is True
    try:
        assert 9_000 < (cache.pttl("attr_assign") or 0) <= 10_000
    finally:
        lock.release()


def test_assigning_a_method_replaces_the_wrapped_one(cache: RespCache, resp_adapter: str):
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide's lock has no driver lock underneath")
    lock = cache.lock("attr_method", lease=30)
    assert lock.locked() is False

    lock.locked = lambda: "patched"

    assert lock.locked() == "patched"


def test_wrapper_can_be_copied(cache: RespCache, resp_adapter: str):
    # Regression: __getattr__ read a slot first, so ``copy.copy`` (which
    # probes an instance built by __new__ for __setstate__) recursed forever.
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide's lock has no driver lock underneath")
    lock = cache.lock("attr_copy", lease=30)

    clone = copy.copy(lock)

    assert type(clone) is type(lock)
    assert clone._lock is lock._lock
    assert clone.timeout == 30
    with pytest.raises(LockError):
        clone.release()


def test_pickling_fails_in_the_driver_lock_not_by_recursion(cache: RespCache, resp_adapter: str):
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide's lock has no driver lock underneath")
    lock = cache.lock("attr_pickle", lease=30)
    # The driver lock holds thread locks (its client's pool), which is what pickle rejects.
    with pytest.raises(TypeError) as driver_error:
        pickle.dumps(lock._lock)

    with pytest.raises(TypeError) as wrapper_error:
        pickle.dumps(lock)

    assert str(wrapper_error.value) == str(driver_error.value)


@pytest.mark.asyncio
async def test_async_release_after_expiry_raises_not_owned(cache: RespCache):
    lock = await cache.alock("async_expired_release", lease=30)
    assert await lock.acquire(blocking=False) is True
    await cache.adelete("async_expired_release")
    with pytest.raises(LockNotOwnedError):
        await lock.release()


@pytest.mark.asyncio
async def test_async_release_unacquired_raises_lock_error(cache: RespCache):
    lock = await cache.alock("async_unacquired_release", lease=30)
    with pytest.raises(LockError):
        await lock.release()


@pytest.mark.asyncio
async def test_async_extend_after_expiry_raises_not_owned(cache: RespCache):
    lock = await cache.alock("async_expired_extend", lease=30)
    assert await lock.acquire(blocking=False) is True
    await cache.adelete("async_expired_extend")
    with pytest.raises(LockNotOwnedError):
        await lock.extend(10)


@pytest.mark.asyncio
async def test_async_context_manager_raises_lock_error_when_held(cache: RespCache):
    holder = await cache.alock("async_held_ctx", lease=30)
    assert await holder.acquire(blocking=False) is True
    try:
        with pytest.raises(LockError):
            async with await cache.alock("async_held_ctx", lease=30, blocking=False):
                pass
    finally:
        await holder.release()


def test_release_lock_from_different_thread(cache: RespCache):
    shared_lock = cache.lock("shared_resource", thread_local=False)
    assert shared_lock.acquire(blocking=True) is True

    def background_release(lock_obj):
        lock_obj.release()

    release_thread = threading.Thread(target=background_release, args=[shared_lock])
    release_thread.start()
    release_thread.join()

    assert cache.has_key("shared_resource") is False


@pytest.mark.usefixtures("_cluster_supported")
def test_cluster_lock_raises_not_supported(
    cache: RespCache,
    client_class: str,
    sentinel_mode: str | bool,
):
    if client_class != "cluster" or sentinel_mode:
        pytest.skip("rejection contract only applies to non-sentinel cluster mode")
    with pytest.raises(NotSupportedError):
        cache.lock("rejected_resource")


@pytest.mark.usefixtures("_cluster_supported")
@pytest.mark.asyncio
async def test_cluster_alock_raises_not_supported(
    cache: RespCache,
    client_class: str,
    sentinel_mode: str | bool,
):
    if client_class != "cluster" or sentinel_mode:
        pytest.skip("rejection contract only applies to non-sentinel cluster mode")
    with pytest.raises(NotSupportedError):
        await cache.alock("rejected_resource_async")


@pytest.mark.asyncio
async def test_alock_sync_with_raises_type_error(
    cache: RespCache,
    client_class: str,
    sentinel_mode: str | bool,
):
    # Regression: AsyncLock inherited Lock.__enter__, which truthy-tests
    # self.acquire(). On AsyncLock that is a coroutine and always truthy,
    # so the body ran having never issued the SET NX, holding no lock and
    # raising nothing.
    if client_class == "cluster" and not sentinel_mode:
        pytest.skip("cluster rejects alock() outright")

    # redis-py and valkey-py hand back their own async lock, which already
    # rejects sync ``with`` in its own words, so assert the contract (a
    # TypeError before the body runs) rather than one backend's wording.
    lock = await cache.alock("sync_with_guard")
    entered = False
    with pytest.raises(TypeError), lock:
        entered = True
    assert not entered
