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
    not cluster-aware; see ``RespClusterCache.lock``). Each test class in this
    module opts out by setting ``cluster_supported = False`` (the default).
    The cluster-rejection contract itself is verified in
    :class:`TestClusterLockRejection`, which sets ``cluster_supported = True``.
    """
    cls = request.cls
    if cls is not None and getattr(cls, "cluster_supported", False):
        return
    if "cache" not in request.fixturenames:
        return
    client_class = request.getfixturevalue("client_class")
    sentinel_mode = request.getfixturevalue("sentinel_mode")
    if client_class == "cluster" and not sentinel_mode:
        pytest.skip("RespClusterCache rejects lock/alock; see TestClusterLockRejection")


def test_lock_error_hierarchy():
    assert issubclass(LockError, CachexError)
    assert issubclass(LockError, ValueError)
    assert issubclass(LockNotOwnedError, LockError)


class TestBasicLockOperations:
    def test_acquire_and_release_blocking_lock(self, cache: RespCache):
        resource_lock = cache.lock("resource_a")
        assert resource_lock.acquire(blocking=True) is True
        assert cache.has_key("resource_a") is True
        resource_lock.release()
        assert cache.has_key("resource_a") is False

    def test_nonblocking_lock_prevents_double_acquire(self, cache: RespCache):
        first_lock = cache.lock("resource_b")
        assert first_lock.acquire(blocking=False) is True

        second_lock = cache.lock("resource_b")
        assert second_lock.acquire(blocking=False) is False

        assert cache.has_key("resource_b") is True
        first_lock.release()
        assert cache.has_key("resource_b") is False

    def test_lock_context_manager(self, cache: RespCache):
        lock = cache.lock("ctx_resource", lease=5)
        with lock:
            assert cache.has_key("ctx_resource") is True
        assert cache.has_key("ctx_resource") is False

    def test_lock_acquire_rejects_old_blocking_timeout(self, cache: RespCache, resp_adapter: str):
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


class TestLockExtend:
    def test_extend_increases_ttl(self, cache: RespCache):
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


class TestLockRelease:
    def test_double_release_raises(self, cache: RespCache):
        lock = cache.lock("dbl_release_resource", lease=5)
        lock.acquire()
        lock.release()
        with pytest.raises(LockError):
            lock.release()


class TestLockErrors:
    """Every adapter raises ``django_cachex.lock.LockError`` and ``LockNotOwnedError``.

    redis-py and valkey-py hand back their driver's own ``Lock``, whose
    errors are ``redis.exceptions.LockError`` / ``valkey.exceptions.LockError``.
    The adapter translates them so callers have one importable name that
    does not pin app code to the driver.
    """

    def test_release_unacquired_raises_lock_error(self, cache: RespCache):
        lock = cache.lock("unacquired_release", lease=5)
        with pytest.raises(LockError) as excinfo:
            lock.release()
        assert not isinstance(excinfo.value, LockNotOwnedError)

    def test_release_after_expiry_raises_not_owned(self, cache: RespCache):
        lock = cache.lock("expired_release", lease=30)
        assert lock.acquire(blocking=False) is True
        cache.delete("expired_release")
        with pytest.raises(LockNotOwnedError):
            lock.release()

    def test_extend_after_expiry_raises_not_owned(self, cache: RespCache):
        lock = cache.lock("expired_extend", lease=30)
        assert lock.acquire(blocking=False) is True
        cache.delete("expired_extend")
        with pytest.raises(LockNotOwnedError):
            lock.extend(10)

    def test_extend_without_lease_raises_lock_error(self, cache: RespCache):
        lock = cache.lock("leaseless_extend")
        assert lock.acquire(blocking=False) is True
        try:
            with pytest.raises(LockError):
                lock.extend(10)
        finally:
            lock.release()

    def test_context_manager_raises_lock_error_when_held(self, cache: RespCache):
        holder = cache.lock("held_ctx", lease=30)
        assert holder.acquire(blocking=False) is True
        try:
            with pytest.raises(LockError), cache.lock("held_ctx", lease=30, blocking=False):
                pass
        finally:
            holder.release()

    def test_translated_error_keeps_the_driver_error_as_cause(self, cache: RespCache, resp_adapter: str):
        if resp_adapter == "valkey-glide":
            pytest.skip("valkey-glide's lock raises the cachex classes directly")
        lock = cache.lock("cause_check", lease=30)
        with pytest.raises(LockError) as excinfo:
            lock.release()
        assert excinfo.value.__cause__ is not None
        assert type(excinfo.value.__cause__).__name__ == "LockError"

    def test_native_lock_attributes_pass_through(self, cache: RespCache, resp_adapter: str):
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

    def test_native_lock_attributes_can_be_assigned(self, cache: RespCache, resp_adapter: str):
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

    def test_assigning_a_method_replaces_the_wrapped_one(self, cache: RespCache, resp_adapter: str):
        if resp_adapter == "valkey-glide":
            pytest.skip("valkey-glide's lock has no driver lock underneath")
        lock = cache.lock("attr_method", lease=30)
        assert lock.locked() is False

        lock.locked = lambda: "patched"

        assert lock.locked() == "patched"

    def test_wrapper_can_be_copied(self, cache: RespCache, resp_adapter: str):
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

    def test_pickling_fails_in_the_driver_lock_not_by_recursion(self, cache: RespCache, resp_adapter: str):
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
    async def test_async_release_after_expiry_raises_not_owned(self, cache: RespCache):
        lock = await cache.alock("async_expired_release", lease=30)
        assert await lock.acquire(blocking=False) is True
        await cache.adelete("async_expired_release")
        with pytest.raises(LockNotOwnedError):
            await lock.release()

    @pytest.mark.asyncio
    async def test_async_release_unacquired_raises_lock_error(self, cache: RespCache):
        lock = await cache.alock("async_unacquired_release", lease=30)
        with pytest.raises(LockError):
            await lock.release()

    @pytest.mark.asyncio
    async def test_async_extend_after_expiry_raises_not_owned(self, cache: RespCache):
        lock = await cache.alock("async_expired_extend", lease=30)
        assert await lock.acquire(blocking=False) is True
        await cache.adelete("async_expired_extend")
        with pytest.raises(LockNotOwnedError):
            await lock.extend(10)

    @pytest.mark.asyncio
    async def test_async_context_manager_raises_lock_error_when_held(self, cache: RespCache):
        holder = await cache.alock("async_held_ctx", lease=30)
        assert await holder.acquire(blocking=False) is True
        try:
            with pytest.raises(LockError):
                async with await cache.alock("async_held_ctx", lease=30, blocking=False):
                    pass
        finally:
            await holder.release()


class TestCrossThreadLockRelease:
    def test_release_lock_from_different_thread(self, cache: RespCache):
        shared_lock = cache.lock("shared_resource", thread_local=False)
        assert shared_lock.acquire(blocking=True) is True

        def background_release(lock_obj):
            lock_obj.release()

        release_thread = threading.Thread(target=background_release, args=[shared_lock])
        release_thread.start()
        release_thread.join()

        assert cache.has_key("shared_resource") is False


class TestClusterLockRejection:
    """``RespClusterCache.lock``/``alock`` rejects cluster mode up front."""

    cluster_supported = True

    def test_cluster_lock_raises_not_supported(
        self,
        cache: RespCache,
        client_class: str,
        sentinel_mode: str | bool,
    ):
        if client_class != "cluster" or sentinel_mode:
            pytest.skip("rejection contract only applies to non-sentinel cluster mode")
        with pytest.raises(NotSupportedError):
            cache.lock("rejected_resource")

    @pytest.mark.asyncio
    async def test_cluster_alock_raises_not_supported(
        self,
        cache: RespCache,
        client_class: str,
        sentinel_mode: str | bool,
    ):
        if client_class != "cluster" or sentinel_mode:
            pytest.skip("rejection contract only applies to non-sentinel cluster mode")
        with pytest.raises(NotSupportedError):
            await cache.alock("rejected_resource_async")


class TestAsyncLockSyncContextManager:
    """AsyncLock must reject sync ``with`` rather than silently not locking."""

    @pytest.mark.asyncio
    async def test_sync_with_raises_type_error(
        self,
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
