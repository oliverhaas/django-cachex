"""Tests for miscellaneous cache operations: scan, decr_version, clear_all_versions, flush_db."""

from typing import TYPE_CHECKING

import pytest

from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


class TestScanOperations:
    def _is_py_cluster(self, client_class: str, sentinel_mode: str | bool, resp_adapter: str) -> bool:
        """redis-py / valkey-py cluster can't combine per-node cursors, so SCAN raises.
        The Rust adapter scans all nodes itself and returns combined keys.
        """
        return client_class == "cluster" and not sentinel_mode and resp_adapter in {"redis-py", "valkey-py"}

    def test_scan_returns_keys(
        self,
        cache: RespCache,
        client_class: str,
        sentinel_mode: str | bool,
        resp_adapter: str,
    ):
        if self._is_py_cluster(client_class, sentinel_mode, resp_adapter):
            with pytest.raises(NotSupportedError):
                cache.scan(pattern="scantest_*")
            return

        cache.set("scantest_a", 1)
        cache.set("scantest_b", 2)

        cursor, keys = cache.scan(pattern="scantest_*")
        assert isinstance(keys, list)
        all_keys = set(keys)
        while cursor != 0:
            cursor, keys = cache.scan(cursor=cursor, pattern="scantest_*")
            all_keys.update(keys)
        assert all_keys == {"scantest_a", "scantest_b"}

    def test_scan_empty(
        self,
        cache: RespCache,
        client_class: str,
        sentinel_mode: str | bool,
        resp_adapter: str,
    ):
        if self._is_py_cluster(client_class, sentinel_mode, resp_adapter):
            with pytest.raises(NotSupportedError):
                cache.scan(pattern="nonexistent_pattern_xyz_*")
            return

        _cursor, keys = cache.scan(pattern="nonexistent_pattern_xyz_*")
        assert keys == []


class TestDecrVersionOperations:
    def test_decr_version(self, cache: RespCache):
        # Use hash tag so versioned keys stay in same cluster slot
        cache.set("{dv}:key", "hello", version=2)
        new_version = cache.decr_version("{dv}:key", version=2)

        assert new_version == 1
        assert cache.get("{dv}:key", version=2) is None
        assert cache.get("{dv}:key", version=1) == "hello"

    def test_decr_version_default(self, cache: RespCache):
        # Set at default version (1), decrement to version 0
        cache.set("{dv2}:key", "value")
        new_version = cache.decr_version("{dv2}:key")

        assert new_version == 0
        assert cache.get("{dv2}:key") is None
        assert cache.get("{dv2}:key", version=0) == "value"


class TestClearAllVersions:
    def test_clear_all_versions(self, cache: RespCache):
        cache.set("cav_key1", "v1", version=1)
        cache.set("cav_key2", "v2", version=2)

        count = cache.clear_all_versions()
        assert count == 2

        assert cache.get("cav_key1", version=1) is None
        assert cache.get("cav_key2", version=2) is None


class TestFlushDb:
    def test_flush_db(self, cache: RespCache):
        cache.set("flush_key", "value")
        assert cache.get("flush_key") == "value"

        result = cache.flush_db()
        assert result is True
        assert cache.get("flush_key") is None


class TestAsyncScan:
    @pytest.mark.asyncio
    async def test_ascan_returns_keys(
        self,
        cache: RespCache,
        client_class: str,
        sentinel_mode: str | bool,
        resp_adapter: str,
    ):
        if client_class == "cluster" and not sentinel_mode and resp_adapter in {"redis-py", "valkey-py"}:
            with pytest.raises(NotSupportedError):
                await cache.ascan(pattern="ascantest_*")
            return

        cache.set("ascantest_a", 1)
        cache.set("ascantest_b", 2)

        cursor, keys = await cache.ascan(pattern="ascantest_*")
        assert isinstance(keys, list)
        all_keys = set(keys)
        while cursor != 0:
            cursor, keys = await cache.ascan(cursor=cursor, pattern="ascantest_*")
            all_keys.update(keys)
        assert all_keys == {"ascantest_a", "ascantest_b"}

    @pytest.mark.asyncio
    async def test_ascan_empty(
        self,
        cache: RespCache,
        client_class: str,
        sentinel_mode: str | bool,
        resp_adapter: str,
    ):
        if client_class == "cluster" and not sentinel_mode and resp_adapter in {"redis-py", "valkey-py"}:
            with pytest.raises(NotSupportedError):
                await cache.ascan(pattern="nonexistent_pattern_xyz_*")
            return

        _cursor, keys = await cache.ascan(pattern="nonexistent_pattern_xyz_*")
        assert keys == []


class TestAsyncLock:
    @pytest.fixture(autouse=True)
    def _skip_cluster(self, client_class: str):
        """Async locks use EVALSHA which doesn't work on cluster replicas."""
        if client_class == "cluster":
            pytest.skip("Async lock not supported on cluster (EVALSHA routing)")

    @pytest.mark.asyncio
    async def test_alock_acquire_and_release(self, cache: RespCache):
        lock = await cache.alock("alock_resource")
        acquired = await lock.acquire(blocking=False)
        assert acquired is True
        assert cache.has_key("alock_resource") is True

        await lock.release()
        assert cache.has_key("alock_resource") is False

    @pytest.mark.asyncio
    async def test_alock_prevents_double_acquire(self, cache: RespCache):
        lock1 = await cache.alock("alock_resource2")
        assert await lock1.acquire(blocking=False) is True

        lock2 = await cache.alock("alock_resource2")
        assert await lock2.acquire(blocking=False) is False

        await lock1.release()

    @pytest.mark.asyncio
    async def test_alock_context_manager(self, cache: RespCache):
        async with await cache.alock("alock_ctx"):
            assert cache.has_key("alock_ctx") is True
        assert cache.has_key("alock_ctx") is False


class TestAsyncVersionOperations:
    @pytest.mark.asyncio
    async def test_aincr_version(self, cache: RespCache):
        cache.set("{av}:key", "value")
        new_version = await cache.aincr_version("{av}:key")

        assert new_version == 2
        assert cache.get("{av}:key") is None
        assert cache.get("{av}:key", version=2) == "value"

    @pytest.mark.asyncio
    async def test_adecr_version(self, cache: RespCache):
        cache.set("{adv}:key", "hello", version=2)
        new_version = await cache.adecr_version("{adv}:key", version=2)

        assert new_version == 1
        assert cache.get("{adv}:key", version=2) is None
        assert cache.get("{adv}:key", version=1) == "hello"

    @pytest.mark.asyncio
    async def test_adecr_version_default(self, cache: RespCache):
        cache.set("{adv2}:key", "value")
        new_version = await cache.adecr_version("{adv2}:key")

        assert new_version == 0
        assert cache.get("{adv2}:key") is None
        assert cache.get("{adv2}:key", version=0) == "value"


class TestAsyncClearAllVersions:
    @pytest.mark.asyncio
    async def test_aclear_all_versions(self, cache: RespCache):
        cache.set("acav_key1", "v1", version=1)
        cache.set("acav_key2", "v2", version=2)

        count = await cache.aclear_all_versions()
        assert count == 2

        assert cache.get("acav_key1", version=1) is None
        assert cache.get("acav_key2", version=2) is None


class TestAsyncGetOrSet:
    @pytest.mark.asyncio
    async def test_aget_or_set_missing_key(self, cache: RespCache):
        result = await cache.aget_or_set("agos_key", "default_value")
        assert result == "default_value"
        assert cache.get("agos_key") == "default_value"

    @pytest.mark.asyncio
    async def test_aget_or_set_existing_key(self, cache: RespCache):
        cache.set("agos_key2", "existing")
        result = await cache.aget_or_set("agos_key2", "default_value")
        assert result == "existing"

    @pytest.mark.asyncio
    async def test_aget_or_set_with_callable(self, cache: RespCache):
        result = await cache.aget_or_set("agos_key3", lambda: "computed")
        assert result == "computed"
        assert cache.get("agos_key3") == "computed"


class TestAsyncFlushDb:
    @pytest.mark.asyncio
    async def test_aflush_db(self, cache: RespCache):
        cache.set("aflush_key", "value")
        assert cache.get("aflush_key") == "value"

        result = await cache.aflush_db()
        assert result is True
        assert cache.get("aflush_key") is None
