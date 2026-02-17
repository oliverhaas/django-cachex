"""Tests for async miscellaneous cache operations: ascan, alock, adecr_version, etc."""

import pytest

from django_cachex.cache import KeyValueCache


class TestAsyncScan:
    @pytest.mark.asyncio
    async def test_ascan_returns_keys(self, cache: KeyValueCache):
        cache.set("ascantest_a", 1)
        cache.set("ascantest_b", 2)

        cursor, keys = await cache.ascan(pattern="ascantest_*")
        assert isinstance(keys, list)
        if isinstance(cursor, int):
            all_keys = set(keys)
            while cursor != 0:
                cursor, keys = await cache.ascan(cursor=cursor, pattern="ascantest_*")
                all_keys.update(keys)
            assert all_keys == {"ascantest_a", "ascantest_b"}
        else:
            assert set(keys) == {"ascantest_a", "ascantest_b"}


class TestAsyncLock:
    @pytest.fixture(autouse=True)
    def _skip_cluster(self, client_class: str):
        """Async locks use EVALSHA which doesn't work on cluster replicas."""
        if client_class == "cluster":
            pytest.skip("Async lock not supported on cluster (EVALSHA routing)")

    @pytest.mark.asyncio
    async def test_alock_acquire_and_release(self, cache: KeyValueCache):
        lock = cache.alock("alock_resource")
        acquired = await lock.acquire(blocking=False)
        assert acquired is True
        assert cache.has_key("alock_resource") is True

        await lock.release()
        assert cache.has_key("alock_resource") is False

    @pytest.mark.asyncio
    async def test_alock_prevents_double_acquire(self, cache: KeyValueCache):
        lock1 = cache.alock("alock_resource2")
        assert await lock1.acquire(blocking=False) is True

        lock2 = cache.alock("alock_resource2")
        assert await lock2.acquire(blocking=False) is False

        await lock1.release()

    @pytest.mark.asyncio
    async def test_alock_context_manager(self, cache: KeyValueCache):
        async with cache.alock("alock_ctx"):
            assert cache.has_key("alock_ctx") is True
        assert cache.has_key("alock_ctx") is False


class TestAsyncVersionOperations:
    @pytest.mark.asyncio
    async def test_aincr_version(self, cache: KeyValueCache):
        cache.set("{av}:key", "value")
        new_version = await cache.aincr_version("{av}:key")

        assert new_version == 2
        assert cache.get("{av}:key") is None
        assert cache.get("{av}:key", version=2) == "value"

    @pytest.mark.asyncio
    async def test_adecr_version(self, cache: KeyValueCache):
        cache.set("{adv}:key", "hello", version=2)
        new_version = await cache.adecr_version("{adv}:key", version=2)

        assert new_version == 1
        assert cache.get("{adv}:key", version=2) is None
        assert cache.get("{adv}:key", version=1) == "hello"


class TestAsyncClearAllVersions:
    @pytest.mark.asyncio
    async def test_aclear_all_versions(self, cache: KeyValueCache):
        cache.set("acav_key1", "v1", version=1)
        cache.set("acav_key2", "v2", version=2)

        count = await cache.aclear_all_versions()
        assert count >= 2

        assert cache.get("acav_key1", version=1) is None
        assert cache.get("acav_key2", version=2) is None


class TestAsyncGetOrSet:
    @pytest.mark.asyncio
    async def test_aget_or_set_missing_key(self, cache: KeyValueCache):
        result = await cache.aget_or_set("agos_key", "default_value")
        assert result == "default_value"
        assert cache.get("agos_key") == "default_value"

    @pytest.mark.asyncio
    async def test_aget_or_set_existing_key(self, cache: KeyValueCache):
        cache.set("agos_key2", "existing")
        result = await cache.aget_or_set("agos_key2", "default_value")
        assert result == "existing"

    @pytest.mark.asyncio
    async def test_aget_or_set_with_callable(self, cache: KeyValueCache):
        result = await cache.aget_or_set("agos_key3", lambda: "computed")
        assert result == "computed"
        assert cache.get("agos_key3") == "computed"


class TestAsyncFlushDb:
    @pytest.mark.asyncio
    async def test_aflush_db(self, cache: KeyValueCache):
        cache.set("aflush_key", "value")
        assert cache.get("aflush_key") == "value"

        result = await cache.aflush_db()
        assert result is True
        assert cache.get("aflush_key") is None
