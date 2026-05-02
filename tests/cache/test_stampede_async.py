"""Tests for cache stampede prevention via XFetch — async surface."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from django_cachex.cache import KeyValueCache


class TestAsyncStampedeBasicOperations:
    """Basic aset/aget/adelete with stampede prevention enabled."""

    @pytest.mark.asyncio
    async def test_aset_and_aget(self, stampede_cache: KeyValueCache):
        await stampede_cache.aset("asp_basic", "hello", timeout=300)
        assert await stampede_cache.aget("asp_basic") == "hello"

    @pytest.mark.asyncio
    async def test_aget_missing_key(self, stampede_cache: KeyValueCache):
        await stampede_cache.adelete("asp_missing")
        assert await stampede_cache.aget("asp_missing") is None

    @pytest.mark.asyncio
    async def test_adelete(self, stampede_cache: KeyValueCache):
        await stampede_cache.aset("asp_del", "val", timeout=300)
        assert await stampede_cache.adelete("asp_del") is True
        assert await stampede_cache.aget("asp_del") is None


class TestAsyncStampedeGetMany:
    """aget_many() stampede behavior."""

    @pytest.mark.asyncio
    async def test_aget_many(self, stampede_cache: KeyValueCache):
        await stampede_cache.aset("asp_m1", "v1", timeout=300)
        await stampede_cache.aset("asp_m2", "v2", timeout=300)
        await stampede_cache.adelete("asp_m3")

        result = await stampede_cache.aget_many(["asp_m1", "asp_m2", "asp_m3"])
        assert "v1" in result.values()
        assert "v2" in result.values()
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_aget_many_filters_expired(self, stampede_cache: KeyValueCache):
        await stampede_cache.aset("asp_gm_exp", "val", timeout=300)
        # Shrink TTL below buffer → logically expired
        await stampede_cache.aexpire("asp_gm_exp", 50)

        result = await stampede_cache.aget_many(["asp_gm_exp"])
        assert len(result) == 0


class TestAsyncStampedeSetMany:
    """aset_many() stampede behavior."""

    @pytest.mark.asyncio
    async def test_aset_many(self, stampede_cache: KeyValueCache):
        await stampede_cache.aset_many({"asp_sm1": "a", "asp_sm2": "b"}, timeout=300)
        assert await stampede_cache.aget("asp_sm1") == "a"
        assert await stampede_cache.aget("asp_sm2") == "b"


class TestAsyncStampedeEarlyRecompute:
    """XFetch must trigger on the async get path too.

    Uses aexpire() to simulate logical expiry deterministically:
    set with timeout=300 (TTL=360), then expire(key, 50) → TTL=50 < buffer=60.
    """

    @pytest.mark.asyncio
    async def test_areturns_none_after_logical_expiry(self, stampede_cache: KeyValueCache):
        await stampede_cache.aset("asp_expire", "val", timeout=300)
        assert await stampede_cache.aget("asp_expire") == "val"

        await stampede_cache.aexpire("asp_expire", 50)
        assert await stampede_cache.aget("asp_expire") is None

    @pytest.mark.asyncio
    async def test_aget_or_set_recomputes_after_expiry(self, stampede_cache: KeyValueCache):
        """aget_or_set() should trigger recomputation after logical expiry."""
        await stampede_cache.aset("asp_gos", "old", timeout=300)
        assert await stampede_cache.aget("asp_gos") == "old"

        await stampede_cache.aexpire("asp_gos", 50)

        result = await stampede_cache.aget_or_set("asp_gos", lambda: "recomputed", timeout=300)
        assert result == "recomputed"


class TestAsyncStampedeGetOrSetRecompute:
    """aget_or_set must overwrite stale keys (not behave like aadd/NX)."""

    @pytest.mark.asyncio
    async def test_aget_or_set_overwrites_stale_key(self, stampede_cache: KeyValueCache):
        await stampede_cache.aset("asp_gos_overwrite", "stale", timeout=300)
        await stampede_cache.aexpire("asp_gos_overwrite", 50)

        result = await stampede_cache.aget_or_set(
            "asp_gos_overwrite",
            lambda: "fresh_async",
            timeout=300,
        )
        assert result == "fresh_async"
        # Verify the new value is actually stored (not just returned once)
        assert await stampede_cache.aget("asp_gos_overwrite") == "fresh_async"

    @pytest.mark.asyncio
    async def test_aget_or_set_returns_fresh_not_retrigger(self, stampede_cache: KeyValueCache):
        """After recomputation, aget_or_set should return the fresh value, not
        re-trigger stampede on the confirmation aget()."""
        await stampede_cache.aset("asp_gos_retrig", "stale", timeout=300)
        await stampede_cache.aexpire("asp_gos_retrig", 50)

        result = await stampede_cache.aget_or_set(
            "asp_gos_retrig",
            lambda: "recomputed",
            timeout=300,
        )
        assert result == "recomputed"
