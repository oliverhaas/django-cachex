"""Tests for two-tiered cache (L1 in-process + L2 Redis/Valkey) — async."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches

if TYPE_CHECKING:
    from django.core.cache.backends.base import BaseCache


@pytest.mark.asyncio
class TestTieredAsync:
    """Async variants of core operations."""

    async def test_aget_aset(self, tiered_cache: BaseCache):
        await tiered_cache.aset("akey", "aval")
        assert await tiered_cache.aget("akey") == "aval"

    async def test_aget_missing(self, tiered_cache: BaseCache):
        assert await tiered_cache.aget("amissing") is None
        assert await tiered_cache.aget("amissing", "fb") == "fb"

    async def test_adelete(self, tiered_cache: BaseCache):
        await tiered_cache.aset("adel", "val")
        await tiered_cache.adelete("adel")
        assert await tiered_cache.aget("adel") is None

    async def test_aget_many_aset_many(self, tiered_cache: BaseCache):
        await tiered_cache.aset_many({"am1": 1, "am2": 2})
        result = await tiered_cache.aget_many(["am1", "am2"])
        assert result == {"am1": 1, "am2": 2}

    async def test_aadd(self, tiered_cache: BaseCache):
        await tiered_cache.adelete("aadd_key")
        assert await tiered_cache.aadd("aadd_key", "first") is True
        assert await tiered_cache.aadd("aadd_key", "second") is False
        assert await tiered_cache.aget("aadd_key") == "first"

    async def test_ahas_key(self, tiered_cache: BaseCache):
        await tiered_cache.aset("ahk", 1)
        assert await tiered_cache.ahas_key("ahk") is True
        assert await tiered_cache.ahas_key("anope") is False

    async def test_aincr(self, tiered_cache: BaseCache):
        await tiered_cache.aset("ainc", 10)
        assert await tiered_cache.aincr("ainc", 5) == 15
        assert await tiered_cache.aget("ainc") == 15

    async def test_atouch(self, tiered_cache: BaseCache):
        await tiered_cache.aset("atouch", "val")
        assert await tiered_cache.atouch("atouch", timeout=60) is True
        assert await tiered_cache.aget("atouch") == "val"

    async def test_adelete_many(self, tiered_cache: BaseCache):
        await tiered_cache.aset_many({"adm1": 1, "adm2": 2})
        await tiered_cache.adelete_many(["adm1", "adm2"])
        assert await tiered_cache.aget("adm1") is None

    async def test_aclear(self, tiered_cache: BaseCache):
        await tiered_cache.aset("acl", "val")
        await tiered_cache.aclear()
        assert await tiered_cache.aget("acl") is None

    async def test_async_l1_populated_on_miss(self, tiered_cache: BaseCache):
        await tiered_cache.aset("al1pop", "val")
        l1 = caches["l1"]
        l1.delete("al1pop")
        assert l1.get("al1pop") is None

        assert await tiered_cache.aget("al1pop") == "val"
        assert l1.get("al1pop") == "val"

    async def test_async_get_many_partial_l1(self, tiered_cache: BaseCache):
        await tiered_cache.aset_many({"agm1": "a", "agm2": "b"})
        l1 = caches["l1"]
        l1.delete("agm2")

        result = await tiered_cache.aget_many(["agm1", "agm2"])
        assert result == {"agm1": "a", "agm2": "b"}
        assert l1.get("agm2") == "b"
