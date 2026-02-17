"""Tests for async key operations: akeys, aiter_keys, adelete_pattern, arename, arenamenx."""

import pytest

from django_cachex.cache import KeyValueCache


class TestAsyncKeys:
    """Tests for akeys() method."""

    @pytest.mark.asyncio
    async def test_akeys_returns_matching(self, cache: KeyValueCache):
        cache.set("akeys_foo1", 1)
        cache.set("akeys_foo2", 2)
        cache.set("akeys_bar1", 3)

        keys = await cache.akeys("akeys_foo*")
        assert len(keys) == 2


class TestAsyncIterKeys:
    """Tests for aiter_keys() method."""

    @pytest.mark.asyncio
    async def test_aiter_keys(self, cache: KeyValueCache):
        cache.set("aikeys_foo1", 1)
        cache.set("aikeys_foo2", 2)
        cache.set("aikeys_foo3", 3)

        result = set()
        async for key in cache.aiter_keys("aikeys_foo*"):
            result.add(key)
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_aiter_keys_with_itersize(self, cache: KeyValueCache):
        cache.set("aikeys2_foo1", 1)
        cache.set("aikeys2_foo2", 2)
        cache.set("aikeys2_foo3", 3)

        result = [key async for key in cache.aiter_keys("aikeys2_foo*", itersize=2)]
        assert len(result) == 3


class TestAsyncDeletePattern:
    """Tests for adelete_pattern() method."""

    @pytest.mark.asyncio
    async def test_adelete_pattern(self, cache: KeyValueCache):
        for key in ["adp_foo-aa", "adp_foo-ab", "adp_foo-bb", "adp_foo-bc"]:
            cache.set(key, "foo")

        result = await cache.adelete_pattern("adp_foo-a*")
        assert bool(result) is True

        keys = cache.keys("adp_foo*")
        assert set(keys) == {"adp_foo-bb", "adp_foo-bc"}

    @pytest.mark.asyncio
    async def test_adelete_pattern_no_match(self, cache: KeyValueCache):
        result = await cache.adelete_pattern("nonexistent_pattern_xyz*")
        assert bool(result) is False


class TestAsyncRename:
    """Tests for arename() method."""

    @pytest.mark.asyncio
    async def test_arename(self, cache: KeyValueCache):
        cache.set("{aslot}:src", "value1")
        await cache.arename("{aslot}:src", "{aslot}:dest")

        assert cache.get("{aslot}:src") is None
        assert cache.get("{aslot}:dest") == "value1"

    @pytest.mark.asyncio
    async def test_arename_overwrites_existing(self, cache: KeyValueCache):
        cache.set("{aslot2}:src", "src_value")
        cache.set("{aslot2}:dst", "dst_value")
        await cache.arename("{aslot2}:src", "{aslot2}:dst")

        assert cache.get("{aslot2}:src") is None
        assert cache.get("{aslot2}:dst") == "src_value"

    @pytest.mark.asyncio
    async def test_arename_nonexistent_raises(self, cache: KeyValueCache):
        with pytest.raises(ValueError, match="not found"):
            await cache.arename("{aslot3}:nonexistent", "{aslot3}:dest")


class TestAsyncRenameNX:
    """Tests for arenamenx() method."""

    @pytest.mark.asyncio
    async def test_arenamenx(self, cache: KeyValueCache):
        cache.set("{aslot4}:src", "value")
        result = await cache.arenamenx("{aslot4}:src", "{aslot4}:dest")

        assert result is True
        assert cache.get("{aslot4}:src") is None
        assert cache.get("{aslot4}:dest") == "value"

    @pytest.mark.asyncio
    async def test_arenamenx_fails_if_dest_exists(self, cache: KeyValueCache):
        cache.set("{aslot5}:src", "src_value")
        cache.set("{aslot5}:dest", "existing_value")
        result = await cache.arenamenx("{aslot5}:src", "{aslot5}:dest")

        assert result is False
        assert cache.get("{aslot5}:src") == "src_value"
        assert cache.get("{aslot5}:dest") == "existing_value"


class TestAsyncVersionSrcDst:
    """Tests for version_src/version_dst on arename and arenamenx."""

    @pytest.mark.asyncio
    async def test_arename_version_src_dst(self, cache: KeyValueCache):
        """arename with different source and destination versions."""
        cache.set("{vs}:arsrc", "value", version=1)

        await cache.arename("{vs}:arsrc", "{vs}:ardst", version_src=1, version_dst=2)
        assert cache.get("{vs}:arsrc", version=1) is None
        assert cache.get("{vs}:ardst", version=2) == "value"

    @pytest.mark.asyncio
    async def test_arenamenx_version_src_dst(self, cache: KeyValueCache):
        """arenamenx with different source and destination versions."""
        cache.set("{vs}:arnxsrc", "value", version=1)

        result = await cache.arenamenx("{vs}:arnxsrc", "{vs}:arnxdst", version_src=1, version_dst=2)
        assert result is True
        assert cache.get("{vs}:arnxsrc", version=1) is None
        assert cache.get("{vs}:arnxdst", version=2) == "value"
