"""Tests for async key operations: keys, iter_keys, delete_pattern, rename, renamenx."""

import pytest

from django_cachex.cache import KeyValueCache


@pytest.fixture
def mk(cache: KeyValueCache):
    """Create a prefixed key for direct client async testing."""
    return lambda key, version=None: cache.make_and_validate_key(key, version=version)


class TestAsyncKeys:
    """Tests for akeys() method."""

    @pytest.mark.asyncio
    async def test_akeys_returns_matching(self, cache: KeyValueCache, mk):
        cache.set("akeys_foo1", 1)
        cache.set("akeys_foo2", 2)
        cache.set("akeys_bar1", 3)

        keys = await cache._cache.akeys("*akeys_foo*")
        assert len(keys) == 2


class TestAsyncIterKeys:
    """Tests for aiter_keys() method."""

    @pytest.mark.asyncio
    async def test_aiter_keys(self, cache: KeyValueCache, mk):
        cache.set("aikeys_foo1", 1)
        cache.set("aikeys_foo2", 2)
        cache.set("aikeys_foo3", 3)

        result = set()
        async for key in cache._cache.aiter_keys("*aikeys_foo*"):
            result.add(key)
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_aiter_keys_with_itersize(self, cache: KeyValueCache, mk):
        cache.set("aikeys2_foo1", 1)
        cache.set("aikeys2_foo2", 2)
        cache.set("aikeys2_foo3", 3)

        result = [key async for key in cache._cache.aiter_keys("*aikeys2_foo*", itersize=2)]
        assert len(result) == 3


class TestAsyncDeletePattern:
    """Tests for adelete_pattern() method."""

    @pytest.mark.asyncio
    async def test_adelete_pattern(self, cache: KeyValueCache, mk):
        for key in ["adp_foo-aa", "adp_foo-ab", "adp_foo-bb", "adp_foo-bc"]:
            cache.set(key, "foo")

        result = await cache._cache.adelete_pattern("*adp_foo-a*")
        assert bool(result) is True

        keys = cache.keys("adp_foo*")
        assert set(keys) == {"adp_foo-bb", "adp_foo-bc"}

    @pytest.mark.asyncio
    async def test_adelete_pattern_no_match(self, cache: KeyValueCache, mk):
        result = await cache._cache.adelete_pattern("*nonexistent_pattern_xyz*")
        assert bool(result) is False


class TestAsyncRename:
    """Tests for arename() method."""

    @pytest.mark.asyncio
    async def test_arename(self, cache: KeyValueCache, mk):
        cache.set("{aslot}:src", "value1")
        await cache._cache.arename(mk("{aslot}:src"), mk("{aslot}:dest"))

        assert cache.get("{aslot}:src") is None
        assert cache.get("{aslot}:dest") == "value1"

    @pytest.mark.asyncio
    async def test_arename_overwrites_existing(self, cache: KeyValueCache, mk):
        cache.set("{aslot2}:src", "src_value")
        cache.set("{aslot2}:dst", "dst_value")
        await cache._cache.arename(mk("{aslot2}:src"), mk("{aslot2}:dst"))

        assert cache.get("{aslot2}:src") is None
        assert cache.get("{aslot2}:dst") == "src_value"

    @pytest.mark.asyncio
    async def test_arename_nonexistent_raises(self, cache: KeyValueCache, mk):
        with pytest.raises(ValueError, match="not found"):
            await cache._cache.arename(mk("{aslot3}:nonexistent"), mk("{aslot3}:dest"))


class TestAsyncRenameNX:
    """Tests for arenamenx() method."""

    @pytest.mark.asyncio
    async def test_arenamenx(self, cache: KeyValueCache, mk):
        cache.set("{aslot4}:src", "value")
        result = await cache._cache.arenamenx(mk("{aslot4}:src"), mk("{aslot4}:dest"))

        assert result is True
        assert cache.get("{aslot4}:src") is None
        assert cache.get("{aslot4}:dest") == "value"

    @pytest.mark.asyncio
    async def test_arenamenx_fails_if_dest_exists(self, cache: KeyValueCache, mk):
        cache.set("{aslot5}:src", "src_value")
        cache.set("{aslot5}:dest", "existing_value")
        result = await cache._cache.arenamenx(mk("{aslot5}:src"), mk("{aslot5}:dest"))

        assert result is False
        assert cache.get("{aslot5}:src") == "src_value"
        assert cache.get("{aslot5}:dest") == "existing_value"
