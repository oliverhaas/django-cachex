"""Tests for async key operations: akeys, aiter_keys, adelete_pattern, arename, arenamenx."""

from contextlib import suppress
from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.test import override_settings

if TYPE_CHECKING:
    from collections.abc import Iterable

    from django_cachex.cache import KeyValueCache
    from tests.settings_wrapper import SettingsWrapper


@pytest.fixture
def patch_itersize_setting() -> Iterable[None]:
    with suppress(AttributeError):
        del caches["default"]
    with override_settings(DJANGO_REDIS_SCAN_ITERSIZE=30):
        yield
    with suppress(AttributeError):
        del caches["default"]


class TestAsyncVersionOperations:
    @pytest.mark.asyncio
    async def test_aversion(self, cache: KeyValueCache):
        await cache.aset("akeytest", 2, version=2)
        res = await cache.aget("akeytest")
        assert res is None

        res = await cache.aget("akeytest", version=2)
        assert res == 2

    @pytest.mark.asyncio
    async def test_aincr_version(self, cache: KeyValueCache):
        await cache.aset("{akeytest}", 2)
        await cache.aincr_version("{akeytest}")

        res = await cache.aget("{akeytest}")
        assert res is None

        res = await cache.aget("{akeytest}", version=2)
        assert res == 2

    @pytest.mark.asyncio
    async def test_attl_aincr_version_no_timeout(self, cache: KeyValueCache):
        await cache.aset("{amy_key}", "hello world!", timeout=None)

        await cache.aincr_version("{amy_key}")

        my_value = await cache.aget("{amy_key}", version=2)

        assert my_value == "hello world!"


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
        cache.set("{vs}:arsrc", "value", version=1)

        await cache.arename("{vs}:arsrc", "{vs}:ardst", version_src=1, version_dst=2)
        assert cache.get("{vs}:arsrc", version=1) is None
        assert cache.get("{vs}:ardst", version=2) == "value"

    @pytest.mark.asyncio
    async def test_arenamenx_version_src_dst(self, cache: KeyValueCache):
        cache.set("{vs}:arnxsrc", "value", version=1)

        result = await cache.arenamenx("{vs}:arnxsrc", "{vs}:arnxdst", version_src=1, version_dst=2)
        assert result is True
        assert cache.get("{vs}:arnxsrc", version=1) is None
        assert cache.get("{vs}:arnxdst", version=2) == "value"


class TestAsyncRenameTTL:
    """Tests covering TTL preservation across arename()."""

    @pytest.mark.asyncio
    async def test_arename_preserves_ttl(self, cache: KeyValueCache):
        await cache.aset("{aslotttl}:key", "value", timeout=3600)
        await cache.arename("{aslotttl}:key", "{aslotttl}:dest")

        ttl = await cache.attl("{aslotttl}:dest")
        assert ttl is not None
        assert ttl > 3500


class TestAsyncDeletePatternExtra:
    """Coverage for itersize / SCAN-count knobs on adelete_pattern."""

    @pytest.mark.asyncio
    async def test_adelete_pattern_with_custom_count(self, cache: KeyValueCache):
        for key in ["afoo-aa", "afoo-ab", "afoo-bb", "afoo-bc"]:
            cache.set(key, "foo")

        res = await cache.adelete_pattern("*afoo-a*", itersize=2)
        assert bool(res) is True

        keys = cache.keys("afoo*")
        assert set(keys) == {"afoo-bb", "afoo-bc"}

    @pytest.mark.asyncio
    async def test_adelete_pattern_with_settings_default_scan_count(
        self,
        patch_itersize_setting,
        cache: KeyValueCache,
        settings: SettingsWrapper,
    ):
        for key in ["asfoo-aa", "asfoo-ab", "asfoo-bb", "asfoo-bc"]:
            cache.set(key, "foo")

        assert settings.DJANGO_REDIS_SCAN_ITERSIZE == 30

        res = await cache.adelete_pattern("*asfoo-a*")
        assert bool(res) is True

        keys = cache.keys("asfoo*")
        assert set(keys) == {"asfoo-bb", "asfoo-bc"}


class TestAsyncIterKeysExtra:
    """Coverage for the async iter_keys generator surface."""

    @pytest.mark.asyncio
    async def test_aiter_keys_itersize(self, cache: KeyValueCache):
        cache.set("aiks_foo1", 1)
        cache.set("aiks_foo2", 1)
        cache.set("aiks_foo3", 1)

        result = [key async for key in cache.aiter_keys("aiks_foo*", itersize=2)]
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_aiter_keys_async_generator(self, cache: KeyValueCache):
        cache.set("aikgen_foo1", 1)
        cache.set("aikgen_foo2", 1)
        cache.set("aikgen_foo3", 1)

        result = cache.aiter_keys("aikgen_foo*")
        next_value = await anext(result)
        assert next_value is not None
