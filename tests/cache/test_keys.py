"""Tests for key operations: version, delete_pattern, iter_keys, etc."""

from contextlib import suppress
from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.test import override_settings

if TYPE_CHECKING:
    from collections.abc import Iterable

    from django_cachex.cache import RespCache
    from tests.settings_wrapper import SettingsWrapper


@pytest.fixture
def patch_itersize_setting() -> Iterable[None]:
    # destroy cache to force recreation with overriden settings
    with suppress(AttributeError):
        del caches["default"]
    with override_settings(DJANGO_REDIS_SCAN_ITERSIZE=30):
        yield
    # destroy cache to force recreation with original settings
    with suppress(AttributeError):
        del caches["default"]


class TestVersionOperations:
    def test_version(self, cache: RespCache):
        cache.set("keytest", 2, version=2)
        res = cache.get("keytest")
        assert res is None

        res = cache.get("keytest", version=2)
        assert res == 2

    def test_incr_version(self, cache: RespCache):
        # Use hash tag so versioned keys stay in same cluster slot
        cache.set("{keytest}", 2)
        cache.incr_version("{keytest}")

        res = cache.get("{keytest}")
        assert res is None

        res = cache.get("{keytest}", version=2)
        assert res == 2

    def test_ttl_incr_version_no_timeout(self, cache: RespCache):
        # Use hash tag so versioned keys stay in same cluster slot
        cache.set("{my_key}", "hello world!", timeout=None)

        cache.incr_version("{my_key}")

        my_value = cache.get("{my_key}", version=2)

        assert my_value == "hello world!"


class TestRenameOperations:
    # Note: Use {slot}: prefix to ensure both keys hash to same cluster slot

    def test_rename(self, cache: RespCache):
        cache.set("{slot}:src", "value1")
        cache.rename("{slot}:src", "{slot}:dest")

        assert cache.get("{slot}:src") is None
        assert cache.get("{slot}:dest") == "value1"

    def test_rename_overwrites_existing(self, cache: RespCache):
        cache.set("{slot2}:src", "src_value")
        cache.set("{slot2}:dst", "dst_value")
        cache.rename("{slot2}:src", "{slot2}:dst")

        assert cache.get("{slot2}:src") is None
        assert cache.get("{slot2}:dst") == "src_value"

    def test_rename_preserves_ttl(self, cache: RespCache):
        cache.set("{slot3}:key", "value", timeout=3600)
        cache.rename("{slot3}:key", "{slot3}:dest")

        ttl = cache.ttl("{slot3}:dest")
        assert ttl is not None
        assert ttl > 3500  # Should be close to 3600

    def test_rename_nonexistent_raises(self, cache: RespCache):
        with pytest.raises(ValueError, match="not found"):
            cache.rename("{slot4}:nonexistent", "{slot4}:dest")

    def test_renamenx(self, cache: RespCache):
        cache.set("{slot5}:src", "value")
        result = cache.renamenx("{slot5}:src", "{slot5}:dest")

        assert result is True
        assert cache.get("{slot5}:src") is None
        assert cache.get("{slot5}:dest") == "value"

    def test_renamenx_fails_if_dest_exists(self, cache: RespCache):
        cache.set("{slot6}:src", "src_value")
        cache.set("{slot6}:dest", "existing_value")
        result = cache.renamenx("{slot6}:src", "{slot6}:dest")

        assert result is False
        assert cache.get("{slot6}:src") == "src_value"
        assert cache.get("{slot6}:dest") == "existing_value"

    def test_rename_version_src_dst(self, cache: RespCache):
        cache.set("{vs}:rsrc", "value", version=1)

        cache.rename("{vs}:rsrc", "{vs}:rdst", version_src=1, version_dst=2)
        assert cache.get("{vs}:rsrc", version=1) is None
        assert cache.get("{vs}:rdst", version=2) == "value"

    def test_renamenx_version_src_dst(self, cache: RespCache):
        cache.set("{vs}:rnxsrc", "value", version=1)

        result = cache.renamenx("{vs}:rnxsrc", "{vs}:rnxdst", version_src=1, version_dst=2)
        assert result is True
        assert cache.get("{vs}:rnxsrc", version=1) is None
        assert cache.get("{vs}:rnxdst", version=2) == "value"


class TestDeletePatternOperations:
    def test_delete_pattern(self, cache: RespCache):
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")

        res = cache.delete_pattern("*foo-a*")
        assert bool(res) is True

        keys = cache.keys("foo*")
        assert set(keys) == {"foo-bb", "foo-bc"}

        res = cache.delete_pattern("*foo-a*")
        assert bool(res) is False

    def test_delete_pattern_with_custom_count(self, cache: RespCache):
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")

        # Test that custom itersize works (we can't easily verify the internal itersize,
        # but we can verify the result is correct)
        res = cache.delete_pattern("*foo-a*", itersize=2)
        assert bool(res) is True

        keys = cache.keys("foo*")
        assert set(keys) == {"foo-bb", "foo-bc"}

    def test_delete_pattern_with_settings_default_scan_count(
        self,
        patch_itersize_setting,
        cache: RespCache,
        settings: SettingsWrapper,
    ):
        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")

        # Verify the setting is applied correctly
        assert settings.DJANGO_REDIS_SCAN_ITERSIZE == 30

        res = cache.delete_pattern("*foo-a*")
        assert bool(res) is True

        keys = cache.keys("foo*")
        assert set(keys) == {"foo-bb", "foo-bc"}


class TestIterKeysOperations:
    def test_iter_keys(self, cache: RespCache):
        cache.set("foo1", 1)
        cache.set("foo2", 1)
        cache.set("foo3", 1)

        # Test simple result
        result = set(cache.iter_keys("foo*"))
        assert result == {"foo1", "foo2", "foo3"}

    def test_iter_keys_itersize(self, cache: RespCache):
        cache.set("foo1", 1)
        cache.set("foo2", 1)
        cache.set("foo3", 1)

        # Test limited result
        result = list(cache.iter_keys("foo*", itersize=2))
        assert len(result) == 3

    def test_iter_keys_generator(self, cache: RespCache):
        cache.set("foo1", 1)
        cache.set("foo2", 1)
        cache.set("foo3", 1)

        # Test generator object
        result = cache.iter_keys("foo*")
        next_value = next(result)
        assert next_value is not None


class TestAsyncVersionOperations:
    @pytest.mark.asyncio
    async def test_aversion(self, cache: RespCache):
        await cache.aset("akeytest", 2, version=2)
        res = await cache.aget("akeytest")
        assert res is None

        res = await cache.aget("akeytest", version=2)
        assert res == 2

    @pytest.mark.asyncio
    async def test_aincr_version(self, cache: RespCache):
        await cache.aset("{akeytest}", 2)
        await cache.aincr_version("{akeytest}")

        res = await cache.aget("{akeytest}")
        assert res is None

        res = await cache.aget("{akeytest}", version=2)
        assert res == 2

    @pytest.mark.asyncio
    async def test_attl_aincr_version_no_timeout(self, cache: RespCache):
        await cache.aset("{amy_key}", "hello world!", timeout=None)

        await cache.aincr_version("{amy_key}")

        my_value = await cache.aget("{amy_key}", version=2)

        assert my_value == "hello world!"


class TestAsyncKeys:
    """Tests for akeys() method."""

    @pytest.mark.asyncio
    async def test_akeys_returns_matching(self, cache: RespCache):
        cache.set("akeys_foo1", 1)
        cache.set("akeys_foo2", 2)
        cache.set("akeys_bar1", 3)

        keys = await cache.akeys("akeys_foo*")
        assert len(keys) == 2


class TestAsyncIterKeys:
    """Tests for aiter_keys() method."""

    @pytest.mark.asyncio
    async def test_aiter_keys(self, cache: RespCache):
        cache.set("aikeys_foo1", 1)
        cache.set("aikeys_foo2", 2)
        cache.set("aikeys_foo3", 3)

        result = set()
        async for key in cache.aiter_keys("aikeys_foo*"):
            result.add(key)
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_aiter_keys_with_itersize(self, cache: RespCache):
        cache.set("aikeys2_foo1", 1)
        cache.set("aikeys2_foo2", 2)
        cache.set("aikeys2_foo3", 3)

        result = [key async for key in cache.aiter_keys("aikeys2_foo*", itersize=2)]
        assert len(result) == 3


class TestAsyncDeletePattern:
    """Tests for adelete_pattern() method."""

    @pytest.mark.asyncio
    async def test_adelete_pattern(self, cache: RespCache):
        for key in ["adp_foo-aa", "adp_foo-ab", "adp_foo-bb", "adp_foo-bc"]:
            cache.set(key, "foo")

        result = await cache.adelete_pattern("adp_foo-a*")
        assert bool(result) is True

        keys = cache.keys("adp_foo*")
        assert set(keys) == {"adp_foo-bb", "adp_foo-bc"}

    @pytest.mark.asyncio
    async def test_adelete_pattern_no_match(self, cache: RespCache):
        result = await cache.adelete_pattern("nonexistent_pattern_xyz*")
        assert bool(result) is False


class TestAsyncRename:
    """Tests for arename() method."""

    @pytest.mark.asyncio
    async def test_arename(self, cache: RespCache):
        cache.set("{aslot}:src", "value1")
        await cache.arename("{aslot}:src", "{aslot}:dest")

        assert cache.get("{aslot}:src") is None
        assert cache.get("{aslot}:dest") == "value1"

    @pytest.mark.asyncio
    async def test_arename_overwrites_existing(self, cache: RespCache):
        cache.set("{aslot2}:src", "src_value")
        cache.set("{aslot2}:dst", "dst_value")
        await cache.arename("{aslot2}:src", "{aslot2}:dst")

        assert cache.get("{aslot2}:src") is None
        assert cache.get("{aslot2}:dst") == "src_value"

    @pytest.mark.asyncio
    async def test_arename_nonexistent_raises(self, cache: RespCache):
        with pytest.raises(ValueError, match="not found"):
            await cache.arename("{aslot3}:nonexistent", "{aslot3}:dest")


class TestAsyncRenameNX:
    """Tests for arenamenx() method."""

    @pytest.mark.asyncio
    async def test_arenamenx(self, cache: RespCache):
        cache.set("{aslot4}:src", "value")
        result = await cache.arenamenx("{aslot4}:src", "{aslot4}:dest")

        assert result is True
        assert cache.get("{aslot4}:src") is None
        assert cache.get("{aslot4}:dest") == "value"

    @pytest.mark.asyncio
    async def test_arenamenx_fails_if_dest_exists(self, cache: RespCache):
        cache.set("{aslot5}:src", "src_value")
        cache.set("{aslot5}:dest", "existing_value")
        result = await cache.arenamenx("{aslot5}:src", "{aslot5}:dest")

        assert result is False
        assert cache.get("{aslot5}:src") == "src_value"
        assert cache.get("{aslot5}:dest") == "existing_value"


class TestAsyncVersionSrcDst:
    """Tests for version_src/version_dst on arename and arenamenx."""

    @pytest.mark.asyncio
    async def test_arename_version_src_dst(self, cache: RespCache):
        cache.set("{vs}:arsrc", "value", version=1)

        await cache.arename("{vs}:arsrc", "{vs}:ardst", version_src=1, version_dst=2)
        assert cache.get("{vs}:arsrc", version=1) is None
        assert cache.get("{vs}:ardst", version=2) == "value"

    @pytest.mark.asyncio
    async def test_arenamenx_version_src_dst(self, cache: RespCache):
        cache.set("{vs}:arnxsrc", "value", version=1)

        result = await cache.arenamenx("{vs}:arnxsrc", "{vs}:arnxdst", version_src=1, version_dst=2)
        assert result is True
        assert cache.get("{vs}:arnxsrc", version=1) is None
        assert cache.get("{vs}:arnxdst", version=2) == "value"


class TestAsyncRenameTTL:
    """Tests covering TTL preservation across arename()."""

    @pytest.mark.asyncio
    async def test_arename_preserves_ttl(self, cache: RespCache):
        await cache.aset("{aslotttl}:key", "value", timeout=3600)
        await cache.arename("{aslotttl}:key", "{aslotttl}:dest")

        ttl = await cache.attl("{aslotttl}:dest")
        assert ttl is not None
        assert ttl > 3500


class TestAsyncDeletePatternExtra:
    """Coverage for itersize / SCAN-count knobs on adelete_pattern."""

    @pytest.mark.asyncio
    async def test_adelete_pattern_with_custom_count(self, cache: RespCache):
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
        cache: RespCache,
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
    async def test_aiter_keys_itersize(self, cache: RespCache):
        cache.set("aiks_foo1", 1)
        cache.set("aiks_foo2", 1)
        cache.set("aiks_foo3", 1)

        result = [key async for key in cache.aiter_keys("aiks_foo*", itersize=2)]
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_aiter_keys_async_generator(self, cache: RespCache):
        cache.set("aikgen_foo1", 1)
        cache.set("aikgen_foo2", 1)
        cache.set("aikgen_foo3", 1)

        result = cache.aiter_keys("aikgen_foo*")
        next_value = await anext(result)
        assert next_value is not None
