"""Tests for async cache operations."""

import datetime
from typing import TYPE_CHECKING

import pytest

from django_cachex.serializers.json import JSONSerializer
from django_cachex.serializers.msgpack import MessagePackSerializer

if TYPE_CHECKING:
    from django_cachex.cache import KeyValueCache
    from tests.settings_wrapper import SettingsWrapper


class TestAsyncAdd:
    """Tests for the aadd() method."""

    @pytest.mark.asyncio
    async def test_aadd_succeeds_for_new_key(self, cache: KeyValueCache):
        cache.delete("async_add_new")
        result = await cache.aadd("async_add_new", "new_value")
        assert result is True
        assert cache.get("async_add_new") == "new_value"

    @pytest.mark.asyncio
    async def test_aadd_fails_for_existing_key(self, cache: KeyValueCache):
        cache.set("async_add_existing", "original")
        result = await cache.aadd("async_add_existing", "new_value")
        assert result is False
        assert cache.get("async_add_existing") == "original"

    @pytest.mark.asyncio
    async def test_aadd_with_timeout(self, cache: KeyValueCache):
        cache.delete("async_add_timeout")
        result = await cache.aadd("async_add_timeout", "timed_value", timeout=60)
        assert result is True
        ttl = cache.ttl("async_add_timeout")
        assert ttl is not None and ttl > 0


class TestAsyncSet:
    """Tests for the aset() method."""

    @pytest.mark.asyncio
    async def test_aset_and_get(self, cache: KeyValueCache):
        await cache.aset("async_set_key", "async_set_value")
        result = cache.get("async_set_key")
        assert result == "async_set_value"

    @pytest.mark.asyncio
    async def test_aset_with_timeout(self, cache: KeyValueCache):
        await cache.aset("async_timeout_key", "timeout_value", timeout=60)
        result = cache.get("async_timeout_key")
        assert result == "timeout_value"
        # Verify TTL was set
        ttl = cache.ttl("async_timeout_key")
        assert ttl is not None and ttl > 0

    @pytest.mark.asyncio
    async def test_aset_overwrites_existing(self, cache: KeyValueCache):
        cache.set("async_overwrite", "original")
        await cache.aset("async_overwrite", "updated")
        result = cache.get("async_overwrite")
        assert result == "updated"

    @pytest.mark.asyncio
    async def test_aset_complex_value(self, cache: KeyValueCache):
        data = {"items": [1, 2, 3], "nested": {"key": "value"}}
        await cache.aset("async_complex_set", data)
        result = cache.get("async_complex_set")
        assert result == data

    @pytest.mark.asyncio
    async def test_aset_with_version(self, cache: KeyValueCache):
        await cache.aset("versioned_set", "v1_data", version=1)
        await cache.aset("versioned_set", "v2_data", version=2)

        result_v1 = cache.get("versioned_set", version=1)
        result_v2 = cache.get("versioned_set", version=2)

        assert result_v1 == "v1_data"
        assert result_v2 == "v2_data"


class TestAsyncDelete:
    """Tests for the adelete() method."""

    @pytest.mark.asyncio
    async def test_adelete_existing_key(self, cache: KeyValueCache):
        cache.set("async_delete_key", "to_delete")
        assert cache.get("async_delete_key") == "to_delete"

        result = await cache.adelete("async_delete_key")
        assert result is True
        assert cache.get("async_delete_key") is None

    @pytest.mark.asyncio
    async def test_adelete_nonexistent_key(self, cache: KeyValueCache):
        cache.delete("nonexistent_async_key")
        result = await cache.adelete("nonexistent_async_key")
        assert result is False

    @pytest.mark.asyncio
    async def test_adelete_with_version(self, cache: KeyValueCache):
        cache.set("versioned_delete", "v1_data", version=1)
        cache.set("versioned_delete", "v2_data", version=2)

        await cache.adelete("versioned_delete", version=1)

        assert cache.get("versioned_delete", version=1) is None
        assert cache.get("versioned_delete", version=2) == "v2_data"


class TestAsyncTouch:
    """Tests for the atouch() method."""

    @pytest.mark.asyncio
    async def test_atouch_updates_timeout(self, cache: KeyValueCache):
        cache.set("async_touch_key", "value", timeout=10)
        result = await cache.atouch("async_touch_key", timeout=60)
        assert result is True
        ttl = cache.ttl("async_touch_key")
        assert ttl is not None and ttl > 10

    @pytest.mark.asyncio
    async def test_atouch_nonexistent_key(self, cache: KeyValueCache):
        cache.delete("async_touch_missing")
        result = await cache.atouch("async_touch_missing", timeout=60)
        assert result is False


class TestAsyncHasKey:
    """Tests for the ahas_key() method."""

    @pytest.mark.asyncio
    async def test_ahas_key_exists(self, cache: KeyValueCache):
        cache.set("async_has_key", "value")
        result = await cache.ahas_key("async_has_key")
        assert result is True

    @pytest.mark.asyncio
    async def test_ahas_key_missing(self, cache: KeyValueCache):
        cache.delete("async_missing_key")
        result = await cache.ahas_key("async_missing_key")
        assert result is False


class TestAsyncIncr:
    """Tests for the aincr() and adecr() methods."""

    @pytest.mark.asyncio
    async def test_aincr_increments(self, cache: KeyValueCache):
        cache.set("async_counter", 10)
        result = await cache.aincr("async_counter")
        assert result == 11
        assert cache.get("async_counter") == 11

    @pytest.mark.asyncio
    async def test_aincr_by_amount(self, cache: KeyValueCache):
        cache.set("async_counter2", 5)
        result = await cache.aincr("async_counter2", 10)
        assert result == 15

    @pytest.mark.asyncio
    async def test_aincr_missing_key_creates_it(self, cache: KeyValueCache):
        cache.delete("async_missing_counter")
        result = await cache.aincr("async_missing_counter")
        assert result == 1

    @pytest.mark.asyncio
    async def test_adecr_decrements(self, cache: KeyValueCache):
        cache.set("async_decr", 10)
        result = await cache.adecr("async_decr")
        assert result == 9


class TestAsyncGetMany:
    """Tests for the aget_many() method."""

    @pytest.mark.asyncio
    async def test_aget_many_retrieves_multiple(self, cache: KeyValueCache):
        cache.set("async_many_a", "value_a")
        cache.set("async_many_b", "value_b")
        cache.set("async_many_c", "value_c")

        result = await cache.aget_many(["async_many_a", "async_many_b", "async_many_c"])
        assert result == {
            "async_many_a": "value_a",
            "async_many_b": "value_b",
            "async_many_c": "value_c",
        }

    @pytest.mark.asyncio
    async def test_aget_many_partial_match(self, cache: KeyValueCache):
        """Test aget_many only returns existing keys."""
        cache.set("async_partial_a", "a")
        cache.delete("async_partial_b")

        result = await cache.aget_many(["async_partial_a", "async_partial_b"])
        assert result == {"async_partial_a": "a"}


class TestAsyncSetMany:
    """Tests for the aset_many() method."""

    @pytest.mark.asyncio
    async def test_aset_many_stores_multiple(self, cache: KeyValueCache):
        await cache.aset_many(
            {
                "async_set_many_x": 1,
                "async_set_many_y": 2,
                "async_set_many_z": 3,
            },
        )

        assert cache.get("async_set_many_x") == 1
        assert cache.get("async_set_many_y") == 2
        assert cache.get("async_set_many_z") == 3


class TestAsyncDeleteMany:
    """Tests for the adelete_many() method."""

    @pytest.mark.asyncio
    async def test_adelete_many_removes_multiple(self, cache: KeyValueCache):
        cache.set_many({"async_del_1": 1, "async_del_2": 2, "async_del_3": 3})

        result = await cache.adelete_many(["async_del_1", "async_del_2"])
        assert result == 2

        assert cache.get("async_del_1") is None
        assert cache.get("async_del_2") is None
        assert cache.get("async_del_3") == 3


class TestAsyncClear:
    """Tests for the aclear() method."""

    @pytest.mark.asyncio
    async def test_aclear_removes_all(self, cache: KeyValueCache):
        cache.set("async_clear_key", "value")
        assert cache.get("async_clear_key") == "value"

        result = await cache.aclear()
        assert result is True
        assert cache.get("async_clear_key") is None


class TestAsyncGet:
    """Tests for the aget() method."""

    @pytest.mark.asyncio
    async def test_aget_existing_key(self, cache: KeyValueCache):
        cache.set("async_key", "async_value")
        result = await cache.aget("async_key")
        assert result == "async_value"

    @pytest.mark.asyncio
    async def test_aget_missing_key_returns_default(self, cache: KeyValueCache):
        cache.delete("missing_async_key")
        result = await cache.aget("missing_async_key")
        assert result is None

    @pytest.mark.asyncio
    async def test_aget_missing_key_with_custom_default(self, cache: KeyValueCache):
        cache.delete("missing_async_key2")
        result = await cache.aget("missing_async_key2", default="fallback")
        assert result == "fallback"

    @pytest.mark.asyncio
    async def test_aget_complex_value(self, cache: KeyValueCache):
        data = {"user": "alice", "scores": [10, 20, 30], "active": True}
        cache.set("async_complex", data)
        result = await cache.aget("async_complex")
        assert result == data

    @pytest.mark.asyncio
    async def test_aget_with_version(self, cache: KeyValueCache):
        cache.set("versioned_key", "v1_value", version=1)
        cache.set("versioned_key", "v2_value", version=2)

        result_v1 = await cache.aget("versioned_key", version=1)
        result_v2 = await cache.aget("versioned_key", version=2)

        assert result_v1 == "v1_value"
        assert result_v2 == "v2_value"


class TestAsyncSetIfNotExists:
    """Tests for aset() with nx (not exists) flag."""

    @pytest.mark.asyncio
    async def test_aset_nx_creates_new_key(self, cache: KeyValueCache):
        await cache.adelete("anx_key")
        assert await cache.aget("anx_key") is None

        result = await cache.aset("anx_key", 42, nx=True)
        assert result is True
        assert await cache.aget("anx_key") == 42

    @pytest.mark.asyncio
    async def test_aset_nx_does_not_overwrite(self, cache: KeyValueCache):
        await cache.aset("anx_existing", "original")
        result = await cache.aset("anx_existing", "changed", nx=True)
        assert result is False
        assert await cache.aget("anx_existing") == "original"

    @pytest.mark.asyncio
    async def test_aset_nx_cleanup(self, cache: KeyValueCache):
        await cache.aset("anx_cleanup", "temp", nx=True)
        await cache.adelete("anx_cleanup")
        assert await cache.aget("anx_cleanup") is None


class TestAsyncSetWithGet:
    """Tests for aset() with get=True (Redis 6.2+ SET GET)."""

    @pytest.mark.asyncio
    async def test_aset_get_returns_old_value(self, cache: KeyValueCache):
        await cache.aset("aget_key", "old_value")
        old = await cache.aset("aget_key", "new_value", get=True)
        assert old == "old_value"
        assert await cache.aget("aget_key") == "new_value"

    @pytest.mark.asyncio
    async def test_aset_get_returns_none_for_missing_key(self, cache: KeyValueCache):
        await cache.adelete("aget_missing")
        old = await cache.aset("aget_missing", "first_value", get=True)
        assert old is None
        assert await cache.aget("aget_missing") == "first_value"

    @pytest.mark.asyncio
    async def test_aset_get_with_different_types(self, cache: KeyValueCache):
        await cache.aset("aget_typed", 42)
        old = await cache.aset("aget_typed", "now_a_string", get=True)
        assert old == 42
        assert await cache.aget("aget_typed") == "now_a_string"

    @pytest.mark.asyncio
    async def test_aset_get_with_timeout(self, cache: KeyValueCache):
        await cache.aset("aget_ttl", "original", timeout=None)
        old = await cache.aset("aget_ttl", "updated", timeout=60, get=True)
        assert old == "original"
        ttl = await cache.attl("aget_ttl")
        assert ttl is not None and ttl > 0

    @pytest.mark.asyncio
    async def test_aset_get_preserves_none_vs_missing(self, cache: KeyValueCache):
        await cache.adelete("aget_chain")
        old1 = await cache.aset("aget_chain", "a", get=True)
        assert old1 is None
        old2 = await cache.aset("aget_chain", "b", get=True)
        assert old2 == "a"
        old3 = await cache.aset("aget_chain", "c", get=True)
        assert old3 == "b"


class TestAsyncUnicodeSupport:
    """Tests for unicode key and value handling on async APIs."""

    @pytest.mark.asyncio
    async def test_acyrillic_key(self, cache: KeyValueCache):
        await cache.aset("ключ", "данные")
        assert await cache.aget("ключ") == "данные"

    @pytest.mark.asyncio
    async def test_aemoji_value(self, cache: KeyValueCache):
        await cache.aset("aemoji_test", "Hello 🌍")
        assert await cache.aget("aemoji_test") == "Hello 🌍"

    @pytest.mark.asyncio
    async def test_achinese_characters(self, cache: KeyValueCache):
        await cache.aset("achinese", "你好世界")
        assert await cache.aget("achinese") == "你好世界"


class TestAsyncDataTypePersistence:
    """Tests verifying data types are preserved through serialization on async APIs."""

    @pytest.mark.asyncio
    async def test_ainteger_storage(self, cache: KeyValueCache):
        await cache.aset("aint_val", 99)
        result = await cache.aget("aint_val", "fallback")
        assert isinstance(result, int)
        assert result == 99

    @pytest.mark.asyncio
    async def test_alarge_string_storage(self, cache: KeyValueCache):
        large_content = "x" * 5000
        await cache.aset("alarge_str", large_content)
        result = await cache.aget("alarge_str")
        assert isinstance(result, str)
        assert len(result) == 5000
        assert result == large_content

    @pytest.mark.asyncio
    async def test_anumeric_string_stays_string(self, cache: KeyValueCache):
        await cache.aset("anum_str", "12345")
        result = await cache.aget("anum_str")
        assert isinstance(result, str)
        assert result == "12345"

    @pytest.mark.asyncio
    async def test_aaccented_string(self, cache: KeyValueCache):
        await cache.aset("aaccented", "café résumé")
        result = await cache.aget("aaccented")
        assert isinstance(result, str)
        assert result == "café résumé"

    @pytest.mark.asyncio
    async def test_adictionary_with_datetime(self, cache: KeyValueCache):
        if isinstance(cache._cache._serializers[0], JSONSerializer | MessagePackSerializer):
            timestamp: str | datetime.datetime = datetime.datetime.now().isoformat()
        else:
            timestamp = datetime.datetime.now()

        data = {"user_id": 42, "created": timestamp, "label": "Test"}
        await cache.aset("adict_data", data)
        result = await cache.aget("adict_data")

        assert isinstance(result, dict)
        assert result["user_id"] == 42
        assert result["label"] == "Test"
        assert result["created"] == timestamp

    @pytest.mark.asyncio
    async def test_afloat_precision(self, cache: KeyValueCache):
        precise_val = 3.141592653589793
        await cache.aset("api", precise_val)
        result = await cache.aget("api")
        assert isinstance(result, float)
        assert result == precise_val

    @pytest.mark.asyncio
    async def test_aboolean_true(self, cache: KeyValueCache):
        await cache.aset("aflag_on", True)
        result = await cache.aget("aflag_on")
        assert isinstance(result, bool)
        assert result is True

    @pytest.mark.asyncio
    async def test_aboolean_false(self, cache: KeyValueCache):
        await cache.aset("aflag_off", False)
        result = await cache.aget("aflag_off")
        assert isinstance(result, bool)
        assert result is False


class TestAsyncBulkGetOperationsExtra:
    """Additional aget_many() coverage."""

    @pytest.mark.asyncio
    async def test_aget_many_integers(self, cache: KeyValueCache):
        await cache.aset("ax", 10)
        await cache.aset("ay", 20)
        await cache.aset("az", 30)
        result = await cache.aget_many(["ax", "ay", "az"])
        assert result == {"ax": 10, "ay": 20, "az": 30}

    @pytest.mark.asyncio
    async def test_aget_many_strings(self, cache: KeyValueCache):
        await cache.aset("as1", "alpha")
        await cache.aset("as2", "beta")
        await cache.aset("as3", "gamma")
        result = await cache.aget_many(["as1", "as2", "as3"])
        assert result == {"as1": "alpha", "as2": "beta", "as3": "gamma"}


class TestAsyncBulkSetOperationsExtra:
    """Additional aset_many() coverage."""

    @pytest.mark.asyncio
    async def test_aset_many_and_retrieve(self, cache: KeyValueCache):
        await cache.aset_many({"am1": 100, "am2": 200, "am3": 300})
        result = await cache.aget_many(["am1", "am2", "am3"])
        assert result == {"am1": 100, "am2": 200, "am3": 300}


class TestAsyncDeleteOperationsExtra:
    """Additional adelete()/adelete_many() coverage."""

    @pytest.mark.asyncio
    async def test_adelete_returns_boolean(self, cache: KeyValueCache):
        await cache.aset("abool_del", "value")
        result = await cache.adelete("abool_del")
        assert isinstance(result, bool)
        assert result is True
        result = await cache.adelete("abool_del")
        assert isinstance(result, bool)
        assert result is False

    @pytest.mark.asyncio
    async def test_adelete_many_already_deleted(self, cache: KeyValueCache):
        await cache.adelete_many(["agone1", "agone2"])
        result = await cache.adelete_many(["agone1", "agone2"])
        assert bool(result) is False

    @pytest.mark.asyncio
    async def test_adelete_many_with_generator(self, cache: KeyValueCache):
        await cache.aset_many({"agen1": 1, "agen2": 2, "agen3": 3})
        result = await cache.adelete_many(k for k in ["agen1", "agen2"])  # type: ignore[arg-type]
        assert bool(result) is True
        remaining = await cache.aget_many(["agen1", "agen2", "agen3"])
        assert remaining == {"agen3": 3}

    @pytest.mark.asyncio
    async def test_adelete_many_empty_generator(self, cache: KeyValueCache):
        from typing import cast

        result = await cache.adelete_many(k for k in cast("list[str]", []))  # type: ignore[arg-type]
        assert bool(result) is False


class TestAsyncCloseOperation:
    """Tests for aclose() method."""

    @pytest.mark.asyncio
    async def test_aclose_with_setting(self, cache: KeyValueCache, settings: SettingsWrapper):
        settings.DJANGO_REDIS_CLOSE_CONNECTION = True
        await cache.aset("apre_close", "value")
        await cache.aclose()

    @pytest.mark.asyncio
    async def test_aclose_and_reconnect(self, cache: KeyValueCache):
        await cache.aset("areconnect_test", "before")
        await cache.aclose()
        await cache.aset("areconnect_test2", "after")
        assert await cache.aget("areconnect_test2") == "after"
