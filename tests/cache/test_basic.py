"""Tests for basic cache operations: set, get, add, delete, get_many, set_many."""

import datetime
from typing import TYPE_CHECKING, cast

import pytest

from django_cachex.exceptions import NotSupportedError
from django_cachex.serializers.json import JsonSerializer
from django_cachex.serializers.msgpack import MsgpackSerializer
from tests.fixtures.cache import server_version

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


def test_set_nx_creates_new_key(cache: RespCache):
    cache.delete("nx_key")
    assert cache.get("nx_key") is None

    result = cache.set("nx_key", 42, nx=True)
    assert result is True
    assert cache.get("nx_key") == 42


def test_set_nx_does_not_overwrite(cache: RespCache):
    cache.set("nx_existing", "original")
    result = cache.set("nx_existing", "changed", nx=True)
    assert result is False
    assert cache.get("nx_existing") == "original"


def test_set_nx_creates_again_after_delete(cache: RespCache):
    assert cache.set("nx_recreate", "temp", nx=True) is True
    cache.delete("nx_recreate")
    assert cache.get("nx_recreate") is None

    assert cache.set("nx_recreate", "second", nx=True) is True
    assert cache.get("nx_recreate") == "second"


def test_set_nx_get_raises_not_supported_before_redis_7(cache: RespCache):
    server, version = server_version(cache)
    if server == "valkey" or version >= (7, 0):
        pytest.skip("the server accepts SET NX GET")
    cache.set("nx_get_old_server", "original")
    with pytest.raises(NotSupportedError):
        cache.set("nx_get_old_server", "new", nx=True, get=True)
    assert cache.get("nx_get_old_server") == "original"


def test_set_get_returns_old_value(cache: RespCache):
    cache.set("get_key", "old_value")
    old = cache.set("get_key", "new_value", get=True)
    assert old == "old_value"
    assert cache.get("get_key") == "new_value"


def test_set_get_returns_none_for_missing_key(cache: RespCache):
    cache.delete("get_missing")
    old = cache.set("get_missing", "first_value", get=True)
    assert old is None
    assert cache.get("get_missing") == "first_value"


def test_set_get_with_different_types(cache: RespCache):
    cache.set("get_typed", 42)
    old = cache.set("get_typed", "now_a_string", get=True)
    assert old == 42
    assert cache.get("get_typed") == "now_a_string"


def test_set_get_with_timeout(cache: RespCache):
    cache.set("get_ttl", "original", timeout=None)
    old = cache.set("get_ttl", "updated", timeout=60, get=True)
    assert old == "original"
    ttl = cache.ttl("get_ttl")
    assert ttl is not None and ttl > 0


def test_set_get_preserves_none_vs_missing(cache: RespCache):
    cache.delete("get_chain")
    old1 = cache.set("get_chain", "a", get=True)
    assert old1 is None
    old2 = cache.set("get_chain", "b", get=True)
    assert old2 == "a"
    old3 = cache.set("get_chain", "c", get=True)
    assert old3 == "b"


def test_cyrillic_key(cache: RespCache):
    cache.set("ключ", "данные")
    assert cache.get("ключ") == "данные"


def test_emoji_value(cache: RespCache):
    cache.set("emoji_test", "Hello 🌍")
    assert cache.get("emoji_test") == "Hello 🌍"


def test_chinese_characters(cache: RespCache):
    cache.set("chinese", "你好世界")
    assert cache.get("chinese") == "你好世界"


def test_integer_storage(cache: RespCache):
    cache.set("int_val", 99)
    result = cache.get("int_val", "fallback")
    assert isinstance(result, int)
    assert result == 99


def test_large_string_storage(cache: RespCache):
    large_content = "x" * 5000
    cache.set("large_str", large_content)
    result = cache.get("large_str")
    assert isinstance(result, str)
    assert len(result) == 5000
    assert result == large_content


def test_numeric_string_stays_string(cache: RespCache):
    cache.set("num_str", "12345")
    result = cache.get("num_str")
    assert isinstance(result, str)
    assert result == "12345"


def test_accented_string(cache: RespCache):
    cache.set("accented", "café résumé")
    result = cache.get("accented")
    assert isinstance(result, str)
    assert result == "café résumé"


def test_dictionary_with_datetime(cache: RespCache):
    if isinstance(cache._serializers[0], JsonSerializer | MsgpackSerializer):
        timestamp: str | datetime.datetime = datetime.datetime.now().isoformat()
    else:
        timestamp = datetime.datetime.now()

    data = {"user_id": 42, "created": timestamp, "label": "Test"}
    cache.set("dict_data", data)
    result = cache.get("dict_data")

    assert isinstance(result, dict)
    assert result["user_id"] == 42
    assert result["label"] == "Test"
    assert result["created"] == timestamp


def test_float_precision(cache: RespCache):
    precise_val = 3.141592653589793
    cache.set("pi", precise_val)
    result = cache.get("pi")
    assert isinstance(result, float)
    assert result == precise_val


def test_boolean_true(cache: RespCache):
    cache.set("flag_on", True)
    result = cache.get("flag_on")
    assert isinstance(result, bool)
    assert result is True


def test_boolean_false(cache: RespCache):
    cache.set("flag_off", False)
    result = cache.get("flag_off")
    assert isinstance(result, bool)
    assert result is False


def test_add_fails_for_existing_key(cache: RespCache):
    cache.set("preexisting", "first")
    result = cache.add("preexisting", "second")
    assert result is False
    assert cache.get("preexisting") == "first"


def test_add_succeeds_for_new_key(cache: RespCache):
    cache.delete("fresh_key")
    result = cache.add("fresh_key", "new_value")
    assert result is True
    assert cache.get("fresh_key") == "new_value"


def test_get_many_integers(cache: RespCache):
    cache.set("x", 10)
    cache.set("y", 20)
    cache.set("z", 30)
    result = cache.get_many(["x", "y", "z"])
    assert result == {"x": 10, "y": 20, "z": 30}


def test_get_many_strings(cache: RespCache):
    cache.set("s1", "alpha")
    cache.set("s2", "beta")
    cache.set("s3", "gamma")
    result = cache.get_many(["s1", "s2", "s3"])
    assert result == {"s1": "alpha", "s2": "beta", "s3": "gamma"}


def test_get_many_partial_match(cache: RespCache):
    cache.set("found", "yes")
    cache.delete("missing")
    result = cache.get_many(["found", "missing"])
    assert result == {"found": "yes"}


def test_set_many_and_retrieve(cache: RespCache):
    cache.set_many({"m1": 100, "m2": 200, "m3": 300})
    result = cache.get_many(["m1", "m2", "m3"])
    assert result == {"m1": 100, "m2": 200, "m3": 300}


def test_pipeline_set_returns_true(cache: RespCache):
    pipe = cache.pipeline()
    pipe.set("pipe_test", "pipe_val")
    results = pipe.execute()

    assert results == [True]
    assert cache.get("pipe_test") == "pipe_val"


def test_delete_existing_key(cache: RespCache):
    cache.set_many({"d1": 1, "d2": 2, "d3": 3})
    result = cache.delete("d1")
    assert result is True
    remaining = cache.get_many(["d1", "d2", "d3"])
    assert remaining == {"d2": 2, "d3": 3}


def test_delete_nonexistent_key(cache: RespCache):
    cache.delete("surely_missing")
    result = cache.delete("surely_missing")
    assert result is False


def test_delete_returns_boolean(cache: RespCache):
    cache.set("bool_del", "value")
    result = cache.delete("bool_del")
    assert isinstance(result, bool)
    assert result is True
    result = cache.delete("bool_del")
    assert isinstance(result, bool)
    assert result is False


def test_delete_many_removes_multiple(cache: RespCache):
    cache.set_many({"dm1": 1, "dm2": 2, "dm3": 3})
    result = cache.delete_many(["dm1", "dm2"])
    assert bool(result) is True
    remaining = cache.get_many(["dm1", "dm2", "dm3"])
    assert remaining == {"dm3": 3}


def test_delete_many_already_deleted(cache: RespCache):
    cache.delete_many(["gone1", "gone2"])
    result = cache.delete_many(["gone1", "gone2"])
    assert bool(result) is False


def test_delete_many_with_generator(cache: RespCache):
    cache.set_many({"gen1": 1, "gen2": 2, "gen3": 3})
    result = cache.delete_many(k for k in ["gen1", "gen2"])
    assert bool(result) is True
    remaining = cache.get_many(["gen1", "gen2", "gen3"])
    assert remaining == {"gen3": 3}


def test_delete_many_empty_generator(cache: RespCache):
    result = cache.delete_many(k for k in cast("list[str]", []))
    assert bool(result) is False


def test_clear_removes_all(cache: RespCache):
    cache.set("to_clear", "exists")
    assert cache.get("to_clear") == "exists"
    cache.clear()
    assert cache.get("to_clear") is None


def test_close_and_reconnect(cache: RespCache):
    cache.set("reconnect_test", "before")
    cache.close()
    cache.set("reconnect_test2", "after")
    assert cache.get("reconnect_test2") == "after"
    assert cache.get("reconnect_test") == "before"


@pytest.mark.asyncio
async def test_aadd_succeeds_for_new_key(cache: RespCache):
    cache.delete("async_add_new")
    result = await cache.aadd("async_add_new", "new_value")
    assert result is True
    assert cache.get("async_add_new") == "new_value"


@pytest.mark.asyncio
async def test_aadd_fails_for_existing_key(cache: RespCache):
    cache.set("async_add_existing", "original")
    result = await cache.aadd("async_add_existing", "new_value")
    assert result is False
    assert cache.get("async_add_existing") == "original"


@pytest.mark.asyncio
async def test_aadd_with_timeout(cache: RespCache):
    cache.delete("async_add_timeout")
    result = await cache.aadd("async_add_timeout", "timed_value", timeout=60)
    assert result is True
    ttl = cache.ttl("async_add_timeout")
    assert ttl is not None and ttl > 0


@pytest.mark.asyncio
async def test_aset_and_get(cache: RespCache):
    await cache.aset("async_set_key", "async_set_value")
    result = cache.get("async_set_key")
    assert result == "async_set_value"


@pytest.mark.asyncio
async def test_aset_with_timeout(cache: RespCache):
    await cache.aset("async_timeout_key", "timeout_value", timeout=60)
    result = cache.get("async_timeout_key")
    assert result == "timeout_value"
    ttl = cache.ttl("async_timeout_key")
    assert ttl is not None and ttl > 0


@pytest.mark.asyncio
async def test_aset_overwrites_existing(cache: RespCache):
    cache.set("async_overwrite", "original")
    await cache.aset("async_overwrite", "updated")
    result = cache.get("async_overwrite")
    assert result == "updated"


@pytest.mark.asyncio
async def test_aset_complex_value(cache: RespCache):
    data = {"items": [1, 2, 3], "nested": {"key": "value"}}
    await cache.aset("async_complex_set", data)
    result = cache.get("async_complex_set")
    assert result == data


@pytest.mark.asyncio
async def test_aset_with_version(cache: RespCache):
    await cache.aset("versioned_set", "v1_data", version=1)
    await cache.aset("versioned_set", "v2_data", version=2)

    result_v1 = cache.get("versioned_set", version=1)
    result_v2 = cache.get("versioned_set", version=2)

    assert result_v1 == "v1_data"
    assert result_v2 == "v2_data"


@pytest.mark.asyncio
async def test_adelete_existing_key(cache: RespCache):
    cache.set("async_delete_key", "to_delete")
    assert cache.get("async_delete_key") == "to_delete"

    result = await cache.adelete("async_delete_key")
    assert result is True
    assert cache.get("async_delete_key") is None


@pytest.mark.asyncio
async def test_adelete_nonexistent_key(cache: RespCache):
    cache.delete("nonexistent_async_key")
    result = await cache.adelete("nonexistent_async_key")
    assert result is False


@pytest.mark.asyncio
async def test_adelete_with_version(cache: RespCache):
    cache.set("versioned_delete", "v1_data", version=1)
    cache.set("versioned_delete", "v2_data", version=2)

    await cache.adelete("versioned_delete", version=1)

    assert cache.get("versioned_delete", version=1) is None
    assert cache.get("versioned_delete", version=2) == "v2_data"


@pytest.mark.asyncio
async def test_adelete_returns_boolean(cache: RespCache):
    await cache.aset("abool_del", "value")
    result = await cache.adelete("abool_del")
    assert isinstance(result, bool)
    assert result is True
    result = await cache.adelete("abool_del")
    assert isinstance(result, bool)
    assert result is False


@pytest.mark.asyncio
async def test_ahas_key_exists(cache: RespCache):
    cache.set("async_has_key", "value")
    result = await cache.ahas_key("async_has_key")
    assert result is True


@pytest.mark.asyncio
async def test_ahas_key_missing(cache: RespCache):
    cache.delete("async_missing_key")
    result = await cache.ahas_key("async_missing_key")
    assert result is False


@pytest.mark.asyncio
async def test_aincr_increments(cache: RespCache):
    cache.set("async_counter", 10)
    result = await cache.aincr("async_counter")
    assert result == 11
    assert cache.get("async_counter") == 11


@pytest.mark.asyncio
async def test_aincr_by_amount(cache: RespCache):
    cache.set("async_counter2", 5)
    result = await cache.aincr("async_counter2", 10)
    assert result == 15


@pytest.mark.asyncio
async def test_aincr_missing_key_creates_it(cache: RespCache):
    cache.delete("async_missing_counter")
    result = await cache.aincr("async_missing_counter")
    assert result == 1


@pytest.mark.asyncio
async def test_adecr_decrements(cache: RespCache):
    cache.set("async_decr", 10)
    result = await cache.adecr("async_decr")
    assert result == 9


@pytest.mark.asyncio
async def test_aget_many_retrieves_multiple(cache: RespCache):
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
async def test_aget_many_integers(cache: RespCache):
    await cache.aset("ax", 10)
    await cache.aset("ay", 20)
    await cache.aset("az", 30)
    assert await cache.aget_many(["ax", "ay", "az"]) == {"ax": 10, "ay": 20, "az": 30}


@pytest.mark.asyncio
async def test_aget_many_partial_match(cache: RespCache):
    cache.set("async_partial_a", "a")
    cache.delete("async_partial_b")

    result = await cache.aget_many(["async_partial_a", "async_partial_b"])
    assert result == {"async_partial_a": "a"}


@pytest.mark.asyncio
async def test_aset_many_stores_multiple(cache: RespCache):
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


@pytest.mark.asyncio
async def test_adelete_many_removes_multiple(cache: RespCache):
    cache.set_many({"async_del_1": 1, "async_del_2": 2, "async_del_3": 3})

    result = await cache.adelete_many(["async_del_1", "async_del_2"])
    assert result == 2

    assert cache.get("async_del_1") is None
    assert cache.get("async_del_2") is None
    assert cache.get("async_del_3") == 3


@pytest.mark.asyncio
async def test_adelete_many_already_deleted(cache: RespCache):
    await cache.adelete_many(["agone1", "agone2"])
    result = await cache.adelete_many(["agone1", "agone2"])
    assert bool(result) is False


@pytest.mark.asyncio
async def test_adelete_many_with_generator(cache: RespCache):
    await cache.aset_many({"agen1": 1, "agen2": 2, "agen3": 3})
    result = await cache.adelete_many(k for k in ["agen1", "agen2"])
    assert bool(result) is True
    remaining = await cache.aget_many(["agen1", "agen2", "agen3"])
    assert remaining == {"agen3": 3}


@pytest.mark.asyncio
async def test_adelete_many_empty_generator(cache: RespCache):
    result = await cache.adelete_many(k for k in cast("list[str]", []))
    assert bool(result) is False


@pytest.mark.asyncio
async def test_aclear_removes_all(cache: RespCache):
    cache.set("async_clear_key", "value")
    assert cache.get("async_clear_key") == "value"

    result = await cache.aclear()
    assert result is True
    assert cache.get("async_clear_key") is None


@pytest.mark.asyncio
async def test_aget_existing_key(cache: RespCache):
    cache.set("async_key", "async_value")
    result = await cache.aget("async_key")
    assert result == "async_value"


@pytest.mark.asyncio
async def test_aget_missing_key_returns_default(cache: RespCache):
    cache.delete("missing_async_key")
    result = await cache.aget("missing_async_key")
    assert result is None


@pytest.mark.asyncio
async def test_aget_missing_key_with_custom_default(cache: RespCache):
    cache.delete("missing_async_key2")
    result = await cache.aget("missing_async_key2", default="fallback")
    assert result == "fallback"


@pytest.mark.asyncio
async def test_aget_complex_value(cache: RespCache):
    data = {"user": "alice", "scores": [10, 20, 30], "active": True}
    cache.set("async_complex", data)
    result = await cache.aget("async_complex")
    assert result == data


@pytest.mark.asyncio
async def test_aget_with_version(cache: RespCache):
    cache.set("versioned_key", "v1_value", version=1)
    cache.set("versioned_key", "v2_value", version=2)

    result_v1 = await cache.aget("versioned_key", version=1)
    result_v2 = await cache.aget("versioned_key", version=2)

    assert result_v1 == "v1_value"
    assert result_v2 == "v2_value"


@pytest.mark.asyncio
async def test_aset_nx_creates_new_key(cache: RespCache):
    await cache.adelete("anx_key")
    assert await cache.aget("anx_key") is None

    result = await cache.aset("anx_key", 42, nx=True)
    assert result is True
    assert await cache.aget("anx_key") == 42


@pytest.mark.asyncio
async def test_aset_nx_does_not_overwrite(cache: RespCache):
    await cache.aset("anx_existing", "original")
    result = await cache.aset("anx_existing", "changed", nx=True)
    assert result is False
    assert await cache.aget("anx_existing") == "original"


@pytest.mark.asyncio
async def test_aset_nx_creates_again_after_delete(cache: RespCache):
    assert await cache.aset("anx_recreate", "temp", nx=True) is True
    await cache.adelete("anx_recreate")
    assert await cache.aget("anx_recreate") is None

    assert await cache.aset("anx_recreate", "second", nx=True) is True
    assert await cache.aget("anx_recreate") == "second"


@pytest.mark.asyncio
async def test_aset_nx_get_raises_not_supported_before_redis_7(cache: RespCache):
    server, version = server_version(cache)
    if server == "valkey" or version >= (7, 0):
        pytest.skip("the server accepts SET NX GET")
    await cache.aset("anx_get_old_server", "original")
    with pytest.raises(NotSupportedError):
        await cache.aset("anx_get_old_server", "new", nx=True, get=True)
    assert await cache.aget("anx_get_old_server") == "original"


@pytest.mark.asyncio
async def test_aset_get_returns_old_value(cache: RespCache):
    await cache.aset("aget_key", "old_value")
    old = await cache.aset("aget_key", "new_value", get=True)
    assert old == "old_value"
    assert await cache.aget("aget_key") == "new_value"


@pytest.mark.asyncio
async def test_aset_get_returns_none_for_missing_key(cache: RespCache):
    await cache.adelete("aget_missing")
    old = await cache.aset("aget_missing", "first_value", get=True)
    assert old is None
    assert await cache.aget("aget_missing") == "first_value"


@pytest.mark.asyncio
async def test_aset_get_with_different_types(cache: RespCache):
    await cache.aset("aget_typed", 42)
    old = await cache.aset("aget_typed", "now_a_string", get=True)
    assert old == 42
    assert await cache.aget("aget_typed") == "now_a_string"


@pytest.mark.asyncio
async def test_aset_get_with_timeout(cache: RespCache):
    await cache.aset("aget_ttl", "original", timeout=None)
    old = await cache.aset("aget_ttl", "updated", timeout=60, get=True)
    assert old == "original"
    ttl = await cache.attl("aget_ttl")
    assert ttl is not None and ttl > 0


@pytest.mark.asyncio
async def test_aset_get_preserves_none_vs_missing(cache: RespCache):
    await cache.adelete("aget_chain")
    old1 = await cache.aset("aget_chain", "a", get=True)
    assert old1 is None
    old2 = await cache.aset("aget_chain", "b", get=True)
    assert old2 == "a"
    old3 = await cache.aset("aget_chain", "c", get=True)
    assert old3 == "b"


@pytest.mark.asyncio
async def test_acyrillic_key(cache: RespCache):
    await cache.aset("ключ", "данные")
    assert await cache.aget("ключ") == "данные"


@pytest.mark.asyncio
async def test_aemoji_value(cache: RespCache):
    await cache.aset("aemoji_test", "Hello 🌍")
    assert await cache.aget("aemoji_test") == "Hello 🌍"


@pytest.mark.asyncio
async def test_achinese_characters(cache: RespCache):
    await cache.aset("achinese", "你好世界")
    assert await cache.aget("achinese") == "你好世界"


@pytest.mark.asyncio
async def test_ainteger_storage(cache: RespCache):
    await cache.aset("aint_val", 99)
    result = await cache.aget("aint_val", "fallback")
    assert isinstance(result, int)
    assert result == 99


@pytest.mark.asyncio
async def test_alarge_string_storage(cache: RespCache):
    large_content = "x" * 5000
    await cache.aset("alarge_str", large_content)
    result = await cache.aget("alarge_str")
    assert isinstance(result, str)
    assert len(result) == 5000
    assert result == large_content


@pytest.mark.asyncio
async def test_anumeric_string_stays_string(cache: RespCache):
    await cache.aset("anum_str", "12345")
    result = await cache.aget("anum_str")
    assert isinstance(result, str)
    assert result == "12345"


@pytest.mark.asyncio
async def test_aaccented_string(cache: RespCache):
    await cache.aset("aaccented", "café résumé")
    result = await cache.aget("aaccented")
    assert isinstance(result, str)
    assert result == "café résumé"


@pytest.mark.asyncio
async def test_adictionary_with_datetime(cache: RespCache):
    if isinstance(cache._serializers[0], JsonSerializer | MsgpackSerializer):
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
async def test_afloat_precision(cache: RespCache):
    precise_val = 3.141592653589793
    await cache.aset("api", precise_val)
    result = await cache.aget("api")
    assert isinstance(result, float)
    assert result == precise_val


@pytest.mark.asyncio
async def test_aboolean_true(cache: RespCache):
    await cache.aset("aflag_on", True)
    result = await cache.aget("aflag_on")
    assert isinstance(result, bool)
    assert result is True


@pytest.mark.asyncio
async def test_aboolean_false(cache: RespCache):
    await cache.aset("aflag_off", False)
    result = await cache.aget("aflag_off")
    assert isinstance(result, bool)
    assert result is False


@pytest.mark.asyncio
async def test_aclose_and_reconnect(cache: RespCache):
    await cache.aset("areconnect_test", "before")
    await cache.aclose()
    await cache.aset("areconnect_test2", "after")
    assert await cache.aget("areconnect_test2") == "after"
    assert await cache.aget("areconnect_test") == "before"
