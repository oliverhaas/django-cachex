"""Tests for key operations: version, delete_pattern, iter_keys, etc."""

import copy
from typing import TYPE_CHECKING

import pytest
from django.conf import settings
from django.core.cache import caches
from django.test import override_settings

from django_cachex.exceptions import CachexError, KeyNotFoundError

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


def test_version(cache: RespCache):
    cache.set("keytest", 2, version=2)
    res = cache.get("keytest")
    assert res is None

    res = cache.get("keytest", version=2)
    assert res == 2


def test_incr_version(cache: RespCache):
    # Use hash tag so versioned keys stay in same cluster slot
    cache.set("{keytest}", 2)
    cache.incr_version("{keytest}")

    res = cache.get("{keytest}")
    assert res is None

    res = cache.get("{keytest}", version=2)
    assert res == 2


def test_ttl_incr_version_no_timeout(cache: RespCache):
    # Use hash tag so versioned keys stay in same cluster slot
    cache.set("{my_key}", "hello world!", timeout=None)

    cache.incr_version("{my_key}")

    my_value = cache.get("{my_key}", version=2)

    assert my_value == "hello world!"
    assert cache.ttl("{my_key}", version=2) is None


# Use {slot}: prefix to ensure both keys hash to same cluster slot
def test_rename(cache: RespCache):
    cache.set("{slot}:src", "value1")
    cache.rename("{slot}:src", "{slot}:dest")

    assert cache.get("{slot}:src") is None
    assert cache.get("{slot}:dest") == "value1"


def test_rename_overwrites_existing(cache: RespCache):
    cache.set("{slot2}:src", "src_value")
    cache.set("{slot2}:dst", "dst_value")
    cache.rename("{slot2}:src", "{slot2}:dst")

    assert cache.get("{slot2}:src") is None
    assert cache.get("{slot2}:dst") == "src_value"


def test_rename_preserves_ttl(cache: RespCache):
    cache.set("{slot3}:key", "value", timeout=3600)
    cache.rename("{slot3}:key", "{slot3}:dest")

    ttl = cache.ttl("{slot3}:dest")
    assert ttl is not None
    assert ttl > 3500  # Should be close to 3600


def test_rename_nonexistent_raises(cache: RespCache):
    with pytest.raises(KeyNotFoundError, match="not found"):
        cache.rename("{slot4}:nonexistent", "{slot4}:dest")


def test_rename_nonexistent_is_catchable_as_value_error(cache: RespCache):
    with pytest.raises(ValueError):
        cache.rename("{slot4}:nonexistent", "{slot4}:dest")


def test_key_not_found_error_hierarchy():
    err = KeyNotFoundError("k")

    assert isinstance(err, CachexError)
    assert isinstance(err, ValueError)
    assert err.key == "k"
    assert str(err) == "Key 'k' not found"


def test_renamenx(cache: RespCache):
    cache.set("{slot5}:src", "value")
    result = cache.renamenx("{slot5}:src", "{slot5}:dest")

    assert result is True
    assert cache.get("{slot5}:src") is None
    assert cache.get("{slot5}:dest") == "value"


def test_renamenx_fails_if_dest_exists(cache: RespCache):
    cache.set("{slot6}:src", "src_value")
    cache.set("{slot6}:dest", "existing_value")
    result = cache.renamenx("{slot6}:src", "{slot6}:dest")

    assert result is False
    assert cache.get("{slot6}:src") == "src_value"
    assert cache.get("{slot6}:dest") == "existing_value"


def test_renamenx_missing_source_returns_false(cache: RespCache):
    result = cache.renamenx("{slot7}:nonexistent", "{slot7}:dest")

    assert result is False
    assert cache.get("{slot7}:dest") is None


def test_rename_version_src_dst(cache: RespCache):
    cache.set("{vs}:rsrc", "value", version=1)

    cache.rename("{vs}:rsrc", "{vs}:rdst", version_src=1, version_dst=2)
    assert cache.get("{vs}:rsrc", version=1) is None
    assert cache.get("{vs}:rdst", version=2) == "value"


def test_renamenx_version_src_dst(cache: RespCache):
    cache.set("{vs}:rnxsrc", "value", version=1)

    result = cache.renamenx("{vs}:rnxsrc", "{vs}:rnxdst", version_src=1, version_dst=2)
    assert result is True
    assert cache.get("{vs}:rnxsrc", version=1) is None
    assert cache.get("{vs}:rnxdst", version=2) == "value"


def test_delete_pattern(cache: RespCache):
    for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
        cache.set(key, "foo")

    res = cache.delete_pattern("*foo-a*")
    assert bool(res) is True

    keys = cache.keys("foo*")
    assert set(keys) == {"foo-bb", "foo-bc"}

    res = cache.delete_pattern("*foo-a*")
    assert bool(res) is False


def test_delete_pattern_with_custom_count(cache: RespCache):
    for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
        cache.set(key, "foo")

    res = cache.delete_pattern("*foo-a*", itersize=2)
    assert bool(res) is True

    keys = cache.keys("foo*")
    assert set(keys) == {"foo-bb", "foo-bc"}


def test_delete_pattern_itersize_smaller_than_match_count(cache: RespCache):
    """An itersize below the number of matches still deletes every match."""
    matching = [f"itersize-foo-{i}" for i in range(12)]
    for key in [*matching, "itersize-bar"]:
        cache.set(key, "foo")

    res = cache.delete_pattern("*itersize-foo-*", itersize=1)
    assert res == len(matching)

    assert cache.keys("itersize-*") == ["itersize-bar"]


def test_delete_pattern_deletes_an_undecodable_name_and_not_its_escaped_spelling(cache: RespCache):
    cache.set("dpbad_\\xff", "escaped spelling")
    cache.get_client(write=True).set(cache.make_key("dpbad_").encode() + b"\xff", b"raw")

    assert cache.delete_pattern("dpbad_?") == 1
    assert cache.keys("dpbad_*") == ["dpbad_\\xff"]


def test_glob_characters_in_key_prefix_match_literally(cache: RespCache):
    """A prefix with glob metacharacters is escaped in patterns, and a backslash escapes itself."""
    config = copy.deepcopy(settings.CACHES)
    config["escaped"] = {**config["default"], "KEY_PREFIX": "pre\\fix*?[x]"}
    config["decoy"] = {**config["default"], "KEY_PREFIX": "prefix*?[x]"}
    with override_settings(CACHES=config):
        escaped, decoy = caches["escaped"], caches["decoy"]
        escaped.set("esc-a", 1)
        escaped.set("esc-b", 2)
        decoy.set("esc-c", 3)

        assert set(escaped.keys("esc-*")) == {"esc-a", "esc-b"}
        assert escaped.delete_pattern("esc-*") == 2
        assert decoy.keys("esc-*") == ["esc-c"]


def test_iter_keys(cache: RespCache):
    cache.set("foo1", 1)
    cache.set("foo2", 1)
    cache.set("foo3", 1)

    result = set(cache.iter_keys("foo*"))
    assert result == {"foo1", "foo2", "foo3"}


def test_iter_keys_itersize(cache: RespCache):
    cache.set("foo1", 1)
    cache.set("foo2", 1)
    cache.set("foo3", 1)

    result = list(cache.iter_keys("foo*", itersize=2))
    assert sorted(result) == ["foo1", "foo2", "foo3"]


def test_iter_keys_generator(cache: RespCache):
    cache.set("foo1", 1)
    cache.set("foo2", 1)
    cache.set("foo3", 1)

    result = cache.iter_keys("foo*")
    assert next(result) in {"foo1", "foo2", "foo3"}


@pytest.mark.asyncio
async def test_aversion(cache: RespCache):
    await cache.aset("akeytest", 2, version=2)
    res = await cache.aget("akeytest")
    assert res is None

    res = await cache.aget("akeytest", version=2)
    assert res == 2


@pytest.mark.asyncio
async def test_aincr_version(cache: RespCache):
    await cache.aset("{akeytest}", 2)
    await cache.aincr_version("{akeytest}")

    res = await cache.aget("{akeytest}")
    assert res is None

    res = await cache.aget("{akeytest}", version=2)
    assert res == 2


@pytest.mark.asyncio
async def test_attl_aincr_version_no_timeout(cache: RespCache):
    await cache.aset("{amy_key}", "hello world!", timeout=None)

    await cache.aincr_version("{amy_key}")

    my_value = await cache.aget("{amy_key}", version=2)

    assert my_value == "hello world!"
    assert await cache.attl("{amy_key}", version=2) is None


@pytest.mark.asyncio
async def test_akeys_returns_matching(cache: RespCache):
    cache.set("akeys_foo1", 1)
    cache.set("akeys_foo2", 2)
    cache.set("akeys_bar1", 3)

    keys = await cache.akeys("akeys_foo*")
    assert sorted(keys) == ["akeys_foo1", "akeys_foo2"]


@pytest.mark.asyncio
async def test_aiter_keys(cache: RespCache):
    cache.set("aikeys_foo1", 1)
    cache.set("aikeys_foo2", 2)
    cache.set("aikeys_foo3", 3)

    result = set()
    async for key in cache.aiter_keys("aikeys_foo*"):
        result.add(key)
    assert result == {"aikeys_foo1", "aikeys_foo2", "aikeys_foo3"}


@pytest.mark.asyncio
async def test_aiter_keys_with_itersize(cache: RespCache):
    cache.set("aikeys2_foo1", 1)
    cache.set("aikeys2_foo2", 2)
    cache.set("aikeys2_foo3", 3)

    result = [key async for key in cache.aiter_keys("aikeys2_foo*", itersize=2)]
    assert sorted(result) == ["aikeys2_foo1", "aikeys2_foo2", "aikeys2_foo3"]


@pytest.mark.asyncio
async def test_aiter_keys_async_generator(cache: RespCache):
    cache.set("aikgen_foo1", 1)
    cache.set("aikgen_foo2", 1)
    cache.set("aikgen_foo3", 1)

    result = cache.aiter_keys("aikgen_foo*")
    assert await anext(result) in {"aikgen_foo1", "aikgen_foo2", "aikgen_foo3"}


@pytest.mark.asyncio
async def test_keys_and_iter_keys_return_an_undecodable_name_as_scan_does(cache: RespCache):
    cache.set("lsbad_\\xff", "escaped spelling")
    cache.get_client(write=True).set(cache.make_key("lsbad_").encode() + b"\xff", b"raw")
    expected = {"lsbad_\\xff", "lsbad_\udcff"}

    assert set(cache.keys("lsbad_*")) == expected
    assert set(cache.iter_keys("lsbad_*")) == expected
    assert set(await cache.akeys("lsbad_*")) == expected
    assert {key async for key in cache.aiter_keys("lsbad_*")} == expected


@pytest.mark.asyncio
async def test_adelete_pattern(cache: RespCache):
    for key in ["adp_foo-aa", "adp_foo-ab", "adp_foo-bb", "adp_foo-bc"]:
        cache.set(key, "foo")

    result = await cache.adelete_pattern("adp_foo-a*")
    assert bool(result) is True

    keys = cache.keys("adp_foo*")
    assert set(keys) == {"adp_foo-bb", "adp_foo-bc"}


@pytest.mark.asyncio
async def test_adelete_pattern_no_match(cache: RespCache):
    result = await cache.adelete_pattern("nonexistent_pattern_xyz*")
    assert bool(result) is False


@pytest.mark.asyncio
async def test_adelete_pattern_with_custom_count(cache: RespCache):
    for key in ["afoo-aa", "afoo-ab", "afoo-bb", "afoo-bc"]:
        cache.set(key, "foo")

    res = await cache.adelete_pattern("*afoo-a*", itersize=2)
    assert bool(res) is True

    keys = cache.keys("afoo*")
    assert set(keys) == {"afoo-bb", "afoo-bc"}


@pytest.mark.asyncio
async def test_adelete_pattern_itersize_smaller_than_match_count(cache: RespCache):
    """An itersize below the number of matches still deletes every match."""
    matching = [f"aitersize-foo-{i}" for i in range(12)]
    for key in [*matching, "aitersize-bar"]:
        cache.set(key, "foo")

    res = await cache.adelete_pattern("*aitersize-foo-*", itersize=1)
    assert res == len(matching)

    assert cache.keys("aitersize-*") == ["aitersize-bar"]


@pytest.mark.asyncio
async def test_adelete_pattern_deletes_an_undecodable_name_and_not_its_escaped_spelling(cache: RespCache):
    cache.set("adpbad_\\xff", "escaped spelling")
    cache.get_client(write=True).set(cache.make_key("adpbad_").encode() + b"\xff", b"raw")

    assert await cache.adelete_pattern("adpbad_?") == 1
    assert cache.keys("adpbad_*") == ["adpbad_\\xff"]


@pytest.mark.asyncio
async def test_arename(cache: RespCache):
    cache.set("{aslot}:src", "value1")
    await cache.arename("{aslot}:src", "{aslot}:dest")

    assert cache.get("{aslot}:src") is None
    assert cache.get("{aslot}:dest") == "value1"


@pytest.mark.asyncio
async def test_arename_overwrites_existing(cache: RespCache):
    cache.set("{aslot2}:src", "src_value")
    cache.set("{aslot2}:dst", "dst_value")
    await cache.arename("{aslot2}:src", "{aslot2}:dst")

    assert cache.get("{aslot2}:src") is None
    assert cache.get("{aslot2}:dst") == "src_value"


@pytest.mark.asyncio
async def test_arename_nonexistent_raises(cache: RespCache):
    with pytest.raises(KeyNotFoundError, match="not found"):
        await cache.arename("{aslot3}:nonexistent", "{aslot3}:dest")


@pytest.mark.asyncio
async def test_arenamenx(cache: RespCache):
    cache.set("{aslot4}:src", "value")
    result = await cache.arenamenx("{aslot4}:src", "{aslot4}:dest")

    assert result is True
    assert cache.get("{aslot4}:src") is None
    assert cache.get("{aslot4}:dest") == "value"


@pytest.mark.asyncio
async def test_arenamenx_fails_if_dest_exists(cache: RespCache):
    cache.set("{aslot5}:src", "src_value")
    cache.set("{aslot5}:dest", "existing_value")
    result = await cache.arenamenx("{aslot5}:src", "{aslot5}:dest")

    assert result is False
    assert cache.get("{aslot5}:src") == "src_value"
    assert cache.get("{aslot5}:dest") == "existing_value"


@pytest.mark.asyncio
async def test_arenamenx_missing_source_returns_false(cache: RespCache):
    result = await cache.arenamenx("{aslot6}:nonexistent", "{aslot6}:dest")

    assert result is False
    assert cache.get("{aslot6}:dest") is None


@pytest.mark.asyncio
async def test_arename_version_src_dst(cache: RespCache):
    cache.set("{vs}:arsrc", "value", version=1)

    await cache.arename("{vs}:arsrc", "{vs}:ardst", version_src=1, version_dst=2)
    assert cache.get("{vs}:arsrc", version=1) is None
    assert cache.get("{vs}:ardst", version=2) == "value"


@pytest.mark.asyncio
async def test_arenamenx_version_src_dst(cache: RespCache):
    cache.set("{vs}:arnxsrc", "value", version=1)

    result = await cache.arenamenx("{vs}:arnxsrc", "{vs}:arnxdst", version_src=1, version_dst=2)
    assert result is True
    assert cache.get("{vs}:arnxsrc", version=1) is None
    assert cache.get("{vs}:arnxdst", version=2) == "value"


@pytest.mark.asyncio
async def test_arename_preserves_ttl(cache: RespCache):
    await cache.aset("{aslotttl}:key", "value", timeout=3600)
    await cache.arename("{aslotttl}:key", "{aslotttl}:dest")

    ttl = await cache.attl("{aslotttl}:dest")
    assert ttl is not None
    assert ttl > 3500


@pytest.mark.parametrize(
    ("setup", "expected"),
    [
        (lambda c: c.set("type_string", "value"), "string"),
        (lambda c: c.hset("type_hash", "field", "value"), "hash"),
        (lambda c: c.lpush("type_list", "value"), "list"),
        (lambda c: c.sadd("type_set", "value"), "set"),
        (lambda c: c.zadd("type_zset", {"member": 1.0}), "zset"),
    ],
    ids=["string", "hash", "list", "set", "zset"],
)
def test_type_returns_kind(cache: RespCache, setup, expected: str):
    setup(cache)
    assert cache.type(f"type_{expected}") == expected


def test_type_missing_key(cache: RespCache):
    assert cache.type("type_missing") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("setup", "expected"),
    [
        (lambda c: c.set("atype_string", "value"), "string"),
        (lambda c: c.hset("atype_hash", "field", "value"), "hash"),
        (lambda c: c.lpush("atype_list", "value"), "list"),
        (lambda c: c.sadd("atype_set", "value"), "set"),
        (lambda c: c.zadd("atype_zset", {"member": 1.0}), "zset"),
    ],
    ids=["string", "hash", "list", "set", "zset"],
)
async def test_atype_returns_kind(cache: RespCache, setup, expected: str):
    setup(cache)
    assert await cache.atype(f"atype_{expected}") == expected


@pytest.mark.asyncio
async def test_atype_missing_key(cache: RespCache):
    assert await cache.atype("atype_missing") is None
