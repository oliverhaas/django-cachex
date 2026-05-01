"""Async smoke tests: verify each major family's async variant returns a working awaitable."""

import pytest


@pytest.mark.asyncio
async def test_async_get_set(driver):
    await driver.aset("k", b"v")
    assert await driver.aget("k") == b"v"


@pytest.mark.asyncio
async def test_async_mget(driver):
    await driver.apipeline_set([("a", b"1"), ("b", b"2")])
    assert await driver.amget(["a", "b", "missing"]) == [b"1", b"2", None]


@pytest.mark.asyncio
async def test_async_hset_hgetall(driver):
    await driver.ahset("h", "a", b"1")
    await driver.ahset("h", "b", b"2")
    result = await driver.ahgetall("h")
    assert result == {b"a": b"1", b"b": b"2"}


@pytest.mark.asyncio
async def test_async_sadd_smembers(driver):
    await driver.asadd("s", [b"x", b"y", b"z"])
    members = await driver.asmembers("s")
    assert sorted(members) == [b"x", b"y", b"z"]


@pytest.mark.asyncio
async def test_async_zadd_zrange(driver):
    await driver.azadd("z", [(b"a", 1.0), (b"b", 2.0)])
    assert await driver.azrange("z", 0, -1) == [b"a", b"b"]


@pytest.mark.asyncio
async def test_async_xadd_xlen(driver):
    await driver.axadd("s", "*", [("f", b"v")])
    assert await driver.axlen("s") == 1


@pytest.mark.asyncio
async def test_async_lpush_lrange(driver):
    await driver.arpush("l", [b"a", b"b", b"c"])
    assert await driver.alrange("l", 0, -1) == [b"a", b"b", b"c"]


@pytest.mark.asyncio
async def test_async_eval(driver):
    result = await driver.aeval("return tonumber(ARGV[1])", [], [b"7"])
    assert result == 7


@pytest.mark.asyncio
async def test_async_lock(driver):
    assert await driver.alock_acquire("lock", "tok", 5000) is True
    assert await driver.alock_release("lock", "tok") == 1


@pytest.mark.asyncio
async def test_async_set_returns_none_like_sync(driver):
    """Sync side: set returns None. Async must match."""
    result = await driver.aset("k", b"v")
    assert result is None


@pytest.mark.asyncio
async def test_async_xadd_returns_str_like_sync(driver):
    """Sync side: xadd returns str. Async must match (was bytes before fix)."""
    msg_id = await driver.axadd("s", "*", [("f", b"v")])
    assert isinstance(msg_id, str)


@pytest.mark.asyncio
async def test_async_script_load_returns_str_like_sync(driver):
    """Sync side: script_load returns str. Async must match."""
    sha = await driver.ascript_load("return 1")
    assert isinstance(sha, str)
    assert len(sha) == 40  # sha1 hex


@pytest.mark.asyncio
async def test_async_type_returns_str_like_sync(driver):
    """Sync side: type returns str. Async must match."""
    await driver.aset("k", b"v")
    t = await driver.atype("k")
    assert t == "string"
