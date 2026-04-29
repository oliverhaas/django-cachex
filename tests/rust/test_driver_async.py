"""Async smoke tests: verify each major family's async variant returns a working awaitable."""

import pytest


@pytest.mark.asyncio
async def test_async_get_set(driver):
    await driver.set("k", b"v")
    assert await driver.get("k") == b"v"


@pytest.mark.asyncio
async def test_async_mget(driver):
    await driver.pipeline_set([("a", b"1"), ("b", b"2")])
    assert await driver.mget(["a", "b", "missing"]) == [b"1", b"2", None]


@pytest.mark.asyncio
async def test_async_hset_hgetall(driver):
    await driver.hset("h", "a", b"1")
    await driver.hset("h", "b", b"2")
    result = await driver.hgetall("h")
    assert result == {b"a": b"1", b"b": b"2"}


@pytest.mark.asyncio
async def test_async_sadd_smembers(driver):
    await driver.sadd("s", [b"x", b"y", b"z"])
    members = await driver.smembers("s")
    assert sorted(members) == [b"x", b"y", b"z"]


@pytest.mark.asyncio
async def test_async_zadd_zrange(driver):
    await driver.zadd("z", [(b"a", 1.0), (b"b", 2.0)])
    assert await driver.zrange("z", 0, -1) == [b"a", b"b"]


@pytest.mark.asyncio
async def test_async_xadd_xlen(driver):
    await driver.xadd("s", "*", [("f", b"v")])
    assert await driver.xlen("s") == 1


@pytest.mark.asyncio
async def test_async_lpush_lrange(driver):
    await driver.rpush("l", [b"a", b"b", b"c"])
    assert await driver.lrange("l", 0, -1) == [b"a", b"b", b"c"]


@pytest.mark.asyncio
async def test_async_eval(driver):
    result = await driver.eval("return tonumber(ARGV[1])", [], [b"7"])
    assert result == 7


@pytest.mark.asyncio
async def test_async_lock(driver):
    assert await driver.lock_acquire("lock", "tok", 5000) is True
    assert await driver.lock_release("lock", "tok") == 1
