import asyncio

from django_cachex._driver import _test_delayed_bytes, _test_resolved_bytes


def test_runtime_survives_loop_close():
    async def go(n: int) -> bytes:
        awaitable = _test_delayed_bytes(f"v{n}".encode(), 25)
        return await awaitable

    loop1 = asyncio.new_event_loop()
    try:
        assert loop1.run_until_complete(go(1)) == b"v1"
    finally:
        loop1.close()

    loop2 = asyncio.new_event_loop()
    try:
        assert loop2.run_until_complete(go(2)) == b"v2"
    finally:
        loop2.close()


def test_runtime_serves_fast_and_slow_in_same_loop():
    async def go() -> tuple[bytes, bytes]:
        a = await _test_resolved_bytes(b"fast")
        b = await _test_delayed_bytes(b"slow", 30)
        return a, b

    loop = asyncio.new_event_loop()
    try:
        assert loop.run_until_complete(go()) == (b"fast", b"slow")
    finally:
        loop.close()
