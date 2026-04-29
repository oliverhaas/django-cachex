import asyncio
import multiprocessing as mp
import sys

import pytest

fork_ctx = mp.get_context("fork")


def _child_body(q) -> None:
    try:
        from django_cachex._driver import _test_delayed_bytes

        async def go() -> bytes:
            return await _test_delayed_bytes(b"child", 25)

        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(go())
        finally:
            loop.close()
        q.put(("ok", result))
    except Exception as exc:  # noqa: BLE001 — proxy any failure back to parent
        q.put(("err", repr(exc)))


@pytest.mark.skipif(sys.platform == "win32", reason="fork not supported on Windows")
def test_fork_creates_fresh_runtime():
    from django_cachex._driver import _test_resolved_bytes

    async def warm() -> None:
        await _test_resolved_bytes(b"warm")

    asyncio.run(warm())

    q = fork_ctx.Queue()
    p = fork_ctx.Process(target=_child_body, args=(q,))
    p.start()
    p.join(timeout=10)
    assert p.exitcode == 0, f"child exited with {p.exitcode}"
    status, payload = q.get(timeout=1)
    assert status == "ok", f"child failed: {payload}"
    assert payload == b"child"
