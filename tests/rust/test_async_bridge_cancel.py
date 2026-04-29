import asyncio

import pytest
from django_cachex._driver import _test_delayed_bytes, _test_pending, _test_resolved_bytes


@pytest.mark.asyncio
async def test_cancel_pending_in_busy_yield():
    awaitable = _test_pending()
    assert awaitable.cancel() is True
    assert awaitable.cancelled() is True
    with pytest.raises(asyncio.CancelledError):
        await awaitable


@pytest.mark.asyncio
async def test_cancel_returns_false_when_already_resolved():
    awaitable = _test_resolved_bytes(b"x")
    await awaitable
    assert awaitable.cancel() is False


@pytest.mark.asyncio
async def test_cancel_in_callback_mode_wakes_task():
    async def runner():
        awaitable = _test_delayed_bytes(b"never", 5_000)
        await awaitable

    task = asyncio.create_task(runner())
    for _ in range(10):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
