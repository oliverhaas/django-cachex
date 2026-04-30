import pytest

from django_cachex._driver import _test_delayed_bytes, _test_dropped


@pytest.mark.asyncio
async def test_slow_path_resolves():
    awaitable = _test_delayed_bytes(b"slow", 50)
    result = await awaitable
    assert result == b"slow"


@pytest.mark.asyncio
async def test_slow_path_marks_future_blocking():
    awaitable = _test_delayed_bytes(b"x", 200)
    it = iter(awaitable)
    for _ in range(6):
        try:
            next(it)
        except StopIteration:
            return
    assert awaitable._asyncio_future_blocking is True
    assert awaitable._loop is not None


@pytest.mark.asyncio
async def test_slow_path_dropped_sender_raises():
    awaitable = _test_dropped()
    with pytest.raises(RuntimeError, match="dropped"):
        await awaitable
