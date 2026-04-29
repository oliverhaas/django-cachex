import pytest
from django_cachex._driver import (
    _test_resolved_bytes,
    _test_resolved_int,
    _test_resolved_none,
)


@pytest.mark.asyncio
async def test_fast_path_bytes():
    awaitable = _test_resolved_bytes(b"hello")
    result = await awaitable
    assert result == b"hello"


@pytest.mark.asyncio
async def test_fast_path_none():
    awaitable = _test_resolved_none()
    assert await awaitable is None


@pytest.mark.asyncio
async def test_fast_path_int():
    awaitable = _test_resolved_int(42)
    assert await awaitable == 42


@pytest.mark.asyncio
async def test_fast_path_does_not_enter_callback_mode():
    awaitable = _test_resolved_bytes(b"x")
    await awaitable
    assert awaitable._loop is None
