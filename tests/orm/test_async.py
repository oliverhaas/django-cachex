"""The ORM cache under the async ORM API.

The tests call the async API through async_to_sync, so its queries run back on the test's thread, where
assert_num_queries counts them.
"""

import asyncio

import pytest
from asgiref.sync import async_to_sync

from django_cachex.orm.api import orm_cache_disabled
from tests.orm.app.models import Test
from tests.orm.utils import assert_num_queries

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture(autouse=True)
def t1():
    return Test.objects.create(name="test1")


async def acount():
    return await Test.objects.acount()


def assert_cached(read, expected):
    with assert_num_queries(1):
        assert async_to_sync(read)() == expected
    with assert_num_queries(0):
        assert async_to_sync(read)() == expected


def test_aget(t1):
    async def get():
        return await Test.objects.aget(name="test1")

    assert_cached(get, t1)


def test_acount():
    assert_cached(acount, 1)


def test_async_for():
    async def names():
        return [t.name async for t in Test.objects.all()]

    assert_cached(names, ["test1"])


def test_aiterator():
    async def names():
        return [t.name async for t in Test.objects.aiterator()]

    assert_cached(names, ["test1"])


def test_acreate_invalidates():
    async def create():
        await Test.objects.acreate(name="test2")

    assert_cached(acount, 1)
    async_to_sync(create)()
    assert_cached(acount, 2)


def test_orm_cache_disabled_around_async_code():
    assert async_to_sync(acount)() == 1
    with orm_cache_disabled(), assert_num_queries(1):
        assert async_to_sync(acount)() == 1


def test_orm_cache_disabled_in_a_coroutine():
    # It holds for the coroutine that entered it, across awaits, not for those beside it.
    assert async_to_sync(acount)() == 1

    async def disabled():
        with orm_cache_disabled():
            await asyncio.sleep(0)
            return await acount()

    async def both():
        return await asyncio.gather(disabled(), acount())

    with assert_num_queries(1):
        assert async_to_sync(both)() == [1, 1]
