"""The ORM cache under the async ORM API.

The tests call the async API through async_to_sync, so its queries run back on the test's thread, where
assertNumQueries counts them.
"""

import asyncio

from asgiref.sync import async_to_sync
from django.test import TransactionTestCase

from django_cachex.orm.api import orm_cache_disabled
from tests.orm.app.models import Test
from tests.orm.utils import TestUtilsMixin


async def acount():
    return await Test.objects.acount()


class AsyncTestCase(TestUtilsMixin, TransactionTestCase):
    def setUp(self):
        super().setUp()
        self.t1 = Test.objects.create(name="test1")

    def assert_cached(self, read, expected):
        with self.assertNumQueries(1):
            self.assertEqual(async_to_sync(read)(), expected)
        with self.assertNumQueries(0):
            self.assertEqual(async_to_sync(read)(), expected)

    def test_aget(self):
        async def get():
            return await Test.objects.aget(name="test1")

        self.assert_cached(get, self.t1)

    def test_acount(self):
        self.assert_cached(acount, 1)

    def test_async_for(self):
        async def names():
            return [t.name async for t in Test.objects.all()]

        self.assert_cached(names, ["test1"])

    def test_aiterator(self):
        async def names():
            return [t.name async for t in Test.objects.aiterator()]

        self.assert_cached(names, ["test1"])

    def test_acreate_invalidates(self):
        async def create():
            await Test.objects.acreate(name="test2")

        self.assert_cached(acount, 1)
        async_to_sync(create)()
        self.assert_cached(acount, 2)

    def test_orm_cache_disabled_around_async_code(self):
        self.assertEqual(async_to_sync(acount)(), 1)
        with orm_cache_disabled(), self.assertNumQueries(1):
            self.assertEqual(async_to_sync(acount)(), 1)

    def test_orm_cache_disabled_in_a_coroutine(self):
        # It holds for the coroutine that entered it, across awaits, not for those beside it.
        self.assertEqual(async_to_sync(acount)(), 1)

        async def disabled():
            with orm_cache_disabled():
                await asyncio.sleep(0)
                return await acount()

        async def both():
            return await asyncio.gather(disabled(), acount())

        with self.assertNumQueries(1):
            self.assertListEqual(async_to_sync(both)(), [1, 1])
