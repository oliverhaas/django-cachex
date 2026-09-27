# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from threading import Thread

from django.db import connection, transaction
from django.test import skipUnlessDBFeature

from tests.orm.app.models import Test
from tests.orm.utils import FilteredTransactionTestCase, TestUtilsMixin


class TestThread(Thread):
    __test__ = False  # Not a pytest test class.

    def start_and_join(self):
        self.start()
        self.join()
        return self.t

    def run(self):
        self.t = Test.objects.first()
        connection.close()


class CreateThread(TestThread):
    def run(self):
        self.t = Test.objects.create(name="test")
        connection.close()


@skipUnlessDBFeature("test_db_allows_multiple_connections")
class ThreadSafetyTestCase(TestUtilsMixin, FilteredTransactionTestCase):
    def test_concurrent_caching(self):
        t1 = TestThread().start_and_join()
        t = Test.objects.create(name="test")
        t2 = TestThread().start_and_join()

        self.assertEqual(t1, None)
        self.assertEqual(t2, t)

    def test_concurrent_caching_during_atomic(self):
        with self.assertNumQueries(1), transaction.atomic():
            t1 = TestThread().start_and_join()
            t = Test.objects.create(name="test")
            t2 = TestThread().start_and_join()

        self.assertEqual(t1, None)
        self.assertEqual(t2, None)

        with self.assertNumQueries(1):
            data = Test.objects.first()
        self.assertEqual(data, t)

    def test_concurrent_caching_before_and_during_atomic_1(self):
        t1 = TestThread().start_and_join()

        with self.assertNumQueries(1), transaction.atomic():
            t2 = TestThread().start_and_join()
            t = Test.objects.create(name="test")

        self.assertEqual(t1, None)
        self.assertEqual(t2, None)

        with self.assertNumQueries(1):
            data = Test.objects.first()
        self.assertEqual(data, t)

    def test_concurrent_caching_before_and_during_atomic_2(self):
        t1 = TestThread().start_and_join()

        with self.assertNumQueries(1), transaction.atomic():
            t = Test.objects.create(name="test")
            t2 = TestThread().start_and_join()

        self.assertEqual(t1, None)
        self.assertEqual(t2, None)

        with self.assertNumQueries(1):
            data = Test.objects.first()
        self.assertEqual(data, t)

    def test_concurrent_caching_during_and_after_atomic_1(self):
        with self.assertNumQueries(1), transaction.atomic():
            t1 = TestThread().start_and_join()
            t = Test.objects.create(name="test")

        t2 = TestThread().start_and_join()

        self.assertEqual(t1, None)
        self.assertEqual(t2, t)

        with self.assertNumQueries(0):
            data = Test.objects.first()
        self.assertEqual(data, t)

    def test_concurrent_caching_during_and_after_atomic_2(self):
        with self.assertNumQueries(1), transaction.atomic():
            t = Test.objects.create(name="test")
            t1 = TestThread().start_and_join()

        t2 = TestThread().start_and_join()

        self.assertEqual(t1, None)
        self.assertEqual(t2, t)

        with self.assertNumQueries(0):
            data = Test.objects.first()
        self.assertEqual(data, t)

    def test_concurrent_caching_during_and_after_atomic_3(self):
        with self.assertNumQueries(1), transaction.atomic():
            t1 = TestThread().start_and_join()
            t = Test.objects.create(name="test")
            t2 = TestThread().start_and_join()

        t3 = TestThread().start_and_join()

        self.assertEqual(t1, None)
        self.assertEqual(t2, None)
        self.assertEqual(t3, t)

        with self.assertNumQueries(0):
            data = Test.objects.first()
        self.assertEqual(data, t)

    # A read between the start of the write and its commit returns the old
    # rows, which must not be cached.
    def test_concurrent_caching_during_autocommit_write(self):
        results = []

        def read_before_write(execute, sql, params, many, context):
            results.append(TestThread().start_and_join())
            return execute(sql, params, many, context)

        with connection.execute_wrapper(read_before_write):
            t = Test.objects.create(name="test")

        self.assertListEqual(results, [None])
        self.assertEqual(Test.objects.first(), t)

    # A result read before a concurrent write must not be cached after it.
    def test_concurrent_write_between_query_and_caching(self):
        created = []

        def write_after_read(execute, sql, params, many, context):
            result = execute(sql, params, many, context)
            created.append(CreateThread().start_and_join())
            return result

        with connection.execute_wrapper(write_after_read):
            self.assertIsNone(Test.objects.first())

        self.assertEqual(Test.objects.first(), created[0])
