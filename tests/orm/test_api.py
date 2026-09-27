# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from time import sleep, time
from unittest import skipIf

from django.conf import settings
from django.contrib.auth.models import Permission, User
from django.core.cache import DEFAULT_CACHE_ALIAS
from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS, connection, transaction
from django.test import TransactionTestCase

from django_cachex.orm.api import get_last_invalidation, invalidate, orm_cache_disabled
from tests.orm.app.models import Test
from tests.orm.utils import TestUtilsMixin, override_orm_settings


class APITestCase(TestUtilsMixin, TransactionTestCase):
    databases = set(settings.DATABASES.keys())

    def setUp(self):
        super().setUp()
        self.t1 = Test.objects.create(name="test1")
        self.cache_alias2 = next(alias for alias in settings.CACHES if alias != DEFAULT_CACHE_ALIAS)
        # For the orm_cache_disabled tests
        self.user = User.objects.create_user("user")
        self.t1__permission = Permission.objects.order_by("?").select_related("content_type")[0]

    def test_invalidate_tables(self):
        with self.assertNumQueries(1):
            data1 = list(Test.objects.values_list("name", flat=True))
            self.assertListEqual(data1, ["test1"])

        with override_orm_settings(INVALIDATE_RAW=False), connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO ormtest_test (name, public) VALUES ('test2', %s);",
                [1 if self.is_sqlite else True],
            )

        with self.assertNumQueries(0):
            data2 = list(Test.objects.values_list("name", flat=True))
            self.assertListEqual(data2, ["test1"])

        invalidate("ormtest_test")

        with self.assertNumQueries(1):
            data3 = list(Test.objects.values_list("name", flat=True))
            self.assertListEqual(data3, ["test1", "test2"])

    def test_invalidate_models_lookups(self):
        with self.assertNumQueries(1):
            data1 = list(Test.objects.values_list("name", flat=True))
            self.assertListEqual(data1, ["test1"])

        with override_orm_settings(INVALIDATE_RAW=False), connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO ormtest_test (name, public) VALUES ('test2', %s);",
                [1 if self.is_sqlite else True],
            )

        with self.assertNumQueries(0):
            data2 = list(Test.objects.values_list("name", flat=True))
            self.assertListEqual(data2, ["test1"])

        invalidate("ormtest.Test")

        with self.assertNumQueries(1):
            data3 = list(Test.objects.values_list("name", flat=True))
            self.assertListEqual(data3, ["test1", "test2"])

    def test_invalidate_models(self):
        with self.assertNumQueries(1):
            data1 = list(Test.objects.values_list("name", flat=True))
            self.assertListEqual(data1, ["test1"])

        with override_orm_settings(INVALIDATE_RAW=False), connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO ormtest_test (name, public) VALUES ('test2', %s);",
                [1 if self.is_sqlite else True],
            )

        with self.assertNumQueries(0):
            data2 = list(Test.objects.values_list("name", flat=True))
            self.assertListEqual(data2, ["test1"])

        invalidate(Test)

        with self.assertNumQueries(1):
            data3 = list(Test.objects.values_list("name", flat=True))
            self.assertListEqual(data3, ["test1", "test2"])

    def test_invalidate_all(self):
        with self.assertNumQueries(1):
            Test.objects.get()

        with self.assertNumQueries(0):
            Test.objects.get()

        invalidate()

        with self.assertNumQueries(1):
            Test.objects.get()

    def test_invalidate_all_in_atomic(self):
        with transaction.atomic():
            with self.assertNumQueries(1):
                Test.objects.get()

            with self.assertNumQueries(0):
                Test.objects.get()

            invalidate()

            with self.assertNumQueries(1):
                Test.objects.get()

        with self.assertNumQueries(1):
            Test.objects.get()

    def test_get_last_invalidation(self):
        invalidate()
        timestamp = get_last_invalidation()
        delta = 0.1
        self.assertAlmostEqual(timestamp, time(), delta=delta)

        sleep(0.1)

        invalidate("ormtest_test")
        timestamp = get_last_invalidation("ormtest_test")
        self.assertAlmostEqual(timestamp, time(), delta=delta)
        same_timestamp = get_last_invalidation("ormtest.Test")
        self.assertEqual(same_timestamp, timestamp)
        same_timestamp = get_last_invalidation(Test)
        self.assertEqual(same_timestamp, timestamp)

        timestamp = get_last_invalidation("ormtest_testparent")
        self.assertNotAlmostEqual(timestamp, time(), delta=0.1)
        timestamp = get_last_invalidation("ormtest_testparent", "ormtest_test")
        self.assertAlmostEqual(timestamp, time(), delta=delta)

    def test_orm_cache_disabled_multiple_queries_ignoring_in_mem_cache(self):
        """
        Test that when queries are given the `orm_cache_disabled` context manager,
        the queries will not be cached.
        """
        with orm_cache_disabled(True):
            qs = Test.objects.all()
            with self.assertNumQueries(1):
                data1 = list(qs.all())
            Test.objects.create(
                name="test3",
                owner=self.user,
                date="1789-07-14",
                datetime="1789-07-14T16:43:27",
                permission=self.t1__permission,
            )
            with self.assertNumQueries(1):
                data2 = list(qs.all())
            self.assertNotEqual(data1, data2)

    def test_query_orm_cache_disabled_even_if_already_cached(self):
        """
        Test that when a query is given the `orm_cache_disabled` context manager,
        the query outside of the context manager will be cached. Any duplicated
        query will use the original query's cached result.
        """
        qs = Test.objects.all()
        self.assert_query_cached(qs)
        with orm_cache_disabled() and self.assertNumQueries(0):
            list(qs.all())

    def test_duplicate_query_execute_anyways(self):
        """After an object is created, a duplicate query should execute
        rather than use the cached result.
        """
        qs = Test.objects.all()
        self.assert_query_cached(qs)
        Test.objects.create(
            name="test3",
            owner=self.user,
            date="1789-07-14",
            datetime="1789-07-14T16:43:27",
            permission=self.t1__permission,
        )
        with orm_cache_disabled() and self.assertNumQueries(1):
            list(qs.all())


class CommandTestCase(TransactionTestCase):
    multi_db = True
    databases = "__all__"

    def setUp(self):
        self.db_alias2 = next(alias for alias in settings.DATABASES if alias != DEFAULT_DB_ALIAS)

        self.cache_alias2 = next(alias for alias in settings.CACHES if alias != DEFAULT_CACHE_ALIAS)

        self.t1 = Test.objects.create(name="test1")
        self.t2 = Test.objects.using(self.db_alias2).create(name="test2")
        self.u = User.objects.create_user("test")

    def test_invalidate_orm_cache(self):
        with self.assertNumQueries(1):
            self.assertListEqual(list(Test.objects.all()), [self.t1])
        call_command("invalidate_orm_cache", verbosity=0)
        with self.assertNumQueries(1):
            self.assertListEqual(list(Test.objects.all()), [self.t1])

        call_command("invalidate_orm_cache", "auth", verbosity=0)
        with self.assertNumQueries(0):
            self.assertListEqual(list(Test.objects.all()), [self.t1])

        call_command("invalidate_orm_cache", "ormtest", verbosity=0)
        with self.assertNumQueries(1):
            self.assertListEqual(list(Test.objects.all()), [self.t1])

        call_command("invalidate_orm_cache", "ormtest.testchild", verbosity=0)
        with self.assertNumQueries(0):
            self.assertListEqual(list(Test.objects.all()), [self.t1])

        call_command("invalidate_orm_cache", "ormtest.test", verbosity=0)
        with self.assertNumQueries(1):
            self.assertListEqual(list(Test.objects.all()), [self.t1])

        with self.assertNumQueries(1):
            self.assertListEqual(list(User.objects.all()), [self.u])
        call_command("invalidate_orm_cache", "ormtest.test", "auth.user", verbosity=0)
        with self.assertNumQueries(1):
            self.assertListEqual(list(Test.objects.all()), [self.t1])
        with self.assertNumQueries(1):
            self.assertListEqual(list(User.objects.all()), [self.u])

    @skipIf(len(settings.DATABASES) == 1, "We can't change the DB used since there's only one configured")
    def test_invalidate_orm_cache_multi_db(self):
        with self.assertNumQueries(1):
            self.assertListEqual(list(Test.objects.all()), [self.t1])
        call_command("invalidate_orm_cache", verbosity=0, db_alias=self.db_alias2)
        with self.assertNumQueries(0):
            self.assertListEqual(list(Test.objects.all()), [self.t1])

        with self.assertNumQueries(1, using=self.db_alias2):
            self.assertListEqual(list(Test.objects.using(self.db_alias2)), [self.t2])
        call_command("invalidate_orm_cache", verbosity=0, db_alias=self.db_alias2)
        with self.assertNumQueries(1, using=self.db_alias2):
            self.assertListEqual(list(Test.objects.using(self.db_alias2)), [self.t2])

    @skipIf(len(settings.CACHES) == 1, "We can't change the cache used since there's only one configured")
    def test_invalidate_orm_cache_multi_cache(self):
        with self.assertNumQueries(1):
            self.assertListEqual(list(Test.objects.all()), [self.t1])
        call_command("invalidate_orm_cache", verbosity=0, cache_alias=self.cache_alias2)
        with self.assertNumQueries(0):
            self.assertListEqual(list(Test.objects.all()), [self.t1])

        with self.assertNumQueries(1), override_orm_settings(CACHE=self.cache_alias2):
            self.assertListEqual(list(Test.objects.all()), [self.t1])
        call_command("invalidate_orm_cache", verbosity=0, cache_alias=self.cache_alias2)
        with self.assertNumQueries(1), override_orm_settings(CACHE=self.cache_alias2):
            self.assertListEqual(list(Test.objects.all()), [self.t1])
