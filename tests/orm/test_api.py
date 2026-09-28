# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from io import StringIO
from unittest import skipIf

from django.conf import settings
from django.contrib.auth.models import User
from django.core.cache import DEFAULT_CACHE_ALIAS
from django.core.management import CommandError, call_command
from django.db import DEFAULT_DB_ALIAS, connection, transaction
from django.test import TransactionTestCase

from django_cachex.orm.api import invalidate, orm_cache_disabled, table_generations
from tests.orm.app.models import Test
from tests.orm.utils import TestUtilsMixin, override_orm_settings


class APITestCase(TestUtilsMixin, TransactionTestCase):
    databases = set(settings.DATABASES.keys())

    def setUp(self):
        super().setUp()
        self.t1 = Test.objects.create(name="test1")
        self.cache_alias2 = next(alias for alias in settings.CACHES if alias != DEFAULT_CACHE_ALIAS)

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

    def test_table_generations(self):
        generations = table_generations(Test)
        self.assertIsNotNone(generations)
        self.assertEqual(len(generations), 1)
        self.assertEqual(table_generations(Test), generations)
        self.assertEqual(table_generations("ormtest.Test"), generations)
        self.assertEqual(table_generations("ormtest_test"), generations)

        Test.objects.create(name="test2")
        self.assertNotEqual(table_generations(Test), generations)

    def test_table_generations_of_several_tables(self):
        generations = table_generations(Test, User)
        self.assertEqual(generations, table_generations(Test) + table_generations(User))

        invalidate(User)

        test_generation, user_generation = table_generations(Test, User)
        self.assertEqual(test_generation, generations[0])
        self.assertNotEqual(user_generation, generations[1])

    def test_table_generations_needs_a_table(self):
        with self.assertRaises(TypeError):
            table_generations()

    def test_table_generations_when_not_cached(self):
        with override_orm_settings(ENABLED=False):
            self.assertIsNone(table_generations(Test))
        with orm_cache_disabled():
            self.assertIsNone(table_generations(Test))
        with override_orm_settings(UNCACHABLE_TABLES=("ormtest_test",)):
            self.assertIsNone(table_generations(Test, User))
        with override_orm_settings(DATABASES=[]):
            self.assertIsNone(table_generations(Test))

    def test_table_generations_during_a_write(self):
        generations = []

        def read_generations(execute, sql, params, many, context):
            generations.append(table_generations(Test))
            return execute(sql, params, many, context)

        with connection.execute_wrapper(read_generations):
            Test.objects.create(name="test2")

        self.assertListEqual(generations, [None])
        self.assertIsNotNone(table_generations(Test))

    def test_table_generations_in_a_transaction(self):
        before = table_generations(Test)
        with transaction.atomic():
            self.assertEqual(table_generations(Test), before)
            Test.objects.create(name="test2")
            # The transaction's own write is not committed yet.
            self.assertIsNone(table_generations(Test))
            self.assertIsNotNone(table_generations(User))
        after = table_generations(Test)
        self.assertIsNotNone(after)
        self.assertNotEqual(after, before)

    def test_orm_cache_disabled(self):
        qs = Test.objects.all()
        self.assert_query_cached(qs)
        with orm_cache_disabled():
            with self.assertNumQueries(1):
                self.assertListEqual(list(qs.all()), [self.t1])
            self.assert_query_cached(qs, after=1)
        with self.assertNumQueries(0):
            self.assertListEqual(list(qs.all()), [self.t1])

    def test_orm_cache_disabled_still_invalidates(self):
        qs = Test.objects.all()
        self.assert_query_cached(qs)
        with orm_cache_disabled():
            t2 = Test.objects.create(name="test2")
        with self.assertNumQueries(1):
            self.assertListEqual(list(qs.all()), [self.t1, t2])

    def test_orm_cache_disabled_nested(self):
        qs = Test.objects.all()
        with orm_cache_disabled():
            with orm_cache_disabled():
                pass
            self.assert_query_cached(qs, after=1)
        self.assert_query_cached(qs)


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

    def test_invalidate_orm_cache_app_includes_many_to_many_tables(self):
        permissions = User.user_permissions.through.objects.all()
        with self.assertNumQueries(1):
            self.assertListEqual(list(permissions.all()), [])
        call_command("invalidate_orm_cache", "auth", verbosity=0)
        with self.assertNumQueries(1):
            self.assertListEqual(list(permissions.all()), [])

    def test_invalidate_orm_cache_app_without_models(self):
        with self.assertNumQueries(1):
            self.assertListEqual(list(Test.objects.all()), [self.t1])
        out = StringIO()
        call_command("invalidate_orm_cache", "cachex_orm", stdout=out)
        self.assertEqual(out.getvalue(), "No models to invalidate.\n")
        with self.assertNumQueries(0):
            self.assertListEqual(list(Test.objects.all()), [self.t1])

    def test_invalidate_orm_cache_unknown_label(self):
        for label in ("unknown", "ormtest.unknown", "ormtest.test.name"):
            with self.subTest(label=label), self.assertRaises(CommandError):
                call_command("invalidate_orm_cache", label, verbosity=0)

    def test_invalidate_orm_cache_output(self):
        out = StringIO()
        call_command("invalidate_orm_cache", "ormtest.test", "ormtest", stdout=out)
        self.assertEqual(out.getvalue(), "Invalidating 6 models...\nORM cache invalidated.\n")
        out = StringIO()
        call_command("invalidate_orm_cache", "ormtest.test", stdout=out, db_alias=DEFAULT_DB_ALIAS)
        self.assertEqual(
            out.getvalue(),
            f"Invalidating 1 model for database '{DEFAULT_DB_ALIAS}'...\nORM cache invalidated.\n",
        )
        out = StringIO()
        call_command("invalidate_orm_cache", stdout=out, cache_alias=DEFAULT_CACHE_ALIAS)
        self.assertEqual(
            out.getvalue(),
            f"Invalidating all tables on cache '{DEFAULT_CACHE_ALIAS}'...\nORM cache invalidated.\n",
        )

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
