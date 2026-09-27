# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from time import sleep
from unittest import skipIf
from unittest.mock import MagicMock, patch

from django.conf import settings
from django.contrib.auth.models import User
from django.core.cache import DEFAULT_CACHE_ALIAS
from django.core.checks import Error, Tags, Warning, run_checks  # noqa: A004
from django.db import connection
from django.test import TransactionTestCase

from django_cachex.orm.api import invalidate
from django_cachex.orm.settings import SUPPORTED_DATABASE_ENGINES, SUPPORTED_ONLY
from django_cachex.orm.utils import _get_tables
from tests.orm.app.models import Test, TestChild, TestParent, UnmanagedModel
from tests.orm.utils import TestUtilsMixin, override_orm_settings


class SettingsTestCase(TestUtilsMixin, TransactionTestCase):
    @override_orm_settings(ENABLED=False)
    def test_decorator(self):
        self.assert_query_cached(Test.objects.all(), after=1)

    def test_django_override(self):
        with override_orm_settings(ENABLED=False):
            qs = Test.objects.all()
            self.assert_query_cached(qs, after=1)
            with override_orm_settings(ENABLED=True):
                self.assert_query_cached(qs)

    def test_enabled(self):
        qs = Test.objects.all()

        with override_orm_settings(ENABLED=True):
            self.assert_query_cached(qs)

        with override_orm_settings(ENABLED=False):
            self.assert_query_cached(qs, after=1)

        with self.assertNumQueries(0):
            list(Test.objects.all())

        with override_orm_settings(ENABLED=False), self.assertNumQueries(1):
            t = Test.objects.create(name="test")
        with self.assertNumQueries(1):
            data = list(Test.objects.all())
        self.assertListEqual(data, [t])

    @skipIf(len(settings.CACHES) == 1, "We can't change the cache used since there's only one configured.")
    def test_cache(self):
        other_cache_alias = next(alias for alias in settings.CACHES if alias != DEFAULT_CACHE_ALIAS)
        invalidate(Test, cache_alias=other_cache_alias)

        qs = Test.objects.all()

        with override_orm_settings(CACHE=DEFAULT_CACHE_ALIAS):
            self.assert_query_cached(qs)

        with override_orm_settings(CACHE=other_cache_alias):
            self.assert_query_cached(qs)

        Test.objects.create(name="test")

        # Only the `CACHE` alias is invalidated, so changing the database should
        # not invalidate all caches.
        with override_orm_settings(CACHE=other_cache_alias):
            self.assert_query_cached(qs, before=0)

    def test_databases(self):
        qs = Test.objects.all()
        with override_orm_settings(DATABASES=SUPPORTED_ONLY):
            self.assert_query_cached(qs)

        invalidate(Test)

        engine = connection.settings_dict["ENGINE"]
        SUPPORTED_DATABASE_ENGINES.remove(engine)
        with override_orm_settings(DATABASES=SUPPORTED_ONLY):
            self.assert_query_cached(qs, after=1)
        invalidate(Test)

        with override_orm_settings(USE_UNSUPPORTED_DATABASE=True):
            self.assert_query_cached(qs)
        invalidate(Test)

        with override_orm_settings(ADDITIONAL_SUPPORTED_DATABASES={engine}):
            self.assert_query_cached(qs)
        invalidate(Test)

        SUPPORTED_DATABASE_ENGINES.add(engine)
        with override_orm_settings(DATABASES=SUPPORTED_ONLY):
            self.assert_query_cached(qs)

        with override_orm_settings(DATABASES=[]):
            self.assert_query_cached(qs, after=1)

    def test_cache_timeout(self):
        qs = Test.objects.all()

        with self.assertNumQueries(1):
            list(qs.all())
        sleep(1)
        with self.assertNumQueries(0):
            list(qs.all())

        invalidate(Test)

        with override_orm_settings(TIMEOUT=0):
            with self.assertNumQueries(1):
                list(qs.all())
            sleep(0.05)
            with self.assertNumQueries(1):
                list(qs.all())

        # We have to test with a full second and not a shorter time because
        # memcached only takes the integer part of the timeout into account.
        with override_orm_settings(TIMEOUT=1):
            self.assert_query_cached(qs)
            sleep(1)
            with self.assertNumQueries(1):
                list(Test.objects.all())

    def test_cache_random(self):
        qs = Test.objects.order_by("?")
        self.assert_query_cached(qs, after=1, compare_results=False)

        with override_orm_settings(CACHE_RANDOM=True):
            self.assert_query_cached(qs)

    def test_invalidate_raw(self):
        with self.assertNumQueries(1):
            list(Test.objects.all())
        with override_orm_settings(INVALIDATE_RAW=False), self.assertNumQueries(1), connection.cursor() as cursor:
            cursor.execute("UPDATE %s SET name = 'new name';" % Test._meta.db_table)
        with self.assertNumQueries(0):
            list(Test.objects.all())

    def test_only_cachable_tables(self):
        with override_orm_settings(ONLY_CACHABLE_TABLES=("ormtest_test",)):
            self.assert_query_cached(Test.objects.all())
            self.assert_query_cached(TestParent.objects.all(), after=1)
            self.assert_query_cached(Test.objects.select_related("owner"), after=1)

        self.assert_query_cached(TestParent.objects.all())

        with override_orm_settings(ONLY_CACHABLE_TABLES=("ormtest_test", "ormtest_testchild", "auth_user")):
            self.assert_query_cached(Test.objects.select_related("owner"))

            # TestChild uses multi-table inheritance, and since its parent,
            # 'ormtest_testparent', is not cachable, a basic
            # TestChild query can't be cached
            self.assert_query_cached(TestChild.objects.all(), after=1)

            # However, if we only fetch data from the 'ormtest_testchild'
            # table, it's cachable.
            self.assert_query_cached(TestChild.objects.values("public"))

    @override_orm_settings(ONLY_CACHABLE_APPS=("ormtest",))
    def test_only_cachable_apps(self):
        self.assert_query_cached(Test.objects.all())
        self.assert_query_cached(TestParent.objects.all())
        self.assert_query_cached(Test.objects.select_related("owner"), after=1)

    @override_orm_settings(ONLY_CACHABLE_TABLES=("ormtest_test", "auth_user"), ONLY_CACHABLE_APPS=("ormtest",))
    def test_only_cachable_apps_set_combo(self):
        self.assert_query_cached(Test.objects.all())
        self.assert_query_cached(TestParent.objects.all())
        self.assert_query_cached(Test.objects.select_related("owner"))

    def test_uncachable_tables(self):
        qs = Test.objects.all()

        with override_orm_settings(UNCACHABLE_TABLES=("ormtest_test",)):
            self.assert_query_cached(qs, after=1)

        self.assert_query_cached(qs)

        with override_orm_settings(UNCACHABLE_TABLES=("ormtest_test",)):
            self.assert_query_cached(qs, after=1)

    @override_orm_settings(UNCACHABLE_APPS=("ormtest",))
    def test_uncachable_apps(self):
        self.assert_query_cached(Test.objects.all(), after=1)
        self.assert_query_cached(TestParent.objects.all(), after=1)

    @override_orm_settings(UNCACHABLE_TABLES=("ormtest_test",), UNCACHABLE_APPS=("ormtest",))
    def test_uncachable_apps_set_combo(self):
        self.assert_query_cached(Test.objects.all(), after=1)
        self.assert_query_cached(TestParent.objects.all(), after=1)

    def test_only_cachable_and_uncachable_table(self):
        with override_orm_settings(
            ONLY_CACHABLE_TABLES=("ormtest_test", "ormtest_testparent"),
            UNCACHABLE_TABLES=("ormtest_test",),
        ):
            self.assert_query_cached(Test.objects.all(), after=1)
            self.assert_query_cached(TestParent.objects.all())
            self.assert_query_cached(User.objects.all(), after=1)

    def test_uncachable_unmanaged_table(self):
        qs = UnmanagedModel.objects.all()
        with override_orm_settings(
            UNCACHABLE_TABLES=("ormtest_unmanagedmodel",),
            ADDITIONAL_TABLES=("ormtest_unmanagedmodel",),
        ):
            self.assert_query_cached(qs, after=1)

    def test_database_compatibility(self):
        compatible_database = {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": "non_existent_db.sqlite3",
        }
        incompatible_database = {
            "ENGINE": "django.db.backends.oracle",
            "NAME": "non_existent_db",
        }

        warning002 = Warning(
            "None of the configured databases are supported by the ORM cache.",
            hint="Use a supported database, or remove django_cachex.orm, or put at least one "
            "database alias in `CACHEX_ORM['DATABASES']` to force the ORM cache to use it.",
            id="cachex_orm.W002",
        )
        warning003 = Warning(
            "Database engine %r is not supported by the ORM cache." % "django.db.backends.oracle",
            hint="Switch to a supported database engine, add an entry in "
            "`CACHEX_ORM['ADDITIONAL_SUPPORTED_DATABASES']`, or set "
            "`CACHEX_ORM['USE_UNSUPPORTED_DATABASE']` to True.",
            id="cachex_orm.W003",
        )
        warning004 = Warning(
            "The ORM cache is useless because no database is configured in `CACHEX_ORM['DATABASES']`.",
            hint="Reconfigure the ORM cache or remove it.",
            id="cachex_orm.W004",
        )
        error001 = Error(
            "Database alias %r from `CACHEX_ORM['DATABASES']` is not defined in `DATABASES`." % "secondary",
            hint="Change `CACHEX_ORM['DATABASES']` to only list aliases from `DATABASES`.",
            id="cachex_orm.E001",
        )
        error002 = Error(
            "`CACHEX_ORM['DATABASES']` must be either %r or a list, tuple, "
            "frozenset or set of database aliases." % SUPPORTED_ONLY,
            hint="Remove `CACHEX_ORM['DATABASES']` or change it.",
            id="cachex_orm.E002",
        )

        with self.settings(DATABASES={"default": incompatible_database}):
            errors = run_checks(tags=[Tags.compatibility])
            self.assertListEqual(errors, [warning002])

        with self.settings(DATABASES={"default": compatible_database, "secondary": incompatible_database}):
            errors = run_checks(tags=[Tags.compatibility])
            self.assertListEqual(errors, [])
        with self.settings(DATABASES={"default": incompatible_database, "secondary": compatible_database}):
            errors = run_checks(tags=[Tags.compatibility])
            self.assertListEqual(errors, [])

        with self.settings(DATABASES={"default": incompatible_database}), override_orm_settings(DATABASES=["default"]):
            errors = run_checks(tags=[Tags.compatibility])
            self.assertListEqual(errors, [warning003])

        with self.settings(DATABASES={"default": incompatible_database}), override_orm_settings(DATABASES=[]):
            errors = run_checks(tags=[Tags.compatibility])
            self.assertListEqual(errors, [warning004])

        with (
            self.settings(DATABASES={"default": incompatible_database}),
            override_orm_settings(DATABASES=["secondary"]),
        ):
            errors = run_checks(tags=[Tags.compatibility])
            self.assertListEqual(errors, [error001])
        with (
            self.settings(DATABASES={"default": compatible_database}),
            override_orm_settings(DATABASES=["default", "secondary"]),
        ):
            errors = run_checks(tags=[Tags.compatibility])
            self.assertListEqual(errors, [error001])

        with override_orm_settings(DATABASES="invalid value"):
            errors = run_checks(tags=[Tags.compatibility])
            self.assertListEqual(errors, [error002])

    def call_get_tables(self):
        qs = Test.objects.all()
        compiler_mock = MagicMock()
        compiler_mock._cachex_orm_generated_sql = ""
        tables = _get_tables(qs.db, qs.query, compiler_mock)
        self.assertTrue(tables)
        return tables

    @override_orm_settings(FINAL_SQL_CHECK=True)
    @patch("django_cachex.orm.utils._get_tables_from_sql")
    def test_final_sql_check_when_true(self, get_tables_from_sql):
        get_tables_from_sql.return_value = {"patched"}
        tables = self.call_get_tables()
        get_tables_from_sql.assert_called_once()
        self.assertIn("patched", tables)

    @override_orm_settings(FINAL_SQL_CHECK=False)
    @patch("django_cachex.orm.utils._get_tables_from_sql")
    def test_final_sql_check_when_false(self, get_tables_from_sql):
        get_tables_from_sql.return_value = {"patched"}
        tables = self.call_get_tables()
        get_tables_from_sql.assert_not_called()
        self.assertNotIn("patched", tables)
