# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from time import sleep

import pytest
from django.conf import settings
from django.contrib.auth.models import User
from django.core.cache import DEFAULT_CACHE_ALIAS
from django.core.checks import Error, Tags, Warning, run_checks  # noqa: A004
from django.db import DEFAULT_DB_ALIAS, connection
from django.db.migrations.recorder import MigrationRecorder
from django.db.models.functions import Random
from django.test import override_settings

from django_cachex.orm.api import invalidate
from django_cachex.orm.settings import DEFAULTS, SUPPORTED_ONLY, database_vendor, orm_settings, supported_databases
from django_cachex.orm.utils import _get_tables
from tests.orm.app.models import Test, TestChild, TestParent, UnmanagedModel
from tests.orm.utils import assert_num_queries, assert_query_cached, override_orm_settings

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture
def no_supported_vendors(mocker):
    """Support no database vendor during the test."""
    mocker.patch("django_cachex.orm.settings.SUPPORTED_VENDORS", frozenset())
    yield
    mocker.stopall()
    orm_settings.load()


@override_orm_settings(ENABLED=False)
def test_decorator():
    assert_query_cached(Test.objects.all(), after=1)


def test_django_override():
    with override_orm_settings(ENABLED=False):
        qs = Test.objects.all()
        assert_query_cached(qs, after=1)
        with override_orm_settings(ENABLED=True):
            assert_query_cached(qs)


def test_enabled():
    qs = Test.objects.all()

    with override_orm_settings(ENABLED=True):
        assert_query_cached(qs)

    with override_orm_settings(ENABLED=False):
        assert_query_cached(qs, after=1)

    with assert_num_queries(0):
        list(Test.objects.all())

    with override_orm_settings(ENABLED=False), assert_num_queries(1):
        t = Test.objects.create(name="test")
    with assert_num_queries(1):
        data = list(Test.objects.all())
    assert data == [t]


def test_cache():
    invalidate(Test, cache_alias="other")

    qs = Test.objects.all()

    with override_orm_settings(CACHE=DEFAULT_CACHE_ALIAS):
        assert_query_cached(qs)

    with override_orm_settings(CACHE="other"):
        assert_query_cached(qs)

    Test.objects.create(name="test")

    # The write invalidated the tables only in the `CACHE` alias it ran under.
    with override_orm_settings(CACHE="other"):
        assert_query_cached(qs, before=0)


def test_databases():
    qs = Test.objects.all()
    with override_orm_settings(DATABASES=SUPPORTED_ONLY):
        assert_query_cached(qs)
    invalidate(Test)

    with override_orm_settings(DATABASES=[DEFAULT_DB_ALIAS]):
        assert_query_cached(qs)
    invalidate(Test)

    with override_orm_settings(DATABASES=[]):
        assert_query_cached(qs, after=1)


@pytest.mark.usefixtures("no_supported_vendors")
def test_unsupported_vendor():
    qs = Test.objects.all()
    assert supported_databases() == set()
    with override_orm_settings(DATABASES=SUPPORTED_ONLY):
        assert_query_cached(qs, after=1)
    with override_orm_settings(DATABASES=[DEFAULT_DB_ALIAS]):
        assert frozenset() == orm_settings.DATABASES
        assert_query_cached(qs, after=1)


@pytest.mark.filterwarnings("ignore:Overriding setting DATABASES:UserWarning")
def test_database_vendor():
    assert database_vendor(DEFAULT_DB_ALIAS) == connection.vendor
    assert database_vendor("undefined") is None
    with override_settings(DATABASES={"default": {"ENGINE": "django.db.backends.oracle", "NAME": "db"}}):
        # Without the oracledb driver, the backend does not load.
        assert database_vendor(DEFAULT_DB_ALIAS) is None


def test_cache_timeout():
    qs = Test.objects.all()

    with assert_num_queries(1):
        list(qs.all())
    sleep(1)
    with assert_num_queries(0):
        list(qs.all())

    invalidate(Test)

    with override_orm_settings(TIMEOUT=0):
        with assert_num_queries(1):
            list(qs.all())
        with assert_num_queries(1):
            list(qs.all())

    with override_orm_settings(TIMEOUT=1):
        assert_query_cached(qs)
        sleep(1)
        with assert_num_queries(1):
            list(Test.objects.all())


@pytest.mark.parametrize(
    "qs",
    [
        pytest.param(Test.objects.order_by("?"), id="order_by_question_mark"),
        # Not order_by(Random()) alone: it compiles to the SQL of order_by("?").
        pytest.param(Test.objects.order_by(Random(), "pk"), id="order_by_random_and_pk"),
        pytest.param(Test.objects.annotate(random=Random()), id="annotate_random"),
    ],
)
def test_random_not_cached(qs):
    assert_query_cached(qs, after=1, compare_results=False)


def test_only_cachable_tables():
    with override_orm_settings(ONLY_CACHABLE_TABLES=("ormtest_test",)):
        assert_query_cached(Test.objects.all())
        assert_query_cached(TestParent.objects.all(), after=1)
        assert_query_cached(Test.objects.select_related("owner"), after=1)

    assert_query_cached(TestParent.objects.all())

    with override_orm_settings(ONLY_CACHABLE_TABLES=("ormtest_test", "ormtest_testchild", "auth_user")):
        assert_query_cached(Test.objects.select_related("owner"))

        # A TestChild query also reads the table of its parent, which is not listed.
        assert_query_cached(TestChild.objects.all(), after=1)

        # This one reads only the table of TestChild.
        assert_query_cached(TestChild.objects.values("public"))


def test_uncachable_tables():
    qs = Test.objects.all()

    with override_orm_settings(UNCACHABLE_TABLES=("ormtest_test",)):
        assert_query_cached(qs, after=1)

    assert_query_cached(qs)

    with override_orm_settings(UNCACHABLE_TABLES=("ormtest_test",)):
        assert_query_cached(qs, after=1)


def test_django_migrations_never_cached():
    with override_orm_settings(UNCACHABLE_TABLES=("ormtest_test",)):
        assert_query_cached(MigrationRecorder(connection).migration_qs, after=1)


def test_only_cachable_and_uncachable_table():
    with override_orm_settings(
        ONLY_CACHABLE_TABLES=("ormtest_test", "ormtest_testparent"),
        UNCACHABLE_TABLES=("ormtest_test",),
    ):
        assert_query_cached(Test.objects.all(), after=1)
        assert_query_cached(TestParent.objects.all())
        assert_query_cached(User.objects.all(), after=1)


def test_uncachable_unmanaged_table():
    qs = UnmanagedModel.objects.all()
    with override_orm_settings(
        UNCACHABLE_TABLES=("ormtest_unmanagedmodel",),
        ADDITIONAL_TABLES=("ormtest_unmanagedmodel",),
    ):
        assert_query_cached(qs, after=1)


@pytest.mark.filterwarnings("ignore:Overriding setting DATABASES:UserWarning")
def test_database_compatibility():
    compatible_database = {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": "non_existent_db.sqlite3",
    }
    # Loads without a driver; its vendor is "unknown".
    incompatible_database = {
        "ENGINE": "django.db.backends.dummy",
        "NAME": "non_existent_db",
    }

    warning002 = Warning(
        "None of the configured databases are supported by the ORM cache.",
        hint="The ORM cache supports PostgreSQL and SQLite. Use one of them, or remove django_cachex.orm.",
        id="cachex_orm.W002",
    )
    warning003 = Warning(
        "The ORM cache is useless because no database is configured in `CACHEX_ORM['DATABASES']`.",
        hint="Reconfigure the ORM cache or remove it.",
        id="cachex_orm.W003",
    )
    error001 = Error(
        "Database alias 'secondary' from `CACHEX_ORM['DATABASES']` is not defined in `DATABASES`.",
        hint="Change `CACHEX_ORM['DATABASES']` to only list aliases from `DATABASES`.",
        id="cachex_orm.E001",
    )
    error002 = Error(
        f"`CACHEX_ORM['DATABASES']` must be either {SUPPORTED_ONLY!r} or a list, tuple, "
        "frozenset or set of database aliases.",
        hint="Remove `CACHEX_ORM['DATABASES']` or change it.",
        id="cachex_orm.E002",
    )
    error006 = Error(
        "Database 'default' (unknown) is not supported by the ORM cache.",
        hint="The ORM cache supports PostgreSQL and SQLite. Remove 'default' from `CACHEX_ORM['DATABASES']`.",
        id="cachex_orm.E006",
    )

    with override_settings(DATABASES={"default": incompatible_database}):
        errors = run_checks(tags=[Tags.compatibility])
        assert errors == [warning002]

    with override_settings(DATABASES={"default": compatible_database, "secondary": incompatible_database}):
        errors = run_checks(tags=[Tags.compatibility])
        assert errors == []
    with override_settings(DATABASES={"default": incompatible_database, "secondary": compatible_database}):
        errors = run_checks(tags=[Tags.compatibility])
        assert errors == []

    with override_settings(DATABASES={"default": incompatible_database}), override_orm_settings(DATABASES=["default"]):
        errors = run_checks(tags=[Tags.compatibility])
        assert errors == [error006]

    with override_settings(DATABASES={"default": incompatible_database}), override_orm_settings(DATABASES=[]):
        errors = run_checks(tags=[Tags.compatibility])
        assert errors == [warning003]

    with (
        override_settings(DATABASES={"default": incompatible_database}),
        override_orm_settings(DATABASES=["secondary"]),
    ):
        errors = run_checks(tags=[Tags.compatibility])
        assert errors == [error001]
    with (
        override_settings(DATABASES={"default": compatible_database}),
        override_orm_settings(DATABASES=["default", "secondary"]),
    ):
        errors = run_checks(tags=[Tags.compatibility])
        assert errors == [error001]

    with override_orm_settings(DATABASES="invalid value"):
        errors = run_checks(tags=[Tags.compatibility])
        assert errors == [error002]


@pytest.mark.filterwarnings("ignore:Overriding setting DATABASES:UserWarning")
def test_replica():
    database = {"ENGINE": "django.db.backends.sqlite3", "NAME": "non_existent_db.sqlite3"}
    replica = {**database, "TEST": {"MIRROR": "default"}}
    warning005 = Warning(
        "Database 'replica' mirrors 'default' (TEST['MIRROR']), so it looks like a replica.",
        hint="Writes to 'default' do not invalidate the queries the ORM cache caches from 'replica'. Remove "
        "it from `CACHEX_ORM['DATABASES']`.",
        id="cachex_orm.W005",
    )
    with override_settings(DATABASES={"default": database, "replica": replica}):
        assert supported_databases() == {"default"}
        assert run_checks(tags=[Tags.compatibility]) == []
        with override_orm_settings(DATABASES=["default", "replica"]):
            assert run_checks(tags=[Tags.compatibility]) == [warning005]


def test_table_settings_of_another_type():
    def error(name):
        return Error(
            f"`CACHEX_ORM['{name}']` must be a list, tuple, frozenset or set.",
            hint="A tuple of one item needs a trailing comma: ('name',).",
            id="cachex_orm.E007",
        )

    with override_orm_settings(UNCACHABLE_TABLES="django_session", ADDITIONAL_TABLES=None):
        assert run_checks(tags=[Tags.models], databases=[]) == [error("UNCACHABLE_TABLES"), error("ADDITIONAL_TABLES")]
        # Each counts as empty.
        assert {"django_migrations"} == orm_settings.UNCACHABLE_TABLES
        assert orm_settings.ADDITIONAL_TABLES == []


def test_cache_checks():
    assert run_checks(tags=[Tags.caches]) == []

    with override_orm_settings(TIMEOUTS=5):
        assert run_checks(tags=[Tags.caches]) == [
            Warning(
                "Unknown `CACHEX_ORM` settings: TIMEOUTS.",
                hint="The ORM cache ignores them. Settings are upper case: " + ", ".join(DEFAULTS) + ".",
                id="cachex_orm.W004",
            ),
        ]

    with override_orm_settings(CACHE="undefined"):
        assert run_checks(tags=[Tags.caches]) == [
            Error(
                "`CACHEX_ORM['CACHE']` is 'undefined', which is not defined in `CACHES`.",
                hint="Set `CACHEX_ORM['CACHE']` to an alias from `CACHES`.",
                id="cachex_orm.E003",
            ),
        ]

    dummy = {"BACKEND": "django.core.cache.backends.dummy.DummyCache"}
    with override_settings(CACHES={**settings.CACHES, "dummy": dummy}), override_orm_settings(CACHE="dummy"):
        assert run_checks(tags=[Tags.caches]) == [
            Warning(
                "The ORM cache cannot use the cache 'dummy' "
                "(django.core.cache.backends.dummy.DummyCache), so it caches nothing.",
                hint="Use a django-cachex Redis or Valkey backend, TrackingCache or LocMemCache.",
                id="cachex_orm.W001",
            ),
        ]
        assert_query_cached(Test.objects.all(), after=1)


@pytest.mark.skipif(
    settings.CACHES[DEFAULT_CACHE_ALIAS]["BACKEND"] == "django_cachex.cache.LocMemCache",
    reason="only the Redis stores go through the cache's serializer",
)
def test_cache_serializer_check():
    cache = {**settings.CACHES[DEFAULT_CACHE_ALIAS]}
    if "transport" in cache.get("OPTIONS", {}):
        cache = {**settings.CACHES[cache["OPTIONS"]["transport"]]}
    cache["OPTIONS"] = {**cache.get("OPTIONS", {}), "serializer": "django_cachex.serializers.json.JsonSerializer"}
    with override_settings(CACHES={**settings.CACHES, "json": cache}), override_orm_settings(CACHE="json"):
        assert run_checks(tags=[Tags.caches]) == [
            Error(
                "The serializer of the cache 'json' does not round-trip query results.",
                hint="Results must come back with their Python types; the default pickle serializer does.",
                id="cachex_orm.E004",
            ),
        ]


def call_get_tables(mocker):
    qs = Test.objects.all()
    compiler_mock = mocker.MagicMock()
    compiler_mock._cachex_orm_generated_sql = ""
    tables = _get_tables(qs.db, qs.query, compiler_mock)
    assert tables
    return tables


@override_orm_settings(FINAL_SQL_CHECK=True)
def test_final_sql_check_when_true(mocker):
    get_tables_from_sql = mocker.patch("django_cachex.orm.utils._get_tables_from_sql", return_value={"patched"})
    tables = call_get_tables(mocker)
    get_tables_from_sql.assert_called_once()
    assert "patched" in tables


@override_orm_settings(FINAL_SQL_CHECK=False)
def test_final_sql_check_when_false(mocker):
    get_tables_from_sql = mocker.patch("django_cachex.orm.utils._get_tables_from_sql", return_value={"patched"})
    tables = call_get_tables(mocker)
    get_tables_from_sql.assert_not_called()
    assert "patched" not in tables
