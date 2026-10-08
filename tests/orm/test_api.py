# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from io import StringIO

import pytest
from django.contrib.auth.models import User
from django.core.cache import DEFAULT_CACHE_ALIAS
from django.core.management import CommandError, call_command
from django.db import DEFAULT_DB_ALIAS, connection, transaction

from django_cachex.orm.api import invalidate, orm_cache_disabled, table_generations
from tests.orm.app.models import Test
from tests.orm.utils import assert_num_queries, assert_query_cached, override_orm_settings

pytestmark = pytest.mark.django_db(transaction=True, databases="__all__")


@pytest.fixture(autouse=True)
def t1():
    return Test.objects.create(name="test1")


@pytest.fixture
def t2():
    return Test.objects.using("second").create(name="test2")


@pytest.fixture
def user():
    return User.objects.create_user("test")


@pytest.mark.parametrize(
    "table_or_model",
    [
        pytest.param("ormtest_test", id="tables"),
        pytest.param("ormtest.Test", id="models_lookups"),
        pytest.param(Test, id="models"),
    ],
)
def test_invalidate(table_or_model):
    with assert_num_queries(1):
        assert list(Test.objects.values_list("name", flat=True)) == ["test1"]

    # On the driver's connection, which the ORM cache does not watch.
    connection.connection.execute("INSERT INTO ormtest_test (name, public) VALUES ('test2', TRUE)")

    with assert_num_queries(0):
        assert list(Test.objects.values_list("name", flat=True)) == ["test1"]

    invalidate(table_or_model)

    with assert_num_queries(1):
        assert list(Test.objects.values_list("name", flat=True)) == ["test1", "test2"]


def test_invalidate_all():
    with assert_num_queries(1):
        Test.objects.get()

    with assert_num_queries(0):
        Test.objects.get()

    invalidate()

    with assert_num_queries(1):
        Test.objects.get()


def test_invalidate_all_in_atomic():
    with transaction.atomic():
        with assert_num_queries(1):
            Test.objects.get()

        with assert_num_queries(0):
            Test.objects.get()

        invalidate()

        with assert_num_queries(1):
            Test.objects.get()

    with assert_num_queries(1):
        Test.objects.get()


def test_table_generations():
    generations = table_generations(Test)
    assert generations is not None
    assert len(generations) == 1
    assert table_generations(Test) == generations
    assert table_generations("ormtest.Test") == generations
    assert table_generations("ormtest_test") == generations

    Test.objects.create(name="test2")
    assert table_generations(Test) != generations


def test_table_generations_of_several_tables():
    generations = table_generations(Test, User)
    assert generations == table_generations(Test) + table_generations(User)

    invalidate(User)

    test_generation, user_generation = table_generations(Test, User)
    assert test_generation == generations[0]
    assert user_generation != generations[1]


def test_table_generations_needs_a_table():
    with pytest.raises(TypeError):
        table_generations()


def test_table_generations_when_not_cached():
    with override_orm_settings(ENABLED=False):
        assert table_generations(Test) is None
    with orm_cache_disabled():
        assert table_generations(Test) is None
    with override_orm_settings(UNCACHABLE_TABLES=("ormtest_test",)):
        assert table_generations(Test, User) is None
    with override_orm_settings(DATABASES=[]):
        assert table_generations(Test) is None


def test_table_generations_during_a_write():
    before = table_generations(Test)
    during = []

    def read_generations(execute, sql, params, many, context):
        during.append(table_generations(Test))
        return execute(sql, params, many, context)

    with connection.execute_wrapper(read_generations):
        Test.objects.create(name="test2")

    # A value computed from the old rows during the write is cached under generations that change after it.
    [during_write] = during
    assert during_write != before
    assert table_generations(Test) != during_write


def test_table_generations_in_a_transaction():
    before = table_generations(Test)
    with transaction.atomic():
        assert table_generations(Test) == before
        Test.objects.create(name="test2")
        # The transaction's own write is not committed yet.
        assert table_generations(Test) is None
        assert table_generations(User) is not None
    after = table_generations(Test)
    assert after is not None
    assert after != before


def test_orm_cache_disabled(t1):
    qs = Test.objects.all()
    assert_query_cached(qs)
    with orm_cache_disabled():
        with assert_num_queries(1):
            assert list(qs.all()) == [t1]
        assert_query_cached(qs, after=1)
    with assert_num_queries(0):
        assert list(qs.all()) == [t1]


def test_orm_cache_disabled_still_invalidates(t1):
    qs = Test.objects.all()
    assert_query_cached(qs)
    with orm_cache_disabled():
        t2 = Test.objects.create(name="test2")
    with assert_num_queries(1):
        assert list(qs.all()) == [t1, t2]


def test_orm_cache_disabled_nested():
    qs = Test.objects.all()
    with orm_cache_disabled():
        with orm_cache_disabled():
            pass
        assert_query_cached(qs, after=1)
    assert_query_cached(qs)


def test_invalidate_orm_cache(t1, user):
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t1]
    call_command("invalidate_orm_cache", verbosity=0)
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t1]

    call_command("invalidate_orm_cache", "auth", verbosity=0)
    with assert_num_queries(0):
        assert list(Test.objects.all()) == [t1]

    call_command("invalidate_orm_cache", "ormtest", verbosity=0)
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t1]

    call_command("invalidate_orm_cache", "ormtest.testchild", verbosity=0)
    with assert_num_queries(0):
        assert list(Test.objects.all()) == [t1]

    call_command("invalidate_orm_cache", "ormtest.test", verbosity=0)
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t1]

    with assert_num_queries(1):
        assert list(User.objects.all()) == [user]
    call_command("invalidate_orm_cache", "ormtest.test", "auth.user", verbosity=0)
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t1]
    with assert_num_queries(1):
        assert list(User.objects.all()) == [user]


def test_invalidate_orm_cache_app_includes_many_to_many_tables():
    permissions = User.user_permissions.through.objects.all()
    with assert_num_queries(1):
        assert list(permissions.all()) == []
    call_command("invalidate_orm_cache", "auth", verbosity=0)
    with assert_num_queries(1):
        assert list(permissions.all()) == []


def test_invalidate_orm_cache_app_without_models(t1):
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t1]
    out = StringIO()
    call_command("invalidate_orm_cache", "cachex_orm", stdout=out)
    assert out.getvalue() == "No models to invalidate.\n"
    with assert_num_queries(0):
        assert list(Test.objects.all()) == [t1]


@pytest.mark.parametrize("label", ["unknown", "ormtest.unknown", "ormtest.test.name"])
def test_invalidate_orm_cache_unknown_label(label):
    with pytest.raises(CommandError):
        call_command("invalidate_orm_cache", label, verbosity=0)


def test_invalidate_orm_cache_output():
    out = StringIO()
    call_command("invalidate_orm_cache", "ormtest.test", "ormtest", stdout=out)
    assert out.getvalue() == "Invalidating 8 models...\nORM cache invalidated.\n"
    out = StringIO()
    call_command("invalidate_orm_cache", "ormtest.test", stdout=out, db_alias=DEFAULT_DB_ALIAS)
    assert out.getvalue() == f"Invalidating 1 model for database '{DEFAULT_DB_ALIAS}'...\nORM cache invalidated.\n"
    out = StringIO()
    call_command("invalidate_orm_cache", stdout=out, cache_alias=DEFAULT_CACHE_ALIAS)
    assert out.getvalue() == f"Invalidating all tables on cache '{DEFAULT_CACHE_ALIAS}'...\nORM cache invalidated.\n"


def test_invalidate_orm_cache_multi_db(t1, t2):
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t1]
    call_command("invalidate_orm_cache", verbosity=0, db_alias="second")
    with assert_num_queries(0):
        assert list(Test.objects.all()) == [t1]

    with assert_num_queries(1, using="second"):
        assert list(Test.objects.using("second")) == [t2]
    call_command("invalidate_orm_cache", verbosity=0, db_alias="second")
    with assert_num_queries(1, using="second"):
        assert list(Test.objects.using("second")) == [t2]


def test_invalidate_orm_cache_multi_cache(t1):
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t1]
    call_command("invalidate_orm_cache", verbosity=0, cache_alias="other")
    with assert_num_queries(0):
        assert list(Test.objects.all()) == [t1]

    with assert_num_queries(1), override_orm_settings(CACHE="other"):
        assert list(Test.objects.all()) == [t1]
    call_command("invalidate_orm_cache", verbosity=0, cache_alias="other")
    with assert_num_queries(1), override_orm_settings(CACHE="other"):
        assert list(Test.objects.all()) == [t1]
