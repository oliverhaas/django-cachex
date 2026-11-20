# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

import re
from dataclasses import dataclass
from decimal import Decimal
from io import StringIO
from types import MappingProxyType
from typing import Any, NamedTuple

import pytest
from django.conf import settings
from django.contrib.auth.models import Permission, User
from django.core.cache import DEFAULT_CACHE_ALIAS, caches
from django.core.management import CommandError, call_command
from django.db import DEFAULT_DB_ALIAS, OperationalError, connection, connections, transaction

from django_cachex.orm.api import cached_data, invalidate, orm_cache_disabled, table_generations
from django_cachex.orm.exceptions import UndeclaredTableWarning
from tests.orm.app.models import Test
from tests.orm.utils import assert_num_queries, assert_query_cached, orm_store, override_orm_settings

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


def test_invalidate_all_with_an_unreachable_database(mocker, t1):
    with assert_num_queries(1, using="second"):
        assert list(Test.objects.using("second")) == []
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t1]
    mocker.patch.object(
        connections[DEFAULT_DB_ALIAS].introspection,
        "table_names",
        side_effect=OperationalError("unreachable"),
    )

    invalidate()

    with assert_num_queries(1, using="second"):
        assert list(Test.objects.using("second")) == []
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [t1]


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


def cached_names(*tables_or_models, **options):
    """A cached_data() function that returns the names of the Test rows; ``calls`` lists its runs."""
    calls = []

    @cached_data(*tables_or_models, **options)
    def names(*args, **kwargs):
        calls.append((args, kwargs))
        return tuple(Test.objects.values_list("name", flat=True))

    names.calls = calls
    return names


@dataclass(frozen=True)
class Row:
    name: str
    tags: Any


class Pair(NamedTuple):
    left: Any
    right: Any


@dataclass
class Mutable:
    name: str


def test_cached_data_holds_the_result():
    names = cached_names(Test)
    first = names()
    assert first == ("test1",)

    with assert_num_queries(0):
        assert names() is first
    assert len(names.calls) == 1


@pytest.mark.parametrize(
    "change",
    [
        pytest.param(lambda: Test.objects.create(name="test2"), id="write"),
        pytest.param(lambda: invalidate(Test), id="invalidate"),
    ],
)
def test_cached_data_runs_again_after_a_change(change):
    names = cached_names(Test)
    names()

    change()

    assert names() == tuple(Test.objects.values_list("name", flat=True))
    assert len(names.calls) == 2


@pytest.mark.parametrize(
    ("declared", "options"),
    [
        pytest.param(Test, {}, id="declared"),
        pytest.param(User, {"TABLE_KEYGEN": lambda **kwargs: "shared"}, id="sharing a key"),
    ],
)
def test_cached_data_in_a_transaction_that_wrote_a_declared_table(declared, options):
    with override_orm_settings(**options):
        names = cached_names(declared)
        assert names() == ("test1",)
        with transaction.atomic():
            Test.objects.create(name="test2")
            assert names() == ("test1", "test2")
            transaction.set_rollback(True)
        assert names() == ("test1",)


def test_cached_data_does_not_hold_a_result_with_its_own_uncommitted_write():
    made = ["test2"]

    @cached_data(Test)
    def names():
        if made:
            Test.objects.create(name=made.pop())
        return tuple(Test.objects.values_list("name", flat=True))

    with transaction.atomic():
        assert names() == ("test1", "test2")
        transaction.set_rollback(True)
    assert names() == ("test1",)


@pytest.mark.parametrize(
    "not_cached",
    [
        pytest.param(orm_cache_disabled, id="orm_cache_disabled"),
        pytest.param(lambda: override_orm_settings(ENABLED=False), id="disabled"),
        pytest.param(lambda: override_orm_settings(TIMEOUT=0), id="timeout"),
        pytest.param(lambda: override_orm_settings(DATABASES=["second"]), id="database"),
        pytest.param(lambda: override_orm_settings(UNCACHABLE_TABLES=("ormtest_test",)), id="uncachable"),
    ],
)
@pytest.mark.filterwarnings("error::django_cachex.orm.exceptions.UndeclaredTableWarning")
def test_cached_data_runs_every_call_when_not_cached(not_cached):
    calls = []

    @cached_data(Test)
    def names_by_pk():
        calls.append(1)
        return dict(Test.objects.values_list("pk", "name"))

    with not_cached():
        first = names_by_pk()
        assert names_by_pk() == first
    assert isinstance(first, MappingProxyType)
    assert len(calls) == 2


def test_cached_data_when_the_generations_cannot_be_read(mocker, caplog):
    names = cached_names(Test)
    mocker.patch.object(type(orm_store()), "read_generations", side_effect=ConnectionError("down"))

    assert names() == ("test1",)

    assert "Could not read table generations" in caplog.text
    assert names() == ("test1",)
    assert len(names.calls) == 2


def test_cached_data_holds_a_result_per_arguments():
    names = cached_names(Test)
    names(1)
    names(2)
    names(1, upper=True, strip=False)
    names(1, strip=False, upper=True)
    names(1)
    assert names.calls == [((1,), {}), ((2,), {}), ((1,), {"upper": True, "strip": False})]


def test_cached_data_rejects_an_unhashable_argument():
    names = cached_names(Test)
    with pytest.raises(TypeError, match="unhashable"):
        names([1])
    with orm_cache_disabled(), pytest.raises(TypeError, match="unhashable"):
        names(key=[1])
    assert names.calls == []


def test_cached_data_drops_the_least_recently_used_result():
    names = cached_names(Test, maxsize=2)
    names(1)
    names(2)
    names(1)
    names(3)
    names(1)
    names(2)
    assert [args for args, _ in names.calls] == [(1,), (2,), (3,), (2,)]


def test_cached_data_returns_a_returned_dict_read_only():
    @cached_data(Test)
    def names_by_pk():
        return dict(Test.objects.values_list("pk", "name"))

    first = names_by_pk()
    assert first == {Test.objects.get().pk: "test1"}
    with pytest.raises(TypeError):
        first[0] = "test0"  # ty: ignore[invalid-assignment]
    assert names_by_pk() is first


@pytest.mark.parametrize(
    ("result", "path"),
    [
        pytest.param(lambda: [1], "f()", id="list"),
        pytest.param(lambda: (1, {2}), "f()[1]", id="set"),
        pytest.param(lambda: {"a": {"b": 1}}, "f()['a']", id="nested dict"),
        pytest.param(lambda: (Test.objects.get(),), "f()[0]", id="model instance"),
        pytest.param(lambda: Pair(1, Test.objects.all()), "f().right", id="queryset"),
        pytest.param(lambda: (iter(()),), "f()[0]", id="iterator"),
        pytest.param(lambda: (Row("a", ["b"]),), "f()[0].tags", id="list in a frozen dataclass"),
        pytest.param(lambda: (Mutable("a"),), "f()[0]", id="dataclass that is not frozen"),
        pytest.param(lambda: {Test(pk=1, name="a"): 1}, "f() key <Test: a>", id="model instance key"),
    ],
)
def test_cached_data_rejects_results_callers_could_change(result, path):
    @cached_data(Test)
    def f():
        return result()

    with pytest.raises(TypeError, match=re.escape(f".{path} is ")):
        f()


def test_cached_data_accepts_frozen_results():
    @cached_data(Test)
    def f():
        return (Row("a", (Pair(1, "b"),)), frozenset({2}), MappingProxyType({"c": (Decimal(3), None)}))

    assert f() == (Row("a", (Pair(1, "b"),)), frozenset({2}), MappingProxyType({"c": (Decimal(3), None)}))


def count_users_with_the_cache_disabled():
    with orm_cache_disabled():
        return User.objects.count()


@pytest.mark.parametrize(
    ("read", "options", "reported"),
    [
        pytest.param(User.objects.count, {}, "auth_user", id="table"),
        pytest.param(
            lambda: Test.objects.using("second").count(),
            {},
            "ormtest_test on database 'second'",
            id="database",
        ),
        pytest.param(
            lambda: Test.objects.using("second").count(),
            {"DATABASES": ["default"]},
            "ormtest_test on database 'second'",
            id="database outside DATABASES",
        ),
        pytest.param(
            User.objects.count,
            {"UNCACHABLE_TABLES": ("auth_user",)},
            "auth_user, which the ORM cache does not cache",
            id="uncachable",
        ),
        pytest.param(
            count_users_with_the_cache_disabled,
            {},
            "auth_user, which the ORM cache does not cache",
            id="orm_cache_disabled()",
        ),
    ],
)
def test_cached_data_does_not_hold_a_result_read_from_an_undeclared_table(read, options, reported):
    calls = []

    @cached_data(Test)
    def f():
        calls.append(1)
        return (Test.objects.count(), read())

    with override_orm_settings(**options):
        for _ in range(2):
            with pytest.warns(UndeclaredTableWarning, match=re.escape(reported)) as record:
                f()
            assert record[0].filename == __file__
    assert len(calls) == 2


def cached_usernames(*tables_or_models):
    """A cached_data() function that returns the usernames; ``calls`` counts its runs."""
    calls = []

    @cached_data(*tables_or_models)
    def usernames():
        calls.append(1)
        return tuple(User.objects.values_list("username", flat=True))

    usernames.calls = calls
    return usernames


@pytest.mark.filterwarnings("error::django_cachex.orm.exceptions.UndeclaredTableWarning")
def test_cached_data_accepts_a_table_sharing_a_declared_key(user):
    with override_orm_settings(TABLE_KEYGEN=lambda **kwargs: "shared"):
        usernames = cached_usernames(Test)
        assert usernames() == ("test",)
        usernames()
        assert len(usernames.calls) == 1


@pytest.mark.filterwarnings("error::django_cachex.orm.exceptions.UndeclaredTableWarning")
@pytest.mark.parametrize(
    ("declared", "read"),
    [
        pytest.param((Test,), lambda user: Test.objects.filter(owner_id=user.pk), id="foreign key id"),
        pytest.param(
            (Permission, User.user_permissions.through),
            lambda user: Permission.objects.filter(user__id=user.pk).order_by("pk"),
            id="many-to-many",
        ),
    ],
)
def test_cached_data_accepts_a_filter_on_a_trimmed_join(user, declared, read):
    permission = Permission.objects.first()
    Test.objects.create(name="owned", owner=user)
    user.user_permissions.add(permission)

    @cached_data(*declared)
    def pks(user_pk):
        return tuple(read(User(pk=user_pk)).values_list("pk", flat=True))

    first = pks(user.pk)
    assert len(first) == 1
    assert pks(user.pk) is first


@pytest.mark.filterwarnings("error::django_cachex.orm.exceptions.UndeclaredTableWarning")
@pytest.mark.parametrize("declared", [User, Test])
def test_cached_data_reports_a_nested_function_to_the_outer_one(declared):
    usernames = cached_usernames(declared)
    calls = []

    @cached_data(Test)
    def report():
        calls.append(1)
        return (Test.objects.count(), usernames())

    for _ in range(2):
        with pytest.warns(UndeclaredTableWarning, match="auth_user"):
            report()
    assert len(calls) == 2


@pytest.mark.parametrize("name", ["ormtest.Missing", "ormtest_missing"])
def test_cached_data_rejects_an_unknown_table(name):
    names = cached_names(name)
    with pytest.raises(LookupError, match=name):
        names()


def test_cached_data_forgets_its_results_when_the_settings_change():
    names = cached_names(Test)
    names()
    with override_orm_settings(TIMEOUT=60):
        names()
    assert len(names.calls) == 2


@pytest.mark.parametrize(
    ("args", "options", "error"),
    [
        pytest.param((), {}, TypeError, id="no tables"),
        pytest.param((lambda: None,), {}, TypeError, id="no parentheses"),
        pytest.param((Test,), {"maxsize": 1.5}, TypeError, id="float maxsize"),
        pytest.param((Test,), {"maxsize": 0}, ValueError, id="zero maxsize"),
    ],
)
def test_cached_data_rejects_bad_arguments(args, options, error):
    with pytest.raises(error):
        cached_data(*args, **options)


def test_cached_data_rejects_an_async_function():
    async def names():
        return ()

    with pytest.raises(TypeError, match="sync_to_async"):
        cached_data(Test)(names)


@pytest.mark.skipif(
    not settings.CACHES[DEFAULT_CACHE_ALIAS].get("OPTIONS", {}).get("tracking"),
    reason="needs client tracking",
)
def test_cached_data_hit_reads_local_copies():
    names = cached_names(Test)
    names()
    names()
    adapter = caches[DEFAULT_CACHE_ALIAS].adapter
    before = adapter.tracking_info()

    names()

    after = adapter.tracking_info()
    assert (after["hits"], after["misses"]) == (before["hits"] + 1, before["misses"])


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


def test_invalidate_orm_cache_when_the_cache_fails(mocker):
    mocker.patch("django_cachex.orm.api.get_store").return_value.bump.side_effect = ConnectionError("down")
    with pytest.raises(CommandError, match="Could not invalidate"):
        call_command("invalidate_orm_cache", verbosity=0)


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
