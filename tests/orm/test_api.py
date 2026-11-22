# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

import re
from contextlib import nullcontext
from dataclasses import dataclass
from decimal import Decimal
from io import StringIO
from typing import Any, NamedTuple

import pytest
from django.conf import settings
from django.contrib.auth.models import Permission, User
from django.core.cache import DEFAULT_CACHE_ALIAS, caches
from django.core.management import CommandError, call_command
from django.db import DEFAULT_DB_ALIAS, OperationalError, connection, connections, transaction

from django_cachex.orm.api import cached_data, invalidate, orm_cache_disabled, table_generations
from django_cachex.orm.exceptions import UndeclaredTableWarning
from django_cachex.orm.store import LocMemStore
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


def test_cached_data_stores_the_result():
    names = cached_names(Test)
    assert names() == ("test1",)

    assert names() == ("test1",)
    assert len(names.calls) == 1


def test_cached_data_shares_results_between_decorations_of_one_function():
    assert cached_names(Test)() == ("test1",)

    again = cached_names(Test)
    assert again() == ("test1",)
    assert again.calls == []


def test_cached_data_keeps_the_results_of_another_version_apart():
    cached_names(Test)()

    other = cached_names(Test, version=1)
    assert other() == ("test1",)
    assert len(other.calls) == 1


def test_cached_data_keeps_the_results_of_changed_code_apart():
    def names():
        return tuple(Test.objects.values_list("name", flat=True))

    def changed():
        return tuple(name.upper() for name in Test.objects.values_list("name", flat=True))

    changed.__qualname__ = names.__qualname__

    assert cached_data(Test)(names)() == ("test1",)
    assert cached_data(Test)(changed)() == ("TEST1",)


def names_with_closure(suffix):
    @cached_data(Test)
    def names():
        return tuple(name + suffix for name in Test.objects.values_list("name", flat=True))

    return names


def names_with_default(suffix):
    @cached_data(Test)
    def names(suffix=suffix):
        return tuple(name + suffix for name in Test.objects.values_list("name", flat=True))

    return names


@pytest.mark.parametrize("make", [names_with_closure, names_with_default])
def test_cached_data_keeps_the_results_of_other_closures_and_defaults_apart(make):
    assert make("a")() == ("test1a",)
    assert make("b")() == ("test1b",)


def test_cached_data_keeps_the_results_of_swapped_parameters_apart():
    @cached_data(Test)
    def pick(first, second):
        return first

    picked = pick(first="a", second="b")

    @cached_data(Test)
    def pick(second, first):
        return second

    assert (picked, pick(first="a", second="b")) == ("a", "b")


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


def test_cached_data_does_not_store_a_result_with_its_own_uncommitted_write():
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
    "outer",
    [pytest.param(nullcontext, id="transaction"), pytest.param(transaction.atomic, id="savepoint")],
)
def test_cached_data_does_not_store_a_result_with_its_own_rolled_back_write(outer):
    made = ["rolled_back"]

    @cached_data(Test)
    def names():
        with transaction.atomic():
            if made:
                Test.objects.create(name=made.pop())
            result = tuple(Test.objects.values_list("name", flat=True))
            transaction.set_rollback(True)
        return result

    with outer():
        assert names() == ("rolled_back", "test1")
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
    names = cached_names(Test)

    with not_cached():
        assert names() == ("test1",)
        assert names() == ("test1",)
    assert len(names.calls) == 2


def test_cached_data_follows_a_settings_change():
    names = cached_names(Test)
    names()

    with override_orm_settings(ENABLED=False):
        names()
    names()

    assert len(names.calls) == 2


@pytest.mark.parametrize(
    ("method", "logged"),
    [
        pytest.param("lookup", "Could not read a cached_data() result", id="lookup"),
        pytest.param("store", "Could not store a cached_data() result", id="store"),
    ],
)
def test_cached_data_runs_every_call_when_the_cache_fails(mocker, caplog, method, logged):
    names = cached_names(Test)
    mocker.patch.object(type(orm_store()), method, side_effect=ConnectionError("down"))

    assert names() == ("test1",)
    assert names() == ("test1",)

    assert logged in caplog.text
    assert len(names.calls) == 2


def test_cached_data_stores_a_result_per_arguments():
    names = cached_names(Test)
    row = Row("a", (Pair(1, "b"), frozenset({Decimal(2)})))
    names(1)
    names(2)
    names(1, upper=True, strip=False)
    names(1, strip=False, upper=True)
    names(row)
    names(Row("a", (Pair(1, "b"), frozenset({Decimal(2)}))))
    names(1)
    assert names.calls == [((1,), {}), ((2,), {}), ((1,), {"upper": True, "strip": False}), ((row,), {})]


@pytest.mark.parametrize(
    "argument",
    [
        pytest.param([1], id="list"),
        pytest.param({"a": 1}, id="dict"),
        pytest.param(Test(name="a"), id="model instance"),
        pytest.param(Mutable("a"), id="dataclass that is not frozen"),
        pytest.param(Row("a", ["b"]), id="list in a frozen dataclass"),
    ],
)
def test_cached_data_rejects_an_argument_a_caller_can_change(argument):
    names = cached_names(Test)
    with pytest.raises(TypeError, match="can change"):
        names(argument)
    with orm_cache_disabled(), pytest.raises(TypeError, match="can change"):
        names(key=argument)
    assert names.calls == []


def test_cached_data_returns_a_result_a_caller_can_change_as_a_copy(t1):
    calls = []

    @cached_data(Test)
    def names_by_pk():
        calls.append(1)
        return dict(Test.objects.values_list("pk", "name"))

    names_by_pk().clear()
    names_by_pk().clear()

    assert names_by_pk() == {t1.pk: "test1"}
    assert len(calls) == 1


def test_cached_data_shares_a_result_no_caller_can_change():
    tracking = settings.CACHES[DEFAULT_CACHE_ALIAS].get("OPTIONS", {}).get("tracking")
    if not tracking and not isinstance(orm_store(), LocMemStore):
        pytest.skip("needs a local memory cache or client tracking")
    names = cached_names(Test)
    names()
    names()
    kept = names()

    assert names() is kept


def test_cached_data_rejects_a_queryset_result():
    @cached_data(Test)
    def tests():
        return Test.objects.all()

    with pytest.raises(TypeError, match="QuerySet"):
        tests()


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
def test_cached_data_does_not_store_a_result_read_from_an_undeclared_table(read, options, reported):
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
    calls = []

    @cached_data(*declared)
    def pks(user_pk):
        calls.append(user_pk)
        return tuple(read(User(pk=user_pk)).values_list("pk", flat=True))

    assert len(pks(user.pk)) == 1
    pks(user.pk)
    assert calls == [user.pk]


@pytest.mark.filterwarnings("error::django_cachex.orm.exceptions.UndeclaredTableWarning")
@override_orm_settings(UNCACHABLE_TABLES=("auth_user",))
def test_cached_data_accepts_a_trimmed_join_to_an_uncachable_table(user):
    Test.objects.create(name="owned", owner=user)

    @cached_data(Test)
    def owned(user_pk):
        return tuple(Test.objects.filter(owner__id=user_pk).values_list("name", flat=True))

    with assert_num_queries(1):
        assert owned(user.pk) == ("owned",)
    with assert_num_queries(0):
        assert owned(user.pk) == ("owned",)
        assert list(Test.objects.filter(owner__id=user.pk).values_list("name", flat=True)) == ["owned"]


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


@pytest.mark.parametrize(
    ("args", "options"),
    [
        pytest.param((), {}, id="no tables"),
        pytest.param((lambda: None,), {}, id="no parentheses"),
        pytest.param((Test,), {"version": 1.5}, id="float version"),
        pytest.param((Test,), {"version": True}, id="bool version"),
    ],
)
def test_cached_data_rejects_bad_arguments(args, options):
    with pytest.raises(TypeError):
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
    assert (after["hits"], after["misses"]) == (before["hits"] + 2, before["misses"])


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
