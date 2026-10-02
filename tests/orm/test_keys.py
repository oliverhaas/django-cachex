"""Keys of cached results, generations and leases, and the keygens that name them."""

import pytest
from django.contrib.auth.models import User
from django.db import DEFAULT_DB_ALIAS, transaction
from django.db.models import Value
from django.db.models.sql.constants import MULTI

from django_cachex.orm.api import invalidate, table_generations
from django_cachex.orm.exceptions import InvalidationError
from django_cachex.orm.utils import QUERY_KEY_PREFIX_MAX_LENGTH, UncachableQuery, query_digest, readable_query_key
from tests.orm.app.models import Test
from tests.orm.utils import assert_num_queries, assert_query_cached, orm_store, override_orm_settings

pytestmark = pytest.mark.django_db(transaction=True)

MAX = QUERY_KEY_PREFIX_MAX_LENGTH


@pytest.fixture(autouse=True)
def t1():
    return Test.objects.create(name="test1")


def uncached_query_key(**kwargs):
    raise UncachableQuery


def digest_query_key(*, digest, **kwargs):
    return digest


def shared_table_key(**kwargs):
    return "shared"


def create_in_a_transaction():
    with transaction.atomic():
        Test.objects.create(name="test2")


@pytest.mark.parametrize(
    ("tables", "prefix"),
    [
        pytest.param({"b", "a"}, "a.b", id="sorted"),
        pytest.param({"a" * (MAX - 2), "b"}, "a" * (MAX - 2) + ".b", id="at_the_cap"),
        pytest.param({"a", "b", "c", "d" * (MAX - 5)}, "a.b.c.+1more", id="over_the_cap"),
        pytest.param({"a" * (MAX // 2), "b" * (MAX // 2), "c"}, "a" * (MAX // 2) + ".+2more", id="cut_in_the_middle"),
        pytest.param({"a" * MAX, "b"}, "a" * MAX + ".+1more", id="first_name_over_the_cap"),
        pytest.param({"a" * (MAX + 1)}, "a" * (MAX + 1), id="single_name_over_the_cap"),
    ],
)
def test_readable_query_key(tables, prefix):
    assert readable_query_key(compiler=None, tables=frozenset(tables), digest="d") == f"{prefix}:d"


def test_default_keys():
    queryset = Test.objects.all()
    list(queryset)
    digest = query_digest(queryset.query.get_compiler(queryset.db))
    assert orm_store().lookup(DEFAULT_DB_ALIAS, f"ormtest_test:{digest}:{MULTI}", ["ormtest_test"]).hit


@pytest.mark.parametrize(
    "keygen",
    [
        pytest.param(uncached_query_key, id="callable"),
        pytest.param("tests.orm.test_keys.uncached_query_key", id="dotted_path"),
    ],
)
def test_query_keygen_can_leave_a_query_uncached(keygen):
    with override_orm_settings(QUERY_KEYGEN=keygen):
        assert_query_cached(Test.objects.all(), after=1)


def test_result_type_follows_the_query_key():
    values = Test.objects.order_by().annotate(a=Value(1)).values_list("a")[:1]
    with override_orm_settings(QUERY_KEYGEN=digest_query_key), assert_num_queries(2) as context:
        assert Test.objects.exists()
        assert list(values) == [(1,)]
    first, second = (query["sql"] for query in context.captured_queries)
    assert first == second


@pytest.mark.parametrize(
    ("setting", "call", "error"),
    [
        pytest.param("QUERY_KEYGEN", lambda: list(Test.objects.all()), RuntimeError, id="query_key"),
        pytest.param("TABLE_KEYGEN", lambda: list(Test.objects.all()), RuntimeError, id="read"),
        pytest.param("TABLE_KEYGEN", lambda: invalidate(Test), RuntimeError, id="invalidate"),
        pytest.param("TABLE_KEYGEN", lambda: table_generations(Test), RuntimeError, id="table_generations"),
        pytest.param("TABLE_KEYGEN", lambda: Test.objects.create(name="test2"), InvalidationError, id="write"),
        pytest.param("TABLE_KEYGEN", create_in_a_transaction, InvalidationError, id="commit"),
    ],
)
def test_keygen_error(mocker, setting, call, error):
    with override_orm_settings(**{setting: mocker.Mock(side_effect=RuntimeError("keygen"))}), pytest.raises(error):
        call()
    assert transaction.get_autocommit()
    assert list(Test.objects.values_list("name", flat=True)) == ["test1"]


@pytest.mark.parametrize("query_key", [pytest.param(None, id="none"), pytest.param(b"key", id="bytes")])
def test_query_key_that_is_not_a_str(mocker, query_key):
    keygen = mocker.Mock(return_value=query_key)
    with override_orm_settings(QUERY_KEYGEN=keygen), pytest.raises(TypeError, match="QUERY_KEYGEN"):
        list(Test.objects.all())


def test_tables_sharing_a_key_invalidate_each_other():
    queryset = Test.objects.all()
    with override_orm_settings(TABLE_KEYGEN=shared_table_key):
        assert_query_cached(queryset)
        generations = table_generations(Test, User)
        assert len(generations) == 2
        User.objects.create_user("writer")
        assert table_generations(Test, User) != generations
        assert_query_cached(queryset)
        invalidate(User)
        assert_query_cached(queryset)
