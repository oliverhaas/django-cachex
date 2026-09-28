# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

import datetime
import json
from decimal import Decimal
from uuid import UUID

import pytest
from django.db import connection
from django.db.models import Value
from django.db.models.functions import Now

from django_cachex.orm.utils import UncachableQuery, _param_key, _psycopg2_param_keys, _psycopg_param_keys
from tests.orm.app.models import SomeChoices, Test
from tests.orm.utils import assert_num_queries, assert_query_cached, assert_tables


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("final_sql_check")
def test_tuple():
    qs = Test.objects.filter(pk__in=(1, 2, 3))
    assert_tables(qs, Test)
    assert_query_cached(qs)

    qs = Test.objects.filter(pk__in=(4, 5, 6))
    assert_tables(qs, Test)
    assert_query_cached(qs)


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("final_sql_check")
def test_list():
    qs = Test.objects.filter(pk__in=[1, 2, 3])
    assert_tables(qs, Test)
    assert_query_cached(qs)

    pks = [4, 5, 6]
    qs = Test.objects.filter(pk__in=pks)
    assert_tables(qs, Test)
    assert_query_cached(qs)

    pks.append(7)
    assert_tables(qs, Test)
    # filter() copied the list, so the new element changes nothing.
    assert_query_cached(qs, before=0)

    qs = Test.objects.filter(pk__in=pks)
    assert_tables(qs, Test)
    assert_query_cached(qs)


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("final_sql_check")
def test_binary():
    """Binary data is cached on PostgreSQL, but SQLite gets a ``memory`` object, an unknown parameter."""
    after = 1 if connection.vendor == "sqlite" else 0
    qs = Test.objects.filter(bin=None)
    assert_tables(qs, Test)
    assert_query_cached(qs)

    qs = Test.objects.filter(bin=b"abc")
    assert_tables(qs, Test)
    assert_query_cached(qs, after=after)

    qs = Test.objects.filter(bin=b"def")
    assert_tables(qs, Test)
    assert_query_cached(qs, after=after)


@pytest.mark.django_db(transaction=True)
def test_long_parameters():
    # Long values sharing a prefix get their own keys, although psycopg shortens their repr.
    after = 1 if connection.vendor == "sqlite" else 0
    prefix = "x" * 60
    for n in (1, 2):
        Test.objects.create(name=f"test{n}", json={"key": prefix, "n": n}, bin=f"{prefix}{n}".encode())
    for n in (1, 2):
        qs = Test.objects.filter(json={"key": prefix, "n": n}).values_list("name", flat=True)
        assert_query_cached(qs, [f"test{n}"])
        qs = Test.objects.filter(bin=f"{prefix}{n}".encode()).values_list("name", flat=True)
        assert_query_cached(qs, [f"test{n}"], after=after)


@pytest.mark.django_db(transaction=True)
def test_parameter_types():
    # The same SQL with 1 and "1" returns an int and a str.
    Test.objects.create(name="test1")
    for value in (1, "1"):
        qs = Test.objects.annotate(value=Value(value)).values_list("value", flat=True)
        assert_query_cached(qs, [value])


@pytest.mark.django_db(transaction=True)
def test_float():
    with assert_num_queries(1):
        Test.objects.create(name="test1", a_float=0.123456789)
    with assert_num_queries(1):
        Test.objects.create(name="test2", a_float=12345.6789)

    qs = Test.objects.values_list("a_float", flat=True).filter(a_float__isnull=False).order_by("a_float")
    assert_query_cached(qs)
    assert list(qs) == pytest.approx([0.123456789, 12345.6789], abs=0.0001)

    with assert_num_queries(1):
        Test.objects.get(a_float=0.123456789)
    with assert_num_queries(0):
        Test.objects.get(a_float=0.123456789)


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("final_sql_check")
def test_decimal():
    with assert_num_queries(1):
        Test.objects.create(name="test1", a_decimal=Decimal("123.45"))
    with assert_num_queries(1):
        Test.objects.create(name="test2", a_decimal=Decimal("12.3"))

    qs = Test.objects.values_list("a_decimal", flat=True).filter(a_decimal__isnull=False).order_by("a_decimal")
    assert_tables(qs, Test)
    assert_query_cached(qs, [Decimal("12.3"), Decimal("123.45")])

    with assert_num_queries(1):
        Test.objects.get(a_decimal=Decimal("123.45"))
    with assert_num_queries(0):
        Test.objects.get(a_decimal=Decimal("123.45"))


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("final_sql_check")
@pytest.mark.parametrize(
    ("low", "high"),
    [
        pytest.param("127.0.0.1", "192.168.0.1", id="ipv4"),
        pytest.param("2001:db8:0:85a3::ac1f:8001", "2001:db8:a0b:12f0::1", id="ipv6"),
    ],
)
def test_ip_address(low, high):
    with assert_num_queries(1):
        Test.objects.create(name="test1", ip=high)
    with assert_num_queries(1):
        Test.objects.create(name="test2", ip=low)

    qs = Test.objects.values_list("ip", flat=True).filter(ip__isnull=False).order_by("ip")
    assert_tables(qs, Test)
    assert_query_cached(qs, [low, high])

    with assert_num_queries(1):
        Test.objects.get(ip=low)
    with assert_num_queries(0):
        Test.objects.get(ip=low)


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("final_sql_check")
def test_duration():
    with assert_num_queries(1):
        Test.objects.create(name="test1", duration=datetime.timedelta(30))
    with assert_num_queries(1):
        Test.objects.create(name="test2", duration=datetime.timedelta(60))

    qs = Test.objects.values_list("duration", flat=True).filter(duration__isnull=False).order_by("duration")
    assert_tables(qs, Test)
    assert_query_cached(qs, [datetime.timedelta(30), datetime.timedelta(60)])

    with assert_num_queries(1):
        Test.objects.get(duration=datetime.timedelta(30))
    with assert_num_queries(0):
        Test.objects.get(duration=datetime.timedelta(30))


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("final_sql_check")
def test_uuid():
    first = UUID("1cc401b7-09f4-4520-b8d0-c267576d196b")
    second = UUID("ebb3b6e1-1737-4321-93e3-4c35d61ff491")
    with assert_num_queries(1):
        Test.objects.create(name="test1", uuid=str(first))
    with assert_num_queries(1):
        Test.objects.create(name="test2", uuid=str(second))

    qs = Test.objects.values_list("uuid", flat=True).filter(uuid__isnull=False).order_by("uuid")
    assert_tables(qs, Test)
    assert_query_cached(qs, [first, second])

    with assert_num_queries(1):
        Test.objects.get(uuid=first)
    with assert_num_queries(0):
        Test.objects.get(uuid=first)


@pytest.mark.django_db(transaction=True)
def test_now_is_not_cached():
    obj = Test.objects.create(datetime="1992-07-02T12:00:00")
    qs = Test.objects.filter(datetime__lte=Now())
    with assert_num_queries(1):
        first = qs.get()
    with assert_num_queries(1):
        second = qs.get()
    assert first == second == obj


def test_param_key_tells_types_and_values_apart():
    values = [1, "1", 1.0, True, b"1", bytearray(b"1"), Decimal(1), Decimal("1.0"), None, "None", [1], (1,)]
    values += [{"a": 1}, {"a": "1"}, datetime.date(2026, 1, 1), datetime.datetime(2026, 1, 1)]
    keys = [_param_key(value) for value in values]
    assert len(set(keys)) == len(keys), keys


def test_param_key_of_a_choice_is_its_value():
    assert _param_key(SomeChoices.foo) == _param_key("foo")


@pytest.mark.parametrize(
    "value",
    [object(), [1, object()], {"a": object()}, memoryview(b"a")],
    ids=["object", "list", "dict", "memoryview"],
)
def test_param_key_rejects_uncachable_values(value):
    with pytest.raises(UncachableQuery):
        _param_key(value)


@pytest.mark.parametrize("wrapper_name", ["Json", "Jsonb"])
def test_psycopg_json(wrapper_name):
    wrapper = getattr(pytest.importorskip("psycopg.types.json"), wrapper_name)
    key = _psycopg_param_keys()[wrapper]
    prefix = "x" * 60
    first = key(wrapper({"key": prefix, "n": 1}, dumps=json.dumps))
    assert first != key(wrapper({"key": prefix, "n": 2}, dumps=json.dumps))
    assert prefix in first
    # Serialized by a function set on the connection.
    with pytest.raises(UncachableQuery):
        key(wrapper({"key": prefix}))
    with pytest.raises(UncachableQuery):
        key(wrapper({"key": object()}, dumps=json.dumps))


def test_psycopg():
    pytest.importorskip("psycopg")
    from psycopg.dbapi20 import Binary
    from psycopg.types.json import Json, Jsonb
    from psycopg.types.range import Range

    key = _psycopg_param_keys()
    prefix = "x" * 60
    assert key[Json](Json(1, dumps=json.dumps)) != key[Jsonb](Jsonb(1, dumps=json.dumps))
    assert key[Binary](Binary(f"{prefix}1".encode())) != key[Binary](Binary(f"{prefix}2".encode()))
    assert key[Binary](Binary(memoryview(b"a"))) == key[Binary](Binary(b"a"))
    with pytest.raises(UncachableQuery):
        key[Binary](Binary("a"))
    assert key[Range](Range(1, 2)) != key[Range](Range(1, 2, "[]"))


def test_psycopg2():
    pytest.importorskip("psycopg2")
    from psycopg2 import Binary
    from psycopg2.extras import Json, NumericRange

    key = _psycopg2_param_keys()
    prefix = "x" * 60
    first = key[Json](Json({"key": prefix, "name": "Jürgen"}))
    assert first != key[Json](Json({"key": prefix, "name": "Jörgen"}))
    assert prefix in first
    with pytest.raises(UncachableQuery):
        key[Json](Json({"key": object()}))
    binary = type(Binary(b""))
    assert key[binary](Binary(f"{prefix}1".encode())) != key[binary](Binary(f"{prefix}2".encode()))
    assert key[NumericRange](NumericRange(1, 2)) != key[NumericRange](NumericRange(1, 2, "[]"))
