# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from django.contrib.postgres.functions import TransactionNow
from django.core.management.color import no_style
from django.db import connection, transaction
from django.db.backends.postgresql.psycopg_any import DateRange, DateTimeTZRange, NumericRange

from django_cachex.orm.api import invalidate
from django_cachex.orm.utils import UncachableQuery
from tests.orm.app.models import PostgresModel, Test
from tests.orm.utils import assert_num_queries, assert_query_cached, assert_tables

pytestmark = [
    pytest.mark.skipif(connection.vendor != "postgresql", reason="This test is only for PostgreSQL"),
    pytest.mark.django_db(transaction=True),
]


@pytest.fixture(autouse=True)
def use_tz(settings):
    settings.USE_TZ = True


@pytest.fixture(autouse=True)
def rows(use_tz):
    obj1 = PostgresModel(
        int_array=[1, 2, 3],
        hstore={"a": "b", "c": None},
        int_range=[1900, 2000],
        date_range=["1678-03-04", "1741-07-28"],
        datetime_range=[datetime(1989, 1, 30, 12, 20, tzinfo=ZoneInfo("Europe/Paris")), None],
    )

    obj2 = PostgresModel(
        int_array=[4, None, 6],
        hstore={"a": "1", "b": "2"},
        int_range=[1989, None],
        date_range=["1989-01-30", None],
        datetime_range=[None, None],
    )

    obj1.decimal_range = [-1e3, 9.87654321]
    obj2.decimal_range = [0.0, None]

    obj1.save()
    obj2.save()
    yield SimpleNamespace(obj1=obj1, obj2=obj2)

    # The flush after each transactional test misses PostgresModel: https://code.djangoproject.com/ticket/29494
    with transaction.atomic(), connection.cursor() as cursor:
        for sql in connection.ops.sql_flush(no_style(), (PostgresModel._meta.db_table,)):
            cursor.execute(sql)


@pytest.mark.usefixtures("final_sql_check")
def test_unaccent():
    Test.objects.create(name="Clémentine")
    Test.objects.create(name="Clementine")
    qs = Test.objects.filter(name__unaccent="Clémentine").values_list("name", flat=True)
    assert_tables(qs, Test)
    assert_query_cached(qs, ["Clementine", "Clémentine"])


@pytest.mark.usefixtures("final_sql_check")
def test_int_array():
    with assert_num_queries(1):
        data1 = [o.int_array for o in PostgresModel.objects.all()]
    with assert_num_queries(1):
        data2 = list(PostgresModel.objects.values_list("int_array", flat=True))
    assert data2 == data1
    assert data2 == [[1, 2, 3], [4, None, 6]]

    invalidate(PostgresModel)

    qs = PostgresModel.objects.values_list("int_array", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [[1, 2, 3], [4, None, 6]])

    qs = PostgresModel.objects.filter(int_array__contains=[3]).values_list("int_array", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [[1, 2, 3]])

    qs = PostgresModel.objects.filter(int_array__contained_by=[1, 2, 3, 4, 5, 6]).values_list(
        "int_array",
        flat=True,
    )
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [[1, 2, 3]])

    qs = PostgresModel.objects.filter(int_array__overlap=[3, 4]).values_list("int_array", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [[1, 2, 3], [4, None, 6]])

    qs = PostgresModel.objects.filter(int_array__len__in=(2, 3)).values_list("int_array", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [[1, 2, 3], [4, None, 6]])

    qs = PostgresModel.objects.filter(int_array__2=6).values_list("int_array", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [[4, None, 6]])

    qs = PostgresModel.objects.filter(int_array__0_2=(1, 2)).values_list("int_array", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [[1, 2, 3]])


@pytest.mark.usefixtures("final_sql_check")
def test_hstore():
    with assert_num_queries(1):
        data1 = [o.hstore for o in PostgresModel.objects.all()]
    with assert_num_queries(1):
        data2 = list(PostgresModel.objects.values_list("hstore", flat=True))
    assert data2 == data1
    assert data2 == [{"a": "b", "c": None}, {"a": "1", "b": "2"}]

    invalidate(PostgresModel)

    qs = PostgresModel.objects.values_list("hstore", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [{"a": "b", "c": None}, {"a": "1", "b": "2"}])

    qs = PostgresModel.objects.filter(hstore__a="1").values_list("hstore", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [{"a": "1", "b": "2"}])

    qs = PostgresModel.objects.filter(hstore__contains={"a": "b"}).values_list("hstore", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [{"a": "b", "c": None}])

    qs = PostgresModel.objects.filter(hstore__contained_by={"a": "b", "c": None, "b": "2"}).values_list(
        "hstore",
        flat=True,
    )
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [{"a": "b", "c": None}])

    qs = PostgresModel.objects.filter(hstore__has_key="c").values_list("hstore", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [{"a": "b", "c": None}])

    qs = PostgresModel.objects.filter(hstore__has_keys=["a", "b"]).values_list("hstore", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [{"a": "1", "b": "2"}])

    qs = PostgresModel.objects.filter(hstore__keys=["a", "b"]).values_list("hstore", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [{"a": "1", "b": "2"}])

    qs = PostgresModel.objects.filter(hstore__values=["1", "2"]).values_list("hstore", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [{"a": "1", "b": "2"}])


def test_mutable_result_change():
    """Changing a mutable returned by a query has no effect on other executions of the query."""
    qs = PostgresModel.objects.values_list("int_array", flat=True)

    data = list(qs.all())
    assert data == [[1, 2, 3], [4, None, 6]]
    data[0].append(4)
    data[1].remove(4)
    data[1][0] = 5
    assert data == [[1, 2, 3, 4], [5, 6]]

    assert list(qs.all()) == [[1, 2, 3], [4, None, 6]]

    qs = PostgresModel.objects.values_list("hstore", flat=True)

    data = list(qs.all())
    assert data == [{"a": "b", "c": None}, {"a": "1", "b": "2"}]
    data[0]["d"] = "e"
    del data[0]["a"]
    data[1].pop("b")
    assert data == [{"c": None, "d": "e"}, {"a": "1"}]

    assert list(qs.all()) == [{"a": "b", "c": None}, {"a": "1", "b": "2"}]


@pytest.mark.usefixtures("final_sql_check")
def test_int_range():
    with assert_num_queries(1):
        data1 = [o.int_range for o in PostgresModel.objects.all()]
    with assert_num_queries(1):
        data2 = list(PostgresModel.objects.values_list("int_range", flat=True))
    assert data2 == data1
    assert data2 == [NumericRange(1900, 2000), NumericRange(1989)]

    invalidate(PostgresModel)

    qs = PostgresModel.objects.values_list("int_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(1900, 2000), NumericRange(1989)])

    qs = PostgresModel.objects.filter(int_range__contains=2015).values_list("int_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(1989)])

    qs = PostgresModel.objects.filter(int_range__contains=NumericRange(1950, 1990)).values_list(
        "int_range",
        flat=True,
    )
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(1900, 2000)])

    qs = PostgresModel.objects.filter(int_range__contained_by=NumericRange(0, 2050)).values_list(
        "int_range",
        flat=True,
    )
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(1900, 2000)])

    qs = PostgresModel.objects.filter(int_range__fully_lt=(2015, None)).values_list("int_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(1900, 2000)])

    qs = PostgresModel.objects.filter(int_range__fully_gt=(1970, 1980)).values_list("int_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(1989)])

    qs = PostgresModel.objects.filter(int_range__not_lt=(1970, 1980)).values_list("int_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(1989)])

    qs = PostgresModel.objects.filter(int_range__not_gt=(1970, 1980)).values_list("int_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [])

    qs = PostgresModel.objects.filter(int_range__adjacent_to=(1900, 1989)).values_list("int_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(1989)])

    qs = PostgresModel.objects.filter(int_range__startswith=1900).values_list("int_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(1900, 2000)])

    qs = PostgresModel.objects.filter(int_range__endswith=2000).values_list("int_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(1900, 2000)])

    PostgresModel.objects.create(int_range=[1900, 1900])

    qs = PostgresModel.objects.filter(int_range__isempty=True).values_list("int_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [NumericRange(empty=True)])


@pytest.mark.usefixtures("final_sql_check")
def test_decimal_range():
    qs = PostgresModel.objects.values_list("decimal_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(
        qs,
        [NumericRange(Decimal("-1000.0"), Decimal("9.87654321")), NumericRange(Decimal("0.0"))],
    )


@pytest.mark.usefixtures("final_sql_check")
def test_date_range():
    qs = PostgresModel.objects.values_list("date_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(qs, [DateRange(date(1678, 3, 4), date(1741, 7, 28)), DateRange(date(1989, 1, 30))])


@pytest.mark.usefixtures("final_sql_check")
def test_datetime_range():
    qs = PostgresModel.objects.values_list("datetime_range", flat=True)
    assert_tables(qs, PostgresModel)
    assert_query_cached(
        qs,
        [
            DateTimeTZRange(datetime(1989, 1, 30, 12, 20, tzinfo=ZoneInfo("Europe/Paris"))),
            DateTimeTZRange(bounds="()"),
        ],
    )


@pytest.mark.usefixtures("final_sql_check")
def test_transaction_now():
    """Queries with a TransactionNow() parameter are not cached."""
    obj = Test.objects.create(datetime="1992-07-02T12:00:00+00:00")
    qs = Test.objects.filter(datetime__lte=TransactionNow())
    with pytest.raises(UncachableQuery):
        assert_tables(qs, Test)
    assert_query_cached(qs, [obj], after=1)
