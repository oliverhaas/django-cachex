# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from datetime import date, datetime
from decimal import Decimal
from unittest import skipUnless
from zoneinfo import ZoneInfo

from django.contrib.postgres.functions import TransactionNow
from django.db import connection
from django.db.backends.postgresql.psycopg_any import DateRange, DateTimeTZRange, NumericRange
from django.test import TransactionTestCase, override_settings

from django_cachex.orm.api import invalidate
from django_cachex.orm.utils import UncachableQuery
from tests.orm.app.models import PostgresModel, Test
from tests.orm.utils import TestUtilsMixin, all_final_sql_checks


@skipUnless(connection.vendor == "postgresql", "This test is only for PostgreSQL")
@override_settings(USE_TZ=True)
class PostgresReadTestCase(TestUtilsMixin, TransactionTestCase):
    def setUp(self):
        self.obj1 = PostgresModel(
            int_array=[1, 2, 3],
            hstore={"a": "b", "c": None},
            int_range=[1900, 2000],
            date_range=["1678-03-04", "1741-07-28"],
            datetime_range=[datetime(1989, 1, 30, 12, 20, tzinfo=ZoneInfo("Europe/Paris")), None],
        )

        self.obj2 = PostgresModel(
            int_array=[4, None, 6],
            hstore={"a": "1", "b": "2"},
            int_range=[1989, None],
            date_range=["1989-01-30", None],
            datetime_range=[None, None],
        )

        self.obj1.decimal_range = [-1e3, 9.87654321]
        self.obj2.decimal_range = [0.0, None]

        self.obj1.save()
        self.obj2.save()

    @all_final_sql_checks
    def test_unaccent(self):
        obj1 = Test.objects.create(name="Clémentine")
        obj2 = Test.objects.create(name="Clementine")
        qs = Test.objects.filter(name__unaccent="Clémentine").values_list("name", flat=True)
        self.assert_tables(qs, Test)
        self.assert_query_cached(qs, ["Clementine", "Clémentine"])
        obj1.delete()
        obj2.delete()

    @all_final_sql_checks
    def test_int_array(self):
        with self.assertNumQueries(1):
            data1 = [o.int_array for o in PostgresModel.objects.all()]
        with self.assertNumQueries(1):
            data2 = list(PostgresModel.objects.values_list("int_array", flat=True))
        self.assertListEqual(data2, data1)
        self.assertListEqual(data2, [[1, 2, 3], [4, None, 6]])

        invalidate(PostgresModel)

        qs = PostgresModel.objects.values_list("int_array", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [[1, 2, 3], [4, None, 6]])

        qs = PostgresModel.objects.filter(int_array__contains=[3]).values_list("int_array", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [[1, 2, 3]])

        qs = PostgresModel.objects.filter(int_array__contained_by=[1, 2, 3, 4, 5, 6]).values_list(
            "int_array",
            flat=True,
        )
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [[1, 2, 3]])

        qs = PostgresModel.objects.filter(int_array__overlap=[3, 4]).values_list("int_array", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [[1, 2, 3], [4, None, 6]])

        qs = PostgresModel.objects.filter(int_array__len__in=(2, 3)).values_list("int_array", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [[1, 2, 3], [4, None, 6]])

        qs = PostgresModel.objects.filter(int_array__2=6).values_list("int_array", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [[4, None, 6]])

        qs = PostgresModel.objects.filter(int_array__0_2=(1, 2)).values_list("int_array", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [[1, 2, 3]])

    @all_final_sql_checks
    def test_hstore(self):
        with self.assertNumQueries(1):
            data1 = [o.hstore for o in PostgresModel.objects.all()]
        with self.assertNumQueries(1):
            data2 = list(PostgresModel.objects.values_list("hstore", flat=True))
        self.assertListEqual(data2, data1)
        self.assertListEqual(data2, [{"a": "b", "c": None}, {"a": "1", "b": "2"}])

        invalidate(PostgresModel)

        qs = PostgresModel.objects.values_list("hstore", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [{"a": "b", "c": None}, {"a": "1", "b": "2"}])

        qs = PostgresModel.objects.filter(hstore__a="1").values_list("hstore", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [{"a": "1", "b": "2"}])

        qs = PostgresModel.objects.filter(hstore__contains={"a": "b"}).values_list("hstore", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [{"a": "b", "c": None}])

        qs = PostgresModel.objects.filter(hstore__contained_by={"a": "b", "c": None, "b": "2"}).values_list(
            "hstore",
            flat=True,
        )
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [{"a": "b", "c": None}])

        qs = PostgresModel.objects.filter(hstore__has_key="c").values_list("hstore", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [{"a": "b", "c": None}])

        qs = PostgresModel.objects.filter(hstore__has_keys=["a", "b"]).values_list("hstore", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [{"a": "1", "b": "2"}])

        qs = PostgresModel.objects.filter(hstore__keys=["a", "b"]).values_list("hstore", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [{"a": "1", "b": "2"}])

        qs = PostgresModel.objects.filter(hstore__values=["1", "2"]).values_list("hstore", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [{"a": "1", "b": "2"}])

    def test_mutable_result_change(self):
        """
        Checks that changing a mutable returned by a query has no effect
        on other executions of the query.
        """
        qs = PostgresModel.objects.values_list("int_array", flat=True)

        data = list(qs.all())
        self.assertListEqual(data, [[1, 2, 3], [4, None, 6]])
        data[0].append(4)
        data[1].remove(4)
        data[1][0] = 5
        self.assertListEqual(data, [[1, 2, 3, 4], [5, 6]])

        self.assertListEqual(list(qs.all()), [[1, 2, 3], [4, None, 6]])

        # hstore values are mutable dicts.
        qs = PostgresModel.objects.values_list("hstore", flat=True)

        data = list(qs.all())
        self.assertListEqual(data, [{"a": "b", "c": None}, {"a": "1", "b": "2"}])
        data[0]["d"] = "e"
        del data[0]["a"]
        data[1].pop("b")
        self.assertListEqual(data, [{"c": None, "d": "e"}, {"a": "1"}])

        self.assertListEqual(list(qs.all()), [{"a": "b", "c": None}, {"a": "1", "b": "2"}])

    @all_final_sql_checks
    def test_int_range(self):
        with self.assertNumQueries(1):
            data1 = [o.int_range for o in PostgresModel.objects.all()]
        with self.assertNumQueries(1):
            data2 = list(PostgresModel.objects.values_list("int_range", flat=True))
        self.assertListEqual(data2, data1)
        self.assertListEqual(data2, [NumericRange(1900, 2000), NumericRange(1989)])

        invalidate(PostgresModel)

        qs = PostgresModel.objects.values_list("int_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(1900, 2000), NumericRange(1989)])

        qs = PostgresModel.objects.filter(int_range__contains=2015).values_list("int_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(1989)])

        qs = PostgresModel.objects.filter(int_range__contains=NumericRange(1950, 1990)).values_list(
            "int_range",
            flat=True,
        )
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(1900, 2000)])

        qs = PostgresModel.objects.filter(int_range__contained_by=NumericRange(0, 2050)).values_list(
            "int_range",
            flat=True,
        )
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(1900, 2000)])

        qs = PostgresModel.objects.filter(int_range__fully_lt=(2015, None)).values_list("int_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(1900, 2000)])

        qs = PostgresModel.objects.filter(int_range__fully_gt=(1970, 1980)).values_list("int_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(1989)])

        qs = PostgresModel.objects.filter(int_range__not_lt=(1970, 1980)).values_list("int_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(1989)])

        qs = PostgresModel.objects.filter(int_range__not_gt=(1970, 1980)).values_list("int_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [])

        qs = PostgresModel.objects.filter(int_range__adjacent_to=(1900, 1989)).values_list("int_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(1989)])

        qs = PostgresModel.objects.filter(int_range__startswith=1900).values_list("int_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(1900, 2000)])

        qs = PostgresModel.objects.filter(int_range__endswith=2000).values_list("int_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(1900, 2000)])

        obj = PostgresModel.objects.create(int_range=[1900, 1900])

        qs = PostgresModel.objects.filter(int_range__isempty=True).values_list("int_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [NumericRange(empty=True)])

        obj.delete()

    @all_final_sql_checks
    def test_decimal_range(self):
        qs = PostgresModel.objects.values_list("decimal_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(
            qs,
            [NumericRange(Decimal("-1000.0"), Decimal("9.87654321")), NumericRange(Decimal("0.0"))],
        )

    @all_final_sql_checks
    def test_date_range(self):
        qs = PostgresModel.objects.values_list("date_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(qs, [DateRange(date(1678, 3, 4), date(1741, 7, 28)), DateRange(date(1989, 1, 30))])

    @all_final_sql_checks
    def test_datetime_range(self):
        qs = PostgresModel.objects.values_list("datetime_range", flat=True)
        self.assert_tables(qs, PostgresModel)
        self.assert_query_cached(
            qs,
            [
                DateTimeTZRange(datetime(1989, 1, 30, 12, 20, tzinfo=ZoneInfo("Europe/Paris"))),
                DateTimeTZRange(bounds="()"),
            ],
        )

    @all_final_sql_checks
    def test_transaction_now(self):
        """
        Checks that queries with a TransactionNow() parameter are not cached.
        """
        obj = Test.objects.create(datetime="1992-07-02T12:00:00+00:00")
        qs = Test.objects.filter(datetime__lte=TransactionNow())
        with self.assertRaises(UncachableQuery):
            self.assert_tables(qs, Test)
        self.assert_query_cached(qs, [obj], after=1)

        obj.delete()
