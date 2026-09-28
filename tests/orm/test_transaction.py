# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

import pytest
from django.contrib.auth.models import User
from django.db import IntegrityError, connection, transaction

from tests.orm.app.models import Test
from tests.orm.utils import assert_num_queries

pytestmark = pytest.mark.django_db(transaction=True)


def test_successful_read_atomic():
    with assert_num_queries(1), transaction.atomic():
        data1 = list(Test.objects.all())
    assert data1 == []

    with assert_num_queries(0):
        data2 = list(Test.objects.all())
    assert data2 == []


def test_unsuccessful_read_atomic():
    with assert_num_queries(1), pytest.raises(ZeroDivisionError), transaction.atomic():
        data1 = list(Test.objects.all())
        raise ZeroDivisionError
    assert data1 == []

    # The transaction read committed data, so its cached result survives the rollback.
    with assert_num_queries(0):
        data2 = list(Test.objects.all())
    assert data2 == []


def test_successful_write_atomic():
    with assert_num_queries(1):
        data1 = list(Test.objects.all())
    assert data1 == []

    with assert_num_queries(1), transaction.atomic():
        t1 = Test.objects.create(name="test1")
    with assert_num_queries(1):
        data2 = list(Test.objects.all())
    assert data2 == [t1]

    with assert_num_queries(1), transaction.atomic():
        t2 = Test.objects.create(name="test2")
    with assert_num_queries(1):
        data3 = list(Test.objects.all())
    assert data3 == [t1, t2]

    with assert_num_queries(3), transaction.atomic():
        data4 = list(Test.objects.all())
        t3 = Test.objects.create(name="test3")
        t4 = Test.objects.create(name="test4")
        data5 = list(Test.objects.all())
    assert data4 == [t1, t2]
    assert data5 == [t1, t2, t3, t4]
    assert t4 != t3


def test_unsuccessful_write_atomic():
    with assert_num_queries(1):
        data1 = list(Test.objects.all())
    assert data1 == []

    with assert_num_queries(1), pytest.raises(ZeroDivisionError), transaction.atomic():
        Test.objects.create(name="test")
        raise ZeroDivisionError
    with assert_num_queries(0):
        data2 = list(Test.objects.all())
    assert data2 == []
    with assert_num_queries(1), pytest.raises(Test.DoesNotExist):
        Test.objects.get(name="test")


def test_cache_inside_atomic():
    with assert_num_queries(1), transaction.atomic():
        data1 = list(Test.objects.all())
        data2 = list(Test.objects.all())
    assert data2 == data1
    assert data2 == []


def test_invalidation_inside_atomic():
    with assert_num_queries(3), transaction.atomic():
        data1 = list(Test.objects.all())
        t = Test.objects.create(name="test")
        data2 = list(Test.objects.all())
    assert data1 == []
    assert data2 == [t]


def test_successful_nested_read_atomic():
    with assert_num_queries(6), transaction.atomic():
        list(Test.objects.all())
        with transaction.atomic():
            list(User.objects.all())
            with assert_num_queries(2), transaction.atomic():
                list(User.objects.all())
        with assert_num_queries(0):
            list(User.objects.all())
    with assert_num_queries(0):
        list(Test.objects.all())
        list(User.objects.all())


def test_unsuccessful_nested_read_atomic():
    # SAVEPOINT, the query, ROLLBACK TO SAVEPOINT and RELEASE SAVEPOINT.
    with assert_num_queries(4), transaction.atomic():
        with pytest.raises(ZeroDivisionError), transaction.atomic():
            with assert_num_queries(1):
                list(Test.objects.all())
            raise ZeroDivisionError
        with assert_num_queries(0):
            list(Test.objects.all())


def test_successful_nested_write_atomic():
    with assert_num_queries(12), transaction.atomic():
        t1 = Test.objects.create(name="test1")
        with transaction.atomic():
            t2 = Test.objects.create(name="test2")
        data1 = list(Test.objects.all())
        assert data1 == [t1, t2]
        with transaction.atomic():
            t3 = Test.objects.create(name="test3")
            with transaction.atomic():
                data2 = list(Test.objects.all())
                assert data2 == [t1, t2, t3]
                t4 = Test.objects.create(name="test4")
    data3 = list(Test.objects.all())
    assert data3 == [t1, t2, t3, t4]


def test_unsuccessful_nested_write_atomic():
    with assert_num_queries(15), transaction.atomic():
        t1 = Test.objects.create(name="test1")
        with pytest.raises(ZeroDivisionError), transaction.atomic():
            t2 = Test.objects.create(name="test2")
            data1 = list(Test.objects.all())
            assert data1 == [t1, t2]
            raise ZeroDivisionError
        data2 = list(Test.objects.all())
        assert data2 == [t1]
        with pytest.raises(ZeroDivisionError), transaction.atomic():
            t3 = Test.objects.create(name="test3")
            with transaction.atomic():
                data2 = list(Test.objects.all())
                assert data2 == [t1, t3]
                raise ZeroDivisionError
    with assert_num_queries(1):
        data3 = list(Test.objects.all())
    assert data3 == [t1]


@pytest.mark.skipif(
    not connection.features.can_defer_constraint_checks,
    reason="Database doesn't support feature(s): can_defer_constraint_checks",
)
def test_deferred_error():
    """An error occurring during the end of a transaction has no impact on future queries."""
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE example (id int UNIQUE DEFERRABLE INITIALLY DEFERRED);")
        with pytest.raises(IntegrityError), transaction.atomic():
            with assert_num_queries(1):
                list(Test.objects.all())
            cursor.execute(
                "INSERT INTO example VALUES (1), (1);-- " + Test._meta.db_table,
            )  # Should invalidate Test.
    # PostgreSQL rejects the duplicate at COMMIT, after the bump; SQLite at the INSERT, before any write.
    with assert_num_queries(1 if connection.vendor == "postgresql" else 0):
        assert list(Test.objects.all()) == []
