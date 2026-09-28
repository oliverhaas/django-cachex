# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from threading import Thread

import pytest
from django.db import connection, transaction

from tests.orm.app.models import Test
from tests.orm.utils import assert_num_queries

pytestmark = [
    pytest.mark.skipif(
        not connection.features.test_db_allows_multiple_connections,
        reason="Database doesn't support feature(s): test_db_allows_multiple_connections",
    ),
    pytest.mark.django_db(transaction=True),
]


class ReadThread(Thread):
    def start_and_join(self):
        self.start()
        self.join()
        return self.t

    def run(self):
        self.t = Test.objects.first()
        connection.close()


class CreateThread(ReadThread):
    def run(self):
        self.t = Test.objects.create(name="test")
        connection.close()


def test_concurrent_caching():
    t1 = ReadThread().start_and_join()
    t = Test.objects.create(name="test")
    t2 = ReadThread().start_and_join()

    assert t1 is None
    assert t2 == t


def test_concurrent_caching_during_atomic():
    with assert_num_queries(1), transaction.atomic():
        t1 = ReadThread().start_and_join()
        t = Test.objects.create(name="test")
        t2 = ReadThread().start_and_join()

    assert t1 is None
    assert t2 is None

    with assert_num_queries(1):
        data = Test.objects.first()
    assert data == t


def test_concurrent_caching_before_and_during_atomic_1():
    t1 = ReadThread().start_and_join()

    with assert_num_queries(1), transaction.atomic():
        t2 = ReadThread().start_and_join()
        t = Test.objects.create(name="test")

    assert t1 is None
    assert t2 is None

    with assert_num_queries(1):
        data = Test.objects.first()
    assert data == t


def test_concurrent_caching_before_and_during_atomic_2():
    t1 = ReadThread().start_and_join()

    with assert_num_queries(1), transaction.atomic():
        t = Test.objects.create(name="test")
        t2 = ReadThread().start_and_join()

    assert t1 is None
    assert t2 is None

    with assert_num_queries(1):
        data = Test.objects.first()
    assert data == t


def test_concurrent_caching_during_and_after_atomic_1():
    with assert_num_queries(1), transaction.atomic():
        t1 = ReadThread().start_and_join()
        t = Test.objects.create(name="test")

    t2 = ReadThread().start_and_join()

    assert t1 is None
    assert t2 == t

    with assert_num_queries(0):
        data = Test.objects.first()
    assert data == t


def test_concurrent_caching_during_and_after_atomic_2():
    with assert_num_queries(1), transaction.atomic():
        t = Test.objects.create(name="test")
        t1 = ReadThread().start_and_join()

    t2 = ReadThread().start_and_join()

    assert t1 is None
    assert t2 == t

    with assert_num_queries(0):
        data = Test.objects.first()
    assert data == t


def test_concurrent_caching_during_and_after_atomic_3():
    with assert_num_queries(1), transaction.atomic():
        t1 = ReadThread().start_and_join()
        t = Test.objects.create(name="test")
        t2 = ReadThread().start_and_join()

    t3 = ReadThread().start_and_join()

    assert t1 is None
    assert t2 is None
    assert t3 == t

    with assert_num_queries(0):
        data = Test.objects.first()
    assert data == t


def test_concurrent_caching_during_autocommit_write():
    results = []

    def read_before_write(execute, sql, params, many, context):
        results.append(ReadThread().start_and_join())
        return execute(sql, params, many, context)

    with connection.execute_wrapper(read_before_write):
        t = Test.objects.create(name="test")

    assert results == [None]
    assert Test.objects.first() == t


def test_concurrent_write_between_query_and_caching():
    created = []

    def write_after_read(execute, sql, params, many, context):
        result = execute(sql, params, many, context)
        created.append(CreateThread().start_and_join())
        return result

    with connection.execute_wrapper(write_after_read):
        assert Test.objects.first() is None

    assert Test.objects.first() == created[0]
