# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from types import SimpleNamespace

import pytest
from django.db import transaction

from tests.orm.app.models import Test
from tests.orm.utils import assert_num_queries

pytestmark = pytest.mark.django_db(transaction=True, databases="__all__")


@pytest.fixture(autouse=True)
def rows():
    return SimpleNamespace(t1=Test.objects.create(name="test1"), t2=Test.objects.create(name="test2"))


def test_read(rows):
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [rows.t1, rows.t2]

    with assert_num_queries(1, using="second"):
        assert list(Test.objects.using("second")) == []

    with assert_num_queries(0, using="second"):
        assert list(Test.objects.using("second")) == []


def test_invalidate_other_db():
    """The non-default database is invalidated when modified."""
    with assert_num_queries(1, using="second"):
        assert list(Test.objects.using("second")) == []

    with assert_num_queries(1, using="second"):
        t3 = Test.objects.using("second").create(name="test3")

    with assert_num_queries(1, using="second"):
        assert list(Test.objects.using("second")) == [t3]


def test_invalidation_independence(rows):
    """Invalidation doesn't affect the unmodified databases."""
    with assert_num_queries(1):
        assert list(Test.objects.all()) == [rows.t1, rows.t2]

    with assert_num_queries(1, using="second"):
        Test.objects.using("second").create(name="test3")

    with assert_num_queries(0):
        assert list(Test.objects.all()) == [rows.t1, rows.t2]


def test_heterogeneous_atomics(rows):
    """An atomic block for one database nested in an atomic block for another does not affect their caching."""
    with transaction.atomic():
        with transaction.atomic("second"):
            with assert_num_queries(1):
                assert list(Test.objects.all()) == [rows.t1, rows.t2]
            with assert_num_queries(1, using="second"):
                assert list(Test.objects.using("second")) == []
            t3 = Test.objects.using("second").create(name="test3")
            with assert_num_queries(1, using="second"):
                assert list(Test.objects.using("second")) == [t3]

        with assert_num_queries(0):
            assert list(Test.objects.all()) == [rows.t1, rows.t2]

        with assert_num_queries(1):
            assert list(Test.objects.filter(name="test3")) == []


def test_heterogeneous_atomics_independence():
    """Rolling back an atomic block still invalidates what a nested atomic block for another database committed."""
    with assert_num_queries(1, using="second"):
        assert list(Test.objects.using("second")) == []

    with pytest.raises(ZeroDivisionError), transaction.atomic():
        with transaction.atomic("second"):
            t3 = Test.objects.using("second").create(name="test3")
        raise ZeroDivisionError
    with assert_num_queries(1, using="second"):
        assert list(Test.objects.using("second")) == [t3]
