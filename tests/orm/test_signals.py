# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

import logging

import pytest
from django.contrib.auth.models import User
from django.db import DEFAULT_DB_ALIAS, transaction

from django_cachex.orm.api import invalidate
from django_cachex.orm.signals import post_invalidation
from tests.orm.app.models import Test

pytestmark = pytest.mark.django_db(transaction=True, databases="__all__")


@pytest.fixture
def connect():
    """Connect receivers to ``post_invalidation`` until the test ends."""
    connected = []

    def connect(receiver, sender=None):
        post_invalidation.connect(receiver, sender=sender)
        connected.append((receiver, sender))

    yield connect
    for receiver, sender in connected:
        post_invalidation.disconnect(receiver, sender=sender)


def test_table_invalidated(connect):
    received = []

    def receiver(sender, **kwargs):
        db_alias = kwargs["db_alias"]
        received.append((sender, db_alias))

    connect(receiver)
    assert received == []
    list(Test.objects.all())
    assert received == []
    Test.objects.create(name="test1")
    assert received == [("ormtest_test", DEFAULT_DB_ALIAS)]
    post_invalidation.disconnect(receiver)

    received.clear()
    connect(receiver, sender=User._meta.db_table)
    Test.objects.create(name="test2")
    assert received == []
    User.objects.create_user("user")
    assert received == [("auth_user", DEFAULT_DB_ALIAS)]


def test_failing_receiver(connect, caplog):
    # Logged, as the write happened already; the other receivers still get the signal.
    received = []

    def failing(sender, **kwargs):
        msg = "receiver failed"
        raise ValueError(msg)

    def receiver(sender, **kwargs):
        received.append(sender)

    connect(failing)
    connect(receiver)
    with caplog.at_level(logging.ERROR, logger="django.dispatch"):
        Test.objects.create(name="test1")
        with transaction.atomic():
            Test.objects.create(name="test2")
        invalidate(Test, db_alias=DEFAULT_DB_ALIAS)
    assert received == ["ormtest_test"] * 3
    assert [record.name for record in caplog.records] == ["django.dispatch"] * 3
    assert Test.objects.count() == 2


def test_table_invalidated_in_transaction(connect):
    """The ``post_invalidation`` signal is triggered only after the end of a transaction."""
    received = []

    def receiver(sender, **kwargs):
        db_alias = kwargs["db_alias"]
        received.append((sender, db_alias))

    connect(receiver)

    assert received == []
    with transaction.atomic():
        Test.objects.create(name="test1")
        assert received == []
    assert received == [("ormtest_test", DEFAULT_DB_ALIAS)]

    received.clear()
    assert received == []
    with transaction.atomic():
        Test.objects.create(name="test2")
        with transaction.atomic():
            Test.objects.create(name="test3")
            assert received == []
        assert received == []
    assert received == [("ormtest_test", DEFAULT_DB_ALIAS)]


def test_table_invalidated_once_per_transaction_or_invalidate(connect):
    """The ``post_invalidation`` signal is sent once per transaction and once per ``invalidate()`` call."""
    received = []

    def receiver(sender, **kwargs):
        db_alias = kwargs["db_alias"]
        received.append((sender, db_alias))

    connect(receiver)

    assert received == []
    with transaction.atomic():
        Test.objects.create(name="test1")
        assert received == []
        Test.objects.create(name="test2")
        assert received == []
    assert received == [("ormtest_test", DEFAULT_DB_ALIAS)]

    received.clear()
    assert received == []
    invalidate(Test, db_alias=DEFAULT_DB_ALIAS)
    assert received == [("ormtest_test", DEFAULT_DB_ALIAS)]

    received.clear()
    assert received == []
    with transaction.atomic():
        invalidate(Test, db_alias=DEFAULT_DB_ALIAS)
        assert received == []
    assert received == [("ormtest_test", DEFAULT_DB_ALIAS)]


def test_table_invalidated_multi_db(connect):
    received = []

    def receiver(sender, **kwargs):
        db_alias = kwargs["db_alias"]
        received.append((sender, db_alias))

    connect(receiver)
    assert received == []
    Test.objects.using(DEFAULT_DB_ALIAS).create(name="test")
    assert received == [("ormtest_test", DEFAULT_DB_ALIAS)]
    Test.objects.using("second").create(name="test")
    assert received == [("ormtest_test", DEFAULT_DB_ALIAS), ("ormtest_test", "second")]
