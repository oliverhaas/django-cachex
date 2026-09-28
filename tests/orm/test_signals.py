# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from unittest import skipIf

from django.conf import settings
from django.contrib.auth.models import User
from django.db import DEFAULT_DB_ALIAS, transaction
from django.test import TransactionTestCase

from django_cachex.orm.api import invalidate
from django_cachex.orm.signals import post_invalidation
from tests.orm.app.models import Test


class SignalsTestCase(TransactionTestCase):
    databases = set(settings.DATABASES.keys())

    def test_table_invalidated(self):
        l = []

        def receiver(sender, **kwargs):
            db_alias = kwargs["db_alias"]
            l.append((sender, db_alias))

        post_invalidation.connect(receiver)
        self.assertListEqual(l, [])
        list(Test.objects.all())
        self.assertListEqual(l, [])
        Test.objects.create(name="test1")
        self.assertListEqual(l, [("ormtest_test", DEFAULT_DB_ALIAS)])
        post_invalidation.disconnect(receiver)

        del l[:]  # Empties the list
        post_invalidation.connect(receiver, sender=User._meta.db_table)
        Test.objects.create(name="test2")
        self.assertListEqual(l, [])
        User.objects.create_user("user")
        self.assertListEqual(l, [("auth_user", DEFAULT_DB_ALIAS)])
        post_invalidation.disconnect(receiver, sender=User._meta.db_table)

    def test_failing_receiver(self):
        # Logged, as the write happened already; the other receivers still get the signal.
        received = []

        def failing(sender, **kwargs):
            msg = "receiver failed"
            raise ValueError(msg)

        def receiver(sender, **kwargs):
            received.append(sender)

        post_invalidation.connect(failing)
        self.addCleanup(post_invalidation.disconnect, failing)
        post_invalidation.connect(receiver)
        self.addCleanup(post_invalidation.disconnect, receiver)
        with self.assertLogs("django.dispatch", "ERROR") as logs:
            Test.objects.create(name="test1")
            with transaction.atomic():
                Test.objects.create(name="test2")
            invalidate(Test, db_alias=DEFAULT_DB_ALIAS)
        self.assertListEqual(received, ["ormtest_test"] * 3)
        self.assertEqual(len(logs.records), 3)
        self.assertEqual(Test.objects.count(), 2)

    def test_table_invalidated_in_transaction(self):
        """
        Checks that the ``post_invalidation`` signal is triggered only after
        the end of a transaction.
        """
        l = []

        def receiver(sender, **kwargs):
            db_alias = kwargs["db_alias"]
            l.append((sender, db_alias))

        post_invalidation.connect(receiver)

        self.assertListEqual(l, [])
        with transaction.atomic():
            Test.objects.create(name="test1")
            self.assertListEqual(l, [])
        self.assertListEqual(l, [("ormtest_test", DEFAULT_DB_ALIAS)])

        del l[:]  # Empties the list
        self.assertListEqual(l, [])
        with transaction.atomic():
            Test.objects.create(name="test2")
            with transaction.atomic():
                Test.objects.create(name="test3")
                self.assertListEqual(l, [])
            self.assertListEqual(l, [])
        self.assertListEqual(l, [("ormtest_test", DEFAULT_DB_ALIAS)])

        post_invalidation.disconnect(receiver)

    def test_table_invalidated_once_per_transaction_or_invalidate(self):
        """
        Checks that the ``post_invalidation`` signal is triggered only after
        the end of a transaction.
        """
        l = []

        def receiver(sender, **kwargs):
            db_alias = kwargs["db_alias"]
            l.append((sender, db_alias))

        post_invalidation.connect(receiver)

        self.assertListEqual(l, [])
        with transaction.atomic():
            Test.objects.create(name="test1")
            self.assertListEqual(l, [])
            Test.objects.create(name="test2")
            self.assertListEqual(l, [])
        self.assertListEqual(l, [("ormtest_test", DEFAULT_DB_ALIAS)])

        del l[:]  # Empties the list
        self.assertListEqual(l, [])
        invalidate(Test, db_alias=DEFAULT_DB_ALIAS)
        self.assertListEqual(l, [("ormtest_test", DEFAULT_DB_ALIAS)])

        del l[:]  # Empties the list
        self.assertListEqual(l, [])
        with transaction.atomic():
            invalidate(Test, db_alias=DEFAULT_DB_ALIAS)
            self.assertListEqual(l, [])
        self.assertListEqual(l, [("ormtest_test", DEFAULT_DB_ALIAS)])

        post_invalidation.disconnect(receiver)

    @skipIf(len(settings.DATABASES) == 1, "We can't change the DB used since there's only one configured")
    def test_table_invalidated_multi_db(self):
        db_alias2 = next(alias for alias in settings.DATABASES if alias != DEFAULT_DB_ALIAS)
        l = []

        def receiver(sender, **kwargs):
            db_alias = kwargs["db_alias"]
            l.append((sender, db_alias))

        post_invalidation.connect(receiver)
        self.assertListEqual(l, [])
        Test.objects.using(DEFAULT_DB_ALIAS).create(name="test")
        self.assertListEqual(l, [("ormtest_test", DEFAULT_DB_ALIAS)])
        Test.objects.using(db_alias2).create(name="test")
        self.assertListEqual(l, [("ormtest_test", DEFAULT_DB_ALIAS), ("ormtest_test", db_alias2)])
        post_invalidation.disconnect(receiver)
