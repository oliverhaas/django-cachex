"""The generation and lease protocol that keeps cached results in step with writes."""

import time
import uuid
from threading import Thread
from types import SimpleNamespace
from unittest import skipIf, skipUnless
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth.models import Permission, User
from django.core.cache import DEFAULT_CACHE_ALIAS
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.db import DEFAULT_DB_ALIAS, connection, connections, models, transaction
from django.test import SimpleTestCase, TestCase
from django.test.utils import isolate_apps

from django_cachex.exceptions import CachexError
from django_cachex.orm import transaction as orm_transaction
from django_cachex.orm.api import invalidate
from django_cachex.orm.exceptions import InvalidationError
from django_cachex.orm.settings import orm_settings
from django_cachex.orm.store import BYPASS, Lookup, RespStore, _entry_key, _LocalResults
from django_cachex.orm.utils import deletion_dependents
from django_cachex.script import keys_only_pre
from tests.orm.app.models import Test, TestChild, TestParent
from tests.orm.utils import FilteredTransactionTestCase, TestUtilsMixin, orm_store, override_orm_settings

LOCMEM = settings.CACHES[DEFAULT_CACHE_ALIAS]["BACKEND"] == "django_cachex.cache.LocMemCache"
# DB_CASCADE, DB_SET_NULL and DB_SET_DEFAULT arrived in Django 6.1.
DATABASE_ON_DELETE = hasattr(models, "DB_CASCADE")


class Row(models.Model):
    """Base of the models that tests declare in an isolated app registry."""

    class Meta:
        abstract = True
        app_label = "ormtest"

    def __str__(self) -> str:
        return str(self.pk)


class StoreTestCase(TestCase):
    """The store of the configured cache, on tables and a query no other test uses."""

    def setUp(self):
        self.store = orm_store()
        suffix = uuid.uuid4().hex
        self.tables = [f"a_{suffix}", f"b_{suffix}"]
        self.query = f"query_{suffix}"

    def lookup(self, store=None):
        return (store or self.store).lookup(DEFAULT_DB_ALIAS, self.query, self.tables)

    def store_result(self, token, result="result", *, store=None, timeout=None):
        return (store or self.store).store(DEFAULT_DB_ALIAS, self.query, self.tables, token, result, timeout)

    def begin_write(self, token, tables=None, lease_timeout=60):
        self.store.begin_write(DEFAULT_DB_ALIAS, tables or self.tables[:1], token, lease_timeout)

    def end_write(self, token, tables=None):
        self.store.end_write(DEFAULT_DB_ALIAS, tables or self.tables[:1], token)

    def test_hit_after_store(self):
        lookup = self.lookup()
        self.assertFalse(lookup.hit)
        self.assertIsNotNone(lookup.token)
        self.assertTrue(self.store_result(lookup.token, [1, 2]))
        self.assertEqual(self.lookup(), Lookup(hit=True, value=[1, 2]))

    def test_bump(self):
        self.assertTrue(self.store_result(self.lookup().token))
        self.store.bump(DEFAULT_DB_ALIAS, self.tables[1:])
        lookup = self.lookup()
        self.assertFalse(lookup.hit)
        self.assertTrue(self.store_result(lookup.token, "new"))
        self.assertEqual(self.lookup(), Lookup(hit=True, value="new"))

    def test_result_read_before_a_bump_is_not_stored(self):
        token = self.lookup().token
        self.store.bump(DEFAULT_DB_ALIAS, self.tables[1:])
        self.assertFalse(self.store_result(token))
        self.assertFalse(self.lookup().hit)

    def test_write(self):
        self.assertTrue(self.store_result(self.lookup().token))
        # A reader misses before the write, then tries to store during it and after it.
        reader = self.store.lookup(DEFAULT_DB_ALIAS, f"{self.query}:reader", self.tables).token
        self.begin_write("writer")
        self.assertIs(self.lookup(), BYPASS)
        self.assertIsNone(self.store.generations(DEFAULT_DB_ALIAS, self.tables))
        self.assertFalse(self.store.store(DEFAULT_DB_ALIAS, f"{self.query}:reader", self.tables, reader, "old", None))
        self.end_write("writer")
        self.assertFalse(self.store.store(DEFAULT_DB_ALIAS, f"{self.query}:reader", self.tables, reader, "old", None))
        lookup = self.lookup()
        self.assertFalse(lookup.hit)
        self.assertIsNotNone(lookup.token)
        self.assertTrue(self.store_result(lookup.token, "new"))
        self.assertEqual(self.lookup(), Lookup(hit=True, value="new"))

    def test_overlapping_writes(self):
        self.begin_write("first")
        self.begin_write("second", self.tables[1:])
        self.end_write("first")
        self.assertIs(self.lookup(), BYPASS)
        self.end_write("second", self.tables[1:])
        self.assertIsNotNone(self.lookup().token)

    def test_write_to_other_tables(self):
        self.assertTrue(self.store_result(self.lookup().token))
        self.begin_write("writer", [f"c_{uuid.uuid4().hex}"])
        self.assertEqual(self.lookup(), Lookup(hit=True, value="result"))

    def test_expired_lease(self):
        # A write that never released its lease, say because its process died.
        self.begin_write("writer", lease_timeout=0.001)
        time.sleep(0.05)
        lookup = self.lookup()
        self.assertIsNotNone(lookup.token)
        self.assertTrue(self.store_result(lookup.token))
        self.assertTrue(self.lookup().hit)

    def test_generations(self):
        generations = self.store.generations(DEFAULT_DB_ALIAS, self.tables)
        self.assertEqual(len(generations), 2)
        self.assertTrue(all(isinstance(generation, str) for generation in generations))
        self.assertEqual(self.store.generations(DEFAULT_DB_ALIAS, self.tables), generations)
        self.store.bump(DEFAULT_DB_ALIAS, self.tables[:1])
        bumped = self.store.generations(DEFAULT_DB_ALIAS, self.tables)
        self.assertNotEqual(bumped[0], generations[0])
        self.assertEqual(bumped[1], generations[1])

    def test_ttl(self):
        self.assertIsNone(self.store.ttl(None))
        self.assertEqual(self.store.ttl(5), 5)
        self.assertEqual(self.store.ttl(DEFAULT_TIMEOUT), self.store.cache.default_timeout)

    def test_timeout(self):
        token = self.lookup().token
        self.assertFalse(self.store_result(token, timeout=0))
        self.assertFalse(self.store_result(token, timeout=-1))
        self.assertFalse(self.lookup().hit)
        self.assertTrue(self.store_result(token, timeout=0.3))
        self.assertTrue(self.lookup().hit)
        time.sleep(0.4)
        self.assertFalse(self.lookup().hit)


@skipIf(LOCMEM, "only the Redis stores keep results in process")
class LocalResultsTestCase(StoreTestCase):
    """A Redis store that keeps results in process too, as it does over a TrackingCache."""

    def setUp(self):
        super().setUp()
        self.store = RespStore(orm_store().cache, _LocalResults(10))

    def set_server_payload(self, payload):
        self.store.cache.eval_script(
            "return redis.call('HSET', KEYS[1], 'v', ARGV[1])",
            keys=[_entry_key(DEFAULT_DB_ALIAS, self.query)],
            args=[payload],
            pre_hook=keys_only_pre,
        )

    def test_hit_is_served_from_the_local_copy(self):
        self.assertTrue(self.store_result(self.lookup().token))
        # The server's copy is not sent while the local one is current.
        self.set_server_payload(b"garbage")
        self.assertEqual(self.lookup(), Lookup(hit=True, value="result"))
        server_only = RespStore(self.store.cache)
        with self.assertLogs("django_cachex.orm", "WARNING"):
            self.assertFalse(self.lookup(server_only).hit)

    def test_local_copy_expires_with_the_server_copy(self):
        self.assertTrue(self.store_result(self.lookup().token, timeout=60))
        self.store.cache.delete(_entry_key(DEFAULT_DB_ALIAS, self.query))
        self.assertFalse(self.lookup().hit)

    def test_local_copy_of_a_result_stored_elsewhere(self):
        # Another process stored a result; this one fetches it once.
        other_process = RespStore(self.store.cache, _LocalResults(10))
        self.assertTrue(self.store_result(self.lookup(other_process).token, store=other_process))
        self.assertEqual(self.lookup(), Lookup(hit=True, value="result"))
        self.set_server_payload(b"garbage")
        self.assertEqual(self.lookup(), Lookup(hit=True, value="result"))

    def test_bounded(self):
        local = _LocalResults(2)
        local.put("a", b"1", b"a")
        local.put("b", b"1", b"b")
        local.get("a")
        local.put("c", b"1", b"c")
        self.assertIsNone(local.get("b"))
        self.assertEqual(local.get("a"), (b"1", b"a"))
        self.assertEqual(local.get("c"), (b"1", b"c"))


class TransactionStateTestCase(SimpleTestCase):
    """What a transaction wrote and cached, across its savepoints."""

    def setUp(self):
        self.connection = SimpleNamespace()

    def assert_written(self, tables):
        self.assertSetEqual(orm_transaction.written(self.connection), tables)

    def assert_cached(self, query_key, result):
        self.assertEqual(orm_transaction.cached(self.connection, query_key), (True, result))

    def assert_not_cached(self, query_key):
        self.assertEqual(orm_transaction.cached(self.connection, query_key), (False, None))

    def test_written(self):
        c = self.connection
        self.assert_written(set())
        orm_transaction.mark_written(c, {"a"})
        orm_transaction.savepoint_created(c, "s1")
        orm_transaction.mark_written(c, {"b"})
        orm_transaction.savepoint_created(c, "s2")
        orm_transaction.mark_written(c, {"c"})
        self.assert_written({"a", "b", "c"})

        orm_transaction.savepoint_rolled_back(c, "s2")
        self.assert_written({"a", "b"})
        # The savepoint survives a rollback to it.
        orm_transaction.mark_written(c, {"d"})
        orm_transaction.savepoint_released(c, "s2")
        self.assert_written({"a", "b", "d"})

        orm_transaction.savepoint_rolled_back(c, "s1")
        self.assert_written({"a"})
        orm_transaction.mark_written(c, {"e"})
        orm_transaction.savepoint_released(c, "s1")
        self.assert_written({"a", "e"})

        orm_transaction.reset(c)
        self.assert_written(set())

    def test_unknown_savepoint(self):
        c = self.connection
        orm_transaction.mark_written(c, {"a"})
        orm_transaction.savepoint_rolled_back(c, "unknown")
        orm_transaction.savepoint_released(c, "unknown")
        self.assert_written({"a"})

    def test_cached(self):
        c = self.connection
        self.assert_not_cached("q1")
        orm_transaction.cache(c, "q1", {"a"}, [1])
        orm_transaction.savepoint_created(c, "s1")
        orm_transaction.cache(c, "q2", {"b"}, [2])
        self.assert_cached("q1", [1])
        self.assert_cached("q2", [2])

        orm_transaction.savepoint_rolled_back(c, "s1")
        self.assert_cached("q1", [1])
        self.assert_not_cached("q2")
        orm_transaction.cache(c, "q3", {"c"}, [3])
        orm_transaction.savepoint_released(c, "s1")
        self.assert_cached("q3", [3])

        # A write drops what was read from its tables, whichever savepoint read it.
        orm_transaction.savepoint_created(c, "s2")
        orm_transaction.cache(c, "q4", {"a", "d"}, [4])
        orm_transaction.mark_written(c, {"a"})
        self.assert_not_cached("q1")
        self.assert_not_cached("q4")
        self.assert_cached("q3", [3])

        orm_transaction.reset(c)
        self.assert_not_cached("q3")

    def test_cached_copy(self):
        result = [1]
        orm_transaction.cache(self.connection, "q", {"a"}, result)
        result.append(2)
        _, cached = orm_transaction.cached(self.connection, "q")
        cached.append(3)
        self.assert_cached("q", [1])


class WriteTestCase(TestUtilsMixin, FilteredTransactionTestCase):
    """Leases and generation bumps around the writes of the ORM and raw SQL."""

    def leased(self):
        table_key = orm_settings.TABLE_KEYGEN(DEFAULT_DB_ALIAS, Test._meta.db_table)
        return orm_store().generations(DEFAULT_DB_ALIAS, [table_key]) is None

    def record_leases(self, method):
        """Patch ``method`` of the connection to record whether Test is leased when it runs."""
        wrapper = connections[DEFAULT_DB_ALIAS]
        original = getattr(wrapper, method)
        leases = []

        def recording(*args, **kwargs):
            leases.append(self.leased())
            return original(*args, **kwargs)

        return patch.object(wrapper, method, recording), leases

    def test_commit_holds_a_lease(self):
        recorder, leases = self.record_leases("_commit")
        with recorder, transaction.atomic():
            t = Test.objects.create(name="test")
            # Other connections use the cache until the transaction commits.
            self.assertFalse(self.leased())
        self.assertListEqual(leases, [True])
        self.assertFalse(self.leased())
        self.assert_query_cached(Test.objects.all(), [t])

    def test_commit_without_writes_takes_no_lease(self):
        recorder, leases = self.record_leases("_commit")
        with recorder, transaction.atomic():
            list(Test.objects.all())
        self.assertListEqual(leases, [False])

    def test_rollback_takes_no_lease(self):
        self.assert_query_cached(Test.objects.all())
        recorder, leases = self.record_leases("_rollback")
        with recorder, transaction.atomic():
            Test.objects.create(name="test")
            transaction.set_rollback(True)
        self.assertListEqual(leases, [False])
        # Nothing was committed, so the cached result stays valid.
        self.assert_query_cached(Test.objects.all(), [], before=0)

    @skipUnless(connection.vendor == "sqlite", "SQLite commits when autocommit is turned back on")
    def test_set_autocommit_holds_a_lease(self):
        self.assert_query_cached(Test.objects.all())
        recorder, leases = self.record_leases("_set_autocommit")
        transaction.set_autocommit(False)
        try:
            t = Test.objects.create(name="test")
        finally:
            with recorder:
                transaction.set_autocommit(True)
        self.assertListEqual(leases, [True])
        self.assert_query_cached(Test.objects.all(), [t])

    def test_failed_lease_aborts_the_write(self):
        store_class = type(orm_store())
        with patch.object(store_class, "begin_write", side_effect=ConnectionError("cache down")):
            message = "Could not invalidate the ORM cache of ormtest_test in database 'default'"
            with self.assertRaisesMessage(InvalidationError, message) as raised:
                Test.objects.create(name="autocommit")
            self.assertIsInstance(raised.exception, CachexError)
            self.assertIsInstance(raised.exception.__cause__, ConnectionError)
            with self.assertRaisesMessage(InvalidationError, message), transaction.atomic():
                Test.objects.create(name="atomic")
        self.assertFalse(Test.objects.exists())

    def test_failed_lease_while_disabled(self):
        # Nothing is served from a disabled cache, so the write goes ahead.
        with (
            override_orm_settings(ENABLED=False),
            patch.object(type(orm_store()), "begin_write", side_effect=ConnectionError("cache down")),
            self.assertLogs("django_cachex.orm", "WARNING"),
        ):
            t = Test.objects.create(name="test")
        self.assertListEqual(list(Test.objects.all()), [t])

    def test_failed_release(self):
        # The unreleased lease blocks serving and storing until it expires.
        with (
            override_orm_settings(LEASE_TIMEOUT=0.5),
            patch.object(type(orm_store()), "end_write", side_effect=ConnectionError("cache down")),
            self.assertLogs("django_cachex.orm", "WARNING"),
        ):
            t = Test.objects.create(name="test")
        released_at = time.monotonic() + 0.5
        self.assertTrue(self.leased())
        self.assert_query_cached(Test.objects.all(), [t], after=1)
        time.sleep(max(0.0, released_at - time.monotonic()) + 0.1)
        self.assertFalse(self.leased())
        self.assert_query_cached(Test.objects.all(), [t])

    def test_failed_lookup(self):
        with (
            patch.object(type(orm_store()), "lookup", side_effect=ConnectionError("cache down")),
            self.assertLogs("django_cachex.orm", "WARNING"),
        ):
            self.assert_query_cached(Test.objects.all(), [], after=1)
        self.assert_query_cached(Test.objects.all(), [])

    def test_failed_store(self):
        with (
            patch.object(type(orm_store()), "store", side_effect=ConnectionError("cache down")),
            self.assertLogs("django_cachex.orm", "WARNING"),
        ):
            self.assert_query_cached(Test.objects.all(), [], after=1)
        self.assert_query_cached(Test.objects.all(), [])

    def test_failed_invalidate(self):
        with patch.object(type(orm_store()), "bump", side_effect=ConnectionError("cache down")):
            with self.assertRaises(InvalidationError):
                invalidate(Test, cache_alias=orm_settings.CACHE)
            with override_orm_settings(ENABLED=False), self.assertLogs("django_cachex.orm", "WARNING"):
                invalidate(Test, cache_alias=orm_settings.CACHE)

    @skipUnless(connection.vendor == "postgresql", "TRUNCATE ... CASCADE is PostgreSQL's")
    def test_truncate_cascade(self):
        permission = Permission.objects.first()
        child = TestChild.objects.create(name="child")
        child.permissions.add(permission)
        permissions = TestChild.permissions.through.objects.values_list("permission", flat=True)
        self.assert_query_cached(permissions, [permission.pk])
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE {TestParent._meta.db_table} CASCADE")
        self.assert_query_cached(permissions, [])


@skipUnless(DATABASE_ON_DELETE, "database-level on_delete needs Django 6.1")
class DatabaseOnDeleteTestCase(TestUtilsMixin, FilteredTransactionTestCase):
    """Rows the database deletes or updates itself when a row they point to is deleted."""

    @isolate_apps("tests.orm.app")
    def test_deletion_dependents(self):
        class Parent(Row):
            pass

        class ProxyParent(Parent):
            class Meta:
                app_label = "ormtest"
                proxy = True

        class Child(Row):
            parent = models.ForeignKey(Parent, models.DB_CASCADE)

        class GrandChild(Row):
            child = models.ForeignKey(Child, models.DB_SET_NULL, null=True)

        class GreatGrandChild(Row):
            grandchild = models.ForeignKey(GrandChild, models.DB_CASCADE)

        class Collected(Row):
            # Django deletes these itself, with statements of their own.
            parent = models.ForeignKey(Parent, models.CASCADE)

        def tables(*models_):
            return {model._meta.db_table for model in models_}

        # DB_SET_NULL changes the grandchildren, but deletes none of them.
        self.assertSetEqual(deletion_dependents([Parent]), tables(Child, GrandChild))
        self.assertSetEqual(deletion_dependents([ProxyParent]), tables(Child, GrandChild))
        self.assertSetEqual(deletion_dependents([GrandChild]), tables(GreatGrandChild))
        self.assertSetEqual(deletion_dependents([Collected]), set())
        self.assertSetEqual(
            deletion_dependents([Parent], truncate=True),
            tables(Child, GrandChild, GreatGrandChild, Collected),
        )

    @isolate_apps("tests.orm.app")
    def test_delete(self):
        class Parent(Row):
            pass

        class Child(Row):
            parent = models.ForeignKey(Parent, models.DB_CASCADE)

        class GrandChild(Row):
            child = models.ForeignKey(Child, models.DB_SET_NULL, null=True)

        created = (Parent, Child, GrandChild)
        with connection.schema_editor() as editor:
            for model in created:
                editor.create_model(model)

        def drop_tables():
            with connection.schema_editor() as editor:
                for model in reversed(created):
                    editor.delete_model(model)

        self.addCleanup(drop_tables)

        parent = Parent.objects.create()
        child = Child.objects.create(parent=parent)
        GrandChild.objects.create(child=child)
        children = Child.objects.values_list("pk", flat=True)
        grandchildren = GrandChild.objects.values_list("child", flat=True)
        self.assert_query_cached(children, [child.pk])
        self.assert_query_cached(grandchildren, [child.pk])

        parent.delete()

        self.assert_query_cached(children, [])
        self.assert_query_cached(grandchildren, [None])


@skipUnless(connection.vendor == "postgresql", "sets the transaction isolation of PostgreSQL")
class SnapshotIsolationTestCase(TestUtilsMixin, FilteredTransactionTestCase):
    """Transactions that read a snapshot, which may be older than the shared cache."""

    def setUp(self):
        super().setUp()
        # Closing the connection resets its session.
        self.addCleanup(connection.close)

    def set_session_isolation(self, level):
        with connection.cursor() as cursor:
            cursor.execute(f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}")

    def test_results_are_cached_for_the_transaction(self):
        self.set_session_isolation("REPEATABLE READ")
        # Outside transactions, every statement reads the latest data.
        self.assert_query_cached(Test.objects.all())
        with transaction.atomic():
            self.assert_query_cached(Test.objects.all(), [])
            t = Test.objects.create(name="test")
            self.assert_query_cached(Test.objects.all(), [t])
        self.assert_query_cached(Test.objects.all(), [t])

    def test_snapshot_is_not_mixed_with_newer_results(self):
        self.set_session_isolation("REPEATABLE READ")

        class Writer(Thread):
            def run(self):
                try:
                    self.t = Test.objects.create(name="test")
                    self.cached = list(Test.objects.all())
                finally:
                    connection.close()

        with transaction.atomic():
            # The first query takes the snapshot.
            self.assertIsNone(User.objects.first())
            writer = Writer()
            writer.start()
            writer.join()
            self.assertListEqual(writer.cached, [writer.t])
            with self.assertNumQueries(1):
                self.assertListEqual(list(Test.objects.all()), [])
        self.assertListEqual(list(Test.objects.all()), [writer.t])

    def test_isolation_is_read_again(self):
        self.assertEqual(orm_transaction.isolation(connection), orm_transaction.SHARED)
        self.set_session_isolation("SERIALIZABLE")
        self.assertEqual(orm_transaction.isolation(connection), orm_transaction.SNAPSHOT)
        self.set_session_isolation("READ COMMITTED")
        self.assertEqual(orm_transaction.isolation(connection), orm_transaction.SHARED)

    def test_isolation_of_one_transaction(self):
        # Unreadable, so the connection counts as reading snapshots until it reconnects.
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            self.assert_query_cached(Test.objects.all(), [])
        self.assertEqual(orm_transaction.isolation(connection), orm_transaction.SNAPSHOT)
        connection.close()
        self.assertEqual(orm_transaction.isolation(connection), orm_transaction.SHARED)
