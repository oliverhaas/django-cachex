"""The generation protocol that keeps cached results in step with writes."""

import logging
import re
import time
import uuid
from types import SimpleNamespace

import pytest
from django.conf import settings
from django.contrib.auth.models import Permission
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.db import DEFAULT_DB_ALIAS, connection, connections, models, transaction
from django.test.utils import isolate_apps, override_settings

from django_cachex.exceptions import CachexError
from django_cachex.orm import transaction as orm_transaction
from django_cachex.orm.api import _table_keys, invalidate
from django_cachex.orm.exceptions import InvalidationError
from django_cachex.orm.settings import orm_settings
from django_cachex.orm.store import Lookup
from django_cachex.orm.utils import deletion_dependents
from tests.orm.app.models import Test, TestChild, TestParent
from tests.orm.utils import assert_query_cached, orm_store, override_orm_settings

# DB_CASCADE, DB_SET_NULL and DB_SET_DEFAULT arrived in Django 6.1.
DATABASE_ON_DELETE = hasattr(models, "DB_CASCADE")


class Row(models.Model):
    """Base of the models that tests declare in an isolated app registry."""

    class Meta:
        abstract = True
        app_label = "ormtest"

    def __str__(self) -> str:
        return str(self.pk)


class Protocol:
    """The store of the configured cache, on tables and a query no other test uses."""

    def __init__(self):
        self.store = orm_store()
        suffix = uuid.uuid4().hex
        self.tables = [f"a_{suffix}", f"b_{suffix}"]
        self.query = f"query_{suffix}"

    def lookup(self):
        return self.store.lookup(DEFAULT_DB_ALIAS, self.query, self.tables)

    def store_result(self, token, result="result", *, timeout=None):
        return self.store.store(DEFAULT_DB_ALIAS, self.query, token, result, timeout)


@pytest.fixture
def protocol():
    return Protocol()


def test_hit_after_store(protocol):
    lookup = protocol.lookup()
    assert not lookup.hit
    assert lookup.token is not None
    assert protocol.store_result(lookup.token, [1, 2])
    assert protocol.lookup() == Lookup(hit=True, value=[1, 2])


def test_bump(protocol):
    assert protocol.store_result(protocol.lookup().token)
    protocol.store.bump(DEFAULT_DB_ALIAS, protocol.tables[1:])
    lookup = protocol.lookup()
    assert not lookup.hit
    assert protocol.store_result(lookup.token, "new")
    assert protocol.lookup() == Lookup(hit=True, value="new")


def test_result_read_before_a_bump_is_not_served(protocol):
    token = protocol.lookup().token
    protocol.store.bump(DEFAULT_DB_ALIAS, protocol.tables[1:])
    protocol.store_result(token)
    assert not protocol.lookup().hit


def test_generations(protocol):
    generations = protocol.store.generations(DEFAULT_DB_ALIAS, protocol.tables)
    assert len(generations) == 2
    assert all(isinstance(generation, str) for generation in generations)
    assert protocol.store.generations(DEFAULT_DB_ALIAS, protocol.tables) == generations
    protocol.store.bump(DEFAULT_DB_ALIAS, protocol.tables[:1])
    bumped = protocol.store.generations(DEFAULT_DB_ALIAS, protocol.tables)
    assert bumped[0] != generations[0]
    assert bumped[1] == generations[1]


def test_ttl(protocol):
    assert protocol.store.ttl(None) is None
    assert protocol.store.ttl(5) == 5
    assert protocol.store.ttl(DEFAULT_TIMEOUT) == protocol.store.cache.default_timeout


def test_timeout(protocol):
    token = protocol.lookup().token
    assert not protocol.store_result(token, timeout=0)
    assert not protocol.store_result(token, timeout=-1)
    assert not protocol.lookup().hit
    assert protocol.store_result(token, timeout=0.3)
    assert protocol.lookup().hit
    time.sleep(0.4)
    assert not protocol.lookup().hit


# What a transaction wrote, across its savepoints.


@pytest.fixture
def conn():
    """A stand-in for a database connection, which the transaction state is kept on."""
    return SimpleNamespace()


def test_written(conn):
    assert orm_transaction.written(conn) == set()
    orm_transaction.mark_written(conn, {"a"})
    orm_transaction.savepoint_created(conn, "s1")
    orm_transaction.mark_written(conn, {"b"})
    orm_transaction.savepoint_created(conn, "s2")
    orm_transaction.mark_written(conn, {"c"})
    assert orm_transaction.written(conn) == {"a", "b", "c"}

    orm_transaction.savepoint_rolled_back(conn, "s2")
    assert orm_transaction.written(conn) == {"a", "b"}
    # The savepoint survives a rollback to it.
    orm_transaction.mark_written(conn, {"d"})
    orm_transaction.savepoint_released(conn, "s2")
    assert orm_transaction.written(conn) == {"a", "b", "d"}

    orm_transaction.savepoint_rolled_back(conn, "s1")
    assert orm_transaction.written(conn) == {"a"}
    orm_transaction.mark_written(conn, {"e"})
    orm_transaction.savepoint_released(conn, "s1")
    assert orm_transaction.written(conn) == {"a", "e"}

    orm_transaction.reset(conn)
    assert orm_transaction.written(conn) == set()


def test_unknown_savepoint(conn):
    orm_transaction.mark_written(conn, {"a"})
    orm_transaction.savepoint_rolled_back(conn, "unknown")
    orm_transaction.savepoint_released(conn, "unknown")
    assert orm_transaction.written(conn) == {"a"}


# Generation bumps around the writes of the ORM and raw SQL.


def generation():
    """The generation of the table of Test."""
    [current] = orm_store().generations(DEFAULT_DB_ALIAS, _table_keys(DEFAULT_DB_ALIAS, [Test._meta.db_table]))
    return current


def record_generations(mocker, method):
    """Patch ``method`` of the connection to record the generation of Test when it runs."""
    wrapper = connections[DEFAULT_DB_ALIAS]
    original = getattr(wrapper, method)
    generations = []

    def recording(*args, **kwargs):
        generations.append(generation())
        return original(*args, **kwargs)

    mocker.patch.object(wrapper, method, recording)
    return generations


@pytest.mark.django_db(transaction=True)
def test_commit_is_bumped_before_and_after(mocker):
    before = generation()
    at_commit = record_generations(mocker, "_commit")
    with transaction.atomic():
        t = Test.objects.create(name="test")
        # Other connections use the cache until the transaction commits.
        assert generation() == before
    [during] = at_commit
    assert during != before
    assert generation() != during
    assert_query_cached(Test.objects.all(), [t])


@pytest.mark.django_db(transaction=True)
def test_commit_without_writes_bumps_nothing():
    assert_query_cached(Test.objects.all())
    with transaction.atomic():
        list(Test.objects.all())
    assert_query_cached(Test.objects.all(), [], before=0)


@pytest.mark.django_db(transaction=True)
def test_rollback_bumps_nothing():
    assert_query_cached(Test.objects.all())
    with transaction.atomic():
        Test.objects.create(name="test")
        transaction.set_rollback(True)
    # Nothing was committed, so the cached result stays valid.
    assert_query_cached(Test.objects.all(), [], before=0)


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(connection.vendor != "sqlite", reason="SQLite commits when autocommit is turned back on")
def test_set_autocommit_is_bumped_before_and_after(mocker):
    assert_query_cached(Test.objects.all())
    before = generation()
    transaction.set_autocommit(False)
    try:
        t = Test.objects.create(name="test")
    finally:
        at_commit = record_generations(mocker, "_set_autocommit")
        transaction.set_autocommit(True)
    [during] = at_commit
    assert during != before
    assert generation() != during
    assert_query_cached(Test.objects.all(), [t])


@pytest.mark.django_db(transaction=True)
def test_failed_bump_aborts_the_write(mocker):
    mocker.patch.object(type(orm_store()), "bump", side_effect=ConnectionError("cache down"))
    message = re.escape("Could not invalidate the ORM cache of ormtest_test in database 'default'")
    with pytest.raises(InvalidationError, match=message) as raised:
        Test.objects.create(name="autocommit")
    assert isinstance(raised.value, CachexError)
    assert isinstance(raised.value.__cause__, ConnectionError)
    with pytest.raises(InvalidationError, match=message), transaction.atomic():
        Test.objects.create(name="atomic")
    assert not Test.objects.exists()


@pytest.mark.django_db(transaction=True)
def test_failed_bump_while_disabled(mocker, caplog):
    # Nothing is served from a disabled cache, so the write goes ahead.
    mocker.patch.object(type(orm_store()), "bump", side_effect=ConnectionError("cache down"))
    with override_orm_settings(ENABLED=False), caplog.at_level(logging.WARNING, logger="django_cachex.orm"):
        t = Test.objects.create(name="test")
    assert {record.name for record in caplog.records} == {"django_cachex.orm.api"}
    assert list(Test.objects.all()) == [t]


@pytest.mark.django_db(transaction=True)
def test_failed_bump_after_the_write(mocker, caplog):
    assert_query_cached(Test.objects.all(), [])
    bump = type(orm_store()).bump
    bumps = []

    def first_bump_only(store, db_alias, table_keys):
        bumps.append(table_keys)
        if len(bumps) > 1:
            raise ConnectionError("cache down")
        bump(store, db_alias, table_keys)

    mocker.patch.object(type(orm_store()), "bump", first_bump_only)
    with caplog.at_level(logging.WARNING, logger="django_cachex.orm"):
        t = Test.objects.create(name="test")
    assert len(bumps) == 2
    assert {record.name for record in caplog.records} == {"django_cachex.orm.monkey_patch"}
    # The bump before the write already dropped the result cached before it.
    assert_query_cached(Test.objects.all(), [t])


@pytest.mark.django_db(transaction=True)
def test_failed_lookup(mocker, caplog):
    lookup = mocker.patch.object(type(orm_store()), "lookup", side_effect=ConnectionError("cache down"))
    with caplog.at_level(logging.WARNING, logger="django_cachex.orm"):
        assert_query_cached(Test.objects.all(), [], after=1)
    assert {record.name for record in caplog.records} == {"django_cachex.orm.monkey_patch"}
    mocker.stop(lookup)
    assert_query_cached(Test.objects.all(), [])


@pytest.mark.django_db(transaction=True)
def test_failed_store(mocker, caplog):
    store = mocker.patch.object(type(orm_store()), "store", side_effect=ConnectionError("cache down"))
    with caplog.at_level(logging.WARNING, logger="django_cachex.orm"):
        assert_query_cached(Test.objects.all(), [], after=1)
    assert {record.name for record in caplog.records} == {"django_cachex.orm.monkey_patch"}
    mocker.stop(store)
    assert_query_cached(Test.objects.all(), [])


@pytest.mark.django_db(transaction=True)
def test_failed_invalidate(mocker, caplog):
    mocker.patch.object(type(orm_store()), "bump", side_effect=ConnectionError("cache down"))
    with pytest.raises(InvalidationError):
        invalidate(Test, cache_alias=orm_settings.CACHE)
    with override_orm_settings(ENABLED=False), caplog.at_level(logging.WARNING, logger="django_cachex.orm"):
        invalidate(Test, cache_alias=orm_settings.CACHE)
    assert {record.name for record in caplog.records} == {"django_cachex.orm.api"}


@pytest.mark.django_db(transaction=True)
def test_failed_invalidate_of_another_cache():
    assert_query_cached(Test.objects.all(), [])
    unreachable = {"BACKEND": "django_cachex.cache.RedisCache", "LOCATION": "redis://127.0.0.1:1"}
    with (
        override_settings(CACHES={"unreachable": unreachable, **settings.CACHES}),
        pytest.raises(InvalidationError, match=re.escape("in cache 'unreachable' for databases")),
    ):
        invalidate(Test)
    assert_query_cached(Test.objects.all(), [])


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(connection.vendor != "postgresql", reason="TRUNCATE ... CASCADE is PostgreSQL's")
def test_truncate_cascade():
    permission = Permission.objects.first()
    child = TestChild.objects.create(name="child")
    child.permissions.add(permission)
    permissions = TestChild.permissions.through.objects.values_list("permission", flat=True)
    assert_query_cached(permissions, [permission.pk])
    with connection.cursor() as cursor:
        cursor.execute(f"TRUNCATE {TestParent._meta.db_table} CASCADE")
    assert_query_cached(permissions, [])


# Rows the database deletes or updates itself when a row they point to is deleted.


@pytest.fixture
def create_tables():
    """Create the tables of models a test declares, and drop them after it."""
    created = []

    def create(*models_):
        with connection.schema_editor() as editor:
            for model in models_:
                editor.create_model(model)
                created.append(model)

    yield create
    with connection.schema_editor() as editor:
        for model in reversed(created):
            editor.delete_model(model)


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(not DATABASE_ON_DELETE, reason="database-level on_delete needs Django 6.1")
@isolate_apps("tests.orm.app")
def test_deletion_dependents():
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
    assert deletion_dependents([Parent]) == tables(Child, GrandChild)
    assert deletion_dependents([ProxyParent]) == tables(Child, GrandChild)
    assert deletion_dependents([GrandChild]) == tables(GreatGrandChild)
    assert deletion_dependents([Collected]) == set()
    assert deletion_dependents([Parent], truncate=True) == tables(Child, GrandChild, GreatGrandChild, Collected)


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(not DATABASE_ON_DELETE, reason="database-level on_delete needs Django 6.1")
@isolate_apps("tests.orm.app")
def test_delete(create_tables):
    class Parent(Row):
        pass

    class Child(Row):
        parent = models.ForeignKey(Parent, models.DB_CASCADE)

    class GrandChild(Row):
        child = models.ForeignKey(Child, models.DB_SET_NULL, null=True)

    create_tables(Parent, Child, GrandChild)

    parent = Parent.objects.create()
    child = Child.objects.create(parent=parent)
    GrandChild.objects.create(child=child)
    children = Child.objects.values_list("pk", flat=True)
    grandchildren = GrandChild.objects.values_list("child", flat=True)
    assert_query_cached(children, [child.pk])
    assert_query_cached(grandchildren, [child.pk])

    parent.delete()

    assert_query_cached(children, [])
    assert_query_cached(grandchildren, [None])
