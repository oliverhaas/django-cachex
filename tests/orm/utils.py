"""Helpers shared by the ORM cache tests."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from functools import wraps
from typing import Any

from django.conf import settings
from django.core.cache import caches
from django.core.management.color import no_style
from django.db import DEFAULT_DB_ALIAS, connection, connections, transaction
from django.db.models.sql.constants import MULTI
from django.test import TransactionTestCase
from django.test.utils import CaptureQueriesContext, override_settings

from django_cachex.orm.settings import orm_settings
from django_cachex.orm.store import LocMemStore, RespStore, _entry_key, _generation_key, get_store
from django_cachex.orm.utils import _get_tables
from django_cachex.script import keys_only_pre
from tests.orm.app.models import PostgresModel


class override_orm_settings(override_settings):  # noqa: N801
    """``override_settings`` for keys of the ``CACHEX_ORM`` dict, merged into its current value."""

    def __init__(self, **kwargs: Any) -> None:
        self.orm_options = kwargs
        super().__init__(CACHEX_ORM=self._merged())

    def _merged(self) -> dict[str, Any]:
        return {**getattr(settings, "CACHEX_ORM", {}), **self.orm_options}

    def enable(self) -> None:
        # Merge at entry so nested overrides stack.
        self.options = {"CACHEX_ORM": self._merged()}
        super().enable()


class TestUtilsMixin:
    def setUp(self):
        self.is_sqlite = connection.vendor == "sqlite"
        self.is_postgresql = connection.vendor == "postgresql"
        self.force_reopen_connection()

    # The flush of TransactionTestCase misses PostgresModel: https://code.djangoproject.com/ticket/29494
    def tearDown(self):
        if connection.vendor == "postgresql":
            flush_sql_list = connection.ops.sql_flush(no_style(), (PostgresModel._meta.db_table,))
            with transaction.atomic():
                for sql in flush_sql_list:
                    with connection.cursor() as cursor:
                        cursor.execute(sql)

    def force_reopen_connection(self):
        if connection.vendor == "postgresql":
            # Reopen the connection now, or Django runs an extra SQL query below.
            connection.cursor()

    def assert_tables(self, queryset, *tables):
        tables = {table if isinstance(table, str) else table._meta.db_table for table in tables}
        self.assertSetEqual(_get_tables(queryset.db, queryset.query), tables, str(queryset.query))

    def assert_query_cached(self, queryset, result=None, result_type=None, compare_results=True, before=1, after=0):
        if result_type is None:
            result_type = list if result is None else type(result)
        with self.assertNumQueries(before):
            data1 = queryset.all()
            if result_type is list:
                data1 = list(data1)
        with self.assertNumQueries(after):
            data2 = queryset.all()
            if result_type is list:
                data2 = list(data2)
        if not compare_results:
            return
        assert_functions = {
            list: self.assertListEqual,
            set: self.assertSetEqual,
            dict: self.assertDictEqual,
        }
        assert_function = assert_functions.get(result_type, self.assertEqual)
        assert_function(data2, data1)
        if result is not None:
            assert_function(data2, result)


class FilteredTransactionTestCase(TransactionTestCase):
    """TransactionTestCase whose assertNumQueries ignores BEGIN, COMMIT and ROLLBACK."""

    def assertNumQueries(self, num, func=None, *args, using=DEFAULT_DB_ALIAS, **kwargs):  # noqa: N802
        conn = connections[using]

        context = FilteredAssertNumQueriesContext(self, num, conn)
        if func is None:
            return context

        with context:
            func(*args, **kwargs)
        return None


class FilteredAssertNumQueriesContext(CaptureQueriesContext):
    """Capture queries and assert their number, ignoring BEGIN, COMMIT and ROLLBACK."""

    EXCLUDE = ("BEGIN", "COMMIT", "ROLLBACK")

    def __init__(self, test_case, num, connection):
        self.test_case = test_case
        self.num = num
        super().__init__(connection)

    def __exit__(self, exc_type, exc_value, traceback):
        super().__exit__(exc_type, exc_value, traceback)
        if exc_type is not None:
            return

        filtered_queries = []
        excluded_queries = []
        for q in self.captured_queries:
            if q["sql"].upper() not in self.EXCLUDE:
                filtered_queries.append(q)
            else:
                excluded_queries.append(q)

        executed = len(filtered_queries)

        self.test_case.assertEqual(
            executed,
            self.num,
            f"\n{executed} queries executed on {self.connection.vendor}, {self.num} expected\n"
            "\nCaptured queries were:\n"
            + "".join(f"{i}. {query['sql']}\n" for i, query in enumerate(filtered_queries, start=1))
            + "\nCaptured queries, that were excluded:\n"
            + "".join(f"{i}. {query['sql']}\n" for i, query in enumerate(excluded_queries, start=1)),
        )


def orm_store() -> Any:
    """Return the store of the ORM cache."""
    return get_store(orm_settings.CACHE)


def evict_generation(db_alias: str, table: str) -> None:
    """Drop the generation of ``table``, as a cache that runs out of memory would."""
    store = orm_store()
    table_key = orm_settings.TABLE_KEYGEN(db_alias, table)
    if isinstance(store, LocMemStore):
        with store.cache._lock:  # ty: ignore[unresolved-attribute]
            store.state.generations.pop((db_alias, table_key), None)
    else:
        store.cache.delete(_generation_key(db_alias, table_key))


def corrupt_entry(queryset: Any) -> None:
    """Replace the cached result of ``queryset`` with bytes that do not decode."""
    compiler = queryset.query.get_compiler(queryset.db)
    query_key = f"{orm_settings.QUERY_KEYGEN(compiler)}:{MULTI}"
    store = orm_store()
    if isinstance(store, LocMemStore):
        entry_key = store._entry_key(queryset.db, query_key)
        with store.cache._lock:  # ty: ignore[unresolved-attribute]
            store.cache._cache[entry_key] = b"garbage"  # ty: ignore[unresolved-attribute]
        return
    assert isinstance(store, RespStore)
    entry_key = _entry_key(queryset.db, query_key)
    store.cache.eval_script(
        "return redis.call('HSET', KEYS[1], 'v', ARGV[1])",
        keys=[entry_key],
        args=[b"garbage"],
        pre_hook=keys_only_pre,
    )
    if store.local is not None:
        generations, _ = store.local.entries[entry_key]
        store.local.entries[entry_key] = (generations, b"garbage")


def all_final_sql_checks(func):
    """Run the test twice, with ``FINAL_SQL_CHECK`` on and off."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        for final_sql_check in (True, False):
            with (
                self.subTest(msg=f"FINAL_SQL_CHECK = {final_sql_check}"),
                override_orm_settings(
                    FINAL_SQL_CHECK=final_sql_check,
                ),
            ):
                func(self, *args, **kwargs)
            caches["default"].clear()

    return wrapper


def no_final_sql_check(func):
    """Run the test with ``FINAL_SQL_CHECK`` off."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with override_orm_settings(FINAL_SQL_CHECK=False):
            func(self, *args, **kwargs)

    return wrapper


def with_final_sql_check(func):
    """Run the test with ``FINAL_SQL_CHECK`` on."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with override_orm_settings(FINAL_SQL_CHECK=True):
            func(self, *args, **kwargs)

    return wrapper
