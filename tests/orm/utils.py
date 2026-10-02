"""Helpers shared by the ORM cache tests."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see django_cachex/orm/LICENSE.

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.db import DEFAULT_DB_ALIAS, connections
from django.db.models.sql.constants import MULTI
from django.test.utils import CaptureQueriesContext, override_settings

from django_cachex.orm.api import _table_keys
from django_cachex.orm.settings import orm_settings
from django_cachex.orm.store import LocMemStore, RespStore, _entry_key, _generation_key, get_store
from django_cachex.orm.utils import _get_tables, query_key_and_tables
from django_cachex.script import keys_only_pre

if TYPE_CHECKING:
    from collections.abc import Iterator

# Django logs these as queries, but they read nothing.
_TRANSACTION_CONTROL = frozenset({"BEGIN", "COMMIT", "ROLLBACK"})


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


@contextmanager
def assert_num_queries(num: int, using: str = DEFAULT_DB_ALIAS) -> Iterator[CaptureQueriesContext]:
    """Assert that the block runs ``num`` queries on ``using``, not counting BEGIN, COMMIT and ROLLBACK."""
    with CaptureQueriesContext(connections[using]) as context:
        yield context
    queries = [query["sql"] for query in context.captured_queries if query["sql"].upper() not in _TRANSACTION_CONTROL]
    listed = "".join(f"\n{number}. {sql}" for number, sql in enumerate(queries, start=1))
    assert len(queries) == num, f"{len(queries)} queries ran on {context.connection.vendor}, {num} expected:{listed}"


def assert_tables(queryset: Any, *tables: Any) -> None:
    """Assert that the ORM cache finds ``queryset`` reading ``tables``, given as models or table names."""
    expected = {table if isinstance(table, str) else table._meta.db_table for table in tables}
    # Compiled first, as the ORM cache does: compiling joins the tables the ordering needs.
    queryset.query.get_compiler(queryset.db).as_sql()
    assert _get_tables(queryset.db, queryset.query) == expected, str(queryset.query)


def assert_query_cached(
    queryset: Any,
    result: list[Any] | None = None,
    *,
    before: int = 1,
    after: int = 0,
    compare_results: bool = True,
) -> None:
    """Evaluate ``queryset`` twice, running ``before`` queries the first time and ``after`` the second."""
    with assert_num_queries(before, using=queryset.db):
        first = list(queryset.all())
    with assert_num_queries(after, using=queryset.db):
        second = list(queryset.all())
    if compare_results:
        assert second == first
        if result is not None:
            assert second == result


def orm_store() -> Any:
    """Return the store of the ORM cache."""
    return get_store(orm_settings.CACHE)


def evict_generation(db_alias: str, table: str) -> None:
    """Drop the generation of ``table``, as a cache that runs out of memory would."""
    store = orm_store()
    [table_key] = _table_keys(db_alias, [table])
    if isinstance(store, LocMemStore):
        with store.cache._lock:  # ty: ignore[unresolved-attribute]
            store.state.generations.pop((db_alias, table_key), None)
    else:
        store.cache.delete(_generation_key(db_alias, table_key))


def corrupt_entry(queryset: Any) -> None:
    """Replace the cached result of ``queryset`` with bytes that do not decode."""
    compiler = queryset.query.get_compiler(queryset.db)
    query_key, _ = query_key_and_tables(compiler, MULTI)
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
        local_key = store.cache.make_key(entry_key)
        generations, _ = store.local.entries[local_key]
        store.local.entries[local_key] = (generations, b"garbage")
