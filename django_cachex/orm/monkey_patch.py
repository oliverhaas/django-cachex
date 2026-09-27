"""Patches Django's SQL compilers, cursor and atomic blocks to cache query results."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import re
import types
from collections.abc import Callable, Iterable
from functools import wraps
from time import time
from typing import Any

from django.core.exceptions import EmptyResultSet
from django.db.backends.utils import CursorWrapper
from django.db.models.signals import post_migrate
from django.db.models.sql.compiler import SQLCompiler, SQLDeleteCompiler, SQLInsertCompiler, SQLUpdateCompiler
from django.db.transaction import Atomic, get_connection

from django_cachex.orm.api import LOCAL_STORAGE, invalidate
from django_cachex.orm.cache import orm_caches
from django_cachex.orm.settings import ITERABLES, orm_settings
from django_cachex.orm.utils import (
    UncachableQuery,
    _get_table_cache_keys,
    _get_tables_from_sql,
    filter_cachable,
    is_cachable,
)

WRITE_COMPILERS = (SQLInsertCompiler, SQLUpdateCompiler, SQLDeleteCompiler)

SQL_DATA_CHANGE_RE = re.compile(
    "|".join(
        [
            rf"(\W|\A){re.escape(keyword)}(\W|\Z)"
            for keyword in ["update", "insert", "delete", "alter", "create", "drop"]
        ],
    ),
    flags=re.IGNORECASE,
)


def _unset_raw_connection(original: Callable[..., Any]) -> Callable[..., Any]:
    def inner(compiler: Any, *args: Any, **kwargs: Any) -> Any:
        compiler.connection.raw = False
        try:
            return original(compiler, *args, **kwargs)
        finally:
            compiler.connection.raw = True

    return inner


def _get_result_or_execute_query(
    execute_query_func: Callable[[], Any],
    cache: Any,
    cache_key: str,
    table_cache_keys: list[str],
) -> Any:
    try:
        data = cache.get_many([*table_cache_keys, cache_key])
    except KeyError, ModuleNotFoundError:
        data = None

    new_table_cache_keys = set(table_cache_keys)
    if data:
        new_table_cache_keys.difference_update(data)

        if not new_table_cache_keys:
            try:
                timestamp, result = data.pop(cache_key)
                if timestamp >= max(data.values()):
                    return result
            except KeyError, TypeError, ValueError:
                # A missing or broken entry: run the query and cache it again.
                pass

    result = execute_query_func()

    if result.__class__ == types.GeneratorType and not orm_settings.CACHE_ITERATORS:
        return result

    if result.__class__ not in ITERABLES and isinstance(result, Iterable):
        result = list(result)

    now = time()
    to_be_set: dict[str, Any] = dict.fromkeys(new_table_cache_keys, now)
    to_be_set[cache_key] = (now, result)
    cache.set_many(to_be_set, orm_settings.TIMEOUT)

    return result


def _patch_compiler(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    @_unset_raw_connection
    def inner(compiler: Any, *args: Any, **kwargs: Any) -> Any:
        def execute_query_func() -> Any:
            return original(compiler, *args, **kwargs)

        if not getattr(LOCAL_STORAGE, "orm_cache_enabled", True):
            return execute_query_func()

        db_alias = compiler.using
        if db_alias not in orm_settings.DATABASES or isinstance(compiler, WRITE_COMPILERS):
            return execute_query_func()

        try:
            cache_key = orm_settings.QUERY_KEYGEN(compiler)
            table_cache_keys = _get_table_cache_keys(compiler)
        except EmptyResultSet, UncachableQuery:
            return execute_query_func()

        return _get_result_or_execute_query(
            execute_query_func,
            orm_caches.get_cache(db_alias=db_alias),
            cache_key,
            table_cache_keys,
        )

    return inner


def _patch_write_compiler(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    @_unset_raw_connection
    def inner(write_compiler: Any, *args: Any, **kwargs: Any) -> Any:
        db_alias = write_compiler.using
        table = write_compiler.query.get_meta().db_table
        if is_cachable(table):
            invalidate(table, db_alias=db_alias, cache_alias=orm_settings.CACHE)
        return original(write_compiler, *args, **kwargs)

    return inner


def _patch_orm() -> None:
    if orm_settings.ENABLED:
        SQLCompiler.execute_sql = _patch_compiler(SQLCompiler.execute_sql)  # ty: ignore[invalid-assignment]
    for compiler in WRITE_COMPILERS:
        compiler.execute_sql = _patch_write_compiler(compiler.execute_sql)  # ty: ignore[invalid-assignment]


def _unpatch_orm() -> None:
    if hasattr(SQLCompiler.execute_sql, "__wrapped__"):
        SQLCompiler.execute_sql = SQLCompiler.execute_sql.__wrapped__  # ty: ignore[invalid-assignment]
    for compiler in WRITE_COMPILERS:
        compiler.execute_sql = compiler.execute_sql.__wrapped__  # ty: ignore[invalid-assignment, unresolved-attribute]


def _patch_cursor() -> None:
    def _patch_cursor_execute(original: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(original)
        def inner(cursor: Any, sql: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return original(cursor, sql, *args, **kwargs)
            finally:
                connection = cursor.db
                if getattr(connection, "raw", True):
                    if isinstance(sql, bytes):
                        sql = sql.decode("utf-8")
                    sql = sql.lower()
                    if SQL_DATA_CHANGE_RE.search(sql):
                        tables = filter_cachable(_get_tables_from_sql(connection, sql))
                        if tables:
                            invalidate(*tables, db_alias=connection.alias, cache_alias=orm_settings.CACHE)

        return inner

    if orm_settings.INVALIDATE_RAW:
        CursorWrapper.execute = _patch_cursor_execute(CursorWrapper.execute)  # ty: ignore[invalid-assignment]
        CursorWrapper.executemany = _patch_cursor_execute(CursorWrapper.executemany)  # ty: ignore[invalid-assignment]


def _unpatch_cursor() -> None:
    if hasattr(CursorWrapper.execute, "__wrapped__"):
        CursorWrapper.execute = CursorWrapper.execute.__wrapped__  # ty: ignore[invalid-assignment]
        CursorWrapper.executemany = CursorWrapper.executemany.__wrapped__  # ty: ignore[invalid-assignment, unresolved-attribute]


def _patch_atomic() -> None:
    def patch_enter(original: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(original)
        def inner(self: Atomic) -> None:
            orm_caches.enter_atomic(self.using)
            original(self)

        return inner

    def patch_exit(original: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(original)
        def inner(self: Atomic, exc_type: Any, exc_value: Any, traceback: Any) -> None:
            needs_rollback = get_connection(self.using).needs_rollback
            try:
                original(self, exc_type, exc_value, traceback)
            finally:
                orm_caches.exit_atomic(self.using, exc_type is None and not needs_rollback)

        return inner

    Atomic.__enter__ = patch_enter(Atomic.__enter__)  # ty: ignore[invalid-assignment]
    Atomic.__exit__ = patch_exit(Atomic.__exit__)  # ty: ignore[invalid-assignment]


def _unpatch_atomic() -> None:
    Atomic.__enter__ = Atomic.__enter__.__wrapped__  # ty: ignore[invalid-assignment, unresolved-attribute]
    Atomic.__exit__ = Atomic.__exit__.__wrapped__  # ty: ignore[invalid-assignment, unresolved-attribute]


def _invalidate_on_migration(sender: Any, **kwargs: Any) -> None:
    invalidate(*sender.get_models(), db_alias=kwargs["using"], cache_alias=orm_settings.CACHE)


def patch() -> None:
    post_migrate.connect(_invalidate_on_migration)

    _patch_cursor()
    _patch_atomic()
    _patch_orm()


def unpatch() -> None:
    post_migrate.disconnect(_invalidate_on_migration)

    _unpatch_cursor()
    _unpatch_atomic()
    _unpatch_orm()
