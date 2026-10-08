"""Patches Django's SQL compilers, cursor and connections to cache query results."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import logging
import re
import types
from collections.abc import Callable, Iterable
from functools import wraps
from typing import Any

from django.core.exceptions import EmptyResultSet
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.utils import CursorWrapper
from django.db.models.signals import post_migrate
from django.db.models.sql.compiler import SQLCompiler, SQLDeleteCompiler, SQLInsertCompiler, SQLUpdateCompiler
from django.db.models.sql.constants import GET_ITERATOR_CHUNK_SIZE, MULTI, SINGLE

from django_cachex.orm import transaction
from django_cachex.orm.api import LOCAL_STORAGE, _invalidation_failed, _table_keys, invalidate
from django_cachex.orm.settings import ITERABLES, orm_settings
from django_cachex.orm.store import Store, get_store
from django_cachex.orm.utils import (
    UncachableQuery,
    _get_tables_from_sql,
    deletion_dependents,
    filter_cachable,
    models_of_tables,
    query_key_and_tables,
)

logger = logging.getLogger(__name__)

WRITE_COMPILERS = (SQLInsertCompiler, SQLUpdateCompiler, SQLDeleteCompiler)

# Other result types return cursors or row counts, which are not cached.
_CACHED_RESULT_TYPES = frozenset({MULTI, SINGLE})

# Raw SQL that can change data or schema, matched on the lowercased SQL. A
# statement that only reads but matches costs an invalidation, nothing more.
SQL_DATA_CHANGE_RE = re.compile(
    r"\b(?:insert|update|delete|truncate|alter|create|drop|refresh)\b|\b(?:replace|merge)\s+into\b",
)
_TRUNCATE_CASCADE_RE = re.compile(r"\btruncate\b.*\bcascade\b", flags=re.DOTALL)
# Raw SQL changing the default isolation of the session, which can be read back.
# Other statements naming isolation change it for one transaction only.
_SESSION_ISOLATION_RE = re.compile(r"default_transaction_isolation|session\s+characteristics|journal_mode")

# Set on a connection while a compiler runs, so the cursor patch leaves the
# compiler's SQL alone.
_COMPILING = "_cachex_orm_compiling"
# Tables the write running on a connection covers, so a nested write to the
# same tables does not bump them again.
_WRITING = "_cachex_orm_writing"

_NOTHING: frozenset[str] = frozenset()


def _execute(execute: Callable[[], Any]) -> tuple[Any, bool]:
    """Run the query; return its result, materialized unless iterator() streams it, and whether it can be cached."""
    result = execute()
    if result.__class__ is types.GeneratorType:
        return result, False
    if result.__class__ not in ITERABLES and isinstance(result, Iterable):
        result = list(result)
    return result, True


def _read(compiler: Any, result_type: Any, execute: Callable[[], Any]) -> Any:
    connection = compiler.connection
    store = get_store(orm_settings.CACHE)
    if store is None:
        return execute()
    ttl = store.ttl(orm_settings.TIMEOUT)
    if ttl is not None and ttl <= 0:
        return execute()
    try:
        query_key, tables = query_key_and_tables(compiler, result_type)
    except EmptyResultSet, UncachableQuery:
        return execute()
    if transaction.in_transaction(connection):
        if transaction.isolation(connection) == transaction.SNAPSHOT:
            return _read_in_snapshot(connection, query_key, tables, execute)
        # The transaction reads its own writes, which the shared cache must not see.
        if not tables.isdisjoint(transaction.written(connection)):
            return execute()
    return _read_shared(store, connection.alias, query_key, tables, execute)


def _read_shared(store: Store, db_alias: str, query_key: str, tables: set[str], execute: Callable[[], Any]) -> Any:
    table_keys = _table_keys(db_alias, tables)
    try:
        lookup = store.lookup(db_alias, query_key, table_keys)
    except Exception:
        logger.warning("ORM cache lookup failed; the query runs against the database.", exc_info=True)
        return execute()
    if lookup.hit:
        return lookup.value
    result, cachable = _execute(execute)
    if cachable and lookup.token is not None:
        try:
            store.store(db_alias, query_key, lookup.token, result, orm_settings.TIMEOUT)
        except Exception:
            logger.warning("Could not store a query result in the ORM cache.", exc_info=True)
    return result


def _read_in_snapshot(connection: Any, query_key: str, tables: set[str], execute: Callable[[], Any]) -> Any:
    hit, value = transaction.cached(connection, query_key)
    if hit:
        return value
    result, cachable = _execute(execute)
    if cachable:
        try:
            transaction.cache(connection, query_key, tables, result)
        except Exception:
            logger.warning("Could not cache a query result for the transaction.", exc_info=True)
    return result


def _patch_read(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def execute_sql(
        compiler: Any,
        result_type: Any = MULTI,
        chunked_fetch: bool = False,
        chunk_size: int = GET_ITERATOR_CHUNK_SIZE,
    ) -> Any:
        def execute() -> Any:
            return original(compiler, result_type, chunked_fetch, chunk_size)

        connection = compiler.connection
        was_compiling = getattr(connection, _COMPILING, False)
        setattr(connection, _COMPILING, True)
        try:
            if (
                orm_settings.ENABLED
                and getattr(LOCAL_STORAGE, "orm_cache_enabled", True)
                and result_type in _CACHED_RESULT_TYPES
                and not isinstance(compiler, WRITE_COMPILERS)
                # EXPLAIN describes the plan, which a write to the tables does not change.
                and compiler.query.explain_info is None
                and connection.alias in orm_settings.DATABASES
            ):
                return _read(compiler, result_type, execute)
            return execute()
        finally:
            setattr(connection, _COMPILING, was_compiling)

    return execute_sql


def _bumped(connection: Any, tables: set[str], run: Callable[[], Any]) -> Any:
    """Run ``run``, which writes to ``tables`` and commits, between two bumps of their generations.

    The bump after the commit drops the results read while the write ran. The one before keeps the write from
    committing while the cache cannot be invalidated, and limits a lost second bump to those results.
    """
    store = get_store(orm_settings.CACHE)
    if store is None:
        return run()
    db_alias = connection.alias
    try:
        # Inside the try, since atomic() rolls back a failed COMMIT on a DatabaseError like InvalidationError only.
        table_keys = _table_keys(db_alias, tables)
        store.bump(db_alias, table_keys)
    except Exception as e:  # noqa: BLE001
        message = f"Could not invalidate the ORM cache of {', '.join(sorted(tables))} in database {db_alias!r}"
        _invalidation_failed(e, message)
        return run()
    try:
        return run()
    finally:
        try:
            store.bump(db_alias, table_keys)
        except Exception:
            logger.warning(
                "Could not invalidate the ORM cache of %s in database %r after the write; results read while it "
                "ran can be served until they expire.",
                ", ".join(sorted(tables)),
                db_alias,
                exc_info=True,
            )


def _write(connection: Any, tables: set[str], run: Callable[[], Any]) -> Any:
    """Run ``run``, a statement writing to ``tables``, and invalidate their cached queries."""
    if not tables or connection.alias not in orm_settings.DATABASES:
        return run()
    active = getattr(connection, _WRITING, _NOTHING)
    if tables <= active:
        return run()
    setattr(connection, _WRITING, active | tables)
    try:
        if transaction.in_transaction(connection):
            # Queries the statement runs itself, like the pre-select of an
            # update, read the tables too: mark them once more afterwards.
            transaction.mark_written(connection, tables)
            try:
                return run()
            finally:
                transaction.mark_written(connection, tables)
        # Under autocommit the statement commits itself.
        return _bumped(connection, tables, run)
    finally:
        setattr(connection, _WRITING, active)


def _patch_write(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def inner(compiler: Any, *args: Any, **kwargs: Any) -> Any:
        connection = compiler.connection
        was_compiling = getattr(connection, _COMPILING, False)
        setattr(connection, _COMPILING, True)
        try:
            meta = compiler.query.get_meta()
            tables = {meta.db_table}
            if isinstance(compiler, SQLDeleteCompiler):
                tables |= deletion_dependents([meta.model])
            return _write(connection, filter_cachable(tables), lambda: original(compiler, *args, **kwargs))
        finally:
            setattr(connection, _COMPILING, was_compiling)

    return inner


def _patch_cursor(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def inner(cursor: Any, sql: Any, *args: Any, **kwargs: Any) -> Any:
        connection = cursor.db
        if getattr(connection, _COMPILING, False) or connection.alias not in orm_settings.DATABASES:
            return original(cursor, sql, *args, **kwargs)
        lowered = (sql.decode(errors="replace") if isinstance(sql, bytes) else str(sql)).lower()
        tables: set[str] = set()
        if SQL_DATA_CHANGE_RE.search(lowered):
            # The tables the SQL can write to, and those it changes through their foreign keys.
            tables = _get_tables_from_sql(connection, lowered)
            truncate = bool(_TRUNCATE_CASCADE_RE.search(lowered))
            if tables and (truncate or "delete" in lowered):
                tables |= deletion_dependents(models_of_tables(tables), truncate=truncate)
            tables = filter_cachable(tables)
        result = _write(connection, tables, lambda: original(cursor, sql, *args, **kwargs))
        if "isolation" in lowered or "journal_mode" in lowered:
            transaction.isolation_changed(connection, known=bool(_SESSION_ISOLATION_RE.search(lowered)))
        return result

    return inner


def _patch_commit(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def commit(connection: Any) -> None:
        tables = transaction.written(connection)
        if tables:
            _bumped(connection, tables, lambda: original(connection))
        else:
            original(connection)
        transaction.reset(connection)

    return commit


def _patch_set_autocommit(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def set_autocommit(connection: Any, autocommit: bool, *args: Any, **kwargs: Any) -> None:
        if not autocommit:
            began = not transaction.in_transaction(connection)
            original(connection, autocommit, *args, **kwargs)
            if began:
                # A new transaction begins.
                transaction.reset(connection)
            return
        tables = transaction.written(connection)
        if tables:
            # SQLite commits a pending transaction when autocommit is turned on.
            _bumped(connection, tables, lambda: original(connection, autocommit, *args, **kwargs))
        else:
            original(connection, autocommit, *args, **kwargs)
        transaction.reset(connection)

    return set_autocommit


def _patch_rollback(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def rollback(connection: Any) -> None:
        original(connection)
        transaction.reset(connection)

    return rollback


def _patch_close(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def close(connection: Any) -> None:
        try:
            original(connection)
        finally:
            transaction.reset(connection)

    return close


def _patch_connect(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def connect(connection: Any) -> None:
        transaction.reset(connection)
        original(connection)

    return connect


def _patch_savepoint(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def savepoint(connection: Any) -> str | None:
        sid = original(connection)
        if sid is not None:
            transaction.savepoint_created(connection, sid)
        return sid

    return savepoint


def _patch_savepoint_rollback(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def savepoint_rollback(connection: Any, sid: str) -> None:
        # Django ignores the call when savepoints are not allowed.
        allowed = connection._savepoint_allowed()
        original(connection, sid)
        if allowed:
            transaction.savepoint_rolled_back(connection, sid)

    return savepoint_rollback


def _patch_savepoint_commit(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def savepoint_commit(connection: Any, sid: str) -> None:
        allowed = connection._savepoint_allowed()
        original(connection, sid)
        if allowed:
            transaction.savepoint_released(connection, sid)

    return savepoint_commit


def _invalidate_on_migration(sender: Any, *, using: str, plan: Any = None, **kwargs: Any) -> None:  # noqa: ARG001
    # migrate sends an empty plan when it applied nothing, flush no plan.
    if plan is not None and not plan:
        return
    models = list(sender.get_models(include_auto_created=True))
    if models:
        invalidate(*models, db_alias=using, cache_alias=orm_settings.CACHE)


def patch() -> None:
    """Patch Django to cache query results and invalidate them on writes."""
    # All originals are looked up before any is replaced: SQLDeleteCompiler
    # inherits SQLCompiler.execute_sql.
    patches: list[tuple[type, str, Any]] = [
        (SQLCompiler, "execute_sql", _patch_read(SQLCompiler.execute_sql)),
        (SQLDeleteCompiler, "execute_sql", _patch_write(SQLDeleteCompiler.execute_sql)),
        (SQLInsertCompiler, "execute_sql", _patch_write(SQLInsertCompiler.execute_sql)),
        (SQLUpdateCompiler, "execute_sql", _patch_write(SQLUpdateCompiler.execute_sql)),
        (SQLUpdateCompiler, "execute_returning_sql", _patch_write(SQLUpdateCompiler.execute_returning_sql)),
        (CursorWrapper, "execute", _patch_cursor(CursorWrapper.execute)),
        (CursorWrapper, "executemany", _patch_cursor(CursorWrapper.executemany)),
    ]
    patches.extend(
        (BaseDatabaseWrapper, name, patcher(getattr(BaseDatabaseWrapper, name)))
        for name, patcher in (
            ("commit", _patch_commit),
            ("set_autocommit", _patch_set_autocommit),
            ("rollback", _patch_rollback),
            ("close", _patch_close),
            ("connect", _patch_connect),
            ("savepoint", _patch_savepoint),
            ("savepoint_rollback", _patch_savepoint_rollback),
            ("savepoint_commit", _patch_savepoint_commit),
        )
    )
    for cls, name, patched in patches:
        setattr(cls, name, patched)
    post_migrate.connect(_invalidate_on_migration, dispatch_uid="django_cachex.orm.invalidate_on_migration")
