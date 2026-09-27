"""Patches Django's SQL compilers, cursor and connections to cache query results."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import logging
import re
import types
import uuid
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
from django_cachex.orm.api import LOCAL_STORAGE, _invalidation_failed, _send_signals, _table_keys, invalidate
from django_cachex.orm.settings import ITERABLES, orm_settings
from django_cachex.orm.store import Store, get_store
from django_cachex.orm.utils import (
    UncachableQuery,
    _get_tables,
    _get_tables_from_sql,
    deletion_dependents,
    filter_cachable,
    models_of_tables,
)

logger = logging.getLogger("django_cachex.orm")

WRITE_COMPILERS = (SQLInsertCompiler, SQLUpdateCompiler, SQLDeleteCompiler)

# Other result types return cursors or row counts, which are not cached.
_CACHED_RESULT_TYPES = frozenset({MULTI, SINGLE})

# Raw SQL that may change data or schema, matched on the lowercased SQL. A
# statement that only reads but matches costs an invalidation, nothing more.
SQL_DATA_CHANGE_RE = re.compile(r"\b(?:insert|update|delete|truncate|alter|create|drop)\b|\b(?:replace|merge)\s+into\b")
_TRUNCATE_CASCADE_RE = re.compile(r"\btruncate\b.*\bcascade\b", flags=re.DOTALL)
# Raw SQL changing the default isolation of the session, which can be read back.
# Other statements naming isolation change it for one transaction only.
_SESSION_ISOLATION_RE = re.compile(r"default_transaction_isolation|session\s+characteristics|journal_mode")

# Set on a connection while a compiler runs, so the cursor patch leaves the
# compiler's SQL alone.
_COMPILING = "_cachex_orm_compiling"
# Tables the write running on a connection covers, so a nested write to the
# same tables does not take a second lease.
_WRITING = "_cachex_orm_writing"

_NOTHING: frozenset[str] = frozenset()


def _cachable_call(compiler: Any, result_type: Any) -> bool:
    return (
        orm_settings.ENABLED
        and getattr(LOCAL_STORAGE, "orm_cache_enabled", True)
        and result_type in _CACHED_RESULT_TYPES
        and not isinstance(compiler, WRITE_COMPILERS)
        # EXPLAIN describes the plan, which a write to the tables does not change.
        and compiler.query.explain_info is None
        and compiler.connection.alias in orm_settings.DATABASES
    )


def _execute(execute: Callable[[], Any]) -> tuple[Any, bool]:
    """Run the query; return its result, materialized, and whether it may be cached."""
    result = execute()
    if result.__class__ is types.GeneratorType and not orm_settings.CACHE_ITERATORS:
        return result, False
    if result.__class__ not in ITERABLES and isinstance(result, Iterable):
        result = list(result)
    return result, True


def _key_and_tables(compiler: Any, result_type: Any, store: Store | None) -> tuple[str, set[str]] | None:
    """Return the cache key and the tables of the query, or None if its result is not cached."""
    if store is None:
        return None
    ttl = store.ttl(orm_settings.TIMEOUT)
    if ttl is not None and ttl <= 0:
        return None
    try:
        # A SINGLE and a MULTI query can share their SQL but not their result.
        query_key = f"{orm_settings.QUERY_KEYGEN(compiler)}:{result_type}"
        tables = _get_tables(compiler.connection.alias, compiler.query, compiler)
    except EmptyResultSet, UncachableQuery:
        return None
    return (query_key, tables) if tables else None


def _read(compiler: Any, result_type: Any, execute: Callable[[], Any]) -> Any:
    connection = compiler.connection
    store = get_store(orm_settings.CACHE)
    key_and_tables = _key_and_tables(compiler, result_type, store)
    if store is None or key_and_tables is None:
        return execute()
    query_key, tables = key_and_tables
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
            store.store(db_alias, query_key, table_keys, lookup.token, result, orm_settings.TIMEOUT)
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
            if _cachable_call(compiler, result_type):
                return _read(compiler, result_type, execute)
            return execute()
        finally:
            setattr(connection, _COMPILING, was_compiling)

    return execute_sql


def _leased(connection: Any, tables: set[str], run: Callable[[], Any]) -> Any:
    """Run ``run``, which writes to ``tables`` and commits, under a lease on them."""
    store = get_store(orm_settings.CACHE)
    if store is None:
        return run()
    db_alias = connection.alias
    table_keys = _table_keys(db_alias, tables)
    token = uuid.uuid4().hex
    try:
        store.begin_write(db_alias, table_keys, token, orm_settings.LEASE_TIMEOUT)
    except Exception as e:  # noqa: BLE001
        _invalidation_failed(e, db_alias, tables)
        return run()
    try:
        return run()
    finally:
        try:
            store.end_write(db_alias, table_keys, token)
        except Exception:
            logger.warning(
                "Could not release the ORM cache lease on %s of database %r; their queries run against the "
                "database until it expires.",
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
        result = _leased(connection, tables, run)
    finally:
        setattr(connection, _WRITING, active)
    _send_signals(connection.alias, tables)
    return result


def _compiler_tables(compiler: Any) -> set[str]:
    meta = compiler.query.get_meta()
    tables = {meta.db_table}
    if isinstance(compiler, SQLDeleteCompiler):
        tables |= deletion_dependents([meta.model])
    return filter_cachable(tables)


def _patch_write(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def inner(compiler: Any, *args: Any, **kwargs: Any) -> Any:
        connection = compiler.connection
        was_compiling = getattr(connection, _COMPILING, False)
        setattr(connection, _COMPILING, True)
        try:
            return _write(connection, _compiler_tables(compiler), lambda: original(compiler, *args, **kwargs))
        finally:
            setattr(connection, _COMPILING, was_compiling)

    return inner


def _raw_tables(connection: Any, lowered_sql: str) -> set[str]:
    """Tables raw SQL may write to, or change through their foreign keys."""
    if not SQL_DATA_CHANGE_RE.search(lowered_sql):
        return set()
    tables = _get_tables_from_sql(connection, lowered_sql)
    if tables:
        truncate = bool(_TRUNCATE_CASCADE_RE.search(lowered_sql))
        if truncate or "delete" in lowered_sql:
            tables |= deletion_dependents(models_of_tables(tables), truncate=truncate)
    return filter_cachable(tables)


def _patch_cursor(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def inner(cursor: Any, sql: Any, *args: Any, **kwargs: Any) -> Any:
        connection = cursor.db
        if getattr(connection, _COMPILING, False) or connection.alias not in orm_settings.DATABASES:
            return original(cursor, sql, *args, **kwargs)
        lowered = (sql.decode(errors="replace") if isinstance(sql, bytes) else str(sql)).lower()
        tables = _raw_tables(connection, lowered) if orm_settings.INVALIDATE_RAW else set()
        result = _write(connection, tables, lambda: original(cursor, sql, *args, **kwargs))
        if "isolation" in lowered or "journal_mode" in lowered:
            transaction.isolation_changed(connection, known=bool(_SESSION_ISOLATION_RE.search(lowered)))
        return result

    return inner


def _patch_commit(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def commit(connection: Any) -> None:
        tables = transaction.written(connection)
        if not tables:
            original(connection)
            transaction.reset(connection)
            return
        _leased(connection, tables, lambda: original(connection))
        transaction.reset(connection)
        _send_signals(connection.alias, tables)

    return commit


def _patch_set_autocommit(original: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(original)
    def set_autocommit(connection: Any, autocommit: bool, *args: Any, **kwargs: Any) -> None:
        if not autocommit:
            # A new transaction begins.
            transaction.reset(connection)
            original(connection, autocommit, *args, **kwargs)
            return
        tables = transaction.written(connection)
        if not tables:
            original(connection, autocommit, *args, **kwargs)
            transaction.reset(connection)
            return
        # SQLite commits a pending transaction when autocommit is turned on.
        _leased(connection, tables, lambda: original(connection, autocommit, *args, **kwargs))
        transaction.reset(connection)
        _send_signals(connection.alias, tables)

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


def _invalidate_on_migration(sender: Any, *, using: str, **kwargs: Any) -> None:  # noqa: ARG001
    models = list(sender.get_models())
    if models:
        invalidate(*models, db_alias=using, cache_alias=orm_settings.CACHE)


_MISSING = object()
# (class, attribute) -> what the class itself defined before patching.
_ORIGINALS: dict[tuple[type, str], Any] = {}


def _replace(cls: type, name: str, patched: Any) -> None:
    _ORIGINALS[cls, name] = cls.__dict__.get(name, _MISSING)
    setattr(cls, name, patched)


def patch() -> None:
    """Patch Django to cache query results; a no-op if already patched."""
    if _ORIGINALS:
        return
    read = SQLCompiler.execute_sql
    _replace(SQLCompiler, "execute_sql", _patch_read(read))
    # SQLDeleteCompiler inherits execute_sql from SQLCompiler.
    _replace(SQLDeleteCompiler, "execute_sql", _patch_write(read))
    _replace(SQLInsertCompiler, "execute_sql", _patch_write(SQLInsertCompiler.execute_sql))
    _replace(SQLUpdateCompiler, "execute_sql", _patch_write(SQLUpdateCompiler.execute_sql))
    if "execute_returning_sql" in SQLUpdateCompiler.__dict__:  # Django 6.1+
        _replace(
            SQLUpdateCompiler,
            "execute_returning_sql",
            _patch_write(SQLUpdateCompiler.execute_returning_sql),
        )
    _replace(CursorWrapper, "execute", _patch_cursor(CursorWrapper.execute))
    _replace(CursorWrapper, "executemany", _patch_cursor(CursorWrapper.executemany))
    for name, patcher in (
        ("commit", _patch_commit),
        ("set_autocommit", _patch_set_autocommit),
        ("rollback", _patch_rollback),
        ("close", _patch_close),
        ("connect", _patch_connect),
        ("savepoint", _patch_savepoint),
        ("savepoint_rollback", _patch_savepoint_rollback),
        ("savepoint_commit", _patch_savepoint_commit),
    ):
        _replace(BaseDatabaseWrapper, name, patcher(getattr(BaseDatabaseWrapper, name)))
    post_migrate.connect(_invalidate_on_migration, dispatch_uid="django_cachex.orm.invalidate_on_migration")


def unpatch() -> None:
    """Undo patch()."""
    post_migrate.disconnect(dispatch_uid="django_cachex.orm.invalidate_on_migration")
    for (cls, name), original in reversed(list(_ORIGINALS.items())):
        if original is _MISSING:
            delattr(cls, name)
        else:
            setattr(cls, name, original)
    _ORIGINALS.clear()
