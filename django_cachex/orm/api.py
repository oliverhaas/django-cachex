"""Public API of the ORM cache."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import functools
import hashlib
import inspect
import logging
import threading
import warnings
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from types import CodeType
from typing import TYPE_CHECKING, Any, Protocol

from asgiref.local import Local
from django.apps import apps
from django.conf import settings
from django.db import DEFAULT_DB_ALIAS, DatabaseError, connections
from django.db.models import Model, QuerySet

from django_cachex.immutable import can_keep
from django_cachex.orm import transaction
from django_cachex.orm.exceptions import InvalidationError, UndeclaredTableWarning
from django_cachex.orm.settings import SETTING_NAME, orm_settings
from django_cachex.orm.store import BYPASS, DATA, get_store
from django_cachex.orm.utils import are_all_cachable, filter_cachable, known_tables

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable, Iterator

logger = logging.getLogger(__name__)

LOCAL_STORAGE = Local()


class Recording:
    """The tables a ``cached_data()`` function reads through the ORM while it runs."""

    __slots__ = ("reads", "uncachable")

    def __init__(self) -> None:
        self.reads: set[tuple[str, str]] = set()
        self.uncachable: set[str] = set()


# The recording of the cached_data() function running in this context, which _read reports to.
RECORDING: ContextVar[Recording | None] = ContextVar("cachex_orm_recording", default=None)


__all__ = ("cached_data", "invalidate", "orm_cache_disabled", "table_generations")


def _table_names(tables_or_models: Iterable[Any]) -> Iterator[str]:
    for table_or_model in tables_or_models:
        if isinstance(table_or_model, str) and "." in table_or_model:
            with suppress(LookupError):
                table_or_model = apps.get_model(table_or_model)  # noqa: PLW2901
        yield (table_or_model if isinstance(table_or_model, str) else table_or_model._meta.db_table)


def _table_key(db_alias: str, table: str) -> str:
    table_key = orm_settings.TABLE_KEYGEN(db_alias=db_alias, table=table)
    if not isinstance(table_key, str):
        msg = f"`{SETTING_NAME}['TABLE_KEYGEN']` must return a str, not {table_key!r}."
        raise TypeError(msg)
    return table_key


def _table_keys(db_alias: str, tables: Iterable[str]) -> list[str]:
    # Without duplicates when tables share a key.
    return sorted({_table_key(db_alias, table) for table in tables})


def _all_tables(db_alias: str) -> set[str]:
    try:
        return known_tables().union(connections[db_alias].introspection.table_names())
    except DatabaseError:
        # An unreachable database must not keep the other aliases from being invalidated.
        logger.warning(
            "Could not list the tables of database %r; invalidating its model tables only.",
            db_alias,
            exc_info=True,
        )
        return known_tables()


def _invalidation_failed(error: Exception, message: str) -> None:
    if orm_settings.ENABLED:
        raise InvalidationError(message) from error
    # Nothing is served from the cache while it is disabled; the docs say to
    # invalidate it before enabling it again.
    logger.warning("%s.", message, exc_info=error)


def invalidate(
    *tables_or_models: Any,
    cache_alias: str | None = None,
    db_alias: str | None = None,
) -> None:
    """Invalidate the cached queries of the given tables or models (all tables when none are given)."""
    # Without cache_alias every cache that can hold the ORM cache is
    # invalidated, without db_alias every database.
    tables = set(_table_names(tables_or_models))
    cache_aliases = list(settings.CACHES) if cache_alias is None else [cache_alias]
    db_aliases = list(settings.DATABASES) if db_alias is None else [db_alias]
    # A failing cache does not keep the others from being invalidated.
    failed_dbs: dict[str, list[str]] = {}
    error: Exception | None = None
    for db in db_aliases:
        db_tables = filter_cachable(tables or _all_tables(db))
        if not db_tables:
            continue
        table_keys = _table_keys(db, db_tables)
        for alias in cache_aliases:
            store = get_store(alias)
            if store is None:
                continue
            try:
                store.bump(db, table_keys)
            except Exception as e:  # noqa: BLE001
                failed_dbs.setdefault(alias, []).append(db)
                error = error or e
        connection = connections[db]
        if db in orm_settings.DATABASES and transaction.in_transaction(connection):
            # The transaction can still write to the tables: its commit bumps them again.
            transaction.mark_written(connection, db_tables)
    if error is not None:
        where = "; ".join(
            f"in cache {alias!r} for database{'' if len(dbs) == 1 else 's'} {', '.join(map(repr, dbs))}"
            for alias, dbs in failed_dbs.items()
        )
        _invalidation_failed(error, f"Could not invalidate the ORM cache {where}")


def table_generations(*tables_or_models: Any, db_alias: str = DEFAULT_DB_ALIAS) -> tuple[str, ...] | None:
    """Return the current generations of the given tables or models, or None if a result read now must not be cached.

    Every committed write to a table changes its generation, so a value computed from the tables can be cached
    under their generations.
    """
    tables = list(_table_names(tables_or_models))
    if not tables:
        msg = "table_generations() needs at least one table or model."
        raise TypeError(msg)
    if (
        not orm_settings.ENABLED
        or not getattr(LOCAL_STORAGE, "orm_cache_enabled", True)
        or db_alias not in orm_settings.DATABASES
        or not are_all_cachable(set(tables))
    ):
        return None
    connection = connections[db_alias]
    if transaction.in_transaction(connection) and not transaction.written(connection).isdisjoint(tables):
        return None
    store = get_store(orm_settings.CACHE)
    if store is None:
        return None
    # Outside the try, so a TABLE_KEYGEN error propagates instead of reading as a cache outage.
    table_keys = [_table_key(db_alias, table) for table in tables]
    try:
        generations = store.generations(db_alias, table_keys)
    except Exception:
        logger.warning("Could not read table generations from the ORM cache.", exc_info=True)
        return None
    return None if generations is None else tuple(generations)


@dataclass(frozen=True, slots=True)
class _Declared:
    """The declared tables of a ``cached_data()`` function under one revision of the settings."""

    revision: int
    tables: frozenset[str]
    # Sorted, so every process reads the generations in one order. None when the settings keep every result from
    # being cached.
    table_keys: tuple[str, ...] | None = None


def _code_parts(value: Any) -> Iterator[bytes]:
    if isinstance(value, CodeType):
        yield value.co_code
        yield repr(value.co_names).encode()
        yield from _code_parts(value.co_consts)
    elif isinstance(value, tuple):
        for item in value:
            yield from _code_parts(item)
    elif isinstance(value, frozenset):
        # Iterated in the order of their hashes, which differs between processes.
        yield repr(sorted(map(repr, value))).encode()
    else:
        yield repr(value).encode()


def _code_hash(func: Callable[..., Any], version: int | str) -> str:
    """A digest of the code, defaults and closure of ``func`` and of ``version``, the same in every process."""
    func = inspect.unwrap(func)
    closure = []
    for cell in getattr(func, "__closure__", None) or ():
        # A cell is empty while the decorated function refers to itself before its name is bound.
        with suppress(ValueError):
            closure.append(cell.cell_contents)
    digest = hashlib.blake2b(digest_size=4)
    for part in _code_parts(
        (
            getattr(func, "__code__", None),
            getattr(func, "__defaults__", None),
            getattr(func, "__kwdefaults__", None),
            tuple(closure),
            version,
        ),
    ):
        digest.update(part)
    return digest.hexdigest()


class _CachedData:
    """A ``cached_data()`` function: its results are stored in the ORM cache under the generations of its tables."""

    def __init__(
        self,
        func: Callable[..., Any],
        name: str,
        declared: tuple[Any, ...],
        db_alias: str,
        version: int | str,
    ) -> None:
        self.func = func
        self.name = name
        self.declared = declared
        self.db_alias = db_alias
        self.lock = threading.Lock()
        self.state: _Declared | None = None
        module = getattr(func, "__module__", None)
        self.key_prefix = f"{module}.{name}:{_code_hash(func, version)}:"

    def _resolve(self) -> _Declared:
        # Read first: settings that change while this runs make the next call resolve again.
        revision = orm_settings.revision
        tables = set()
        known = known_tables()
        for declared, table in zip(self.declared, _table_names(self.declared), strict=True):
            if table not in known:
                msg = (
                    f"@cached_data() of {self.name}() declares {declared!r}, which is neither an installed model "
                    f"nor a table in `{SETTING_NAME}['ADDITIONAL_TABLES']`."
                )
                raise LookupError(msg)
            tables.add(table)
        if orm_settings.ENABLED and self.db_alias in orm_settings.DATABASES and are_all_cachable(tables):
            store = get_store(orm_settings.CACHE)
            ttl = None if store is None else store.ttl(orm_settings.TIMEOUT)
            if store is not None and (ttl is None or ttl > 0):
                return _Declared(revision, frozenset(tables), tuple(_table_keys(self.db_alias, tables)))
        return _Declared(revision, frozenset(tables))

    def _current(self) -> _Declared:
        state = self.state
        if state is not None and state.revision == orm_settings.revision:
            return state
        with self.lock:
            state = self.state
            if state is None or state.revision != orm_settings.revision:
                state = self._resolve()
                self.state = state
        return state

    def _undeclared(self, recording: Recording, state: _Declared, table_keys: tuple[str, ...]) -> list[str]:
        found = []
        for db_alias, table in sorted(recording.reads):
            if db_alias != self.db_alias:
                found.append(f"{table} on database {db_alias!r}")
            elif table not in state.tables and _table_key(db_alias, table) not in table_keys:
                found.append(table)
        found.extend(f"{table}, which the ORM cache does not cache" for table in sorted(recording.uncachable))
        return found

    def _key(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        if not can_keep((*args, *kwargs.values()) if kwargs else args):
            msg = (
                f"{self.name}() got an argument a caller can change, such as a list or a model instance, but "
                "@cached_data() keys results by their arguments."
            )
            raise TypeError(msg)
        # Keyword arguments by name, so a call gets one key in any order of them.
        arguments = ", ".join([*map(repr, args), *(f"{name}={kwargs[name]!r}" for name in sorted(kwargs))])
        return f"{self.key_prefix}({arguments})"

    def call(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        key = self._key(args, kwargs)
        state = self._current()
        table_keys = state.table_keys
        store = None if table_keys is None else get_store(orm_settings.CACHE)
        if (
            table_keys is None
            or store is None
            or not getattr(LOCAL_STORAGE, "orm_cache_enabled", True)
            or self._wrote(table_keys)
        ):
            return self.func(*args, **kwargs)
        if (outer := RECORDING.get()) is not None:
            # An outer function caching its result would otherwise miss writes to these tables.
            outer.reads.update((self.db_alias, table) for table in state.tables)
        try:
            found = store.lookup(self.db_alias, key, table_keys, kind=DATA)
        except Exception:
            logger.warning("Could not read a cached_data() result from the ORM cache.", exc_info=True)
            found = BYPASS
        if found.hit:
            return found.value
        recording = Recording()
        token = RECORDING.set(recording)
        try:
            result = self.func(*args, **kwargs)
        finally:
            RECORDING.reset(token)
            if outer is not None:
                # The outer result depends on every table this function read, declared or not.
                outer.reads |= recording.reads
                outer.uncachable |= recording.uncachable
        if isinstance(result, QuerySet):
            msg = (
                f"{self.name}() returned a QuerySet, which reads its tables after @cached_data() checked them; "
                "return a list or tuple of its rows instead."
            )
            raise TypeError(msg)
        if undeclared := self._undeclared(recording, state, table_keys):
            warnings.warn(
                f"{self.name}() read tables its @cached_data() does not declare, so its result is not cached: "
                f"{', '.join(undeclared)}.",
                UndeclaredTableWarning,
                # At the caller of the decorated function.
                stacklevel=3,
            )
            return result
        if found.token is None or self._wrote(table_keys):
            # Not stored without generations. A function that wrote a declared table can return rows that the
            # transaction rolls back.
            return result
        try:
            store.store(self.db_alias, key, found.token, result, orm_settings.TIMEOUT, kind=DATA)
        except Exception:
            logger.warning("Could not store a cached_data() result in the ORM cache.", exc_info=True)
        return result

    def _wrote(self, table_keys: tuple[str, ...]) -> bool:
        """Return whether the transaction of this thread wrote a declared table, or one that shares its key."""
        connection = connections[self.db_alias]
        return transaction.in_transaction(connection) and not set(table_keys).isdisjoint(
            _table_keys(self.db_alias, transaction.written(connection)),
        )


class _Decorator(Protocol):
    def __call__[**P, R](self, func: Callable[P, R]) -> Callable[P, R]: ...


def cached_data(*tables_or_models: Any, db_alias: str = DEFAULT_DB_ALIAS, version: int | str = 0) -> _Decorator:
    """Cache the results of the decorated function in the ORM cache until a write changes a declared table.

    Results are keyed by the function's module, name, code and ``version``, and by its arguments, which no caller
    can change. The function must compute its result from its arguments and the declared tables alone.
    """
    if not tables_or_models:
        msg = "cached_data() needs at least one table or model."
        raise TypeError(msg)
    for table_or_model in tables_or_models:
        if not isinstance(table_or_model, str) and not (
            isinstance(table_or_model, type) and issubclass(table_or_model, Model)
        ):
            msg = f"cached_data() takes models and table names, not {table_or_model!r}."
            raise TypeError(msg)
    if isinstance(version, bool) or not isinstance(version, int | str):
        msg = f"cached_data() needs an int or str version, not {version!r}."
        raise TypeError(msg)

    def decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        name = getattr(func, "__qualname__", repr(func))
        if inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func):
            msg = f"cached_data() cannot cache the results of async {name}(); call a sync function through "
            msg += "sync_to_async() instead."
            raise TypeError(msg)
        cached = _CachedData(func, name, tables_or_models, db_alias, version)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return cached.call(args, kwargs)

        return wrapper

    return decorate


@contextmanager
def orm_cache_disabled() -> Generator[None]:
    """Run the queries of the block against the database; its writes still invalidate the cache."""
    was_enabled = getattr(LOCAL_STORAGE, "orm_cache_enabled", True)
    LOCAL_STORAGE.orm_cache_enabled = False
    try:
        yield
    finally:
        LOCAL_STORAGE.orm_cache_enabled = was_enabled
