"""Public API of the ORM cache."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import functools
import inspect
import logging
import threading
import warnings
from collections import OrderedDict
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, overload

from asgiref.local import Local
from django.apps import apps
from django.conf import settings
from django.db import DEFAULT_DB_ALIAS, DatabaseError, connections
from django.db.models import Model

from django_cachex.orm import transaction
from django_cachex.orm.exceptions import InvalidationError, UndeclaredTableWarning
from django_cachex.orm.freeze import freeze
from django_cachex.orm.settings import SETTING_NAME, orm_settings
from django_cachex.orm.store import get_store
from django_cachex.orm.utils import are_all_cachable, filter_cachable, known_tables

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Hashable, Iterable, Iterator
    from types import MappingProxyType

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
    table_keys: frozenset[str] = frozenset()
    # None when the settings keep every result from being held.
    generation_keys: list[Any] | None = None


class _CachedData:
    """The results of one ``cached_data()`` function, held under the generations of its tables."""

    def __init__(
        self,
        func: Callable[..., Any],
        name: str,
        declared: tuple[Any, ...],
        db_alias: str,
        maxsize: int,
    ) -> None:
        self.func = func
        self.name = name
        self.declared = declared
        self.db_alias = db_alias
        self.maxsize = maxsize
        self.lock = threading.Lock()
        self.entries: OrderedDict[Hashable, tuple[list[str], Any]] = OrderedDict()
        self.state: _Declared | None = None

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
                table_keys = _table_keys(self.db_alias, tables)
                return _Declared(
                    revision,
                    frozenset(tables),
                    frozenset(table_keys),
                    store.generation_keys(self.db_alias, table_keys),
                )
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
                self.entries.clear()
        return state

    def _undeclared(self, recording: Recording, state: _Declared) -> list[str]:
        found = []
        for db_alias, table in sorted(recording.reads):
            if db_alias != self.db_alias:
                found.append(f"{table} on database {db_alias!r}")
            elif table not in state.tables and _table_key(db_alias, table) not in state.table_keys:
                found.append(table)
        found.extend(f"{table}, which the ORM cache does not cache" for table in sorted(recording.uncachable))
        return found

    def call(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        key = (args, tuple(sorted(kwargs.items())) if kwargs else ())
        try:
            hash(key)
        except TypeError as e:
            msg = f"{self.name}() got an unhashable argument, but @cached_data() holds results by their arguments."
            raise TypeError(msg) from e
        state = self._current()
        generations = self._generations(state)
        if generations is None:
            return freeze(self.func(*args, **kwargs), self.name)
        if (outer := RECORDING.get()) is not None:
            # An outer function holding its result would otherwise miss writes to these tables.
            outer.reads.update((self.db_alias, table) for table in state.tables)
        with self.lock:
            held = self.entries.get(key)
            if held is not None and held[0] == generations:
                self.entries.move_to_end(key)
                return held[1]
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
        result = freeze(result, self.name)
        if undeclared := self._undeclared(recording, state):
            warnings.warn(
                f"{self.name}() read tables its @cached_data() does not declare, so its result is not held: "
                f"{', '.join(undeclared)}.",
                UndeclaredTableWarning,
                # At the caller of the decorated function.
                stacklevel=3,
            )
            return result
        if self._wrote(state):
            # The function wrote a declared table, so its result can hold rows the transaction has not committed.
            return result
        with self.lock:
            # Not held under generations read with the keys of other settings.
            if self.state is state:
                self.entries[key] = (generations, result)
                self.entries.move_to_end(key)
                while len(self.entries) > self.maxsize:
                    self.entries.popitem(last=False)
        return result

    def _generations(self, state: _Declared) -> list[str] | None:
        """Return the generations of the declared tables, or None if the result must not be held."""
        if state.generation_keys is None or not getattr(LOCAL_STORAGE, "orm_cache_enabled", True) or self._wrote(state):
            return None
        store = get_store(orm_settings.CACHE)
        if store is None:
            return None
        try:
            return store.read_generations(state.generation_keys)
        except Exception:
            logger.warning("Could not read table generations from the ORM cache.", exc_info=True)
            return None

    def _wrote(self, state: _Declared) -> bool:
        """Return whether the transaction of this thread wrote a declared table, or one that shares its key."""
        connection = connections[self.db_alias]
        return transaction.in_transaction(connection) and not state.table_keys.isdisjoint(
            _table_keys(self.db_alias, transaction.written(connection)),
        )


class _Decorator(Protocol):
    @overload
    def __call__[**P, K, V](self, func: Callable[P, dict[K, V]]) -> Callable[P, MappingProxyType[K, V]]: ...

    @overload
    def __call__[**P, R](self, func: Callable[P, R]) -> Callable[P, R]: ...


def cached_data(*tables_or_models: Any, db_alias: str = DEFAULT_DB_ALIAS, maxsize: int = 1024) -> _Decorator:
    """Hold the results of the decorated function in this process until a declared table changes.

    The function must compute its result from its arguments and the declared tables alone. Each result is
    frozen: a returned dict becomes a read-only mapping, and lists, sets, dicts, model instances and QuerySets
    inside it raise TypeError. Results are held per arguments, at most ``maxsize`` of them.
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
    if isinstance(maxsize, bool) or not isinstance(maxsize, int):
        msg = f"cached_data() needs an int maxsize, not {maxsize!r}."
        raise TypeError(msg)
    if maxsize < 1:
        msg = f"cached_data() needs a maxsize of at least 1, not {maxsize}."
        raise ValueError(msg)

    def decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        name = getattr(func, "__qualname__", repr(func))
        if inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func):
            msg = f"cached_data() cannot hold the results of async {name}(); call a sync function through "
            msg += "sync_to_async() instead."
            raise TypeError(msg)
        held = _CachedData(func, name, tables_or_models, db_alias, maxsize)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return held.call(args, kwargs)

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
