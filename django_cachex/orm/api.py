"""Public API of the ORM cache."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import logging
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any

from asgiref.local import Local
from django.apps import apps
from django.conf import settings
from django.db import DEFAULT_DB_ALIAS, connections

from django_cachex.orm import transaction
from django_cachex.orm.exceptions import InvalidationError
from django_cachex.orm.settings import orm_settings
from django_cachex.orm.signals import post_invalidation
from django_cachex.orm.store import get_store
from django_cachex.orm.utils import are_all_cachable, filter_cachable

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

logger = logging.getLogger("django_cachex.orm")

LOCAL_STORAGE = Local()


__all__ = ("invalidate", "orm_cache_disabled", "table_generations")


def _table_names(tables_or_models: Iterable[Any]) -> Iterator[str]:
    for table_or_model in tables_or_models:
        if isinstance(table_or_model, str) and "." in table_or_model:
            with suppress(LookupError):
                table_or_model = apps.get_model(table_or_model)  # noqa: PLW2901
        yield (table_or_model if isinstance(table_or_model, str) else table_or_model._meta.db_table)


def _table_keys(db_alias: str, tables: Iterable[str]) -> list[str]:
    keygen = orm_settings.TABLE_KEYGEN
    return [keygen(db_alias, table) for table in sorted(tables)]


def _send_signals(db_alias: str, tables: Iterable[str]) -> None:
    # The write is done by now, so a receiver's error must not look like its
    # failure: send_robust() logs the error to django.dispatch instead.
    for table in sorted(tables):
        post_invalidation.send_robust(table, db_alias=db_alias)


def _invalidation_failed(error: Exception, db_alias: str, tables: Iterable[str]) -> None:
    message = f"Could not invalidate the ORM cache of {', '.join(sorted(tables))} in database {db_alias!r}"
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
    signals: list[tuple[str, set[str]]] = []
    for db in db_aliases:
        db_tables = filter_cachable(tables or set(connections[db].introspection.table_names()))
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
                _invalidation_failed(e, db, db_tables)
        connection = connections[db]
        if db in orm_settings.DATABASES and transaction.in_transaction(connection):
            # The transaction may still write to the tables: its commit bumps
            # them again and sends the signals.
            transaction.mark_written(connection, db_tables)
        else:
            signals.append((db, db_tables))
    for db, db_tables in signals:
        _send_signals(db, db_tables)


def table_generations(*tables_or_models: Any, db_alias: str = DEFAULT_DB_ALIAS) -> tuple[str, ...] | None:
    """Return the current generations of the given tables or models, or None if a result read now must not be cached."""
    # Every committed write to a table changes its generation, so a value
    # computed from the tables can be cached under their generations. None
    # while a write to one of them runs, and whenever the ORM cache itself
    # would not cache a query of them.
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
    if transaction.in_transaction(connection) and (
        transaction.isolation(connection) == transaction.SNAPSHOT
        or not transaction.written(connection).isdisjoint(tables)
    ):
        return None
    store = get_store(orm_settings.CACHE)
    if store is None:
        return None
    keygen = orm_settings.TABLE_KEYGEN
    try:
        generations = store.generations(db_alias, [keygen(db_alias, table) for table in tables])
    except Exception:
        logger.warning("Could not read table generations from the ORM cache.", exc_info=True)
        return None
    return None if generations is None else tuple(generations)


@contextmanager
def orm_cache_disabled() -> Iterator[None]:
    """Run the queries of the block against the database; its writes still invalidate the cache."""
    was_enabled = getattr(LOCAL_STORAGE, "orm_cache_enabled", True)
    LOCAL_STORAGE.orm_cache_enabled = False
    try:
        yield
    finally:
        LOCAL_STORAGE.orm_cache_enabled = was_enabled
