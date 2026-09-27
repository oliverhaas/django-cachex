"""Public API of the ORM cache."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any

from asgiref.local import Local
from django.apps import apps
from django.conf import settings
from django.db import connections

from django_cachex.orm.cache import orm_caches
from django_cachex.orm.settings import orm_settings
from django_cachex.orm.signals import post_invalidation
from django_cachex.orm.transaction import AtomicCache
from django_cachex.orm.utils import _invalidate_tables

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

LOCAL_STORAGE = Local()


__all__ = ("get_last_invalidation", "invalidate", "orm_cache_disabled")


def _cache_db_tables_iterator(
    tables: list[str],
    cache_alias: str | None,
    db_alias: str | None,
) -> Iterator[tuple[str, str, list[str]]]:
    no_tables = not tables
    cache_aliases = settings.CACHES if cache_alias is None else (cache_alias,)
    db_aliases = settings.DATABASES if db_alias is None else (db_alias,)
    for db_alias_ in db_aliases:
        if no_tables:
            tables = connections[db_alias_].introspection.table_names()
        if tables:
            for cache_alias_ in cache_aliases:
                yield cache_alias_, db_alias_, tables


def _get_tables(tables_or_models: Iterable[Any]) -> Iterator[str]:
    for table_or_model in tables_or_models:
        if isinstance(table_or_model, str) and "." in table_or_model:
            with suppress(LookupError):
                table_or_model = apps.get_model(table_or_model)  # noqa: PLW2901
        yield (table_or_model if isinstance(table_or_model, str) else table_or_model._meta.db_table)


def invalidate(
    *tables_or_models: Any,
    cache_alias: str | None = None,
    db_alias: str | None = None,
) -> None:
    """Invalidate the cached queries of the given tables or models (all tables when none are given)."""
    send_signal = False
    invalidated: set[str] = set()
    for cache_alias_, db_alias_, tables in _cache_db_tables_iterator(
        list(_get_tables(tables_or_models)),
        cache_alias,
        db_alias,
    ):
        cache = orm_caches.get_cache(cache_alias_, db_alias_)
        if not isinstance(cache, AtomicCache):
            send_signal = True
        _invalidate_tables(cache, db_alias_, tables)
        invalidated.update(tables)

    if send_signal:
        for table in invalidated:
            post_invalidation.send(table, db_alias=db_alias)


def get_last_invalidation(
    *tables_or_models: Any,
    cache_alias: str | None = None,
    db_alias: str | None = None,
) -> float:
    """Return the timestamp of the most recent invalidation of the given tables or models."""
    last_invalidation = 0.0
    for cache_alias_, db_alias_, tables in _cache_db_tables_iterator(
        list(_get_tables(tables_or_models)),
        cache_alias,
        db_alias,
    ):
        get_table_cache_key = orm_settings.TABLE_KEYGEN
        table_cache_keys = [get_table_cache_key(db_alias_, t) for t in tables]
        invalidations = orm_caches.get_cache(cache_alias_, db_alias_).get_many(table_cache_keys).values()
        if invalidations:
            current_last_invalidation = max(invalidations)
            last_invalidation = max(last_invalidation, current_last_invalidation)
    return last_invalidation


@contextmanager
def orm_cache_disabled(all_queries: bool = False) -> Iterator[None]:
    """Run the queries of the block against the database, bypassing the ORM cache."""
    was_enabled = getattr(LOCAL_STORAGE, "orm_cache_enabled", orm_settings.ENABLED)
    LOCAL_STORAGE.orm_cache_enabled = False
    LOCAL_STORAGE.disable_on_all = all_queries
    yield
    LOCAL_STORAGE.orm_cache_enabled = was_enabled
