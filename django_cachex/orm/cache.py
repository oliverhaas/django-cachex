"""Per-thread access to the cache the ORM cache stores into."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

from collections import defaultdict
from threading import local
from typing import Any

from django.core.cache import caches
from django.db import DEFAULT_DB_ALIAS

from django_cachex.orm.settings import orm_settings
from django_cachex.orm.signals import post_invalidation
from django_cachex.orm.transaction import AtomicCache


class CacheHandler(local):
    @property
    def atomic_caches(self) -> defaultdict[str, list[dict[str, AtomicCache]]]:
        if not hasattr(self, "_atomic_caches"):
            self._atomic_caches: defaultdict[str, list[dict[str, AtomicCache]]] = defaultdict(list)
        return self._atomic_caches

    def get_atomic_cache(self, cache_alias: str, db_alias: str, level: int) -> AtomicCache:
        if cache_alias not in self.atomic_caches[db_alias][level]:
            self.atomic_caches[db_alias][level][cache_alias] = AtomicCache(
                self.get_cache(cache_alias, db_alias, level - 1),
                db_alias,
            )
        return self.atomic_caches[db_alias][level][cache_alias]

    def get_cache(self, cache_alias: str | None = None, db_alias: str | None = None, atomic_level: int = -1) -> Any:
        if db_alias is None:
            db_alias = DEFAULT_DB_ALIAS
        if cache_alias is None:
            cache_alias = orm_settings.CACHE

        min_level = -len(self.atomic_caches[db_alias])
        if atomic_level < min_level:
            return caches[cache_alias]
        return self.get_atomic_cache(cache_alias, db_alias, atomic_level)

    def enter_atomic(self, db_alias: str | None) -> None:
        if db_alias is None:
            db_alias = DEFAULT_DB_ALIAS
        self.atomic_caches[db_alias].append({})

    def exit_atomic(self, db_alias: str | None, commit: bool) -> None:
        if db_alias is None:
            db_alias = DEFAULT_DB_ALIAS
        atomic_caches = self.atomic_caches[db_alias].pop().values()
        if commit:
            to_be_invalidated: set[str] = set()
            for atomic_cache in atomic_caches:
                atomic_cache.commit()
                to_be_invalidated.update(atomic_cache.to_be_invalidated)
            # Only when the outermost atomic block commits.
            if not self.atomic_caches[db_alias]:
                for table in to_be_invalidated:
                    post_invalidation.send(table, db_alias=db_alias)


orm_caches = CacheHandler()
