"""Per-atomic-block cache layer."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

from typing import Any

from django_cachex.orm.settings import orm_settings


class AtomicCache(dict[str, Any]):
    def __init__(self, parent_cache: Any, db_alias: str) -> None:
        super().__init__()
        self.parent_cache = parent_cache
        self.db_alias = db_alias
        self.to_be_invalidated: set[str] = set()

    def set(self, k: str, v: Any, timeout: Any) -> None:
        self[k] = v

    def get_many(self, keys: Any) -> dict[str, Any]:
        # Values present at this level.
        data = {k: self[k] for k in keys if k in self}

        missing_keys = set(keys)
        missing_keys.difference_update(data)

        if missing_keys:
            # Walk down to the first non-AtomicCache without recursing.
            current_cache = self.parent_cache
            visited_caches = {id(self)}

            while isinstance(current_cache, AtomicCache) and id(current_cache) not in visited_caches:
                visited_caches.add(id(current_cache))

                available_keys = missing_keys.intersection(current_cache.keys())
                if available_keys:
                    for k in available_keys:
                        data[k] = current_cache[k]
                    missing_keys.difference_update(available_keys)

                if not missing_keys:
                    break

                current_cache = current_cache.parent_cache

            if missing_keys and not isinstance(current_cache, AtomicCache) and hasattr(current_cache, "get_many"):
                parent_data = current_cache.get_many(missing_keys)
                data.update(parent_data)

        return data

    def set_many(self, data: dict[str, Any], timeout: Any) -> None:
        self.update(data)

    def commit(self) -> None:
        # Imported here to avoid a circular import.
        from django_cachex.orm.utils import _invalidate_tables

        if self:
            self.parent_cache.set_many(self, orm_settings.TIMEOUT)
        # The set_many above is not enough: another transaction may have
        # written in the meantime, so the parent is invalidated too.
        _invalidate_tables(self.parent_cache, self.db_alias, self.to_be_invalidated)
