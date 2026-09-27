"""Settings of the ORM cache, read from the ``CACHEX_ORM`` dict."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

from itertools import chain
from typing import TYPE_CHECKING, Any

from django.apps import apps
from django.conf import settings
from django.utils.module_loading import import_string

if TYPE_CHECKING:
    from collections.abc import Callable

SETTING_NAME = "CACHEX_ORM"

SUPPORTED_DATABASE_ENGINES = {
    "django.db.backends.sqlite3",
    "django.db.backends.postgresql",
    "django.db.backends.mysql",
    # GeoDjango
    "django.contrib.gis.db.backends.spatialite",
    "django.contrib.gis.db.backends.postgis",
    "django.contrib.gis.db.backends.mysql",
    # django-transaction-hooks
    "transaction_hooks.backends.sqlite3",
    "transaction_hooks.backends.postgis",
    "transaction_hooks.backends.mysql",
    # django-prometheus wrapped engines
    "django_prometheus.db.backends.sqlite3",
    "django_prometheus.db.backends.postgresql",
    "django_prometheus.db.backends.mysql",
}

SUPPORTED_ONLY = "supported_only"
ITERABLES = {tuple, list, frozenset, set}

DEFAULTS: dict[str, Any] = {
    "ENABLED": True,
    "CACHE": "default",
    "DATABASES": SUPPORTED_ONLY,
    "USE_UNSUPPORTED_DATABASE": False,
    "ADDITIONAL_SUPPORTED_DATABASES": (),
    "TIMEOUT": None,
    "CACHE_RANDOM": False,
    "CACHE_ITERATORS": True,
    "INVALIDATE_RAW": True,
    "ONLY_CACHABLE_TABLES": (),
    "ONLY_CACHABLE_APPS": (),
    "UNCACHABLE_TABLES": ("django_migrations",),
    "UNCACHABLE_APPS": (),
    "ADDITIONAL_TABLES": (),
    "QUERY_KEYGEN": "django_cachex.orm.utils.get_query_cache_key",
    "TABLE_KEYGEN": "django_cachex.orm.utils.get_table_cache_key",
    "FINAL_SQL_CHECK": False,
}


def user_settings() -> dict[str, Any]:
    """Return the ``CACHEX_ORM`` dict exactly as the project defines it."""
    return getattr(settings, SETTING_NAME, None) or {}


def _convert_databases(value: Any, raw: dict[str, Any]) -> Any:
    if value == SUPPORTED_ONLY:
        if raw["USE_UNSUPPORTED_DATABASE"]:
            value = set(settings.DATABASES)
        else:
            additional = raw["ADDITIONAL_SUPPORTED_DATABASES"]
            value = {
                alias
                for alias, db in settings.DATABASES.items()
                if db["ENGINE"] in SUPPORTED_DATABASE_ENGINES or db["ENGINE"] in additional
            }
    if value.__class__ in ITERABLES:
        return frozenset(value)
    return value


def _tables_with_apps(value: Any, app_labels: Any) -> frozenset[str]:
    if app_labels:
        # ``all_models[label]`` so an app listed before its models are
        # registered fails loudly instead of contributing nothing.
        app_tables = tuple(
            model._meta.db_table
            for model in chain.from_iterable(apps.all_models[label].values() for label in app_labels)
        )
        return frozenset(tuple(value) + app_tables)
    return frozenset(value)


def _import_if_path(value: Any, _raw: dict[str, Any]) -> Any:
    return import_string(value) if isinstance(value, str) else value


CONVERTERS: dict[str, Callable[[Any, dict[str, Any]], Any]] = {
    "DATABASES": _convert_databases,
    "ONLY_CACHABLE_TABLES": lambda value, raw: _tables_with_apps(value, raw["ONLY_CACHABLE_APPS"]),
    "UNCACHABLE_TABLES": lambda value, raw: _tables_with_apps(value, raw["UNCACHABLE_APPS"]),
    "ADDITIONAL_TABLES": lambda value, _raw: list(value),
    "QUERY_KEYGEN": _import_if_path,
    "TABLE_KEYGEN": _import_if_path,
}


class OrmSettings:
    """The ``CACHEX_ORM`` values after defaults and conversions are applied."""

    ENABLED: bool
    CACHE: str
    DATABASES: Any
    USE_UNSUPPORTED_DATABASE: bool
    ADDITIONAL_SUPPORTED_DATABASES: Any
    TIMEOUT: Any
    CACHE_RANDOM: bool
    CACHE_ITERATORS: bool
    INVALIDATE_RAW: bool
    ONLY_CACHABLE_TABLES: frozenset[str]
    ONLY_CACHABLE_APPS: Any
    UNCACHABLE_TABLES: frozenset[str]
    UNCACHABLE_APPS: Any
    ADDITIONAL_TABLES: list[str]
    QUERY_KEYGEN: Callable[..., str]
    TABLE_KEYGEN: Callable[[str, str], str]
    FINAL_SQL_CHECK: bool

    def __init__(self) -> None:
        self.patched = False

    def load(self) -> None:
        raw = {**DEFAULTS, **{k: v for k, v in user_settings().items() if k in DEFAULTS}}
        for name, value in raw.items():
            converter = CONVERTERS.get(name)
            setattr(self, name, value if converter is None else converter(value, raw))

        if not self.patched:
            from django_cachex.orm.monkey_patch import patch

            patch()
            self.patched = True

    def unload(self) -> None:
        if self.patched:
            from django_cachex.orm.monkey_patch import unpatch

            unpatch()
            self.patched = False

    def reload(self) -> None:
        self.unload()
        self.load()


orm_settings = OrmSettings()
