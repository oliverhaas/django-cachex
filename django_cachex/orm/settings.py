"""Settings of the ORM cache, read from the ``CACHEX_ORM`` dict."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

from typing import TYPE_CHECKING, Any

from django.apps import apps
from django.conf import settings
from django.core.cache import DEFAULT_CACHE_ALIAS
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.core.exceptions import ImproperlyConfigured
from django.db.utils import load_backend
from django.utils.module_loading import import_string

if TYPE_CHECKING:
    from collections.abc import Callable

SETTING_NAME = "CACHEX_ORM"

# Database vendors whose transaction isolation the ORM cache knows how to read.
SUPPORTED_VENDORS = frozenset({"postgresql", "sqlite"})

SUPPORTED_ONLY = "supported_only"
ITERABLES = frozenset({tuple, list, frozenset, set})

# Never cached, whatever UNCACHABLE_TABLES says. The model of the migration
# recorder is in no installed app, so creating its table afresh, as each test
# run does, invalidates nothing, and a cached read of it would list migrations
# the new database lacks.
ALWAYS_UNCACHABLE_TABLES = frozenset({"django_migrations"})

# Settings holding table names or app labels.
TABLE_SETTINGS = (
    "ONLY_CACHABLE_TABLES",
    "ONLY_CACHABLE_APPS",
    "UNCACHABLE_TABLES",
    "UNCACHABLE_APPS",
    "ADDITIONAL_TABLES",
)

DEFAULTS: dict[str, Any] = {
    "ENABLED": True,
    "CACHE": DEFAULT_CACHE_ALIAS,
    "DATABASES": SUPPORTED_ONLY,
    # The cache's own default timeout.
    "TIMEOUT": DEFAULT_TIMEOUT,
    # Seconds a write keeps its lease if it cannot release it; keep it above
    # the database's statement timeout.
    "LEASE_TIMEOUT": 60,
    "CACHE_RANDOM": False,
    "CACHE_ITERATORS": True,
    "INVALIDATE_RAW": True,
    "ONLY_CACHABLE_TABLES": (),
    "ONLY_CACHABLE_APPS": (),
    "UNCACHABLE_TABLES": (),
    "UNCACHABLE_APPS": (),
    "ADDITIONAL_TABLES": (),
    "QUERY_KEYGEN": "django_cachex.orm.utils.get_query_cache_key",
    "TABLE_KEYGEN": "django_cachex.orm.utils.get_table_cache_key",
    "FINAL_SQL_CHECK": False,
}


def user_settings() -> dict[str, Any]:
    """Return the ``CACHEX_ORM`` dict exactly as the project defines it."""
    return getattr(settings, SETTING_NAME, None) or {}


def database_vendor(alias: str) -> str | None:
    """Return the vendor of database ``alias``, or None if its backend does not load."""
    # From the backend class, so no connection is set up.
    try:
        return load_backend(settings.DATABASES[alias]["ENGINE"]).DatabaseWrapper.vendor
    except ImproperlyConfigured, KeyError:
        return None


def replica_of(alias: str) -> str | None:
    """Return the alias database ``alias`` mirrors in tests (``TEST['MIRROR']``), as replicas do."""
    database: dict[str, Any] = settings.DATABASES[alias]
    return (database.get("TEST") or {}).get("MIRROR")


def supported_databases() -> set[str]:
    """Return the aliases of the databases whose vendor the ORM cache supports, replicas excepted."""
    # Writes to a primary invalidate the queries cached from its own alias
    # only, so results read from a replica would go stale.
    return {
        alias for alias in settings.DATABASES if database_vendor(alias) in SUPPORTED_VENDORS and not replica_of(alias)
    }


def _convert_databases(value: Any, _raw: dict[str, Any]) -> Any:
    if value == SUPPORTED_ONLY:
        value = supported_databases()
    if value.__class__ in ITERABLES:
        return frozenset(value)
    return value


def _items(value: Any) -> tuple[Any, ...]:
    # A value of another type, like a string missing the comma of a one-item
    # tuple, counts as empty; the cachex_orm.E007 check reports it.
    return tuple(value) if value.__class__ in ITERABLES else ()


def _tables_with_apps(value: Any, app_labels: Any) -> frozenset[str]:
    # A label no installed app has adds nothing; the cachex_orm.E006 check
    # reports it.
    labels = set(_items(app_labels))
    app_tables = [
        model._meta.db_table
        for app_config in apps.get_app_configs()
        if app_config.label in labels
        for model in app_config.get_models(include_auto_created=True)
    ]
    return frozenset((*_items(value), *app_tables))


def _import_if_path(value: Any, _raw: dict[str, Any]) -> Any:
    return import_string(value) if isinstance(value, str) else value


CONVERTERS: dict[str, Callable[[Any, dict[str, Any]], Any]] = {
    "DATABASES": _convert_databases,
    "ONLY_CACHABLE_TABLES": lambda value, raw: _tables_with_apps(value, raw["ONLY_CACHABLE_APPS"]),
    "UNCACHABLE_TABLES": lambda value, raw: _tables_with_apps(value, raw["UNCACHABLE_APPS"]) | ALWAYS_UNCACHABLE_TABLES,
    "ADDITIONAL_TABLES": lambda value, _raw: list(_items(value)),
    "QUERY_KEYGEN": _import_if_path,
    "TABLE_KEYGEN": _import_if_path,
}


class OrmSettings:
    """The ``CACHEX_ORM`` values after defaults and conversions are applied."""

    ENABLED: bool
    CACHE: str
    DATABASES: Any
    TIMEOUT: Any
    LEASE_TIMEOUT: float
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
            # Imported here because monkey_patch imports this module.
            from django_cachex.orm.monkey_patch import patch

            patch()
            self.patched = True


orm_settings = OrmSettings()
