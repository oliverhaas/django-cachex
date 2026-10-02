"""Settings of the ORM cache, read from the ``CACHEX_ORM`` dict."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

from typing import TYPE_CHECKING, Any

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

# The migration recorder's model is in no installed app, so recreating the table,
# as each test run does, would not invalidate a cached list of old migrations.
ALWAYS_UNCACHABLE_TABLES = frozenset({"django_migrations"})

# Settings holding table names.
TABLE_SETTINGS = ("ONLY_CACHABLE_TABLES", "UNCACHABLE_TABLES", "ADDITIONAL_TABLES")

DEFAULTS: dict[str, Any] = {
    "ENABLED": True,
    "CACHE": DEFAULT_CACHE_ALIAS,
    "DATABASES": SUPPORTED_ONLY,
    # The cache's own default timeout.
    "TIMEOUT": DEFAULT_TIMEOUT,
    # Seconds a write keeps its lease if it cannot release it; keep it above
    # the database's statement timeout.
    "LEASE_TIMEOUT": 60,
    "ONLY_CACHABLE_TABLES": (),
    "UNCACHABLE_TABLES": (),
    "ADDITIONAL_TABLES": (),
    "FINAL_SQL_CHECK": False,
    # Dotted paths, since django_cachex.orm.utils imports this module.
    "QUERY_KEYGEN": "django_cachex.orm.utils.readable_query_key",
    "TABLE_KEYGEN": "django_cachex.orm.utils.readable_table_key",
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


def _items(value: Any) -> tuple[Any, ...]:
    # A value of another type, like a string missing the comma of a one-item
    # tuple, counts as empty; the cachex_orm.E002 and E007 checks report it.
    return tuple(value) if value.__class__ in ITERABLES else ()


def _convert_databases(value: Any) -> frozenset[str]:
    if value == SUPPORTED_ONLY:
        return frozenset(supported_databases())
    # Other vendors are left out; the cachex_orm.E006 check reports them.
    return frozenset(alias for alias in _items(value) if database_vendor(alias) in SUPPORTED_VENDORS)


def _import_keygen(name: str, value: Any) -> Callable[..., str]:
    # Raised, unlike the errors of other settings, because system checks do not run under a WSGI or ASGI server.
    message = f"`{SETTING_NAME}['{name}']` must be a callable or the dotted path of one, not {value!r}."
    try:
        keygen = import_string(value) if isinstance(value, str) else value
    except ImportError as e:
        raise ImproperlyConfigured(message) from e
    if not callable(keygen):
        raise ImproperlyConfigured(message)
    return keygen


CONVERTERS: dict[str, Callable[[Any], Any]] = {
    "DATABASES": _convert_databases,
    "ONLY_CACHABLE_TABLES": lambda value: frozenset(_items(value)),
    "UNCACHABLE_TABLES": lambda value: frozenset(_items(value)) | ALWAYS_UNCACHABLE_TABLES,
    "ADDITIONAL_TABLES": lambda value: list(_items(value)),
    "QUERY_KEYGEN": lambda value: _import_keygen("QUERY_KEYGEN", value),
    "TABLE_KEYGEN": lambda value: _import_keygen("TABLE_KEYGEN", value),
}


class OrmSettings:
    """The ``CACHEX_ORM`` values after defaults and conversions are applied."""

    ENABLED: bool
    CACHE: str
    DATABASES: frozenset[str]
    TIMEOUT: Any
    LEASE_TIMEOUT: float
    ONLY_CACHABLE_TABLES: frozenset[str]
    UNCACHABLE_TABLES: frozenset[str]
    ADDITIONAL_TABLES: list[str]
    FINAL_SQL_CHECK: bool
    QUERY_KEYGEN: Callable[..., str]
    TABLE_KEYGEN: Callable[..., str]

    def __init__(self) -> None:
        self.patched = False

    def load(self) -> None:
        raw = {**DEFAULTS, **{k: v for k, v in user_settings().items() if k in DEFAULTS}}
        for name, value in raw.items():
            converter = CONVERTERS.get(name)
            setattr(self, name, value if converter is None else converter(value))

        if not self.patched:
            # Imported here because monkey_patch imports this module.
            from django_cachex.orm.monkey_patch import patch

            patch()
            self.patched = True


orm_settings = OrmSettings()
