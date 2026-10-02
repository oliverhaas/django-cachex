"""App config and system checks of the ORM cache."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import copyreg
import datetime
import math
from decimal import Decimal
from typing import Any
from uuid import UUID

from django.apps import AppConfig
from django.conf import settings
from django.core.checks import CheckMessage, Error, Tags, Warning, register  # noqa: A004
from django.core.signals import setting_changed

from django_cachex.orm.settings import (
    DEFAULTS,
    ITERABLES,
    SETTING_NAME,
    SUPPORTED_ONLY,
    SUPPORTED_VENDORS,
    TABLE_SETTINGS,
    database_vendor,
    orm_settings,
    replica_of,
    user_settings,
)
from django_cachex.orm.store import RespStore, get_store

# Shaped like the rows a query returns, with the column types most databases have.
_SAMPLE_RESULT = [
    [
        (
            1,
            "text",
            Decimal("1.5"),
            1.5,
            True,
            None,
            b"bytes",
            datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.UTC),
            datetime.date(2026, 1, 1),
            datetime.time(12, 0),
            datetime.timedelta(seconds=1),
            UUID(int=1),
        ),
    ],
]


@register(Tags.database, Tags.compatibility)
def check_databases_compatibility(app_configs: Any, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    errors: list[CheckMessage] = []
    configured = user_settings().get("DATABASES", SUPPORTED_ONLY)
    if configured == SUPPORTED_ONLY:
        if not orm_settings.DATABASES:
            errors.append(
                Warning(
                    "None of the configured databases are supported by the ORM cache.",
                    hint="The ORM cache supports PostgreSQL and SQLite. Use one of them, or remove django_cachex.orm.",
                    id="cachex_orm.W002",
                ),
            )
    elif configured.__class__ in ITERABLES:
        for db_alias in configured:
            if db_alias not in settings.DATABASES:
                errors.append(
                    Error(
                        f"Database alias {db_alias!r} from `{SETTING_NAME}['DATABASES']` is not defined in `DATABASES`.",
                        hint=f"Change `{SETTING_NAME}['DATABASES']` to only list aliases from `DATABASES`.",
                        id="cachex_orm.E001",
                    ),
                )
                continue
            if (vendor := database_vendor(db_alias)) not in SUPPORTED_VENDORS:
                errors.append(
                    Error(
                        f"Database {db_alias!r} ({vendor or 'backend not loadable'}) is not supported by the "
                        "ORM cache.",
                        hint=f"The ORM cache supports PostgreSQL and SQLite. Remove {db_alias!r} from "
                        f"`{SETTING_NAME}['DATABASES']`.",
                        id="cachex_orm.E006",
                    ),
                )
            if primary := replica_of(db_alias):
                errors.append(
                    Warning(
                        f"Database {db_alias!r} mirrors {primary!r} (TEST['MIRROR']), so it looks like a replica.",
                        hint=f"Writes to {primary!r} do not invalidate the queries the ORM cache caches from "
                        f"{db_alias!r}. Remove it from `{SETTING_NAME}['DATABASES']`.",
                        id="cachex_orm.W005",
                    ),
                )

        if not configured:
            errors.append(
                Warning(
                    f"The ORM cache is useless because no database is configured in `{SETTING_NAME}['DATABASES']`.",
                    hint="Reconfigure the ORM cache or remove it.",
                    id="cachex_orm.W003",
                ),
            )
    else:
        errors.append(
            Error(
                f"`{SETTING_NAME}['DATABASES']` must be either {SUPPORTED_ONLY!r} or a list, tuple, "
                "frozenset or set of database aliases.",
                hint=f"Remove `{SETTING_NAME}['DATABASES']` or change it.",
                id="cachex_orm.E002",
            ),
        )
    return errors


@register(Tags.caches)
def check_cache(app_configs: Any, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    errors: list[CheckMessage] = []
    unknown = sorted(set(user_settings()) - set(DEFAULTS))
    if unknown:
        errors.append(
            Warning(
                f"Unknown `{SETTING_NAME}` settings: {', '.join(map(str, unknown))}.",
                hint=f"The ORM cache ignores them. Settings are upper case: {', '.join(DEFAULTS)}.",
                id="cachex_orm.W004",
            ),
        )
    alias = orm_settings.CACHE
    if alias not in settings.CACHES:
        errors.append(
            Error(
                f"`{SETTING_NAME}['CACHE']` is {alias!r}, which is not defined in `CACHES`.",
                hint=f"Set `{SETTING_NAME}['CACHE']` to an alias from `CACHES`.",
                id="cachex_orm.E003",
            ),
        )
        return errors
    try:
        store = get_store(alias)
    except Exception as e:  # noqa: BLE001
        errors.append(Error(f"The ORM cache could not load the cache {alias!r}: {e}", id="cachex_orm.E005"))
        return errors
    if store is None:
        errors.append(
            Warning(
                f"The ORM cache cannot use the cache {alias!r} ({settings.CACHES[alias]['BACKEND']}), so it "
                "caches nothing.",
                hint="Use a django-cachex Redis or Valkey backend, TrackingCache or LocMemCache.",
                id="cachex_orm.W001",
            ),
        )
    elif isinstance(store, RespStore):
        try:
            round_trip = store.cache.decode(store.cache.encode(_SAMPLE_RESULT))
        except Exception:  # noqa: BLE001
            round_trip = None
        if round_trip != _SAMPLE_RESULT:
            errors.append(
                Error(
                    f"The serializer of the cache {alias!r} does not round-trip query results.",
                    hint="Results must come back with their Python types; the default pickle serializer does.",
                    id="cachex_orm.E004",
                ),
            )
    return errors


@register(Tags.models)
def check_table_settings(app_configs: Any, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    return [
        Error(
            f"`{SETTING_NAME}['{name}']` must be a list, tuple, frozenset or set.",
            hint="A tuple of one item needs a trailing comma: ('name',).",
            id="cachex_orm.E007",
        )
        for name in TABLE_SETTINGS
        if user_settings().get(name, ()).__class__ not in ITERABLES
    ]


@register(Tags.caches)
def check_lease_timeout(app_configs: Any, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    value = orm_settings.LEASE_TIMEOUT
    if not isinstance(value, bool) and isinstance(value, int | float) and 0 < value < math.inf:
        return []
    return [
        Error(
            f"`{SETTING_NAME}['LEASE_TIMEOUT']` must be a positive number of seconds, not {value!r}.",
            id="cachex_orm.E008",
        ),
    ]


def _reload_settings(*, setting: str, **kwargs: Any) -> None:  # noqa: ARG001
    # The patches read the settings on every call, so they stay in place.
    if setting in {SETTING_NAME, "DATABASES", "CACHES"}:
        orm_settings.load()


class OrmCacheConfig(AppConfig):
    name = "django_cachex.orm"
    label = "cachex_orm"
    verbose_name = "ORM cache"

    def ready(self) -> None:
        # Pickle memoryview (binary columns on PostgreSQL) as bytes.
        copyreg.pickle(memoryview, lambda val: (memoryview, (bytes(val),)))
        orm_settings.load()
        setting_changed.connect(_reload_settings, dispatch_uid="django_cachex.orm.reload_settings")
