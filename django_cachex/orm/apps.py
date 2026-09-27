"""App config and system checks of the ORM cache."""

# Derived from django-cachalot 2.9.1 (BSD-3-Clause, Copyright (c) 2014-2016
# Bertrand Bordage); see the LICENSE file in this directory.

import copyreg
from typing import Any

from django.apps import AppConfig
from django.conf import settings
from django.core.checks import CheckMessage, Error, Tags, Warning, register  # noqa: A004
from django.core.signals import setting_changed

from django_cachex.orm.settings import (
    ITERABLES,
    SETTING_NAME,
    SUPPORTED_DATABASE_ENGINES,
    SUPPORTED_ONLY,
    orm_settings,
    user_settings,
)


@register(Tags.database, Tags.compatibility)
def check_databases_compatibility(app_configs: Any, **kwargs: Any) -> list[CheckMessage]:  # noqa: ARG001
    errors: list[CheckMessage] = []
    databases = settings.DATABASES
    original_enabled_databases = user_settings().get("DATABASES", SUPPORTED_ONLY)
    enabled_databases = orm_settings.DATABASES
    if original_enabled_databases == SUPPORTED_ONLY:
        if not orm_settings.DATABASES:
            errors.append(
                Warning(
                    "None of the configured databases are supported by the ORM cache.",
                    hint=f"Use a supported database, or remove django_cachex.orm, or put at least one "
                    f"database alias in `{SETTING_NAME}['DATABASES']` to force the ORM cache to use it.",
                    id="cachex_orm.W002",
                ),
            )
    elif enabled_databases.__class__ in ITERABLES:
        for db_alias in enabled_databases:
            if db_alias in databases:
                engine = databases[db_alias]["ENGINE"]
                if (
                    engine not in SUPPORTED_DATABASE_ENGINES
                    and engine not in orm_settings.ADDITIONAL_SUPPORTED_DATABASES
                    and not orm_settings.USE_UNSUPPORTED_DATABASE
                ):
                    errors.append(
                        Warning(
                            f"Database engine {engine!r} is not supported by the ORM cache.",
                            hint=f"Switch to a supported database engine, add an entry in "
                            f"`{SETTING_NAME}['ADDITIONAL_SUPPORTED_DATABASES']`, or set "
                            f"`{SETTING_NAME}['USE_UNSUPPORTED_DATABASE']` to True.",
                            id="cachex_orm.W003",
                        ),
                    )
            else:
                errors.append(
                    Error(
                        f"Database alias {db_alias!r} from `{SETTING_NAME}['DATABASES']` is not defined in `DATABASES`.",
                        hint=f"Change `{SETTING_NAME}['DATABASES']` to only list aliases from `DATABASES`.",
                        id="cachex_orm.E001",
                    ),
                )

        if not enabled_databases:
            errors.append(
                Warning(
                    f"The ORM cache is useless because no database is configured in `{SETTING_NAME}['DATABASES']`.",
                    hint="Reconfigure the ORM cache or remove it.",
                    id="cachex_orm.W004",
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


def _reload_settings(*, setting: str, **kwargs: Any) -> None:  # noqa: ARG001
    if setting in {SETTING_NAME, "DATABASES", "CACHES"}:
        orm_settings.reload()


class OrmCacheConfig(AppConfig):
    name = "django_cachex.orm"
    label = "cachex_orm"
    verbose_name = "ORM cache"

    def ready(self) -> None:
        # Pickle memoryview (binary columns on PostgreSQL) as bytes.
        copyreg.pickle(memoryview, lambda val: (memoryview, (bytes(val),)))
        orm_settings.load()
        setting_changed.connect(_reload_settings, dispatch_uid="django_cachex.orm.reload_settings")
