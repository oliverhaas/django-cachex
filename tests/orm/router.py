"""Database router of the ORM cache tests."""

from typing import Any

from django.conf import settings


class PostgresRouter:
    """Keep the PostgreSQL-only model off the other databases."""

    def allow_migrate(self, db: str, app_label: str, model_name: str | None = None, **hints: Any) -> bool | None:
        if app_label == "ormtest" and model_name == "postgresmodel":
            # Not `connections`, which fails on aliases outside DATABASES like `__no_db__`.
            return settings.DATABASES.get(db, {}).get("ENGINE") == "django.db.backends.postgresql"
        return None
