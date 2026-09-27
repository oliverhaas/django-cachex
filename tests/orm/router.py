"""Database router of the ORM cache tests."""

from typing import Any

from django.conf import settings


class PostgresRouter:
    """Keep the PostgreSQL-only model off the other databases."""

    def allow_migrate(self, db: str, app_label: str, model_name: str | None = None, **hints: Any) -> bool | None:
        if app_label == "ormtest" and model_name == "postgresmodel":
            # Read the settings rather than `connections`: Django also asks about
            # aliases outside DATABASES, like the `__no_db__` connection that
            # creates the test database.
            return settings.DATABASES.get(db, {}).get("ENGINE") == "django.db.backends.postgresql"
        return None
