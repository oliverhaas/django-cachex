"""Custom migration operations for PostgreSQL UNLOGGED tables."""

from __future__ import annotations

from typing import Any

from django.db import migrations


class CreateUnloggedModel(migrations.CreateModel):
    """Like CreateModel, but creates an UNLOGGED table on PostgreSQL.

    UNLOGGED tables skip WAL writes (~2-3x faster), making them ideal for cache.
    Data is truncated on crash, which is acceptable for ephemeral cache data.

    On non-PostgreSQL backends this falls back to a regular CREATE TABLE.
    """

    def database_forwards(
        self,
        app_label: str,
        schema_editor: Any,
        from_state: Any,
        to_state: Any,
    ) -> None:
        if schema_editor.connection.vendor == "postgresql":
            original = schema_editor.sql_create_table
            schema_editor.sql_create_table = "CREATE UNLOGGED TABLE %(table)s (%(definition)s)"
            try:
                super().database_forwards(app_label, schema_editor, from_state, to_state)
            finally:
                schema_editor.sql_create_table = original
        else:
            super().database_forwards(app_label, schema_editor, from_state, to_state)
