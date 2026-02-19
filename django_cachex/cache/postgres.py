"""PostgreSQL cache backend using UNLOGGED tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django_cachex.cache.default import KeyValueCache
from django_cachex.client.postgres import PostgreSQLCacheClient
from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from collections.abc import Sequence


class PostgreSQLCache(KeyValueCache):
    """Django cache backend using PostgreSQL UNLOGGED tables.

    Setup::

        INSTALLED_APPS = [
            ...,
            "django_cachex.postgres",   # creates tables via migrate
        ]

        CACHES = {
            "default": {
                "BACKEND": "django_cachex.cache.PostgreSQLCache",
                "LOCATION": "cachex",         # must match model db_table prefix
                "OPTIONS": {
                    "DATABASE": "default",    # Django DATABASES alias
                },
            }
        }

    Then run ``manage.py migrate`` to create the UNLOGGED tables.
    """

    _class = PostgreSQLCacheClient  # type: ignore[assignment]
    _cachex_support = "cachex"

    def eval_script(
        self,
        script: str,
        *,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        pre_hook: Any = None,
        post_hook: Any = None,
        version: int | None = None,
    ) -> Any:
        raise NotSupportedError("eval_script", "PostgreSQL")

    async def aeval_script(
        self,
        script: str,
        *,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        pre_hook: Any = None,
        post_hook: Any = None,
        version: int | None = None,
    ) -> Any:
        raise NotSupportedError("eval_script", "PostgreSQL")
