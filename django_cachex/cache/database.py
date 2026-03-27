"""Cachex DatabaseCache — drop-in replacement for Django's DatabaseCache.

Extends ``django.core.cache.backends.db.DatabaseCache`` with:

- Data structure operations (lists, sets, hashes, sorted sets)
- TTL operations (ttl, expire, persist)
- Key scanning and type detection
- Admin support (info, keys, scan)

Usage::

    CACHES = {
        "default": {
            "BACKEND": "django_cachex.cache.DatabaseCache",
            "LOCATION": "django_cache_table",
        },
    }

Then run ``manage.py createcachetable``.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, cast

from django.core.cache.backends.db import DatabaseCache as DjangoDatabaseCache
from django.db import connection

from django_cachex.cache.mixin import CachexMixin

if TYPE_CHECKING:
    from django_cachex.types import ExpiryT, KeyT


class DatabaseCache(CachexMixin, DjangoDatabaseCache):
    """DatabaseCache with cachex extensions.

    Drop-in replacement for ``django.core.cache.backends.db.DatabaseCache``.
    All standard Django cache operations work identically. Additionally provides
    data structure operations, TTL management, and admin support.

    Data structures (lists, sets, hashes, sorted sets) are stored as serialized
    Python objects via the standard get/set interface — no schema changes needed.
    """

    _cachex_support: str = "cachex"

    def _get_table_name(self) -> str:
        """Get the database table name for this cache."""
        return cast("Any", self)._table

    def _expires_condition(self) -> str:
        """Return the SQL condition for non-expired entries."""
        if connection.vendor in ("postgresql", "oracle"):
            return "expires > to_timestamp(%s)"
        if connection.vendor == "mysql":
            return "expires > FROM_UNIXTIME(%s)"
        return "expires > %s"

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get TTL by reading the expires column from database.

        Returns:
            -2 if key does not exist or is expired, else seconds remaining.
        """
        table = self._get_table_name()
        quoted = connection.ops.quote_name(table)
        cache_key = self.make_key(str(key), version=version)
        now = time.time()

        with connection.cursor() as cursor:
            if connection.vendor in ("postgresql", "oracle"):
                sql = f"SELECT EXTRACT(EPOCH FROM expires) - %s FROM {quoted} WHERE cache_key = %s"  # noqa: S608
            elif connection.vendor == "mysql":
                sql = f"SELECT UNIX_TIMESTAMP(expires) - %s FROM {quoted} WHERE cache_key = %s"  # noqa: S608
            else:
                sql = f"SELECT expires - %s FROM {quoted} WHERE cache_key = %s"  # noqa: S608

            cursor.execute(sql, [now, cache_key])
            row = cursor.fetchone()
            if row is None:
                return -2
            ttl_val = row[0]
            if ttl_val <= 0:
                return -2
            return int(ttl_val)

    def expire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool:
        """Set the TTL of a key by updating the expires column."""
        from datetime import timedelta

        table = self._get_table_name()
        quoted = connection.ops.quote_name(table)
        cache_key = self.make_key(str(key), version=version)

        if isinstance(timeout, timedelta):
            timeout_secs = timeout.total_seconds()
        else:
            timeout_secs = float(timeout)

        new_expires = time.time() + timeout_secs

        with connection.cursor() as cursor:
            if connection.vendor in ("postgresql", "oracle"):
                sql = f"UPDATE {quoted} SET expires = to_timestamp(%s) WHERE cache_key = %s"  # noqa: S608
            elif connection.vendor == "mysql":
                sql = f"UPDATE {quoted} SET expires = FROM_UNIXTIME(%s) WHERE cache_key = %s"  # noqa: S608
            else:
                sql = f"UPDATE {quoted} SET expires = %s WHERE cache_key = %s"  # noqa: S608

            cursor.execute(sql, [new_expires, cache_key])
            return cursor.rowcount > 0

    def persist(self, key: KeyT, version: int | None = None) -> bool:
        """Remove the TTL from a key (set to far-future expiry)."""
        from datetime import timedelta

        # Django's DatabaseCache doesn't support "no expiry" — use ~10 years
        return self.expire(key, timedelta(days=3650), version=version)

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List keys matching the pattern by querying the database."""
        table = self._get_table_name()
        quoted = connection.ops.quote_name(table)

        if pattern and pattern != "*":
            transformed_pattern = self.make_key(pattern)
            sql_pattern = transformed_pattern.replace("*", "%").replace("?", "_")
        else:
            sql_pattern = "%"

        now = time.time()
        expires_cond = self._expires_condition()

        with connection.cursor() as cursor:
            sql = f"""
                SELECT cache_key FROM {quoted}
                WHERE cache_key LIKE %s AND {expires_cond}
                ORDER BY cache_key
            """  # noqa: S608
            cursor.execute(sql, [sql_pattern, now])
            raw_keys = [row[0] for row in cursor.fetchall()]

        result = []
        for cache_key in raw_keys:
            if cache_key.startswith(self.key_prefix):
                key_without_prefix = cache_key[len(self.key_prefix) :]
                parts = key_without_prefix.split(":", 2)
                original_key = parts[2] if len(parts) >= 3 else key_without_prefix
                result.append(original_key)
            else:
                result.append(cache_key)
        return result

    # =========================================================================
    # Info
    # =========================================================================

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get DatabaseCache info in structured format."""
        table = self._get_table_name()
        quoted = connection.ops.quote_name(table)

        total_count: int | str = 0
        active_count: int | str = 0
        expired_count: int | str = 0

        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {quoted}")  # noqa: S608
                total_count = cursor.fetchone()[0]

                now = time.time()
                expires_cond = self._expires_condition()
                cursor.execute(
                    f"SELECT COUNT(*) FROM {quoted} WHERE {expires_cond}",  # noqa: S608
                    [now],
                )
                active_count = cursor.fetchone()[0]
                expired_count = (
                    total_count - active_count if isinstance(total_count, int) and isinstance(active_count, int) else 0
                )
        except Exception:  # noqa: BLE001
            total_count = "error"
            active_count = "error"

        return {
            "backend": "DatabaseCache",
            "server": {
                "redis_version": f"DatabaseCache ({connection.vendor})",
                "os": f"table: {table}",
            },
            "keyspace": {
                "db0": {
                    "keys": active_count if active_count != "error" else 0,
                    "expires": active_count if active_count != "error" else 0,
                },
            },
            "stats": {
                "expired_keys": expired_count,
            },
        }
