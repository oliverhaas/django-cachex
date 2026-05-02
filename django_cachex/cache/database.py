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

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from django.conf import settings
from django.core.cache.backends.db import DatabaseCache as DjangoDatabaseCache
from django.db import connections, router

from django_cachex.cache.mixin import CachexMixin

if TYPE_CHECKING:
    from django.db.backends.base.base import BaseDatabaseWrapper

    from django_cachex.types import ExpiryT, KeyT


def _now() -> datetime:
    """Return current time with microseconds truncated, matching Django's storage."""
    tz = UTC if settings.USE_TZ else None
    return datetime.now(tz=tz).replace(microsecond=0)


def _adapt_dt(conn: BaseDatabaseWrapper, dt: datetime) -> Any:
    """Adapt a datetime for the given database connection."""
    return conn.ops.adapt_datetimefield_value(dt.replace(microsecond=0))


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

    def _get_connection(self, *, write: bool = False) -> BaseDatabaseWrapper:
        """Get the router-aware database connection for this cache."""
        if write:
            db = router.db_for_write(self.cache_model_class)
        else:
            db = router.db_for_read(self.cache_model_class)
        return connections[db]

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get TTL by reading the expires column from database.

        Returns:
            -2 if key does not exist or is expired, else seconds remaining.
        """
        conn = self._get_connection()
        table = self._get_table_name()
        quoted = conn.ops.quote_name(table)
        cache_key = self.make_key(str(key), version=version)
        now = _now()
        now_adapted = _adapt_dt(conn, now)

        with conn.cursor() as cursor:
            sql = f"SELECT expires FROM {quoted} WHERE cache_key = %s AND expires > %s"  # noqa: S608
            cursor.execute(sql, [cache_key, now_adapted])
            row = cursor.fetchone()
            if row is None:
                return -2

            # Convert the stored expires value back to a datetime for TTL calc
            raw_expires = row[0]

            if isinstance(raw_expires, str):
                # SQLite stores as string — parse it back
                fmt = "%Y-%m-%d %H:%M:%S"
                expires_dt = datetime.strptime(raw_expires, fmt)  # noqa: DTZ007
            elif isinstance(raw_expires, datetime):
                expires_dt = raw_expires
            else:
                return -2

            # Normalize tz-awareness to match `now` — SQLite returns naive
            # datetimes regardless of USE_TZ, and Django stores them as UTC.
            if settings.USE_TZ and expires_dt.tzinfo is None:
                expires_dt = expires_dt.replace(tzinfo=UTC)
            elif not settings.USE_TZ and expires_dt.tzinfo is not None:
                expires_dt = expires_dt.replace(tzinfo=None)

            remaining = (expires_dt - now).total_seconds()
            if remaining <= 0:
                return -2
            return int(remaining)

    def expire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool:
        """Set the TTL of a key by updating the expires column."""
        conn = self._get_connection(write=True)
        table = self._get_table_name()
        quoted = conn.ops.quote_name(table)
        cache_key = self.make_key(str(key), version=version)

        if isinstance(timeout, timedelta):
            timeout_secs = timeout.total_seconds()
        else:
            timeout_secs = float(timeout)

        new_expires = _now() + timedelta(seconds=timeout_secs)
        adapted_expires = _adapt_dt(conn, new_expires)

        with conn.cursor() as cursor:
            sql = f"UPDATE {quoted} SET expires = %s WHERE cache_key = %s"  # noqa: S608
            cursor.execute(sql, [adapted_expires, cache_key])
            return cursor.rowcount > 0

    def persist(self, key: KeyT, version: int | None = None) -> bool:
        """Remove the TTL from a key (set to far-future expiry)."""
        # Django's DatabaseCache doesn't support "no expiry" — use ~10 years
        return self.expire(key, timedelta(days=3650), version=version)

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List keys matching the pattern by querying the database."""
        conn = self._get_connection()
        table = self._get_table_name()
        quoted = conn.ops.quote_name(table)

        if pattern and pattern != "*":
            transformed_pattern = self.make_key(pattern)
            sql_pattern = transformed_pattern.replace("*", "%").replace("?", "_")
        else:
            sql_pattern = "%"

        now_adapted = _adapt_dt(conn, _now())

        with conn.cursor() as cursor:
            sql = f"""
                SELECT cache_key FROM {quoted}
                WHERE cache_key LIKE %s AND expires > %s
                ORDER BY cache_key
            """  # noqa: S608
            cursor.execute(sql, [sql_pattern, now_adapted])
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
        """Get DatabaseCache info in Redis-INFO-shaped format for admin uniformity."""
        conn = self._get_connection()
        table = self._get_table_name()
        quoted = conn.ops.quote_name(table)

        total_count = 0
        active_count = 0
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {quoted}")  # noqa: S608
                total_count = cursor.fetchone()[0]
                now_adapted = _adapt_dt(conn, _now())
                cursor.execute(
                    f"SELECT COUNT(*) FROM {quoted} WHERE expires > %s",  # noqa: S608
                    [now_adapted],
                )
                active_count = cursor.fetchone()[0]
        except Exception:  # noqa: BLE001, S110
            # Don't crash the admin if a query fails; return zeros.
            pass

        return {
            "backend": "DatabaseCache",
            "server": {
                "redis_version": f"DatabaseCache ({conn.vendor})",
                "os": f"table: {table}",
            },
            "keyspace": {
                "db0": {
                    "keys": active_count,
                    "expires": active_count,
                },
            },
            "stats": {
                "expired_keys": total_count - active_count,
            },
        }
