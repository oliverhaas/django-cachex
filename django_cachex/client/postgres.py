"""PostgreSQL cache client for django-cachex.

Provides a PostgreSQL-backed cache client that implements the same interface as
KeyValueCacheClient, using Django's database connections and UNLOGGED tables to
provide Redis-like caching with data structures (hashes, lists, sets, sorted sets).
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from asgiref.sync import sync_to_async

from django_cachex.compat import create_compressor, create_serializer
from django_cachex.exceptions import CompressorError, NotSupportedError, SerializerError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    import builtins
    from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence

    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT

# Type IDs matching the schema
_TYPE_STRING = 0
_TYPE_HASH = 1
_TYPE_LIST = 2
_TYPE_SET = 3
_TYPE_ZSET = 4

_TYPE_ID_TO_KEYTYPE: dict[int, KeyType] = {
    _TYPE_STRING: KeyType.STRING,
    _TYPE_HASH: KeyType.HASH,
    _TYPE_LIST: KeyType.LIST,
    _TYPE_SET: KeyType.SET,
    _TYPE_ZSET: KeyType.ZSET,
}

_KEYTYPE_TO_TYPE_ID: dict[str, int] = {
    "string": _TYPE_STRING,
    "hash": _TYPE_HASH,
    "list": _TYPE_LIST,
    "set": _TYPE_SET,
    "zset": _TYPE_ZSET,
}

_BACKEND_NAME = "PostgreSQL"


def _async(method_name: str) -> Any:
    """Create an async wrapper for a sync method."""

    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(getattr(self, method_name))(*args, **kwargs)

    wrapper.__name__ = f"a{method_name}"
    return wrapper


def _not_supported(operation: str) -> Any:
    """Create a method that raises NotSupportedError."""

    def method(self: Any, *args: Any, **kwargs: Any) -> Any:
        raise NotSupportedError(operation, _BACKEND_NAME)

    method.__name__ = operation
    return method


def _parse_score_bound(value: float | str) -> tuple[bool, float]:
    """Parse a Redis-style score bound.

    Returns (inclusive, numeric_value). Handles '-inf', '+inf', 'inf',
    and '(' prefix for exclusive bounds.
    """
    if isinstance(value, str):
        s = value.strip()
        if s == "-inf":
            return True, float("-inf")
        if s in ("+inf", "inf"):
            return True, float("inf")
        if s.startswith("("):
            return False, float(s[1:])
        return True, float(s)
    return True, float(value)


class PostgreSQLCacheClient:
    """PostgreSQL-backed cache client implementing the KeyValueCacheClient interface.

    Uses Django's database connections and UNLOGGED tables to provide Redis-like
    caching with data structures (hashes, lists, sets, sorted sets).
    """

    _default_scan_itersize: int = 100

    def __init__(
        self,
        servers: list[str],
        serializer: str | list | builtins.type[Any] | None = None,
        pool_class: str | builtins.type[Any] | None = None,
        parser_class: str | builtins.type[Any] | None = None,
        async_pool_class: str | builtins.type[Any] | None = None,
        **options: Any,
    ) -> None:
        """Initialize the PostgreSQL cache client."""
        self._table = servers[0]
        self._db_alias = options.pop("DATABASE", "default")

        # Ignore Redis-specific options silently
        options.pop("compressor_config", None)

        # Setup compressors
        compressor_config = options.get("compressor")
        self._compressors = self._create_compressors(compressor_config)

        # Setup serializers
        serializer_config = (
            serializer
            if serializer is not None
            else options.get("serializer", "django_cachex.serializers.pickle.PickleSerializer")
        )
        self._serializers = self._create_serializers(serializer_config)

        self._options = options

    # =========================================================================
    # Serializer/Compressor Setup
    # =========================================================================

    def _create_serializers(self, config: str | list | builtins.type[Any] | Any) -> list:
        """Create serializer instance(s) from config."""
        if isinstance(config, list):
            return [create_serializer(item) for item in config]
        return [create_serializer(config)]

    def _create_compressors(self, config: str | list | builtins.type[Any] | Any | None) -> list:
        """Create compressor instance(s) from config."""
        if config is None:
            return []
        if isinstance(config, list):
            return [create_compressor(item) for item in config]
        return [create_compressor(config)]

    def _decompress(self, value: bytes) -> bytes:
        """Decompress with fallback support for multiple compressors."""
        for compressor in self._compressors:
            try:
                return compressor.decompress(value)
            except CompressorError:
                continue
        return value

    def _deserialize(self, value: bytes) -> Any:
        """Deserialize with fallback support for multiple serializers."""
        last_error: SerializerError | None = None
        for serializer in self._serializers:
            try:
                return serializer.loads(value)
            except SerializerError as e:
                last_error = e
                continue

        if last_error is not None:
            raise last_error
        raise SerializerError("No serializers configured")

    # =========================================================================
    # Encoding/Decoding
    # =========================================================================

    def encode(self, value: Any) -> bytes | int:
        """Encode a value for storage (serialize + compress). Plain ints pass through unchanged."""
        if isinstance(value, bool) or not isinstance(value, int):
            value = self._serializers[0].dumps(value)
            if self._compressors:
                return self._compressors[0].compress(value)
            return value
        return value

    def decode(self, value: Any) -> Any:
        """Decode a value from storage. Returns int directly if parseable, otherwise decompress + deserialize."""
        try:
            return int(value)
        except (ValueError, TypeError):
            if isinstance(value, memoryview):
                value = bytes(value)
            value = self._decompress(value)
            return self._deserialize(value)

    # =========================================================================
    # Database helpers
    # =========================================================================

    @property
    def _conn(self) -> Any:
        """Get the Django database connection."""
        from django.db import connections

        return connections[self._db_alias]

    def _now(self) -> datetime:
        """Get current time, matching Django's USE_TZ setting."""
        from django.utils import timezone

        return timezone.now()

    def _make_expiry(self, timeout: float | None) -> datetime | None | int:
        """Convert a timeout to an expiry timestamp.

        Returns None for no expiry, 0 for immediate deletion, datetime for future expiry.
        """
        if timeout is None:
            return None
        if timeout == 0:
            return 0
        return self._now() + timedelta(seconds=timeout)

    def _expiry_filter(self) -> str:
        """SQL fragment to filter out expired keys."""
        return "AND (expires_at IS NULL OR expires_at > NOW())"

    def _ensure_key(self, cursor: Any, key: KeyT, type_id: int) -> None:
        """Ensure key exists in main table with the correct type.

        Handles expired keys (treats as non-existent) and type transitions
        (cleans up old auxiliary data before changing type).
        """
        cursor.execute(
            f"SELECT type, expires_at FROM {self._table} WHERE key = %s",
            [key],
        )
        row = cursor.fetchone()

        if row is not None:
            old_type, expires_at = row
            now = self._now()
            expired = expires_at is not None and expires_at <= now

            if expired:
                # Expired key: remove completely and treat as new
                self._delete_all_key_data(cursor, key)
                cursor.execute(f"DELETE FROM {self._table} WHERE key = %s", [key])
            elif old_type != type_id:
                # Type change: clean up old auxiliary data
                self._delete_key_data(cursor, key, old_type)

        # Upsert key with correct type
        cursor.execute(
            f"""
            INSERT INTO {self._table} (key, type)
            VALUES (%s, %s)
            ON CONFLICT (key)
            DO UPDATE SET type = EXCLUDED.type, value = NULL
            WHERE {self._table}.type != EXCLUDED.type
            """,
            [key, type_id],
        )

    def _delete_key_data(self, cursor: Any, key: KeyT, type_id: int) -> None:
        """Delete data from auxiliary tables based on type."""
        if type_id == _TYPE_HASH:
            cursor.execute(f"DELETE FROM {self._table}_hashes WHERE key = %s", [key])
        elif type_id == _TYPE_LIST:
            cursor.execute(f"DELETE FROM {self._table}_lists WHERE key = %s", [key])
        elif type_id == _TYPE_SET:
            cursor.execute(f"DELETE FROM {self._table}_sets WHERE key = %s", [key])
        elif type_id == _TYPE_ZSET:
            cursor.execute(f"DELETE FROM {self._table}_zsets WHERE key = %s", [key])

    def _delete_all_key_data(self, cursor: Any, key: KeyT) -> None:
        """Delete data from all auxiliary tables for a key."""
        for suffix in ("_hashes", "_lists", "_sets", "_zsets"):
            cursor.execute(f"DELETE FROM {self._table}{suffix} WHERE key = %s", [key])

    def _get_key_type(self, cursor: Any, key: KeyT) -> int | None:
        """Get the type of a key, returning None if key doesn't exist or is expired."""
        cursor.execute(
            f"SELECT type FROM {self._table} WHERE key = %s {self._expiry_filter()}",
            [key],
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def _to_bytea(self, encoded: bytes | int) -> bytes:
        """Convert an encoded value to bytes suitable for BYTEA storage."""
        if isinstance(encoded, int):
            return str(encoded).encode()
        return encoded

    def _glob_to_sql(self, pattern: str) -> tuple[str, bool]:  # noqa: C901, PLR0912
        """Convert a Redis glob pattern to SQL.

        Returns (sql_pattern, use_regex).
        If the pattern contains bracket expressions, returns a regex for use with ~.
        Otherwise returns a LIKE pattern.
        """
        if "[" in pattern:
            # Use PostgreSQL regex
            regex = ""
            i = 0
            while i < len(pattern):
                c = pattern[i]
                if c == "*":
                    regex += ".*"
                elif c == "?":
                    regex += "."
                elif c == "[":
                    # Find the closing bracket
                    j = pattern.index("]", i + 1)
                    regex += pattern[i : j + 1]
                    i = j
                elif c in r"\.+^${}()|":
                    regex += "\\" + c
                else:
                    regex += c
                i += 1
            return "^" + regex + "$", True

        # Use LIKE pattern
        # First escape existing % and _ in the original
        like = ""
        for c in pattern:
            if c == "%":
                like += "\\%"
            elif c == "_":
                like += "\\_"
            elif c == "*":
                like += "%"
            elif c == "?":
                like += "_"
            elif c == "\\":
                like += "\\\\"
            else:
                like += c
        return like, False

    # =========================================================================
    # Core Cache Operations
    # =========================================================================

    def set(self, key: KeyT, value: Any, timeout: int | None) -> None:
        """Set a value in the cache."""
        if timeout == 0:
            self.delete(key)
            return

        encoded = self.encode(value)
        bytea = self._to_bytea(encoded)
        expiry = self._make_expiry(timeout)

        with self._conn.cursor() as cursor:
            # Delete any auxiliary data if the key existed with a different type
            self._delete_all_key_data(cursor, key)
            cursor.execute(
                f"""
                INSERT INTO {self._table} (key, type, value, expires_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (key)
                DO UPDATE SET type = EXCLUDED.type, value = EXCLUDED.value, expires_at = EXCLUDED.expires_at
                """,
                [key, _TYPE_STRING, bytea, expiry],
            )

    def get(self, key: KeyT) -> Any:
        """Fetch a value from the cache."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"SELECT value FROM {self._table} WHERE key = %s AND type = %s {self._expiry_filter()}",
                [key, _TYPE_STRING],
            )
            row = cursor.fetchone()
            if row is None or row[0] is None:
                return None
            return self.decode(row[0])

    def add(self, key: KeyT, value: Any, timeout: int | None) -> bool:
        """Set a value only if the key doesn't exist."""
        encoded = self.encode(value)
        bytea = self._to_bytea(encoded)

        if timeout == 0:
            # Check if it would have been set (key doesn't exist), but then delete
            with self._conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT 1 FROM {self._table} WHERE key = %s {self._expiry_filter()}",
                    [key],
                )
                return cursor.fetchone() is None

        expiry = self._make_expiry(timeout)
        with self._conn.cursor() as cursor:
            # Clean up expired entry first
            cursor.execute(
                f"DELETE FROM {self._table} WHERE key = %s AND expires_at IS NOT NULL AND expires_at <= NOW()",
                [key],
            )
            cursor.execute(
                f"""
                INSERT INTO {self._table} (key, type, value, expires_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (key) DO NOTHING
                """,
                [key, _TYPE_STRING, bytea, expiry],
            )
            return cursor.rowcount > 0

    def set_with_flags(  # noqa: PLR0911
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
    ) -> bool | Any:
        """Set a value with nx/xx/get flags."""
        if timeout == 0:
            if get:
                return None
            return False

        encoded = self.encode(value)
        bytea = self._to_bytea(encoded)
        expiry = self._make_expiry(timeout)

        with self._conn.cursor() as cursor:
            old_value = None
            if get:
                cursor.execute(
                    f"SELECT value FROM {self._table} WHERE key = %s AND type = %s {self._expiry_filter()}",
                    [key, _TYPE_STRING],
                )
                row = cursor.fetchone()
                old_value = row[0] if row else None

            if nx:
                # Clean up expired entry first
                cursor.execute(
                    f"DELETE FROM {self._table} WHERE key = %s AND expires_at IS NOT NULL AND expires_at <= NOW()",
                    [key],
                )
                self._delete_all_key_data(cursor, key)
                cursor.execute(
                    f"""
                    INSERT INTO {self._table} (key, type, value, expires_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (key) DO NOTHING
                    """,
                    [key, _TYPE_STRING, bytea, expiry],
                )
                if get:
                    return self.decode(old_value) if old_value is not None else None
                return cursor.rowcount > 0

            if xx:
                cursor.execute(
                    f"""
                    UPDATE {self._table}
                    SET type = %s, value = %s, expires_at = %s
                    WHERE key = %s {self._expiry_filter()}
                    """,
                    [_TYPE_STRING, bytea, expiry, key],
                )
                updated = cursor.rowcount > 0
                if updated:
                    self._delete_all_key_data(cursor, key)
                if get:
                    return self.decode(old_value) if old_value is not None else None
                return updated

            # Plain set
            self._delete_all_key_data(cursor, key)
            cursor.execute(
                f"""
                INSERT INTO {self._table} (key, type, value, expires_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (key)
                DO UPDATE SET type = EXCLUDED.type, value = EXCLUDED.value, expires_at = EXCLUDED.expires_at
                """,
                [key, _TYPE_STRING, bytea, expiry],
            )
            if get:
                return self.decode(old_value) if old_value is not None else None
            return True

    def delete(self, key: KeyT) -> bool:
        """Remove a key from the cache."""
        with self._conn.cursor() as cursor:
            self._delete_all_key_data(cursor, key)
            cursor.execute(f"DELETE FROM {self._table} WHERE key = %s", [key])
            return cursor.rowcount > 0

    def has_key(self, key: KeyT) -> bool:
        """Check if a key exists."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"SELECT 1 FROM {self._table} WHERE key = %s {self._expiry_filter()}",
                [key],
            )
            return cursor.fetchone() is not None

    def touch(self, key: KeyT, timeout: int | None) -> bool:
        """Update the timeout on a key."""
        if timeout is None:
            return self.persist(key)
        expiry = self._make_expiry(timeout)
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE {self._table} SET expires_at = %s WHERE key = %s {self._expiry_filter()}",
                [expiry, key],
            )
            return cursor.rowcount > 0

    def get_many(self, keys: Iterable[KeyT]) -> dict[KeyT, Any]:
        """Retrieve many keys."""
        keys = list(keys)
        if not keys:
            return {}

        with self._conn.cursor() as cursor:
            placeholders = ", ".join(["%s"] * len(keys))
            cursor.execute(
                f"SELECT key, value FROM {self._table} WHERE key IN ({placeholders}) AND type = %s {self._expiry_filter()}",
                [*keys, _TYPE_STRING],
            )
            result = {}
            for row in cursor.fetchall():
                if row[1] is not None:
                    result[row[0]] = self.decode(row[1])
            return result

    def set_many(self, data: Mapping[KeyT, Any], timeout: int | None) -> list:
        """Set multiple values."""
        if not data:
            return []

        if timeout == 0:
            self.delete_many(list(data.keys()))
            return []

        expiry = self._make_expiry(timeout)
        with self._conn.cursor() as cursor:
            for key, value in data.items():
                encoded = self.encode(value)
                bytea = self._to_bytea(encoded)
                self._delete_all_key_data(cursor, key)
                cursor.execute(
                    f"""
                    INSERT INTO {self._table} (key, type, value, expires_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (key)
                    DO UPDATE SET type = EXCLUDED.type, value = EXCLUDED.value, expires_at = EXCLUDED.expires_at
                    """,
                    [key, _TYPE_STRING, bytea, expiry],
                )
        return []

    def delete_many(self, keys: Sequence[KeyT]) -> int:
        """Remove multiple keys."""
        if not keys:
            return 0

        with self._conn.cursor() as cursor:
            placeholders = ", ".join(["%s"] * len(keys))
            for suffix in ("_hashes", "_lists", "_sets", "_zsets"):
                cursor.execute(
                    f"DELETE FROM {self._table}{suffix} WHERE key IN ({placeholders})",
                    list(keys),
                )
            cursor.execute(
                f"DELETE FROM {self._table} WHERE key IN ({placeholders})",
                list(keys),
            )
            return cursor.rowcount

    def incr(self, key: KeyT, delta: int = 1) -> int:
        """Increment a value."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"SELECT value FROM {self._table} WHERE key = %s AND type = %s {self._expiry_filter()} FOR UPDATE",
                [key, _TYPE_STRING],
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Key {key!r} not found")

            try:
                current = int(row[0])
            except (ValueError, TypeError) as e:
                raise ValueError(f"Value at key {key!r} is not an integer") from e

            new_val = current + delta
            cursor.execute(
                f"UPDATE {self._table} SET value = %s WHERE key = %s",
                [str(new_val).encode(), key],
            )
            return new_val

    def clear(self) -> bool:
        """Delete all keys from the cache tables."""
        with self._conn.cursor() as cursor:
            for suffix in ("_hashes", "_lists", "_sets", "_zsets"):
                cursor.execute(f"DELETE FROM {self._table}{suffix}")
            cursor.execute(f"DELETE FROM {self._table}")
        return True

    def close(self, **kwargs: Any) -> None:
        """No-op. Connections managed by Django."""

    async def aclose(self, **kwargs: Any) -> None:
        """No-op. Connections managed by Django."""

    def type(self, key: KeyT) -> KeyType | None:
        """Get the data type of a key."""
        with self._conn.cursor() as cursor:
            type_id = self._get_key_type(cursor, key)
            if type_id is None:
                return None
            return _TYPE_ID_TO_KEYTYPE.get(type_id)

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def ttl(self, key: KeyT) -> int | None:
        """Get TTL in seconds. Returns None if no expiry, -2 if key doesn't exist."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"SELECT expires_at FROM {self._table} WHERE key = %s {self._expiry_filter()}",
                [key],
            )
            row = cursor.fetchone()
            if row is None:
                return -2
            if row[0] is None:
                return None
            remaining = (row[0] - self._now()).total_seconds()
            return max(0, int(remaining))

    def pttl(self, key: KeyT) -> int | None:
        """Get TTL in milliseconds. Returns None if no expiry, -2 if key doesn't exist."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"SELECT expires_at FROM {self._table} WHERE key = %s {self._expiry_filter()}",
                [key],
            )
            row = cursor.fetchone()
            if row is None:
                return -2
            if row[0] is None:
                return None
            remaining = (row[0] - self._now()).total_seconds() * 1000
            return max(0, int(remaining))

    def expiretime(self, key: KeyT) -> int | None:
        """Get the absolute Unix timestamp (seconds) when a key will expire."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"SELECT expires_at FROM {self._table} WHERE key = %s {self._expiry_filter()}",
                [key],
            )
            row = cursor.fetchone()
            if row is None:
                return -2
            if row[0] is None:
                return None
            return int(row[0].timestamp())

    def expire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry on a key."""
        if isinstance(timeout, timedelta):
            timeout = int(timeout.total_seconds())
        expiry = self._now() + timedelta(seconds=timeout)
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE {self._table} SET expires_at = %s WHERE key = %s {self._expiry_filter()}",
                [expiry, key],
            )
            return cursor.rowcount > 0

    def pexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        """Set expiry in milliseconds."""
        if isinstance(timeout, timedelta):
            ms = timeout.total_seconds() * 1000
        else:
            ms = int(timeout)
        expiry = self._now() + timedelta(milliseconds=ms)
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE {self._table} SET expires_at = %s WHERE key = %s {self._expiry_filter()}",
                [expiry, key],
            )
            return cursor.rowcount > 0

    def expireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time."""
        if isinstance(when, int):
            expiry = datetime.fromtimestamp(when, tz=UTC)
        else:
            expiry = when
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE {self._table} SET expires_at = %s WHERE key = %s {self._expiry_filter()}",
                [expiry, key],
            )
            return cursor.rowcount > 0

    def pexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        """Set expiry at absolute time in milliseconds."""
        if isinstance(when, int):
            expiry = datetime.fromtimestamp(when / 1000.0, tz=UTC)
        else:
            expiry = when
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE {self._table} SET expires_at = %s WHERE key = %s {self._expiry_filter()}",
                [expiry, key],
            )
            return cursor.rowcount > 0

    def persist(self, key: KeyT) -> bool:
        """Remove expiry from a key."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE {self._table} SET expires_at = NULL WHERE key = %s {self._expiry_filter()}",
                [key],
            )
            return cursor.rowcount > 0

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str) -> list[str]:
        """Get all keys matching pattern."""
        sql_pattern, use_regex = self._glob_to_sql(pattern)
        with self._conn.cursor() as cursor:
            if use_regex:
                cursor.execute(
                    f"SELECT key FROM {self._table} WHERE key ~ %s {self._expiry_filter()} ORDER BY key",
                    [sql_pattern],
                )
            else:
                cursor.execute(
                    f"SELECT key FROM {self._table} WHERE key LIKE %s {self._expiry_filter()} ORDER BY key",
                    [sql_pattern],
                )
            return [row[0] for row in cursor.fetchall()]

    def iter_keys(self, pattern: str, itersize: int | None = None) -> Iterator[str]:
        """Iterate keys matching pattern."""
        if itersize is None:
            itersize = self._default_scan_itersize

        cursor_val = 0
        while True:
            next_cursor, batch = self.scan(cursor=cursor_val, match=pattern, count=itersize)
            yield from batch
            if next_cursor == 0:
                break
            cursor_val = next_cursor

    def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        """Perform a single SCAN iteration using OFFSET-based pagination."""
        if count is None:
            count = self._default_scan_itersize

        conditions = [self._expiry_filter().lstrip("AND ")]
        params: list[Any] = []

        if match is not None:
            sql_pattern, use_regex = self._glob_to_sql(match)
            if use_regex:
                conditions.append("key ~ %s")
            else:
                conditions.append("key LIKE %s")
            params.append(sql_pattern)

        if _type is not None:
            type_id = _KEYTYPE_TO_TYPE_ID.get(_type)
            if type_id is not None:
                conditions.append("type = %s")
                params.append(type_id)

        where = " AND ".join(conditions)
        params.extend([count, cursor])

        with self._conn.cursor() as db_cursor:
            db_cursor.execute(
                f"SELECT key FROM {self._table} WHERE {where} ORDER BY key LIMIT %s OFFSET %s",
                params,
            )
            keys = [row[0] for row in db_cursor.fetchall()]

        next_offset = cursor + len(keys)
        if len(keys) < count:
            next_offset = 0
        return next_offset, keys

    def delete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        """Delete all keys matching pattern."""
        if itersize is None:
            itersize = self._default_scan_itersize

        total = 0
        while True:
            # Always scan from offset 0 since deleting shifts OFFSET positions
            _, batch = self.scan(cursor=0, match=pattern, count=itersize)
            if not batch:
                break
            total += self.delete_many(batch)
        return total

    def rename(self, src: KeyT, dst: KeyT) -> bool:
        """Rename a key."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"SELECT type FROM {self._table} WHERE key = %s {self._expiry_filter()}",
                [src],
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Key {src!r} not found")

            # Delete destination and its data
            self._delete_all_key_data(cursor, dst)
            cursor.execute(f"DELETE FROM {self._table} WHERE key = %s", [dst])

            # Rename in auxiliary tables
            for suffix in ("_hashes", "_lists", "_sets", "_zsets"):
                cursor.execute(
                    f"UPDATE {self._table}{suffix} SET key = %s WHERE key = %s",
                    [dst, src],
                )

            # Rename in main table
            cursor.execute(
                f"UPDATE {self._table} SET key = %s WHERE key = %s",
                [dst, src],
            )
        return True

    def renamenx(self, src: KeyT, dst: KeyT) -> bool:
        """Rename a key only if the destination does not exist."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"SELECT type FROM {self._table} WHERE key = %s {self._expiry_filter()}",
                [src],
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Key {src!r} not found")

            # Check if dest exists
            cursor.execute(
                f"SELECT 1 FROM {self._table} WHERE key = %s {self._expiry_filter()}",
                [dst],
            )
            if cursor.fetchone() is not None:
                return False

            # Rename in auxiliary tables
            for suffix in ("_hashes", "_lists", "_sets", "_zsets"):
                cursor.execute(
                    f"UPDATE {self._table}{suffix} SET key = %s WHERE key = %s",
                    [dst, src],
                )

            # Rename in main table
            cursor.execute(
                f"UPDATE {self._table} SET key = %s WHERE key = %s",
                [dst, src],
            )
        return True

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        """Set hash field(s)."""
        pairs: list[tuple[str, Any]] = []

        if field is not None:
            pairs.append((field, value))
        if mapping:
            pairs.extend(mapping.items())
        if items:
            it = iter(items)
            for f in it:
                v = next(it)
                pairs.append((f, v))

        if not pairs:
            return 0

        count = 0
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, key, _TYPE_HASH)
            for f, v in pairs:
                encoded = self.encode(v)
                bytea = self._to_bytea(encoded)
                cursor.execute(
                    f"SELECT 1 FROM {self._table}_hashes WHERE key = %s AND field = %s",
                    [key, f],
                )
                existed = cursor.fetchone() is not None
                cursor.execute(
                    f"""
                    INSERT INTO {self._table}_hashes (key, field, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value
                    """,
                    [key, f, bytea],
                )
                if not existed:
                    count += 1
        return count

    def hsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        """Set a hash field only if it doesn't exist."""
        encoded = self.encode(value)
        bytea = self._to_bytea(encoded)
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, key, _TYPE_HASH)
            cursor.execute(
                f"""
                INSERT INTO {self._table}_hashes (key, field, value)
                VALUES (%s, %s, %s)
                ON CONFLICT (key, field) DO NOTHING
                """,
                [key, field, bytea],
            )
            return cursor.rowcount > 0

    def hget(self, key: KeyT, field: str) -> Any | None:
        """Get a hash field."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT h.value FROM {self._table}_hashes h
                INNER JOIN {self._table} m ON m.key = h.key
                WHERE h.key = %s AND h.field = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, field, _TYPE_HASH],
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return self.decode(row[0])

    def hmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        """Get multiple hash fields."""
        if not fields:
            return []
        with self._conn.cursor() as cursor:
            placeholders = ", ".join(["%s"] * len(fields))
            cursor.execute(
                f"""
                SELECT h.field, h.value FROM {self._table}_hashes h
                INNER JOIN {self._table} m ON m.key = h.key
                WHERE h.key = %s AND h.field IN ({placeholders}) AND m.type = %s {self._expiry_filter()}
                """,
                [key, *fields, _TYPE_HASH],
            )
            found = {row[0]: row[1] for row in cursor.fetchall()}
        return [self.decode(found[f]) if f in found else None for f in fields]

    def hgetall(self, key: KeyT) -> dict[str, Any]:
        """Get all hash fields."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT h.field, h.value FROM {self._table}_hashes h
                INNER JOIN {self._table} m ON m.key = h.key
                WHERE h.key = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, _TYPE_HASH],
            )
            return {row[0]: self.decode(row[1]) for row in cursor.fetchall()}

    def hdel(self, key: KeyT, *fields: str) -> int:
        """Delete hash fields."""
        if not fields:
            return 0
        with self._conn.cursor() as cursor:
            placeholders = ", ".join(["%s"] * len(fields))
            cursor.execute(
                f"DELETE FROM {self._table}_hashes WHERE key = %s AND field IN ({placeholders})",
                [key, *fields],
            )
            return cursor.rowcount

    def hexists(self, key: KeyT, field: str) -> bool:
        """Check if a hash field exists."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT 1 FROM {self._table}_hashes h
                INNER JOIN {self._table} m ON m.key = h.key
                WHERE h.key = %s AND h.field = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, field, _TYPE_HASH],
            )
            return cursor.fetchone() is not None

    def hlen(self, key: KeyT) -> int:
        """Get the number of fields in a hash."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {self._table}_hashes h
                INNER JOIN {self._table} m ON m.key = h.key
                WHERE h.key = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, _TYPE_HASH],
            )
            row = cursor.fetchone()
            return row[0] if row else 0

    def hkeys(self, key: KeyT) -> list[str]:
        """Get all field names in a hash."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT h.field FROM {self._table}_hashes h
                INNER JOIN {self._table} m ON m.key = h.key
                WHERE h.key = %s AND m.type = %s {self._expiry_filter()}
                ORDER BY h.field
                """,
                [key, _TYPE_HASH],
            )
            return [row[0] for row in cursor.fetchall()]

    def hvals(self, key: KeyT) -> list[Any]:
        """Get all values in a hash."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT h.value FROM {self._table}_hashes h
                INNER JOIN {self._table} m ON m.key = h.key
                WHERE h.key = %s AND m.type = %s {self._expiry_filter()}
                ORDER BY h.field
                """,
                [key, _TYPE_HASH],
            )
            return [self.decode(row[0]) for row in cursor.fetchall()]

    def hincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        """Increment a hash field by an integer."""
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, key, _TYPE_HASH)
            cursor.execute(
                f"SELECT value FROM {self._table}_hashes WHERE key = %s AND field = %s FOR UPDATE",
                [key, field],
            )
            row = cursor.fetchone()
            if row is None:
                new_val = amount
            else:
                current = int(self.decode(row[0]))
                new_val = current + amount
            encoded = self._to_bytea(self.encode(new_val))
            if row is None:
                cursor.execute(
                    f"INSERT INTO {self._table}_hashes (key, field, value) VALUES (%s, %s, %s)",
                    [key, field, encoded],
                )
            else:
                cursor.execute(
                    f"UPDATE {self._table}_hashes SET value = %s WHERE key = %s AND field = %s",
                    [encoded, key, field],
                )
            return new_val

    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0) -> float:
        """Increment a hash field by a float."""
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, key, _TYPE_HASH)
            cursor.execute(
                f"SELECT value FROM {self._table}_hashes WHERE key = %s AND field = %s FOR UPDATE",
                [key, field],
            )
            row = cursor.fetchone()
            if row is None:
                new_val = amount
            else:
                current = float(self.decode(row[0]))
                new_val = current + amount
            encoded = self._to_bytea(self.encode(new_val))
            if row is None:
                cursor.execute(
                    f"INSERT INTO {self._table}_hashes (key, field, value) VALUES (%s, %s, %s)",
                    [key, field, encoded],
                )
            else:
                cursor.execute(
                    f"UPDATE {self._table}_hashes SET value = %s WHERE key = %s AND field = %s",
                    [encoded, key, field],
                )
            return new_val

    # =========================================================================
    # List Operations
    # =========================================================================

    def _list_bounds(self, cursor: Any, key: KeyT) -> tuple[int | None, int | None]:
        """Get min and max positions for a list key."""
        cursor.execute(
            f"""
            SELECT MIN(l.pos), MAX(l.pos) FROM {self._table}_lists l
            INNER JOIN {self._table} m ON m.key = l.key
            WHERE l.key = %s AND m.type = %s {self._expiry_filter()}
            """,
            [key, _TYPE_LIST],
        )
        row = cursor.fetchone()
        if row is None or row[0] is None:
            return None, None
        return row[0], row[1]

    def _list_positions(self, cursor: Any, key: KeyT) -> list[int]:
        """Get all positions in order for a list key."""
        cursor.execute(
            f"""
            SELECT l.pos FROM {self._table}_lists l
            INNER JOIN {self._table} m ON m.key = l.key
            WHERE l.key = %s AND m.type = %s {self._expiry_filter()}
            ORDER BY l.pos
            """,
            [key, _TYPE_LIST],
        )
        return [row[0] for row in cursor.fetchall()]

    def _resolve_list_index(self, positions: list[int], index: int) -> int | None:
        """Resolve a Redis-style list index (supports negative) to a position value."""
        if not positions:
            return None
        if index < 0:
            index = len(positions) + index
        if index < 0 or index >= len(positions):
            return None
        return positions[index]

    def lpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the left of a list."""
        if not values:
            return 0
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, key, _TYPE_LIST)
            min_pos, _ = self._list_bounds(cursor, key)
            current_pos = (min_pos - 1) if min_pos is not None else 0
            for v in values:
                encoded = self.encode(v)
                bytea = self._to_bytea(encoded)
                cursor.execute(
                    f"INSERT INTO {self._table}_lists (key, pos, value) VALUES (%s, %s, %s)",
                    [key, current_pos, bytea],
                )
                current_pos -= 1
            cursor.execute(
                f"SELECT COUNT(*) FROM {self._table}_lists WHERE key = %s",
                [key],
            )
            return cursor.fetchone()[0]

    def rpush(self, key: KeyT, *values: Any) -> int:
        """Push values to the right of a list."""
        if not values:
            return 0
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, key, _TYPE_LIST)
            _, max_pos = self._list_bounds(cursor, key)
            current_pos = (max_pos + 1) if max_pos is not None else 0
            for v in values:
                encoded = self.encode(v)
                bytea = self._to_bytea(encoded)
                cursor.execute(
                    f"INSERT INTO {self._table}_lists (key, pos, value) VALUES (%s, %s, %s)",
                    [key, current_pos, bytea],
                )
                current_pos += 1
            cursor.execute(
                f"SELECT COUNT(*) FROM {self._table}_lists WHERE key = %s",
                [key],
            )
            return cursor.fetchone()[0]

    def lpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the left of a list."""
        with self._conn.cursor() as cursor:
            # Check key exists and is not expired
            cursor.execute(
                f"SELECT 1 FROM {self._table} WHERE key = %s AND type = %s {self._expiry_filter()}",
                [key, _TYPE_LIST],
            )
            if cursor.fetchone() is None:
                return [] if count is not None else None

            if count is not None:
                cursor.execute(
                    f"SELECT pos, value FROM {self._table}_lists WHERE key = %s ORDER BY pos LIMIT %s",
                    [key, count],
                )
                rows = cursor.fetchall()
                if not rows:
                    return []
                positions = [row[0] for row in rows]
                values = [self.decode(row[1]) for row in rows]
                placeholders = ", ".join(["%s"] * len(positions))
                cursor.execute(
                    f"DELETE FROM {self._table}_lists WHERE key = %s AND pos IN ({placeholders})",
                    [key, *positions],
                )
                return values
            cursor.execute(
                f"SELECT pos, value FROM {self._table}_lists WHERE key = %s ORDER BY pos LIMIT 1",
                [key],
            )
            row = cursor.fetchone()
            if row is None:
                return None
            cursor.execute(
                f"DELETE FROM {self._table}_lists WHERE key = %s AND pos = %s",
                [key, row[0]],
            )
            return self.decode(row[1])

    def rpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Pop value(s) from the right of a list."""
        with self._conn.cursor() as cursor:
            # Check key exists and is not expired
            cursor.execute(
                f"SELECT 1 FROM {self._table} WHERE key = %s AND type = %s {self._expiry_filter()}",
                [key, _TYPE_LIST],
            )
            if cursor.fetchone() is None:
                return [] if count is not None else None

            if count is not None:
                cursor.execute(
                    f"SELECT pos, value FROM {self._table}_lists WHERE key = %s ORDER BY pos DESC LIMIT %s",
                    [key, count],
                )
                rows = cursor.fetchall()
                if not rows:
                    return []
                positions = [row[0] for row in rows]
                # Reverse to get left-to-right order (rightmost popped first = reversed)
                values = [self.decode(row[1]) for row in rows]
                placeholders = ", ".join(["%s"] * len(positions))
                cursor.execute(
                    f"DELETE FROM {self._table}_lists WHERE key = %s AND pos IN ({placeholders})",
                    [key, *positions],
                )
                return values
            cursor.execute(
                f"SELECT pos, value FROM {self._table}_lists WHERE key = %s ORDER BY pos DESC LIMIT 1",
                [key],
            )
            row = cursor.fetchone()
            if row is None:
                return None
            cursor.execute(
                f"DELETE FROM {self._table}_lists WHERE key = %s AND pos = %s",
                [key, row[0]],
            )
            return self.decode(row[1])

    def lrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        """Get a range of elements from a list."""
        with self._conn.cursor() as cursor:
            positions = self._list_positions(cursor, key)
            if not positions:
                return []

            length = len(positions)
            # Resolve negative indices
            if start < 0:
                start = length + start
            if end < 0:
                end = length + end
            start = max(0, start)
            end = min(length - 1, end)

            if start > end:
                return []

            selected_positions = positions[start : end + 1]
            if not selected_positions:
                return []

            placeholders = ", ".join(["%s"] * len(selected_positions))
            cursor.execute(
                f"SELECT pos, value FROM {self._table}_lists WHERE key = %s AND pos IN ({placeholders}) ORDER BY pos",
                [key, *selected_positions],
            )
            return [self.decode(row[1]) for row in cursor.fetchall()]

    def lindex(self, key: KeyT, index: int) -> Any | None:
        """Get an element from a list by index."""
        with self._conn.cursor() as cursor:
            positions = self._list_positions(cursor, key)
            pos = self._resolve_list_index(positions, index)
            if pos is None:
                return None
            cursor.execute(
                f"SELECT value FROM {self._table}_lists WHERE key = %s AND pos = %s",
                [key, pos],
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return self.decode(row[0])

    def llen(self, key: KeyT) -> int:
        """Get the length of a list."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {self._table}_lists l
                INNER JOIN {self._table} m ON m.key = l.key
                WHERE l.key = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, _TYPE_LIST],
            )
            return cursor.fetchone()[0]

    def lpos(  # noqa: C901, PLR0911, PLR0912
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list."""
        encoded = self.encode(value)
        bytea = self._to_bytea(encoded)

        with self._conn.cursor() as cursor:
            positions = self._list_positions(cursor, key)
            if not positions:
                if count is not None:
                    return []
                return None

            # Fetch all values in order
            if maxlen is not None:
                scan_positions = positions[:maxlen]
            else:
                scan_positions = positions

            if not scan_positions:
                if count is not None:
                    return []
                return None

            placeholders = ", ".join(["%s"] * len(scan_positions))
            cursor.execute(
                f"SELECT pos, value FROM {self._table}_lists WHERE key = %s AND pos IN ({placeholders}) ORDER BY pos",
                [key, *scan_positions],
            )
            rows = cursor.fetchall()

        # Build position->index mapping
        pos_to_idx = {p: i for i, p in enumerate(positions)}

        # Find matching indices
        matching_indices: list[int] = []
        for pos, val in rows:
            if bytes(val) if isinstance(val, memoryview) else val == bytea:
                matching_indices.append(pos_to_idx[pos])

        if not matching_indices:
            if count is not None:
                return []
            return None

        # Apply rank
        if rank is not None:
            if rank > 0:
                idx = rank - 1
                if idx >= len(matching_indices):
                    if count is not None:
                        return []
                    return None
                matching_indices = matching_indices[idx:]
            elif rank < 0:
                idx = len(matching_indices) + rank
                if idx < 0:
                    if count is not None:
                        return []
                    return None
                matching_indices = matching_indices[: idx + 1]
                matching_indices.reverse()

        if count is not None:
            if count == 0:
                return matching_indices
            return matching_indices[:count]

        return matching_indices[0] if matching_indices else None

    def lmove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
    ) -> Any | None:
        """Atomically move an element from one list to another."""
        with self._conn.cursor() as cursor:
            # Pop from source
            if wherefrom.upper() == "LEFT":
                cursor.execute(
                    f"SELECT pos, value FROM {self._table}_lists WHERE key = %s ORDER BY pos LIMIT 1",
                    [src],
                )
            else:
                cursor.execute(
                    f"SELECT pos, value FROM {self._table}_lists WHERE key = %s ORDER BY pos DESC LIMIT 1",
                    [src],
                )
            row = cursor.fetchone()
            if row is None:
                return None

            pos, val = row
            cursor.execute(
                f"DELETE FROM {self._table}_lists WHERE key = %s AND pos = %s",
                [src, pos],
            )

            # Push to destination
            self._ensure_key(cursor, dst, _TYPE_LIST)
            if whereto.upper() == "LEFT":
                min_pos, _ = self._list_bounds(cursor, dst)
                new_pos = (min_pos - 1) if min_pos is not None else 0
            else:
                _, max_pos = self._list_bounds(cursor, dst)
                new_pos = (max_pos + 1) if max_pos is not None else 0

            bytea = bytes(val) if isinstance(val, memoryview) else val
            cursor.execute(
                f"INSERT INTO {self._table}_lists (key, pos, value) VALUES (%s, %s, %s)",
                [dst, new_pos, bytea],
            )
            return self.decode(val)

    def lrem(self, key: KeyT, count: int, value: Any) -> int:
        """Remove elements from a list."""
        encoded = self.encode(value)
        bytea = self._to_bytea(encoded)

        with self._conn.cursor() as cursor:
            if count == 0:
                # Remove all occurrences
                cursor.execute(
                    f"DELETE FROM {self._table}_lists WHERE key = %s AND value = %s",
                    [key, bytea],
                )
                return cursor.rowcount
            if count > 0:
                # Remove first `count` occurrences (from head)
                cursor.execute(
                    f"""
                    DELETE FROM {self._table}_lists
                    WHERE (key, pos) IN (
                        SELECT key, pos FROM {self._table}_lists
                        WHERE key = %s AND value = %s
                        ORDER BY pos LIMIT %s
                    )
                    """,
                    [key, bytea, count],
                )
                return cursor.rowcount
            # Remove last |count| occurrences (from tail)
            cursor.execute(
                f"""
                    DELETE FROM {self._table}_lists
                    WHERE (key, pos) IN (
                        SELECT key, pos FROM {self._table}_lists
                        WHERE key = %s AND value = %s
                        ORDER BY pos DESC LIMIT %s
                    )
                    """,
                [key, bytea, -count],
            )
            return cursor.rowcount

    def ltrim(self, key: KeyT, start: int, end: int) -> bool:
        """Trim a list to the specified range."""
        with self._conn.cursor() as cursor:
            positions = self._list_positions(cursor, key)
            if not positions:
                return True

            length = len(positions)
            if start < 0:
                start = length + start
            if end < 0:
                end = length + end
            start = max(0, start)
            end = min(length - 1, end)

            if start > end:
                # Delete everything
                cursor.execute(
                    f"DELETE FROM {self._table}_lists WHERE key = %s",
                    [key],
                )
                return True

            keep_positions = set(positions[start : end + 1])
            delete_positions = [p for p in positions if p not in keep_positions]

            if delete_positions:
                placeholders = ", ".join(["%s"] * len(delete_positions))
                cursor.execute(
                    f"DELETE FROM {self._table}_lists WHERE key = %s AND pos IN ({placeholders})",
                    [key, *delete_positions],
                )
        return True

    def lset(self, key: KeyT, index: int, value: Any) -> bool:
        """Set an element in a list by index."""
        encoded = self.encode(value)
        bytea = self._to_bytea(encoded)

        with self._conn.cursor() as cursor:
            positions = self._list_positions(cursor, key)
            pos = self._resolve_list_index(positions, index)
            if pos is None:
                raise IndexError("list index out of range")
            cursor.execute(
                f"UPDATE {self._table}_lists SET value = %s WHERE key = %s AND pos = %s",
                [bytea, key, pos],
            )
        return True

    def linsert(
        self,
        key: KeyT,
        where: str,
        pivot: Any,
        value: Any,
    ) -> int:
        """Insert an element before or after another element."""
        encoded_pivot = self.encode(pivot)
        bytea_pivot = self._to_bytea(encoded_pivot)
        encoded_value = self.encode(value)
        bytea_value = self._to_bytea(encoded_value)

        with self._conn.cursor() as cursor:
            # Check key exists and is not expired
            cursor.execute(
                f"SELECT 1 FROM {self._table} WHERE key = %s AND type = %s {self._expiry_filter()}",
                [key, _TYPE_LIST],
            )
            if cursor.fetchone() is None:
                return 0

            # Find the pivot position
            cursor.execute(
                f"SELECT pos FROM {self._table}_lists WHERE key = %s AND value = %s ORDER BY pos LIMIT 1",
                [key, bytea_pivot],
            )
            row = cursor.fetchone()
            if row is None:
                return -1

            pivot_pos = row[0]

            if where.upper() == "BEFORE":
                # Insert before: find a position between the previous element and pivot
                cursor.execute(
                    f"SELECT MAX(pos) FROM {self._table}_lists WHERE key = %s AND pos < %s",
                    [key, pivot_pos],
                )
                prev_row = cursor.fetchone()
                if prev_row[0] is None:
                    new_pos = pivot_pos - 1
                else:
                    new_pos = prev_row[0] + 1
                    if new_pos >= pivot_pos:
                        # Need to make room: shift pivot and everything after up
                        cursor.execute(
                            f"UPDATE {self._table}_lists SET pos = pos + 1 WHERE key = %s AND pos >= %s",
                            [key, pivot_pos],
                        )
                        new_pos = pivot_pos
            else:
                # Insert after: find a position between pivot and next element
                cursor.execute(
                    f"SELECT MIN(pos) FROM {self._table}_lists WHERE key = %s AND pos > %s",
                    [key, pivot_pos],
                )
                next_row = cursor.fetchone()
                if next_row[0] is None:
                    new_pos = pivot_pos + 1
                else:
                    new_pos = pivot_pos + 1
                    if new_pos >= next_row[0]:
                        # Need to make room: shift everything after pivot up
                        cursor.execute(
                            f"UPDATE {self._table}_lists SET pos = pos + 1 WHERE key = %s AND pos > %s",
                            [key, pivot_pos],
                        )
                        new_pos = pivot_pos + 1

            cursor.execute(
                f"INSERT INTO {self._table}_lists (key, pos, value) VALUES (%s, %s, %s)",
                [key, new_pos, bytea_value],
            )
            cursor.execute(
                f"SELECT COUNT(*) FROM {self._table}_lists WHERE key = %s",
                [key],
            )
            return cursor.fetchone()[0]

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(self, key: KeyT, *members: Any) -> int:
        """Add members to a set."""
        if not members:
            return 0
        count = 0
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, key, _TYPE_SET)
            for m in members:
                encoded = self.encode(m)
                bytea = self._to_bytea(encoded)
                cursor.execute(
                    f"""
                    INSERT INTO {self._table}_sets (key, member)
                    VALUES (%s, %s)
                    ON CONFLICT (key, member) DO NOTHING
                    """,
                    [key, bytea],
                )
                count += cursor.rowcount
        return count

    def scard(self, key: KeyT) -> int:
        """Get the number of members in a set."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {self._table}_sets s
                INNER JOIN {self._table} m ON m.key = s.key
                WHERE s.key = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, _TYPE_SET],
            )
            return cursor.fetchone()[0]

    def sismember(self, key: KeyT, member: Any) -> bool:
        """Check if a value is a member of a set."""
        encoded = self.encode(member)
        bytea = self._to_bytea(encoded)
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT 1 FROM {self._table}_sets s
                INNER JOIN {self._table} m ON m.key = s.key
                WHERE s.key = %s AND s.member = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, bytea, _TYPE_SET],
            )
            return cursor.fetchone() is not None

    def smismember(self, key: KeyT, *members: Any) -> list[bool]:
        """Check if multiple values are members of a set."""
        if not members:
            return []
        results = []
        with self._conn.cursor() as cursor:
            for m in members:
                encoded = self.encode(m)
                bytea = self._to_bytea(encoded)
                cursor.execute(
                    f"""
                    SELECT 1 FROM {self._table}_sets s
                    INNER JOIN {self._table} m ON m.key = s.key
                    WHERE s.key = %s AND s.member = %s AND m.type = %s {self._expiry_filter()}
                    """,
                    [key, bytea, _TYPE_SET],
                )
                results.append(cursor.fetchone() is not None)
        return results

    def smembers(self, key: KeyT) -> builtins.set[Any]:
        """Get all members of a set."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT s.member FROM {self._table}_sets s
                INNER JOIN {self._table} m ON m.key = s.key
                WHERE s.key = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, _TYPE_SET],
            )
            return {self.decode(row[0]) for row in cursor.fetchall()}

    def srandmember(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Get random member(s) from a set."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT s.member FROM {self._table}_sets s
                INNER JOIN {self._table} m ON m.key = s.key
                WHERE s.key = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, _TYPE_SET],
            )
            rows = cursor.fetchall()

        if not rows:
            if count is not None:
                return []
            return None

        members = [row[0] for row in rows]

        if count is None:
            return self.decode(random.choice(members))  # noqa: S311

        if count >= 0:
            sample = random.sample(members, min(count, len(members)))
            return [self.decode(m) for m in sample]
        # Negative count: allow duplicates
        result = random.choices(members, k=-count)  # noqa: S311
        return [self.decode(m) for m in result]

    def spop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        """Remove and return random member(s) from a set."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT s.member FROM {self._table}_sets s
                INNER JOIN {self._table} m ON m.key = s.key
                WHERE s.key = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, _TYPE_SET],
            )
            rows = cursor.fetchall()

        if not rows:
            if count is not None:
                return []
            return None

        members = [row[0] for row in rows]

        if count is None:
            chosen = random.choice(members)  # noqa: S311
            with self._conn.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM {self._table}_sets WHERE key = %s AND member = %s",
                    [key, chosen],
                )
            return self.decode(chosen)

        selected = random.sample(members, min(count, len(members)))
        if selected:
            with self._conn.cursor() as cursor:
                placeholders = ", ".join(["%s"] * len(selected))
                cursor.execute(
                    f"DELETE FROM {self._table}_sets WHERE key = %s AND member IN ({placeholders})",
                    [key, *selected],
                )
        return [self.decode(m) for m in selected]

    def srem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a set."""
        if not members:
            return 0
        count = 0
        with self._conn.cursor() as cursor:
            for m in members:
                encoded = self.encode(m)
                bytea = self._to_bytea(encoded)
                cursor.execute(
                    f"DELETE FROM {self._table}_sets WHERE key = %s AND member = %s",
                    [key, bytea],
                )
                count += cursor.rowcount
        return count

    def smove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        """Move a member from one set to another."""
        encoded = self.encode(member)
        bytea = self._to_bytea(encoded)
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {self._table}_sets WHERE key = %s AND member = %s",
                [src, bytea],
            )
            if cursor.rowcount == 0:
                return False
            self._ensure_key(cursor, dst, _TYPE_SET)
            cursor.execute(
                f"""
                INSERT INTO {self._table}_sets (key, member)
                VALUES (%s, %s)
                ON CONFLICT (key, member) DO NOTHING
                """,
                [dst, bytea],
            )
        return True

    def _set_op_members(self, cursor: Any, keys: KeyT | Sequence[KeyT]) -> list[builtins.set[bytes]]:
        """Fetch set members for multiple keys, returned as list of byte-sets."""
        if isinstance(keys, (str, bytes, memoryview)):
            keys = [keys]
        result = []
        for k in keys:
            cursor.execute(
                f"""
                SELECT s.member FROM {self._table}_sets s
                INNER JOIN {self._table} m ON m.key = s.key
                WHERE s.key = %s AND m.type = %s {self._expiry_filter()}
                """,
                [k, _TYPE_SET],
            )
            members = set()
            for row in cursor.fetchall():
                val = row[0]
                members.add(bytes(val) if isinstance(val, memoryview) else val)
            result.append(members)
        return result

    def sdiff(self, keys: KeyT | Sequence[KeyT]) -> builtins.set[Any]:
        """Return the difference of sets."""
        with self._conn.cursor() as cursor:
            sets = self._set_op_members(cursor, keys)
        if not sets:
            return set()
        result = sets[0]
        for s in sets[1:]:
            result = result - s
        return {self.decode(v) for v in result}

    def sdiffstore(self, dest: KeyT, keys: KeyT | Sequence[KeyT]) -> int:
        """Store the difference of sets."""
        diff = self.sdiff(keys)
        # Store result
        self.delete(dest)
        if not diff:
            return 0
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, dest, _TYPE_SET)
            for member in diff:
                encoded = self.encode(member)
                bytea = self._to_bytea(encoded)
                cursor.execute(
                    f"INSERT INTO {self._table}_sets (key, member) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    [dest, bytea],
                )
        return len(diff)

    def sinter(self, keys: KeyT | Sequence[KeyT]) -> builtins.set[Any]:
        """Return the intersection of sets."""
        with self._conn.cursor() as cursor:
            sets = self._set_op_members(cursor, keys)
        if not sets:
            return set()
        result = sets[0]
        for s in sets[1:]:
            result = result & s
        return {self.decode(v) for v in result}

    def sinterstore(self, dest: KeyT, keys: KeyT | Sequence[KeyT]) -> int:
        """Store the intersection of sets."""
        inter = self.sinter(keys)
        self.delete(dest)
        if not inter:
            return 0
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, dest, _TYPE_SET)
            for member in inter:
                encoded = self.encode(member)
                bytea = self._to_bytea(encoded)
                cursor.execute(
                    f"INSERT INTO {self._table}_sets (key, member) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    [dest, bytea],
                )
        return len(inter)

    def sunion(self, keys: KeyT | Sequence[KeyT]) -> builtins.set[Any]:
        """Return the union of sets."""
        with self._conn.cursor() as cursor:
            sets = self._set_op_members(cursor, keys)
        if not sets:
            return set()
        result: set[bytes] = set()
        for s in sets:
            result = result | s
        return {self.decode(v) for v in result}

    def sunionstore(self, dest: KeyT, keys: KeyT | Sequence[KeyT]) -> int:
        """Store the union of sets."""
        union = self.sunion(keys)
        self.delete(dest)
        if not union:
            return 0
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, dest, _TYPE_SET)
            for member in union:
                encoded = self.encode(member)
                bytea = self._to_bytea(encoded)
                cursor.execute(
                    f"INSERT INTO {self._table}_sets (key, member) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    [dest, bytea],
                )
        return len(union)

    # =========================================================================
    # Sorted Set Operations
    # =========================================================================

    def zadd(  # noqa: C901
        self,
        key: KeyT,
        mapping: Mapping[Any, float],
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
    ) -> int:
        """Add members to a sorted set."""
        if not mapping:
            return 0

        count = 0
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, key, _TYPE_ZSET)
            for member, score in mapping.items():
                encoded = self.encode(member)
                bytea = self._to_bytea(encoded)

                # Check if member exists
                cursor.execute(
                    f"SELECT score FROM {self._table}_zsets WHERE key = %s AND member = %s FOR UPDATE",
                    [key, bytea],
                )
                row = cursor.fetchone()

                if row is None:
                    # Member doesn't exist
                    if xx:
                        continue
                    cursor.execute(
                        f"INSERT INTO {self._table}_zsets (key, member, score) VALUES (%s, %s, %s)",
                        [key, bytea, score],
                    )
                    count += 1
                else:
                    # Member exists
                    if nx:
                        continue
                    old_score = row[0]
                    new_score = score
                    should_update = True

                    if gt and lt:
                        should_update = False
                    elif gt:
                        should_update = new_score > old_score
                    elif lt:
                        should_update = new_score < old_score

                    if should_update:
                        cursor.execute(
                            f"UPDATE {self._table}_zsets SET score = %s WHERE key = %s AND member = %s",
                            [new_score, key, bytea],
                        )
                        if ch and old_score != new_score:
                            count += 1
                    # If ch and not updated, don't count
        return count

    def zcard(self, key: KeyT) -> int:
        """Get the number of members in a sorted set."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {self._table}_zsets z
                INNER JOIN {self._table} m ON m.key = z.key
                WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, _TYPE_ZSET],
            )
            return cursor.fetchone()[0]

    def zcount(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Count members in a score range."""
        min_inclusive, min_val = _parse_score_bound(min_score)
        max_inclusive, max_val = _parse_score_bound(max_score)

        min_op = ">=" if min_inclusive else ">"
        max_op = "<=" if max_inclusive else "<"

        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {self._table}_zsets z
                INNER JOIN {self._table} m ON m.key = z.key
                WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
                AND z.score {min_op} %s AND z.score {max_op} %s
                """,
                [key, _TYPE_ZSET, min_val, max_val],
            )
            return cursor.fetchone()[0]

    def zincrby(self, key: KeyT, amount: float, member: Any) -> float:
        """Increment the score of a member."""
        encoded = self.encode(member)
        bytea = self._to_bytea(encoded)
        with self._conn.cursor() as cursor:
            self._ensure_key(cursor, key, _TYPE_ZSET)
            cursor.execute(
                f"SELECT score FROM {self._table}_zsets WHERE key = %s AND member = %s FOR UPDATE",
                [key, bytea],
            )
            row = cursor.fetchone()
            if row is None:
                cursor.execute(
                    f"INSERT INTO {self._table}_zsets (key, member, score) VALUES (%s, %s, %s)",
                    [key, bytea, amount],
                )
                return float(amount)
            new_score = row[0] + amount
            cursor.execute(
                f"UPDATE {self._table}_zsets SET score = %s WHERE key = %s AND member = %s",
                [new_score, key, bytea],
            )
            return float(new_score)

    def zpopmin(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT z.member, z.score FROM {self._table}_zsets z
                INNER JOIN {self._table} m ON m.key = z.key
                WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
                ORDER BY z.score, z.member LIMIT %s
                """,
                [key, _TYPE_ZSET, count],
            )
            rows = cursor.fetchall()
            if not rows:
                return []
            for member, _score in rows:
                cursor.execute(
                    f"DELETE FROM {self._table}_zsets WHERE key = %s AND member = %s",
                    [key, member],
                )
            return [(self.decode(m), float(s)) for m, s in rows]

    def zpopmax(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT z.member, z.score FROM {self._table}_zsets z
                INNER JOIN {self._table} m ON m.key = z.key
                WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
                ORDER BY z.score DESC, z.member DESC LIMIT %s
                """,
                [key, _TYPE_ZSET, count],
            )
            rows = cursor.fetchall()
            if not rows:
                return []
            for member, _score in rows:
                cursor.execute(
                    f"DELETE FROM {self._table}_zsets WHERE key = %s AND member = %s",
                    [key, member],
                )
            return [(self.decode(m), float(s)) for m, s in rows]

    def zrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get a range of members by index."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT member, score FROM {self._table}_zsets z
                INNER JOIN {self._table} m ON m.key = z.key
                WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
                ORDER BY z.score, z.member
                """,
                [key, _TYPE_ZSET],
            )
            all_rows = cursor.fetchall()

        if not all_rows:
            return []

        length = len(all_rows)
        if start < 0:
            start = length + start
        if end < 0:
            end = length + end
        start = max(0, start)
        end = min(length - 1, end)

        if start > end:
            return []

        selected = all_rows[start : end + 1]
        if withscores:
            return [(self.decode(m), float(s)) for m, s in selected]
        return [self.decode(m) for m, s in selected]

    def zrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get a range of members by index, reversed."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT member, score FROM {self._table}_zsets z
                INNER JOIN {self._table} m ON m.key = z.key
                WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
                ORDER BY z.score DESC, z.member DESC
                """,
                [key, _TYPE_ZSET],
            )
            all_rows = cursor.fetchall()

        if not all_rows:
            return []

        length = len(all_rows)
        if start < 0:
            start = length + start
        if end < 0:
            end = length + end
        start = max(0, start)
        end = min(length - 1, end)

        if start > end:
            return []

        selected = all_rows[start : end + 1]
        if withscores:
            return [(self.decode(m), float(s)) for m, s in selected]
        return [self.decode(m) for m, s in selected]

    def zrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        *,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get members by score range."""
        min_inclusive, min_val = _parse_score_bound(min_score)
        max_inclusive, max_val = _parse_score_bound(max_score)

        min_op = ">=" if min_inclusive else ">"
        max_op = "<=" if max_inclusive else "<"

        sql = f"""
            SELECT z.member, z.score FROM {self._table}_zsets z
            INNER JOIN {self._table} m ON m.key = z.key
            WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
            AND z.score {min_op} %s AND z.score {max_op} %s
            ORDER BY z.score, z.member
        """
        params: list[Any] = [key, _TYPE_ZSET, min_val, max_val]

        if start is not None and num is not None:
            sql += " LIMIT %s OFFSET %s"
            params.extend([num, start])

        with self._conn.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()

        if withscores:
            return [(self.decode(m), float(s)) for m, s in rows]
        return [self.decode(m) for m, s in rows]

    def zrevrangebyscore(
        self,
        key: KeyT,
        max_score: float | str,
        min_score: float | str,
        *,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Get members by score range, reversed."""
        min_inclusive, min_val = _parse_score_bound(min_score)
        max_inclusive, max_val = _parse_score_bound(max_score)

        min_op = ">=" if min_inclusive else ">"
        max_op = "<=" if max_inclusive else "<"

        sql = f"""
            SELECT z.member, z.score FROM {self._table}_zsets z
            INNER JOIN {self._table} m ON m.key = z.key
            WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
            AND z.score {min_op} %s AND z.score {max_op} %s
            ORDER BY z.score DESC, z.member DESC
        """
        params: list[Any] = [key, _TYPE_ZSET, min_val, max_val]

        if start is not None and num is not None:
            sql += " LIMIT %s OFFSET %s"
            params.extend([num, start])

        with self._conn.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()

        if withscores:
            return [(self.decode(m), float(s)) for m, s in rows]
        return [self.decode(m) for m, s in rows]

    def zrank(self, key: KeyT, member: Any) -> int | None:
        """Get the rank of a member (0-based)."""
        encoded = self.encode(member)
        bytea = self._to_bytea(encoded)
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT member, score FROM {self._table}_zsets z
                INNER JOIN {self._table} m ON m.key = z.key
                WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
                ORDER BY z.score, z.member
                """,
                [key, _TYPE_ZSET],
            )
            for i, row in enumerate(cursor.fetchall()):
                raw_member = bytes(row[0]) if isinstance(row[0], memoryview) else row[0]
                if raw_member == bytea:
                    return i
        return None

    def zrevrank(self, key: KeyT, member: Any) -> int | None:
        """Get the reverse rank of a member."""
        encoded = self.encode(member)
        bytea = self._to_bytea(encoded)
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT member, score FROM {self._table}_zsets z
                INNER JOIN {self._table} m ON m.key = z.key
                WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
                ORDER BY z.score DESC, z.member DESC
                """,
                [key, _TYPE_ZSET],
            )
            for i, row in enumerate(cursor.fetchall()):
                raw_member = bytes(row[0]) if isinstance(row[0], memoryview) else row[0]
                if raw_member == bytea:
                    return i
        return None

    def zrem(self, key: KeyT, *members: Any) -> int:
        """Remove members from a sorted set."""
        if not members:
            return 0
        count = 0
        with self._conn.cursor() as cursor:
            for m in members:
                encoded = self.encode(m)
                bytea = self._to_bytea(encoded)
                cursor.execute(
                    f"DELETE FROM {self._table}_zsets WHERE key = %s AND member = %s",
                    [key, bytea],
                )
                count += cursor.rowcount
        return count

    def zremrangebyscore(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        """Remove members by score range."""
        min_inclusive, min_val = _parse_score_bound(min_score)
        max_inclusive, max_val = _parse_score_bound(max_score)

        min_op = ">=" if min_inclusive else ">"
        max_op = "<=" if max_inclusive else "<"

        with self._conn.cursor() as cursor:
            # Check key exists and is not expired
            cursor.execute(
                f"SELECT 1 FROM {self._table} WHERE key = %s AND type = %s {self._expiry_filter()}",
                [key, _TYPE_ZSET],
            )
            if cursor.fetchone() is None:
                return 0
            cursor.execute(
                f"""
                DELETE FROM {self._table}_zsets
                WHERE key = %s AND score {min_op} %s AND score {max_op} %s
                """,
                [key, min_val, max_val],
            )
            return cursor.rowcount

    def zremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        """Remove members by rank range."""
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT z.member, z.score FROM {self._table}_zsets z
                INNER JOIN {self._table} m ON m.key = z.key
                WHERE z.key = %s AND m.type = %s {self._expiry_filter()}
                ORDER BY z.score, z.member
                """,
                [key, _TYPE_ZSET],
            )
            all_rows = cursor.fetchall()

        if not all_rows:
            return 0

        length = len(all_rows)
        if start < 0:
            start = length + start
        if end < 0:
            end = length + end
        start = max(0, start)
        end = min(length - 1, end)

        if start > end:
            return 0

        to_remove = all_rows[start : end + 1]
        with self._conn.cursor() as cursor:
            for member, _score in to_remove:
                cursor.execute(
                    f"DELETE FROM {self._table}_zsets WHERE key = %s AND member = %s",
                    [key, member],
                )
        return len(to_remove)

    def zscore(self, key: KeyT, member: Any) -> float | None:
        """Get the score of a member."""
        encoded = self.encode(member)
        bytea = self._to_bytea(encoded)
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT z.score FROM {self._table}_zsets z
                INNER JOIN {self._table} m ON m.key = z.key
                WHERE z.key = %s AND z.member = %s AND m.type = %s {self._expiry_filter()}
                """,
                [key, bytea, _TYPE_ZSET],
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return float(row[0])

    def zmscore(self, key: KeyT, *members: Any) -> list[float | None]:
        """Get scores for multiple members."""
        return [self.zscore(key, m) for m in members]

    # =========================================================================
    # Not Supported Operations
    # =========================================================================

    # Stream operations
    xadd = _not_supported("xadd")
    xlen = _not_supported("xlen")
    xrange = _not_supported("xrange")
    xrevrange = _not_supported("xrevrange")
    xread = _not_supported("xread")
    xtrim = _not_supported("xtrim")
    xdel = _not_supported("xdel")
    xinfo_stream = _not_supported("xinfo_stream")
    xinfo_groups = _not_supported("xinfo_groups")
    xinfo_consumers = _not_supported("xinfo_consumers")
    xgroup_create = _not_supported("xgroup_create")
    xgroup_destroy = _not_supported("xgroup_destroy")
    xgroup_setid = _not_supported("xgroup_setid")
    xgroup_delconsumer = _not_supported("xgroup_delconsumer")
    xreadgroup = _not_supported("xreadgroup")
    xack = _not_supported("xack")
    xpending = _not_supported("xpending")
    xclaim = _not_supported("xclaim")
    xautoclaim = _not_supported("xautoclaim")

    # Lua scripts
    eval = _not_supported("eval")

    # Blocking operations
    blpop = _not_supported("blpop")
    brpop = _not_supported("brpop")
    blmove = _not_supported("blmove")

    # Set scan (use smembers instead)
    sscan = _not_supported("sscan")
    sscan_iter = _not_supported("sscan_iter")

    # Pipeline
    def pipeline(self, *, transaction: bool = True, version: int | None = None) -> Any:
        """Not supported."""
        raise NotSupportedError("pipeline", _BACKEND_NAME)

    # Lock
    def lock(self, key: str, **kwargs: Any) -> Any:
        """Not supported."""
        raise NotSupportedError("lock", _BACKEND_NAME)

    def alock(self, key: str, **kwargs: Any) -> Any:
        """Not supported."""
        raise NotSupportedError("alock", _BACKEND_NAME)

    # Client access
    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Return the Django database connection."""
        return self._conn

    def get_async_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Return the Django database connection."""
        return self._conn

    # Server info
    def info(self, section: str | None = None) -> dict[str, Any]:
        """Not supported."""
        raise NotSupportedError("info", _BACKEND_NAME)

    def slowlog_get(self, count: int = 10) -> list[Any]:
        """Not supported."""
        raise NotSupportedError("slowlog_get", _BACKEND_NAME)

    def slowlog_len(self) -> int:
        """Not supported."""
        raise NotSupportedError("slowlog_len", _BACKEND_NAME)

    # =========================================================================
    # Async Wrappers
    # =========================================================================

    # Core
    aget = _async("get")
    aset = _async("set")
    aadd = _async("add")
    aset_with_flags = _async("set_with_flags")
    adelete = _async("delete")
    ahas_key = _async("has_key")
    atouch = _async("touch")
    aget_many = _async("get_many")
    aset_many = _async("set_many")
    adelete_many = _async("delete_many")
    aincr = _async("incr")
    aclear = _async("clear")
    atype = _async("type")

    # TTL
    attl = _async("ttl")
    apttl = _async("pttl")
    aexpiretime = _async("expiretime")
    aexpire = _async("expire")
    apexpire = _async("pexpire")
    aexpireat = _async("expireat")
    apexpireat = _async("pexpireat")
    apersist = _async("persist")

    # Keys
    akeys = _async("keys")
    ascan = _async("scan")
    adelete_pattern = _async("delete_pattern")
    arename = _async("rename")
    arenamenx = _async("renamenx")

    # Hashes
    ahset = _async("hset")
    ahsetnx = _async("hsetnx")
    ahget = _async("hget")
    ahmget = _async("hmget")
    ahgetall = _async("hgetall")
    ahdel = _async("hdel")
    ahexists = _async("hexists")
    ahlen = _async("hlen")
    ahkeys = _async("hkeys")
    ahvals = _async("hvals")
    ahincrby = _async("hincrby")
    ahincrbyfloat = _async("hincrbyfloat")

    # Lists
    alpush = _async("lpush")
    arpush = _async("rpush")
    alpop = _async("lpop")
    arpop = _async("rpop")
    alrange = _async("lrange")
    alindex = _async("lindex")
    allen = _async("llen")
    alpos = _async("lpos")
    almove = _async("lmove")
    alrem = _async("lrem")
    altrim = _async("ltrim")
    alset = _async("lset")
    alinsert = _async("linsert")

    # Sets
    asadd = _async("sadd")
    ascard = _async("scard")
    asismember = _async("sismember")
    asmismember = _async("smismember")
    asmembers = _async("smembers")
    asrandmember = _async("srandmember")
    aspop = _async("spop")
    asrem = _async("srem")
    asmove = _async("smove")
    asdiff = _async("sdiff")
    asdiffstore = _async("sdiffstore")
    asinter = _async("sinter")
    asinterstore = _async("sinterstore")
    asunion = _async("sunion")
    asunionstore = _async("sunionstore")

    # Sorted Sets
    azadd = _async("zadd")
    azcard = _async("zcard")
    azcount = _async("zcount")
    azincrby = _async("zincrby")
    azpopmin = _async("zpopmin")
    azpopmax = _async("zpopmax")
    azrange = _async("zrange")
    azrevrange = _async("zrevrange")
    azrangebyscore = _async("zrangebyscore")
    azrevrangebyscore = _async("zrevrangebyscore")
    azrank = _async("zrank")
    azrevrank = _async("zrevrank")
    azrem = _async("zrem")
    azremrangebyscore = _async("zremrangebyscore")
    azremrangebyrank = _async("zremrangebyrank")
    azscore = _async("zscore")
    azmscore = _async("zmscore")

    # Not supported async wrappers
    axadd = _not_supported("axadd")
    axlen = _not_supported("axlen")
    axrange = _not_supported("axrange")
    axrevrange = _not_supported("axrevrange")
    axread = _not_supported("axread")
    axtrim = _not_supported("axtrim")
    axdel = _not_supported("axdel")
    axinfo_stream = _not_supported("axinfo_stream")
    axinfo_groups = _not_supported("axinfo_groups")
    axinfo_consumers = _not_supported("axinfo_consumers")
    axgroup_create = _not_supported("axgroup_create")
    axgroup_destroy = _not_supported("axgroup_destroy")
    axgroup_setid = _not_supported("axgroup_setid")
    axgroup_delconsumer = _not_supported("axgroup_delconsumer")
    axreadgroup = _not_supported("axreadgroup")
    axack = _not_supported("axack")
    axpending = _not_supported("axpending")
    axclaim = _not_supported("axclaim")
    axautoclaim = _not_supported("axautoclaim")
    aeval = _not_supported("aeval")
    ablpop = _not_supported("ablpop")
    abrpop = _not_supported("abrpop")
    ablmove = _not_supported("ablmove")
    asscan = _not_supported("asscan")
    asscan_iter = _not_supported("asscan_iter")

    # iter_keys needs a proper async generator
    async def aiter_keys(self, pattern: str, itersize: int | None = None) -> AsyncIterator[str]:
        """Iterate keys matching pattern asynchronously."""
        if itersize is None:
            itersize = self._default_scan_itersize

        cursor_val = 0
        while True:
            next_cursor, batch = await self.ascan(cursor=cursor_val, match=pattern, count=itersize)
            for k in batch:
                yield k
            if next_cursor == 0:
                break
            cursor_val = next_cursor
