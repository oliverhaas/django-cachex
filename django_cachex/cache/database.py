"""Cachex DatabaseCache — drop-in replacement for Django's DatabaseCache.

Extends ``django.core.cache.backends.db.DatabaseCache`` with the cachex
extension surface (lists, sets, hashes, sorted sets, TTL ops, key scanning,
admin info) implemented natively against the underlying cache table.

Notable design points:

- Compound ops run inside a single ``transaction.atomic()`` block with a
  ``SELECT ... FOR UPDATE`` row lock (PostgreSQL, MySQL/InnoDB; no-op on
  SQLite). Two concurrent ``lpush``/``sadd``/``hincrby`` calls against the
  same key are serialized at the database, eliminating the GET-then-SET
  race the naive emulation path is exposed to.
- One pickle round-trip per op, the same shape Django's stock backend
  uses (``pickle.dumps`` → base64 → ``TEXT`` column). No double encoding
  through the public ``set``/``get`` surface.
- Existing keys preserve their ``expires`` column on in-place mutation;
  only new rows get a fresh ``expires`` (set to ``datetime.max`` —
  matching Django's "no expiry" sentinel for compound ops).

Usage::

    CACHES = {
        "default": {
            "BACKEND": "django_cachex.cache.DatabaseCache",
            "LOCATION": "django_cache_table",
        },
    }

Then run ``manage.py createcachetable``.
"""

import base64
import logging
import pickle
import random
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from asgiref.sync import sync_to_async
from django.conf import settings
from django.core.cache.backends.db import DatabaseCache as DjangoDatabaseCache
from django.db import connections, models, router, transaction

from django_cachex.cache.base import BaseCachex
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from django.db.backends.base.base import BaseDatabaseWrapper

logger = logging.getLogger(__name__)

# Sentinels for compound-op transforms.
_MISSING = object()  # current value: row absent (or expired)
_DELETE = object()  # transform output: drop the row


def _now() -> datetime:
    """Current time, microseconds truncated to match Django's stored precision."""
    tz = UTC if settings.USE_TZ else None
    return datetime.now(tz=tz).replace(microsecond=0)


def _no_expiry_dt() -> datetime:
    """The ``datetime.max`` value Django writes for ``timeout=None``."""
    far_future = datetime.max.replace(microsecond=0)  # noqa: DTZ901
    if settings.USE_TZ:
        return far_future.replace(tzinfo=UTC)
    return far_future


def _adapt_dt(conn: BaseDatabaseWrapper, dt: datetime) -> Any:
    """Adapt a datetime for the given database connection."""
    return conn.ops.adapt_datetimefield_value(dt.replace(microsecond=0))


def _normalize_expires(raw: Any, conn: BaseDatabaseWrapper) -> datetime | None:
    """Convert a raw ``expires`` cell to a tz-aligned ``datetime`` (or None)."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
    else:
        # SQLite reads expires back as a string. Use Django's converters
        # to re-parse it the same way the ORM would.
        expression = models.Expression(output_field=models.DateTimeField())
        converters = conn.ops.get_db_converters(expression) + expression.get_db_converters(conn)
        dt = raw
        for converter in converters:
            dt = converter(dt, expression, conn)
        if not isinstance(dt, datetime):
            return None
    if settings.USE_TZ and dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    elif not settings.USE_TZ and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


class DatabaseCache(BaseCachex, DjangoDatabaseCache):
    """DatabaseCache with native cachex extensions.

    Drop-in replacement for ``django.core.cache.backends.db.DatabaseCache``.
    Standard cache ops (``get``/``set``/``delete``/...) are inherited
    unchanged. Cachex extensions read and write the cache table directly
    inside ``transaction.atomic()`` blocks with row-level locking, so
    compound ops are serialized correctly even under concurrent writers.

    Data structures (lists, sets, hashes, sorted sets) are stored as
    pickled-then-base64 Python objects in the existing ``value`` column —
    no schema changes needed beyond ``createcachetable``.
    """

    _cachex_support: str = "cachex"

    # =========================================================================
    # Connection / table plumbing
    # =========================================================================

    def _get_table_name(self) -> str:
        """Get the database table name for this cache."""
        return cast("Any", self)._table

    def _get_connection(self, *, write: bool = False) -> BaseDatabaseWrapper:
        """Return the router-aware database connection for this cache."""
        if write:
            db = router.db_for_write(self.cache_model_class)
        else:
            db = router.db_for_read(self.cache_model_class)
        return connections[db]

    def _decode_value(self, raw: Any, conn: BaseDatabaseWrapper) -> Any:
        """Decode a stored ``value`` column back to its Python value."""
        stored = conn.ops.process_clob(raw)
        return pickle.loads(base64.b64decode(stored.encode()))  # noqa: S301

    def _encode_value(self, value: Any) -> str:
        """Encode a Python value for storage in the ``value`` column."""
        pickled = pickle.dumps(value, self.pickle_protocol)
        return base64.b64encode(pickled).decode("latin1")

    def _internal_key(self, key: str, version: int | None = None) -> str:
        """Resolve a user key (with version) to the internal cache-table key."""
        return self.make_key(str(key), version=version)

    # =========================================================================
    # Core read / atomic compound op
    # =========================================================================

    def _read(self, internal_key: str) -> Any:
        """Read a key's value, returning ``_MISSING`` if missing or expired."""
        conn = self._get_connection()
        quote = conn.ops.quote_name
        table = quote(self._get_table_name())
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT {quote('value')}, {quote('expires')} FROM {table} "  # noqa: S608
                f"WHERE {quote('cache_key')} = %s",
                [internal_key],
            )
            row = cursor.fetchone()
        if row is None:
            return _MISSING
        raw_value, raw_expires = row
        expires_dt = _normalize_expires(raw_expires, conn)
        if expires_dt is None or expires_dt <= _now():
            return _MISSING
        return self._decode_value(raw_value, conn)

    def _atomic_compound(
        self,
        internal_key: str,
        transform: Callable[[Any], tuple[Any, Any]],
    ) -> Any:
        """Atomically read-modify-write a single key.

        ``transform(current_value)`` receives the deserialized stored value
        (or ``_MISSING`` for absent/expired rows) and returns a
        ``(new_value, return_value)`` tuple. If ``new_value`` is ``_DELETE``,
        the row is deleted; otherwise it is upserted. The existing
        ``expires`` is preserved on UPDATE; new rows get ``datetime.max``
        (no expiry — matches Redis compound-op semantics).
        """
        conn = self._get_connection(write=True)
        quote = conn.ops.quote_name
        table = quote(self._get_table_name())
        select_sql = (
            f"SELECT {quote('value')}, {quote('expires')} FROM {table} "  # noqa: S608
            f"WHERE {quote('cache_key')} = %s"
        )
        if conn.features.has_select_for_update:
            select_sql += " FOR UPDATE"
        delete_sql = f"DELETE FROM {table} WHERE {quote('cache_key')} = %s"  # noqa: S608
        update_sql = f"UPDATE {table} SET {quote('value')} = %s WHERE {quote('cache_key')} = %s"  # noqa: S608
        insert_sql = (
            f"INSERT INTO {table} ({quote('cache_key')}, {quote('value')}, {quote('expires')}) "  # noqa: S608
            f"VALUES (%s, %s, %s)"
        )
        db = router.db_for_write(self.cache_model_class)
        with transaction.atomic(using=db), conn.cursor() as cursor:
            cursor.execute(select_sql, [internal_key])
            row = cursor.fetchone()
            row_exists = False
            current: Any = _MISSING
            if row is not None:
                raw_value, raw_expires = row
                expires_dt = _normalize_expires(raw_expires, conn)
                if expires_dt is not None and expires_dt > _now():
                    current = self._decode_value(raw_value, conn)
                    row_exists = True
                else:
                    cursor.execute(delete_sql, [internal_key])
            new_value, ret = transform(current)
            if new_value is _DELETE:
                if row_exists:
                    cursor.execute(delete_sql, [internal_key])
                return ret
            encoded = self._encode_value(new_value)
            if row_exists:
                cursor.execute(update_sql, [encoded, internal_key])
            else:
                cursor.execute(insert_sql, [internal_key, encoded, _adapt_dt(conn, _no_expiry_dt())])
            return ret

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def ttl(self, key: str, version: int | None = None) -> int | None:
        """Return seconds remaining; ``-2`` if missing/expired, ``-1`` if no expiry."""
        conn = self._get_connection()
        quote = conn.ops.quote_name
        table = quote(self._get_table_name())
        internal_key = self._internal_key(key, version=version)
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT {quote('expires')} FROM {table} WHERE {quote('cache_key')} = %s",  # noqa: S608
                [internal_key],
            )
            row = cursor.fetchone()
        if row is None:
            return -2
        expires_dt = _normalize_expires(row[0], conn)
        if expires_dt is None:
            return -2
        now = _now()
        if expires_dt <= now:
            return -2
        # Django stores "no expiry" as datetime.max; surface that as -1.
        if expires_dt.year >= datetime.max.year:  # noqa: DTZ901
            return -1
        return int((expires_dt - now).total_seconds())

    def expire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        """Set the TTL of a key. Returns ``True`` if the key existed and was live."""
        if isinstance(timeout, timedelta):
            timeout_secs = timeout.total_seconds()
        else:
            timeout_secs = float(timeout)
        now = _now()
        new_expires = now + timedelta(seconds=timeout_secs)
        conn = self._get_connection(write=True)
        quote = conn.ops.quote_name
        table = quote(self._get_table_name())
        internal_key = self._internal_key(key, version=version)
        with conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE {table} SET {quote('expires')} = %s "  # noqa: S608
                f"WHERE {quote('cache_key')} = %s AND {quote('expires')} > %s",
                [_adapt_dt(conn, new_expires), internal_key, _adapt_dt(conn, now)],
            )
            return cursor.rowcount > 0

    def persist(self, key: str, version: int | None = None) -> bool:
        """Remove the TTL by setting expires to ``datetime.max``.

        Skips rows whose ``expires`` is already in the past so a logically
        expired key isn't accidentally revived.
        """
        now = _now()
        conn = self._get_connection(write=True)
        quote = conn.ops.quote_name
        table = quote(self._get_table_name())
        internal_key = self._internal_key(key, version=version)
        with conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE {table} SET {quote('expires')} = %s "  # noqa: S608
                f"WHERE {quote('cache_key')} = %s AND {quote('expires')} > %s",
                [_adapt_dt(conn, _no_expiry_dt()), internal_key, _adapt_dt(conn, now)],
            )
            return cursor.rowcount > 0

    # =========================================================================
    # Type Detection
    # =========================================================================

    def type(self, key: str, version: int | None = None) -> KeyType | None:
        """Get the data type of a key by inspecting the stored Python value."""
        value = self._read(self._internal_key(key, version=version))
        if value is _MISSING:
            return None
        if isinstance(value, list):
            return KeyType.LIST
        if isinstance(value, set):
            return KeyType.SET
        if isinstance(value, dict) and all(isinstance(k, str) for k in value):
            return KeyType.HASH
        return KeyType.STRING

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List user keys matching ``pattern`` (Redis-style glob).

        ``*`` and ``?`` translate to SQL ``LIKE`` wildcards (``%`` and ``_``).
        Assumes Django's default key format ``KEY_PREFIX:VERSION:key``;
        falls back to the raw cache key if it doesn't fit that shape.
        ``version`` scopes results to a single version (default: this
        cache's ``self.version``).
        """
        conn = self._get_connection()
        quote = conn.ops.quote_name
        table = quote(self._get_table_name())
        # Always anchor the SQL pattern to ``KEY_PREFIX:VERSION:`` so the
        # LIKE only matches the requested version; otherwise ``pattern="*"``
        # or unrelated prefixes leak through.
        prefixed = self.make_key(pattern or "*", version=version)
        sql_pattern = prefixed.replace("*", "%").replace("?", "_")
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT {quote('cache_key')} FROM {table} "  # noqa: S608
                f"WHERE {quote('cache_key')} LIKE %s AND {quote('expires')} > %s "
                f"ORDER BY {quote('cache_key')}",
                [sql_pattern, _adapt_dt(conn, _now())],
            )
            raw_keys = [row[0] for row in cursor.fetchall()]
        result = []
        # Anchor the prefix match with a trailing ``:`` so a key_prefix like
        # ``"cache"`` doesn't claim rows from a sibling prefix like
        # ``"cache_buster:"``.
        prefix_match = f"{self.key_prefix}:" if self.key_prefix else ""
        for cache_key in raw_keys:
            if prefix_match and cache_key.startswith(prefix_match):
                without_prefix = cache_key[len(prefix_match) :]
                parts = without_prefix.split(":", 1)
                result.append(parts[1] if len(parts) >= 2 else without_prefix)
            else:
                parts = cache_key.split(":", 2)
                result.append(parts[2] if len(parts) >= 3 else cache_key)
        return result

    def iter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]:
        """Iterate over keys matching pattern."""
        yield from self.keys(pattern, version=version)

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        """Delete all keys matching pattern. Returns the number deleted."""
        deleted = 0
        for key in self.keys(pattern, version=version):
            if self.delete(key, version=version):
                deleted += 1
        return deleted

    # =========================================================================
    # Info
    # =========================================================================

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get DatabaseCache info in a Redis-INFO-shaped format for admin uniformity."""
        conn = self._get_connection()
        quote = conn.ops.quote_name
        table = quote(self._get_table_name())
        total_count = 0
        active_count = 0
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
                total_count = cursor.fetchone()[0]
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {quote('expires')} > %s",  # noqa: S608
                    [_adapt_dt(conn, _now())],
                )
                active_count = cursor.fetchone()[0]
        except Exception:
            logger.exception("DatabaseCache.info(): row-count query failed; reporting 0")
        return {
            "backend": "DatabaseCache",
            "server": {
                "redis_version": f"DatabaseCache ({conn.vendor})",
                "os": f"table: {self._get_table_name()}",
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

    # =========================================================================
    # List Operations
    # =========================================================================

    @staticmethod
    def _coerce_list(current: Any, *, allow_missing: bool = True) -> list[Any] | None:
        if current is _MISSING:
            return None if allow_missing else []
        if not isinstance(current, list):
            msg = "Key does not hold a list value."
            raise TypeError(msg)
        return current

    def lpush(self, key: str, *values: Any, version: int | None = None) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_list(current) or []
            new_list = list(reversed(values)) + existing
            return new_list, len(new_list)

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def rpush(self, key: str, *values: Any, version: int | None = None) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_list(current) or []
            new_list = existing + list(values)
            return new_list, len(new_list)

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def lpop(self, key: str, count: int | None = None, version: int | None = None) -> Any | list[Any] | None:
        def transform(current: Any) -> tuple[Any, Any | list[Any] | None]:
            existing = self._coerce_list(current)
            if not existing:
                return _DELETE if existing is not None else _MISSING, None
            n = count if count is not None else 1
            popped = existing[:n]
            remaining = existing[n:]
            return (remaining or _DELETE), (popped if count is not None else popped[0])

        return self._atomic_compound(self._internal_key(key, version=version), transform)

    def rpop(self, key: str, count: int | None = None, version: int | None = None) -> Any | list[Any] | None:
        def transform(current: Any) -> tuple[Any, Any | list[Any] | None]:
            existing = self._coerce_list(current)
            if not existing:
                return _DELETE if existing is not None else _MISSING, None
            n = count if count is not None else 1
            popped = list(reversed(existing[-n:]))
            remaining = existing[:-n] if n < len(existing) else []
            return (remaining or _DELETE), (popped if count is not None else popped[0])

        return self._atomic_compound(self._internal_key(key, version=version), transform)

    def lrange(self, key: str, start: int, end: int, version: int | None = None) -> list[Any]:
        current = self._read(self._internal_key(key, version=version))
        existing = self._coerce_list(current)
        if not existing:
            return []
        length = len(existing)
        if start < 0:
            start = max(length + start, 0)
        if end < 0:
            end = length + end
        if start >= length or end < start:
            return []
        return existing[start : end + 1]

    def llen(self, key: str, version: int | None = None) -> int:
        existing = self._coerce_list(self._read(self._internal_key(key, version=version)))
        return 0 if existing is None else len(existing)

    def lrem(self, key: str, count: int, value: Any, version: int | None = None) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_list(current)
            if not existing:
                return _MISSING, 0
            removed = 0
            if count == 0:
                new_list = [item for item in existing if item != value]
                removed = len(existing) - len(new_list)
            elif count > 0:
                new_list = []
                for item in existing:
                    if item == value and removed < count:
                        removed += 1
                    else:
                        new_list.append(item)
            else:
                abs_count = abs(count)
                new_list = []
                for item in reversed(existing):
                    if item == value and removed < abs_count:
                        removed += 1
                    else:
                        new_list.append(item)
                new_list.reverse()
            if removed == 0:
                return _MISSING, 0
            return (new_list or _DELETE), removed

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def ltrim(self, key: str, start: int, end: int, version: int | None = None) -> bool:
        def transform(current: Any) -> tuple[Any, bool]:
            existing = self._coerce_list(current)
            if existing is None:
                return _MISSING, True
            length = len(existing)
            s = max(length + start, 0) if start < 0 else start
            e = length + end if end < 0 else end
            if s >= length or e < s:
                return _DELETE, True
            trimmed = existing[s : e + 1]
            return (trimmed or _DELETE), True

        return cast("bool", self._atomic_compound(self._internal_key(key, version=version), transform))

    def lindex(self, key: str, index: int, version: int | None = None) -> Any:
        existing = self._coerce_list(self._read(self._internal_key(key, version=version)))
        if not existing:
            return None
        try:
            return existing[index]
        except IndexError:
            return None

    def lset(self, key: str, index: int, value: Any, version: int | None = None) -> bool:
        def transform(current: Any) -> tuple[Any, bool]:
            existing = self._coerce_list(current)
            if not existing:
                msg = "no such key"
                raise ValueError(msg)
            try:
                existing[index] = value
            except IndexError:
                msg = "index out of range"
                raise ValueError(msg) from None
            return existing, True

        return cast("bool", self._atomic_compound(self._internal_key(key, version=version), transform))

    def linsert(self, key: str, where: str, pivot: Any, value: Any, version: int | None = None) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_list(current)
            if not existing:
                return _MISSING, 0
            try:
                idx = existing.index(pivot)
            except ValueError:
                return _MISSING, -1
            if where.upper() == "AFTER":
                idx += 1
            existing.insert(idx, value)
            return existing, len(existing)

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def lpos(
        self,
        key: str,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None:
        existing = self._coerce_list(self._read(self._internal_key(key, version=version)))
        if not existing:
            return [] if count is not None else None
        scan = existing[:maxlen] if maxlen else existing
        positions = [i for i, v in enumerate(scan) if v == value]
        if rank is not None:
            if rank > 0:
                positions = positions[rank - 1 :]
            elif rank < 0:
                # Negative rank: scan from the tail. ``rank=-1`` returns the
                # last match, ``rank=-2`` the second-to-last, etc. Continue
                # toward the head from there.
                positions = list(reversed(positions))[abs(rank) - 1 :]
        if count is not None:
            return positions if count == 0 else positions[:count]
        return positions[0] if positions else None

    # =========================================================================
    # Set Operations
    # =========================================================================

    @staticmethod
    def _coerce_set(current: Any) -> set[Any] | None:
        if current is _MISSING:
            return None
        if not isinstance(current, set):
            msg = "Key does not hold a set value."
            raise TypeError(msg)
        return current

    def sadd(self, key: str, *members: Any, version: int | None = None) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_set(current) or set()
            before = len(existing)
            existing.update(members)
            return existing, len(existing) - before

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def srem(self, key: str, *members: Any, version: int | None = None) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_set(current)
            if not existing:
                return _MISSING, 0
            removed = len(existing.intersection(members))
            existing.difference_update(members)
            return (existing or _DELETE), removed

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def scard(self, key: str, version: int | None = None) -> int:
        existing = self._coerce_set(self._read(self._internal_key(key, version=version)))
        return 0 if existing is None else len(existing)

    def sismember(self, key: str, member: Any, version: int | None = None) -> bool:
        existing = self._coerce_set(self._read(self._internal_key(key, version=version)))
        return False if existing is None else member in existing

    def smembers(self, key: str, version: int | None = None) -> set[Any]:
        existing = self._coerce_set(self._read(self._internal_key(key, version=version)))
        return set() if existing is None else set(existing)

    def spop(self, key: str, count: int | None = None, version: int | None = None) -> Any | set[Any] | None:
        def transform(current: Any) -> tuple[Any, Any]:
            existing = self._coerce_set(current)
            if not existing:
                return _MISSING, (set() if count is not None else None)
            if count is None:
                member = random.choice(list(existing))  # noqa: S311
                existing.discard(member)
                return (existing or _DELETE), member
            n = min(count, len(existing))
            popped = set(random.sample(list(existing), n))
            existing.difference_update(popped)
            return (existing or _DELETE), popped

        return self._atomic_compound(self._internal_key(key, version=version), transform)

    def srandmember(self, key: str, count: int | None = None, version: int | None = None) -> Any | list[Any]:
        existing = self._coerce_set(self._read(self._internal_key(key, version=version)))
        if not existing:
            return [] if count is not None else None
        if count is None:
            return random.choice(list(existing))  # noqa: S311
        return random.sample(list(existing), min(count, len(existing)))

    def smismember(self, key: str, *members: Any, version: int | None = None) -> list[bool]:
        existing = self._coerce_set(self._read(self._internal_key(key, version=version)))
        if existing is None:
            return [False] * len(members)
        return [m in existing for m in members]

    def _collect_sets(self, keys: str | Sequence[str], version: int | None = None) -> list[set[Any]]:
        if isinstance(keys, str):
            keys = [keys]
        return [self._coerce_set(self._read(self._internal_key(k, version=version))) or set() for k in keys]

    def sdiff(self, keys: str | Sequence[str], version: int | None = None) -> set[Any]:
        sets = self._collect_sets(keys, version=version)
        if not sets:
            return set()
        result = sets[0]
        for s in sets[1:]:
            result = result - s
        return result

    def sinter(self, keys: str | Sequence[str], version: int | None = None) -> set[Any]:
        sets = self._collect_sets(keys, version=version)
        if not sets:
            return set()
        result = sets[0]
        for s in sets[1:]:
            result = result & s
        return result

    def sunion(self, keys: str | Sequence[str], version: int | None = None) -> set[Any]:
        result: set[Any] = set()
        for s in self._collect_sets(keys, version=version):
            result |= s
        return result

    # =========================================================================
    # Hash Operations
    # =========================================================================

    @staticmethod
    def _coerce_hash(current: Any) -> dict[str, Any] | None:
        if current is _MISSING:
            return None
        if not isinstance(current, dict) or not all(isinstance(k, str) for k in current):
            msg = "Key does not hold a hash value."
            raise TypeError(msg)
        return current

    def hset(  # noqa: C901
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_hash(current) or {}
            added = 0
            if field is not None:
                if field not in existing:
                    added += 1
                existing[field] = value
            if mapping:
                for f, v in mapping.items():
                    if f not in existing:
                        added += 1
                    existing[f] = v
            if items:
                if len(items) % 2 != 0:
                    msg = "items must contain an even number of elements (field/value pairs)"
                    raise ValueError(msg)
                for i in range(0, len(items), 2):
                    f, v = items[i], items[i + 1]
                    if f not in existing:
                        added += 1
                    existing[f] = v
            return existing, added

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def hdel(self, key: str, *fields: str, version: int | None = None) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_hash(current)
            if not existing:
                return _MISSING, 0
            removed = sum(1 for f in fields if f in existing)
            for f in fields:
                existing.pop(f, None)
            if removed == 0:
                return _MISSING, 0
            return (existing or _DELETE), removed

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def hget(self, key: str, field: str, version: int | None = None) -> Any:
        existing = self._coerce_hash(self._read(self._internal_key(key, version=version)))
        return None if existing is None else existing.get(field)

    def hgetall(self, key: str, version: int | None = None) -> dict[str, Any]:
        existing = self._coerce_hash(self._read(self._internal_key(key, version=version)))
        return {} if existing is None else dict(existing)

    def hlen(self, key: str, version: int | None = None) -> int:
        existing = self._coerce_hash(self._read(self._internal_key(key, version=version)))
        return 0 if existing is None else len(existing)

    def hkeys(self, key: str, version: int | None = None) -> list[str]:
        existing = self._coerce_hash(self._read(self._internal_key(key, version=version)))
        return [] if existing is None else list(existing.keys())

    def hvals(self, key: str, version: int | None = None) -> list[Any]:
        existing = self._coerce_hash(self._read(self._internal_key(key, version=version)))
        return [] if existing is None else list(existing.values())

    def hexists(self, key: str, field: str, version: int | None = None) -> bool:
        existing = self._coerce_hash(self._read(self._internal_key(key, version=version)))
        return False if existing is None else field in existing

    def hmget(self, key: str, *fields: str, version: int | None = None) -> list[Any]:
        existing = self._coerce_hash(self._read(self._internal_key(key, version=version)))
        if existing is None:
            return [None] * len(fields)
        return [existing.get(f) for f in fields]

    def hsetnx(self, key: str, field: str, value: Any, version: int | None = None) -> bool:
        def transform(current: Any) -> tuple[Any, bool]:
            existing = self._coerce_hash(current) or {}
            if field in existing:
                return _MISSING, False
            existing[field] = value
            return existing, True

        return cast("bool", self._atomic_compound(self._internal_key(key, version=version), transform))

    def hincrby(self, key: str, field: str, amount: int = 1, version: int | None = None) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_hash(current) or {}
            existing[field] = int(existing.get(field, 0)) + amount
            return existing, existing[field]

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def hincrbyfloat(self, key: str, field: str, amount: float = 1.0, version: int | None = None) -> float:
        def transform(current: Any) -> tuple[Any, float]:
            existing = self._coerce_hash(current) or {}
            existing[field] = float(existing.get(field, 0)) + amount
            return existing, existing[field]

        return cast("float", self._atomic_compound(self._internal_key(key, version=version), transform))

    # =========================================================================
    # Sorted Set Operations
    # =========================================================================

    @staticmethod
    def _coerce_zset(current: Any) -> dict[Any, float] | None:
        if current is _MISSING:
            return None
        if not isinstance(current, dict):
            msg = "Key does not hold a sorted set value."
            raise TypeError(msg)
        return current

    @staticmethod
    def _sorted_members(zset: dict[Any, float]) -> list[tuple[Any, float]]:
        return sorted(zset.items(), key=lambda x: (x[1], str(x[0])))

    def zadd(
        self,
        key: str,
        mapping: Mapping[Any, float],
        *,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        gt: bool = False,
        lt: bool = False,
        version: int | None = None,
    ) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_zset(current) or {}
            changed = 0
            for member, score in mapping.items():
                exists = member in existing
                if nx and exists:
                    continue
                if xx and not exists:
                    continue
                old_score = existing.get(member)
                if gt and old_score is not None and score <= old_score:
                    continue
                if lt and old_score is not None and score >= old_score:
                    continue
                if ch:
                    if old_score != score:
                        changed += 1
                elif not exists:
                    changed += 1
                existing[member] = score
            return existing, changed

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def zcard(self, key: str, version: int | None = None) -> int:
        existing = self._coerce_zset(self._read(self._internal_key(key, version=version)))
        return 0 if existing is None else len(existing)

    def zscore(self, key: str, member: Any, version: int | None = None) -> float | None:
        existing = self._coerce_zset(self._read(self._internal_key(key, version=version)))
        return None if existing is None else existing.get(member)

    def zrank(self, key: str, member: Any, version: int | None = None) -> int | None:
        existing = self._coerce_zset(self._read(self._internal_key(key, version=version)))
        if existing is None or member not in existing:
            return None
        for i, (m, _) in enumerate(self._sorted_members(existing)):
            if m == member:
                return i
        return None

    def zrevrank(self, key: str, member: Any, version: int | None = None) -> int | None:
        existing = self._coerce_zset(self._read(self._internal_key(key, version=version)))
        if existing is None or member not in existing:
            return None
        for i, (m, _) in enumerate(reversed(self._sorted_members(existing))):
            if m == member:
                return i
        return None

    def zrange(
        self,
        key: str,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        existing = self._coerce_zset(self._read(self._internal_key(key, version=version)))
        if not existing:
            return []
        sorted_members = self._sorted_members(existing)
        length = len(sorted_members)
        s = max(length + start, 0) if start < 0 else start
        e = length + end if end < 0 else end
        if s >= length or e < s:
            return []
        sliced = sorted_members[s : e + 1]
        return sliced if withscores else [m for m, _ in sliced]

    def zrevrange(
        self,
        key: str,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        existing = self._coerce_zset(self._read(self._internal_key(key, version=version)))
        if not existing:
            return []
        sorted_members = list(reversed(self._sorted_members(existing)))
        length = len(sorted_members)
        s = max(length + start, 0) if start < 0 else start
        e = length + end if end < 0 else end
        if s >= length or e < s:
            return []
        sliced = sorted_members[s : e + 1]
        return sliced if withscores else [m for m, _ in sliced]

    def zrangebyscore(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        *,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        existing = self._coerce_zset(self._read(self._internal_key(key, version=version)))
        if not existing:
            return []
        lo = float("-inf") if min_score == "-inf" else float(min_score)
        hi = float("inf") if max_score == "+inf" else float(max_score)
        filtered = [(m, s) for m, s in self._sorted_members(existing) if lo <= s <= hi]
        if start is not None and num is not None:
            filtered = filtered[start : start + num]
        return filtered if withscores else [m for m, _ in filtered]

    def zrem(self, key: str, *members: Any, version: int | None = None) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_zset(current)
            if not existing:
                return _MISSING, 0
            removed = sum(1 for m in members if m in existing)
            for m in members:
                existing.pop(m, None)
            if removed == 0:
                return _MISSING, 0
            return (existing or _DELETE), removed

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def zincrby(self, key: str, amount: float, member: Any, version: int | None = None) -> float:
        def transform(current: Any) -> tuple[Any, float]:
            existing = self._coerce_zset(current) or {}
            existing[member] = existing.get(member, 0.0) + amount
            return existing, existing[member]

        return cast("float", self._atomic_compound(self._internal_key(key, version=version), transform))

    def zcount(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        existing = self._coerce_zset(self._read(self._internal_key(key, version=version)))
        if not existing:
            return 0
        lo = float("-inf") if min_score == "-inf" else float(min_score)
        hi = float("inf") if max_score == "+inf" else float(max_score)
        return sum(1 for s in existing.values() if lo <= s <= hi)

    def zpopmin(self, key: str, count: int | None = None, version: int | None = None) -> list[tuple[Any, float]]:
        def transform(current: Any) -> tuple[Any, list[tuple[Any, float]]]:
            existing = self._coerce_zset(current)
            if not existing:
                return _MISSING, []
            sorted_members = self._sorted_members(existing)
            n = 1 if count is None else count
            popped = sorted_members[:n]
            for m, _ in popped:
                del existing[m]
            return (existing or _DELETE), popped

        return cast(
            "list[tuple[Any, float]]",
            self._atomic_compound(self._internal_key(key, version=version), transform),
        )

    def zpopmax(self, key: str, count: int | None = None, version: int | None = None) -> list[tuple[Any, float]]:
        def transform(current: Any) -> tuple[Any, list[tuple[Any, float]]]:
            existing = self._coerce_zset(current)
            if not existing:
                return _MISSING, []
            sorted_members = list(reversed(self._sorted_members(existing)))
            n = 1 if count is None else count
            popped = sorted_members[:n]
            for m, _ in popped:
                del existing[m]
            return (existing or _DELETE), popped

        return cast(
            "list[tuple[Any, float]]",
            self._atomic_compound(self._internal_key(key, version=version), transform),
        )

    def zmscore(self, key: str, *members: Any, version: int | None = None) -> list[float | None]:
        existing = self._coerce_zset(self._read(self._internal_key(key, version=version)))
        if existing is None:
            return [None] * len(members)
        return [existing.get(m) for m in members]

    def zremrangebyscore(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        lo = float("-inf") if min_score == "-inf" else float(min_score)
        hi = float("inf") if max_score == "+inf" else float(max_score)

        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_zset(current)
            if not existing:
                return _MISSING, 0
            to_remove = [m for m, s in existing.items() if lo <= s <= hi]
            for m in to_remove:
                del existing[m]
            if not to_remove:
                return _MISSING, 0
            return (existing or _DELETE), len(to_remove)

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    def zremrangebyrank(self, key: str, start: int, end: int, version: int | None = None) -> int:
        def transform(current: Any) -> tuple[Any, int]:
            existing = self._coerce_zset(current)
            if not existing:
                return _MISSING, 0
            sorted_members = self._sorted_members(existing)
            length = len(sorted_members)
            s = max(length + start, 0) if start < 0 else start
            e = length + end if end < 0 else end
            if s >= length or e < s:
                return _MISSING, 0
            to_remove = sorted_members[s : e + 1]
            for m, _ in to_remove:
                del existing[m]
            return (existing or _DELETE), len(to_remove)

        return cast("int", self._atomic_compound(self._internal_key(key, version=version), transform))

    # =========================================================================
    # Async surface
    # =========================================================================
    # DatabaseCache reads and writes a real DB, so each ``a*`` wrapper offloads
    # the sync call to a thread via ``sync_to_async`` to avoid blocking the
    # event loop. ``thread_sensitive=True`` keeps successive DB calls on the
    # same connection (Django connections are thread-local). When Django gains
    # native async DB-cursor APIs, we'll swap each ``await sync_to_async(...)``
    # for a real async query without touching the public surface.

    async def attl(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.ttl, thread_sensitive=True)(*args, **kwargs)

    async def atype(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.type, thread_sensitive=True)(*args, **kwargs)

    async def apersist(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.persist, thread_sensitive=True)(*args, **kwargs)

    async def aexpire(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.expire, thread_sensitive=True)(*args, **kwargs)

    async def akeys(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.keys, thread_sensitive=True)(*args, **kwargs)

    async def aiter_keys(self, *args: Any, **kwargs: Any) -> Any:
        # iter_keys is a generator over a list snapshot — materializing once
        # in the worker thread, then yielding from the coroutine, is fine.
        items = await sync_to_async(lambda: list(self.iter_keys(*args, **kwargs)), thread_sensitive=True)()
        for item in items:
            yield item

    async def adelete_pattern(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.delete_pattern, thread_sensitive=True)(*args, **kwargs)

    async def alpush(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.lpush, thread_sensitive=True)(*args, **kwargs)

    async def arpush(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.rpush, thread_sensitive=True)(*args, **kwargs)

    async def alpop(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.lpop, thread_sensitive=True)(*args, **kwargs)

    async def arpop(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.rpop, thread_sensitive=True)(*args, **kwargs)

    async def alrange(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.lrange, thread_sensitive=True)(*args, **kwargs)

    async def allen(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.llen, thread_sensitive=True)(*args, **kwargs)

    async def alrem(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.lrem, thread_sensitive=True)(*args, **kwargs)

    async def altrim(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.ltrim, thread_sensitive=True)(*args, **kwargs)

    async def alindex(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.lindex, thread_sensitive=True)(*args, **kwargs)

    async def alset(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.lset, thread_sensitive=True)(*args, **kwargs)

    async def alinsert(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.linsert, thread_sensitive=True)(*args, **kwargs)

    async def alpos(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.lpos, thread_sensitive=True)(*args, **kwargs)

    async def asadd(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.sadd, thread_sensitive=True)(*args, **kwargs)

    async def asrem(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.srem, thread_sensitive=True)(*args, **kwargs)

    async def ascard(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.scard, thread_sensitive=True)(*args, **kwargs)

    async def asismember(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.sismember, thread_sensitive=True)(*args, **kwargs)

    async def asmembers(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.smembers, thread_sensitive=True)(*args, **kwargs)

    async def aspop(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.spop, thread_sensitive=True)(*args, **kwargs)

    async def asrandmember(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.srandmember, thread_sensitive=True)(*args, **kwargs)

    async def asmismember(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.smismember, thread_sensitive=True)(*args, **kwargs)

    async def asdiff(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.sdiff, thread_sensitive=True)(*args, **kwargs)

    async def asinter(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.sinter, thread_sensitive=True)(*args, **kwargs)

    async def asunion(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.sunion, thread_sensitive=True)(*args, **kwargs)

    async def ahset(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hset, thread_sensitive=True)(*args, **kwargs)

    async def ahdel(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hdel, thread_sensitive=True)(*args, **kwargs)

    async def ahget(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hget, thread_sensitive=True)(*args, **kwargs)

    async def ahgetall(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hgetall, thread_sensitive=True)(*args, **kwargs)

    async def ahlen(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hlen, thread_sensitive=True)(*args, **kwargs)

    async def ahkeys(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hkeys, thread_sensitive=True)(*args, **kwargs)

    async def ahvals(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hvals, thread_sensitive=True)(*args, **kwargs)

    async def ahexists(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hexists, thread_sensitive=True)(*args, **kwargs)

    async def ahmget(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hmget, thread_sensitive=True)(*args, **kwargs)

    async def ahsetnx(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hsetnx, thread_sensitive=True)(*args, **kwargs)

    async def ahincrby(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hincrby, thread_sensitive=True)(*args, **kwargs)

    async def ahincrbyfloat(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.hincrbyfloat, thread_sensitive=True)(*args, **kwargs)

    async def azadd(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zadd, thread_sensitive=True)(*args, **kwargs)

    async def azcard(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zcard, thread_sensitive=True)(*args, **kwargs)

    async def azscore(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zscore, thread_sensitive=True)(*args, **kwargs)

    async def azrank(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zrank, thread_sensitive=True)(*args, **kwargs)

    async def azrevrank(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zrevrank, thread_sensitive=True)(*args, **kwargs)

    async def azrange(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zrange, thread_sensitive=True)(*args, **kwargs)

    async def azrevrange(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zrevrange, thread_sensitive=True)(*args, **kwargs)

    async def azrangebyscore(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zrangebyscore, thread_sensitive=True)(*args, **kwargs)

    async def azrem(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zrem, thread_sensitive=True)(*args, **kwargs)

    async def azincrby(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zincrby, thread_sensitive=True)(*args, **kwargs)

    async def azcount(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zcount, thread_sensitive=True)(*args, **kwargs)

    async def azpopmin(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zpopmin, thread_sensitive=True)(*args, **kwargs)

    async def azpopmax(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zpopmax, thread_sensitive=True)(*args, **kwargs)

    async def azmscore(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zmscore, thread_sensitive=True)(*args, **kwargs)

    async def azremrangebyrank(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zremrangebyrank, thread_sensitive=True)(*args, **kwargs)

    async def azremrangebyscore(self, *args: Any, **kwargs: Any) -> Any:
        return await sync_to_async(self.zremrangebyscore, thread_sensitive=True)(*args, **kwargs)
