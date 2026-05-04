"""Cachex LocMemCache — drop-in replacement for Django's LocMemCache.

Extends ``django.core.cache.backends.locmem.LocMemCache`` with the cachex
extension surface (lists, sets, hashes, sorted sets, TTL ops, key scanning,
admin info) implemented natively against the underlying ``OrderedDict``.

Compared to running ``CachexCompat`` over a plain ``BaseCache``:

- Compound ops acquire Django's own ``self._lock`` once, instead of
  layering a separate reentrant lock on top of two ``self.get``/``self.set``
  calls. One lock acquire per op, not two.
- One pickle round-trip per op (read pickled bytes, mutate, write pickled
  bytes) — no extra serialize/deserialize through the public ``set``/``get``.
- Existing keys preserve their TTL automatically (``_expire_info`` is left
  untouched on in-place mutation), avoiding the read-ttl-then-set-with-ttl
  dance.

Usage::

    CACHES = {
        "default": {
            "BACKEND": "django_cachex.cache.LocMemCache",
            "LOCATION": "my-cache",
        },
    }
"""

import fnmatch
import pickle
import random
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from django.core.cache.backends.locmem import LocMemCache as DjangoLocMemCache

from django_cachex.cache.base import BaseCachex
from django_cachex.types import KeyType
from django_cachex.utils import _deep_getsizeof, _format_bytes

if TYPE_CHECKING:
    from collections import OrderedDict
    from collections.abc import Iterator, Mapping, Sequence
    from threading import Lock

# Sentinel for "key not found" vs "key holds None".
_MISSING = object()


class LocMemCache(BaseCachex, DjangoLocMemCache):
    """LocMemCache with cachex extensions, implemented natively.

    Drop-in replacement for ``django.core.cache.backends.locmem.LocMemCache``.
    Standard cache ops (``get``/``set``/``delete``/...) are inherited
    unchanged. Cachex extensions read and write the underlying
    ``OrderedDict`` directly under Django's per-name lock, so compound
    ops are atomic within a single process.
    """

    _cachex_support: str = "cachex"

    # Type-only declarations for attributes/methods Django sets in __init__
    # but doesn't surface in stubs.
    if TYPE_CHECKING:
        _cache: OrderedDict[str, bytes]
        _expire_info: dict[str, float | None]
        _lock: Lock
        _max_entries: int
        _cull_frequency: int

        def _has_expired(self, key: str) -> bool: ...
        def _delete(self, key: str) -> bool: ...
        # Django's lower-level pickled-bytes writer; the public API is ``set``.
        def _set(self, key: str, value: bytes, timeout: float | None = ...) -> None: ...

    # =========================================================================
    # Native helpers (caller must hold ``self._lock``)
    # =========================================================================

    def _native_get(self, internal_key: str) -> Any:
        """Read and unpickle the value for ``internal_key``, or ``_MISSING``.

        Caller must hold ``self._lock``. Treats expired keys as missing
        and evicts them from the dict + expire info.
        """
        if internal_key not in self._cache:
            return _MISSING
        if self._has_expired(internal_key):
            self._delete(internal_key)
            return _MISSING
        return pickle.loads(self._cache[internal_key])  # noqa: S301

    def _native_write(self, internal_key: str, value: Any) -> None:
        """Pickle ``value`` and store it for ``internal_key``.

        Caller must hold ``self._lock``. Existing keys keep their TTL —
        we update the dict slot and bump LRU position but leave
        ``_expire_info`` untouched. New keys get no expiry (matching
        Redis' compound-op semantics: ``LPUSH`` etc. don't touch TTL).
        """
        pickled = pickle.dumps(value, self.pickle_protocol)
        if internal_key in self._cache:
            self._cache[internal_key] = pickled
            self._cache.move_to_end(internal_key, last=False)
        else:
            self._set(internal_key, pickled, timeout=None)

    def _internal_key(self, key: str, version: int | None = None) -> str:
        """Resolve a user key (with version) to the internal cache-dict key."""
        return self.make_and_validate_key(str(key), version=version)

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def ttl(self, key: str, version: int | None = None) -> int | None:
        """Get the TTL of a key in seconds.

        Returns ``-2`` if the key is missing, ``-1`` if it has no expiry,
        otherwise the integer seconds remaining (clamped at 0).
        """
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            if internal_key not in self._cache:
                return -2
            if self._has_expired(internal_key):
                self._delete(internal_key)
                return -2
            exp_time = self._expire_info.get(internal_key)
            if exp_time is None:
                return -1
            return max(0, int(exp_time - time.time()))

    def expire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        """Set the TTL of a key. Returns ``True`` if the key existed."""
        internal_key = self._internal_key(key, version=version)
        if isinstance(timeout, timedelta):
            timeout_secs = timeout.total_seconds()
        else:
            timeout_secs = float(timeout)
        with self._lock:
            if internal_key not in self._cache or self._has_expired(internal_key):
                if self._has_expired(internal_key):
                    self._delete(internal_key)
                return False
            self._expire_info[internal_key] = time.time() + timeout_secs
            return True

    def persist(self, key: str, version: int | None = None) -> bool:
        """Remove the TTL from a key. Returns ``True`` if the key existed."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            if internal_key not in self._cache or self._has_expired(internal_key):
                if self._has_expired(internal_key):
                    self._delete(internal_key)
                return False
            self._expire_info[internal_key] = None
            return True

    # =========================================================================
    # Type Detection
    # =========================================================================

    def type(self, key: str, version: int | None = None) -> KeyType | None:
        """Get the data type of a key by inspecting the stored Python value."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            value = self._native_get(internal_key)
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

        Assumes Django's default key format ``KEY_PREFIX:VERSION:key``.
        Custom ``KEY_FUNCTION`` settings may produce different formats —
        we fall back to the raw internal key in that case.
        """
        with self._lock:
            internal_keys = list(self._cache)
        all_keys = []
        for internal_key in internal_keys:
            parts = internal_key.split(":", 2)
            if len(parts) >= 3:
                user_key = parts[2]
                if self.key_prefix and user_key.startswith(self.key_prefix):
                    user_key = user_key[len(self.key_prefix) :]
                all_keys.append(user_key)
            else:
                all_keys.append(internal_key)
        if pattern and pattern != "*":
            all_keys = [k for k in all_keys if fnmatch.fnmatch(k, pattern)]
        all_keys.sort()
        return all_keys

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
        """Get LocMemCache info in a Redis-INFO-like structured format."""
        with self._lock:
            current_time = time.time()
            key_count = len(self._cache)
            keys_with_expiry = sum(1 for exp in self._expire_info.values() if exp and exp > current_time)
            try:
                total_size = sum(_deep_getsizeof(v) for v in self._cache.values())
                total_size += sum(_deep_getsizeof(k) for k in self._cache)
            except Exception:  # noqa: BLE001
                total_size = 0
        return {
            "backend": "LocMemCache",
            "server": {
                "redis_version": "LocMemCache (in-process)",
                "process_id": None,
            },
            "memory": {
                "used_memory": total_size,
                "used_memory_human": _format_bytes(total_size),
                "maxmemory": self._max_entries,
                "maxmemory_human": f"{self._max_entries} entries",
                "maxmemory_policy": f"cull 1/{self._cull_frequency} when full",
            },
            "keyspace": {
                "db0": {
                    "keys": key_count,
                    "expires": keys_with_expiry,
                },
            },
        }

    # =========================================================================
    # List Operations
    # =========================================================================

    def _typed_get_list(self, internal_key: str) -> list[Any] | None:
        """Caller holds ``self._lock``. Returns the stored list or None."""
        value = self._native_get(internal_key)
        if value is _MISSING:
            return None
        if not isinstance(value, list):
            msg = f"Key {internal_key!r} does not hold a list value."
            raise TypeError(msg)
        return value

    def lpush(self, key: str, *values: Any, version: int | None = None) -> int:
        """Prepend values to the head of a list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key) or []
            new_list = list(reversed(values)) + current
            self._native_write(internal_key, new_list)
            return len(new_list)

    def rpush(self, key: str, *values: Any, version: int | None = None) -> int:
        """Append values to the tail of a list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key) or []
            new_list = current + list(values)
            self._native_write(internal_key, new_list)
            return len(new_list)

    def lpop(self, key: str, count: int | None = None, version: int | None = None) -> list[Any]:
        """Remove and return element(s) from the head of a list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key)
            if not current:
                return []
            pop_count = count if count is not None else 1
            popped = current[:pop_count]
            remaining = current[pop_count:]
            if remaining:
                self._native_write(internal_key, remaining)
            else:
                self._delete(internal_key)
            return popped

    def rpop(self, key: str, count: int | None = None, version: int | None = None) -> list[Any]:
        """Remove and return element(s) from the tail of a list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key)
            if not current:
                return []
            pop_count = count if count is not None else 1
            popped = list(reversed(current[-pop_count:]))
            remaining = current[:-pop_count] if pop_count < len(current) else []
            if remaining:
                self._native_write(internal_key, remaining)
            else:
                self._delete(internal_key)
            return popped

    def lrange(self, key: str, start: int, end: int, version: int | None = None) -> list[Any]:
        """Return a range of elements from a list (inclusive end, Redis-style)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key)
        if not current:
            return []
        length = len(current)
        if start < 0:
            start = max(length + start, 0)
        if end < 0:
            end = length + end
        if start >= length or end < start:
            return []
        return current[start : end + 1]

    def llen(self, key: str, version: int | None = None) -> int:
        """Return the length of a list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key)
        return 0 if current is None else len(current)

    def lrem(  # noqa: PLR0912
        self,
        key: str,
        count: int,
        value: Any,
        version: int | None = None,
    ) -> int:
        """Remove occurrences of value from a list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key)
            if not current:
                return 0
            removed = 0
            if count == 0:
                new_list = [item for item in current if item != value]
                removed = len(current) - len(new_list)
            elif count > 0:
                new_list = []
                for item in current:
                    if item == value and removed < count:
                        removed += 1
                    else:
                        new_list.append(item)
            else:
                abs_count = abs(count)
                new_list = []
                for item in reversed(current):
                    if item == value and removed < abs_count:
                        removed += 1
                    else:
                        new_list.append(item)
                new_list.reverse()
            if removed > 0:
                if new_list:
                    self._native_write(internal_key, new_list)
                else:
                    self._delete(internal_key)
            return removed

    def ltrim(self, key: str, start: int, end: int, version: int | None = None) -> bool:
        """Trim a list to the specified range (inclusive end, Redis-style)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key)
            if current is None:
                return True
            length = len(current)
            if start < 0:
                start = max(length + start, 0)
            if end < 0:
                end = length + end
            if start >= length or end < start:
                self._delete(internal_key)
                return True
            trimmed = current[start : end + 1]
            if trimmed:
                self._native_write(internal_key, trimmed)
            else:
                self._delete(internal_key)
            return True

    def lindex(self, key: str, index: int, version: int | None = None) -> Any:
        """Get element at index in list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key)
        if not current:
            return None
        try:
            return current[index]
        except IndexError:
            return None

    def lset(self, key: str, index: int, value: Any, version: int | None = None) -> bool:
        """Set element at index in list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key)
            if not current:
                msg = "no such key"
                raise ValueError(msg)
            try:
                current[index] = value
            except IndexError:
                msg = "index out of range"
                raise ValueError(msg) from None
            self._native_write(internal_key, current)
            return True

    def linsert(self, key: str, where: str, pivot: Any, value: Any, version: int | None = None) -> int:
        """Insert value before or after pivot in list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key)
            if not current:
                return 0
            try:
                idx = current.index(pivot)
            except ValueError:
                return -1
            if where.upper() == "AFTER":
                idx += 1
            current.insert(idx, value)
            self._native_write(internal_key, current)
            return len(current)

    def lpos(
        self,
        key: str,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key)
        if not current:
            return [] if count is not None else None
        scan = current[:maxlen] if maxlen else current
        positions = [i for i, v in enumerate(scan) if v == value]
        if rank is not None:
            if rank > 0:
                positions = positions[rank - 1 :]
            elif rank < 0:
                positions = list(reversed(positions))[: abs(rank)]
        if count is not None:
            return positions if count == 0 else positions[:count]
        return positions[0] if positions else None

    # =========================================================================
    # Set Operations
    # =========================================================================

    def _typed_get_set(self, internal_key: str) -> set[Any] | None:
        """Caller holds ``self._lock``. Returns the stored set or None."""
        value = self._native_get(internal_key)
        if value is _MISSING:
            return None
        if not isinstance(value, set):
            msg = f"Key {internal_key!r} does not hold a set value."
            raise TypeError(msg)
        return value

    def sadd(self, key: str, *members: Any, version: int | None = None) -> int:
        """Add members to a set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key)
            if current is None:
                current = set()
            before = len(current)
            current.update(members)
            self._native_write(internal_key, current)
            return len(current) - before

    def srem(self, key: str, *members: Any, version: int | None = None) -> int:
        """Remove members from a set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key)
            if not current:
                return 0
            removed = len(current.intersection(members))
            current.difference_update(members)
            if current:
                self._native_write(internal_key, current)
            else:
                self._delete(internal_key)
            return removed

    def scard(self, key: str, version: int | None = None) -> int:
        """Get the number of members in a set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key)
        return 0 if current is None else len(current)

    def sismember(self, key: str, member: Any, version: int | None = None) -> bool:
        """Check if member is in set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key)
        return False if current is None else member in current

    def smembers(self, key: str, version: int | None = None) -> set[Any]:
        """Get all members of a set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key)
        return set() if current is None else set(current)

    def spop(self, key: str, count: int | None = None, version: int | None = None) -> Any | set[Any]:
        """Remove and return random member(s) from set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key)
            if not current:
                return set() if count is not None else None
            if count is None:
                member = random.choice(list(current))  # noqa: S311
                current.discard(member)
                if current:
                    self._native_write(internal_key, current)
                else:
                    self._delete(internal_key)
                return member
            pop_count = min(count, len(current))
            popped = set(random.sample(list(current), pop_count))
            current.difference_update(popped)
            if current:
                self._native_write(internal_key, current)
            else:
                self._delete(internal_key)
            return popped

    def srandmember(self, key: str, count: int | None = None, version: int | None = None) -> Any | list[Any]:
        """Get random member(s) from set without removing."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key)
        if not current:
            return [] if count is not None else None
        if count is None:
            return random.choice(list(current))  # noqa: S311
        return random.sample(list(current), min(count, len(current)))

    def smismember(self, key: str, *members: Any, version: int | None = None) -> list[bool]:
        """Check if multiple values are members of a set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key)
        if current is None:
            return [False] * len(members)
        return [m in current for m in members]

    def _collect_sets(self, keys: str | Sequence[str], version: int | None = None) -> list[set[Any]]:
        """Read multiple keys' sets under a single lock acquire."""
        if isinstance(keys, str):
            keys = [keys]
        internal_keys = [self._internal_key(k, version=version) for k in keys]
        with self._lock:
            return [self._typed_get_set(ik) or set() for ik in internal_keys]

    def sdiff(self, keys: str | Sequence[str], version: int | None = None) -> set[Any]:
        """Return the difference between sets."""
        sets = self._collect_sets(keys, version=version)
        if not sets:
            return set()
        result = sets[0]
        for s in sets[1:]:
            result = result - s
        return result

    def sinter(self, keys: str | Sequence[str], version: int | None = None) -> set[Any]:
        """Return the intersection of sets."""
        sets = self._collect_sets(keys, version=version)
        if not sets:
            return set()
        result = sets[0]
        for s in sets[1:]:
            result = result & s
        return result

    def sunion(self, keys: str | Sequence[str], version: int | None = None) -> set[Any]:
        """Return the union of sets."""
        result: set[Any] = set()
        for s in self._collect_sets(keys, version=version):
            result |= s
        return result

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def _typed_get_hash(self, internal_key: str) -> dict[str, Any] | None:
        """Caller holds ``self._lock``. Returns the stored hash or None."""
        value = self._native_get(internal_key)
        if value is _MISSING:
            return None
        if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
            msg = f"Key {internal_key!r} does not hold a hash value."
            raise TypeError(msg)
        return value

    def hset(  # noqa: C901
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        """Set hash field(s)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
            if current is None:
                current = {}
            added = 0
            if field is not None:
                if field not in current:
                    added += 1
                current[field] = value
            if mapping:
                for f, v in mapping.items():
                    if f not in current:
                        added += 1
                    current[f] = v
            if items:
                if len(items) % 2 != 0:
                    msg = "items must contain an even number of elements (field/value pairs)"
                    raise ValueError(msg)
                for i in range(0, len(items), 2):
                    f, v = items[i], items[i + 1]
                    if f not in current:
                        added += 1
                    current[f] = v
            self._native_write(internal_key, current)
            return added

    def hdel(self, key: str, *fields: str, version: int | None = None) -> int:
        """Delete hash fields."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
            if not current:
                return 0
            removed = sum(1 for f in fields if f in current)
            for f in fields:
                current.pop(f, None)
            if removed > 0:
                if current:
                    self._native_write(internal_key, current)
                else:
                    self._delete(internal_key)
            return removed

    def hget(self, key: str, field: str, version: int | None = None) -> Any:
        """Get value of field in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
        return None if current is None else current.get(field)

    def hgetall(self, key: str, version: int | None = None) -> dict[str, Any]:
        """Get all fields and values in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
        return {} if current is None else dict(current)

    def hlen(self, key: str, version: int | None = None) -> int:
        """Get number of fields in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
        return 0 if current is None else len(current)

    def hkeys(self, key: str, version: int | None = None) -> list[str]:
        """Get all field names in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
        return [] if current is None else list(current.keys())

    def hvals(self, key: str, version: int | None = None) -> list[Any]:
        """Get all values in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
        return [] if current is None else list(current.values())

    def hexists(self, key: str, field: str, version: int | None = None) -> bool:
        """Check if field exists in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
        return False if current is None else field in current

    def hmget(self, key: str, *fields: str, version: int | None = None) -> list[Any]:
        """Get values of multiple fields."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
        if current is None:
            return [None] * len(fields)
        return [current.get(f) for f in fields]

    def hsetnx(self, key: str, field: str, value: Any, version: int | None = None) -> bool:
        """Set field in hash only if it doesn't exist."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
            if current is None:
                current = {}
            if field in current:
                return False
            current[field] = value
            self._native_write(internal_key, current)
            return True

    def hincrby(self, key: str, field: str, amount: int = 1, version: int | None = None) -> int:
        """Increment integer value of field in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
            if current is None:
                current = {}
            current[field] = int(current.get(field, 0)) + amount
            self._native_write(internal_key, current)
            return current[field]

    def hincrbyfloat(self, key: str, field: str, amount: float = 1.0, version: int | None = None) -> float:
        """Increment float value of field in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key)
            if current is None:
                current = {}
            current[field] = float(current.get(field, 0)) + amount
            self._native_write(internal_key, current)
            return current[field]

    # =========================================================================
    # Sorted Set Operations
    # =========================================================================

    def _typed_get_zset(self, internal_key: str) -> dict[Any, float] | None:
        """Caller holds ``self._lock``. Returns the stored sorted set or None."""
        value = self._native_get(internal_key)
        if value is _MISSING:
            return None
        if not isinstance(value, dict):
            msg = f"Key {internal_key!r} does not hold a sorted set value."
            raise TypeError(msg)
        return value

    @staticmethod
    def _sorted_members(zset: dict[Any, float]) -> list[tuple[Any, float]]:
        """Order members by (score, str(member)) — Redis-style stable ordering."""
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
        """Add members to a sorted set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key) or {}
            changed = 0
            for member, score in mapping.items():
                exists = member in current
                if nx and exists:
                    continue
                if xx and not exists:
                    continue
                old_score = current.get(member)
                if gt and old_score is not None and score <= old_score:
                    continue
                if lt and old_score is not None and score >= old_score:
                    continue
                if ch:
                    if old_score != score:
                        changed += 1
                elif not exists:
                    changed += 1
                current[member] = score
            self._native_write(internal_key, current)
            return changed

    def zcard(self, key: str, version: int | None = None) -> int:
        """Get the number of members in a sorted set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
        return 0 if current is None else len(current)

    def zscore(self, key: str, member: Any, version: int | None = None) -> float | None:
        """Get the score of a member."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
        return None if current is None else current.get(member)

    def zrank(self, key: str, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member (lowest score = 0)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
        if current is None or member not in current:
            return None
        for i, (m, _) in enumerate(self._sorted_members(current)):
            if m == member:
                return i
        return None

    def zrevrank(self, key: str, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member (highest score = 0)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
        if current is None or member not in current:
            return None
        for i, (m, _) in enumerate(reversed(self._sorted_members(current))):
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
        """Return a range of members by index."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
        if not current:
            return []
        sorted_members = self._sorted_members(current)
        length = len(sorted_members)
        if start < 0:
            start = max(length + start, 0)
        if end < 0:
            end = length + end
        if start >= length or end < start:
            return []
        sliced = sorted_members[start : end + 1]
        if withscores:
            return sliced
        return [m for m, _ in sliced]

    def zrevrange(
        self,
        key: str,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index, highest to lowest."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
        if not current:
            return []
        sorted_members = list(reversed(self._sorted_members(current)))
        length = len(sorted_members)
        if start < 0:
            start = max(length + start, 0)
        if end < 0:
            end = length + end
        if start >= length or end < start:
            return []
        sliced = sorted_members[start : end + 1]
        if withscores:
            return sliced
        return [m for m, _ in sliced]

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
        """Return members with scores between min and max."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
        if not current:
            return []
        lo = float("-inf") if min_score == "-inf" else float(min_score)
        hi = float("inf") if max_score == "+inf" else float(max_score)
        filtered = [(m, s) for m, s in self._sorted_members(current) if lo <= s <= hi]
        if start is not None and num is not None:
            filtered = filtered[start : start + num]
        if withscores:
            return filtered
        return [m for m, _ in filtered]

    def zrem(self, key: str, *members: Any, version: int | None = None) -> int:
        """Remove members from a sorted set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
            if not current:
                return 0
            removed = sum(1 for m in members if m in current)
            for m in members:
                current.pop(m, None)
            if removed > 0:
                if current:
                    self._native_write(internal_key, current)
                else:
                    self._delete(internal_key)
            return removed

    def zincrby(self, key: str, amount: float, member: Any, version: int | None = None) -> float:
        """Increment the score of a member."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key) or {}
            current[member] = current.get(member, 0.0) + amount
            self._native_write(internal_key, current)
            return current[member]

    def zcount(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Count members with scores between min and max."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
        if not current:
            return 0
        lo = float("-inf") if min_score == "-inf" else float(min_score)
        hi = float("inf") if max_score == "+inf" else float(max_score)
        return sum(1 for s in current.values() if lo <= s <= hi)

    def zpopmin(self, key: str, count: int | None = None, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
            if not current:
                return []
            sorted_members = self._sorted_members(current)
            n = 1 if count is None else count
            popped = sorted_members[:n]
            for m, _ in popped:
                del current[m]
            if current:
                self._native_write(internal_key, current)
            else:
                self._delete(internal_key)
            return popped

    def zpopmax(self, key: str, count: int | None = None, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
            if not current:
                return []
            sorted_members = list(reversed(self._sorted_members(current)))
            n = 1 if count is None else count
            popped = sorted_members[:n]
            for m, _ in popped:
                del current[m]
            if current:
                self._native_write(internal_key, current)
            else:
                self._delete(internal_key)
            return popped

    def zmscore(self, key: str, *members: Any, version: int | None = None) -> list[float | None]:
        """Get the scores of multiple members."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
        if current is None:
            return [None] * len(members)
        return [current.get(m) for m in members]

    def zremrangebyscore(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Remove members with scores between min and max."""
        internal_key = self._internal_key(key, version=version)
        lo = float("-inf") if min_score == "-inf" else float(min_score)
        hi = float("inf") if max_score == "+inf" else float(max_score)
        with self._lock:
            current = self._typed_get_zset(internal_key)
            if not current:
                return 0
            to_remove = [m for m, s in current.items() if lo <= s <= hi]
            for m in to_remove:
                del current[m]
            if to_remove:
                if current:
                    self._native_write(internal_key, current)
                else:
                    self._delete(internal_key)
            return len(to_remove)

    def zremrangebyrank(self, key: str, start: int, end: int, version: int | None = None) -> int:
        """Remove members by rank range."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key)
            if not current:
                return 0
            sorted_members = self._sorted_members(current)
            length = len(sorted_members)
            if start < 0:
                start = max(length + start, 0)
            if end < 0:
                end = length + end
            if start >= length or end < start:
                return 0
            to_remove = sorted_members[start : end + 1]
            for m, _ in to_remove:
                del current[m]
            if current:
                self._native_write(internal_key, current)
            else:
                self._delete(internal_key)
            return len(to_remove)
