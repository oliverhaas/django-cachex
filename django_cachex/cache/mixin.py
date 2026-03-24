"""CachexMixin — shared functionality for non-Redis cache backends.

Provides data structure operations (lists, sets, hashes, sorted sets),
TTL helpers, type detection, key scanning, and admin support markers.

All data structure operations are implemented via get/set on Python objects,
making them compatible with any BaseCache-conformant backend.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any

from django_cachex.exceptions import NotSupportedError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from django_cachex.client.pipeline import Pipeline
    from django_cachex.script import ScriptHelpers
    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT

# Alias to avoid shadowing by method names
_set = set

# Sentinel for distinguishing "key not found" from "key holds None"
_MISSING = object()


class CachexMixin:
    """Mixin providing cachex extension methods for any BaseCache subclass.

    Add this as a parent class alongside a Django cache backend to get:
    - Data structure operations (lists, sets, hashes, sorted sets)
    - Type detection
    - Admin support markers (``_cachex_support``)
    - Default ``scan()`` built on ``keys()``

    Subclasses should implement ``ttl()``, ``expire()``, ``persist()``,
    ``info()``, and ``keys()`` for full admin support.
    """

    _cachex_support: str = "cachex"

    # =========================================================================
    # TTL Operations (subclasses should override)
    # =========================================================================

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the TTL of a key in seconds."""
        raise NotSupportedError("ttl", self.__class__.__name__)

    def pttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the TTL of a key in milliseconds."""
        raise NotSupportedError("pttl", self.__class__.__name__)

    def persist(self, key: KeyT, version: int | None = None) -> bool:
        """Remove the TTL from a key."""
        raise NotSupportedError("persist", self.__class__.__name__)

    def expire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool:
        """Set expiry time on a key."""
        raise NotSupportedError("expire", self.__class__.__name__)

    def expire_at(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool:
        """Set expiry to an absolute time."""
        raise NotSupportedError("expire_at", self.__class__.__name__)

    def pexpire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool:
        """Set expiry time in milliseconds."""
        raise NotSupportedError("pexpire", self.__class__.__name__)

    def pexpire_at(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool:
        """Set expiry to an absolute time in milliseconds."""
        raise NotSupportedError("pexpire_at", self.__class__.__name__)

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List keys matching the pattern."""
        raise NotSupportedError("keys", self.__class__.__name__)

    def iter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]:
        """Iterate over keys matching pattern."""
        yield from self.keys(pattern, version=version)

    def scan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]:
        """Cursor-based key pagination built on keys()."""
        all_keys = sorted(self.keys(pattern, version=version))
        count = count or 100
        end_idx = cursor + count
        return (end_idx if end_idx < len(all_keys) else 0, all_keys[cursor:end_idx])

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        """Delete all keys matching pattern."""
        matching = self.keys(pattern, version=version)
        deleted = 0
        for key in matching:
            if self.delete(key, version=version):  # type: ignore[attr-defined]
                deleted += 1
        return deleted

    def rename(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key."""
        raise NotSupportedError("rename", self.__class__.__name__)

    def renamenx(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key only if the destination does not exist."""
        raise NotSupportedError("renamenx", self.__class__.__name__)

    def make_pattern(self, pattern: str, version: int | None = None) -> str:
        """Build a pattern for key matching."""
        raise NotSupportedError("make_pattern", self.__class__.__name__)

    def reverse_key(self, key: str) -> str:
        """Reverse a made key back to original."""
        raise NotSupportedError("reverse_key", self.__class__.__name__)

    # =========================================================================
    # Lock & Pipeline (not available for non-Redis backends)
    # =========================================================================

    def lock(
        self,
        key: str,
        version: int | None = None,
        timeout: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        """Return a Lock object for distributed locking."""
        raise NotSupportedError("lock", self.__class__.__name__)

    def pipeline(self, *, transaction: bool = True, version: int | None = None) -> Pipeline:
        """Create a pipeline for batched operations."""
        raise NotSupportedError("pipeline", self.__class__.__name__)

    # =========================================================================
    # Client Access & Info
    # =========================================================================

    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Get the underlying client."""
        raise NotSupportedError("get_client", self.__class__.__name__)

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get cache server information."""
        return {}

    def slowlog_get(self, count: int = 10) -> list[Any]:
        """Get slow query log entries."""
        return []

    def slowlog_len(self) -> int:
        """Get the number of entries in the slow query log."""
        return 0

    # =========================================================================
    # Lua Script Operations (not available for non-Redis backends)
    # =========================================================================

    def eval_script(
        self,
        script: str,
        *,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        pre_hook: ScriptHelpers | None = None,
        post_hook: ScriptHelpers | None = None,
        version: int | None = None,
    ) -> Any:
        """Execute a Lua script."""
        raise NotSupportedError("eval_script", self.__class__.__name__)

    async def aeval_script(
        self,
        script: str,
        *,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        pre_hook: ScriptHelpers | None = None,
        post_hook: ScriptHelpers | None = None,
        version: int | None = None,
    ) -> Any:
        """Execute a Lua script asynchronously."""
        raise NotSupportedError("aeval_script", self.__class__.__name__)

    # =========================================================================
    # Stream Operations (not available for non-Redis backends)
    # =========================================================================

    def xlen(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of entries in a stream."""
        raise NotSupportedError("xlen", self.__class__.__name__)

    # =========================================================================
    # Type Detection
    # =========================================================================

    def type(self, key: KeyT, version: int | None = None) -> KeyType | None:
        """Get the data type of a key by inspecting the stored Python value."""
        value = self.get(key, default=_MISSING, version=version)  # type: ignore[attr-defined]
        if value is _MISSING:
            return None
        if isinstance(value, list):
            return KeyType.LIST
        if isinstance(value, _set):
            return KeyType.SET
        if isinstance(value, dict) and all(isinstance(k, str) for k in value):
            return KeyType.HASH
        return KeyType.STRING

    # =========================================================================
    # TTL Helpers (used by data structure operations)
    # =========================================================================

    def _get_ttl_timeout(self, key: KeyT, version: int | None = None) -> int | None:
        """Convert ttl() result to a timeout value suitable for self.set()."""
        current_ttl = self.ttl(key, version=version)
        if current_ttl is None or current_ttl <= 0:
            return None
        return current_ttl

    # =========================================================================
    # List Helpers
    # =========================================================================

    def _get_list(self, key: KeyT, version: int | None = None) -> list[Any] | None:
        """Get the stored list value, or None if key doesn't exist."""
        value = self.get(key, default=_MISSING, version=version)  # type: ignore[attr-defined]
        if value is _MISSING:
            return None
        if not isinstance(value, list):
            msg = f"Key {key!r} does not hold a list value."
            raise TypeError(msg)
        return value

    # =========================================================================
    # List Operations
    # =========================================================================

    def lpush(self, key: KeyT, *values: Any, version: int | None = None) -> int:
        """Prepend values to the head of a list."""
        current = self._get_list(key, version=version)
        timeout = self._get_ttl_timeout(key, version=version)
        if current is None:
            current = []
        new_list = list(reversed(values)) + current
        self.set(key, new_list, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return len(new_list)

    def rpush(self, key: KeyT, *values: Any, version: int | None = None) -> int:
        """Append values to the tail of a list."""
        current = self._get_list(key, version=version)
        timeout = self._get_ttl_timeout(key, version=version)
        if current is None:
            current = []
        new_list = current + list(values)
        self.set(key, new_list, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return len(new_list)

    def lpop(self, key: KeyT, count: int | None = None, version: int | None = None) -> list[Any]:
        """Remove and return element(s) from the head of a list."""
        current = self._get_list(key, version=version)
        if not current:
            return []
        timeout = self._get_ttl_timeout(key, version=version)
        pop_count = count if count is not None else 1
        popped = current[:pop_count]
        remaining = current[pop_count:]
        if remaining:
            self.set(key, remaining, timeout=timeout, version=version)  # type: ignore[attr-defined]
        else:
            self.delete(key, version=version)  # type: ignore[attr-defined]
        return popped

    def rpop(self, key: KeyT, count: int | None = None, version: int | None = None) -> list[Any]:
        """Remove and return element(s) from the tail of a list."""
        current = self._get_list(key, version=version)
        if not current:
            return []
        timeout = self._get_ttl_timeout(key, version=version)
        pop_count = count if count is not None else 1
        popped = list(reversed(current[-pop_count:]))
        remaining = current[:-pop_count] if pop_count < len(current) else []
        if remaining:
            self.set(key, remaining, timeout=timeout, version=version)  # type: ignore[attr-defined]
        else:
            self.delete(key, version=version)  # type: ignore[attr-defined]
        return popped

    def lrange(self, key: KeyT, start: int, end: int, version: int | None = None) -> list[Any]:
        """Return a range of elements from a list (inclusive end, Redis-style)."""
        current = self._get_list(key, version=version)
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

    def llen(self, key: KeyT, version: int | None = None) -> int:
        """Return the length of a list."""
        current = self._get_list(key, version=version)
        if current is None:
            return 0
        return len(current)

    def lrem(  # noqa: PLR0912
        self,
        key: KeyT,
        count: int,
        value: Any,
        version: int | None = None,
    ) -> int:
        """Remove occurrences of value from a list."""
        current = self._get_list(key, version=version)
        if not current:
            return 0
        timeout = self._get_ttl_timeout(key, version=version)
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
                self.set(key, new_list, timeout=timeout, version=version)  # type: ignore[attr-defined]
            else:
                self.delete(key, version=version)  # type: ignore[attr-defined]
        return removed

    def ltrim(self, key: KeyT, start: int, end: int, version: int | None = None) -> bool:
        """Trim a list to the specified range (inclusive end, Redis-style)."""
        current = self._get_list(key, version=version)
        if current is None:
            return True
        timeout = self._get_ttl_timeout(key, version=version)
        length = len(current)
        if start < 0:
            start = max(length + start, 0)
        if end < 0:
            end = length + end
        if start >= length or end < start:
            self.delete(key, version=version)  # type: ignore[attr-defined]
            return True
        trimmed = current[start : end + 1]
        if trimmed:
            self.set(key, trimmed, timeout=timeout, version=version)  # type: ignore[attr-defined]
        else:
            self.delete(key, version=version)  # type: ignore[attr-defined]
        return True

    def lindex(self, key: KeyT, index: int, version: int | None = None) -> Any:
        """Get element at index in list."""
        current = self._get_list(key, version=version)
        if not current:
            return None
        try:
            return current[index]
        except IndexError:
            return None

    def lset(self, key: KeyT, index: int, value: Any, version: int | None = None) -> bool:
        """Set element at index in list."""
        current = self._get_list(key, version=version)
        if not current:
            msg = "no such key"
            raise ValueError(msg)
        try:
            current[index] = value
        except IndexError:
            msg = "index out of range"
            raise ValueError(msg) from None
        timeout = self._get_ttl_timeout(key, version=version)
        self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return True

    def linsert(self, key: KeyT, where: str, pivot: Any, value: Any, version: int | None = None) -> int:
        """Insert value before or after pivot in list."""
        current = self._get_list(key, version=version)
        if not current:
            return 0
        try:
            idx = current.index(pivot)
        except ValueError:
            return -1
        if where.upper() == "AFTER":
            idx += 1
        current.insert(idx, value)
        timeout = self._get_ttl_timeout(key, version=version)
        self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return len(current)

    def lpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None:
        """Find position(s) of element in list."""
        current = self._get_list(key, version=version)
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
            return positions[:count]
        return positions[0] if positions else None

    # =========================================================================
    # Set Helpers
    # =========================================================================

    def _get_set(self, key: KeyT, version: int | None = None) -> _set[Any] | None:
        """Get the stored set value, or None if key doesn't exist."""
        value = self.get(key, default=_MISSING, version=version)  # type: ignore[attr-defined]
        if value is _MISSING:
            return None
        if not isinstance(value, _set):
            msg = f"Key {key!r} does not hold a set value."
            raise TypeError(msg)
        return value

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(self, key: KeyT, *members: Any, version: int | None = None) -> int:
        """Add members to a set."""
        current = self._get_set(key, version=version)
        timeout = self._get_ttl_timeout(key, version=version)
        if current is None:
            current = _set()
        before = len(current)
        current.update(members)
        self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return len(current) - before

    def srem(self, key: KeyT, *members: Any, version: int | None = None) -> int:
        """Remove members from a set."""
        current = self._get_set(key, version=version)
        if not current:
            return 0
        timeout = self._get_ttl_timeout(key, version=version)
        removed = len(current.intersection(members))
        current.difference_update(members)
        if current:
            self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        else:
            self.delete(key, version=version)  # type: ignore[attr-defined]
        return removed

    def scard(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of members in a set."""
        current = self._get_set(key, version=version)
        return 0 if current is None else len(current)

    def sismember(self, key: KeyT, member: Any, version: int | None = None) -> bool:
        """Check if member is in set."""
        current = self._get_set(key, version=version)
        return False if current is None else member in current

    def smembers(self, key: KeyT, version: int | None = None) -> _set[Any]:
        """Get all members of a set."""
        current = self._get_set(key, version=version)
        return _set() if current is None else _set(current)

    def spop(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | _set[Any]:
        """Remove and return random member(s) from set."""
        current = self._get_set(key, version=version)
        if not current:
            return _set() if count is not None else None
        timeout = self._get_ttl_timeout(key, version=version)
        if count is None:
            member = random.choice(list(current))  # noqa: S311
            current.discard(member)
            if current:
                self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
            else:
                self.delete(key, version=version)  # type: ignore[attr-defined]
            return member
        pop_count = min(count, len(current))
        popped = _set(random.sample(list(current), pop_count))
        current.difference_update(popped)
        if current:
            self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        else:
            self.delete(key, version=version)  # type: ignore[attr-defined]
        return popped

    def srandmember(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | list[Any]:
        """Get random member(s) from set without removing."""
        current = self._get_set(key, version=version)
        if not current:
            return [] if count is not None else None
        if count is None:
            return random.choice(list(current))  # noqa: S311
        return random.sample(list(current), min(count, len(current)))

    def smismember(self, key: KeyT, *members: Any, version: int | None = None) -> list[bool]:
        """Check if multiple values are members of a set."""
        current = self._get_set(key, version=version)
        if current is None:
            return [False] * len(members)
        return [m in current for m in members]

    def sdiff(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _set[Any]:
        """Return the difference between sets."""
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        result: _set[Any] | None = None
        for k in keys:
            s = self._get_set(k, version=version) or _set()
            result = s if result is None else result - s
        return result or _set()

    def sinter(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _set[Any]:
        """Return the intersection of sets."""
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        result: _set[Any] | None = None
        for k in keys:
            s = self._get_set(k, version=version) or _set()
            result = s if result is None else result & s
        return result or _set()

    def sunion(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _set[Any]:
        """Return the union of sets."""
        if isinstance(keys, (str, bytes)):
            keys = [keys]
        result: _set[Any] = _set()
        for k in keys:
            s = self._get_set(k, version=version) or _set()
            result |= s
        return result

    # =========================================================================
    # Hash Helpers
    # =========================================================================

    def _get_hash(self, key: KeyT, version: int | None = None) -> dict[str, Any] | None:
        """Get the stored hash value, or None if key doesn't exist."""
        value = self.get(key, default=_MISSING, version=version)  # type: ignore[attr-defined]
        if value is _MISSING:
            return None
        if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
            msg = f"Key {key!r} does not hold a hash value."
            raise TypeError(msg)
        return value

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        """Set hash field(s)."""
        current = self._get_hash(key, version=version)
        timeout = self._get_ttl_timeout(key, version=version)
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
            for i in range(0, len(items), 2):
                f, v = items[i], items[i + 1]
                if f not in current:
                    added += 1
                current[f] = v
        self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return added

    def hdel(self, key: KeyT, *fields: str, version: int | None = None) -> int:
        """Delete hash fields."""
        current = self._get_hash(key, version=version)
        if not current:
            return 0
        timeout = self._get_ttl_timeout(key, version=version)
        removed = sum(1 for f in fields if f in current)
        for f in fields:
            current.pop(f, None)
        if removed > 0:
            if current:
                self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
            else:
                self.delete(key, version=version)  # type: ignore[attr-defined]
        return removed

    def hget(self, key: KeyT, field: str, version: int | None = None) -> Any:
        """Get value of field in hash."""
        current = self._get_hash(key, version=version)
        return None if current is None else current.get(field)

    def hgetall(self, key: KeyT, version: int | None = None) -> dict[str, Any]:
        """Get all fields and values in hash."""
        current = self._get_hash(key, version=version)
        return {} if current is None else dict(current)

    def hlen(self, key: KeyT, version: int | None = None) -> int:
        """Get number of fields in hash."""
        current = self._get_hash(key, version=version)
        return 0 if current is None else len(current)

    def hkeys(self, key: KeyT, version: int | None = None) -> list[str]:
        """Get all field names in hash."""
        current = self._get_hash(key, version=version)
        return [] if current is None else list(current.keys())

    def hvals(self, key: KeyT, version: int | None = None) -> list[Any]:
        """Get all values in hash."""
        current = self._get_hash(key, version=version)
        return [] if current is None else list(current.values())

    def hexists(self, key: KeyT, field: str, version: int | None = None) -> bool:
        """Check if field exists in hash."""
        current = self._get_hash(key, version=version)
        return False if current is None else field in current

    def hmget(self, key: KeyT, *fields: str, version: int | None = None) -> list[Any]:
        """Get values of multiple fields."""
        current = self._get_hash(key, version=version)
        if current is None:
            return [None] * len(fields)
        return [current.get(f) for f in fields]

    def hsetnx(self, key: KeyT, field: str, value: Any, version: int | None = None) -> bool:
        """Set field in hash only if it doesn't exist."""
        current = self._get_hash(key, version=version)
        timeout = self._get_ttl_timeout(key, version=version)
        if current is None:
            current = {}
        if field in current:
            return False
        current[field] = value
        self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return True

    def hincrby(self, key: KeyT, field: str, amount: int = 1, version: int | None = None) -> int:
        """Increment integer value of field in hash."""
        current = self._get_hash(key, version=version)
        timeout = self._get_ttl_timeout(key, version=version)
        if current is None:
            current = {}
        current[field] = int(current.get(field, 0)) + amount
        self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return current[field]

    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0, version: int | None = None) -> float:
        """Increment float value of field in hash."""
        current = self._get_hash(key, version=version)
        timeout = self._get_ttl_timeout(key, version=version)
        if current is None:
            current = {}
        current[field] = float(current.get(field, 0)) + amount
        self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return current[field]

    # =========================================================================
    # Sorted Set Helpers
    # =========================================================================

    def _get_zset(self, key: KeyT, version: int | None = None) -> dict[Any, float] | None:
        """Get the stored sorted set as a {member: score} dict, or None."""
        value = self.get(key, default=_MISSING, version=version)  # type: ignore[attr-defined]
        if value is _MISSING:
            return None
        if not isinstance(value, dict):
            msg = f"Key {key!r} does not hold a sorted set value."
            raise TypeError(msg)
        return value

    def _sorted_members(self, zset: dict[Any, float]) -> list[tuple[Any, float]]:
        """Return members sorted by (score, member)."""
        return sorted(zset.items(), key=lambda x: (x[1], str(x[0])))

    # =========================================================================
    # Sorted Set Operations
    # =========================================================================

    def zadd(
        self,
        key: KeyT,
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
        current = self._get_zset(key, version=version) or {}
        timeout = self._get_ttl_timeout(key, version=version)
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
        self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return changed

    def zcard(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of members in a sorted set."""
        current = self._get_zset(key, version=version)
        return 0 if current is None else len(current)

    def zscore(self, key: KeyT, member: Any, version: int | None = None) -> float | None:
        """Get the score of a member."""
        current = self._get_zset(key, version=version)
        return None if current is None else current.get(member)

    def zrank(self, key: KeyT, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member (lowest score = 0)."""
        current = self._get_zset(key, version=version)
        if current is None or member not in current:
            return None
        sorted_members = self._sorted_members(current)
        for i, (m, _) in enumerate(sorted_members):
            if m == member:
                return i
        return None

    def zrevrank(self, key: KeyT, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member (highest score = 0)."""
        current = self._get_zset(key, version=version)
        if current is None or member not in current:
            return None
        sorted_members = list(reversed(self._sorted_members(current)))
        for i, (m, _) in enumerate(sorted_members):
            if m == member:
                return i
        return None

    def zrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index."""
        current = self._get_zset(key, version=version)
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
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return a range of members by index, highest to lowest."""
        current = self._get_zset(key, version=version)
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
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        *,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return members with scores between min and max."""
        current = self._get_zset(key, version=version)
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

    def zrem(self, key: KeyT, *members: Any, version: int | None = None) -> int:
        """Remove members from a sorted set."""
        current = self._get_zset(key, version=version)
        if not current:
            return 0
        timeout = self._get_ttl_timeout(key, version=version)
        removed = sum(1 for m in members if m in current)
        for m in members:
            current.pop(m, None)
        if removed > 0:
            if current:
                self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
            else:
                self.delete(key, version=version)  # type: ignore[attr-defined]
        return removed

    def zincrby(self, key: KeyT, amount: float, member: Any, version: int | None = None) -> float:
        """Increment the score of a member."""
        current = self._get_zset(key, version=version) or {}
        timeout = self._get_ttl_timeout(key, version=version)
        current[member] = current.get(member, 0.0) + amount
        self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        return current[member]

    def zcount(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Count members with scores between min and max."""
        current = self._get_zset(key, version=version)
        if not current:
            return 0
        lo = float("-inf") if min_score == "-inf" else float(min_score)
        hi = float("inf") if max_score == "+inf" else float(max_score)
        return sum(1 for s in current.values() if lo <= s <= hi)

    def zpopmin(self, key: KeyT, count: int = 1, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        current = self._get_zset(key, version=version)
        if not current:
            return []
        timeout = self._get_ttl_timeout(key, version=version)
        sorted_members = self._sorted_members(current)
        popped = sorted_members[:count]
        for m, _ in popped:
            del current[m]
        if current:
            self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        else:
            self.delete(key, version=version)  # type: ignore[attr-defined]
        return popped

    def zpopmax(self, key: KeyT, count: int = 1, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        current = self._get_zset(key, version=version)
        if not current:
            return []
        timeout = self._get_ttl_timeout(key, version=version)
        sorted_members = list(reversed(self._sorted_members(current)))
        popped = sorted_members[:count]
        for m, _ in popped:
            del current[m]
        if current:
            self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        else:
            self.delete(key, version=version)  # type: ignore[attr-defined]
        return popped

    def zmscore(self, key: KeyT, *members: Any, version: int | None = None) -> list[float | None]:
        """Get the scores of multiple members."""
        current = self._get_zset(key, version=version)
        if current is None:
            return [None] * len(members)
        return [current.get(m) for m in members]

    def zremrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Remove members with scores between min and max."""
        current = self._get_zset(key, version=version)
        if not current:
            return 0
        lo = float("-inf") if min_score == "-inf" else float(min_score)
        hi = float("inf") if max_score == "+inf" else float(max_score)
        timeout = self._get_ttl_timeout(key, version=version)
        to_remove = [m for m, s in current.items() if lo <= s <= hi]
        for m in to_remove:
            del current[m]
        if to_remove:
            if current:
                self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
            else:
                self.delete(key, version=version)  # type: ignore[attr-defined]
        return len(to_remove)

    def zremrangebyrank(self, key: KeyT, start: int, end: int, version: int | None = None) -> int:
        """Remove members by rank range."""
        current = self._get_zset(key, version=version)
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
        timeout = self._get_ttl_timeout(key, version=version)
        to_remove = sorted_members[start : end + 1]
        for m, _ in to_remove:
            del current[m]
        if current:
            self.set(key, current, timeout=timeout, version=version)  # type: ignore[attr-defined]
        else:
            self.delete(key, version=version)  # type: ignore[attr-defined]
        return len(to_remove)
