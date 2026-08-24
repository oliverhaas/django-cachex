"""Cachex LocMemCache: drop-in replacement for Django's LocMemCache.

Extends ``django.core.cache.backends.locmem.LocMemCache`` with the cachex
extension surface (lists, sets, hashes, sorted sets, TTL ops, key scanning,
admin info) implemented natively against the underlying ``OrderedDict``.

Notable design points:

- Compound ops acquire Django's own ``self._lock`` once per op.
- Tagged RESP collections (``_List``/``_Set``/``_Hash``/``_ZSet``) live as
  long-lived Python objects in a dedicated ``self._collections`` map;
  mutations are in-place under the lock, so collection ops avoid the
  per-call pickle round-trip Django's stock backend pays for opaque
  ``cache.set()`` values. Ops on opaque (string-typed) values still go
  through the normal pickled-bytes path in ``self._cache``.
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
import logging
import pickle
import random
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.core.cache.backends.locmem import LocMemCache as DjangoLocMemCache
from sortedcontainers import SortedList  # type: ignore[import-untyped]

from django_cachex.cache.base import BaseCachex, CachexSupportLevel
from django_cachex.exceptions import WrongTypeError
from django_cachex.types import KeyType
from django_cachex.utils import _deep_getsizeof, _format_bytes

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections import OrderedDict
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from threading import Lock

    from django_cachex.semaphore import _SemaphoreRegistry

# Sentinel for "key not found" vs "key holds None".
_MISSING = object()

# Alias for the ``set`` builtin shadowed by the ``set`` method (PEP 649 defers
# annotations at runtime, but type checkers still resolve them in class scope).
# Named ``_PySet`` to avoid colliding with ``LocMemCache._set`` (Django hook).
_PySet = set


class _List(list[Any]):
    """Tagged ``list`` marking a key as a RESP list.

    A plain ``list`` stored via ``cache.set()`` is opaque (RESP "string"); only
    values created by list ops (``lpush``/``rpush``/...) carry this tag and are
    accepted by the list typed accessor.
    """


class _Set(set[Any]):
    """Tagged ``set`` marking a key as a RESP set. See :class:`_List`."""


class _Hash(dict[str, Any]):
    """Tagged ``dict`` marking a key as a RESP hash. See :class:`_List`."""


def _zset_sort_key(item: tuple[float, str, Any]) -> tuple[float, str]:
    # Members of different types can share a str form (1 and "1") but don't
    # compare; ordering by the raw member would raise TypeError on score ties.
    return item[:2]


class _ZSet(dict[Any, float]):
    """Tagged sorted set: ``dict[member, score]`` + sorted-index sidecar.

    The sidecar is a :class:`sortedcontainers.SortedList` of
    ``(score, str(member), member)`` triples keyed by :func:`_zset_sort_key`,
    giving O(log N) ``zrange``/``zrank``/``zpopmin``/``zpopmax``; the raw
    member rides along only to disambiguate ``remove``/``index`` on ties.
    """

    def __init__(self, items: Any = None) -> None:
        if items is None:
            super().__init__()
        else:
            super().__init__(items)
        self._sorted: SortedList = SortedList(
            ((s, str(m), m) for m, s in self.items()),
            key=_zset_sort_key,
        )

    def __reduce__(self) -> tuple[Any, ...]:
        return (_make_zset, (dict(self),))

    def __setitem__(self, member: Any, score: float) -> None:
        if super().__contains__(member):
            old = super().__getitem__(member)
            self._sorted.remove((old, str(member), member))
        super().__setitem__(member, score)
        self._sorted.add((score, str(member), member))

    def __delitem__(self, member: Any) -> None:
        old = super().__getitem__(member)
        super().__delitem__(member)
        self._sorted.remove((old, str(member), member))

    def pop(self, member: Any, *args: Any) -> Any:
        if super().__contains__(member):
            old = super().__getitem__(member)
            super().__delitem__(member)
            self._sorted.remove((old, str(member), member))
            return old
        if args:
            return args[0]
        raise KeyError(member)

    def clear(self) -> None:
        super().clear()
        self._sorted.clear()

    def sorted_members(self) -> list[tuple[Any, float]]:
        """All ``(member, score)`` pairs in ``(score, str(member))`` order. O(N)."""
        return [(m, s) for s, _, m in self._sorted]

    def reversed_members(self) -> list[tuple[Any, float]]:
        """All ``(member, score)`` pairs in reverse sorted order. O(N)."""
        return [(m, s) for s, _, m in reversed(self._sorted)]

    def rank_of(self, member: Any) -> int | None:
        """Rank of ``member`` (lowest score = 0). O(log N). ``None`` if missing."""
        if not super().__contains__(member):
            return None
        score = super().__getitem__(member)
        return self._sorted.index((score, str(member), member))

    def revrank_of(self, member: Any) -> int | None:
        """Reverse rank (highest score = 0). O(log N). ``None`` if missing."""
        rank = self.rank_of(member)
        return None if rank is None else len(self._sorted) - 1 - rank

    def range_by_score(self, lo: float, hi: float) -> list[tuple[Any, float]]:
        """``(member, score)`` pairs where ``lo <= score <= hi``, in sorted order.

        Iterates the sorted index and short-circuits once ``score > hi``, so it
        runs in O(log N + k) when the matched range is small relative to N
        (worst case O(N) when the whole set is within range).
        """
        out: list[tuple[Any, float]] = []
        for s, _, m in self._sorted:
            if s < lo:
                continue
            if s > hi:
                break
            out.append((m, s))
        return out

    def count_by_score(self, lo: float, hi: float) -> int:
        """Number of members with ``lo <= score <= hi``. Same complexity as :meth:`range_by_score`."""
        n = 0
        for s, _, _m in self._sorted:
            if s < lo:
                continue
            if s > hi:
                break
            n += 1
        return n


def _make_zset(items: dict[Any, float]) -> _ZSet:
    """Pickle reconstructor; rebuilds the SortedList sidecar via ``__init__``."""
    return _ZSet(items)


# Django builds one backend instance per thread, so state is shared per
# LOCATION, like Django's module-level ``_caches``/``_locks``.
_collections: dict[str, dict[str, Any]] = {}
_semaphore_registries: dict[str, _SemaphoreRegistry] = {}


class LocMemCache(BaseCachex, DjangoLocMemCache):
    """LocMemCache with cachex extensions, implemented natively.

    Drop-in replacement for ``django.core.cache.backends.locmem.LocMemCache``.
    Standard cache ops (``get``/``set``/``delete``/...) are inherited
    unchanged. Cachex extensions read and write the underlying
    ``OrderedDict`` directly under Django's per-name lock, so compound
    ops are atomic within a single process.
    """

    _cachex_support: CachexSupportLevel = "cachex"

    # Type-only declarations for attributes Django sets in __init__ but
    # doesn't surface in stubs. ``_set`` and ``_delete`` are real overrides
    # below; no stub needed.
    if TYPE_CHECKING:
        _cache: OrderedDict[str, bytes]
        _expire_info: dict[str, float | None]
        _lock: Lock
        _max_entries: int
        _cull_frequency: int

        def _has_expired(self, key: str) -> bool: ...

    def __init__(self, name: str, params: dict[str, Any]) -> None:
        super().__init__(name, params)
        # Kept outside the pickled-bytes ``self._cache`` to mutate in place.
        self._collections: dict[str, Any] = _collections.setdefault(name, {})
        from django_cachex.semaphore import _SemaphoreRegistry

        self._semaphore_registry = _semaphore_registries.setdefault(name, _SemaphoreRegistry())

    # =========================================================================
    # Native helpers (caller must hold ``self._lock``)
    # =========================================================================

    def _native_get(self, internal_key: str) -> Any:
        """Return the live value for ``internal_key``, or ``_MISSING``.

        Caller must hold ``self._lock``. Tagged collections come back as
        the live in-memory object (mutations are visible immediately);
        opaque values are unpickled from ``self._cache``. Expired keys
        are evicted from whichever store they live in.
        """
        if self._has_expired(internal_key):
            self._delete(internal_key)
            return _MISSING
        if internal_key in self._collections:
            return self._collections[internal_key]
        if internal_key not in self._cache:
            return _MISSING
        return pickle.loads(self._cache[internal_key])  # noqa: S301

    def _native_write(self, internal_key: str, value: _List | _Set | _Hash | _ZSet) -> None:
        """Store a tagged collection for ``internal_key``.

        Caller must hold ``self._lock``. Tagged collections (``_List``/
        ``_Set``/``_Hash``/``_ZSet``) are stored by reference in
        ``self._collections``, no pickling, so subsequent in-place mutation
        through the same reference is visible to future reads. Existing keys
        keep their TTL untouched; new keys get no expiry, matching Redis'
        compound-op semantics (``LPUSH`` etc. don't touch TTL).
        """
        # Cull only on a first write: on a rewrite it could evict the key being
        # written, which ``setdefault`` would then resurrect without its TTL.
        if internal_key not in self._collections and len(self._cache) + len(self._collections) >= self._max_entries:
            self._cull()
        self._collections[internal_key] = value
        self._expire_info.setdefault(internal_key, None)

    # =========================================================================
    # Django interface overrides; keep ``_collections`` in sync with
    # Django's ``_cache`` / ``_expire_info`` lifecycle hooks.
    # =========================================================================

    def _set(self, key: str, value: Any, timeout: Any = DEFAULT_TIMEOUT) -> None:
        # Opaque ``cache.set()`` overwrites any prior collection at this key,
        # mirroring Redis ``SET`` (which replaces a list/hash/set/zset key
        # without complaint). django-stubs treats Django's ``_set``/``_delete``
        # as private to the concrete subclass and doesn't expose them on the
        # public type, silence ``attr-defined`` here. The call works at
        # runtime via the MRO.
        self._collections.pop(key, None)
        # Django's own cull trigger in ``_set`` counts ``_cache`` only;
        # enforce MAX_ENTRIES over both stores here.
        if len(self._cache) + len(self._collections) >= self._max_entries:
            self._cull()
        super()._set(key, value, timeout)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    def _cull(self) -> None:
        # Django's ``_cull`` extended to ``_collections``; ``_cache`` evicts
        # first because its LRU-ish order is more informative.
        if self._cull_frequency == 0:
            self._cache.clear()
            self._expire_info.clear()
            self._collections.clear()
            return
        count = (len(self._cache) + len(self._collections)) // self._cull_frequency
        for _ in range(count):
            if self._cache:
                key, _ = self._cache.popitem()
            elif self._collections:
                key = next(iter(self._collections))
                del self._collections[key]
            else:
                break
            self._expire_info.pop(key, None)

    def _delete(self, key: str) -> bool:
        had_collection = self._collections.pop(key, None) is not None
        had_pickled: bool = super()._delete(key)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        if had_collection and not had_pickled:
            # Django's ``_delete`` only touches ``_expire_info`` when the
            # key is in ``_cache``; clean up the collection's TTL entry
            # ourselves.
            self._expire_info.pop(key, None)
        return had_collection or had_pickled

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._expire_info.clear()
            self._collections.clear()

    def has_key(self, key: str, version: int | None = None) -> bool:
        internal_key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if self._has_expired(internal_key):
                self._delete(internal_key)
                return False
            return internal_key in self._cache or internal_key in self._collections

    def _internal_key(self, key: str, version: int | None = None) -> str:
        """Resolve a user key (with version) to the internal cache-dict key."""
        return self.make_and_validate_key(str(key), version=version)

    # =========================================================================
    # String Operations (RESP-faithful overrides)
    # =========================================================================

    def get(self, key: str, default: Any = None, version: int | None = None) -> Any:
        """Get a value, mirroring Redis ``GET``: WRONGTYPE on non-string keys.

        A key holding a RESP collection (list/set/hash/zset, written via the
        respective ops) cannot be retrieved through the string ``GET`` API.
        Redis raises ``WRONGTYPE`` and so do we.

        Django's ``get`` is inlined rather than called through ``super()``:
        ``self._lock`` is not reentrant, and releasing it between the
        collection check and the read lets a concurrent collection write turn
        the key into one, which Django's unguarded ``self._cache[key]`` would
        surface as a raw ``KeyError``.
        """
        internal_key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if self._has_expired(internal_key):
                self._delete(internal_key)
                return default
            if internal_key in self._collections:
                msg = f"WRONGTYPE Key {key!r} does not hold a string value."
                raise WrongTypeError(msg)
            if internal_key not in self._cache:
                return default
            pickled = self._cache[internal_key]
            self._cache.move_to_end(internal_key, last=False)
        return pickle.loads(pickled)  # noqa: S301

    def incr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Increment a value, mirroring Redis ``INCRBY``: WRONGTYPE on non-string keys.

        Django's inherited ``incr`` reads ``self._cache`` directly and would
        raise ``KeyError`` for a live key that holds a RESP collection; a
        missing key still raises ``ValueError`` per the Django cache contract.
        Inlined for the same lock-atomicity reason as :meth:`get`.
        """
        internal_key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if self._has_expired(internal_key):
                self._delete(internal_key)
                msg = f"Key '{key}' not found"
                raise ValueError(msg)
            if internal_key in self._collections:
                msg = f"WRONGTYPE Key {key!r} does not hold a string value."
                raise WrongTypeError(msg)
            if internal_key not in self._cache:
                msg = f"Key '{key}' not found"
                raise ValueError(msg)
            new_value = pickle.loads(self._cache[internal_key]) + delta  # noqa: S301
            self._cache[internal_key] = pickle.dumps(new_value, self.pickle_protocol)
            self._cache.move_to_end(internal_key, last=False)
        return new_value

    def get_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        """Get many values at once, mirroring Redis ``MGET``.

        ``BaseCache.get_many`` calls :meth:`get` per key, so a single
        collection key would abort the whole batch with ``WrongTypeError``.
        ``MGET`` reports a list/set/hash/zset key as nil instead, so RespCache
        simply omits it from the result; do the same here.
        """
        key_map = {self.make_and_validate_key(key, version=version): key for key in keys}
        found: dict[str, Any] = {}
        with self._lock:
            for internal_key, key in key_map.items():
                if self._has_expired(internal_key):
                    self._delete(internal_key)
                    continue
                if internal_key in self._collections or internal_key not in self._cache:
                    continue
                found[key] = self._cache[internal_key]
                self._cache.move_to_end(internal_key, last=False)
        return {key: pickle.loads(pickled) for key, pickled in found.items()}  # noqa: S301

    def incr_version(self, key: str, delta: int = 1, version: int | None = None) -> int:
        """Move a key to a new version, mirroring Redis ``RENAME``.

        ``BaseCache.incr_version`` is a ``get``/``set``/``delete`` round trip,
        so it would raise ``WrongTypeError`` on a collection key. Moving the
        entry works for any type and preserves the TTL, matching the
        ``RENAME``-based RespCache override.
        """
        if version is None:
            version = self.version
        old_key = self.make_and_validate_key(key, version=version)
        new_key = self.make_and_validate_key(key, version=version + delta)
        with self._lock:
            if self._has_expired(old_key):
                self._delete(old_key)
                msg = f"Key '{key}' not found"
                raise ValueError(msg)
            if old_key in self._collections:
                self._delete(new_key)
                self._collections[new_key] = self._collections.pop(old_key)
            elif old_key in self._cache:
                self._delete(new_key)
                self._cache[new_key] = self._cache.pop(old_key)
                self._cache.move_to_end(new_key, last=False)
            else:
                msg = f"Key '{key}' not found"
                raise ValueError(msg)
            self._expire_info[new_key] = self._expire_info.pop(old_key, None)
        return version + delta

    def set(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
    ) -> Any:
        """Set a value, with optional Redis-style ``nx``/``xx``/``get`` flags.

        With no flags, behaves like Django's ``LocMemCache.set``. ``nx``/``xx``
        without ``get`` return ``bool`` (whether the write happened). ``get``
        returns the prior value (or ``None``) regardless of whether the write
        happened. ``get`` on a key holding a RESP collection raises
        :class:`~django_cachex.exceptions.WrongTypeError`, mirroring Redis.
        """
        if not (nx or xx or get):
            return super().set(key, value, timeout, version)
        if nx and xx:
            msg = "nx and xx are mutually exclusive"
            raise ValueError(msg)
        internal_key = self.make_and_validate_key(key, version=version)
        pickled = pickle.dumps(value, self.pickle_protocol)
        with self._lock:
            if self._has_expired(internal_key):
                self._delete(internal_key)
            exists = self._key_present(internal_key)
            prior: Any = None
            if get and exists:
                if internal_key in self._collections:
                    msg = f"WRONGTYPE Key {key!r} does not hold a string value."
                    raise WrongTypeError(msg)
                prior = pickle.loads(self._cache[internal_key])  # noqa: S301
            if (nx and exists) or (xx and not exists):
                return prior if get else False
            self._set(internal_key, pickled, timeout)
            return prior if get else True

    async def aset(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
    ) -> Any:
        """Async: see :meth:`set`. Calls the sync method directly, like the rest
        of the async surface (see the note above ``attl``)."""
        return self.set(key, value, timeout, version, nx=nx, xx=xx, get=get)

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def _key_present(self, internal_key: str) -> bool:
        """Caller holds ``self._lock``. ``True`` if the key is in either store."""
        return internal_key in self._cache or internal_key in self._collections

    def ttl(self, key: str, version: int | None = None) -> int | None:
        """Get the TTL of a key in seconds.

        Returns ``-2`` if the key is missing, ``-1`` if it has no expiry,
        otherwise the integer seconds remaining (clamped at 0).
        """
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            if not self._key_present(internal_key):
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
            if not self._key_present(internal_key) or self._has_expired(internal_key):
                if self._has_expired(internal_key):
                    self._delete(internal_key)
                return False
            self._expire_info[internal_key] = time.time() + timeout_secs
            return True

    def persist(self, key: str, version: int | None = None) -> bool:
        """Remove the TTL from a key. Returns ``True`` if the key existed."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            if not self._key_present(internal_key) or self._has_expired(internal_key):
                if self._has_expired(internal_key):
                    self._delete(internal_key)
                return False
            self._expire_info[internal_key] = None
            return True

    # =========================================================================
    # Type Detection
    # =========================================================================

    def type(self, key: str, version: int | None = None) -> KeyType | None:
        """Get the RESP data type of a key.

        Mirrors Redis ``TYPE``: only values created by RESP ops (lpush, sadd,
        hset, zadd, ...) report as ``LIST``/``SET``/``HASH``/``ZSET``. Anything
        stored via plain ``cache.set()`` is opaque and reports as ``STRING``,
        regardless of the underlying Python type.
        """
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            value = self._native_get(internal_key)
        if value is _MISSING:
            return None
        if isinstance(value, _ZSet):
            return KeyType.ZSET
        if isinstance(value, _Hash):
            return KeyType.HASH
        if isinstance(value, _List):
            return KeyType.LIST
        if isinstance(value, _Set):
            return KeyType.SET
        return KeyType.STRING

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List user keys matching ``pattern`` (Redis-style glob).

        Internal keys are matched against the exact prefix ``make_key``
        produces for the requested ``version`` (default: this cache's
        ``self.version``) and stripped back to user keys, so results
        round-trip through the same key pipeline the RESP backends use,
        whatever ``KEY_PREFIX``/``KEY_FUNCTION`` produce. Expired but
        not-yet-culled entries are excluded.
        """
        all_keys = list(self._matching_keys(pattern, version=version))
        all_keys.sort()
        return all_keys

    def _matching_keys(self, pattern: str, version: int | None) -> Iterator[str]:
        """Yield user keys matching ``pattern``, unsorted.

        The internal key list is snapshotted under the lock (it has to be:
        yielding while holding a non-reentrant lock would deadlock any cache
        op the consumer runs), then filtered lazily.
        """
        prefix = self.make_key("", version=version)
        with self._lock:
            internal_keys = [k for k in [*self._cache, *self._collections] if not self._has_expired(k)]
        for internal_key in internal_keys:
            if not internal_key.startswith(prefix):
                continue
            user_key = internal_key.removeprefix(prefix)
            # ``fnmatch`` normcases both sides, which would make patterns
            # case-insensitive on Windows only; Redis globs never are.
            if pattern and pattern != "*" and not fnmatch.fnmatchcase(user_key, pattern):
                continue
            yield user_key

    def iter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]:
        """Iterate over keys matching pattern.

        ``itersize`` is accepted for parity with the RESP backends, where it
        sizes the ``SCAN`` batches; there is no server round trip here, so it
        has nothing to size and is ignored.
        """
        yield from self._matching_keys(pattern, version=version)

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
            key_count = len(self._cache) + len(self._collections)
            keys_with_expiry = sum(1 for exp in self._expire_info.values() if exp and exp > current_time)
            try:
                total_size = sum(_deep_getsizeof(v) for v in self._cache.values())
                total_size += sum(_deep_getsizeof(k) for k in self._cache)
                total_size += sum(_deep_getsizeof(v) for v in self._collections.values())
                total_size += sum(_deep_getsizeof(k) for k in self._collections)
            except Exception:
                logger.exception("LocMemCache.info(): _deep_getsizeof failed; reporting 0")
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

    def _typed_get_list(self, internal_key: str, key: str) -> _List | None:
        """Caller holds ``self._lock``. Returns the stored RESP list or None.

        Raises :class:`WrongTypeError` if the key holds any other type. A
        plain Python ``list`` set via ``cache.set()`` does not qualify; only
        values produced by ``lpush``/``rpush``/... do. ``key`` is the user key,
        reported in the error instead of the prefixed internal one.
        """
        value = self._native_get(internal_key)
        if value is _MISSING:
            return None
        if not isinstance(value, _List):
            msg = f"WRONGTYPE Key {key!r} does not hold a list value."
            raise WrongTypeError(msg)
        return value

    def lpush(self, key: str, *values: Any, version: int | None = None) -> int:
        """Prepend values to the head of a list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key, key)
            if current is None:
                # Redis never creates an empty list; an empty one here would be
                # unreachable (every list op bails on a falsy list) and immortal.
                if not values:
                    return 0
                current = _List(reversed(values))
                self._native_write(internal_key, current)
            else:
                # In-place prepend on the live list, no copy, no rewrite.
                current[0:0] = reversed(values)
            return len(current)

    def rpush(self, key: str, *values: Any, version: int | None = None) -> int:
        """Append values to the tail of a list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key, key)
            if current is None:
                # See :meth:`lpush`: no values, no key.
                if not values:
                    return 0
                current = _List(values)
                self._native_write(internal_key, current)
            else:
                current.extend(values)
            return len(current)

    @staticmethod
    def _validate_pop_count(count: int | None) -> None:
        """Reject a negative pop count before it slices from the wrong end.

        ``current[-count:]`` / ``current[:count]`` silently pop from the
        opposite end for a negative ``count``; Redis rejects it outright.
        """
        if count is not None and count < 0:
            msg = "value is out of range, must be positive"
            raise ValueError(msg)

    def lpop(self, key: str, count: int | None = None, version: int | None = None) -> Any | list[Any] | None:
        """Remove and return element(s) from the head of a list.

        Matches Redis ``LPOP``: a missing key returns ``None`` whether or
        not ``count`` is supplied. ``count=int`` returns a list when the
        key exists; ``count=None`` returns the bare value. A negative
        ``count`` is rejected, as Redis rejects it.
        """
        self._validate_pop_count(count)
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key, key)
            if not current:
                return None
            pop_count = count if count is not None else 1
            popped = list(current[:pop_count])
            del current[:pop_count]
            if not current:
                self._delete(internal_key)
            return popped if count is not None else popped[0]

    def rpop(self, key: str, count: int | None = None, version: int | None = None) -> Any | list[Any] | None:
        """Remove and return element(s) from the tail of a list.

        Matches Redis ``RPOP``: a missing key returns ``None`` whether or
        not ``count`` is supplied. ``count=int`` returns a list when the
        key exists; ``count=None`` returns the bare value. A negative
        ``count`` is rejected, as Redis rejects it.
        """
        self._validate_pop_count(count)
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key, key)
            if not current:
                return None
            if count == 0:
                # ``current[-0:]`` would slice the whole list; Redis pops
                # nothing and returns an empty array.
                return []
            pop_count = count if count is not None else 1
            popped = list(reversed(current[-pop_count:]))
            del current[-pop_count:]
            if not current:
                self._delete(internal_key)
            return popped if count is not None else popped[0]

    def lrange(self, key: str, start: int, end: int, version: int | None = None) -> list[Any]:
        """Return a range of elements from a list (inclusive end, Redis-style)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key, key)
            if not current:
                return []
            length = len(current)
            if start < 0:
                start = max(length + start, 0)
            if end < 0:
                end = length + end
            if start >= length or end < start:
                return []
            return list(current[start : end + 1])

    def llen(self, key: str, version: int | None = None) -> int:
        """Return the length of a list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key, key)
            return 0 if current is None else len(current)

    def lrem(self, key: str, count: int, value: Any, version: int | None = None) -> int:
        """Remove occurrences of value from a list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key, key)
            if not current:
                return 0
            length_before = len(current)
            if count >= 0:
                # ``count == 0`` removes all matches; positive ``count`` caps
                # the number of head-side removals.
                limit = count if count > 0 else length_before
                indices = [i for i, item in enumerate(current) if item == value][:limit]
            else:
                # Negative ``count`` removes from the tail.
                limit = -count
                rev_indices = [length_before - 1 - i for i, item in enumerate(reversed(current)) if item == value][
                    :limit
                ]
                indices = sorted(rev_indices)
            for i in reversed(indices):
                del current[i]
            removed = len(indices)
            if removed and not current:
                self._delete(internal_key)
            return removed

    def ltrim(self, key: str, start: int, end: int, version: int | None = None) -> bool:
        """Trim a list to the specified range (inclusive end, Redis-style)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key, key)
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
            del current[end + 1 :]
            del current[:start]
            if not current:
                self._delete(internal_key)
            return True

    def lindex(self, key: str, index: int, version: int | None = None) -> Any:
        """Get element at index in list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key, key)
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
            current = self._typed_get_list(internal_key, key)
            if not current:
                msg = "no such key"
                raise ValueError(msg)
            try:
                current[index] = value
            except IndexError:
                msg = "index out of range"
                raise ValueError(msg) from None
            return True

    def linsert(self, key: str, where: str, pivot: Any, value: Any, version: int | None = None) -> int:
        """Insert value before or after pivot in list."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_list(internal_key, key)
            if not current:
                return 0
            try:
                idx = current.index(pivot)
            except ValueError:
                return -1
            if where.upper() == "AFTER":
                idx += 1
            current.insert(idx, value)
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
            current = self._typed_get_list(internal_key, key)
            if not current:
                return [] if count is not None else None
            scan = current[:maxlen] if maxlen else list(current)
            positions = [i for i, v in enumerate(scan) if v == value]
        if rank is not None:
            if rank > 0:
                positions = positions[rank - 1 :]
            elif rank < 0:
                positions = list(reversed(positions))[abs(rank) - 1 :]
        if count is not None:
            return positions if count == 0 else positions[:count]
        return positions[0] if positions else None

    # =========================================================================
    # Set Operations
    # =========================================================================

    def _typed_get_set(self, internal_key: str, key: str) -> _Set | None:
        """Caller holds ``self._lock``. Returns the stored RESP set or None.

        Only values produced by ``sadd``/``srem``/... qualify; a plain
        Python ``set`` stored via ``cache.set()`` is rejected. ``key`` is the
        user key, reported in the error instead of the prefixed internal one.
        """
        value = self._native_get(internal_key)
        if value is _MISSING:
            return None
        if not isinstance(value, _Set):
            msg = f"WRONGTYPE Key {key!r} does not hold a set value."
            raise WrongTypeError(msg)
        return value

    def sadd(self, key: str, *members: Any, version: int | None = None) -> int:
        """Add members to a set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key, key)
            if current is None:
                current = _Set()
            before = len(current)
            current.update(members)
            if current:
                self._native_write(internal_key, current)
            return len(current) - before

    def srem(self, key: str, *members: Any, version: int | None = None) -> int:
        """Remove members from a set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key, key)
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
            current = self._typed_get_set(internal_key, key)
            return 0 if current is None else len(current)

    def sismember(self, key: str, member: Any, version: int | None = None) -> bool:
        """Check if member is in set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key, key)
            return False if current is None else member in current

    def smembers(self, key: str, version: int | None = None) -> _PySet[Any]:
        """Get all members of a set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key, key)
            return set() if current is None else set(current)

    def spop(self, key: str, count: int | None = None, version: int | None = None) -> Any | _PySet[Any] | None:
        """Remove and return random member(s) from set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key, key)
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
            current = self._typed_get_set(internal_key, key)
            if not current:
                return [] if count is not None else None
            members = list(current)
        if count is None:
            return random.choice(members)  # noqa: S311
        return random.sample(members, min(count, len(members)))

    def smismember(self, key: str, *members: Any, version: int | None = None) -> list[bool]:
        """Check if multiple values are members of a set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_set(internal_key, key)
            if current is None:
                return [False] * len(members)
            return [m in current for m in members]

    def _collect_sets(self, keys: str | Sequence[str], version: int | None = None) -> list[_PySet[Any]]:
        """Read multiple keys' sets under a single lock acquire.

        Returns defensive copies so callers can't race against in-place
        mutation of the live tagged sets.
        """
        if isinstance(keys, str):
            keys = [keys]
        internal_keys = [self._internal_key(k, version=version) for k in keys]
        with self._lock:
            return [set(self._typed_get_set(ik, k) or ()) for ik, k in zip(internal_keys, keys, strict=True)]

    def sdiff(self, keys: str | Sequence[str], version: int | None = None) -> _PySet[Any]:
        """Return the difference between sets."""
        sets = self._collect_sets(keys, version=version)
        if not sets:
            return set()
        result = sets[0]
        for s in sets[1:]:
            result = result - s
        return result

    def sinter(self, keys: str | Sequence[str], version: int | None = None) -> _PySet[Any]:
        """Return the intersection of sets."""
        sets = self._collect_sets(keys, version=version)
        if not sets:
            return set()
        result = sets[0]
        for s in sets[1:]:
            result = result & s
        return result

    def sunion(self, keys: str | Sequence[str], version: int | None = None) -> _PySet[Any]:
        """Return the union of sets."""
        result: set[Any] = set()
        for s in self._collect_sets(keys, version=version):
            result |= s
        return result

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def _typed_get_hash(self, internal_key: str, key: str) -> _Hash | None:
        """Caller holds ``self._lock``. Returns the stored RESP hash or None.

        Only values produced by ``hset``/``hdel``/... qualify; a plain
        Python ``dict`` stored via ``cache.set()`` is rejected. ``key`` is the
        user key, reported in the error instead of the prefixed internal one.
        """
        value = self._native_get(internal_key)
        if value is _MISSING:
            return None
        if not isinstance(value, _Hash):
            msg = f"WRONGTYPE Key {key!r} does not hold a hash value."
            raise WrongTypeError(msg)
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
            current = self._typed_get_hash(internal_key, key)
            if current is None:
                current = _Hash()
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
            if current:
                self._native_write(internal_key, current)
            return added

    def hdel(self, key: str, *fields: str, version: int | None = None) -> int:
        """Delete hash fields."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key, key)
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
            current = self._typed_get_hash(internal_key, key)
            return None if current is None else current.get(field)

    def hgetall(self, key: str, version: int | None = None) -> dict[str, Any]:
        """Get all fields and values in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key, key)
            return {} if current is None else dict(current)

    def hlen(self, key: str, version: int | None = None) -> int:
        """Get number of fields in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key, key)
            return 0 if current is None else len(current)

    def hkeys(self, key: str, version: int | None = None) -> list[str]:
        """Get all field names in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key, key)
            return [] if current is None else list(current.keys())

    def hvals(self, key: str, version: int | None = None) -> list[Any]:
        """Get all values in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key, key)
            return [] if current is None else list(current.values())

    def hexists(self, key: str, field: str, version: int | None = None) -> bool:
        """Check if field exists in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key, key)
            return False if current is None else field in current

    def hmget(self, key: str, *fields: str, version: int | None = None) -> list[Any]:
        """Get values of multiple fields."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key, key)
            if current is None:
                return [None] * len(fields)
            return [current.get(f) for f in fields]

    def hsetnx(self, key: str, field: str, value: Any, version: int | None = None) -> bool:
        """Set field in hash only if it doesn't exist."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key, key)
            if current is None:
                current = _Hash()
            if field in current:
                return False
            current[field] = value
            self._native_write(internal_key, current)
            return True

    def hincrby(self, key: str, field: str, amount: int = 1, version: int | None = None) -> int:
        """Increment integer value of field in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key, key)
            if current is None:
                current = _Hash()
            value = current.get(field, 0)
            if isinstance(value, bool) or not isinstance(value, int):
                msg = "hash value is not an integer"
                # ValueError, not TypeError: mirrors Redis's HINCRBY error,
                # matching the ValueError convention of ``lset``.
                raise ValueError(msg)  # noqa: TRY004
            current[field] = value + amount
            self._native_write(internal_key, current)
            return current[field]

    def hincrbyfloat(self, key: str, field: str, amount: float = 1.0, version: int | None = None) -> float:
        """Increment float value of field in hash."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_hash(internal_key, key)
            if current is None:
                current = _Hash()
            value = current.get(field, 0)
            if isinstance(value, bool) or not isinstance(value, int | float):
                msg = "hash value is not a float"
                raise ValueError(msg)  # noqa: TRY004
            current[field] = float(value) + amount
            self._native_write(internal_key, current)
            return current[field]

    # =========================================================================
    # Sorted Set Operations
    # =========================================================================

    def _typed_get_zset(self, internal_key: str, key: str) -> _ZSet | None:
        """Caller holds ``self._lock``. Returns the stored sorted set or None.

        ``key`` is the user key, reported in the error instead of the prefixed
        internal one.
        """
        value = self._native_get(internal_key)
        if value is _MISSING:
            return None
        if not isinstance(value, _ZSet):
            msg = f"WRONGTYPE Key {key!r} does not hold a sorted set value."
            raise WrongTypeError(msg)
        return value

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
            current = self._typed_get_zset(internal_key, key) or _ZSet()
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
            # Skip the write when every member was filtered out (e.g. ``xx``
            # on a missing key): Redis does not create an empty zset.
            if current:
                self._native_write(internal_key, current)
            return changed

    def zcard(self, key: str, version: int | None = None) -> int:
        """Get the number of members in a sorted set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key, key)
            return 0 if current is None else len(current)

    def zscore(self, key: str, member: Any, version: int | None = None) -> float | None:
        """Get the score of a member."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key, key)
            return None if current is None else current.get(member)

    def zrank(self, key: str, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member (lowest score = 0). O(log N)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key, key)
            return None if current is None else current.rank_of(member)

    def zrevrank(self, key: str, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member (highest score = 0). O(log N)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key, key)
            return None if current is None else current.revrank_of(member)

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
            current = self._typed_get_zset(internal_key, key)
            if not current:
                return []
            length = len(current)
            if start < 0:
                start = max(length + start, 0)
            if end < 0:
                end = length + end
            if start >= length or end < start:
                return []
            sliced = [(m, s) for s, _, m in current._sorted[start : end + 1]]
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
            current = self._typed_get_zset(internal_key, key)
            if not current:
                return []
            length = len(current)
            if start < 0:
                start = max(length + start, 0)
            if end < 0:
                end = length + end
            if start >= length or end < start:
                return []
            rev_start = max(length - end - 1, 0)
            rev_end = length - start
            sliced = [(m, s) for s, _, m in reversed(current._sorted[rev_start:rev_end])]
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
            current = self._typed_get_zset(internal_key, key)
            if not current:
                return []
            lo = float("-inf") if min_score == "-inf" else float(min_score)
            hi = float("inf") if max_score == "+inf" else float(max_score)
            filtered = current.range_by_score(lo, hi)
        if start is not None and num is not None:
            filtered = filtered[start : start + num]
        if withscores:
            return filtered
        return [m for m, _ in filtered]

    def zrem(self, key: str, *members: Any, version: int | None = None) -> int:
        """Remove members from a sorted set."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key, key)
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
            current = self._typed_get_zset(internal_key, key) or _ZSet()
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
            current = self._typed_get_zset(internal_key, key)
            if not current:
                return 0
            lo = float("-inf") if min_score == "-inf" else float(min_score)
            hi = float("inf") if max_score == "+inf" else float(max_score)
            return current.count_by_score(lo, hi)

    def zpopmin(self, key: str, count: int | None = None, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores. O((log N) * count)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key, key)
            if not current:
                return []
            n = 1 if count is None else count
            popped = [(m, s) for s, _, m in current._sorted[:n]]
            for m, _ in popped:
                del current[m]
            if current:
                self._native_write(internal_key, current)
            else:
                self._delete(internal_key)
            return popped

    def zpopmax(self, key: str, count: int | None = None, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores. O((log N) * count)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key, key)
            if not current:
                return []
            n = 1 if count is None else count
            if n == 0:
                # ``_sorted[-0:]`` would slice the whole zset; Redis pops
                # nothing and returns an empty array.
                return []
            tail = current._sorted[-n:] if n <= len(current) else list(current._sorted)
            popped = [(m, s) for s, _, m in reversed(tail)]
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
            current = self._typed_get_zset(internal_key, key)
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
            current = self._typed_get_zset(internal_key, key)
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
        """Remove members by rank range. O((log N) * k)."""
        internal_key = self._internal_key(key, version=version)
        with self._lock:
            current = self._typed_get_zset(internal_key, key)
            if not current:
                return 0
            length = len(current)
            if start < 0:
                start = max(length + start, 0)
            if end < 0:
                end = length + end
            if start >= length or end < start:
                return 0
            to_remove = [m for _s, _k, m in current._sorted[start : end + 1]]
            for m in to_remove:
                del current[m]
            if current:
                self._native_write(internal_key, current)
            else:
                self._delete(internal_key)
            return len(to_remove)

    # =========================================================================
    # Semaphore
    # =========================================================================

    def semaphore(
        self,
        key: str,
        capacity: int,
        *,
        weight: int = 1,
        version: int | None = None,
        lease: float | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Return an in-process weighted semaphore scoped to this cache.

        The returned :class:`~django_cachex.semaphore.Semaphore` exposes
        paired sync/async methods (``acquire``/``aacquire``, ``release``/
        ``arelease``), so the same instance works from sync or async code.

        The ``lease`` parameter is accepted for API parity with the RESP
        backend but is ignored: in-process release on ``__exit__`` is
        reliable.
        """
        from django_cachex.semaphore import Semaphore

        full_key = self.make_and_validate_key(key, version=version)
        return Semaphore(
            full_key,
            capacity=capacity,
            weight=weight,
            lease=lease,
            timeout=timeout,
            _registry=self._semaphore_registry,
        )

    async def asemaphore(
        self,
        key: str,
        capacity: int,
        *,
        weight: int = 1,
        version: int | None = None,
        lease: float | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Async factory for :meth:`semaphore`.

        ``async def`` for parity with RESP ``asemaphore``; constructs
        synchronously since the local backend has no async I/O. Use as
        ``async with await cache.asemaphore(...) as sem:`` in async views.
        """
        return self.semaphore(
            key,
            capacity,
            weight=weight,
            version=version,
            lease=lease,
            timeout=timeout,
        )

    # =========================================================================
    # Async surface
    # =========================================================================
    # Each ``a*`` calls its sync twin directly. The per-LOCATION ``_lock`` can
    # block the loop under contention, bounded by short in-memory sections.

    async def attl(self, *args: Any, **kwargs: Any) -> Any:
        return self.ttl(*args, **kwargs)

    async def atype(self, *args: Any, **kwargs: Any) -> Any:
        return self.type(*args, **kwargs)

    async def apersist(self, *args: Any, **kwargs: Any) -> Any:
        return self.persist(*args, **kwargs)

    async def aexpire(self, *args: Any, **kwargs: Any) -> Any:
        return self.expire(*args, **kwargs)

    async def akeys(self, *args: Any, **kwargs: Any) -> Any:
        return self.keys(*args, **kwargs)

    async def aiter_keys(self, *args: Any, **kwargs: Any) -> Any:
        for key in self.iter_keys(*args, **kwargs):
            yield key

    async def adelete_pattern(self, *args: Any, **kwargs: Any) -> Any:
        return self.delete_pattern(*args, **kwargs)

    async def alpush(self, *args: Any, **kwargs: Any) -> Any:
        return self.lpush(*args, **kwargs)

    async def arpush(self, *args: Any, **kwargs: Any) -> Any:
        return self.rpush(*args, **kwargs)

    async def alpop(self, *args: Any, **kwargs: Any) -> Any:
        return self.lpop(*args, **kwargs)

    async def arpop(self, *args: Any, **kwargs: Any) -> Any:
        return self.rpop(*args, **kwargs)

    async def alrange(self, *args: Any, **kwargs: Any) -> Any:
        return self.lrange(*args, **kwargs)

    async def allen(self, *args: Any, **kwargs: Any) -> Any:
        return self.llen(*args, **kwargs)

    async def alrem(self, *args: Any, **kwargs: Any) -> Any:
        return self.lrem(*args, **kwargs)

    async def altrim(self, *args: Any, **kwargs: Any) -> Any:
        return self.ltrim(*args, **kwargs)

    async def alindex(self, *args: Any, **kwargs: Any) -> Any:
        return self.lindex(*args, **kwargs)

    async def alset(self, *args: Any, **kwargs: Any) -> Any:
        return self.lset(*args, **kwargs)

    async def alinsert(self, *args: Any, **kwargs: Any) -> Any:
        return self.linsert(*args, **kwargs)

    async def alpos(self, *args: Any, **kwargs: Any) -> Any:
        return self.lpos(*args, **kwargs)

    async def asadd(self, *args: Any, **kwargs: Any) -> Any:
        return self.sadd(*args, **kwargs)

    async def asrem(self, *args: Any, **kwargs: Any) -> Any:
        return self.srem(*args, **kwargs)

    async def ascard(self, *args: Any, **kwargs: Any) -> Any:
        return self.scard(*args, **kwargs)

    async def asismember(self, *args: Any, **kwargs: Any) -> Any:
        return self.sismember(*args, **kwargs)

    async def asmembers(self, *args: Any, **kwargs: Any) -> Any:
        return self.smembers(*args, **kwargs)

    async def aspop(self, *args: Any, **kwargs: Any) -> Any:
        return self.spop(*args, **kwargs)

    async def asrandmember(self, *args: Any, **kwargs: Any) -> Any:
        return self.srandmember(*args, **kwargs)

    async def asmismember(self, *args: Any, **kwargs: Any) -> Any:
        return self.smismember(*args, **kwargs)

    async def asdiff(self, *args: Any, **kwargs: Any) -> Any:
        return self.sdiff(*args, **kwargs)

    async def asinter(self, *args: Any, **kwargs: Any) -> Any:
        return self.sinter(*args, **kwargs)

    async def asunion(self, *args: Any, **kwargs: Any) -> Any:
        return self.sunion(*args, **kwargs)

    async def ahset(self, *args: Any, **kwargs: Any) -> Any:
        return self.hset(*args, **kwargs)

    async def ahdel(self, *args: Any, **kwargs: Any) -> Any:
        return self.hdel(*args, **kwargs)

    async def ahget(self, *args: Any, **kwargs: Any) -> Any:
        return self.hget(*args, **kwargs)

    async def ahgetall(self, *args: Any, **kwargs: Any) -> Any:
        return self.hgetall(*args, **kwargs)

    async def ahlen(self, *args: Any, **kwargs: Any) -> Any:
        return self.hlen(*args, **kwargs)

    async def ahkeys(self, *args: Any, **kwargs: Any) -> Any:
        return self.hkeys(*args, **kwargs)

    async def ahvals(self, *args: Any, **kwargs: Any) -> Any:
        return self.hvals(*args, **kwargs)

    async def ahexists(self, *args: Any, **kwargs: Any) -> Any:
        return self.hexists(*args, **kwargs)

    async def ahmget(self, *args: Any, **kwargs: Any) -> Any:
        return self.hmget(*args, **kwargs)

    async def ahsetnx(self, *args: Any, **kwargs: Any) -> Any:
        return self.hsetnx(*args, **kwargs)

    async def ahincrby(self, *args: Any, **kwargs: Any) -> Any:
        return self.hincrby(*args, **kwargs)

    async def ahincrbyfloat(self, *args: Any, **kwargs: Any) -> Any:
        return self.hincrbyfloat(*args, **kwargs)

    async def azadd(self, *args: Any, **kwargs: Any) -> Any:
        return self.zadd(*args, **kwargs)

    async def azcard(self, *args: Any, **kwargs: Any) -> Any:
        return self.zcard(*args, **kwargs)

    async def azscore(self, *args: Any, **kwargs: Any) -> Any:
        return self.zscore(*args, **kwargs)

    async def azrank(self, *args: Any, **kwargs: Any) -> Any:
        return self.zrank(*args, **kwargs)

    async def azrevrank(self, *args: Any, **kwargs: Any) -> Any:
        return self.zrevrank(*args, **kwargs)

    async def azrange(self, *args: Any, **kwargs: Any) -> Any:
        return self.zrange(*args, **kwargs)

    async def azrevrange(self, *args: Any, **kwargs: Any) -> Any:
        return self.zrevrange(*args, **kwargs)

    async def azrangebyscore(self, *args: Any, **kwargs: Any) -> Any:
        return self.zrangebyscore(*args, **kwargs)

    async def azrem(self, *args: Any, **kwargs: Any) -> Any:
        return self.zrem(*args, **kwargs)

    async def azincrby(self, *args: Any, **kwargs: Any) -> Any:
        return self.zincrby(*args, **kwargs)

    async def azcount(self, *args: Any, **kwargs: Any) -> Any:
        return self.zcount(*args, **kwargs)

    async def azpopmin(self, *args: Any, **kwargs: Any) -> Any:
        return self.zpopmin(*args, **kwargs)

    async def azpopmax(self, *args: Any, **kwargs: Any) -> Any:
        return self.zpopmax(*args, **kwargs)

    async def azmscore(self, *args: Any, **kwargs: Any) -> Any:
        return self.zmscore(*args, **kwargs)

    async def azremrangebyrank(self, *args: Any, **kwargs: Any) -> Any:
        return self.zremrangebyrank(*args, **kwargs)

    async def azremrangebyscore(self, *args: Any, **kwargs: Any) -> Any:
        return self.zremrangebyscore(*args, **kwargs)
