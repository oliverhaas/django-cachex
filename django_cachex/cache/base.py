"""Cache-layer base class: the contract every cachex cache satisfies.

:class:`BaseCachex` extends Django's ``BaseCache`` with the cachex
extension surface (TTL ops, hashes, sets, sorted sets, lists, locks,
pipelines, scripting, …). Every cachex cache class inherits from it so the
contract is declared and enforced in one place. Streams are the one
exception: only ``xlen``/``axlen`` are declared here, and the rest of the
stream commands exist on :class:`~django_cachex.cache.resp.RespCache`
alone, so backend-agnostic callers probe for them (the admin uses
``hasattr(cache, "xrange")``).

Native backends (:class:`~django_cachex.cache.locmem.LocMemCache`,
:class:`~django_cachex.cache.database.DatabaseCache`,
:class:`~django_cachex.cache.resp.RespCache`) override the methods
with real implementations; methods left at the default raise
:class:`~django_cachex.exceptions.NotSupportedError`.
"""

import hashlib
import math
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache

from django_cachex.exceptions import NotSupportedError
from django_cachex.types import KeyType

CachexSupportLevel = Literal["cachex", "limited"]

# ``detail`` of the NotSupportedError the defaults below raise: what to use instead.
_USE_RESP = "use a Valkey or Redis backend"
_USE_TTL = "use ttl()"
_USE_EXPIRE = "use expire(), which takes a timedelta"
_USE_CLEAR = "use clear(), which deletes every key of this backend"
_USE_POP = "use lpop() or rpop(), which do not block"
_USE_SMEMBERS = "use smembers()"

# Alias for the ``set`` builtin shadowed by the ``set`` method (PEP 649 defers
# annotations at runtime, but type checkers still resolve them in class scope).
_set = set

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Mapping, Sequence
    from datetime import datetime, timedelta

    from django_cachex.adapters.pipeline import AsyncPipeline, Pipeline
    from django_cachex.script import ScriptHelpers


def _scan_hash(key: str) -> int:
    """Position of ``key`` in the default :meth:`BaseCachex.scan` order, the same in every process."""
    digest = hashlib.blake2b(key.encode("utf-8", "surrogatepass"), digest_size=8).digest()
    return int.from_bytes(digest, "big") >> 1


def _scan_page(keys: Iterable[str], cursor: int, count: int) -> tuple[int, list[str]]:
    """One page of a SCAN over ``keys`` in :func:`_scan_hash` order, starting at ``cursor``.

    The next cursor is the last returned position plus one, so deleting other
    keys between calls never skips a key. Keys sharing that last position stay
    on the page, which can then exceed ``count``.
    """
    if count < 1:
        # As Redis does: a page of none would hand back cursor 0, which ends a scan loop.
        msg = "count must be at least 1"
        raise ValueError(msg)
    ranked = sorted((position, key) for key in keys if (position := _scan_hash(key)) >= cursor)
    page: list[str] = []
    last = -1
    for position, key in ranked:
        if len(page) >= count and position != last:
            return last + 1, sorted(page)
        page.append(key)
        last = position
    return 0, sorted(page)


# The native backends raise ValueError with the server's reply text where
# Redis rejects an increment, so LocMem, database and RESP fail on the same input.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_NOT_AN_INTEGER = "value is not an integer or out of range"


def _is_int64(value: Any) -> bool:
    """``True`` for an ``int`` (not a ``bool``) that fits a signed 64-bit integer, as ``INCRBY`` needs."""
    return isinstance(value, int) and not isinstance(value, bool) and _INT64_MIN <= value <= _INT64_MAX


def _int64_sum(value: int, delta: int) -> int:
    """``value + delta``, rejecting a sum outside 64 bits as ``INCRBY`` and ``HINCRBY`` do."""
    total = value + delta
    if not _INT64_MIN <= total <= _INT64_MAX:
        msg = "increment or decrement would overflow"
        raise ValueError(msg)
    return total


def _float_sum(value: float, amount: Any) -> float:
    """``value + amount`` as ``HINCRBYFLOAT`` adds them: in decimal, so 0.1 + 0.2 is 0.3."""
    try:
        increment = float(amount)
    except TypeError, ValueError:
        increment = math.nan
    if math.isnan(increment):
        msg = "value is not a valid float"
        raise ValueError(msg)
    if math.isinf(increment):
        msg = "value is NaN or Infinity"
        raise ValueError(msg)
    total = float(Decimal(repr(value)) + Decimal(repr(increment)))
    if not math.isfinite(total):
        msg = "increment would produce NaN or Infinity"
        raise ValueError(msg)
    return total


# =============================================================================
# Base Extensions Interface
# =============================================================================


class BaseCachex(BaseCache):
    """Cache contract; declares the cachex extension surface on top of ``BaseCache``.

    Methods default to :class:`~django_cachex.exceptions.NotSupportedError`;
    native cachex backends override them with real implementations.
    Subclasses can pick which operations they support; the admin discovers
    support via the ``_cachex_support`` marker. Of the stream commands only
    ``xlen``/``axlen`` are declared here; callers that want more probe the
    concrete backend with ``hasattr``.
    """

    _cachex_support: CachexSupportLevel = "limited"

    # =========================================================================
    # Standard set / aset, widened with ``nx``/``xx``/``get`` flags
    #
    # Widens Django's ``BaseCache.set``/``aset`` signature with the
    # RESP-style flags. Backends that can't honor a flag raise
    # ``NotSupportedError`` from their override; the default delegates to
    # the inherited Django implementation when no flag is requested.
    # =========================================================================

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
        """Set a value in the cache.

        ``nx=True`` only sets if the key doesn't exist, ``xx=True`` only
        if it exists, ``get=True`` returns the prior value. Backends that
        don't support a given flag raise :class:`NotSupportedError`.
        """
        if nx or xx or get:
            raise NotSupportedError("set with nx/xx/get", self.__class__.__name__, detail=_USE_RESP)
        return super().set(key, value, timeout, version)

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
        """Async: set a value in the cache. See :meth:`set` for flag semantics."""
        if nx or xx or get:
            raise NotSupportedError("aset with nx/xx/get", self.__class__.__name__, detail=_USE_RESP)
        return await super().aset(key, value, timeout, version)

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def ttl(self, key: str, version: int | None = None) -> int | None:
        """Get the TTL of a key in seconds."""
        raise NotSupportedError("ttl", self.__class__.__name__, detail=_USE_RESP)

    def pttl(self, key: str, version: int | None = None) -> int | None:
        """Get the TTL of a key in milliseconds."""
        raise NotSupportedError("pttl", self.__class__.__name__, detail=_USE_TTL)

    def memory_usage(self, key: str, version: int | None = None, *, samples: int | None = None) -> int | None:
        """Bytes the key and its value take on the server, or None if the key is missing."""
        raise NotSupportedError("memory_usage", self.__class__.__name__, detail=_USE_RESP)

    def largest_keys(
        self,
        pattern: str = "*",
        count: int = 10,
        version: int | None = None,
        *,
        samples: int | None = None,
        itersize: int | None = None,
    ) -> list[tuple[str, int]]:
        """The ``count`` largest keys matching ``pattern`` as ``(key, bytes)``, largest first."""
        raise NotSupportedError("largest_keys", self.__class__.__name__, detail=_USE_RESP)

    def type(self, key: str, version: int | None = None) -> KeyType | None:
        """Get the data type of a key.

        Backends with no data structures beyond opaque values report every
        live key as ``STRING`` and a missing key as ``None``, matching Redis
        ``TYPE``.
        """
        return KeyType.STRING if self.has_key(key, version=version) else None

    def persist(self, key: str, version: int | None = None) -> bool:
        """Remove the TTL from a key."""
        raise NotSupportedError("persist", self.__class__.__name__, detail=_USE_RESP)

    def expire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        """Set expiry time on a key."""
        raise NotSupportedError("expire", self.__class__.__name__, detail=_USE_RESP)

    def expireat(self, key: str, when: int | datetime, version: int | None = None) -> bool:
        """Set expiry to an absolute time."""
        raise NotSupportedError("expireat", self.__class__.__name__, detail=_USE_EXPIRE)

    def pexpire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        """Set expiry time in milliseconds."""
        raise NotSupportedError("pexpire", self.__class__.__name__, detail=_USE_EXPIRE)

    def pexpireat(self, key: str, when: int | datetime, version: int | None = None) -> bool:
        """Set expiry to an absolute time in milliseconds."""
        raise NotSupportedError("pexpireat", self.__class__.__name__, detail=_USE_EXPIRE)

    def expiretime(self, key: str, version: int | None = None) -> int | None:
        """Get the absolute Unix timestamp (seconds) when a key will expire."""
        raise NotSupportedError("expiretime", self.__class__.__name__, detail=_USE_TTL)

    async def attl(self, key: str, version: int | None = None) -> int | None:
        """Async: get the TTL of a key in seconds."""
        raise NotSupportedError("attl", self.__class__.__name__, detail=_USE_RESP)

    async def apttl(self, key: str, version: int | None = None) -> int | None:
        """Async: get the TTL of a key in milliseconds."""
        raise NotSupportedError("apttl", self.__class__.__name__, detail=_USE_TTL)

    async def amemory_usage(self, key: str, version: int | None = None, *, samples: int | None = None) -> int | None:
        """Async: bytes the key and its value take on the server, or None if the key is missing."""
        raise NotSupportedError("amemory_usage", self.__class__.__name__, detail=_USE_RESP)

    async def alargest_keys(
        self,
        pattern: str = "*",
        count: int = 10,
        version: int | None = None,
        *,
        samples: int | None = None,
        itersize: int | None = None,
    ) -> list[tuple[str, int]]:
        """Async: the ``count`` largest keys matching ``pattern`` as ``(key, bytes)``, largest first."""
        raise NotSupportedError("alargest_keys", self.__class__.__name__, detail=_USE_RESP)

    async def atype(self, key: str, version: int | None = None) -> KeyType | None:
        """Async: get the data type of a key. See :meth:`type`."""
        return KeyType.STRING if await self.ahas_key(key, version=version) else None

    async def apersist(self, key: str, version: int | None = None) -> bool:
        """Async: remove the TTL from a key."""
        raise NotSupportedError("apersist", self.__class__.__name__, detail=_USE_RESP)

    async def aexpire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        """Async: set expiry time on a key."""
        raise NotSupportedError("aexpire", self.__class__.__name__, detail=_USE_RESP)

    async def aexpireat(self, key: str, when: int | datetime, version: int | None = None) -> bool:
        """Async: set expiry to an absolute time."""
        raise NotSupportedError("aexpireat", self.__class__.__name__, detail=_USE_EXPIRE)

    async def apexpire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        """Async: set expiry time in milliseconds."""
        raise NotSupportedError("apexpire", self.__class__.__name__, detail=_USE_EXPIRE)

    async def apexpireat(self, key: str, when: int | datetime, version: int | None = None) -> bool:
        """Async: set expiry to an absolute time in milliseconds."""
        raise NotSupportedError("apexpireat", self.__class__.__name__, detail=_USE_EXPIRE)

    async def aexpiretime(self, key: str, version: int | None = None) -> int | None:
        """Async: get the absolute Unix timestamp (seconds) when a key will expire."""
        raise NotSupportedError("aexpiretime", self.__class__.__name__, detail=_USE_TTL)

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List keys matching the pattern."""
        raise NotSupportedError("keys", self.__class__.__name__, detail=_USE_RESP)

    def iter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]:
        """Iterate over keys matching pattern."""
        raise NotSupportedError("iter_keys", self.__class__.__name__, detail=_USE_RESP)

    def scan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]:
        """Perform a single SCAN iteration using cursor-based pagination.

        The cursor is a position in a hash order of :meth:`keys`, so a key that
        exists for the whole iteration is returned even when other keys are
        deleted between calls. ``key_type`` filters client-side via :meth:`type`: backends reaching
        this default have no server-side ``TYPE`` filter, and silently
        ignoring the argument would make the admin's type filter a no-op.
        """
        keys = self.keys(pattern, version=version)
        if key_type is not None:
            keys = [k for k in keys if self.type(k, version=version) == key_type]
        return _scan_page(keys, cursor, 100 if count is None else count)

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        """Delete all keys matching pattern."""
        raise NotSupportedError("delete_pattern", self.__class__.__name__, detail=_USE_RESP)

    def clear_all_versions(self, itersize: int | None = None) -> int:
        """Delete every key under this cache's prefix, across all versions."""
        raise NotSupportedError("clear_all_versions", self.__class__.__name__, detail=_USE_CLEAR)

    def flush_db(self) -> bool:
        """Delete every key in the database (``FLUSHDB``), whatever its prefix."""
        raise NotSupportedError("flush_db", self.__class__.__name__, detail=_USE_CLEAR)

    def rename(
        self,
        src: str,
        dst: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key atomically. Raises :class:`KeyNotFoundError` when ``src`` does not exist."""
        raise NotSupportedError("rename", self.__class__.__name__, detail=_USE_RESP)

    def renamenx(
        self,
        src: str,
        dst: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key only if the destination does not exist.

        ``False`` means nothing was renamed, either because ``dst`` already
        exists or because ``src`` does not. Neither case raises.
        """
        raise NotSupportedError("renamenx", self.__class__.__name__, detail=_USE_RESP)

    def make_pattern(self, pattern: str, version: int | None = None) -> str:
        """Build a pattern for key matching."""
        raise NotSupportedError("make_pattern", self.__class__.__name__, detail=_USE_RESP)

    def reverse_key(self, key: str) -> str:
        """Reverse a made key back to original."""
        raise NotSupportedError("reverse_key", self.__class__.__name__, detail=_USE_RESP)

    async def akeys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """Async: list keys matching the pattern."""
        raise NotSupportedError("akeys", self.__class__.__name__, detail=_USE_RESP)

    def aiter_keys(  # async generator: see note on ``RespAdapterProtocol.aiter_keys`` for ``def`` (not ``async def``)
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> AsyncIterator[str]:
        """Async: iterate over keys matching pattern."""
        raise NotSupportedError("aiter_keys", self.__class__.__name__, detail=_USE_RESP)

    async def ascan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]:
        """Async: perform a single SCAN iteration using cursor-based pagination."""
        raise NotSupportedError("ascan", self.__class__.__name__, detail=_USE_RESP)

    async def adelete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        """Async: delete all keys matching pattern."""
        raise NotSupportedError("adelete_pattern", self.__class__.__name__, detail=_USE_RESP)

    async def aclear_all_versions(self, itersize: int | None = None) -> int:
        """Async: delete every key under this cache's prefix, across all versions."""
        raise NotSupportedError("aclear_all_versions", self.__class__.__name__, detail=_USE_CLEAR)

    async def aflush_db(self) -> bool:
        """Async: delete every key in the database (``FLUSHDB``), whatever its prefix."""
        raise NotSupportedError("aflush_db", self.__class__.__name__, detail=_USE_CLEAR)

    async def arename(
        self,
        src: str,
        dst: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Async: rename a key atomically. Raises :class:`KeyNotFoundError` when ``src`` does not exist."""
        raise NotSupportedError("arename", self.__class__.__name__, detail=_USE_RESP)

    async def arenamenx(
        self,
        src: str,
        dst: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Async: rename a key only if the destination does not exist.

        ``False`` means nothing was renamed, either because ``dst`` already
        exists or because ``src`` does not. Neither case raises.
        """
        raise NotSupportedError("arenamenx", self.__class__.__name__, detail=_USE_RESP)

    # =========================================================================
    # Lock & Pipeline
    # =========================================================================

    def _lock_detail(self) -> str:
        """``detail`` for a missing ``lock``: a one-slot semaphore does the job where this backend has one."""
        if type(self).semaphore is not BaseCachex.semaphore:
            return "use semaphore(key, 1)"
        return _USE_RESP

    def lock(
        self,
        key: str,
        version: int | None = None,
        lease: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        """Return a Lock object for distributed locking."""
        raise NotSupportedError("lock", self.__class__.__name__, detail=self._lock_detail())

    def pipeline(self, *, transaction: bool = True, version: int | None = None) -> Pipeline:
        """Create a pipeline for batched operations."""
        raise NotSupportedError("pipeline", self.__class__.__name__, detail=_USE_RESP)

    async def alock(
        self,
        key: str,
        version: int | None = None,
        lease: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        """Async: return an async Lock object for distributed locking."""
        raise NotSupportedError("alock", self.__class__.__name__, detail=self._lock_detail())

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
        """Return a weighted semaphore for concurrency gating."""
        raise NotSupportedError("semaphore", self.__class__.__name__, detail=_USE_RESP)

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
        """Async: return an async weighted semaphore."""
        raise NotSupportedError("asemaphore", self.__class__.__name__, detail=_USE_RESP)

    async def apipeline(self, *, transaction: bool = True, version: int | None = None) -> AsyncPipeline:
        """Create an async pipeline for batched operations."""
        raise NotSupportedError("apipeline", self.__class__.__name__, detail=_USE_RESP)

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        """Set field in hash."""
        raise NotSupportedError("hset", self.__class__.__name__, detail=_USE_RESP)

    def hdel(self, key: str, *fields: str, version: int | None = None) -> int:
        """Delete hash fields."""
        raise NotSupportedError("hdel", self.__class__.__name__, detail=_USE_RESP)

    def hlen(self, key: str, version: int | None = None) -> int:
        """Get number of fields in hash."""
        raise NotSupportedError("hlen", self.__class__.__name__, detail=_USE_RESP)

    def hkeys(self, key: str, version: int | None = None) -> list[str]:
        """Get all field names in hash."""
        raise NotSupportedError("hkeys", self.__class__.__name__, detail=_USE_RESP)

    def hexists(self, key: str, field: str, version: int | None = None) -> bool:
        """Check if field exists in hash."""
        raise NotSupportedError("hexists", self.__class__.__name__, detail=_USE_RESP)

    def hget(self, key: str, field: str, version: int | None = None) -> Any:
        """Get value of field in hash."""
        raise NotSupportedError("hget", self.__class__.__name__, detail=_USE_RESP)

    def hgetall(self, key: str, version: int | None = None) -> dict[str, Any]:
        """Get all fields and values in hash."""
        raise NotSupportedError("hgetall", self.__class__.__name__, detail=_USE_RESP)

    def hmget(self, key: str, *fields: str, version: int | None = None) -> list[Any]:
        """Get values of multiple fields."""
        raise NotSupportedError("hmget", self.__class__.__name__, detail=_USE_RESP)

    def hincrby(self, key: str, field: str, amount: int = 1, version: int | None = None) -> int:
        """Increment value of field in hash."""
        raise NotSupportedError("hincrby", self.__class__.__name__, detail=_USE_RESP)

    def hincrbyfloat(self, key: str, field: str, amount: float = 1.0, version: int | None = None) -> float:
        """Increment float value of field in hash."""
        raise NotSupportedError("hincrbyfloat", self.__class__.__name__, detail=_USE_RESP)

    def hsetnx(self, key: str, field: str, value: Any, version: int | None = None) -> bool:
        """Set field in hash only if it doesn't exist."""
        raise NotSupportedError("hsetnx", self.__class__.__name__, detail=_USE_RESP)

    def hvals(self, key: str, version: int | None = None) -> list[Any]:
        """Get all values in hash."""
        raise NotSupportedError("hvals", self.__class__.__name__, detail=_USE_RESP)

    async def ahset(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        """Async: set field in hash."""
        raise NotSupportedError("ahset", self.__class__.__name__, detail=_USE_RESP)

    async def ahdel(self, key: str, *fields: str, version: int | None = None) -> int:
        """Async: delete hash fields."""
        raise NotSupportedError("ahdel", self.__class__.__name__, detail=_USE_RESP)

    async def ahlen(self, key: str, version: int | None = None) -> int:
        """Async: get number of fields in hash."""
        raise NotSupportedError("ahlen", self.__class__.__name__, detail=_USE_RESP)

    async def ahkeys(self, key: str, version: int | None = None) -> list[str]:
        """Async: get all field names in hash."""
        raise NotSupportedError("ahkeys", self.__class__.__name__, detail=_USE_RESP)

    async def ahexists(self, key: str, field: str, version: int | None = None) -> bool:
        """Async: check if field exists in hash."""
        raise NotSupportedError("ahexists", self.__class__.__name__, detail=_USE_RESP)

    async def ahget(self, key: str, field: str, version: int | None = None) -> Any:
        """Async: get value of field in hash."""
        raise NotSupportedError("ahget", self.__class__.__name__, detail=_USE_RESP)

    async def ahgetall(self, key: str, version: int | None = None) -> dict[str, Any]:
        """Async: get all fields and values in hash."""
        raise NotSupportedError("ahgetall", self.__class__.__name__, detail=_USE_RESP)

    async def ahmget(self, key: str, *fields: str, version: int | None = None) -> list[Any]:
        """Async: get values of multiple fields."""
        raise NotSupportedError("ahmget", self.__class__.__name__, detail=_USE_RESP)

    async def ahincrby(self, key: str, field: str, amount: int = 1, version: int | None = None) -> int:
        """Async: increment value of field in hash."""
        raise NotSupportedError("ahincrby", self.__class__.__name__, detail=_USE_RESP)

    async def ahincrbyfloat(self, key: str, field: str, amount: float = 1.0, version: int | None = None) -> float:
        """Async: increment float value of field in hash."""
        raise NotSupportedError("ahincrbyfloat", self.__class__.__name__, detail=_USE_RESP)

    async def ahsetnx(self, key: str, field: str, value: Any, version: int | None = None) -> bool:
        """Async: set field in hash only if it doesn't exist."""
        raise NotSupportedError("ahsetnx", self.__class__.__name__, detail=_USE_RESP)

    async def ahvals(self, key: str, version: int | None = None) -> list[Any]:
        """Async: get all values in hash."""
        raise NotSupportedError("ahvals", self.__class__.__name__, detail=_USE_RESP)

    # =========================================================================
    # Hash Field Expiration
    # =========================================================================

    def hexpire(
        self,
        key: str,
        timeout: int | timedelta,
        *fields: str,
        version: int | None = None,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]:
        """Set a TTL in seconds on hash fields."""
        raise NotSupportedError("hexpire", self.__class__.__name__, detail=_USE_RESP)

    def hpexpire(
        self,
        key: str,
        timeout: int | timedelta,
        *fields: str,
        version: int | None = None,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]:
        """Set a TTL in milliseconds on hash fields."""
        raise NotSupportedError("hpexpire", self.__class__.__name__, detail=_USE_RESP)

    def hexpireat(
        self,
        key: str,
        when: int | datetime,
        *fields: str,
        version: int | None = None,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]:
        """Set an absolute expiry on hash fields."""
        raise NotSupportedError("hexpireat", self.__class__.__name__, detail=_USE_RESP)

    def hpexpireat(
        self,
        key: str,
        when: int | datetime,
        *fields: str,
        version: int | None = None,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]:
        """Set an absolute expiry in milliseconds on hash fields."""
        raise NotSupportedError("hpexpireat", self.__class__.__name__, detail=_USE_RESP)

    def httl(self, key: str, *fields: str, version: int | None = None) -> list[int | None]:
        """Get the TTL in seconds of hash fields."""
        raise NotSupportedError("httl", self.__class__.__name__, detail=_USE_RESP)

    def hpttl(self, key: str, *fields: str, version: int | None = None) -> list[int | None]:
        """Get the TTL in milliseconds of hash fields."""
        raise NotSupportedError("hpttl", self.__class__.__name__, detail=_USE_RESP)

    def hexpiretime(self, key: str, *fields: str, version: int | None = None) -> list[int | None]:
        """Get the absolute Unix timestamp (seconds) when hash fields expire."""
        raise NotSupportedError("hexpiretime", self.__class__.__name__, detail=_USE_RESP)

    def hpersist(self, key: str, *fields: str, version: int | None = None) -> list[int]:
        """Remove the TTL from hash fields."""
        raise NotSupportedError("hpersist", self.__class__.__name__, detail=_USE_RESP)

    def hsetex(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
        *,
        fnx: bool = False,
        fxx: bool = False,
        keepttl: bool = False,
    ) -> bool:
        """Set hash field(s) with a per-field TTL."""
        raise NotSupportedError("hsetex", self.__class__.__name__, detail=_USE_RESP)

    def hgetex(
        self,
        key: str,
        *fields: str,
        timeout: float | None = None,
        persist: bool = False,
        version: int | None = None,
    ) -> list[Any]:
        """Get hash field values and update their TTL."""
        raise NotSupportedError("hgetex", self.__class__.__name__, detail=_USE_RESP)

    async def ahexpire(
        self,
        key: str,
        timeout: int | timedelta,
        *fields: str,
        version: int | None = None,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]:
        """Async: set a TTL in seconds on hash fields."""
        raise NotSupportedError("ahexpire", self.__class__.__name__, detail=_USE_RESP)

    async def ahpexpire(
        self,
        key: str,
        timeout: int | timedelta,
        *fields: str,
        version: int | None = None,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]:
        """Async: set a TTL in milliseconds on hash fields."""
        raise NotSupportedError("ahpexpire", self.__class__.__name__, detail=_USE_RESP)

    async def ahexpireat(
        self,
        key: str,
        when: int | datetime,
        *fields: str,
        version: int | None = None,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]:
        """Async: set an absolute expiry on hash fields."""
        raise NotSupportedError("ahexpireat", self.__class__.__name__, detail=_USE_RESP)

    async def ahpexpireat(
        self,
        key: str,
        when: int | datetime,
        *fields: str,
        version: int | None = None,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]:
        """Async: set an absolute expiry in milliseconds on hash fields."""
        raise NotSupportedError("ahpexpireat", self.__class__.__name__, detail=_USE_RESP)

    async def ahttl(self, key: str, *fields: str, version: int | None = None) -> list[int | None]:
        """Async: get the TTL in seconds of hash fields."""
        raise NotSupportedError("ahttl", self.__class__.__name__, detail=_USE_RESP)

    async def ahpttl(self, key: str, *fields: str, version: int | None = None) -> list[int | None]:
        """Async: get the TTL in milliseconds of hash fields."""
        raise NotSupportedError("ahpttl", self.__class__.__name__, detail=_USE_RESP)

    async def ahexpiretime(self, key: str, *fields: str, version: int | None = None) -> list[int | None]:
        """Async: get the absolute Unix timestamp (seconds) when hash fields expire."""
        raise NotSupportedError("ahexpiretime", self.__class__.__name__, detail=_USE_RESP)

    async def ahpersist(self, key: str, *fields: str, version: int | None = None) -> list[int]:
        """Async: remove the TTL from hash fields."""
        raise NotSupportedError("ahpersist", self.__class__.__name__, detail=_USE_RESP)

    async def ahsetex(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
        *,
        fnx: bool = False,
        fxx: bool = False,
        keepttl: bool = False,
    ) -> bool:
        """Async: set hash field(s) with a per-field TTL."""
        raise NotSupportedError("ahsetex", self.__class__.__name__, detail=_USE_RESP)

    async def ahgetex(
        self,
        key: str,
        *fields: str,
        timeout: float | None = None,
        persist: bool = False,
        version: int | None = None,
    ) -> list[Any]:
        """Async: get hash field values and update their TTL."""
        raise NotSupportedError("ahgetex", self.__class__.__name__, detail=_USE_RESP)

    # =========================================================================
    # List Operations
    # =========================================================================

    def lpush(self, key: str, *values: Any, version: int | None = None) -> int:
        """Push values onto head of list."""
        raise NotSupportedError("lpush", self.__class__.__name__, detail=_USE_RESP)

    def rpush(self, key: str, *values: Any, version: int | None = None) -> int:
        """Push values onto tail of list."""
        raise NotSupportedError("rpush", self.__class__.__name__, detail=_USE_RESP)

    def lpop(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from head of list."""
        raise NotSupportedError("lpop", self.__class__.__name__, detail=_USE_RESP)

    def rpop(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from tail of list."""
        raise NotSupportedError("rpop", self.__class__.__name__, detail=_USE_RESP)

    def lrange(self, key: str, start: int, end: int, version: int | None = None) -> list[Any]:
        """Get a range of elements from list."""
        raise NotSupportedError("lrange", self.__class__.__name__, detail=_USE_RESP)

    def lindex(self, key: str, index: int, version: int | None = None) -> Any:
        """Get element at index in list."""
        raise NotSupportedError("lindex", self.__class__.__name__, detail=_USE_RESP)

    def llen(self, key: str, version: int | None = None) -> int:
        """Get length of list."""
        raise NotSupportedError("llen", self.__class__.__name__, detail=_USE_RESP)

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
        raise NotSupportedError("lpos", self.__class__.__name__, detail=_USE_RESP)

    def lmove(
        self,
        src: str,
        dst: str,
        wherefrom: str,
        whereto: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None:
        """Atomically move an element from one list to another."""
        raise NotSupportedError("lmove", self.__class__.__name__, detail=_USE_RESP)

    def lrem(self, key: str, count: int, value: Any, version: int | None = None) -> int:
        """Remove elements from a list."""
        raise NotSupportedError("lrem", self.__class__.__name__, detail=_USE_RESP)

    def ltrim(self, key: str, start: int, end: int, version: int | None = None) -> bool:
        """Trim list to specified range."""
        raise NotSupportedError("ltrim", self.__class__.__name__, detail=_USE_RESP)

    def lset(self, key: str, index: int, value: Any, version: int | None = None) -> bool:
        """Set element at index in list."""
        raise NotSupportedError("lset", self.__class__.__name__, detail=_USE_RESP)

    def linsert(self, key: str, where: str, pivot: Any, value: Any, version: int | None = None) -> int:
        """Insert value before or after pivot in list."""
        raise NotSupportedError("linsert", self.__class__.__name__, detail=_USE_RESP)

    def blpop(
        self,
        keys: str | Sequence[str],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list."""
        raise NotSupportedError("blpop", self.__class__.__name__, detail=_USE_POP)

    def brpop(
        self,
        keys: str | Sequence[str],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list."""
        raise NotSupportedError("brpop", self.__class__.__name__, detail=_USE_POP)

    def blmove(
        self,
        src: str,
        dst: str,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None:
        """Blocking atomically move element from one list to another."""
        raise NotSupportedError("blmove", self.__class__.__name__, detail=_USE_RESP)

    async def alpush(self, key: str, *values: Any, version: int | None = None) -> int:
        """Async: push values onto head of list."""
        raise NotSupportedError("alpush", self.__class__.__name__, detail=_USE_RESP)

    async def arpush(self, key: str, *values: Any, version: int | None = None) -> int:
        """Async: push values onto tail of list."""
        raise NotSupportedError("arpush", self.__class__.__name__, detail=_USE_RESP)

    async def alpop(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Async: remove and return element(s) from head of list."""
        raise NotSupportedError("alpop", self.__class__.__name__, detail=_USE_RESP)

    async def arpop(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Async: remove and return element(s) from tail of list."""
        raise NotSupportedError("arpop", self.__class__.__name__, detail=_USE_RESP)

    async def alrange(self, key: str, start: int, end: int, version: int | None = None) -> list[Any]:
        """Async: get a range of elements from list."""
        raise NotSupportedError("alrange", self.__class__.__name__, detail=_USE_RESP)

    async def alindex(self, key: str, index: int, version: int | None = None) -> Any:
        """Async: get element at index in list."""
        raise NotSupportedError("alindex", self.__class__.__name__, detail=_USE_RESP)

    async def allen(self, key: str, version: int | None = None) -> int:
        """Async: get length of list."""
        raise NotSupportedError("allen", self.__class__.__name__, detail=_USE_RESP)

    async def alpos(
        self,
        key: str,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None:
        """Async: find position(s) of element in list."""
        raise NotSupportedError("alpos", self.__class__.__name__, detail=_USE_RESP)

    async def almove(
        self,
        src: str,
        dst: str,
        wherefrom: str,
        whereto: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None:
        """Async: atomically move an element from one list to another."""
        raise NotSupportedError("almove", self.__class__.__name__, detail=_USE_RESP)

    async def alrem(self, key: str, count: int, value: Any, version: int | None = None) -> int:
        """Async: remove elements from a list."""
        raise NotSupportedError("alrem", self.__class__.__name__, detail=_USE_RESP)

    async def altrim(self, key: str, start: int, end: int, version: int | None = None) -> bool:
        """Async: trim list to specified range."""
        raise NotSupportedError("altrim", self.__class__.__name__, detail=_USE_RESP)

    async def alset(self, key: str, index: int, value: Any, version: int | None = None) -> bool:
        """Async: set element at index in list."""
        raise NotSupportedError("alset", self.__class__.__name__, detail=_USE_RESP)

    async def alinsert(self, key: str, where: str, pivot: Any, value: Any, version: int | None = None) -> int:
        """Async: insert value before or after pivot in list."""
        raise NotSupportedError("alinsert", self.__class__.__name__, detail=_USE_RESP)

    async def ablpop(
        self,
        keys: str | Sequence[str],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Async: blocking pop from head of list."""
        raise NotSupportedError("ablpop", self.__class__.__name__, detail=_USE_POP)

    async def abrpop(
        self,
        keys: str | Sequence[str],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Async: blocking pop from tail of list."""
        raise NotSupportedError("abrpop", self.__class__.__name__, detail=_USE_POP)

    async def ablmove(
        self,
        src: str,
        dst: str,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None:
        """Async: blocking atomically move element from one list to another."""
        raise NotSupportedError("ablmove", self.__class__.__name__, detail=_USE_RESP)

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(self, key: str, *members: Any, version: int | None = None) -> int:
        """Add members to a set."""
        raise NotSupportedError("sadd", self.__class__.__name__, detail=_USE_RESP)

    def scard(self, key: str, version: int | None = None) -> int:
        """Get the number of members in a set."""
        raise NotSupportedError("scard", self.__class__.__name__, detail=_USE_RESP)

    def sdiff(self, keys: str | Sequence[str], version: int | None = None) -> _set[Any]:
        """Return the difference between sets."""
        raise NotSupportedError("sdiff", self.__class__.__name__, detail=_USE_RESP)

    def sdiffstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the difference of sets."""
        raise NotSupportedError("sdiffstore", self.__class__.__name__, detail=_USE_RESP)

    def sinter(self, keys: str | Sequence[str], version: int | None = None) -> _set[Any]:
        """Return the intersection of sets."""
        raise NotSupportedError("sinter", self.__class__.__name__, detail=_USE_RESP)

    def sinterstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the intersection of sets."""
        raise NotSupportedError("sinterstore", self.__class__.__name__, detail=_USE_RESP)

    def sismember(self, key: str, member: Any, version: int | None = None) -> bool:
        """Check if member is in set."""
        raise NotSupportedError("sismember", self.__class__.__name__, detail=_USE_RESP)

    def smembers(self, key: str, version: int | None = None) -> _set[Any]:
        """Get all members of a set."""
        raise NotSupportedError("smembers", self.__class__.__name__, detail=_USE_RESP)

    def smove(
        self,
        src: str,
        dst: str,
        member: Any,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Move member from one set to another."""
        raise NotSupportedError("smove", self.__class__.__name__, detail=_USE_RESP)

    def spop(self, key: str, count: int | None = None, version: int | None = None) -> Any | _set[Any] | None:
        """Remove and return random member(s) from set."""
        raise NotSupportedError("spop", self.__class__.__name__, detail=_USE_RESP)

    def srandmember(self, key: str, count: int | None = None, version: int | None = None) -> Any | list[Any]:
        """Get random member(s) from set without removing."""
        raise NotSupportedError("srandmember", self.__class__.__name__, detail=_USE_RESP)

    def srem(self, key: str, *members: Any, version: int | None = None) -> int:
        """Remove members from a set."""
        raise NotSupportedError("srem", self.__class__.__name__, detail=_USE_RESP)

    def sunion(self, keys: str | Sequence[str], version: int | None = None) -> _set[Any]:
        """Return the union of sets."""
        raise NotSupportedError("sunion", self.__class__.__name__, detail=_USE_RESP)

    def sunionstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the union of sets."""
        raise NotSupportedError("sunionstore", self.__class__.__name__, detail=_USE_RESP)

    def smismember(self, key: str, *members: Any, version: int | None = None) -> list[bool]:
        """Check if multiple values are members of a set."""
        raise NotSupportedError("smismember", self.__class__.__name__, detail=_USE_RESP)

    def sscan(
        self,
        key: str,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, _set[Any]]:
        """Incrementally iterate over set members."""
        raise NotSupportedError("sscan", self.__class__.__name__, detail=_USE_SMEMBERS)

    def sscan_iter(
        self,
        key: str,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> Iterator[Any]:
        """Iterate over set members."""
        raise NotSupportedError("sscan_iter", self.__class__.__name__, detail=_USE_SMEMBERS)

    async def asadd(self, key: str, *members: Any, version: int | None = None) -> int:
        """Async: add members to a set."""
        raise NotSupportedError("asadd", self.__class__.__name__, detail=_USE_RESP)

    async def ascard(self, key: str, version: int | None = None) -> int:
        """Async: get the number of members in a set."""
        raise NotSupportedError("ascard", self.__class__.__name__, detail=_USE_RESP)

    async def asdiff(self, keys: str | Sequence[str], version: int | None = None) -> _set[Any]:
        """Async: return the difference between sets."""
        raise NotSupportedError("asdiff", self.__class__.__name__, detail=_USE_RESP)

    async def asdiffstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Async: store the difference of sets."""
        raise NotSupportedError("asdiffstore", self.__class__.__name__, detail=_USE_RESP)

    async def asinter(self, keys: str | Sequence[str], version: int | None = None) -> _set[Any]:
        """Async: return the intersection of sets."""
        raise NotSupportedError("asinter", self.__class__.__name__, detail=_USE_RESP)

    async def asinterstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Async: store the intersection of sets."""
        raise NotSupportedError("asinterstore", self.__class__.__name__, detail=_USE_RESP)

    async def asismember(self, key: str, member: Any, version: int | None = None) -> bool:
        """Async: check if member is in set."""
        raise NotSupportedError("asismember", self.__class__.__name__, detail=_USE_RESP)

    async def asmembers(self, key: str, version: int | None = None) -> _set[Any]:
        """Async: get all members of a set."""
        raise NotSupportedError("asmembers", self.__class__.__name__, detail=_USE_RESP)

    async def asmove(
        self,
        src: str,
        dst: str,
        member: Any,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Async: move member from one set to another."""
        raise NotSupportedError("asmove", self.__class__.__name__, detail=_USE_RESP)

    async def aspop(self, key: str, count: int | None = None, version: int | None = None) -> Any | _set[Any] | None:
        """Async: remove and return random member(s) from set."""
        raise NotSupportedError("aspop", self.__class__.__name__, detail=_USE_RESP)

    async def asrandmember(self, key: str, count: int | None = None, version: int | None = None) -> Any | list[Any]:
        """Async: get random member(s) from set without removing."""
        raise NotSupportedError("asrandmember", self.__class__.__name__, detail=_USE_RESP)

    async def asrem(self, key: str, *members: Any, version: int | None = None) -> int:
        """Async: remove members from a set."""
        raise NotSupportedError("asrem", self.__class__.__name__, detail=_USE_RESP)

    async def asunion(self, keys: str | Sequence[str], version: int | None = None) -> _set[Any]:
        """Async: return the union of sets."""
        raise NotSupportedError("asunion", self.__class__.__name__, detail=_USE_RESP)

    async def asunionstore(
        self,
        dest: str,
        keys: str | Sequence[str],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Async: store the union of sets."""
        raise NotSupportedError("asunionstore", self.__class__.__name__, detail=_USE_RESP)

    async def asmismember(self, key: str, *members: Any, version: int | None = None) -> list[bool]:
        """Async: check if multiple values are members of a set."""
        raise NotSupportedError("asmismember", self.__class__.__name__, detail=_USE_RESP)

    async def asscan(
        self,
        key: str,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, _set[Any]]:
        """Async: incrementally iterate over set members."""
        raise NotSupportedError("asscan", self.__class__.__name__, detail=_USE_SMEMBERS)

    def asscan_iter(  # async generator: see note on ``RespAdapterProtocol.aiter_keys`` for ``def`` (not ``async def``)
        self,
        key: str,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> AsyncIterator[Any]:
        """Async: iterate over set members."""
        raise NotSupportedError("asscan_iter", self.__class__.__name__, detail=_USE_SMEMBERS)

    # =========================================================================
    # Sorted Set Operations
    # =========================================================================

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
        raise NotSupportedError("zadd", self.__class__.__name__, detail=_USE_RESP)

    def zcard(self, key: str, version: int | None = None) -> int:
        """Get the number of members in a sorted set."""
        raise NotSupportedError("zcard", self.__class__.__name__, detail=_USE_RESP)

    def zcount(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Count members with scores between min and max."""
        raise NotSupportedError("zcount", self.__class__.__name__, detail=_USE_RESP)

    def zincrby(self, key: str, amount: float, member: Any, version: int | None = None) -> float:
        """Increment the score of a member."""
        raise NotSupportedError("zincrby", self.__class__.__name__, detail=_USE_RESP)

    def zpopmax(self, key: str, count: int | None = None, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        raise NotSupportedError("zpopmax", self.__class__.__name__, detail=_USE_RESP)

    def zpopmin(self, key: str, count: int | None = None, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        raise NotSupportedError("zpopmin", self.__class__.__name__, detail=_USE_RESP)

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
        raise NotSupportedError("zrange", self.__class__.__name__, detail=_USE_RESP)

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
        raise NotSupportedError("zrangebyscore", self.__class__.__name__, detail=_USE_RESP)

    def zrank(self, key: str, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member."""
        raise NotSupportedError("zrank", self.__class__.__name__, detail=_USE_RESP)

    def zrem(self, key: str, *members: Any, version: int | None = None) -> int:
        """Remove members from a sorted set."""
        raise NotSupportedError("zrem", self.__class__.__name__, detail=_USE_RESP)

    def zremrangebyscore(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Remove members with scores between min and max."""
        raise NotSupportedError("zremrangebyscore", self.__class__.__name__, detail=_USE_RESP)

    def zremrangebyrank(self, key: str, start: int, end: int, version: int | None = None) -> int:
        """Remove members by rank range."""
        raise NotSupportedError("zremrangebyrank", self.__class__.__name__, detail=_USE_RESP)

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
        raise NotSupportedError("zrevrange", self.__class__.__name__, detail=_USE_RESP)

    def zrevrangebyscore(
        self,
        key: str,
        max_score: float | str,
        min_score: float | str,
        *,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return members with scores between max and min, highest first."""
        raise NotSupportedError("zrevrangebyscore", self.__class__.__name__, detail=_USE_RESP)

    def zscore(self, key: str, member: Any, version: int | None = None) -> float | None:
        """Get the score of a member."""
        raise NotSupportedError("zscore", self.__class__.__name__, detail=_USE_RESP)

    def zrevrank(self, key: str, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member (highest score first)."""
        raise NotSupportedError("zrevrank", self.__class__.__name__, detail=_USE_RESP)

    def zmscore(self, key: str, *members: Any, version: int | None = None) -> list[float | None]:
        """Get the scores of multiple members."""
        raise NotSupportedError("zmscore", self.__class__.__name__, detail=_USE_RESP)

    async def azadd(
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
        """Async: add members to a sorted set."""
        raise NotSupportedError("azadd", self.__class__.__name__, detail=_USE_RESP)

    async def azcard(self, key: str, version: int | None = None) -> int:
        """Async: get the number of members in a sorted set."""
        raise NotSupportedError("azcard", self.__class__.__name__, detail=_USE_RESP)

    async def azcount(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Async: count members with scores between min and max."""
        raise NotSupportedError("azcount", self.__class__.__name__, detail=_USE_RESP)

    async def azincrby(self, key: str, amount: float, member: Any, version: int | None = None) -> float:
        """Async: increment the score of a member."""
        raise NotSupportedError("azincrby", self.__class__.__name__, detail=_USE_RESP)

    async def azpopmax(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[Any, float]]:
        """Async: remove and return members with highest scores."""
        raise NotSupportedError("azpopmax", self.__class__.__name__, detail=_USE_RESP)

    async def azpopmin(
        self,
        key: str,
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[Any, float]]:
        """Async: remove and return members with lowest scores."""
        raise NotSupportedError("azpopmin", self.__class__.__name__, detail=_USE_RESP)

    async def azrange(
        self,
        key: str,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Async: return a range of members by index."""
        raise NotSupportedError("azrange", self.__class__.__name__, detail=_USE_RESP)

    async def azrangebyscore(
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
        """Async: return members with scores between min and max."""
        raise NotSupportedError("azrangebyscore", self.__class__.__name__, detail=_USE_RESP)

    async def azrank(self, key: str, member: Any, version: int | None = None) -> int | None:
        """Async: get the rank of a member."""
        raise NotSupportedError("azrank", self.__class__.__name__, detail=_USE_RESP)

    async def azrem(self, key: str, *members: Any, version: int | None = None) -> int:
        """Async: remove members from a sorted set."""
        raise NotSupportedError("azrem", self.__class__.__name__, detail=_USE_RESP)

    async def azremrangebyscore(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Async: remove members with scores between min and max."""
        raise NotSupportedError("azremrangebyscore", self.__class__.__name__, detail=_USE_RESP)

    async def azremrangebyrank(self, key: str, start: int, end: int, version: int | None = None) -> int:
        """Async: remove members by rank range."""
        raise NotSupportedError("azremrangebyrank", self.__class__.__name__, detail=_USE_RESP)

    async def azrevrange(
        self,
        key: str,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Async: return a range of members by index, highest to lowest."""
        raise NotSupportedError("azrevrange", self.__class__.__name__, detail=_USE_RESP)

    async def azrevrangebyscore(
        self,
        key: str,
        max_score: float | str,
        min_score: float | str,
        *,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Async: return members with scores between max and min, highest first."""
        raise NotSupportedError("azrevrangebyscore", self.__class__.__name__, detail=_USE_RESP)

    async def azscore(self, key: str, member: Any, version: int | None = None) -> float | None:
        """Async: get the score of a member."""
        raise NotSupportedError("azscore", self.__class__.__name__, detail=_USE_RESP)

    async def azrevrank(self, key: str, member: Any, version: int | None = None) -> int | None:
        """Async: get the rank of a member (highest score first)."""
        raise NotSupportedError("azrevrank", self.__class__.__name__, detail=_USE_RESP)

    async def azmscore(self, key: str, *members: Any, version: int | None = None) -> list[float | None]:
        """Async: get the scores of multiple members."""
        raise NotSupportedError("azmscore", self.__class__.__name__, detail=_USE_RESP)

    # =========================================================================
    # Stream Operations (xlen/axlen only; the rest lives on RespCache)
    # =========================================================================

    def xlen(self, key: str, version: int | None = None) -> int:
        """Get the number of entries in a stream."""
        raise NotSupportedError("xlen", self.__class__.__name__, detail=_USE_RESP)

    async def axlen(self, key: str, version: int | None = None) -> int:
        """Async: get the number of entries in a stream."""
        raise NotSupportedError("axlen", self.__class__.__name__, detail=_USE_RESP)

    # =========================================================================
    # Client Access & Info
    # =========================================================================

    def get_client(self, key: str | None = None, *, write: bool = False) -> Any:
        """Get the underlying client."""
        raise NotSupportedError("get_client", self.__class__.__name__, detail=_USE_RESP)

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Get cache server information."""
        raise NotSupportedError("info", self.__class__.__name__, detail=_USE_RESP)

    def slowlog_get(self, count: int = 10) -> list[Any]:
        """Get slow query log entries."""
        raise NotSupportedError("slowlog_get", self.__class__.__name__, detail=_USE_RESP)

    def slowlog_len(self) -> int:
        """Get the number of entries in the slow query log."""
        raise NotSupportedError("slowlog_len", self.__class__.__name__, detail=_USE_RESP)

    # =========================================================================
    # Lua Script Operations
    # =========================================================================

    def eval_script(
        self,
        script: str,
        *,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        pre_hook: Callable[[ScriptHelpers, Sequence[Any], Sequence[Any]], tuple[list[Any], list[Any]]] | None = None,
        post_hook: Callable[[ScriptHelpers, Any], Any] | None = None,
        version: int | None = None,
    ) -> Any:
        """Execute a Lua script."""
        raise NotSupportedError("eval_script", self.__class__.__name__, detail=_USE_RESP)

    async def aeval_script(
        self,
        script: str,
        *,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        pre_hook: Callable[[ScriptHelpers, Sequence[Any], Sequence[Any]], tuple[list[Any], list[Any]]] | None = None,
        post_hook: Callable[[ScriptHelpers, Any], Any] | None = None,
        version: int | None = None,
    ) -> Any:
        """Execute a Lua script asynchronously."""
        raise NotSupportedError("aeval_script", self.__class__.__name__, detail=_USE_RESP)
