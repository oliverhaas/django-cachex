"""Cache-layer base class — the contract every cachex cache satisfies.

:class:`BaseCachex` extends Django's ``BaseCache`` with the cachex
extension surface (TTL ops, hashes, sets, sorted sets, lists, streams,
locks, pipelines, …). Every cachex cache class inherits from it so the
contract is declared and enforced in one place. Native backends
(:class:`~django_cachex.cache.locmem.LocMemCache`,
:class:`~django_cachex.cache.database.DatabaseCache`,
:class:`~django_cachex.cache.default.KeyValueCache`) override the methods
with real implementations; :class:`~django_cachex.cache.mixin.CachexCompat`
provides emulated impls. Methods left at the default raise
:class:`~django_cachex.exceptions.NotSupportedError`; the admin uses
``hasattr`` / ``try-except NotSupportedError`` to detect support.
"""

from typing import TYPE_CHECKING, Any

from django.core.cache.backends.base import BaseCache

from django_cachex.exceptions import NotSupportedError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from django_cachex.adapters.pipeline import Pipeline
    from django_cachex.script import ScriptHelpers
    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT

# Alias to avoid shadowing by method names
_set = set


# =============================================================================
# Base Extensions Interface
# =============================================================================


class BaseCachex(BaseCache):
    """Cache contract — declares the full cachex extension surface on top of ``BaseCache``.

    Methods default to :class:`~django_cachex.exceptions.NotSupportedError`;
    native cachex backends and :class:`~django_cachex.cache.mixin.CachexCompat`
    override them with real implementations. Subclasses can pick which
    operations they support — the admin discovers support via
    ``hasattr`` / ``try-except NotSupportedError``.
    """

    _cachex_support: str = "limited"

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the TTL of a key in seconds."""
        raise NotSupportedError("ttl", self.__class__.__name__)

    def pttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Get the TTL of a key in milliseconds."""
        raise NotSupportedError("pttl", self.__class__.__name__)

    def type(self, key: KeyT, version: int | None = None) -> KeyType | None:
        """Get the data type of a key."""
        return KeyType.STRING

    def persist(self, key: KeyT, version: int | None = None) -> bool:
        """Remove the TTL from a key."""
        raise NotSupportedError("persist", self.__class__.__name__)

    def expire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool:
        """Set expiry time on a key."""
        raise NotSupportedError("expire", self.__class__.__name__)

    def expireat(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool:
        """Set expiry to an absolute time."""
        raise NotSupportedError("expireat", self.__class__.__name__)

    def pexpire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool:
        """Set expiry time in milliseconds."""
        raise NotSupportedError("pexpire", self.__class__.__name__)

    def pexpireat(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool:
        """Set expiry to an absolute time in milliseconds."""
        raise NotSupportedError("pexpireat", self.__class__.__name__)

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
        raise NotSupportedError("iter_keys", self.__class__.__name__)

    def scan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]:
        """Perform a single SCAN iteration using cursor-based pagination."""
        all_keys = self.keys(pattern, version=version)
        if hasattr(all_keys, "sort"):
            all_keys.sort()
        else:
            all_keys = sorted(all_keys)
        count = count or 100
        start_idx = cursor
        end_idx = start_idx + count
        paginated_keys = all_keys[start_idx:end_idx]
        next_cursor = end_idx if end_idx < len(all_keys) else 0
        return (next_cursor, paginated_keys)

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        """Delete all keys matching pattern."""
        raise NotSupportedError("delete_pattern", self.__class__.__name__)

    def rename(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool:
        """Rename a key atomically."""
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
    # Lock & Pipeline
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
        """Set field in hash."""
        raise NotSupportedError("hset", self.__class__.__name__)

    def hdel(self, key: KeyT, *fields: str, version: int | None = None) -> int:
        """Delete hash fields."""
        raise NotSupportedError("hdel", self.__class__.__name__)

    def hlen(self, key: KeyT, version: int | None = None) -> int:
        """Get number of fields in hash."""
        raise NotSupportedError("hlen", self.__class__.__name__)

    def hkeys(self, key: KeyT, version: int | None = None) -> list[str]:
        """Get all field names in hash."""
        raise NotSupportedError("hkeys", self.__class__.__name__)

    def hexists(self, key: KeyT, field: str, version: int | None = None) -> bool:
        """Check if field exists in hash."""
        raise NotSupportedError("hexists", self.__class__.__name__)

    def hget(self, key: KeyT, field: str, version: int | None = None) -> Any:
        """Get value of field in hash."""
        raise NotSupportedError("hget", self.__class__.__name__)

    def hgetall(self, key: KeyT, version: int | None = None) -> dict[str, Any]:
        """Get all fields and values in hash."""
        raise NotSupportedError("hgetall", self.__class__.__name__)

    def hmget(self, key: KeyT, *fields: str, version: int | None = None) -> list[Any]:
        """Get values of multiple fields."""
        raise NotSupportedError("hmget", self.__class__.__name__)

    def hincrby(self, key: KeyT, field: str, amount: int = 1, version: int | None = None) -> int:
        """Increment value of field in hash."""
        raise NotSupportedError("hincrby", self.__class__.__name__)

    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0, version: int | None = None) -> float:
        """Increment float value of field in hash."""
        raise NotSupportedError("hincrbyfloat", self.__class__.__name__)

    def hsetnx(self, key: KeyT, field: str, value: Any, version: int | None = None) -> bool:
        """Set field in hash only if it doesn't exist."""
        raise NotSupportedError("hsetnx", self.__class__.__name__)

    def hvals(self, key: KeyT, version: int | None = None) -> list[Any]:
        """Get all values in hash."""
        raise NotSupportedError("hvals", self.__class__.__name__)

    # =========================================================================
    # List Operations
    # =========================================================================

    def lpush(self, key: KeyT, *values: Any, version: int | None = None) -> int:
        """Push values onto head of list."""
        raise NotSupportedError("lpush", self.__class__.__name__)

    def rpush(self, key: KeyT, *values: Any, version: int | None = None) -> int:
        """Push values onto tail of list."""
        raise NotSupportedError("rpush", self.__class__.__name__)

    def lpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from head of list."""
        raise NotSupportedError("lpop", self.__class__.__name__)

    def rpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None:
        """Remove and return element(s) from tail of list."""
        raise NotSupportedError("rpop", self.__class__.__name__)

    def lrange(self, key: KeyT, start: int, end: int, version: int | None = None) -> list[Any]:
        """Get a range of elements from list."""
        raise NotSupportedError("lrange", self.__class__.__name__)

    def lindex(self, key: KeyT, index: int, version: int | None = None) -> Any:
        """Get element at index in list."""
        raise NotSupportedError("lindex", self.__class__.__name__)

    def llen(self, key: KeyT, version: int | None = None) -> int:
        """Get length of list."""
        raise NotSupportedError("llen", self.__class__.__name__)

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
        raise NotSupportedError("lpos", self.__class__.__name__)

    def lmove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
        version: int | None = None,
    ) -> Any | None:
        """Atomically move an element from one list to another."""
        raise NotSupportedError("lmove", self.__class__.__name__)

    def lrem(self, key: KeyT, count: int, value: Any, version: int | None = None) -> int:
        """Remove elements from a list."""
        raise NotSupportedError("lrem", self.__class__.__name__)

    def ltrim(self, key: KeyT, start: int, end: int, version: int | None = None) -> bool:
        """Trim list to specified range."""
        raise NotSupportedError("ltrim", self.__class__.__name__)

    def lset(self, key: KeyT, index: int, value: Any, version: int | None = None) -> bool:
        """Set element at index in list."""
        raise NotSupportedError("lset", self.__class__.__name__)

    def linsert(self, key: KeyT, where: str, pivot: Any, value: Any, version: int | None = None) -> int:
        """Insert value before or after pivot in list."""
        raise NotSupportedError("linsert", self.__class__.__name__)

    def blpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from head of list."""
        raise NotSupportedError("blpop", self.__class__.__name__)

    def brpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None:
        """Blocking pop from tail of list."""
        raise NotSupportedError("brpop", self.__class__.__name__)

    def blmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
        version: int | None = None,
    ) -> Any | None:
        """Blocking atomically move element from one list to another."""
        raise NotSupportedError("blmove", self.__class__.__name__)

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(self, key: KeyT, *members: Any, version: int | None = None) -> int:
        """Add members to a set."""
        raise NotSupportedError("sadd", self.__class__.__name__)

    def scard(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of members in a set."""
        raise NotSupportedError("scard", self.__class__.__name__)

    def sdiff(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _set[Any]:
        """Return the difference between sets."""
        raise NotSupportedError("sdiff", self.__class__.__name__)

    def sdiffstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the difference of sets."""
        raise NotSupportedError("sdiffstore", self.__class__.__name__)

    def sinter(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _set[Any]:
        """Return the intersection of sets."""
        raise NotSupportedError("sinter", self.__class__.__name__)

    def sinterstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the intersection of sets."""
        raise NotSupportedError("sinterstore", self.__class__.__name__)

    def sismember(self, key: KeyT, member: Any, version: int | None = None) -> bool:
        """Check if member is in set."""
        raise NotSupportedError("sismember", self.__class__.__name__)

    def smembers(self, key: KeyT, version: int | None = None) -> _set[Any]:
        """Get all members of a set."""
        raise NotSupportedError("smembers", self.__class__.__name__)

    def smove(self, src: KeyT, dst: KeyT, member: Any, version: int | None = None) -> bool:
        """Move member from one set to another."""
        raise NotSupportedError("smove", self.__class__.__name__)

    def spop(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | _set[Any]:
        """Remove and return random member(s) from set."""
        raise NotSupportedError("spop", self.__class__.__name__)

    def srandmember(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | list[Any]:
        """Get random member(s) from set without removing."""
        raise NotSupportedError("srandmember", self.__class__.__name__)

    def srem(self, key: KeyT, *members: Any, version: int | None = None) -> int:
        """Remove members from a set."""
        raise NotSupportedError("srem", self.__class__.__name__)

    def sunion(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _set[Any]:
        """Return the union of sets."""
        raise NotSupportedError("sunion", self.__class__.__name__)

    def sunionstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int:
        """Store the union of sets."""
        raise NotSupportedError("sunionstore", self.__class__.__name__)

    def smismember(self, key: KeyT, *members: Any, version: int | None = None) -> list[bool]:
        """Check if multiple values are members of a set."""
        raise NotSupportedError("smismember", self.__class__.__name__)

    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, _set[Any]]:
        """Incrementally iterate over set members."""
        raise NotSupportedError("sscan", self.__class__.__name__)

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> Iterator[Any]:
        """Iterate over set members."""
        raise NotSupportedError("sscan_iter", self.__class__.__name__)

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
        raise NotSupportedError("zadd", self.__class__.__name__)

    def zcard(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of members in a sorted set."""
        raise NotSupportedError("zcard", self.__class__.__name__)

    def zcount(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Count members with scores between min and max."""
        raise NotSupportedError("zcount", self.__class__.__name__)

    def zincrby(self, key: KeyT, amount: float, member: Any, version: int | None = None) -> float:
        """Increment the score of a member."""
        raise NotSupportedError("zincrby", self.__class__.__name__)

    def zpopmax(self, key: KeyT, count: int | None = None, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with highest scores."""
        raise NotSupportedError("zpopmax", self.__class__.__name__)

    def zpopmin(self, key: KeyT, count: int | None = None, version: int | None = None) -> list[tuple[Any, float]]:
        """Remove and return members with lowest scores."""
        raise NotSupportedError("zpopmin", self.__class__.__name__)

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
        raise NotSupportedError("zrange", self.__class__.__name__)

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
        raise NotSupportedError("zrangebyscore", self.__class__.__name__)

    def zrank(self, key: KeyT, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member."""
        raise NotSupportedError("zrank", self.__class__.__name__)

    def zrem(self, key: KeyT, *members: Any, version: int | None = None) -> int:
        """Remove members from a sorted set."""
        raise NotSupportedError("zrem", self.__class__.__name__)

    def zremrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int:
        """Remove members with scores between min and max."""
        raise NotSupportedError("zremrangebyscore", self.__class__.__name__)

    def zremrangebyrank(self, key: KeyT, start: int, end: int, version: int | None = None) -> int:
        """Remove members by rank range."""
        raise NotSupportedError("zremrangebyrank", self.__class__.__name__)

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
        raise NotSupportedError("zrevrange", self.__class__.__name__)

    def zrevrangebyscore(
        self,
        key: KeyT,
        max_score: float | str,
        min_score: float | str,
        *,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]:
        """Return members with scores between max and min, highest first."""
        raise NotSupportedError("zrevrangebyscore", self.__class__.__name__)

    def zscore(self, key: KeyT, member: Any, version: int | None = None) -> float | None:
        """Get the score of a member."""
        raise NotSupportedError("zscore", self.__class__.__name__)

    def zrevrank(self, key: KeyT, member: Any, version: int | None = None) -> int | None:
        """Get the rank of a member (highest score first)."""
        raise NotSupportedError("zrevrank", self.__class__.__name__)

    def zmscore(self, key: KeyT, *members: Any, version: int | None = None) -> list[float | None]:
        """Get the scores of multiple members."""
        raise NotSupportedError("zmscore", self.__class__.__name__)

    # =========================================================================
    # Stream Operations
    # =========================================================================

    def xlen(self, key: KeyT, version: int | None = None) -> int:
        """Get the number of entries in a stream."""
        raise NotSupportedError("xlen", self.__class__.__name__)

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
        raise NotSupportedError("eval_script", self.__class__.__name__)

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
        raise NotSupportedError("aeval_script", self.__class__.__name__)
