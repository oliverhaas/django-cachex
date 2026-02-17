"""Type aliases for django-cachex.

Compatible with redis-py and valkey-py type systems, defined locally
to avoid a runtime dependency on either library for type annotations.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence

    from django_cachex.client.pipeline import Pipeline
    from django_cachex.script import ScriptHelpers

# =============================================================================
# Types matching redis-py / valkey-py (for compatibility with their APIs)
# =============================================================================

# Key types - matches redis.typing.KeyT and valkey.typing.KeyT
type KeyT = bytes | str | memoryview

# Expiry types (relative timeout) - matches redis.typing.ExpiryT
type ExpiryT = int | timedelta

# Absolute expiry types - matches redis.typing.AbsExpiryT
type AbsExpiryT = int | datetime


class KeyType(StrEnum):
    """Redis key data types."""

    STRING = "string"
    LIST = "list"
    SET = "set"
    HASH = "hash"
    ZSET = "zset"
    STREAM = "stream"


# Alias to avoid shadowing by .set() methods
_Set = set


@runtime_checkable
class CacheProtocol(Protocol):
    """Protocol defining the public interface for django-cachex cache backends.

    Covers Django's BaseCache interface plus all django-cachex extensions
    (TTL, data structures, pipelines, locks, Lua scripts).
    """

    # Support level: "cachex" (native), "wrapped" (Django builtin), "limited" (unknown)
    _cachex_support: ClassVar[str]

    # =========================================================================
    # Properties (from BaseCache)
    # =========================================================================

    @property
    def key_prefix(self) -> str: ...

    @property
    def version(self) -> int: ...

    @property
    def default_timeout(self) -> float | None: ...

    # =========================================================================
    # Key utilities
    # =========================================================================

    def make_key(self, key: str, version: int | None = None) -> str: ...

    def make_and_validate_key(self, key: str, version: int | None = None) -> str: ...

    def make_pattern(self, pattern: str, version: int | None = None) -> str: ...

    def reverse_key(self, key: str) -> str: ...

    # =========================================================================
    # Core Cache Operations (Django BaseCache interface)
    # =========================================================================

    def add(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = ...,
        version: int | None = None,
    ) -> bool: ...

    async def aadd(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = ...,
        version: int | None = None,
    ) -> bool: ...

    def get(self, key: KeyT, default: Any = None, version: int | None = None) -> Any: ...

    async def aget(self, key: KeyT, default: Any = None, version: int | None = None) -> Any: ...

    def set(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> Any: ...

    async def aset(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> Any: ...

    def touch(self, key: KeyT, timeout: float | None = ..., version: int | None = None) -> bool: ...

    async def atouch(self, key: KeyT, timeout: float | None = ..., version: int | None = None) -> bool: ...

    def delete(self, key: KeyT, version: int | None = None) -> bool: ...

    async def adelete(self, key: KeyT, version: int | None = None) -> bool: ...

    def get_many(self, keys: list[KeyT], version: int | None = None) -> dict[KeyT, Any]: ...

    async def aget_many(self, keys: list[KeyT], version: int | None = None) -> dict[KeyT, Any]: ...

    def has_key(self, key: KeyT, version: int | None = None) -> bool: ...

    async def ahas_key(self, key: KeyT, version: int | None = None) -> bool: ...

    def incr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int: ...

    async def aincr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int: ...

    def decr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int: ...

    async def adecr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int: ...

    def get_or_set(
        self,
        key: KeyT,
        default: Any,
        timeout: float | None = ...,
        version: int | None = None,
    ) -> Any: ...

    async def aget_or_set(
        self,
        key: KeyT,
        default: Any,
        timeout: float | None = ...,
        version: int | None = None,
    ) -> Any: ...

    def set_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: float | None = ...,
        version: int | None = None,
    ) -> list: ...

    async def aset_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: float | None = ...,
        version: int | None = None,
    ) -> list: ...

    def delete_many(self, keys: list[KeyT], version: int | None = None) -> int: ...

    async def adelete_many(self, keys: list[KeyT], version: int | None = None) -> int: ...

    def clear(self) -> bool: ...

    async def aclear(self) -> bool: ...

    def close(self, **kwargs: Any) -> None: ...

    async def aclose(self, **kwargs: Any) -> None: ...

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def ttl(self, key: KeyT, version: int | None = None) -> int | None: ...

    async def attl(self, key: KeyT, version: int | None = None) -> int | None: ...

    def pttl(self, key: KeyT, version: int | None = None) -> int | None: ...

    async def apttl(self, key: KeyT, version: int | None = None) -> int | None: ...

    def type(self, key: KeyT, version: int | None = None) -> KeyType | None: ...

    async def atype(self, key: KeyT, version: int | None = None) -> KeyType | None: ...

    def persist(self, key: KeyT, version: int | None = None) -> bool: ...

    async def apersist(self, key: KeyT, version: int | None = None) -> bool: ...

    def expire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool: ...

    async def aexpire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool: ...

    def expire_at(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool: ...

    async def aexpire_at(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool: ...

    def pexpire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool: ...

    async def apexpire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool: ...

    def pexpire_at(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool: ...

    async def apexpire_at(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool: ...

    def expiretime(self, key: KeyT, version: int | None = None) -> int | None: ...

    async def aexpiretime(self, key: KeyT, version: int | None = None) -> int | None: ...

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]: ...

    async def akeys(self, pattern: str = "*", version: int | None = None) -> list[str]: ...

    def iter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]: ...

    def aiter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> AsyncIterator[str]: ...

    def scan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]: ...

    async def ascan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]: ...

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int: ...

    async def adelete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int: ...

    def clear_all_versions(self, itersize: int | None = None) -> int: ...

    async def aclear_all_versions(self, itersize: int | None = None) -> int: ...

    def flush_db(self) -> bool: ...

    async def aflush_db(self) -> bool: ...

    def rename(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool: ...

    async def arename(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool: ...

    def renamenx(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool: ...

    async def arenamenx(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool: ...

    def incr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int: ...

    def decr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int: ...

    async def aincr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int: ...

    async def adecr_version(self, key: KeyT, delta: int = 1, version: int | None = None) -> int: ...

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
    ) -> Any: ...

    def alock(
        self,
        key: str,
        version: int | None = None,
        timeout: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any: ...

    def pipeline(self, *, transaction: bool = True, version: int | None = None) -> Pipeline: ...

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
    ) -> int: ...

    def hdel(self, key: KeyT, *fields: str, version: int | None = None) -> int: ...

    def hlen(self, key: KeyT, version: int | None = None) -> int: ...

    def hkeys(self, key: KeyT, version: int | None = None) -> list[str]: ...

    def hexists(self, key: KeyT, field: str, version: int | None = None) -> bool: ...

    def hget(self, key: KeyT, field: str, version: int | None = None) -> Any: ...

    def hgetall(self, key: KeyT, version: int | None = None) -> dict[str, Any]: ...

    def hmget(self, key: KeyT, *fields: str, version: int | None = None) -> list[Any]: ...

    def hincrby(self, key: KeyT, field: str, amount: int = 1, version: int | None = None) -> int: ...

    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0, version: int | None = None) -> float: ...

    def hsetnx(self, key: KeyT, field: str, value: Any, version: int | None = None) -> bool: ...

    def hvals(self, key: KeyT, version: int | None = None) -> list[Any]: ...

    async def ahset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int: ...

    async def ahdel(self, key: KeyT, *fields: str, version: int | None = None) -> int: ...

    async def ahlen(self, key: KeyT, version: int | None = None) -> int: ...

    async def ahkeys(self, key: KeyT, version: int | None = None) -> list[str]: ...

    async def ahexists(self, key: KeyT, field: str, version: int | None = None) -> bool: ...

    async def ahget(self, key: KeyT, field: str, version: int | None = None) -> Any: ...

    async def ahgetall(self, key: KeyT, version: int | None = None) -> dict[str, Any]: ...

    async def ahmget(self, key: KeyT, *fields: str, version: int | None = None) -> list[Any]: ...

    async def ahincrby(self, key: KeyT, field: str, amount: int = 1, version: int | None = None) -> int: ...

    async def ahincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0, version: int | None = None) -> float: ...

    async def ahsetnx(self, key: KeyT, field: str, value: Any, version: int | None = None) -> bool: ...

    async def ahvals(self, key: KeyT, version: int | None = None) -> list[Any]: ...

    # =========================================================================
    # List Operations
    # =========================================================================

    def lpush(self, key: KeyT, *values: Any, version: int | None = None) -> int: ...

    def rpush(self, key: KeyT, *values: Any, version: int | None = None) -> int: ...

    def lpop(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | list[Any] | None: ...

    def rpop(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | list[Any] | None: ...

    def lrange(self, key: KeyT, start: int, end: int, version: int | None = None) -> list[Any]: ...

    def lindex(self, key: KeyT, index: int, version: int | None = None) -> Any: ...

    def llen(self, key: KeyT, version: int | None = None) -> int: ...

    def lpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None: ...

    def lmove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None: ...

    def lrem(self, key: KeyT, count: int, value: Any, version: int | None = None) -> int: ...

    def ltrim(self, key: KeyT, start: int, end: int, version: int | None = None) -> bool: ...

    def lset(self, key: KeyT, index: int, value: Any, version: int | None = None) -> bool: ...

    def linsert(self, key: KeyT, where: str, pivot: Any, value: Any, version: int | None = None) -> int: ...

    def blpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None: ...

    def brpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None: ...

    def blmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None: ...

    async def alpush(self, key: KeyT, *values: Any, version: int | None = None) -> int: ...

    async def arpush(self, key: KeyT, *values: Any, version: int | None = None) -> int: ...

    async def alpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None: ...

    async def arpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any] | None: ...

    async def alrange(self, key: KeyT, start: int, end: int, version: int | None = None) -> list[Any]: ...

    async def alindex(self, key: KeyT, index: int, version: int | None = None) -> Any: ...

    async def allen(self, key: KeyT, version: int | None = None) -> int: ...

    async def alpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> int | list[int] | None: ...

    async def almove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str,
        whereto: str,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None: ...

    async def alrem(self, key: KeyT, count: int, value: Any, version: int | None = None) -> int: ...

    async def altrim(self, key: KeyT, start: int, end: int, version: int | None = None) -> bool: ...

    async def alset(self, key: KeyT, index: int, value: Any, version: int | None = None) -> bool: ...

    async def alinsert(self, key: KeyT, where: str, pivot: Any, value: Any, version: int | None = None) -> int: ...

    async def ablpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None: ...

    async def abrpop(
        self,
        keys: KeyT | Sequence[KeyT],
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None: ...

    async def ablmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Any | None: ...

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(self, key: KeyT, *members: Any, version: int | None = None) -> int: ...

    def scard(self, key: KeyT, version: int | None = None) -> int: ...

    def sdiff(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _Set[Any]: ...

    def sdiffstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int: ...

    def sinter(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _Set[Any]: ...

    def sinterstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int: ...

    def sismember(self, key: KeyT, member: Any, version: int | None = None) -> bool: ...

    def smembers(self, key: KeyT, version: int | None = None) -> _Set[Any]: ...

    def smove(
        self,
        src: KeyT,
        dst: KeyT,
        member: Any,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool: ...

    def spop(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | _Set[Any]: ...

    def srandmember(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | list[Any]: ...

    def srem(self, key: KeyT, *members: Any, version: int | None = None) -> int: ...

    def sunion(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _Set[Any]: ...

    def sunionstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int: ...

    def smismember(self, key: KeyT, *members: Any, version: int | None = None) -> list[bool]: ...

    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, _Set[Any]]: ...

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> Iterator[Any]: ...

    async def asadd(self, key: KeyT, *members: Any, version: int | None = None) -> int: ...

    async def ascard(self, key: KeyT, version: int | None = None) -> int: ...

    async def asdiff(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _Set[Any]: ...

    async def asdiffstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int: ...

    async def asinter(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _Set[Any]: ...

    async def asinterstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int: ...

    async def asismember(self, key: KeyT, member: Any, version: int | None = None) -> bool: ...

    async def asmembers(self, key: KeyT, version: int | None = None) -> _Set[Any]: ...

    async def asmove(
        self,
        src: KeyT,
        dst: KeyT,
        member: Any,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> bool: ...

    async def aspop(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | _Set[Any]: ...

    async def asrandmember(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Any | list[Any]: ...

    async def asrem(self, key: KeyT, *members: Any, version: int | None = None) -> int: ...

    async def asunion(self, keys: KeyT | Sequence[KeyT], version: int | None = None) -> _Set[Any]: ...

    async def asunionstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int: ...

    async def asmismember(self, key: KeyT, *members: Any, version: int | None = None) -> list[bool]: ...

    async def asscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, _Set[Any]]: ...

    async def asscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> AsyncIterator[Any]: ...

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
    ) -> int: ...

    def zcard(self, key: KeyT, version: int | None = None) -> int: ...

    def zcount(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int: ...

    def zincrby(self, key: KeyT, amount: float, member: Any, version: int | None = None) -> float: ...

    def zpopmax(self, key: KeyT, count: int = 1, version: int | None = None) -> list[tuple[Any, float]]: ...

    def zpopmin(self, key: KeyT, count: int = 1, version: int | None = None) -> list[tuple[Any, float]]: ...

    def zrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]: ...

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
    ) -> list[Any] | list[tuple[Any, float]]: ...

    def zrank(self, key: KeyT, member: Any, version: int | None = None) -> int | None: ...

    def zrem(self, key: KeyT, *members: Any, version: int | None = None) -> int: ...

    def zremrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int: ...

    def zremrangebyrank(self, key: KeyT, start: int, end: int, version: int | None = None) -> int: ...

    def zrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]: ...

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
    ) -> list[Any] | list[tuple[Any, float]]: ...

    def zscore(self, key: KeyT, member: Any, version: int | None = None) -> float | None: ...

    def zrevrank(self, key: KeyT, member: Any, version: int | None = None) -> int | None: ...

    def zmscore(self, key: KeyT, *members: Any, version: int | None = None) -> list[float | None]: ...

    async def azadd(
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
    ) -> int: ...

    async def azcard(self, key: KeyT, version: int | None = None) -> int: ...

    async def azcount(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int: ...

    async def azincrby(self, key: KeyT, amount: float, member: Any, version: int | None = None) -> float: ...

    async def azpopmax(self, key: KeyT, count: int = 1, version: int | None = None) -> list[tuple[Any, float]]: ...

    async def azpopmin(self, key: KeyT, count: int = 1, version: int | None = None) -> list[tuple[Any, float]]: ...

    async def azrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]: ...

    async def azrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        *,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]: ...

    async def azrank(self, key: KeyT, member: Any, version: int | None = None) -> int | None: ...

    async def azrem(self, key: KeyT, *members: Any, version: int | None = None) -> int: ...

    async def azremrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        version: int | None = None,
    ) -> int: ...

    async def azremrangebyrank(self, key: KeyT, start: int, end: int, version: int | None = None) -> int: ...

    async def azrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]: ...

    async def azrevrangebyscore(
        self,
        key: KeyT,
        max_score: float | str,
        min_score: float | str,
        *,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
        version: int | None = None,
    ) -> list[Any] | list[tuple[Any, float]]: ...

    async def azscore(self, key: KeyT, member: Any, version: int | None = None) -> float | None: ...

    async def azrevrank(self, key: KeyT, member: Any, version: int | None = None) -> int | None: ...

    async def azmscore(self, key: KeyT, *members: Any, version: int | None = None) -> list[float | None]: ...

    # =========================================================================
    # Stream Operations
    # =========================================================================

    def xadd(
        self,
        key: KeyT,
        fields: dict[str, Any],
        entry_id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> str: ...

    async def axadd(
        self,
        key: KeyT,
        fields: dict[str, Any],
        entry_id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> str: ...

    def xlen(self, key: KeyT, version: int | None = None) -> int: ...

    async def axlen(self, key: KeyT, version: int | None = None) -> int: ...

    def xrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]: ...

    async def axrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]: ...

    def xrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]: ...

    async def axrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]: ...

    def xread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None: ...

    async def axread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None: ...

    def xtrim(
        self,
        key: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> int: ...

    async def axtrim(
        self,
        key: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> int: ...

    def xdel(self, key: KeyT, *entry_ids: str, version: int | None = None) -> int: ...

    async def axdel(self, key: KeyT, *entry_ids: str, version: int | None = None) -> int: ...

    def xinfo_stream(self, key: KeyT, full: bool = False, version: int | None = None) -> dict[str, Any]: ...

    async def axinfo_stream(self, key: KeyT, full: bool = False, version: int | None = None) -> dict[str, Any]: ...

    def xinfo_groups(self, key: KeyT, version: int | None = None) -> list[dict[str, Any]]: ...

    async def axinfo_groups(self, key: KeyT, version: int | None = None) -> list[dict[str, Any]]: ...

    def xinfo_consumers(self, key: KeyT, group: str, version: int | None = None) -> list[dict[str, Any]]: ...

    async def axinfo_consumers(self, key: KeyT, group: str, version: int | None = None) -> list[dict[str, Any]]: ...

    def xgroup_create(
        self,
        key: KeyT,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool: ...

    async def axgroup_create(
        self,
        key: KeyT,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool: ...

    def xgroup_destroy(self, key: KeyT, group: str, version: int | None = None) -> int: ...

    async def axgroup_destroy(self, key: KeyT, group: str, version: int | None = None) -> int: ...

    def xgroup_setid(
        self,
        key: KeyT,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool: ...

    async def axgroup_setid(
        self,
        key: KeyT,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> bool: ...

    def xgroup_delconsumer(self, key: KeyT, group: str, consumer: str, version: int | None = None) -> int: ...

    async def axgroup_delconsumer(self, key: KeyT, group: str, consumer: str, version: int | None = None) -> int: ...

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None: ...

    async def axreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
        version: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None: ...

    def xack(self, key: KeyT, group: str, *entry_ids: str, version: int | None = None) -> int: ...

    async def axack(self, key: KeyT, group: str, *entry_ids: str, version: int | None = None) -> int: ...

    def xpending(
        self,
        key: KeyT,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
        version: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]: ...

    async def axpending(
        self,
        key: KeyT,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
        version: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]: ...

    def xclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        entry_ids: list[str],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]] | list[str]: ...

    async def axclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        entry_ids: list[str],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
        version: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]] | list[str]: ...

    def xautoclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
        version: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]] | list[str], list[str]]: ...

    async def axautoclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
        version: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]] | list[str], list[str]]: ...

    # =========================================================================
    # Client Access & Info
    # =========================================================================

    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any: ...

    def info(self, section: str | None = None) -> dict[str, Any]: ...

    def slowlog_get(self, count: int = 10) -> list[Any]: ...

    def slowlog_len(self) -> int: ...

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
    ) -> Any: ...

    async def aeval_script(
        self,
        script: str,
        *,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        pre_hook: Callable[[ScriptHelpers, Sequence[Any], Sequence[Any]], tuple[list[Any], list[Any]]] | None = None,
        post_hook: Callable[[ScriptHelpers, Any], Any] | None = None,
        version: int | None = None,
    ) -> Any: ...
