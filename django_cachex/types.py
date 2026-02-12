"""Type aliases for django-cachex.

Compatible with redis-py and valkey-py type systems, defined locally
to avoid a runtime dependency on either library for type annotations.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from django_cachex.client.pipeline import Pipeline
    from django_cachex.script import LuaScript, ScriptHelpers

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


# Alias to avoid shadowing by CacheProtocol.set method
_set = set


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
    ) -> bool | None: ...

    async def aset(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = ...,
        version: int | None = None,
    ) -> None: ...

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

    def pttl(self, key: KeyT, version: int | None = None) -> int | None: ...

    def type(self, key: KeyT, version: int | None = None) -> KeyType | None: ...

    def persist(self, key: KeyT, version: int | None = None) -> bool: ...

    def expire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool: ...

    def expire_at(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool: ...

    def pexpire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool: ...

    def pexpire_at(self, key: KeyT, when: AbsExpiryT, version: int | None = None) -> bool: ...

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]: ...

    def iter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]: ...

    def scan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
    ) -> tuple[int, list[str]]: ...

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int: ...

    def rename(
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

    def pipeline(self, *, transaction: bool = True, version: int | None = None) -> Pipeline: ...

    # =========================================================================
    # Hash Operations
    # =========================================================================

    def hset(self, key: KeyT, field: str, value: Any, version: int | None = None) -> int: ...

    def hdel(self, key: KeyT, *fields: str, version: int | None = None) -> int: ...

    def hlen(self, key: KeyT, version: int | None = None) -> int: ...

    def hkeys(self, key: KeyT, version: int | None = None) -> list[str]: ...

    def hexists(self, key: KeyT, field: str, version: int | None = None) -> bool: ...

    def hget(self, key: KeyT, field: str, version: int | None = None) -> Any: ...

    def hgetall(self, key: KeyT, version: int | None = None) -> dict[str, Any]: ...

    def hmget(self, key: KeyT, *fields: str, version: int | None = None) -> list[Any]: ...

    def hmset(self, key: KeyT, mapping: Mapping[str, Any], version: int | None = None) -> bool: ...

    def hincrby(self, key: KeyT, field: str, amount: int = 1, version: int | None = None) -> int: ...

    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0, version: int | None = None) -> float: ...

    def hsetnx(self, key: KeyT, field: str, value: Any, version: int | None = None) -> bool: ...

    def hvals(self, key: KeyT, version: int | None = None) -> list[Any]: ...

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
    ) -> Any | None: ...

    def lrem(self, key: KeyT, count: int, value: Any, version: int | None = None) -> int: ...

    def ltrim(self, key: KeyT, start: int, end: int, version: int | None = None) -> bool: ...

    def lset(self, key: KeyT, index: int, value: Any, version: int | None = None) -> bool: ...

    def linsert(self, key: KeyT, where: str, pivot: Any, value: Any, version: int | None = None) -> int: ...

    def blpop(
        self,
        *keys: KeyT,
        timeout: float = 0,
        version: int | None = None,
    ) -> tuple[str, Any] | None: ...

    def brpop(
        self,
        *keys: KeyT,
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
    ) -> Any | None: ...

    # =========================================================================
    # Set Operations
    # =========================================================================

    def sadd(self, key: KeyT, *members: Any, version: int | None = None) -> int: ...

    def scard(self, key: KeyT, version: int | None = None) -> int: ...

    def sdiff(self, *keys: KeyT, version: int | None = None) -> _set[Any]: ...

    def sdiffstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int: ...

    def sinter(self, *keys: KeyT, version: int | None = None) -> _set[Any]: ...

    def sinterstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> int: ...

    def sismember(self, key: KeyT, member: Any, version: int | None = None) -> bool: ...

    def smembers(self, key: KeyT, version: int | None = None) -> _set[Any]: ...

    def smove(self, src: KeyT, dst: KeyT, member: Any, version: int | None = None) -> bool: ...

    def spop(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | _set[Any]: ...

    def srandmember(self, key: KeyT, count: int | None = None, version: int | None = None) -> Any | list[Any]: ...

    def srem(self, key: KeyT, *members: Any, version: int | None = None) -> int: ...

    def sunion(self, *keys: KeyT, version: int | None = None) -> _set[Any]: ...

    def sunionstore(
        self,
        dest: KeyT,
        *keys: KeyT,
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
    ) -> tuple[int, _set[Any]]: ...

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
        version: int | None = None,
    ) -> Iterator[Any]: ...

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

    def register_script(
        self,
        name: str,
        script: str,
        *,
        num_keys: int | None = None,
        pre_func: Callable[[ScriptHelpers, Sequence[Any], Sequence[Any]], tuple[list[Any], list[Any]]] | None = None,
        post_func: Callable[[ScriptHelpers, Any], Any] | None = None,
    ) -> LuaScript: ...

    def eval_script(
        self,
        name: str,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        *,
        version: int | None = None,
    ) -> Any: ...

    async def aeval_script(
        self,
        name: str,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        *,
        version: int | None = None,
    ) -> Any: ...
