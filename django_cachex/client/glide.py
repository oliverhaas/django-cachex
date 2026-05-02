"""Spike: ``valkey-glide``-backed cache client (sync + async).

Adapts ``valkey-glide-sync`` (sync) and ``valkey-glide`` (async) to the
redis-py-shaped surface ``KeyValueCacheClient`` calls. This file is
intentionally pragmatic — the shim covers the entire feature surface the
cache class touches, with explicit translation for divergent signatures
and a ``custom_command`` fallback for the long tail.

Standalone only; no cluster, no sentinel.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Self
from urllib.parse import urlparse

from glide import GlideClient as AsyncGlideClient
from glide import GlideClientConfiguration as AsyncGlideCfg
from glide import NodeAddress as AsyncNodeAddress
from glide_sync import (
    Batch,
    ConditionalChange,
    ExpirySet,
    ExpiryType,
    FlushMode,
    GlideClientConfiguration,
    NodeAddress,
)
from glide_sync.glide_client import GlideClient

from django_cachex.client.default import KeyValueCacheClient

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


# =============================================================================
# Encoding helpers
# =============================================================================


def _enc(v: Any) -> bytes | str:
    """Coerce a value to glide's accepted argument shape (bytes or str)."""
    if isinstance(v, (bytes, str)):
        return v
    if isinstance(v, bool):
        return b"1" if v else b"0"
    if isinstance(v, (int, float)):
        return str(v).encode()
    return v  # let glide raise if truly unsupported


def _enc_list(values: Iterable[Any]) -> list[bytes | str]:
    return [_enc(v) for v in values]


def _enc_map(mapping: Mapping[Any, Any]) -> dict[Any, bytes | str]:
    return {k: _enc(v) for k, v in mapping.items()}


def _set_kwargs(
    *,
    ex: int | None = None,
    px: int | None = None,
    nx: bool = False,
    xx: bool = False,
    keepttl: bool = False,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if ex is not None:
        kwargs["expiry"] = ExpirySet(ExpiryType.SEC, ex)
    elif px is not None:
        kwargs["expiry"] = ExpirySet(ExpiryType.MILLSEC, px)
    elif keepttl:
        kwargs["expiry"] = ExpirySet(ExpiryType.KEEP_TTL, None)
    if nx:
        kwargs["conditional_set"] = ConditionalChange.ONLY_IF_DOES_NOT_EXIST
    elif xx:
        kwargs["conditional_set"] = ConditionalChange.ONLY_IF_EXISTS
    return kwargs


# =============================================================================
# Sync pipeline shim (Batch-backed)
# =============================================================================


class _GlidePipelineShim:
    """redis-py-shaped pipeline buffering ops into a glide ``Batch``.

    Falls back to ``custom_command`` for any method not explicitly
    translated. ``execute()`` / ``exec()`` runs the batch.
    """

    def __init__(self, client: GlideClient, *, transaction: bool = False) -> None:
        self._client = client
        self._batch = Batch(is_atomic=transaction)

    # ---- string / generic ----
    def set(self, key: Any, value: Any, **kw: Any) -> _GlidePipelineShim:
        self._batch.set(key, _enc(value), **_set_kwargs(**kw))
        return self

    def get(self, key: Any) -> _GlidePipelineShim:
        self._batch.get(key)
        return self

    def delete(self, *keys: Any) -> _GlidePipelineShim:
        self._batch.delete(_enc_list(keys))
        return self

    def mget(self, keys: Iterable[Any]) -> _GlidePipelineShim:
        self._batch.mget(_enc_list(keys))
        return self

    def mset(self, mapping: Mapping[Any, Any]) -> _GlidePipelineShim:
        self._batch.mset(_enc_map(mapping))
        return self

    def incr(self, key: Any, amount: int = 1) -> _GlidePipelineShim:
        if amount == 1:
            self._batch.incr(key)
        else:
            self._batch.incrby(key, amount)
        return self

    def expire(self, key: Any, seconds: int) -> _GlidePipelineShim:
        self._batch.expire(key, seconds)
        return self

    def pexpire(self, key: Any, ms: int) -> _GlidePipelineShim:
        self._batch.pexpire(key, ms)
        return self

    def ttl(self, key: Any) -> _GlidePipelineShim:
        self._batch.ttl(key)
        return self

    def pttl(self, key: Any) -> _GlidePipelineShim:
        self._batch.pttl(key)
        return self

    def exists(self, *keys: Any) -> _GlidePipelineShim:
        self._batch.exists(_enc_list(keys))
        return self

    def persist(self, key: Any) -> _GlidePipelineShim:
        self._batch.persist(key)
        return self

    def type(self, key: Any) -> _GlidePipelineShim:
        self._batch.type(key)
        return self

    # ---- raw fallback ----
    def __getattr__(self, name: str) -> Any:
        cmd = name.upper()

        def call(*args: Any) -> _GlidePipelineShim:
            self._batch.custom_command([cmd, *_enc_list(args)])
            return self

        return call

    # ---- execution ----
    def execute(self) -> list[Any]:
        return self._client.exec(self._batch, raise_on_error=True) or []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


# =============================================================================
# Sync redis-py shim
# =============================================================================


class _GlideRedisShim:
    """redis-py-shaped wrapper around ``glide_sync.GlideClient``."""

    def __init__(self, client: GlideClient) -> None:
        self._c = client

    # ---- strings ----
    def get(self, key: Any) -> bytes | None:
        return self._c.get(key)

    def set(
        self,
        key: Any,
        value: Any,
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
    ) -> bool:
        result = self._c.set(key, _enc(value), **_set_kwargs(ex=ex, px=px, nx=nx, xx=xx, keepttl=keepttl))
        return result == "OK"

    def mget(self, keys: Iterable[Any]) -> list[bytes | None]:
        return self._c.mget(list(keys))

    def mset(self, mapping: Mapping[Any, Any]) -> str:
        return self._c.mset(_enc_map(mapping))

    def incr(self, key: Any, amount: int = 1) -> int:
        if amount == 1:
            return self._c.incr(key)
        return self._c.incrby(key, amount)

    def incrby(self, key: Any, amount: int) -> int:
        return self._c.incrby(key, amount)

    def incrbyfloat(self, key: Any, amount: float) -> float:
        return self._c.incrbyfloat(key, amount)

    def decr(self, key: Any, amount: int = 1) -> int:
        if amount == 1:
            return self._c.decr(key)
        return self._c.decrby(key, amount)

    # ---- keys ----
    def delete(self, *keys: Any) -> int:
        if not keys:
            return 0
        return self._c.delete(_enc_list(keys))

    def exists(self, *keys: Any) -> int:
        if not keys:
            return 0
        return self._c.exists(_enc_list(keys))

    def expire(self, key: Any, seconds: int) -> bool:
        return self._c.expire(key, seconds)

    def pexpire(self, key: Any, ms: int) -> bool:
        return self._c.pexpire(key, ms)

    def expireat(self, key: Any, when: int) -> bool:
        return self._c.expireat(key, when)

    def pexpireat(self, key: Any, when: int) -> bool:
        return self._c.pexpireat(key, when)

    def expiretime(self, key: Any) -> int:
        return self._c.expiretime(key)

    def ttl(self, key: Any) -> int:
        return self._c.ttl(key)

    def pttl(self, key: Any) -> int:
        return self._c.pttl(key)

    def persist(self, key: Any) -> bool:
        return self._c.persist(key)

    def type(self, key: Any) -> bytes:
        return self._c.type(key)

    def rename(self, src: Any, dst: Any) -> str:
        return self._c.rename(src, dst)

    def renamenx(self, src: Any, dst: Any) -> bool:
        return self._c.renamenx(src, dst)

    def keys(self, pattern: Any = "*") -> list[bytes]:
        result = self._c.custom_command([b"KEYS", _enc(pattern)])
        return list(result) if result else []

    def scan(
        self,
        cursor: Any = 0,
        match: Any = None,
        count: int | None = None,
    ) -> tuple[bytes, list[bytes]]:
        result = self._c.scan(_enc(cursor), match=match, count=count)
        return result[0], list(result[1])

    def scan_iter(self, match: Any = None, count: int | None = None):
        cursor: Any = b"0"
        while True:
            result = self._c.scan(cursor, match=match, count=count)
            cursor, keys = result[0], result[1]
            yield from keys
            if cursor in (b"0", "0", 0):
                return

    # ---- hashes ----
    def hset(
        self,
        key: Any,
        field: Any = None,
        value: Any = None,
        mapping: Mapping[Any, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        m: dict[Any, Any] = {}
        if field is not None:
            m[field] = _enc(value)
        if mapping:
            m.update(_enc_map(mapping))
        if items:
            for i in range(0, len(items), 2):
                m[items[i]] = _enc(items[i + 1])
        if not m:
            return 0
        return self._c.hset(key, m)

    def hsetnx(self, key: Any, field: Any, value: Any) -> bool:
        return self._c.hsetnx(key, field, _enc(value))

    def hget(self, key: Any, field: Any) -> bytes | None:
        return self._c.hget(key, field)

    def hmget(self, key: Any, *fields: Any) -> list[bytes | None]:
        if len(fields) == 1 and isinstance(fields[0], (list, tuple)):
            fields = tuple(fields[0])
        return self._c.hmget(key, list(fields))

    def hgetall(self, key: Any) -> dict[bytes, bytes]:
        return self._c.hgetall(key)

    def hkeys(self, key: Any) -> list[bytes]:
        return self._c.hkeys(key)

    def hvals(self, key: Any) -> list[bytes]:
        return self._c.hvals(key)

    def hlen(self, key: Any) -> int:
        return self._c.hlen(key)

    def hexists(self, key: Any, field: Any) -> bool:
        return self._c.hexists(key, field)

    def hdel(self, key: Any, *fields: Any) -> int:
        return self._c.hdel(key, list(fields))

    def hincrby(self, key: Any, field: Any, amount: int = 1) -> int:
        return self._c.hincrby(key, field, amount)

    def hincrbyfloat(self, key: Any, field: Any, amount: float) -> float:
        return self._c.hincrbyfloat(key, field, amount)

    # ---- sets ----
    def sadd(self, key: Any, *members: Any) -> int:
        return self._c.sadd(key, _enc_list(members))

    def srem(self, key: Any, *members: Any) -> int:
        return self._c.srem(key, _enc_list(members))

    def smembers(self, key: Any) -> set[bytes]:
        return self._c.smembers(key)

    def sismember(self, key: Any, member: Any) -> bool:
        return self._c.sismember(key, _enc(member))

    def smismember(self, key: Any, *members: Any) -> list[bool]:
        return self._c.smismember(key, _enc_list(members))

    def scard(self, key: Any) -> int:
        return self._c.scard(key)

    def spop(self, key: Any, count: int | None = None) -> bytes | set[bytes] | None:
        if count is None:
            return self._c.spop(key)
        return self._c.spop_count(key, count)

    def srandmember(self, key: Any, count: int | None = None) -> bytes | list[bytes] | None:
        if count is None:
            return self._c.srandmember(key)
        return self._c.srandmember_count(key, count)

    def smove(self, src: Any, dst: Any, member: Any) -> bool:
        return self._c.smove(src, dst, _enc(member))

    def sinter(self, *keys: Any) -> set[bytes]:
        return self._c.sinter(_enc_list(keys))

    def sunion(self, *keys: Any) -> set[bytes]:
        return self._c.sunion(_enc_list(keys))

    def sdiff(self, *keys: Any) -> set[bytes]:
        return self._c.sdiff(_enc_list(keys))

    def sinterstore(self, dst: Any, *keys: Any) -> int:
        return self._c.sinterstore(dst, _enc_list(keys))

    def sunionstore(self, dst: Any, *keys: Any) -> int:
        return self._c.sunionstore(dst, _enc_list(keys))

    def sdiffstore(self, dst: Any, *keys: Any) -> int:
        return self._c.sdiffstore(dst, _enc_list(keys))

    def sscan(
        self,
        key: Any,
        cursor: Any = 0,
        match: Any = None,
        count: int | None = None,
    ) -> tuple[bytes, list[bytes]]:
        result = self._c.sscan(key, _enc(cursor), match=match, count=count)
        return result[0], list(result[1])

    def sscan_iter(self, key: Any, match: Any = None, count: int | None = None):
        cursor: Any = b"0"
        while True:
            result = self._c.sscan(key, cursor, match=match, count=count)
            cursor, members = result[0], result[1]
            yield from members
            if cursor in (b"0", "0", 0):
                return

    # ---- sorted sets ----
    def zadd(self, key: Any, mapping: Mapping[Any, float], **kwargs: Any) -> int:
        # Most callers just pass mapping; flags rarely used. Fall back to raw command for flags.
        if kwargs:
            args: list[Any] = [b"ZADD", key]
            if kwargs.get("nx"):
                args.append(b"NX")
            elif kwargs.get("xx"):
                args.append(b"XX")
            if kwargs.get("ch"):
                args.append(b"CH")
            if kwargs.get("incr"):
                args.append(b"INCR")
            for member, score in mapping.items():
                args.extend([_enc(score), _enc(member)])
            return self._c.custom_command(args)
        return self._c.zadd(key, {_enc(m): float(s) for m, s in mapping.items()})

    def zrange(self, key: Any, start: int, end: int, withscores: bool = False, desc: bool = False) -> Any:
        args = [b"ZRANGE", key, str(start).encode(), str(end).encode()]
        if desc:
            args.append(b"REV")
        if withscores:
            args.append(b"WITHSCORES")
        return self._c.custom_command(args)

    def zrevrange(self, key: Any, start: int, end: int, withscores: bool = False) -> Any:
        return self.zrange(key, start, end, withscores=withscores, desc=True)

    def zrangebyscore(self, key: Any, mn: Any, mx: Any, withscores: bool = False) -> Any:
        args = [b"ZRANGEBYSCORE", key, _enc(mn), _enc(mx)]
        if withscores:
            args.append(b"WITHSCORES")
        return self._c.custom_command(args)

    def zrevrangebyscore(self, key: Any, mx: Any, mn: Any, withscores: bool = False) -> Any:
        args = [b"ZREVRANGEBYSCORE", key, _enc(mx), _enc(mn)]
        if withscores:
            args.append(b"WITHSCORES")
        return self._c.custom_command(args)

    def zscore(self, key: Any, member: Any) -> float | None:
        return self._c.zscore(key, _enc(member))

    def zmscore(self, key: Any, members: list[Any]) -> list[float | None]:
        return self._c.zmscore(key, _enc_list(members))

    def zrank(self, key: Any, member: Any) -> int | None:
        return self._c.zrank(key, _enc(member))

    def zrevrank(self, key: Any, member: Any) -> int | None:
        return self._c.zrevrank(key, _enc(member))

    def zincrby(self, key: Any, amount: float, member: Any) -> float:
        return self._c.zincrby(key, amount, _enc(member))

    def zrem(self, key: Any, *members: Any) -> int:
        return self._c.zrem(key, _enc_list(members))

    def zremrangebyrank(self, key: Any, start: int, end: int) -> int:
        return self._c.zremrangebyrank(key, start, end)

    def zremrangebyscore(self, key: Any, mn: Any, mx: Any) -> int:
        return self._c.custom_command([b"ZREMRANGEBYSCORE", key, _enc(mn), _enc(mx)])

    def zcard(self, key: Any) -> int:
        return self._c.zcard(key)

    def zcount(self, key: Any, mn: Any, mx: Any) -> int:
        return self._c.zcount(key, _enc(mn), _enc(mx))

    def zpopmin(self, key: Any, count: int = 1) -> Any:
        return self._c.zpopmin(key, count)

    def zpopmax(self, key: Any, count: int = 1) -> Any:
        return self._c.zpopmax(key, count)

    # ---- lists ----
    def lpush(self, key: Any, *values: Any) -> int:
        return self._c.lpush(key, _enc_list(values))

    def rpush(self, key: Any, *values: Any) -> int:
        return self._c.rpush(key, _enc_list(values))

    def lpop(self, key: Any, count: int | None = None) -> Any:
        if count is None:
            return self._c.lpop(key)
        return self._c.lpop_count(key, count)

    def rpop(self, key: Any, count: int | None = None) -> Any:
        if count is None:
            return self._c.rpop(key)
        return self._c.rpop_count(key, count)

    def lrange(self, key: Any, start: int, end: int) -> list[bytes]:
        return self._c.lrange(key, start, end)

    def ltrim(self, key: Any, start: int, end: int) -> str:
        return self._c.ltrim(key, start, end)

    def llen(self, key: Any) -> int:
        return self._c.llen(key)

    def lindex(self, key: Any, index: int) -> bytes | None:
        return self._c.lindex(key, index)

    def lset(self, key: Any, index: int, value: Any) -> str:
        return self._c.lset(key, index, _enc(value))

    def lrem(self, key: Any, count: int, value: Any) -> int:
        return self._c.lrem(key, count, _enc(value))

    def linsert(self, key: Any, where: str, pivot: Any, value: Any) -> int:
        return self._c.custom_command(
            [
                b"LINSERT",
                key,
                _enc(where.upper()),
                _enc(pivot),
                _enc(value),
            ],
        )

    def lpos(self, key: Any, element: Any, **kwargs: Any) -> Any:
        args: list[Any] = [b"LPOS", key, _enc(element)]
        if "rank" in kwargs:
            args.extend([b"RANK", str(kwargs["rank"]).encode()])
        if "count" in kwargs:
            args.extend([b"COUNT", str(kwargs["count"]).encode()])
        if "maxlen" in kwargs:
            args.extend([b"MAXLEN", str(kwargs["maxlen"]).encode()])
        return self._c.custom_command(args)

    def lmove(self, src: Any, dst: Any, src_dir: str, dst_dir: str) -> bytes | None:
        return self._c.custom_command(
            [
                b"LMOVE",
                src,
                dst,
                _enc(src_dir.upper()),
                _enc(dst_dir.upper()),
            ],
        )

    def blmove(self, src: Any, dst: Any, src_dir: str, dst_dir: str, timeout: float) -> bytes | None:
        return self._c.custom_command(
            [
                b"BLMOVE",
                src,
                dst,
                _enc(src_dir.upper()),
                _enc(dst_dir.upper()),
                str(timeout).encode(),
            ],
        )

    def blpop(self, keys: Any, timeout: float = 0) -> Any:
        ks = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        return self._c.custom_command([b"BLPOP", *_enc_list(ks), str(timeout).encode()])

    def brpop(self, keys: Any, timeout: float = 0) -> Any:
        ks = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        return self._c.custom_command([b"BRPOP", *_enc_list(ks), str(timeout).encode()])

    # ---- streams (minimal) ----
    def xadd(self, key: Any, fields: Mapping[Any, Any], id: str = "*", **kwargs: Any) -> Any:
        args = [b"XADD", key, _enc(id)]
        for f, v in fields.items():
            args.extend([_enc(f), _enc(v)])
        return self._c.custom_command(args)

    def xlen(self, key: Any) -> int:
        return self._c.xlen(key)

    def xrange(self, key: Any, mn: str = "-", mx: str = "+", count: int | None = None) -> Any:
        args = [b"XRANGE", key, _enc(mn), _enc(mx)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        return self._c.custom_command(args)

    def xrevrange(self, key: Any, mx: str = "+", mn: str = "-", count: int | None = None) -> Any:
        args = [b"XREVRANGE", key, _enc(mx), _enc(mn)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        return self._c.custom_command(args)

    def xdel(self, key: Any, *ids: Any) -> int:
        return self._c.custom_command([b"XDEL", key, *_enc_list(ids)])

    def xtrim(self, key: Any, **kwargs: Any) -> int:
        args: list[Any] = [b"XTRIM", key]
        if "maxlen" in kwargs:
            args.extend([b"MAXLEN", str(kwargs["maxlen"]).encode()])
        elif "minid" in kwargs:
            args.extend([b"MINID", _enc(kwargs["minid"])])
        return self._c.custom_command(args)

    # ---- scripting ----
    def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        return self._c.custom_command([b"EVAL", _enc(script), str(numkeys).encode(), *_enc_list(args)])

    # ---- server ----
    def info(self, section: str | None = None) -> bytes:
        if section:
            return self._c.custom_command([b"INFO", _enc(section)])
        return self._c.custom_command([b"INFO"])

    def slowlog_get(self, n: int | None = None) -> Any:
        args = [b"SLOWLOG", b"GET"]
        if n is not None:
            args.append(str(n).encode())
        return self._c.custom_command(args)

    def slowlog_len(self) -> int:
        return self._c.custom_command([b"SLOWLOG", b"LEN"])

    def flushdb(self, asynchronous: bool = False) -> str:
        return self._c.flushdb(FlushMode.ASYNC if asynchronous else FlushMode.SYNC)

    def close(self) -> None:
        self._c.close()

    # ---- pipeline ----
    def pipeline(self, transaction: bool = False) -> _GlidePipelineShim:
        return _GlidePipelineShim(self._c, transaction=transaction)

    # ---- raw fallback for any unmapped method ----
    def __getattr__(self, name: str) -> Any:
        cmd = name.upper().encode()

        def call(*args: Any) -> Any:
            return self._c.custom_command([cmd, *_enc_list(args)])

        return call


# =============================================================================
# Async pipeline + redis-py shim
# =============================================================================


class _AsyncGlidePipelineShim:
    def __init__(self, client: AsyncGlideClient, *, transaction: bool = False) -> None:
        self._client = client
        self._batch = Batch(is_atomic=transaction)

    def set(self, key: Any, value: Any, **kw: Any) -> _AsyncGlidePipelineShim:
        self._batch.set(key, _enc(value), **_set_kwargs(**kw))
        return self

    def get(self, key: Any) -> _AsyncGlidePipelineShim:
        self._batch.get(key)
        return self

    def delete(self, *keys: Any) -> _AsyncGlidePipelineShim:
        self._batch.delete(_enc_list(keys))
        return self

    def mget(self, keys: Iterable[Any]) -> _AsyncGlidePipelineShim:
        self._batch.mget(_enc_list(keys))
        return self

    def mset(self, mapping: Mapping[Any, Any]) -> _AsyncGlidePipelineShim:
        self._batch.mset(_enc_map(mapping))
        return self

    def incr(self, key: Any, amount: int = 1) -> _AsyncGlidePipelineShim:
        if amount == 1:
            self._batch.incr(key)
        else:
            self._batch.incrby(key, amount)
        return self

    def expire(self, key: Any, seconds: int) -> _AsyncGlidePipelineShim:
        self._batch.expire(key, seconds)
        return self

    def ttl(self, key: Any) -> _AsyncGlidePipelineShim:
        self._batch.ttl(key)
        return self

    def exists(self, *keys: Any) -> _AsyncGlidePipelineShim:
        self._batch.exists(_enc_list(keys))
        return self

    def persist(self, key: Any) -> _AsyncGlidePipelineShim:
        self._batch.persist(key)
        return self

    def type(self, key: Any) -> _AsyncGlidePipelineShim:
        self._batch.type(key)
        return self

    def __getattr__(self, name: str) -> Any:
        cmd = name.upper()

        def call(*args: Any) -> _AsyncGlidePipelineShim:
            self._batch.custom_command([cmd, *_enc_list(args)])
            return self

        return call

    async def execute(self) -> list[Any]:
        result = await self._client.exec(self._batch, raise_on_error=True)
        return result or []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class _AsyncGlideRedisShim:
    """Async redis-py-shaped wrapper around ``glide.GlideClient``.

    The underlying client is created lazily on first call so that
    ``get_async_client`` can stay synchronous.
    """

    def __init__(self, server_url: str) -> None:
        self._server_url = server_url
        self._client: AsyncGlideClient | None = None
        self._lock = asyncio.Lock()

    async def _ensure(self) -> AsyncGlideClient:
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                u = urlparse(self._server_url)
                cfg = AsyncGlideCfg(
                    addresses=[AsyncNodeAddress(u.hostname or "localhost", u.port or 6379)],
                )
                self._client = await AsyncGlideClient.create(cfg)
        return self._client

    # ---- strings ----
    async def get(self, key: Any) -> bytes | None:
        c = await self._ensure()
        return await c.get(key)

    async def set(
        self,
        key: Any,
        value: Any,
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
    ) -> bool:
        c = await self._ensure()
        result = await c.set(key, _enc(value), **_set_kwargs(ex=ex, px=px, nx=nx, xx=xx, keepttl=keepttl))
        return result == "OK"

    async def mget(self, keys: Iterable[Any]) -> list[bytes | None]:
        c = await self._ensure()
        return await c.mget(list(keys))

    async def mset(self, mapping: Mapping[Any, Any]) -> str:
        c = await self._ensure()
        return await c.mset(_enc_map(mapping))

    async def incr(self, key: Any, amount: int = 1) -> int:
        c = await self._ensure()
        if amount == 1:
            return await c.incr(key)
        return await c.incrby(key, amount)

    async def incrby(self, key: Any, amount: int) -> int:
        c = await self._ensure()
        return await c.incrby(key, amount)

    async def incrbyfloat(self, key: Any, amount: float) -> float:
        c = await self._ensure()
        return await c.incrbyfloat(key, amount)

    async def decr(self, key: Any, amount: int = 1) -> int:
        c = await self._ensure()
        if amount == 1:
            return await c.decr(key)
        return await c.decrby(key, amount)

    # ---- keys ----
    async def delete(self, *keys: Any) -> int:
        if not keys:
            return 0
        c = await self._ensure()
        return await c.delete(_enc_list(keys))

    async def exists(self, *keys: Any) -> int:
        if not keys:
            return 0
        c = await self._ensure()
        return await c.exists(_enc_list(keys))

    async def expire(self, key: Any, seconds: int) -> bool:
        c = await self._ensure()
        return await c.expire(key, seconds)

    async def pexpire(self, key: Any, ms: int) -> bool:
        c = await self._ensure()
        return await c.pexpire(key, ms)

    async def expireat(self, key: Any, when: int) -> bool:
        c = await self._ensure()
        return await c.expireat(key, when)

    async def pexpireat(self, key: Any, when: int) -> bool:
        c = await self._ensure()
        return await c.pexpireat(key, when)

    async def expiretime(self, key: Any) -> int:
        c = await self._ensure()
        return await c.expiretime(key)

    async def ttl(self, key: Any) -> int:
        c = await self._ensure()
        return await c.ttl(key)

    async def pttl(self, key: Any) -> int:
        c = await self._ensure()
        return await c.pttl(key)

    async def persist(self, key: Any) -> bool:
        c = await self._ensure()
        return await c.persist(key)

    async def type(self, key: Any) -> bytes:
        c = await self._ensure()
        return await c.type(key)

    async def rename(self, src: Any, dst: Any) -> str:
        c = await self._ensure()
        return await c.rename(src, dst)

    async def renamenx(self, src: Any, dst: Any) -> bool:
        c = await self._ensure()
        return await c.renamenx(src, dst)

    async def keys(self, pattern: Any = "*") -> list[bytes]:
        c = await self._ensure()
        result = await c.custom_command([b"KEYS", _enc(pattern)])
        return list(result) if result else []

    async def scan(
        self,
        cursor: Any = 0,
        match: Any = None,
        count: int | None = None,
    ) -> tuple[bytes, list[bytes]]:
        c = await self._ensure()
        result = await c.scan(_enc(cursor), match=match, count=count)
        return result[0], list(result[1])

    async def scan_iter(self, match: Any = None, count: int | None = None):
        c = await self._ensure()
        cursor: Any = b"0"
        while True:
            result = await c.scan(cursor, match=match, count=count)
            cursor, keys = result[0], result[1]
            for k in keys:
                yield k
            if cursor in (b"0", "0", 0):
                return

    # ---- hashes ----
    async def hset(
        self,
        key: Any,
        field: Any = None,
        value: Any = None,
        mapping: Mapping[Any, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        c = await self._ensure()
        m: dict[Any, Any] = {}
        if field is not None:
            m[field] = _enc(value)
        if mapping:
            m.update(_enc_map(mapping))
        if items:
            for i in range(0, len(items), 2):
                m[items[i]] = _enc(items[i + 1])
        if not m:
            return 0
        return await c.hset(key, m)

    async def hsetnx(self, key: Any, field: Any, value: Any) -> bool:
        c = await self._ensure()
        return await c.hsetnx(key, field, _enc(value))

    async def hget(self, key: Any, field: Any) -> bytes | None:
        c = await self._ensure()
        return await c.hget(key, field)

    async def hmget(self, key: Any, *fields: Any) -> list[bytes | None]:
        c = await self._ensure()
        if len(fields) == 1 and isinstance(fields[0], (list, tuple)):
            fields = tuple(fields[0])
        return await c.hmget(key, list(fields))

    async def hgetall(self, key: Any) -> dict[bytes, bytes]:
        c = await self._ensure()
        return await c.hgetall(key)

    async def hkeys(self, key: Any) -> list[bytes]:
        c = await self._ensure()
        return await c.hkeys(key)

    async def hvals(self, key: Any) -> list[bytes]:
        c = await self._ensure()
        return await c.hvals(key)

    async def hlen(self, key: Any) -> int:
        c = await self._ensure()
        return await c.hlen(key)

    async def hexists(self, key: Any, field: Any) -> bool:
        c = await self._ensure()
        return await c.hexists(key, field)

    async def hdel(self, key: Any, *fields: Any) -> int:
        c = await self._ensure()
        return await c.hdel(key, list(fields))

    async def hincrby(self, key: Any, field: Any, amount: int = 1) -> int:
        c = await self._ensure()
        return await c.hincrby(key, field, amount)

    async def hincrbyfloat(self, key: Any, field: Any, amount: float) -> float:
        c = await self._ensure()
        return await c.hincrbyfloat(key, field, amount)

    # ---- sets ----
    async def sadd(self, key: Any, *members: Any) -> int:
        c = await self._ensure()
        return await c.sadd(key, _enc_list(members))

    async def srem(self, key: Any, *members: Any) -> int:
        c = await self._ensure()
        return await c.srem(key, _enc_list(members))

    async def smembers(self, key: Any) -> set[bytes]:
        c = await self._ensure()
        return await c.smembers(key)

    async def sismember(self, key: Any, member: Any) -> bool:
        c = await self._ensure()
        return await c.sismember(key, _enc(member))

    async def scard(self, key: Any) -> int:
        c = await self._ensure()
        return await c.scard(key)

    # ---- sorted sets ----
    async def zadd(self, key: Any, mapping: Mapping[Any, float], **kwargs: Any) -> int:
        c = await self._ensure()
        if kwargs:
            args: list[Any] = [b"ZADD", key]
            if kwargs.get("nx"):
                args.append(b"NX")
            elif kwargs.get("xx"):
                args.append(b"XX")
            if kwargs.get("ch"):
                args.append(b"CH")
            if kwargs.get("incr"):
                args.append(b"INCR")
            for member, score in mapping.items():
                args.extend([_enc(score), _enc(member)])
            return await c.custom_command(args)
        return await c.zadd(key, {_enc(m): float(s) for m, s in mapping.items()})

    async def zrem(self, key: Any, *members: Any) -> int:
        c = await self._ensure()
        return await c.zrem(key, _enc_list(members))

    async def zscore(self, key: Any, member: Any) -> float | None:
        c = await self._ensure()
        return await c.zscore(key, _enc(member))

    async def zcard(self, key: Any) -> int:
        c = await self._ensure()
        return await c.zcard(key)

    async def zrange(self, key: Any, start: int, end: int, withscores: bool = False, desc: bool = False) -> Any:
        c = await self._ensure()
        args = [b"ZRANGE", key, str(start).encode(), str(end).encode()]
        if desc:
            args.append(b"REV")
        if withscores:
            args.append(b"WITHSCORES")
        return await c.custom_command(args)

    # ---- lists ----
    async def lpush(self, key: Any, *values: Any) -> int:
        c = await self._ensure()
        return await c.lpush(key, _enc_list(values))

    async def rpush(self, key: Any, *values: Any) -> int:
        c = await self._ensure()
        return await c.rpush(key, _enc_list(values))

    async def lpop(self, key: Any, count: int | None = None) -> Any:
        c = await self._ensure()
        if count is None:
            return await c.lpop(key)
        return await c.lpop_count(key, count)

    async def rpop(self, key: Any, count: int | None = None) -> Any:
        c = await self._ensure()
        if count is None:
            return await c.rpop(key)
        return await c.rpop_count(key, count)

    async def lrange(self, key: Any, start: int, end: int) -> list[bytes]:
        c = await self._ensure()
        return await c.lrange(key, start, end)

    async def llen(self, key: Any) -> int:
        c = await self._ensure()
        return await c.llen(key)

    # ---- scripting ----
    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        c = await self._ensure()
        return await c.custom_command([b"EVAL", _enc(script), str(numkeys).encode(), *_enc_list(args)])

    # ---- server ----
    async def info(self, section: str | None = None) -> bytes:
        c = await self._ensure()
        if section:
            return await c.custom_command([b"INFO", _enc(section)])
        return await c.custom_command([b"INFO"])

    async def flushdb(self, asynchronous: bool = False) -> str:
        c = await self._ensure()
        return await c.flushdb(FlushMode.ASYNC if asynchronous else FlushMode.SYNC)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()

    # ---- pipeline ----
    def pipeline(self, transaction: bool = False) -> _AsyncGlidePipelineShim:
        # Caller must await the first method to materialize the underlying client.
        # _AsyncGlidePipelineShim re-resolves via _ensure() on execute().
        if self._client is None:
            msg = "Async pipeline requires an awaited op first to instantiate the client"
            raise RuntimeError(msg)
        return _AsyncGlidePipelineShim(self._client, transaction=transaction)

    # ---- raw fallback ----
    def __getattr__(self, name: str) -> Any:
        cmd = name.upper().encode()

        async def call(*args: Any) -> Any:
            c = await self._ensure()
            return await c.custom_command([cmd, *_enc_list(args)])

        return call


# =============================================================================
# Cache client
# =============================================================================


class ValkeyGlideCacheClient(KeyValueCacheClient):
    """Cache client backed by ``valkey-glide`` (sync + async)."""

    _lib: Any = None
    _client_class: Any = None
    _pool_class: Any = None
    _async_client_class: Any = None
    _async_pool_class: Any = None

    def __init__(self, servers: list[str], **options: Any) -> None:
        super().__init__(servers, **options)
        self._glide_clients: dict[int, _GlideRedisShim] = {}
        self._async_glide_clients: dict[tuple[int, int], _AsyncGlideRedisShim] = {}

    def _build_glide_client(self, server_url: str) -> _GlideRedisShim:
        u = urlparse(server_url)
        cfg = GlideClientConfiguration(
            addresses=[NodeAddress(u.hostname or "localhost", u.port or 6379)],
        )
        return _GlideRedisShim(GlideClient.create(cfg))

    def get_client(self, key: Any = None, *, write: bool = False) -> _GlideRedisShim:
        if 0 not in self._glide_clients:
            self._glide_clients[0] = self._build_glide_client(self._servers[0])
        return self._glide_clients[0]

    def get_async_client(self, key: Any = None, *, write: bool = False) -> _AsyncGlideRedisShim:
        loop = asyncio.get_running_loop()
        cache_key = (id(loop), 0)
        if cache_key not in self._async_glide_clients:
            self._async_glide_clients[cache_key] = _AsyncGlideRedisShim(self._servers[0])
        return self._async_glide_clients[cache_key]


__all__ = ["ValkeyGlideCacheClient"]
