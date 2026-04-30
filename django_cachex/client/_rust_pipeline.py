"""Raw pipeline shim for the Rust driver.

The shared :class:`~django_cachex.client.pipeline.Pipeline` wrapper drives an
underlying ``self._pipeline`` object whose method surface mirrors redis-py's
``Pipeline`` (``set``, ``get``, ``hset``, ``zadd``, ...). This module provides
that surface for the Rust driver: each method buffers a Redis wire command
plus an optional response parser, and ``execute()`` dispatches the whole batch
in one round trip via ``RustValkeyDriver.pipeline_exec_sync``.

We deliver results in the same Python shapes redis-py returns so the wrapper's
decoders (which were written against redis-py) work unchanged. The driver runs
RESP3, so HGETALL/CONFIG GET return real ``dict``s, ZRANGE WITHSCORES returns
nested arrays, sets come back as lists, etc. — see the parsers below.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from django_cachex._driver import RustValkeyDriver  # ty: ignore[unresolved-import]


def _to_bytes(value: Any) -> bytes:  # noqa: PLR0911
    """Coerce a Redis argument to bytes for the wire."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bool):
        return b"1" if value else b"0"
    if isinstance(value, int):
        return str(value).encode("ascii")
    if isinstance(value, float):
        return repr(value).encode("ascii")
    msg = f"cannot encode {type(value).__name__} as a Redis argument"
    raise TypeError(msg)


def _seconds(value: int | timedelta) -> int:
    if isinstance(value, timedelta):
        return int(value.total_seconds())
    return int(value)


def _milliseconds(value: int | timedelta) -> int:
    if isinstance(value, timedelta):
        return int(value.total_seconds() * 1000)
    return int(value)


def _epoch_seconds(value: int | datetime) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp())
    return int(value)


def _epoch_milliseconds(value: int | datetime) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return int(value)


# =============================================================================
# Parsers — translate raw `redis::Value` (already converted to Python natives
# by the driver) into the shapes redis-py would produce.
# =============================================================================


def _identity(v: Any) -> Any:
    return v


def _to_int_or_none(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, (bytes, bytearray)):
        return int(v)
    if isinstance(v, str):
        return int(v)
    return v


def _to_float_or_none(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, (bytes, bytearray, str)):
        return float(v)
    return v


def _list_to_float_or_none(v: Any) -> list[float | None]:
    return [_to_float_or_none(x) for x in v]


def _bytes_or_none_to_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bytes):
        return v.decode()
    return v


def _hgetall(v: Any) -> dict[bytes, bytes]:
    """RESP3 HGETALL is a Map; older protocols send a flat array. Normalize to dict."""
    if isinstance(v, dict):
        return v
    if v is None:
        return {}
    items = list(v)
    return {items[i]: items[i + 1] for i in range(0, len(items), 2)}


def _zset_with_scores(v: Any) -> list[tuple[bytes, float]]:
    """ZRANGE/ZREVRANGE WITHSCORES — handle RESP3 nested arrays and RESP2 flat lists."""
    if not v:
        return []
    if isinstance(v, dict):
        return [(member, float(score)) for member, score in v.items()]
    first = v[0]
    if isinstance(first, (list, tuple)):
        # RESP3: list of [member, score]
        return [(pair[0], float(pair[1])) for pair in v]
    # RESP2: flat alternating member/score
    return [(v[i], float(v[i + 1])) for i in range(0, len(v), 2)]


def _zpop(v: Any) -> list[tuple[bytes, float]]:
    """ZPOPMIN/ZPOPMAX — same nested/flat handling as WITHSCORES results."""
    return _zset_with_scores(v)


def _zincrby(v: Any) -> float | None:
    """ZINCRBY returns the new score. RESP3: double; RESP2: bulk string."""
    return _to_float_or_none(v)


def _stream_entry(entry: Any) -> tuple[str, dict[str, bytes]]:
    """One stream entry: [id, [k1,v1,k2,v2,...]] → (id_str, {k_str: v_bytes})."""
    eid = entry[0]
    if isinstance(eid, bytes):
        eid = eid.decode()
    fields = entry[1]
    if isinstance(fields, dict):
        d = {(k.decode() if isinstance(k, bytes) else k): v for k, v in fields.items()}
    elif fields is None:
        d = {}
    else:
        flat = list(fields)
        d = {(flat[i].decode() if isinstance(flat[i], bytes) else flat[i]): flat[i + 1] for i in range(0, len(flat), 2)}
    return (eid, d)


def _stream_entries(v: Any) -> list[tuple[str, dict[str, bytes]]]:
    if v is None:
        return []
    return [_stream_entry(e) for e in v]


def _stream_read(v: Any) -> list[tuple[Any, list[tuple[str, dict[str, bytes]]]]] | None:
    """XREAD/XREADGROUP — list of (stream_key, entries). RESP3 may yield a Map."""
    if v is None or v in ([], {}):
        return None
    if isinstance(v, dict):
        return [(k, _stream_entries(entries)) for k, entries in v.items()]
    return [(stream[0], _stream_entries(stream[1])) for stream in v]


def _xpending_summary(v: Any) -> Any:
    """Summary form: [count, min_id, max_id, [[consumer, count_str], ...]]."""
    return v


def _xpending_range(v: Any) -> list[list[Any]]:
    """Range form: list of [id, consumer, idle_ms, deliveries]."""
    if v is None:
        return []
    return [list(entry) for entry in v]


def _xinfo_dict(v: Any) -> dict[str, Any]:
    """XINFO STREAM — RESP3 Map, RESP2 flat list of alternating k/v."""
    if isinstance(v, dict):
        return {(k.decode() if isinstance(k, bytes) else k): val for k, val in v.items()}
    if v is None:
        return {}
    flat = list(v)
    return {(flat[i].decode() if isinstance(flat[i], bytes) else flat[i]): flat[i + 1] for i in range(0, len(flat), 2)}


def _xinfo_dict_list(v: Any) -> list[dict[str, Any]]:
    if v is None:
        return []
    return [_xinfo_dict(item) for item in v]


def _xautoclaim(v: Any) -> list[Any]:
    """XAUTOCLAIM returns [next_id, [entries], [deleted_ids]]."""
    out: list[Any] = []
    if v is None:
        return out
    items = list(v)
    if items:
        out.append(items[0])
    if len(items) > 1:
        out.append(_stream_entries(items[1]))
    if len(items) > 2:
        out.append(items[2])
    return out


# =============================================================================
# The pipeline class itself.
# =============================================================================


class _RustRawPipeline:
    """Buffers commands for batched execution against the Rust driver.

    Implements the subset of the redis-py ``Pipeline`` API that the shared
    :class:`~django_cachex.client.pipeline.Pipeline` wrapper actually calls.
    """

    def __init__(self, driver: RustValkeyDriver, *, transaction: bool = True) -> None:
        self._driver = driver
        self._transaction = transaction
        self._commands: list[tuple[str, list[bytes]]] = []
        self._parsers: list[Callable[[Any], Any] | None] = []

    # ------------------------------------------------------------------ core

    def _queue(
        self,
        cmd: str,
        *args: Any,
        parser: Callable[[Any], Any] | None = None,
    ) -> _RustRawPipeline:
        self._commands.append((cmd, [_to_bytes(a) for a in args]))
        self._parsers.append(parser)
        return self

    def reset(self) -> None:
        self._commands.clear()
        self._parsers.clear()

    def execute(self) -> list[Any]:
        if not self._commands:
            self._parsers.clear()
            return []
        commands = self._commands
        parsers = self._parsers
        self._commands = []
        self._parsers = []
        raw = self._driver.pipeline_exec_sync(commands, self._transaction)
        out: list[Any] = []
        for value, parser in zip(raw, parsers, strict=True):
            out.append(parser(value) if parser is not None else value)
        return out

    def execute_command(self, *args: Any) -> _RustRawPipeline:
        """Queue a raw Redis command (used by the wrapper's ``eval_script``)."""
        if not args:
            msg = "execute_command requires at least the command name"
            raise ValueError(msg)
        cmd_name = args[0] if isinstance(args[0], str) else args[0].decode()
        return self._queue(cmd_name, *args[1:])

    # ============================================================== strings

    def set(
        self,
        key: Any,
        value: Any,
        *,
        ex: int | timedelta | None = None,
        px: int | timedelta | None = None,
        nx: bool = False,
        xx: bool = False,
        exat: int | datetime | None = None,
        pxat: int | datetime | None = None,
        keepttl: bool = False,
        get: bool = False,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, value]
        if ex is not None:
            args.extend([b"EX", _seconds(ex)])
        if px is not None:
            args.extend([b"PX", _milliseconds(px)])
        if exat is not None:
            args.extend([b"EXAT", _epoch_seconds(exat)])
        if pxat is not None:
            args.extend([b"PXAT", _epoch_milliseconds(pxat)])
        if keepttl:
            args.append(b"KEEPTTL")
        if nx:
            args.append(b"NX")
        if xx:
            args.append(b"XX")
        if get:
            args.append(b"GET")
        return self._queue("SET", *args)

    def get(self, key: Any) -> _RustRawPipeline:
        return self._queue("GET", key)

    def delete(self, *keys: Any) -> _RustRawPipeline:
        return self._queue("DEL", *keys)

    def exists(self, *keys: Any) -> _RustRawPipeline:
        return self._queue("EXISTS", *keys)

    def expire(
        self,
        key: Any,
        seconds: int | timedelta,
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, _seconds(seconds)]
        if nx:
            args.append(b"NX")
        if xx:
            args.append(b"XX")
        if gt:
            args.append(b"GT")
        if lt:
            args.append(b"LT")
        return self._queue("EXPIRE", *args, parser=bool)

    def expireat(
        self,
        key: Any,
        when: int | datetime,
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, _epoch_seconds(when)]
        if nx:
            args.append(b"NX")
        if xx:
            args.append(b"XX")
        if gt:
            args.append(b"GT")
        if lt:
            args.append(b"LT")
        return self._queue("EXPIREAT", *args, parser=bool)

    def pexpire(
        self,
        key: Any,
        milliseconds: int | timedelta,
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, _milliseconds(milliseconds)]
        if nx:
            args.append(b"NX")
        if xx:
            args.append(b"XX")
        if gt:
            args.append(b"GT")
        if lt:
            args.append(b"LT")
        return self._queue("PEXPIRE", *args, parser=bool)

    def pexpireat(
        self,
        key: Any,
        when: int | datetime,
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, _epoch_milliseconds(when)]
        if nx:
            args.append(b"NX")
        if xx:
            args.append(b"XX")
        if gt:
            args.append(b"GT")
        if lt:
            args.append(b"LT")
        return self._queue("PEXPIREAT", *args, parser=bool)

    def persist(self, key: Any) -> _RustRawPipeline:
        return self._queue("PERSIST", key, parser=bool)

    def ttl(self, key: Any) -> _RustRawPipeline:
        return self._queue("TTL", key)

    def pttl(self, key: Any) -> _RustRawPipeline:
        return self._queue("PTTL", key)

    def expiretime(self, key: Any) -> _RustRawPipeline:
        return self._queue("EXPIRETIME", key)

    def type(self, key: Any) -> _RustRawPipeline:
        return self._queue("TYPE", key)

    def rename(self, src: Any, dst: Any) -> _RustRawPipeline:
        return self._queue("RENAME", src, dst)

    def renamenx(self, src: Any, dst: Any) -> _RustRawPipeline:
        return self._queue("RENAMENX", src, dst)

    def incrby(self, key: Any, amount: int = 1) -> _RustRawPipeline:
        return self._queue("INCRBY", key, amount)

    def decrby(self, key: Any, amount: int = 1) -> _RustRawPipeline:
        return self._queue("DECRBY", key, amount)

    # ================================================================ lists

    def lpush(self, key: Any, *values: Any) -> _RustRawPipeline:
        return self._queue("LPUSH", key, *values)

    def rpush(self, key: Any, *values: Any) -> _RustRawPipeline:
        return self._queue("RPUSH", key, *values)

    def lpop(self, key: Any, count: int | None = None) -> _RustRawPipeline:
        if count is None:
            return self._queue("LPOP", key)
        return self._queue("LPOP", key, count)

    def rpop(self, key: Any, count: int | None = None) -> _RustRawPipeline:
        if count is None:
            return self._queue("RPOP", key)
        return self._queue("RPOP", key, count)

    def lrange(self, key: Any, start: int, end: int) -> _RustRawPipeline:
        return self._queue("LRANGE", key, start, end)

    def lindex(self, key: Any, index: int) -> _RustRawPipeline:
        return self._queue("LINDEX", key, index)

    def llen(self, key: Any) -> _RustRawPipeline:
        return self._queue("LLEN", key)

    def lrem(self, key: Any, count: int, value: Any) -> _RustRawPipeline:
        return self._queue("LREM", key, count, value)

    def ltrim(self, key: Any, start: int, end: int) -> _RustRawPipeline:
        return self._queue("LTRIM", key, start, end)

    def lset(self, key: Any, index: int, value: Any) -> _RustRawPipeline:
        return self._queue("LSET", key, index, value)

    def linsert(self, key: Any, where: str, pivot: Any, value: Any) -> _RustRawPipeline:
        return self._queue("LINSERT", key, where.upper(), pivot, value)

    def lpos(
        self,
        key: Any,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, value]
        if rank is not None:
            args.extend([b"RANK", rank])
        if count is not None:
            args.extend([b"COUNT", count])
        if maxlen is not None:
            args.extend([b"MAXLEN", maxlen])
        return self._queue("LPOS", *args)

    def lmove(
        self,
        source: Any,
        destination: Any,
        src: str = "LEFT",
        dest: str = "RIGHT",
    ) -> _RustRawPipeline:
        return self._queue("LMOVE", source, destination, src.upper(), dest.upper())

    # ================================================================= sets

    def sadd(self, key: Any, *members: Any) -> _RustRawPipeline:
        return self._queue("SADD", key, *members)

    def srem(self, key: Any, *members: Any) -> _RustRawPipeline:
        return self._queue("SREM", key, *members)

    def smembers(self, key: Any) -> _RustRawPipeline:
        return self._queue("SMEMBERS", key)

    def sismember(self, key: Any, member: Any) -> _RustRawPipeline:
        return self._queue("SISMEMBER", key, member)

    def smismember(self, key: Any, *members: Any) -> _RustRawPipeline:
        return self._queue("SMISMEMBER", key, *members)

    def scard(self, key: Any) -> _RustRawPipeline:
        return self._queue("SCARD", key)

    def sdiff(self, *keys: Any) -> _RustRawPipeline:
        return self._queue("SDIFF", *keys)

    def sdiffstore(self, dest: Any, *keys: Any) -> _RustRawPipeline:
        return self._queue("SDIFFSTORE", dest, *keys)

    def sinter(self, *keys: Any) -> _RustRawPipeline:
        return self._queue("SINTER", *keys)

    def sinterstore(self, dest: Any, *keys: Any) -> _RustRawPipeline:
        return self._queue("SINTERSTORE", dest, *keys)

    def sunion(self, *keys: Any) -> _RustRawPipeline:
        return self._queue("SUNION", *keys)

    def sunionstore(self, dest: Any, *keys: Any) -> _RustRawPipeline:
        return self._queue("SUNIONSTORE", dest, *keys)

    def smove(self, source: Any, destination: Any, member: Any) -> _RustRawPipeline:
        return self._queue("SMOVE", source, destination, member)

    def spop(self, key: Any, count: int | None = None) -> _RustRawPipeline:
        if count is None:
            return self._queue("SPOP", key)
        return self._queue("SPOP", key, count)

    def srandmember(self, key: Any, count: int | None = None) -> _RustRawPipeline:
        if count is None:
            return self._queue("SRANDMEMBER", key)
        return self._queue("SRANDMEMBER", key, count)

    # =============================================================== hashes

    def hset(
        self,
        key: Any,
        field: str | None = None,
        value: Any = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> _RustRawPipeline:
        args: list[Any] = [key]
        if field is not None:
            args.extend([field, value])
        if mapping:
            for k, v in mapping.items():
                args.extend([k, v])
        if items:
            it = iter(items)
            for f, v in zip(it, it, strict=False):
                args.extend([f, v])
        return self._queue("HSET", *args)

    def hsetnx(self, key: Any, field: str, value: Any) -> _RustRawPipeline:
        return self._queue("HSETNX", key, field, value)

    def hdel(self, key: Any, *fields: str) -> _RustRawPipeline:
        return self._queue("HDEL", key, *fields)

    def hexists(self, key: Any, field: str) -> _RustRawPipeline:
        return self._queue("HEXISTS", key, field)

    def hget(self, key: Any, field: str) -> _RustRawPipeline:
        return self._queue("HGET", key, field)

    def hgetall(self, key: Any) -> _RustRawPipeline:
        return self._queue("HGETALL", key, parser=_hgetall)

    def hkeys(self, key: Any) -> _RustRawPipeline:
        return self._queue("HKEYS", key)

    def hvals(self, key: Any) -> _RustRawPipeline:
        return self._queue("HVALS", key)

    def hlen(self, key: Any) -> _RustRawPipeline:
        return self._queue("HLEN", key)

    def hmget(self, key: Any, fields: Iterable[str] | str) -> _RustRawPipeline:
        if isinstance(fields, (str, bytes)):
            return self._queue("HMGET", key, fields)
        return self._queue("HMGET", key, *fields)

    def hincrby(self, key: Any, field: str, amount: int = 1) -> _RustRawPipeline:
        return self._queue("HINCRBY", key, field, amount)

    def hincrbyfloat(self, key: Any, field: str, amount: float = 1.0) -> _RustRawPipeline:
        return self._queue("HINCRBYFLOAT", key, field, amount, parser=_to_float_or_none)

    # ========================================================== sorted sets

    def zadd(
        self,
        key: Any,
        mapping: Mapping[Any, float],
        *,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> _RustRawPipeline:
        args: list[Any] = [key]
        if nx:
            args.append(b"NX")
        if xx:
            args.append(b"XX")
        if gt:
            args.append(b"GT")
        if lt:
            args.append(b"LT")
        if ch:
            args.append(b"CH")
        if incr:
            args.append(b"INCR")
        for member, score in mapping.items():
            args.extend([score, member])
        # ZADD ... INCR returns the new score (bulk string / double); plain ZADD returns int.
        parser = _to_float_or_none if incr else None
        return self._queue("ZADD", *args, parser=parser)

    def zcard(self, key: Any) -> _RustRawPipeline:
        return self._queue("ZCARD", key)

    def zcount(self, key: Any, min: Any, max: Any) -> _RustRawPipeline:
        return self._queue("ZCOUNT", key, min, max)

    def zincrby(self, key: Any, amount: float, value: Any) -> _RustRawPipeline:
        return self._queue("ZINCRBY", key, amount, value, parser=_zincrby)

    def zpopmax(self, key: Any, count: int = 1) -> _RustRawPipeline:
        return self._queue("ZPOPMAX", key, count, parser=_zpop)

    def zpopmin(self, key: Any, count: int = 1) -> _RustRawPipeline:
        return self._queue("ZPOPMIN", key, count, parser=_zpop)

    def zrange(
        self,
        key: Any,
        start: int,
        end: int,
        *,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: type = float,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, start, end]
        if desc:
            args.append(b"REV")
        if withscores:
            args.append(b"WITHSCORES")
        parser = _zset_with_scores if withscores else None
        return self._queue("ZRANGE", *args, parser=parser)

    def zrevrange(
        self,
        key: Any,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        score_cast_func: type = float,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, start, end]
        if withscores:
            args.append(b"WITHSCORES")
        parser = _zset_with_scores if withscores else None
        return self._queue("ZREVRANGE", *args, parser=parser)

    def zrangebyscore(
        self,
        key: Any,
        min: Any,
        max: Any,
        start: int | None = None,
        num: int | None = None,
        *,
        withscores: bool = False,
        score_cast_func: type = float,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, min, max]
        if withscores:
            args.append(b"WITHSCORES")
        if start is not None and num is not None:
            args.extend([b"LIMIT", start, num])
        parser = _zset_with_scores if withscores else None
        return self._queue("ZRANGEBYSCORE", *args, parser=parser)

    def zrevrangebyscore(
        self,
        key: Any,
        max: Any,
        min: Any,
        start: int | None = None,
        num: int | None = None,
        *,
        withscores: bool = False,
        score_cast_func: type = float,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, max, min]
        if withscores:
            args.append(b"WITHSCORES")
        if start is not None and num is not None:
            args.extend([b"LIMIT", start, num])
        parser = _zset_with_scores if withscores else None
        return self._queue("ZREVRANGEBYSCORE", *args, parser=parser)

    def zrank(self, key: Any, value: Any) -> _RustRawPipeline:
        return self._queue("ZRANK", key, value)

    def zrevrank(self, key: Any, value: Any) -> _RustRawPipeline:
        return self._queue("ZREVRANK", key, value)

    def zscore(self, key: Any, value: Any) -> _RustRawPipeline:
        return self._queue("ZSCORE", key, value, parser=_to_float_or_none)

    def zmscore(self, key: Any, members: Iterable[Any]) -> _RustRawPipeline:
        return self._queue("ZMSCORE", key, *members, parser=_list_to_float_or_none)

    def zrem(self, key: Any, *values: Any) -> _RustRawPipeline:
        return self._queue("ZREM", key, *values)

    def zremrangebyscore(
        self,
        key: Any,
        min: Any,
        max: Any,
    ) -> _RustRawPipeline:
        return self._queue("ZREMRANGEBYSCORE", key, min, max)

    def zremrangebyrank(self, key: Any, start: int, end: int) -> _RustRawPipeline:
        return self._queue("ZREMRANGEBYRANK", key, start, end)

    # ============================================================== streams

    def xadd(
        self,
        key: Any,
        fields: Mapping[str, Any],
        *,
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
    ) -> _RustRawPipeline:
        args: list[Any] = [key]
        if nomkstream:
            args.append(b"NOMKSTREAM")
        if maxlen is not None:
            args.append(b"MAXLEN")
            args.append(b"~" if approximate else b"=")
            args.append(maxlen)
        elif minid is not None:
            args.append(b"MINID")
            args.append(b"~" if approximate else b"=")
            args.append(minid)
        if limit is not None:
            args.extend([b"LIMIT", limit])
        args.append(id)
        for k, v in fields.items():
            args.extend([k, v])
        return self._queue("XADD", *args, parser=_bytes_or_none_to_str)

    def xlen(self, key: Any) -> _RustRawPipeline:
        return self._queue("XLEN", key)

    def xrange(
        self,
        key: Any,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, min, max]
        if count is not None:
            args.extend([b"COUNT", count])
        return self._queue("XRANGE", *args, parser=_stream_entries)

    def xrevrange(
        self,
        key: Any,
        max: str = "+",
        min: str = "-",
        count: int | None = None,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, max, min]
        if count is not None:
            args.extend([b"COUNT", count])
        return self._queue("XREVRANGE", *args, parser=_stream_entries)

    def xread(
        self,
        streams: Mapping[Any, Any],
        count: int | None = None,
        block: int | None = None,
    ) -> _RustRawPipeline:
        args: list[Any] = []
        if count is not None:
            args.extend([b"COUNT", count])
        if block is not None:
            args.extend([b"BLOCK", block])
        args.append(b"STREAMS")
        keys = list(streams.keys())
        ids = list(streams.values())
        args.extend(keys)
        args.extend(ids)
        return self._queue("XREAD", *args, parser=_stream_read)

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Mapping[Any, Any],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> _RustRawPipeline:
        args: list[Any] = [b"GROUP", groupname, consumername]
        if count is not None:
            args.extend([b"COUNT", count])
        if block is not None:
            args.extend([b"BLOCK", block])
        if noack:
            args.append(b"NOACK")
        args.append(b"STREAMS")
        keys = list(streams.keys())
        ids = list(streams.values())
        args.extend(keys)
        args.extend(ids)
        return self._queue("XREADGROUP", *args, parser=_stream_read)

    def xtrim(
        self,
        key: Any,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
    ) -> _RustRawPipeline:
        args: list[Any] = [key]
        if maxlen is not None:
            args.append(b"MAXLEN")
            args.append(b"~" if approximate else b"=")
            args.append(maxlen)
        elif minid is not None:
            args.append(b"MINID")
            args.append(b"~" if approximate else b"=")
            args.append(minid)
        if limit is not None:
            args.extend([b"LIMIT", limit])
        return self._queue("XTRIM", *args)

    def xdel(self, key: Any, *entry_ids: str) -> _RustRawPipeline:
        return self._queue("XDEL", key, *entry_ids)

    def xinfo_stream(self, key: Any, full: bool = False) -> _RustRawPipeline:
        if full:
            return self._queue("XINFO", b"STREAM", key, b"FULL", parser=_xinfo_dict)
        return self._queue("XINFO", b"STREAM", key, parser=_xinfo_dict)

    def xinfo_groups(self, key: Any) -> _RustRawPipeline:
        return self._queue("XINFO", b"GROUPS", key, parser=_xinfo_dict_list)

    def xinfo_consumers(self, key: Any, group: str) -> _RustRawPipeline:
        return self._queue("XINFO", b"CONSUMERS", key, group, parser=_xinfo_dict_list)

    def xgroup_create(
        self,
        key: Any,
        group: str,
        id: str = "$",
        *,
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> _RustRawPipeline:
        args: list[Any] = [b"CREATE", key, group, id]
        if mkstream:
            args.append(b"MKSTREAM")
        if entries_read is not None:
            args.extend([b"ENTRIESREAD", entries_read])
        return self._queue("XGROUP", *args)

    def xgroup_destroy(self, key: Any, group: str) -> _RustRawPipeline:
        return self._queue("XGROUP", b"DESTROY", key, group)

    def xgroup_setid(
        self,
        key: Any,
        group: str,
        id: str,
        *,
        entries_read: int | None = None,
    ) -> _RustRawPipeline:
        args: list[Any] = [b"SETID", key, group, id]
        if entries_read is not None:
            args.extend([b"ENTRIESREAD", entries_read])
        return self._queue("XGROUP", *args)

    def xgroup_delconsumer(self, key: Any, group: str, consumer: str) -> _RustRawPipeline:
        return self._queue("XGROUP", b"DELCONSUMER", key, group, consumer)

    def xack(self, key: Any, group: str, *ids: str) -> _RustRawPipeline:
        return self._queue("XACK", key, group, *ids)

    def xpending(self, key: Any, group: str) -> _RustRawPipeline:
        """Summary form: ``XPENDING key group``."""
        return self._queue("XPENDING", key, group, parser=_xpending_summary)

    def xpending_range(
        self,
        key: Any,
        group: str,
        *,
        min: str,
        max: str,
        count: int,
        consumername: str | None = None,
        idle: int | None = None,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, group]
        if idle is not None:
            args.extend([b"IDLE", idle])
        args.extend([min, max, count])
        if consumername is not None:
            args.append(consumername)
        return self._queue("XPENDING", *args, parser=_xpending_range)

    def xclaim(
        self,
        key: Any,
        group: str,
        consumer: str,
        min_idle_time: int,
        message_ids: list[str],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, group, consumer, min_idle_time, *message_ids]
        if idle is not None:
            args.extend([b"IDLE", idle])
        if time is not None:
            args.extend([b"TIME", time])
        if retrycount is not None:
            args.extend([b"RETRYCOUNT", retrycount])
        if force:
            args.append(b"FORCE")
        if justid:
            args.append(b"JUSTID")
            return self._queue("XCLAIM", *args)
        return self._queue("XCLAIM", *args, parser=_stream_entries)

    def xautoclaim(
        self,
        key: Any,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> _RustRawPipeline:
        args: list[Any] = [key, group, consumer, min_idle_time, start_id]
        if count is not None:
            args.extend([b"COUNT", count])
        if justid:
            args.append(b"JUSTID")
            return self._queue("XAUTOCLAIM", *args)
        return self._queue("XAUTOCLAIM", *args, parser=_xautoclaim)


__all__ = ["_RustRawPipeline"]
