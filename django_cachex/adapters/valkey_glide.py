# ruff: noqa: ERA001, PERF401, PLW2901
"""Design B: ``valkey-glide``-backed cache client (sync + async).

Each operation method overrides ``RespAdapterProtocol`` and calls
``glide_sync.GlideClient`` / ``glide.GlideClient`` natively. There is
no redis-py-shaped intermediary for the operation surface — only
``encode``/``decode``/``_resolve_stampede`` are shared from the base.

The pipeline adapter (``ValkeyGlidePipelineAdapter`` / ``ValkeyGlideAsyncPipelineAdapter``)
implements ``RespPipelineProtocol`` natively against glide's ``Batch``
— no redis-py-shaped intermediary on the queueing surface either.

Standalone only; no cluster, no sentinel. Spike-quality — many
ruff rules suppressed at the file level since this is intentionally
rough until we decide whether to keep this approach.
"""

import asyncio
import os
import time
from typing import TYPE_CHECKING, Any, Self
from urllib.parse import urlparse

from django_cachex.adapters.protocols import RespAdapterProtocol, RespPipelineProtocol
from django_cachex.stampede import (
    StampedeConfig,
    get_timeout_with_buffer,
    make_stampede_config,
    resolve_stampede,
    should_recompute,
)
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence

    from django_cachex.types import KeyT


# valkey-glide is an optional install. The names below are unbound when it
# isn't available; method bodies that reference them only run at runtime
# after ``_check_installed`` has gated construction, so a missing install
# raises at backend instantiation time with an actionable message rather
# than at module import time. Mirrors the pattern in ``_redis_rs_clients``.
try:
    from glide import GlideClient as AsyncGlideClient  # ty: ignore[unresolved-import]
    from glide import GlideClientConfiguration as AsyncGlideClientConfiguration  # ty: ignore[unresolved-import]
    from glide import NodeAddress as AsyncNodeAddress  # ty: ignore[unresolved-import]
    from glide_sync import (  # ty: ignore[unresolved-import]
        Batch,
        ConditionalChange,
        ExpirySet,
        ExpiryType,
        FlushMode,
        GlideClientConfiguration,
        NodeAddress,
    )
    from glide_sync.glide_client import GlideClient  # ty: ignore[unresolved-import]
except ImportError as _exc:
    _GLIDE_IMPORT_ERROR: ImportError | None = _exc
else:
    _GLIDE_IMPORT_ERROR = None


def _check_installed() -> None:
    if _GLIDE_IMPORT_ERROR is not None:
        msg = (
            "valkey-glide is not installed. Install with the `valkey-glide` "
            "extra: pip install django-cachex[valkey-glide]. This pulls in "
            "both `valkey-glide-sync` (sync API) and `valkey-glide` (async API)."
        )
        raise ImportError(msg) from _GLIDE_IMPORT_ERROR


# Alias for the `set` builtin shadowed by the `set` method (PEP 649 defers
# annotations at runtime, but type checkers still resolve them in class scope).
_set = set


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
    return v


def _enc_list(values: Iterable[Any]) -> list[bytes | str]:
    return [_enc(v) for v in values]


def _enc_map(mapping: Mapping[Any, Any]) -> dict[Any, bytes | str]:
    return {k: _enc(v) for k, v in mapping.items()}


def _expiry(timeout: int | None) -> ExpirySet | None:
    if timeout is None:
        return None
    return ExpirySet(ExpiryType.SEC, timeout)


def _set_kw(*, ex: int | None = None, nx: bool = False, xx: bool = False, get: bool = False) -> dict[str, Any]:
    kw: dict[str, Any] = {}
    if ex is not None:
        kw["expiry"] = ExpirySet(ExpiryType.SEC, ex)
    if nx:
        kw["conditional_set"] = ConditionalChange.ONLY_IF_DOES_NOT_EXIST
    elif xx:
        kw["conditional_set"] = ConditionalChange.ONLY_IF_EXISTS
    if get:
        kw["return_old_value"] = True
    return kw


def _normalize_ttl(result: int) -> int | None:
    """Normalize TTL/PTTL/EXPIRETIME results: -1 (no expiry) -> None."""
    if result == -1:
        return None
    return result


# =============================================================================
# Pipeline wrappers (redis-py-shaped) — required by django_cachex.adapters.pipeline.Pipeline
# =============================================================================


class ValkeyGlidePipelineAdapter(RespPipelineProtocol):
    """Pipeline adapter that buffers cachex ops into glide's ``Batch``."""

    def __init__(self, client: GlideClient, *, transaction: bool = False) -> None:
        self._client = client
        self._batch = Batch(is_atomic=transaction)

    # ---- strings ----
    def set(self, key: Any, value: Any, **kw: Any) -> Self:
        self._batch.set(key, _enc(value), **_set_kw(**kw))
        return self

    def get(self, key: Any) -> Self:
        self._batch.get(key)
        return self

    def delete(self, *keys: Any) -> Self:
        self._batch.delete(_enc_list(keys))
        return self

    def mget(self, keys: Iterable[Any]) -> Self:
        self._batch.mget(_enc_list(keys))
        return self

    def mset(self, mapping: Mapping[Any, Any]) -> Self:
        self._batch.mset(_enc_map(mapping))
        return self

    def incrby(self, key: Any, amount: int) -> Self:
        self._batch.incrby(key, amount)
        return self

    def decrby(self, key: Any, amount: int) -> Self:
        self._batch.decrby(key, amount)
        return self

    # ---- generic / keys ----
    def exists(self, *keys: Any) -> Self:
        self._batch.exists(_enc_list(keys))
        return self

    def expire(self, key: Any, seconds: int) -> Self:
        self._batch.expire(key, seconds)
        return self

    def pexpire(self, key: Any, ms: int) -> Self:
        self._batch.pexpire(key, ms)
        return self

    def expireat(self, key: Any, when: int) -> Self:
        self._batch.expireat(key, when)
        return self

    def pexpireat(self, key: Any, when: int) -> Self:
        self._batch.pexpireat(key, when)
        return self

    def expiretime(self, key: Any) -> Self:
        self._batch.expiretime(key)
        return self

    def ttl(self, key: Any) -> Self:
        self._batch.ttl(key)
        return self

    def pttl(self, key: Any) -> Self:
        self._batch.pttl(key)
        return self

    def persist(self, key: Any) -> Self:
        self._batch.persist(key)
        return self

    def type(self, key: Any) -> Self:
        self._batch.type(key)
        return self

    def rename(self, src: Any, dst: Any) -> Self:
        self._batch.rename(src, dst)
        return self

    def renamenx(self, src: Any, dst: Any) -> Self:
        self._batch.renamenx(src, dst)
        return self

    # ---- hashes ----
    def hset(
        self,
        key: Any,
        field: Any = None,
        value: Any = None,
        mapping: Mapping[Any, Any] | None = None,
        items: list[Any] | None = None,
    ) -> Self:
        m: dict[Any, Any] = {}
        if field is not None:
            m[field] = _enc(value)
        if mapping:
            m.update(_enc_map(mapping))
        if items:
            for i in range(0, len(items), 2):
                m[items[i]] = _enc(items[i + 1])
        if m:
            self._batch.hset(key, m)
        return self

    def hsetnx(self, key: Any, field: Any, value: Any) -> Self:
        self._batch.hsetnx(key, field, _enc(value))
        return self

    def hget(self, key: Any, field: Any) -> Self:
        self._batch.hget(key, field)
        return self

    def hmget(self, key: Any, *fields: Any) -> Self:
        if len(fields) == 1 and isinstance(fields[0], (list, tuple)):
            fields = tuple(fields[0])
        self._batch.hmget(key, list(fields))
        return self

    def hgetall(self, key: Any) -> Self:
        self._batch.hgetall(key)
        return self

    def hkeys(self, key: Any) -> Self:
        self._batch.hkeys(key)
        return self

    def hvals(self, key: Any) -> Self:
        self._batch.hvals(key)
        return self

    def hlen(self, key: Any) -> Self:
        self._batch.hlen(key)
        return self

    def hexists(self, key: Any, field: Any) -> Self:
        self._batch.hexists(key, field)
        return self

    def hdel(self, key: Any, *fields: Any) -> Self:
        self._batch.hdel(key, list(fields))
        return self

    def hincrby(self, key: Any, field: Any, amount: int = 1) -> Self:
        self._batch.hincrby(key, field, amount)
        return self

    def hincrbyfloat(self, key: Any, field: Any, amount: float) -> Self:
        self._batch.hincrbyfloat(key, field, amount)
        return self

    # ---- sets ----
    def sadd(self, key: Any, *members: Any) -> Self:
        self._batch.sadd(key, _enc_list(members))
        return self

    def srem(self, key: Any, *members: Any) -> Self:
        self._batch.srem(key, _enc_list(members))
        return self

    def smembers(self, key: Any) -> Self:
        self._batch.smembers(key)
        return self

    def sismember(self, key: Any, member: Any) -> Self:
        self._batch.sismember(key, _enc(member))
        return self

    def smismember(self, key: Any, *members: Any) -> Self:
        self._batch.smismember(key, _enc_list(members))
        return self

    def scard(self, key: Any) -> Self:
        self._batch.scard(key)
        return self

    def spop(self, key: Any, count: int | None = None) -> Self:
        if count is None:
            self._batch.spop(key)
        else:
            self._batch.spop_count(key, count)
        return self

    def srandmember(self, key: Any, count: int | None = None) -> Self:
        if count is None:
            self._batch.srandmember(key)
        else:
            self._batch.srandmember_count(key, count)
        return self

    def smove(self, src: Any, dst: Any, member: Any) -> Self:
        self._batch.smove(src, dst, _enc(member))
        return self

    def sinter(self, *keys: Any) -> Self:
        self._batch.sinter(_enc_list(keys))
        return self

    def sunion(self, *keys: Any) -> Self:
        self._batch.sunion(_enc_list(keys))
        return self

    def sdiff(self, *keys: Any) -> Self:
        self._batch.sdiff(_enc_list(keys))
        return self

    def sinterstore(self, dst: Any, *keys: Any) -> Self:
        self._batch.sinterstore(dst, _enc_list(keys))
        return self

    def sunionstore(self, dst: Any, *keys: Any) -> Self:
        self._batch.sunionstore(dst, _enc_list(keys))
        return self

    def sdiffstore(self, dst: Any, *keys: Any) -> Self:
        self._batch.sdiffstore(dst, _enc_list(keys))
        return self

    # ---- sorted sets ----
    def zadd(self, key: Any, mapping: Mapping[Any, float], **kwargs: Any) -> Self:
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
            self._batch.custom_command(args)
        else:
            self._batch.zadd(key, {_enc(m): float(s) for m, s in mapping.items()})
        return self

    def zrem(self, key: Any, *members: Any) -> Self:
        self._batch.zrem(key, _enc_list(members))
        return self

    def zscore(self, key: Any, member: Any) -> Self:
        self._batch.zscore(key, _enc(member))
        return self

    def zmscore(self, key: Any, members: list[Any]) -> Self:
        self._batch.zmscore(key, _enc_list(members))
        return self

    def zrank(self, key: Any, member: Any) -> Self:
        self._batch.zrank(key, _enc(member))
        return self

    def zrevrank(self, key: Any, member: Any) -> Self:
        self._batch.zrevrank(key, _enc(member))
        return self

    def zincrby(self, key: Any, amount: float, member: Any) -> Self:
        self._batch.zincrby(key, amount, _enc(member))
        return self

    def zremrangebyrank(self, key: Any, start: int, end: int) -> Self:
        self._batch.zremrangebyrank(key, start, end)
        return self

    def zremrangebyscore(self, key: Any, mn: Any, mx: Any) -> Self:
        self._batch.custom_command([b"ZREMRANGEBYSCORE", key, _enc(mn), _enc(mx)])
        return self

    def zcard(self, key: Any) -> Self:
        self._batch.zcard(key)
        return self

    def zcount(self, key: Any, mn: Any, mx: Any) -> Self:
        self._batch.zcount(key, _enc(mn), _enc(mx))
        return self

    def zrange(self, key: Any, start: int, end: int, withscores: bool = False, desc: bool = False) -> Self:
        args = [b"ZRANGE", key, str(start).encode(), str(end).encode()]
        if desc:
            args.append(b"REV")
        if withscores:
            args.append(b"WITHSCORES")
        self._batch.custom_command(args)
        return self

    def zrevrange(self, key: Any, start: int, end: int, withscores: bool = False) -> Self:
        return self.zrange(key, start, end, withscores=withscores, desc=True)

    def zrangebyscore(self, key: Any, mn: Any, mx: Any, withscores: bool = False) -> Self:
        args = [b"ZRANGEBYSCORE", key, _enc(mn), _enc(mx)]
        if withscores:
            args.append(b"WITHSCORES")
        self._batch.custom_command(args)
        return self

    def zrevrangebyscore(self, key: Any, mx: Any, mn: Any, withscores: bool = False) -> Self:
        args = [b"ZREVRANGEBYSCORE", key, _enc(mx), _enc(mn)]
        if withscores:
            args.append(b"WITHSCORES")
        self._batch.custom_command(args)
        return self

    def zpopmin(self, key: Any, count: int = 1) -> Self:
        self._batch.zpopmin(key, count)
        return self

    def zpopmax(self, key: Any, count: int = 1) -> Self:
        self._batch.zpopmax(key, count)
        return self

    # ---- lists ----
    def lpush(self, key: Any, *values: Any) -> Self:
        self._batch.lpush(key, _enc_list(values))
        return self

    def rpush(self, key: Any, *values: Any) -> Self:
        self._batch.rpush(key, _enc_list(values))
        return self

    def lpop(self, key: Any, count: int | None = None) -> Self:
        if count is None:
            self._batch.lpop(key)
        else:
            self._batch.lpop_count(key, count)
        return self

    def rpop(self, key: Any, count: int | None = None) -> Self:
        if count is None:
            self._batch.rpop(key)
        else:
            self._batch.rpop_count(key, count)
        return self

    def lrange(self, key: Any, start: int, end: int) -> Self:
        self._batch.lrange(key, start, end)
        return self

    def ltrim(self, key: Any, start: int, end: int) -> Self:
        self._batch.ltrim(key, start, end)
        return self

    def llen(self, key: Any) -> Self:
        self._batch.llen(key)
        return self

    def lindex(self, key: Any, index: int) -> Self:
        self._batch.lindex(key, index)
        return self

    def lset(self, key: Any, index: int, value: Any) -> Self:
        self._batch.lset(key, index, _enc(value))
        return self

    def lrem(self, key: Any, count: int, value: Any) -> Self:
        self._batch.lrem(key, count, _enc(value))
        return self

    def linsert(self, key: Any, where: str, pivot: Any, value: Any) -> Self:
        self._batch.custom_command([b"LINSERT", key, _enc(where.upper()), _enc(pivot), _enc(value)])
        return self

    def lpos(self, key: Any, element: Any, **kwargs: Any) -> Self:
        args: list[Any] = [b"LPOS", key, _enc(element)]
        if "rank" in kwargs:
            args.extend([b"RANK", str(kwargs["rank"]).encode()])
        if "count" in kwargs:
            args.extend([b"COUNT", str(kwargs["count"]).encode()])
        if "maxlen" in kwargs:
            args.extend([b"MAXLEN", str(kwargs["maxlen"]).encode()])
        self._batch.custom_command(args)
        return self

    def lmove(self, src: Any, dst: Any, src_dir: str, dst_dir: str) -> Self:
        self._batch.custom_command([b"LMOVE", src, dst, _enc(src_dir.upper()), _enc(dst_dir.upper())])
        return self

    # ---- streams (via custom_command for everything that's not single-response) ----
    def xadd(self, key: Any, fields: Mapping[Any, Any], id: str = "*", **kwargs: Any) -> Self:
        args = [b"XADD", key, _enc(id)]
        for f, v in fields.items():
            args.extend([_enc(f), _enc(v)])
        self._batch.custom_command(args)
        return self

    def xlen(self, key: Any) -> Self:
        self._batch.xlen(key)
        return self

    def xrange(self, key: Any, mn: str = "-", mx: str = "+", count: int | None = None) -> Self:
        args = [b"XRANGE", key, _enc(mn), _enc(mx)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        self._batch.custom_command(args)
        return self

    def xrevrange(self, key: Any, mx: str = "+", mn: str = "-", count: int | None = None) -> Self:
        args = [b"XREVRANGE", key, _enc(mx), _enc(mn)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        self._batch.custom_command(args)
        return self

    def xdel(self, key: Any, *ids: Any) -> Self:
        self._batch.custom_command([b"XDEL", key, *_enc_list(ids)])
        return self

    def xtrim(self, key: Any, **kwargs: Any) -> Self:
        args: list[Any] = [b"XTRIM", key]
        if "maxlen" in kwargs:
            args.extend([b"MAXLEN", str(kwargs["maxlen"]).encode()])
        elif "minid" in kwargs:
            args.extend([b"MINID", _enc(kwargs["minid"])])
        self._batch.custom_command(args)
        return self

    # ---- raw ----
    def execute_command(self, *args: Any) -> Self:
        self._batch.custom_command(_enc_list(args))
        return self

    def __getattr__(self, name: str) -> Any:
        cmd = name.upper()

        def call(*args: Any) -> Self:
            self._batch.custom_command([cmd, *_enc_list(args)])
            return self

        return call

    # ---- execution ----
    def execute(self) -> list[Any]:
        return self._client.exec(self._batch, raise_on_error=True) or []

    def reset(self) -> None:
        self._batch = Batch(is_atomic=False)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class ValkeyGlideAsyncPipelineAdapter:
    """Async parallel of ``ValkeyGlidePipelineAdapter``.

    Conforms to :class:`RespAsyncPipelineProtocol`. Accepts a callable that
    returns the awaitable async client (rather than a resolved client) so the
    factory can stay sync — the underlying glide client is acquired lazily
    inside ``execute()``.
    """

    def __init__(
        self,
        client_factory: Callable[[], Awaitable[AsyncGlideClient]],
        *,
        transaction: bool = False,
    ) -> None:
        self._client_factory = client_factory
        self._batch = Batch(is_atomic=transaction)

    def set(self, key: Any, value: Any, **kw: Any) -> Self:
        self._batch.set(key, _enc(value), **_set_kw(**kw))
        return self

    def get(self, key: Any) -> Self:
        self._batch.get(key)
        return self

    def delete(self, *keys: Any) -> Self:
        self._batch.delete(_enc_list(keys))
        return self

    def mget(self, keys: Iterable[Any]) -> Self:
        self._batch.mget(_enc_list(keys))
        return self

    def mset(self, mapping: Mapping[Any, Any]) -> Self:
        self._batch.mset(_enc_map(mapping))
        return self

    def incrby(self, key: Any, amount: int) -> Self:
        self._batch.incrby(key, amount)
        return self

    def expire(self, key: Any, seconds: int) -> Self:
        self._batch.expire(key, seconds)
        return self

    def ttl(self, key: Any) -> Self:
        self._batch.ttl(key)
        return self

    def exists(self, *keys: Any) -> Self:
        self._batch.exists(_enc_list(keys))
        return self

    def __getattr__(self, name: str) -> Any:
        cmd = name.upper()

        def call(*args: Any) -> Self:
            self._batch.custom_command([cmd, *_enc_list(args)])
            return self

        return call

    async def execute(self) -> list[Any]:
        client = await self._client_factory()
        result = await client.exec(self._batch, raise_on_error=True)
        return result or []

    async def reset(self) -> None:
        self._batch = Batch(is_atomic=False)

    def execute_command(self, *args: Any) -> Self:
        if not args:
            msg = "execute_command requires at least the command name"
            raise ValueError(msg)
        cmd_name = args[0] if isinstance(args[0], str) else args[0].decode()
        self._batch.custom_command([cmd_name, *_enc_list(args[1:])])
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


# =============================================================================
# Cache client
# =============================================================================


class ValkeyGlideAdapter(RespAdapterProtocol):
    """Design B backend: each operation method calls glide natively.

    Implements the cachex adapter surface against ``valkey-glide-sync``.
    Everything that touches the wire is overridden directly; the redis-py
    pool/parser machinery in :class:`~django_cachex.adapters.valkey_py.ValkeyPyAdapter`
    isn't used here.
    """

    def __init__(self, servers: list[str], **options: Any) -> None:
        _check_installed()
        self._servers = servers
        self._options = options
        self._stampede_config: StampedeConfig | None = make_stampede_config(options.get("stampede_prevention"))
        self._sync_glide_client: GlideClient | None = None
        self._async_glide_clients: dict[int, AsyncGlideClient] = {}

    def _resolve_stampede(self, stampede_prevention: bool | dict | None = None) -> StampedeConfig | None:
        return resolve_stampede(self._stampede_config, stampede_prevention)

    def _get_timeout_with_buffer(
        self,
        timeout: int | None,
        stampede_prevention: bool | dict | None = None,
    ) -> int | None:
        return get_timeout_with_buffer(timeout, self._stampede_config, stampede_prevention)

    # ---- client lifecycle ----
    def _client(self) -> GlideClient:
        if self._sync_glide_client is None:
            u = urlparse(self._servers[0])
            cfg = GlideClientConfiguration(
                addresses=[NodeAddress(u.hostname or "localhost", u.port or 6379)],
            )
            self._sync_glide_client = GlideClient.create(cfg)
        return self._sync_glide_client

    async def _aclient(self) -> AsyncGlideClient:
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
        client = self._async_glide_clients.get(loop_id)
        if client is None:
            u = urlparse(self._servers[0])
            cfg = AsyncGlideClientConfiguration(
                addresses=[AsyncNodeAddress(u.hostname or "localhost", u.port or 6379)],
            )
            client = await AsyncGlideClient.create(cfg)
            self._async_glide_clients[loop_id] = client
        return client

    def get_client(self, key: Any = None, *, write: bool = False) -> GlideClient:
        return self._client()

    def get_async_client(self, key: Any = None, *, write: bool = False) -> AsyncGlideClient:
        # Awaiting must happen at call sites; this method is sync-only by
        # RespAdapterProtocol's contract. Only here for completeness; the
        # aXXX methods below do not call this.
        msg = "Use the a* methods on this client; get_async_client is not supported"
        raise NotImplementedError(msg)

    # =========================================================================
    # Core ops (sync)
    # =========================================================================

    def add(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool:
        client = self._client()
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            result = client.set(
                key,
                _enc(nvalue),
                conditional_set=ConditionalChange.ONLY_IF_DOES_NOT_EXIST,
            )
            if result == "OK":
                client.delete([key])
                return True
            return False

        kw: dict[str, Any] = {"conditional_set": ConditionalChange.ONLY_IF_DOES_NOT_EXIST}
        if actual_timeout is not None:
            kw["expiry"] = ExpirySet(ExpiryType.SEC, actual_timeout)
        return client.set(key, _enc(nvalue), **kw) == "OK"

    def get(self, key: KeyT, *, stampede_prevention: bool | dict | None = None) -> Any:
        client = self._client()
        val = client.get(key)
        if val is None:
            return None
        config = self._resolve_stampede(stampede_prevention)
        if config and isinstance(val, bytes):
            ttl = client.ttl(key)
            if ttl > 0 and should_recompute(ttl, config):
                return None
        return val

    def set(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> None:
        client = self._client()
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            client.delete([key])
        elif actual_timeout is None:
            client.set(key, _enc(nvalue))
        else:
            client.set(key, _enc(nvalue), expiry=ExpirySet(ExpiryType.SEC, actual_timeout))

    def set_with_flags(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        stampede_prevention: bool | dict | None = None,
    ) -> bool | Any:
        client = self._client()
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            return None if get else False

        kw: dict[str, Any] = {}
        if actual_timeout is not None:
            kw["expiry"] = ExpirySet(ExpiryType.SEC, actual_timeout)
        if nx:
            kw["conditional_set"] = ConditionalChange.ONLY_IF_DOES_NOT_EXIST
        elif xx:
            kw["conditional_set"] = ConditionalChange.ONLY_IF_EXISTS
        if get:
            kw["return_old_value"] = True

        result = client.set(key, _enc(nvalue), **kw)
        if get:
            return None if result is None else result
        return result == "OK"

    def touch(self, key: KeyT, timeout: int | None) -> bool:
        client = self._client()
        if timeout is None:
            return bool(client.persist(key))
        return bool(client.expire(key, timeout))

    def delete(self, key: KeyT) -> bool:
        return bool(self._client().delete([key]))

    def get_many(self, keys: Iterable[KeyT], *, stampede_prevention: bool | dict | None = None) -> dict[KeyT, Any]:
        keys = list(keys)
        if not keys:
            return {}

        client = self._client()
        results = client.mget(keys)
        found = {k: v for k, v in zip(keys, results, strict=False) if v is not None}

        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            stampede_keys = [k for k, v in found.items() if isinstance(v, bytes)]
            if stampede_keys:
                pipe = self._pipeline()
                for k in stampede_keys:
                    pipe.ttl(k)
                ttls = pipe.execute()
                for k, ttl in zip(stampede_keys, ttls, strict=False):
                    if isinstance(ttl, int) and ttl > 0 and should_recompute(ttl, config):
                        del found[k]

        return dict(found.items())

    def has_key(self, key: KeyT) -> bool:
        return bool(self._client().exists([key]))

    def type(self, key: KeyT) -> KeyType | None:
        result = self._client().type(key)
        if isinstance(result, bytes):
            result = result.decode("utf-8")
        return None if result == "none" else KeyType(result)

    def incr(self, key: KeyT, delta: int = 1) -> int:
        client = self._client()
        if delta == 1:
            return client.incr(key)
        return client.incrby(key, delta)

    def set_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        if not data:
            return []
        client = self._client()
        prepared = {k: _enc(v) for k, v in data.items()}
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            client.delete(list(prepared.keys()))
        elif actual_timeout is None:
            client.mset(prepared)
        else:
            batch = Batch(is_atomic=False)
            batch.mset(prepared)
            for key in prepared:
                batch.expire(key, actual_timeout)
            client.exec(batch, raise_on_error=True)
        return []

    def delete_many(self, keys: Sequence[KeyT]) -> int:
        if not keys:
            return 0
        return self._client().delete(list(keys))

    def clear(self) -> bool:
        return self._client().flushdb(FlushMode.SYNC) == "OK"

    def close(self, **kwargs: Any) -> None:
        if self._sync_glide_client is not None:
            self._sync_glide_client.close()
            self._sync_glide_client = None

    # ---- TTL ----
    def ttl(self, key: KeyT) -> int | None:
        return _normalize_ttl(self._client().ttl(key))

    def pttl(self, key: KeyT) -> int | None:
        return _normalize_ttl(self._client().pttl(key))

    def persist(self, key: KeyT) -> bool:
        return bool(self._client().persist(key))

    def expire(self, key: KeyT, timeout: int) -> bool:
        return bool(self._client().expire(key, timeout))

    def pexpire(self, key: KeyT, timeout: int) -> bool:
        return bool(self._client().pexpire(key, timeout))

    def expireat(self, key: KeyT, when: int) -> bool:
        return bool(self._client().expireat(key, when))

    def pexpireat(self, key: KeyT, when: int) -> bool:
        return bool(self._client().pexpireat(key, when))

    def expiretime(self, key: KeyT) -> int | None:
        return _normalize_ttl(self._client().expiretime(key))

    def rename(self, src: KeyT, dst: KeyT) -> bool:
        return self._client().rename(src, dst) == "OK"

    def renamenx(self, src: KeyT, dst: KeyT) -> bool:
        return bool(self._client().renamenx(src, dst))

    # ---- scan / keys ----
    def keys(self, pattern: str = "*") -> list[bytes]:
        result = self._client().custom_command([b"KEYS", _enc(pattern)])
        return list(result) if result else []

    def scan(self, cursor: int = 0, match: str | None = None, count: int | None = None) -> tuple[bytes, list[bytes]]:
        result = self._client().scan(_enc(cursor), match=match, count=count)
        return result[0], list(result[1])

    def iter_keys(self, pattern: str, itersize: int | None = None) -> Iterable[bytes]:
        client = self._client()
        cursor: Any = b"0"
        while True:
            result = client.scan(cursor, match=pattern, count=itersize)
            cursor, keys = result[0], result[1]
            yield from keys
            if cursor in (b"0", "0", 0):
                return

    def delete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        client = self._client()
        deleted = 0
        for batch_keys in _batched(self.iter_keys(pattern, itersize=itersize), itersize or 100):
            if batch_keys:
                deleted += client.delete(batch_keys)
        return deleted

    # =========================================================================
    # Hashes (sync)
    # =========================================================================

    def hset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        client = self._client()
        m: dict[Any, Any] = {}
        if field is not None:
            m[field] = _enc(value)
        if mapping:
            m.update({f: _enc(v) for f, v in mapping.items()})
        if items:
            for i in range(0, len(items), 2):
                m[items[i]] = _enc(items[i + 1])
        if not m:
            return 0
        return client.hset(key, m)

    def hsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        return self._client().hsetnx(key, field, _enc(value))

    def hget(self, key: KeyT, field: str) -> Any | None:
        val = self._client().hget(key, field)
        return None if val is None else val

    def hmget(self, key: KeyT, *fields: str) -> list[Any]:
        if len(fields) == 1 and isinstance(fields[0], (list, tuple)):
            fields = tuple(fields[0])
        values = self._client().hmget(key, list(fields))
        return [v if v is not None else None for v in values]

    def hgetall(self, key: KeyT) -> dict[str, Any]:
        result = self._client().hgetall(key)
        return {k.decode() if isinstance(k, bytes) else k: v for k, v in result.items()}

    def hkeys(self, key: KeyT) -> list[str]:
        return [k.decode() if isinstance(k, bytes) else k for k in self._client().hkeys(key)]

    def hvals(self, key: KeyT) -> list[Any]:
        return list(self._client().hvals(key))

    def hlen(self, key: KeyT) -> int:
        return self._client().hlen(key)

    def hexists(self, key: KeyT, field: str) -> bool:
        return bool(self._client().hexists(key, field))

    def hdel(self, key: KeyT, *fields: str) -> int:
        return self._client().hdel(key, list(fields))

    def hincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        return self._client().hincrby(key, field, amount)

    def hincrbyfloat(self, key: KeyT, field: str, amount: float) -> float:
        return self._client().hincrbyfloat(key, field, amount)

    # =========================================================================
    # Sets (sync)
    # =========================================================================

    def sadd(self, key: KeyT, *members: Any) -> int:
        return self._client().sadd(key, [_enc(m) for m in members])

    def srem(self, key: KeyT, *members: Any) -> int:
        return self._client().srem(key, [_enc(m) for m in members])

    def smembers(self, key: KeyT) -> _set[Any]:
        return set(self._client().smembers(key))

    def sismember(self, key: KeyT, member: Any) -> bool:
        return bool(self._client().sismember(key, _enc(member)))

    def smismember(self, key: KeyT, *members: Any) -> list[bool]:
        return list(self._client().smismember(key, [_enc(m) for m in members]))

    def scard(self, key: KeyT) -> int:
        return self._client().scard(key)

    def spop(self, key: KeyT, count: int | None = None) -> Any:
        client = self._client()
        if count is None:
            v = client.spop(key)
            return None if v is None else v
        return set(client.spop_count(key, count))

    def srandmember(self, key: KeyT, count: int | None = None) -> Any:
        client = self._client()
        if count is None:
            v = client.srandmember(key)
            return None if v is None else v
        return list(client.srandmember_count(key, count))

    def smove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        return bool(self._client().smove(src, dst, _enc(member)))

    def sinter(self, *keys: KeyT) -> _set[Any]:
        return set(self._client().sinter(list(keys)))

    def sunion(self, *keys: KeyT) -> _set[Any]:
        return set(self._client().sunion(list(keys)))

    def sdiff(self, *keys: KeyT) -> _set[Any]:
        return set(self._client().sdiff(list(keys)))

    def sinterstore(self, dst: KeyT, *keys: KeyT) -> int:
        return self._client().sinterstore(dst, list(keys))

    def sunionstore(self, dst: KeyT, *keys: KeyT) -> int:
        return self._client().sunionstore(dst, list(keys))

    def sdiffstore(self, dst: KeyT, *keys: KeyT) -> int:
        return self._client().sdiffstore(dst, list(keys))

    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[bytes, list]:
        result = self._client().sscan(key, _enc(cursor), match=match, count=count)
        return result[0], list(result[1])

    def sscan_iter(self, key: KeyT, match: str | None = None, count: int | None = None) -> Iterable[Any]:
        client = self._client()
        cursor: Any = b"0"
        while True:
            result = client.sscan(key, cursor, match=match, count=count)
            cursor, members = result[0], result[1]
            yield from members
            if cursor in (b"0", "0", 0):
                return

    # =========================================================================
    # Sorted sets (sync)
    # =========================================================================

    def zadd(self, key: KeyT, mapping: Mapping[Any, float], **kwargs: Any) -> int:
        client = self._client()
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
            return client.custom_command(args)
        return client.zadd(key, {_enc(m): float(s) for m, s in mapping.items()})

    def zrem(self, key: KeyT, *members: Any) -> int:
        return self._client().zrem(key, [_enc(m) for m in members])

    def zscore(self, key: KeyT, member: Any) -> float | None:
        return self._client().zscore(key, _enc(member))

    def zmscore(self, key: KeyT, members: list[Any]) -> list[float | None]:
        return list(self._client().zmscore(key, [_enc(m) for m in members]))

    def zrank(self, key: KeyT, member: Any) -> int | None:
        return self._client().zrank(key, _enc(member))

    def zrevrank(self, key: KeyT, member: Any) -> int | None:
        return self._client().zrevrank(key, _enc(member))

    def zincrby(self, key: KeyT, amount: float, member: Any) -> float:
        return self._client().zincrby(key, amount, _enc(member))

    def zremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        return self._client().zremrangebyrank(key, start, end)

    def zremrangebyscore(self, key: KeyT, mn: Any, mx: Any) -> int:
        return self._client().custom_command([b"ZREMRANGEBYSCORE", key, _enc(mn), _enc(mx)])

    def zcard(self, key: KeyT) -> int:
        return self._client().zcard(key)

    def zcount(self, key: KeyT, mn: Any, mx: Any) -> int:
        return self._client().zcount(key, _enc(mn), _enc(mx))

    def zrange(self, key: KeyT, start: int, end: int, withscores: bool = False, desc: bool = False) -> list:
        args = [b"ZRANGE", key, str(start).encode(), str(end).encode()]
        if desc:
            args.append(b"REV")
        if withscores:
            args.append(b"WITHSCORES")
        result = self._client().custom_command(args)
        return _decode_zrange(result, _passthrough, withscores=withscores)

    def zrevrange(self, key: KeyT, start: int, end: int, withscores: bool = False) -> list:
        return self.zrange(key, start, end, withscores=withscores, desc=True)

    def zrangebyscore(self, key: KeyT, mn: Any, mx: Any, withscores: bool = False) -> list:
        args = [b"ZRANGEBYSCORE", key, _enc(mn), _enc(mx)]
        if withscores:
            args.append(b"WITHSCORES")
        return _decode_zrange(self._client().custom_command(args), _passthrough, withscores=withscores)

    def zrevrangebyscore(self, key: KeyT, mx: Any, mn: Any, withscores: bool = False) -> list:
        args = [b"ZREVRANGEBYSCORE", key, _enc(mx), _enc(mn)]
        if withscores:
            args.append(b"WITHSCORES")
        return _decode_zrange(self._client().custom_command(args), _passthrough, withscores=withscores)

    def zpopmin(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        return _decode_zpop(self._client().zpopmin(key, count), _passthrough)

    def zpopmax(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        return _decode_zpop(self._client().zpopmax(key, count), _passthrough)

    # =========================================================================
    # Lists (sync)
    # =========================================================================

    def lpush(self, key: KeyT, *values: Any) -> int:
        return self._client().lpush(key, [_enc(v) for v in values])

    def rpush(self, key: KeyT, *values: Any) -> int:
        return self._client().rpush(key, [_enc(v) for v in values])

    def lpop(self, key: KeyT, count: int | None = None) -> Any:
        client = self._client()
        if count is None:
            v = client.lpop(key)
            return None if v is None else v
        result = client.lpop_count(key, count)
        return list(result) if result else []

    def rpop(self, key: KeyT, count: int | None = None) -> Any:
        client = self._client()
        if count is None:
            v = client.rpop(key)
            return None if v is None else v
        result = client.rpop_count(key, count)
        return list(result) if result else []

    def lrange(self, key: KeyT, start: int, end: int) -> list:
        return list(self._client().lrange(key, start, end))

    def ltrim(self, key: KeyT, start: int, end: int) -> bool:
        return self._client().ltrim(key, start, end) == "OK"

    def llen(self, key: KeyT) -> int:
        return self._client().llen(key)

    def lindex(self, key: KeyT, index: int) -> Any:
        v = self._client().lindex(key, index)
        return None if v is None else v

    def lset(self, key: KeyT, index: int, value: Any) -> bool:
        return self._client().lset(key, index, _enc(value)) == "OK"

    def lrem(self, key: KeyT, count: int, value: Any) -> int:
        return self._client().lrem(key, count, _enc(value))

    def linsert(self, key: KeyT, where: str, pivot: Any, value: Any) -> int:
        return self._client().custom_command(
            [
                b"LINSERT",
                key,
                _enc(where.upper()),
                _enc(pivot),
                _enc(value),
            ],
        )

    def lpos(self, key: KeyT, element: Any, **kwargs: Any) -> Any:
        args: list[Any] = [b"LPOS", key, _enc(element)]
        if "rank" in kwargs:
            args.extend([b"RANK", str(kwargs["rank"]).encode()])
        if "count" in kwargs:
            args.extend([b"COUNT", str(kwargs["count"]).encode()])
        if "maxlen" in kwargs:
            args.extend([b"MAXLEN", str(kwargs["maxlen"]).encode()])
        return self._client().custom_command(args)

    def lmove(self, src: KeyT, dst: KeyT, src_dir: str, dst_dir: str) -> Any:
        v = self._client().custom_command([b"LMOVE", src, dst, _enc(src_dir.upper()), _enc(dst_dir.upper())])
        return None if v is None else v

    def blmove(self, src: KeyT, dst: KeyT, src_dir: str, dst_dir: str, timeout: float) -> Any:
        v = self._client().custom_command(
            [
                b"BLMOVE",
                src,
                dst,
                _enc(src_dir.upper()),
                _enc(dst_dir.upper()),
                str(timeout).encode(),
            ],
        )
        return None if v is None else v

    def blpop(self, keys: Any, timeout: float = 0) -> Any:
        ks = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        return self._client().custom_command([b"BLPOP", *_enc_list(ks), str(timeout).encode()])

    def brpop(self, keys: Any, timeout: float = 0) -> Any:
        ks = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        return self._client().custom_command([b"BRPOP", *_enc_list(ks), str(timeout).encode()])

    # =========================================================================
    # Streams (sync) — via custom_command
    # =========================================================================

    def xadd(self, key: KeyT, fields: Mapping[Any, Any], id: str = "*", **kwargs: Any) -> str:
        args = [b"XADD", key, _enc(id)]
        for f, v in fields.items():
            args.extend([_enc(f), _enc(v)])
        result = self._client().custom_command(args)
        return result.decode() if isinstance(result, bytes) else result

    def xlen(self, key: KeyT) -> int:
        return self._client().xlen(key)

    def xrange(self, key: KeyT, mn: str = "-", mx: str = "+", count: int | None = None) -> Any:
        args = [b"XRANGE", key, _enc(mn), _enc(mx)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        return self._client().custom_command(args)

    def xrevrange(self, key: KeyT, mx: str = "+", mn: str = "-", count: int | None = None) -> Any:
        args = [b"XREVRANGE", key, _enc(mx), _enc(mn)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        return self._client().custom_command(args)

    def xdel(self, key: KeyT, *ids: Any) -> int:
        return self._client().custom_command([b"XDEL", key, *_enc_list(ids)])

    def xtrim(self, key: KeyT, **kwargs: Any) -> int:
        args: list[Any] = [b"XTRIM", key]
        if "maxlen" in kwargs:
            args.extend([b"MAXLEN", str(kwargs["maxlen"]).encode()])
        elif "minid" in kwargs:
            args.extend([b"MINID", _enc(kwargs["minid"])])
        return self._client().custom_command(args)

    def xack(self, key: KeyT, group: str, *ids: Any) -> int:
        return self._client().custom_command([b"XACK", key, _enc(group), *_enc_list(ids)])

    def xclaim(self, *args: Any, **kwargs: Any) -> Any:
        return self._client().custom_command([b"XCLAIM", *_enc_list(args)])

    def xautoclaim(self, *args: Any, **kwargs: Any) -> Any:
        return self._client().custom_command([b"XAUTOCLAIM", *_enc_list(args)])

    def xpending(self, key: KeyT, group: str) -> Any:
        return self._client().custom_command([b"XPENDING", key, _enc(group)])

    def xpending_range(self, *args: Any, **kwargs: Any) -> Any:
        return self._client().custom_command([b"XPENDING", *_enc_list(args)])

    def xinfo_stream(self, key: KeyT) -> Any:
        return self._client().custom_command([b"XINFO", b"STREAM", key])

    def xinfo_groups(self, key: KeyT) -> Any:
        return self._client().custom_command([b"XINFO", b"GROUPS", key])

    def xinfo_consumers(self, key: KeyT, group: str) -> Any:
        return self._client().custom_command([b"XINFO", b"CONSUMERS", key, _enc(group)])

    def xgroup_destroy(self, key: KeyT, group: str) -> int:
        return self._client().custom_command([b"XGROUP", b"DESTROY", key, _enc(group)])

    def xgroup_setid(self, key: KeyT, group: str, id: str) -> bool:
        return self._client().custom_command([b"XGROUP", b"SETID", key, _enc(group), _enc(id)]) == "OK"

    def xgroup_delconsumer(self, key: KeyT, group: str, consumer: str) -> int:
        return self._client().custom_command([b"XGROUP", b"DELCONSUMER", key, _enc(group), _enc(consumer)])

    def xread(self, *args: Any, **kwargs: Any) -> Any:
        # Note: glide.custom_command docs say this isn't safe for streaming reads.
        # Use only for non-blocking xread (block=None or block=0 with empty result).
        return self._client().custom_command([b"XREAD", *_enc_list(args)])

    def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
        return self._client().custom_command([b"XREADGROUP", *_enc_list(args)])

    # =========================================================================
    # Scripting (sync)
    # =========================================================================

    def eval(self, script: str, *, keys: Sequence[Any] = (), args: Sequence[Any] = ()) -> Any:
        return self._client().custom_command(
            [
                b"EVAL",
                _enc(script),
                str(len(keys)).encode(),
                *_enc_list(keys),
                *_enc_list(args),
            ],
        )

    # =========================================================================
    # Server (sync)
    # =========================================================================

    def info(self, section: str | None = None) -> dict[str, Any]:
        args: list[bytes | str] = [b"INFO"]
        if section:
            args.append(_enc(section))
        result = self._client().custom_command(args)
        return _parse_info(result)

    def slowlog_len(self) -> int:
        return self._client().custom_command([b"SLOWLOG", b"LEN"])

    # =========================================================================
    # Lock (sync) — inherits from base which uses redis-py-style lock
    # =========================================================================

    def lock(
        self,
        key: str,
        timeout: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        return _GlideLock(
            self._client(),
            key,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
        )

    # =========================================================================
    # Pipeline (sync)
    # =========================================================================

    def _pipeline(self, *, transaction: bool = False) -> ValkeyGlidePipelineAdapter:
        return ValkeyGlidePipelineAdapter(self._client(), transaction=transaction)

    def pipeline(self, *, transaction: bool = True) -> ValkeyGlidePipelineAdapter:
        return self._pipeline(transaction=transaction)

    def apipeline(self, *, transaction: bool = True) -> ValkeyGlideAsyncPipelineAdapter:
        """Construct an async pipeline adapter wrapping glide's async ``Batch``.

        The async client is acquired lazily inside ``execute()`` because
        ``_aclient()`` is itself awaitable; chainable methods only buffer
        commands on the ``Batch`` object so they don't need a client yet.
        """
        return ValkeyGlideAsyncPipelineAdapter(self._aclient, transaction=transaction)

    # =========================================================================
    # Async core ops
    # =========================================================================

    async def aadd(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool:
        client = await self._aclient()
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            result = await client.set(
                key,
                _enc(nvalue),
                conditional_set=ConditionalChange.ONLY_IF_DOES_NOT_EXIST,
            )
            if result == "OK":
                await client.delete([key])
                return True
            return False

        kw: dict[str, Any] = {"conditional_set": ConditionalChange.ONLY_IF_DOES_NOT_EXIST}
        if actual_timeout is not None:
            kw["expiry"] = ExpirySet(ExpiryType.SEC, actual_timeout)
        return await client.set(key, _enc(nvalue), **kw) == "OK"

    async def aget(self, key: KeyT, *, stampede_prevention: bool | dict | None = None) -> Any:
        client = await self._aclient()
        val = await client.get(key)
        if val is None:
            return None
        config = self._resolve_stampede(stampede_prevention)
        if config and isinstance(val, bytes):
            ttl = await client.ttl(key)
            if ttl > 0 and should_recompute(ttl, config):
                return None
        return val

    async def aset(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> None:
        client = await self._aclient()
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            await client.delete([key])
        elif actual_timeout is None:
            await client.set(key, _enc(nvalue))
        else:
            await client.set(key, _enc(nvalue), expiry=ExpirySet(ExpiryType.SEC, actual_timeout))

    async def aset_with_flags(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        stampede_prevention: bool | dict | None = None,
    ) -> bool | Any:
        client = await self._aclient()
        nvalue = value
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            return None if get else False

        kw: dict[str, Any] = {}
        if actual_timeout is not None:
            kw["expiry"] = ExpirySet(ExpiryType.SEC, actual_timeout)
        if nx:
            kw["conditional_set"] = ConditionalChange.ONLY_IF_DOES_NOT_EXIST
        elif xx:
            kw["conditional_set"] = ConditionalChange.ONLY_IF_EXISTS
        if get:
            kw["return_old_value"] = True

        result = await client.set(key, _enc(nvalue), **kw)
        if get:
            return None if result is None else result
        return result == "OK"

    async def atouch(self, key: KeyT, timeout: int | None) -> bool:
        client = await self._aclient()
        if timeout is None:
            return bool(await client.persist(key))
        return bool(await client.expire(key, timeout))

    async def adelete(self, key: KeyT) -> bool:
        return bool(await (await self._aclient()).delete([key]))

    async def aget_many(
        self,
        keys: Iterable[KeyT],
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> dict[KeyT, Any]:
        keys = list(keys)
        if not keys:
            return {}

        client = await self._aclient()
        results = await client.mget(keys)
        found = {k: v for k, v in zip(keys, results, strict=False) if v is not None}

        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            stampede_keys = [k for k, v in found.items() if isinstance(v, bytes)]
            if stampede_keys:
                batch = Batch(is_atomic=False)
                for k in stampede_keys:
                    batch.ttl(k)
                ttls = await client.exec(batch, raise_on_error=True) or []
                for k, ttl in zip(stampede_keys, ttls, strict=False):
                    if isinstance(ttl, int) and ttl > 0 and should_recompute(ttl, config):
                        del found[k]

        return dict(found.items())

    async def ahas_key(self, key: KeyT) -> bool:
        return bool(await (await self._aclient()).exists([key]))

    async def atype(self, key: KeyT) -> KeyType | None:
        result = await (await self._aclient()).type(key)
        if isinstance(result, bytes):
            result = result.decode("utf-8")
        return None if result == "none" else KeyType(result)

    async def aincr(self, key: KeyT, delta: int = 1) -> int:
        client = await self._aclient()
        if delta == 1:
            return await client.incr(key)
        return await client.incrby(key, delta)

    async def aset_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        if not data:
            return []
        client = await self._aclient()
        prepared = {k: _enc(v) for k, v in data.items()}
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            await client.delete(list(prepared.keys()))
        elif actual_timeout is None:
            await client.mset(prepared)
        else:
            batch = Batch(is_atomic=False)
            batch.mset(prepared)
            for key in prepared:
                batch.expire(key, actual_timeout)
            await client.exec(batch, raise_on_error=True)
        return []

    async def adelete_many(self, keys: Sequence[KeyT]) -> int:
        if not keys:
            return 0
        return await (await self._aclient()).delete(list(keys))

    async def aclear(self) -> bool:
        return (await (await self._aclient()).flushdb(FlushMode.SYNC)) == "OK"

    async def aclose(self, **kwargs: Any) -> None:
        for client in self._async_glide_clients.values():
            await client.close()
        self._async_glide_clients.clear()

    # ---- Async TTL ----
    async def attl(self, key: KeyT) -> int | None:
        return _normalize_ttl(await (await self._aclient()).ttl(key))

    async def apttl(self, key: KeyT) -> int | None:
        return _normalize_ttl(await (await self._aclient()).pttl(key))

    async def apersist(self, key: KeyT) -> bool:
        return bool(await (await self._aclient()).persist(key))

    async def aexpire(self, key: KeyT, timeout: int) -> bool:
        return bool(await (await self._aclient()).expire(key, timeout))

    async def apexpire(self, key: KeyT, timeout: int) -> bool:
        return bool(await (await self._aclient()).pexpire(key, timeout))

    async def aexpireat(self, key: KeyT, when: int) -> bool:
        return bool(await (await self._aclient()).expireat(key, when))

    async def apexpireat(self, key: KeyT, when: int) -> bool:
        return bool(await (await self._aclient()).pexpireat(key, when))

    async def aexpiretime(self, key: KeyT) -> int | None:
        return _normalize_ttl(await (await self._aclient()).expiretime(key))

    async def arename(self, src: KeyT, dst: KeyT) -> bool:
        return (await (await self._aclient()).rename(src, dst)) == "OK"

    async def arenamenx(self, src: KeyT, dst: KeyT) -> bool:
        return bool(await (await self._aclient()).renamenx(src, dst))

    # ---- Async scan ----
    async def akeys(self, pattern: str = "*") -> list[bytes]:
        result = await (await self._aclient()).custom_command([b"KEYS", _enc(pattern)])
        return list(result) if result else []

    async def ascan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[bytes, list[bytes]]:
        result = await (await self._aclient()).scan(_enc(cursor), match=match, count=count)
        return result[0], list(result[1])

    async def aiter_keys(self, pattern: str, itersize: int | None = None):
        client = await self._aclient()
        cursor: Any = b"0"
        while True:
            result = await client.scan(cursor, match=pattern, count=itersize)
            cursor, keys = result[0], result[1]
            for k in keys:
                yield k
            if cursor in (b"0", "0", 0):
                return

    async def adelete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        client = await self._aclient()
        deleted = 0
        keys: list = []
        async for k in self.aiter_keys(pattern, itersize=itersize):
            keys.append(k)
            if len(keys) >= (itersize or 100):
                deleted += await client.delete(keys)
                keys = []
        if keys:
            deleted += await client.delete(keys)
        return deleted

    # =========================================================================
    # Async hashes
    # =========================================================================

    async def ahset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        client = await self._aclient()
        m: dict[Any, Any] = {}
        if field is not None:
            m[field] = _enc(value)
        if mapping:
            m.update({f: _enc(v) for f, v in mapping.items()})
        if items:
            for i in range(0, len(items), 2):
                m[items[i]] = _enc(items[i + 1])
        if not m:
            return 0
        return await client.hset(key, m)

    async def ahsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        return await (await self._aclient()).hsetnx(key, field, _enc(value))

    async def ahget(self, key: KeyT, field: str) -> Any:
        val = await (await self._aclient()).hget(key, field)
        return None if val is None else val

    async def ahmget(self, key: KeyT, *fields: str) -> list:
        if len(fields) == 1 and isinstance(fields[0], (list, tuple)):
            fields = tuple(fields[0])
        values = await (await self._aclient()).hmget(key, list(fields))
        return [v if v is not None else None for v in values]

    async def ahgetall(self, key: KeyT) -> dict[str, Any]:
        result = await (await self._aclient()).hgetall(key)
        return {k.decode() if isinstance(k, bytes) else k: v for k, v in result.items()}

    async def ahkeys(self, key: KeyT) -> list[str]:
        return [k.decode() if isinstance(k, bytes) else k for k in await (await self._aclient()).hkeys(key)]

    async def ahvals(self, key: KeyT) -> list:
        return list(await (await self._aclient()).hvals(key))

    async def ahlen(self, key: KeyT) -> int:
        return await (await self._aclient()).hlen(key)

    async def ahexists(self, key: KeyT, field: str) -> bool:
        return bool(await (await self._aclient()).hexists(key, field))

    async def ahdel(self, key: KeyT, *fields: str) -> int:
        return await (await self._aclient()).hdel(key, list(fields))

    async def ahincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        return await (await self._aclient()).hincrby(key, field, amount)

    async def ahincrbyfloat(self, key: KeyT, field: str, amount: float) -> float:
        return await (await self._aclient()).hincrbyfloat(key, field, amount)

    # =========================================================================
    # Async sets
    # =========================================================================

    async def asadd(self, key: KeyT, *members: Any) -> int:
        return await (await self._aclient()).sadd(key, [_enc(m) for m in members])

    async def asrem(self, key: KeyT, *members: Any) -> int:
        return await (await self._aclient()).srem(key, [_enc(m) for m in members])

    async def asmembers(self, key: KeyT) -> _set[Any]:
        return set(await (await self._aclient()).smembers(key))

    async def asismember(self, key: KeyT, member: Any) -> bool:
        return bool(await (await self._aclient()).sismember(key, _enc(member)))

    async def asmismember(self, key: KeyT, *members: Any) -> list[bool]:
        return list(await (await self._aclient()).smismember(key, [_enc(m) for m in members]))

    async def ascard(self, key: KeyT) -> int:
        return await (await self._aclient()).scard(key)

    async def aspop(self, key: KeyT, count: int | None = None) -> Any:
        client = await self._aclient()
        if count is None:
            v = await client.spop(key)
            return None if v is None else v
        return set(await client.spop_count(key, count))

    async def asrandmember(self, key: KeyT, count: int | None = None) -> Any:
        client = await self._aclient()
        if count is None:
            v = await client.srandmember(key)
            return None if v is None else v
        return list(await client.srandmember_count(key, count))

    async def asmove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        return bool(await (await self._aclient()).smove(src, dst, _enc(member)))

    async def asinter(self, *keys: KeyT) -> _set[Any]:
        return set(await (await self._aclient()).sinter(list(keys)))

    async def asunion(self, *keys: KeyT) -> _set[Any]:
        return set(await (await self._aclient()).sunion(list(keys)))

    async def asdiff(self, *keys: KeyT) -> _set[Any]:
        return set(await (await self._aclient()).sdiff(list(keys)))

    async def asinterstore(self, dst: KeyT, *keys: KeyT) -> int:
        return await (await self._aclient()).sinterstore(dst, list(keys))

    async def asunionstore(self, dst: KeyT, *keys: KeyT) -> int:
        return await (await self._aclient()).sunionstore(dst, list(keys))

    async def asdiffstore(self, dst: KeyT, *keys: KeyT) -> int:
        return await (await self._aclient()).sdiffstore(dst, list(keys))

    async def asscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[bytes, list]:
        result = await (await self._aclient()).sscan(key, _enc(cursor), match=match, count=count)
        return result[0], list(result[1])

    async def asscan_iter(self, key: KeyT, match: str | None = None, count: int | None = None):
        client = await self._aclient()
        cursor: Any = b"0"
        while True:
            result = await client.sscan(key, cursor, match=match, count=count)
            cursor, members = result[0], result[1]
            for m in members:
                yield m
            if cursor in (b"0", "0", 0):
                return

    # =========================================================================
    # Async sorted sets
    # =========================================================================

    async def azadd(self, key: KeyT, mapping: Mapping[Any, float], **kwargs: Any) -> int:
        client = await self._aclient()
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
            return await client.custom_command(args)
        return await client.zadd(key, {_enc(m): float(s) for m, s in mapping.items()})

    async def azrem(self, key: KeyT, *members: Any) -> int:
        return await (await self._aclient()).zrem(key, [_enc(m) for m in members])

    async def azscore(self, key: KeyT, member: Any) -> float | None:
        return await (await self._aclient()).zscore(key, _enc(member))

    async def azmscore(self, key: KeyT, members: list[Any]) -> list[float | None]:
        return list(await (await self._aclient()).zmscore(key, [_enc(m) for m in members]))

    async def azrank(self, key: KeyT, member: Any) -> int | None:
        return await (await self._aclient()).zrank(key, _enc(member))

    async def azrevrank(self, key: KeyT, member: Any) -> int | None:
        return await (await self._aclient()).zrevrank(key, _enc(member))

    async def azincrby(self, key: KeyT, amount: float, member: Any) -> float:
        return await (await self._aclient()).zincrby(key, amount, _enc(member))

    async def azremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        return await (await self._aclient()).zremrangebyrank(key, start, end)

    async def azremrangebyscore(self, key: KeyT, mn: Any, mx: Any) -> int:
        return await (await self._aclient()).custom_command([b"ZREMRANGEBYSCORE", key, _enc(mn), _enc(mx)])

    async def azcard(self, key: KeyT) -> int:
        return await (await self._aclient()).zcard(key)

    async def azcount(self, key: KeyT, mn: Any, mx: Any) -> int:
        return await (await self._aclient()).zcount(key, _enc(mn), _enc(mx))

    async def azrange(self, key: KeyT, start: int, end: int, withscores: bool = False, desc: bool = False) -> list:
        args = [b"ZRANGE", key, str(start).encode(), str(end).encode()]
        if desc:
            args.append(b"REV")
        if withscores:
            args.append(b"WITHSCORES")
        return _decode_zrange(await (await self._aclient()).custom_command(args), _passthrough, withscores=withscores)

    async def azrevrange(self, key: KeyT, start: int, end: int, withscores: bool = False) -> list:
        return await self.azrange(key, start, end, withscores=withscores, desc=True)

    async def azrangebyscore(self, key: KeyT, mn: Any, mx: Any, withscores: bool = False) -> list:
        args = [b"ZRANGEBYSCORE", key, _enc(mn), _enc(mx)]
        if withscores:
            args.append(b"WITHSCORES")
        return _decode_zrange(await (await self._aclient()).custom_command(args), _passthrough, withscores=withscores)

    async def azrevrangebyscore(self, key: KeyT, mx: Any, mn: Any, withscores: bool = False) -> list:
        args = [b"ZREVRANGEBYSCORE", key, _enc(mx), _enc(mn)]
        if withscores:
            args.append(b"WITHSCORES")
        return _decode_zrange(await (await self._aclient()).custom_command(args), _passthrough, withscores=withscores)

    async def azpopmin(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        return _decode_zpop(await (await self._aclient()).zpopmin(key, count), _passthrough)

    async def azpopmax(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        return _decode_zpop(await (await self._aclient()).zpopmax(key, count), _passthrough)

    # =========================================================================
    # Async lists
    # =========================================================================

    async def alpush(self, key: KeyT, *values: Any) -> int:
        return await (await self._aclient()).lpush(key, [_enc(v) for v in values])

    async def arpush(self, key: KeyT, *values: Any) -> int:
        return await (await self._aclient()).rpush(key, [_enc(v) for v in values])

    async def alpop(self, key: KeyT, count: int | None = None) -> Any:
        client = await self._aclient()
        if count is None:
            v = await client.lpop(key)
            return None if v is None else v
        result = await client.lpop_count(key, count)
        return list(result) if result else []

    async def arpop(self, key: KeyT, count: int | None = None) -> Any:
        client = await self._aclient()
        if count is None:
            v = await client.rpop(key)
            return None if v is None else v
        result = await client.rpop_count(key, count)
        return list(result) if result else []

    async def alrange(self, key: KeyT, start: int, end: int) -> list:
        return list(await (await self._aclient()).lrange(key, start, end))

    async def altrim(self, key: KeyT, start: int, end: int) -> bool:
        return (await (await self._aclient()).ltrim(key, start, end)) == "OK"

    async def allen(self, key: KeyT) -> int:
        return await (await self._aclient()).llen(key)

    async def alindex(self, key: KeyT, index: int) -> Any:
        v = await (await self._aclient()).lindex(key, index)
        return None if v is None else v

    async def alset(self, key: KeyT, index: int, value: Any) -> bool:
        return (await (await self._aclient()).lset(key, index, _enc(value))) == "OK"

    async def alrem(self, key: KeyT, count: int, value: Any) -> int:
        return await (await self._aclient()).lrem(key, count, _enc(value))

    async def alinsert(self, key: KeyT, where: str, pivot: Any, value: Any) -> int:
        return await (await self._aclient()).custom_command(
            [
                b"LINSERT",
                key,
                _enc(where.upper()),
                _enc(pivot),
                _enc(value),
            ],
        )

    async def alpos(self, key: KeyT, element: Any, **kwargs: Any) -> Any:
        args: list[Any] = [b"LPOS", key, _enc(element)]
        if "rank" in kwargs:
            args.extend([b"RANK", str(kwargs["rank"]).encode()])
        if "count" in kwargs:
            args.extend([b"COUNT", str(kwargs["count"]).encode()])
        if "maxlen" in kwargs:
            args.extend([b"MAXLEN", str(kwargs["maxlen"]).encode()])
        return await (await self._aclient()).custom_command(args)

    async def almove(self, src: KeyT, dst: KeyT, src_dir: str, dst_dir: str) -> Any:
        v = await (await self._aclient()).custom_command(
            [b"LMOVE", src, dst, _enc(src_dir.upper()), _enc(dst_dir.upper())],
        )
        return None if v is None else v

    async def ablmove(self, src: KeyT, dst: KeyT, src_dir: str, dst_dir: str, timeout: float) -> Any:
        v = await (await self._aclient()).custom_command(
            [
                b"BLMOVE",
                src,
                dst,
                _enc(src_dir.upper()),
                _enc(dst_dir.upper()),
                str(timeout).encode(),
            ],
        )
        return None if v is None else v

    async def ablpop(self, keys: Any, timeout: float = 0) -> Any:
        ks = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        return await (await self._aclient()).custom_command([b"BLPOP", *_enc_list(ks), str(timeout).encode()])

    async def abrpop(self, keys: Any, timeout: float = 0) -> Any:
        ks = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        return await (await self._aclient()).custom_command([b"BRPOP", *_enc_list(ks), str(timeout).encode()])

    # =========================================================================
    # Async streams (mostly via custom_command)
    # =========================================================================

    async def axadd(self, key: KeyT, fields: Mapping[Any, Any], id: str = "*", **kwargs: Any) -> str:
        args = [b"XADD", key, _enc(id)]
        for f, v in fields.items():
            args.extend([_enc(f), _enc(v)])
        result = await (await self._aclient()).custom_command(args)
        return result.decode() if isinstance(result, bytes) else result

    async def axlen(self, key: KeyT) -> int:
        return await (await self._aclient()).xlen(key)

    async def axrange(self, key: KeyT, mn: str = "-", mx: str = "+", count: int | None = None) -> Any:
        args = [b"XRANGE", key, _enc(mn), _enc(mx)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        return await (await self._aclient()).custom_command(args)

    async def axrevrange(self, key: KeyT, mx: str = "+", mn: str = "-", count: int | None = None) -> Any:
        args = [b"XREVRANGE", key, _enc(mx), _enc(mn)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        return await (await self._aclient()).custom_command(args)

    async def axdel(self, key: KeyT, *ids: Any) -> int:
        return await (await self._aclient()).custom_command([b"XDEL", key, *_enc_list(ids)])

    async def axtrim(self, key: KeyT, **kwargs: Any) -> int:
        args: list[Any] = [b"XTRIM", key]
        if "maxlen" in kwargs:
            args.extend([b"MAXLEN", str(kwargs["maxlen"]).encode()])
        elif "minid" in kwargs:
            args.extend([b"MINID", _enc(kwargs["minid"])])
        return await (await self._aclient()).custom_command(args)

    async def axack(self, key: KeyT, group: str, *ids: Any) -> int:
        return await (await self._aclient()).custom_command([b"XACK", key, _enc(group), *_enc_list(ids)])

    async def axclaim(self, *args: Any, **kwargs: Any) -> Any:
        return await (await self._aclient()).custom_command([b"XCLAIM", *_enc_list(args)])

    async def axautoclaim(self, *args: Any, **kwargs: Any) -> Any:
        return await (await self._aclient()).custom_command([b"XAUTOCLAIM", *_enc_list(args)])

    async def axpending(self, key: KeyT, group: str) -> Any:
        return await (await self._aclient()).custom_command([b"XPENDING", key, _enc(group)])

    async def axinfo_stream(self, key: KeyT) -> Any:
        return await (await self._aclient()).custom_command([b"XINFO", b"STREAM", key])

    async def axinfo_groups(self, key: KeyT) -> Any:
        return await (await self._aclient()).custom_command([b"XINFO", b"GROUPS", key])

    async def axinfo_consumers(self, key: KeyT, group: str) -> Any:
        return await (await self._aclient()).custom_command([b"XINFO", b"CONSUMERS", key, _enc(group)])

    async def axgroup_destroy(self, key: KeyT, group: str) -> int:
        return await (await self._aclient()).custom_command([b"XGROUP", b"DESTROY", key, _enc(group)])

    async def axgroup_setid(self, key: KeyT, group: str, id: str) -> bool:
        return (await (await self._aclient()).custom_command([b"XGROUP", b"SETID", key, _enc(group), _enc(id)])) == "OK"

    async def axgroup_delconsumer(self, key: KeyT, group: str, consumer: str) -> int:
        return await (await self._aclient()).custom_command(
            [b"XGROUP", b"DELCONSUMER", key, _enc(group), _enc(consumer)],
        )

    async def axread(self, *args: Any, **kwargs: Any) -> Any:
        return await (await self._aclient()).custom_command([b"XREAD", *_enc_list(args)])

    async def axreadgroup(self, *args: Any, **kwargs: Any) -> Any:
        return await (await self._aclient()).custom_command([b"XREADGROUP", *_enc_list(args)])

    # =========================================================================
    # Async eval
    # =========================================================================

    async def aeval(self, script: str, *, keys: Sequence[Any] = (), args: Sequence[Any] = ()) -> Any:
        return await (await self._aclient()).custom_command(
            [
                b"EVAL",
                _enc(script),
                str(len(keys)).encode(),
                *_enc_list(keys),
                *_enc_list(args),
            ],
        )

    # =========================================================================
    # Async lock
    # =========================================================================

    def alock(
        self,
        key: str,
        timeout: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        return _AsyncGlideLock(
            self,
            key,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
        )


# =============================================================================
# Helpers
# =============================================================================


def _batched(it: Iterable[Any], n: int) -> Iterable[list[Any]]:
    """Yield successive n-sized batches from iterable."""
    chunk: list[Any] = []
    for item in it:
        chunk.append(item)
        if len(chunk) >= n:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _passthrough(value: Any) -> Any:
    """No-op decoder. Decoding now happens at the cache layer."""
    return value


def _decode_zrange(result: Any, decoder: Any, *, withscores: bool) -> list:
    """Decode ZRANGE/ZRANGEBYSCORE result into [(member, score), ...] or [member, ...]."""
    if not result:
        return []
    if not withscores:
        return [decoder(v) for v in result]
    # result is flat [m1, s1, m2, s2, ...]
    out = []
    for i in range(0, len(result), 2):
        out.append((decoder(result[i]), float(result[i + 1])))
    return out


def _decode_zpop(result: Any, decoder: Any) -> list[tuple[Any, float]]:
    """Decode ZPOPMIN/ZPOPMAX result into [(member, score), ...]."""
    if not result:
        return []
    if isinstance(result, dict):
        return [(decoder(m), float(s)) for m, s in result.items()]
    # list shape
    return [(decoder(m), float(s)) for m, s in result]


def _parse_info(raw: bytes | str) -> dict[str, Any]:
    """Parse INFO output into a dict (mimics redis-py.info())."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    out: dict[str, Any] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            k, v = line.split(":", 1)
            out[k] = _coerce_info_value(v)
    return out


def _coerce_info_value(v: str) -> Any:
    if v.isdigit():
        return int(v)
    try:
        return float(v)
    except ValueError:
        return v


# =============================================================================
# Locks (minimal SET NX EX + Lua release implementation)
# =============================================================================


_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class _GlideLock:
    """Sync distributed lock backed by SET NX EX + Lua release."""

    def __init__(
        self,
        client: GlideClient,
        key: str,
        *,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
    ) -> None:
        self._client = client
        self._key = key
        self._timeout = timeout
        self._sleep = sleep
        self._blocking = blocking
        self._blocking_timeout = blocking_timeout
        self._token: bytes | None = None
        self._initial_token = os.urandom(16).hex().encode()

    def acquire(self, *, blocking: bool | None = None, blocking_timeout: float | None = None) -> bool:
        bl = self._blocking if blocking is None else blocking
        bt = self._blocking_timeout if blocking_timeout is None else blocking_timeout
        deadline = time.monotonic() + bt if bt else None

        kw: dict[str, Any] = {"conditional_set": ConditionalChange.ONLY_IF_DOES_NOT_EXIST}
        if self._timeout is not None:
            kw["expiry"] = ExpirySet(ExpiryType.SEC, int(self._timeout))

        while True:
            result = self._client.set(self._key, self._initial_token, **kw)
            if result == "OK":
                self._token = self._initial_token
                return True
            if not bl:
                return False
            if deadline and time.monotonic() >= deadline:
                return False
            time.sleep(self._sleep)

    def release(self) -> None:
        if self._token is None:
            msg = "Cannot release un-acquired lock"
            raise RuntimeError(msg)
        self._client.custom_command([b"EVAL", _RELEASE_LUA.encode(), b"1", _enc(self._key), self._token])
        self._token = None

    def __enter__(self) -> Self:
        if not self.acquire():
            msg = f"Could not acquire lock on {self._key}"
            raise RuntimeError(msg)
        return self

    def __exit__(self, *exc: object) -> None:
        if self._token is not None:
            self.release()


class _AsyncGlideLock:
    def __init__(
        self,
        adapter: ValkeyGlideAdapter,
        key: str,
        *,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
    ) -> None:
        self._adapter = adapter
        self._key = key
        self._timeout = timeout
        self._sleep = sleep
        self._blocking = blocking
        self._blocking_timeout = blocking_timeout
        self._token: bytes | None = None
        self._initial_token = os.urandom(16).hex().encode()

    async def acquire(self, *, blocking: bool | None = None, blocking_timeout: float | None = None) -> bool:
        bl = self._blocking if blocking is None else blocking
        bt = self._blocking_timeout if blocking_timeout is None else blocking_timeout
        deadline = time.monotonic() + bt if bt else None
        client = await self._adapter._aclient()

        kw: dict[str, Any] = {"conditional_set": ConditionalChange.ONLY_IF_DOES_NOT_EXIST}
        if self._timeout is not None:
            kw["expiry"] = ExpirySet(ExpiryType.SEC, int(self._timeout))

        while True:
            result = await client.set(self._key, self._initial_token, **kw)
            if result == "OK":
                self._token = self._initial_token
                return True
            if not bl:
                return False
            if deadline and time.monotonic() >= deadline:
                return False
            await asyncio.sleep(self._sleep)

    async def release(self) -> None:
        if self._token is None:
            msg = "Cannot release un-acquired lock"
            raise RuntimeError(msg)
        client = await self._adapter._aclient()
        await client.custom_command([b"EVAL", _RELEASE_LUA.encode(), b"1", _enc(self._key), self._token])
        self._token = None

    async def __aenter__(self) -> Self:
        if not await self.acquire():
            msg = f"Could not acquire lock on {self._key}"
            raise RuntimeError(msg)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._token is not None:
            await self.release()


__all__ = ["ValkeyGlideAdapter", "ValkeyGlideAsyncPipelineAdapter", "ValkeyGlidePipelineAdapter"]
