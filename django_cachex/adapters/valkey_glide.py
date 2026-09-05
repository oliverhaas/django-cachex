"""``valkey-glide``-backed cache adapter (sync + async).

Each operation method implements ``RespAdapterProtocol`` and calls
``glide_sync.GlideClient`` / ``glide.GlideClient`` natively. There is
no redis-py-shaped intermediary for the operation surface and no base
adapter to inherit from: serialization and decoding both happen at the
cache layer, so values pass through this module untouched.

The pipeline adapter (``ValkeyGlidePipelineAdapter`` / ``ValkeyGlideAsyncPipelineAdapter``)
implements ``RespPipelineProtocol`` natively against glide's ``Batch``
with no redis-py-shaped intermediary on the queueing surface either.

Standalone and cluster topologies are supported; Sentinel is not exposed
(``valkey-glide`` itself does not ship a Sentinel client).
"""

import asyncio
import contextlib
import datetime
import inspect
import os
import threading
import time
import weakref
from itertools import batched
from typing import TYPE_CHECKING, Any, Self, cast
from urllib.parse import parse_qs, unquote, urlparse

from django.core.exceptions import ImproperlyConfigured

from django_cachex.adapters.protocols import RespAdapterProtocol, RespAsyncPipelineProtocol, RespPipelineProtocol
from django_cachex.adapters.valkey_py import _check_xpending_args, _options_key
from django_cachex.exceptions import KeyNotFoundError, maybe_wrap_wrongtype
from django_cachex.stampede import (
    StampedeConfig,
    get_timeout_with_buffer,
    make_stampede_config,
    resolve_stampede,
    should_recompute,
)
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence


# valkey-glide is an optional install: the names below stay unbound without it,
# and ``_check_installed`` turns that into an actionable error at construction.
try:
    from glide import ClusterScanCursor as AsyncClusterScanCursor  # ty: ignore[unresolved-import]
    from glide import GlideClient as AsyncGlideClient  # ty: ignore[unresolved-import]
    from glide import GlideClientConfiguration as AsyncGlideClientConfiguration  # ty: ignore[unresolved-import]
    from glide import GlideClusterClient as AsyncGlideClusterClient  # ty: ignore[unresolved-import]
    from glide import (  # ty: ignore[unresolved-import]
        GlideClusterClientConfiguration as AsyncGlideClusterClientConfiguration,
    )
    from glide import NodeAddress as AsyncNodeAddress  # ty: ignore[unresolved-import]
    from glide import ServerCredentials as AsyncServerCredentials  # ty: ignore[unresolved-import]
    from glide_sync import (  # ty: ignore[unresolved-import]
        Batch,
        ClosingError,
        ClusterBatch,
        ClusterScanCursor,
        ConditionalChange,
        ExpirySet,
        ExpiryType,
        FlushMode,
        GlideClientConfiguration,
        GlideClusterClient,
        GlideClusterClientConfiguration,
        NodeAddress,
        ObjectType,
        RandomNode,
        RequestError,
        ServerCredentials,
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
# WRONGTYPE translation
# =============================================================================


async def _await_translated(awaitable: Any) -> Any:
    try:
        return await awaitable
    except RequestError as exc:
        wrapped = maybe_wrap_wrongtype(exc)
        if wrapped is exc:
            raise
        raise wrapped from exc


def _translating(fn: Any) -> Any:
    def call(*args: Any, **kwargs: Any) -> Any:
        try:
            result = fn(*args, **kwargs)
        except RequestError as exc:
            wrapped = maybe_wrap_wrongtype(exc)
            if wrapped is exc:
                raise
            raise wrapped from exc
        # The async client's methods are coroutine functions, so the error
        # surfaces on await rather than on call.
        return _await_translated(result) if inspect.isawaitable(result) else result

    return call


class _WrongTypeClient:
    """Forward every attribute to a glide client, translating WRONGTYPE responses.

    Glide's Rust-backed clients have no ``execute_command`` seam for the patch
    :mod:`~django_cachex.adapters.valkey_py` uses, so wrap the client instead.
    Callables come back wrapped (cached per name); anything else passes through.
    """

    __slots__ = ("_glide_client", "_wrappers")

    def __init__(self, client: Any) -> None:
        self._glide_client = client
        self._wrappers: dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        wrapper = self._wrappers.get(name)
        if wrapper is not None:
            return wrapper
        attr = getattr(self._glide_client, name)
        if not callable(attr):
            return attr
        wrapper = _translating(attr)
        self._wrappers[name] = wrapper
        return wrapper

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._glide_client!r})"

    # Python looks dunders up on the type, so __getattr__ never sees them and
    # ``with client:`` would fail even though the wrapped client supports it.

    def __enter__(self) -> Any:
        self._glide_client.__enter__()
        return self

    def __exit__(self, *args: object) -> Any:
        return self._glide_client.__exit__(*args)

    async def __aenter__(self) -> Any:
        await self._glide_client.__aenter__()
        return self

    async def __aexit__(self, *args: object) -> Any:
        return await self._glide_client.__aexit__(*args)


# =============================================================================
# Process-wide client registries
# =============================================================================
# Glide clients are expensive and Django hands out a fresh ``BaseCache`` per
# asyncio task, so these registries share one per loop (async) or config (sync).

if TYPE_CHECKING:
    _GlideSyncRegistry = dict[tuple[Any, ...], "GlideClient"]
    _GlideAsyncRegistry = weakref.WeakKeyDictionary[
        asyncio.AbstractEventLoop,
        dict[tuple[Any, ...], "AsyncGlideClient"],
    ]

_GLIDE_SYNC_CLIENTS: dict[tuple[Any, ...], Any] = {}
_GLIDE_SYNC_LOCK = threading.Lock()
_GLIDE_ASYNC_CLIENTS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[tuple[Any, ...], Any]] = (
    weakref.WeakKeyDictionary()
)
# Per-loop async-create locks: without them two tasks both miss the registry,
# both create a client, and the loser's is dropped without ``close()``.
_GLIDE_ASYNC_LOCKS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = weakref.WeakKeyDictionary()
# Guards the sweep, which iterates the registries while other threads insert.
# Matches :mod:`~django_cachex.adapters.valkey_py`'s ``_ASYNC_REGISTRY_LOCK``.
_GLIDE_ASYNC_REGISTRY_LOCK = threading.RLock()


async def _aclose_glide_client(client: Any) -> None:
    """Close a glide async client, tolerating an already-closed one."""
    close = getattr(client, "close", None)
    if close is None:
        return
    with contextlib.suppress(ClosingError):
        result = close()
        if inspect.isawaitable(result):
            await result


def _glide_config_key(servers: list[str], options: dict[str, Any]) -> tuple[Any, ...]:
    """Stable hashable key from the constructor inputs."""
    return (tuple(servers), _options_key(options))


_TLS_SCHEMES = frozenset({"rediss", "valkeys"})


def _parse_db(u: Any, options: dict[str, Any]) -> int | None:
    """Database index: OPTIONS ``db`` beats the URL query, which beats the URL path."""
    if (db := options.get("db")) is not None:
        return int(db)
    query_db = parse_qs(u.query).get("db")
    if query_db and query_db[-1].isdigit():
        return int(query_db[-1])
    path = u.path.lstrip("/")
    if path.isdigit():
        return int(path)
    return None


def _glide_config_kwargs(
    servers: list[str],
    options: dict[str, Any],
    *,
    credentials_cls: Any,
    include_database: bool = True,
) -> dict[str, Any]:
    """``Glide*ClientConfiguration`` kwargs from the first URL plus OPTIONS.

    ``credentials_cls`` is the sync or async ``ServerCredentials`` flavor;
    cluster configs pass ``include_database=False`` (cluster only serves db 0).
    """
    u = urlparse(servers[0])
    kwargs: dict[str, Any] = {}

    use_tls = u.scheme in _TLS_SCHEMES
    for opt in ("use_tls", "ssl"):
        if opt in options:
            use_tls = bool(options[opt])
            break
    if use_tls:
        kwargs["use_tls"] = True

    # ``urlparse`` leaves percent-escapes in place; redis-py's ``parse_url``
    # unquotes credentials, so match it.
    username = options.get("username") or (unquote(u.username) if u.username else None)
    password = options.get("password") or (unquote(u.password) if u.password else None)
    if password is not None:
        # glide rejects a username without a password, so nopass users connect unauthenticated.
        kwargs["credentials"] = credentials_cls(password=password, username=username)

    if include_database and (db := _parse_db(u, options)) is not None:
        kwargs["database_id"] = db

    if (request_timeout := options.get("request_timeout")) is not None:
        kwargs["request_timeout"] = int(request_timeout)
    if (client_name := options.get("client_name")) is not None:
        kwargs["client_name"] = client_name
    return kwargs


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


def _object_type(name: str | None) -> Any:
    """Map a RESP type name to glide's ``ObjectType``, which SCAN requires."""
    # An unknown name must raise: dropping the filter would return keys of
    # every type, which reads as a filter that matched everything.
    if name is None:
        return None
    wanted = name.lower()
    match = next((t for t in ObjectType if t.value.lower() == wanted), None)
    if match is None:
        known = ", ".join(sorted(t.value.lower() for t in ObjectType))
        msg = f"Unknown key type {name!r}. Expected one of: {known}."
        raise ValueError(msg)
    return match


def _enc_list(values: Iterable[Any]) -> list[bytes | str]:
    return [_enc(v) for v in values]


def _enc_map(mapping: Mapping[Any, Any]) -> dict[Any, bytes | str]:
    return {k: _enc(v) for k, v in mapping.items()}


def _xadd_args(
    key: Any,
    fields: Mapping[Any, Any],
    entry_id: str,
    *,
    maxlen: int | None = None,
    approximate: bool = True,
    nomkstream: bool = False,
    minid: str | None = None,
    limit: int | None = None,
) -> list[Any]:
    args: list[Any] = [b"XADD", key]
    if nomkstream:
        args.append(b"NOMKSTREAM")
    if maxlen is not None:
        args.append(b"MAXLEN")
        if approximate:
            args.append(b"~")
        args.append(str(maxlen).encode())
    elif minid is not None:
        args.append(b"MINID")
        if approximate:
            args.append(b"~")
        args.append(_enc(minid))
    if limit is not None:
        args.extend([b"LIMIT", str(limit).encode()])
    args.append(_enc(entry_id))
    for field, value in fields.items():
        args.extend([_enc(field), _enc(value)])
    return args


def _normalize_ttl(result: int) -> int | None:
    """Normalize TTL/PTTL/EXPIRETIME results: -1 (no expiry) -> None."""
    if result == -1:
        return None
    return result


def _expire_arg(timeout: int | datetime.timedelta, *, milliseconds: bool = False) -> int:
    """Render an EXPIRE/PEXPIRE argument as an ``int``.

    ``RespCache.expire``/``pexpire`` accept ``int | timedelta`` and hand the
    value straight to the adapter. redis-py and valkey-py convert a
    ``timedelta`` themselves; glide renders arguments with ``str()``, so
    ``EXPIRE k 0:05:00`` would reach the server. Convert it here.
    """
    if isinstance(timeout, datetime.timedelta):
        seconds = timeout.total_seconds()
        return int(seconds * 1000) if milliseconds else int(seconds)
    return int(timeout)


def _to_unix(when: int | datetime.datetime, *, milliseconds: bool = False) -> int:
    """Convert a datetime or unix timestamp to ``int`` epoch seconds (or ms)."""
    if isinstance(when, datetime.datetime):
        ts = when.timestamp()
        return int(ts * 1000) if milliseconds else int(ts)
    return int(when)


def _dec_str(v: Any) -> str:
    """Decode bytes to str; pass through anything else."""
    return v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v


def _dec_keys(values: Iterable[Any]) -> list[str]:
    return [_dec_str(v) for v in values]


def _decode_stream_entries(raw: Any) -> list[tuple[str, dict[str, Any]]]:
    """Normalize glide's XRANGE/XREVRANGE response into ``[(entry_id, {field: value}), ...]``.

    Glide's ``custom_command`` returns ``{entry_id_bytes: [[field_bytes, value_bytes], ...]}``;
    the cache layer expects an ordered list of ``(entry_id_str, {field_str: value})``.
    """
    if not raw:
        return []
    out: list[tuple[str, dict[str, Any]]] = []
    for entry_id, pairs in raw.items():
        fields: dict[str, Any] = {}
        for pair in pairs:
            f, v = pair[0], pair[1]
            fields[_dec_str(f)] = v
        out.append((_dec_str(entry_id), fields))
    return out


def _decode_xread(raw: Any) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
    """Normalize XREAD/XREADGROUP response: ``{stream: [(id, {field: value}), ...]}``."""
    if not raw:
        return None
    return {_dec_str(stream): _decode_stream_entries(entries) for stream, entries in raw.items()}


def _decode_xinfo(raw: Any) -> Any:
    """Normalize XINFO STREAM/GROUPS/CONSUMERS responses to keyed dicts with ``str`` keys.

    ``XINFO STREAM FULL`` nests the group and consumer dicts inside list
    values, so the walk has to recurse through lists as well.
    """
    if isinstance(raw, dict):
        return {_dec_str(k): _decode_xinfo(v) for k, v in raw.items()}
    if isinstance(raw, list):
        return [_decode_xinfo(item) for item in raw]
    return raw


def _decode_xpending_summary(raw: Any) -> dict[str, Any]:
    """Reshape ``[total, min_id, max_id, [[consumer, count], ...]]`` into the protocol's dict."""
    if not raw or raw[0] == 0:
        return {"pending": 0, "min": None, "max": None, "consumers": []}
    return {
        "pending": int(raw[0]),
        "min": _dec_str(raw[1]) if raw[1] is not None else None,
        "max": _dec_str(raw[2]) if raw[2] is not None else None,
        "consumers": [{"name": _dec_str(c[0]), "pending": int(c[1])} for c in (raw[3] or [])],
    }


def _decode_xpending_range(raw: Any) -> list[dict[str, Any]]:
    """Reshape ``[[id, consumer, idle_ms, deliveries], ...]`` into the protocol's dicts."""
    return [
        {
            "message_id": _dec_str(row[0]),
            "consumer": _dec_str(row[1]),
            "time_since_delivered": int(row[2]),
            "times_delivered": int(row[3]),
        }
        for row in (raw or [])
    ]


def _key_type(name: str) -> KeyType | None:
    """Map a TYPE reply to ``KeyType``; a module type the package doesn't model is ``UNKNOWN``."""
    if name == "none":
        return None
    try:
        return KeyType(name)
    except ValueError:
        return KeyType.UNKNOWN


# =============================================================================
# Pipeline adapters for django_cachex.adapters.pipeline.Pipeline
# =============================================================================


def _ok_to_bool(v: Any) -> bool:
    return v in ("OK", b"OK")


def _new_batch(*, atomic: bool) -> Any:
    return Batch(is_atomic=atomic)


def _decode_xread_pipeline(raw: Any) -> Any:
    """Pipeline-shaped xread/xreadgroup decoder.

    The cache layer's pipeline decoder iterates ``for stream_key, entries in results``
    and then ``for entry_id, fields in entries`` (where ``fields`` is a ``dict``).
    We hand it ``[(stream_key, [(entry_id, {field: value_bytes}), ...]), ...]``
    so the cache layer can decode the values without further reshaping.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        return raw
    out: list[tuple[Any, list[tuple[Any, dict[Any, Any]]]]] = []
    for stream, entries in raw.items():
        shaped: list[tuple[Any, dict[Any, Any]]] = []
        if isinstance(entries, dict):
            for entry_id, pairs in entries.items():
                fields: dict[Any, Any] = {}
                for pair in pairs:
                    fields[pair[0]] = pair[1]
                shaped.append((entry_id, fields))
        out.append((stream, shaped))
    return out


class ValkeyGlidePipelineAdapter(RespPipelineProtocol):
    """Pipeline adapter that buffers cachex ops into glide's ``Batch``."""

    def __init__(self, client: GlideClient, *, transaction: bool = False, batch_factory: Any = None) -> None:
        self._client: Any = client
        # ``GlideClusterClient.exec`` is typed for ``ClusterBatch``, so the
        # topology hands its own factory in.
        self._new_batch = batch_factory or _new_batch
        self._batch = self._new_batch(atomic=transaction)
        # Sparse post-processors keyed by the command's index in
        # ``self._batch.commands``; a reply that already fits skips this.
        self._post: dict[int, Any] = {}

    def _track(self, post: Any) -> None:
        """Apply ``post`` to the result of the most recently queued command."""
        self._post[len(self._batch.commands) - 1] = post

    # ---- strings ----
    def set(
        self,
        key: Any,
        value: Any,
        *,
        ex: int | datetime.timedelta | None = None,
        px: int | datetime.timedelta | None = None,
        nx: bool = False,
        xx: bool = False,
        exat: int | datetime.datetime | None = None,
        pxat: int | datetime.datetime | None = None,
        keepttl: bool = False,
        get: bool = False,
    ) -> Self:
        expiries = [
            (ExpiryType.SEC, ex),
            (ExpiryType.MILLSEC, px),
            (ExpiryType.UNIX_SEC, exat),
            (ExpiryType.UNIX_MILLSEC, pxat),
        ]
        given = [pair for pair in expiries if pair[1] is not None]
        if keepttl:
            given.append((ExpiryType.KEEP_TTL, None))
        if len(given) > 1:
            msg = "set() accepts at most one of ex, px, exat, pxat and keepttl"
            raise ValueError(msg)
        options: dict[str, Any] = {}
        if given:
            options["expiry"] = ExpirySet(*given[0])
        if nx:
            options["conditional_set"] = ConditionalChange.ONLY_IF_DOES_NOT_EXIST
        elif xx:
            options["conditional_set"] = ConditionalChange.ONLY_IF_EXISTS
        if get:
            options["return_old_value"] = True
        self._batch.set(key, _enc(value), **options)
        # A GET reply is the old value, not an OK.
        if not get:
            self._track(_ok_to_bool)
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

    def expire(self, key: Any, seconds: int | datetime.timedelta) -> Self:
        self._batch.expire(key, _expire_arg(seconds))
        return self

    def pexpire(self, key: Any, ms: int | datetime.timedelta) -> Self:
        self._batch.pexpire(key, _expire_arg(ms, milliseconds=True))
        return self

    def expireat(self, key: Any, when: int | datetime.datetime) -> Self:
        self._batch.expireat(key, _to_unix(when))
        return self

    def pexpireat(self, key: Any, when: int | datetime.datetime) -> Self:
        self._batch.pexpireat(key, _to_unix(when, milliseconds=True))
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
        if not m:
            # Queueing nothing would shift every later result in the batch.
            msg = "hset requires at least one field/value pair"
            raise ValueError(msg)
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

    def hincrbyfloat(self, key: Any, field: Any, amount: float = 1.0) -> Self:
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
            if kwargs.get("gt"):
                args.append(b"GT")
            elif kwargs.get("lt"):
                args.append(b"LT")
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

    def zmscore(self, key: Any, members: Sequence[Any]) -> Self:
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
        self._batch.custom_command([b"ZCOUNT", key, _enc(mn), _enc(mx)])
        return self

    def zrange(
        self,
        key: Any,
        start: int,
        end: int,
        withscores: bool = False,
        desc: bool = False,
    ) -> Self:
        args = [b"ZRANGE", key, str(start).encode(), str(end).encode()]
        if desc:
            args.append(b"REV")
        if withscores:
            args.append(b"WITHSCORES")
        self._batch.custom_command(args)
        self._track(lambda r: _decode_zrange(r, withscores=withscores))
        return self

    def zrevrange(
        self,
        key: Any,
        start: int,
        end: int,
        withscores: bool = False,
    ) -> Self:
        return self.zrange(key, start, end, withscores=withscores, desc=True)

    def zrangebyscore(
        self,
        key: Any,
        min: Any,
        max: Any,
        start: int | None = None,
        num: int | None = None,
        *,
        withscores: bool = False,
    ) -> Self:
        args = [b"ZRANGEBYSCORE", key, _enc(min), _enc(max)]
        if withscores:
            args.append(b"WITHSCORES")
        if start is not None and num is not None:
            args.extend([b"LIMIT", str(start).encode(), str(num).encode()])
        self._batch.custom_command(args)
        self._track(lambda r: _decode_zrange(r, withscores=withscores))
        return self

    def zrevrangebyscore(
        self,
        key: Any,
        max: Any,
        min: Any,
        start: int | None = None,
        num: int | None = None,
        *,
        withscores: bool = False,
    ) -> Self:
        args = [b"ZREVRANGEBYSCORE", key, _enc(max), _enc(min)]
        if withscores:
            args.append(b"WITHSCORES")
        if start is not None and num is not None:
            args.extend([b"LIMIT", str(start).encode(), str(num).encode()])
        self._batch.custom_command(args)
        self._track(lambda r: _decode_zrange(r, withscores=withscores))
        return self

    def zpopmin(self, key: Any, count: int | None = None) -> Self:
        if count is None:
            count = 1
        self._batch.zpopmin(key, count)
        self._track(_decode_zpop)
        return self

    def zpopmax(self, key: Any, count: int | None = None) -> Self:
        if count is None:
            count = 1
        self._batch.zpopmax(key, count)
        self._track(_decode_zpop)
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
        self._track(_ok_to_bool)
        return self

    def llen(self, key: Any) -> Self:
        self._batch.llen(key)
        return self

    def lindex(self, key: Any, index: int) -> Self:
        self._batch.lindex(key, index)
        return self

    def lset(self, key: Any, index: int, value: Any) -> Self:
        self._batch.lset(key, index, _enc(value))
        self._track(_ok_to_bool)
        return self

    def lrem(self, key: Any, count: int, value: Any) -> Self:
        self._batch.lrem(key, count, _enc(value))
        return self

    def linsert(self, key: Any, where: str, pivot: Any, value: Any) -> Self:
        self._batch.custom_command([b"LINSERT", key, _enc(where.upper()), _enc(pivot), _enc(value)])
        return self

    def lpos(
        self,
        key: Any,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> Self:
        args: list[Any] = [b"LPOS", key, _enc(value)]
        if rank is not None:
            args.extend([b"RANK", str(rank).encode()])
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if maxlen is not None:
            args.extend([b"MAXLEN", str(maxlen).encode()])
        self._batch.custom_command(args)
        return self

    def lmove(self, src: Any, dst: Any, wherefrom: str = "LEFT", whereto: str = "RIGHT") -> Self:
        self._batch.custom_command([b"LMOVE", src, dst, _enc(wherefrom.upper()), _enc(whereto.upper())])
        return self

    # ---- streams (via custom_command for everything that's not single-response) ----
    def xadd(
        self,
        key: Any,
        fields: Mapping[Any, Any],
        id: str = "*",
        *,
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
    ) -> Self:
        self._batch.custom_command(
            _xadd_args(
                key,
                fields,
                id,
                maxlen=maxlen,
                approximate=approximate,
                nomkstream=nomkstream,
                minid=minid,
                limit=limit,
            ),
        )
        return self

    def xlen(self, key: Any) -> Self:
        self._batch.xlen(key)
        return self

    def xrange(self, key: Any, min: str = "-", max: str = "+", count: int | None = None) -> Self:
        args = [b"XRANGE", key, _enc(min), _enc(max)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        self._batch.custom_command(args)
        self._track(_decode_stream_entries)
        return self

    def xrevrange(self, key: Any, max: str = "+", min: str = "-", count: int | None = None) -> Self:
        args = [b"XREVRANGE", key, _enc(max), _enc(min)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        self._batch.custom_command(args)
        self._track(_decode_stream_entries)
        return self

    def xread(
        self,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> Self:
        args: list[Any] = [b"XREAD"]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if block is not None:
            args.extend([b"BLOCK", str(block).encode()])
        args.append(b"STREAMS")
        args.extend(_enc_list(streams.keys()))
        args.extend(_enc_list(streams.values()))
        self._batch.custom_command(args)
        self._track(_decode_xread_pipeline)
        return self

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> Self:
        args: list[Any] = [b"XREADGROUP", b"GROUP", _enc(group), _enc(consumer)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if block is not None:
            args.extend([b"BLOCK", str(block).encode()])
        if noack:
            args.append(b"NOACK")
        args.append(b"STREAMS")
        args.extend(_enc_list(streams.keys()))
        args.extend(_enc_list(streams.values()))
        self._batch.custom_command(args)
        self._track(_decode_xread_pipeline)
        return self

    def xpending(self, key: Any, group: str) -> Self:
        self._batch.custom_command([b"XPENDING", key, _enc(group)])
        self._track(_decode_xpending_summary)
        return self

    def xpending_range(
        self,
        key: Any,
        group: str,
        min: str = "-",
        max: str = "+",
        count: int = 10,
        **kwargs: Any,
    ) -> Self:
        args: list[Any] = [b"XPENDING", key, _enc(group)]
        if (idle := kwargs.get("idle")) is not None:
            args.extend([b"IDLE", str(idle).encode()])
        args.extend([_enc(min), _enc(max), str(count).encode()])
        # ``pipeline.py`` writes ``kwargs["consumername"] = consumer``;
        # tolerate the older ``"consumer"`` spelling too.
        consumer = kwargs.get("consumername", kwargs.get("consumer"))
        if consumer is not None:
            args.append(_enc(consumer))
        self._batch.custom_command(args)
        self._track(_decode_xpending_range)
        return self

    def xdel(self, key: Any, *ids: Any) -> Self:
        self._batch.custom_command([b"XDEL", key, *_enc_list(ids)])
        return self

    def xtrim(
        self,
        key: Any,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
    ) -> Self:
        args: list[Any] = [b"XTRIM", key]
        if maxlen is not None:
            args.append(b"MAXLEN")
            if approximate:
                args.append(b"~")
            args.append(str(maxlen).encode())
        elif minid is not None:
            args.append(b"MINID")
            if approximate:
                args.append(b"~")
            args.append(_enc(minid))
        if limit is not None:
            args.extend([b"LIMIT", str(limit).encode()])
        self._batch.custom_command(args)
        return self

    def xack(self, key: Any, group: str, *ids: Any) -> Self:
        self._batch.custom_command([b"XACK", key, _enc(group), *_enc_list(ids)])
        return self

    def xclaim(
        self,
        key: Any,
        group: str,
        consumer: str,
        min_idle_time: int,
        entry_ids: Sequence[str],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
    ) -> Self:
        args: list[Any] = [
            b"XCLAIM",
            key,
            _enc(group),
            _enc(consumer),
            str(min_idle_time).encode(),
            *_enc_list(entry_ids),
        ]
        if idle is not None:
            args.extend([b"IDLE", str(idle).encode()])
        if time is not None:
            args.extend([b"TIME", str(time).encode()])
        if retrycount is not None:
            args.extend([b"RETRYCOUNT", str(retrycount).encode()])
        if force:
            args.append(b"FORCE")
        if justid:
            args.append(b"JUSTID")
        self._batch.custom_command(args)
        if justid:
            self._track(lambda r: _dec_keys(r or []))
        else:
            self._track(_decode_stream_entries)
        return self

    def xautoclaim(
        self,
        key: Any,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> Self:
        args: list[Any] = [
            b"XAUTOCLAIM",
            key,
            _enc(group),
            _enc(consumer),
            str(min_idle_time).encode(),
            _enc(start_id),
        ]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if justid:
            args.append(b"JUSTID")
        self._batch.custom_command(args)
        # Decode to redis-py shapes: a flat ID list for justid, else
        # ``[next_id, [(id, fields), ...], deleted_ids]``.
        if justid:
            self._track(lambda r: _dec_keys(r[1] or []))
        else:
            self._track(
                lambda r: [
                    _dec_str(r[0]),
                    _decode_stream_entries(r[1]),
                    _dec_keys(r[2]) if len(r) > 2 and r[2] else [],
                ],
            )
        return self

    def xgroup_create(
        self,
        key: Any,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> Self:
        args: list[Any] = [b"XGROUP", b"CREATE", key, _enc(group), _enc(entry_id)]
        if mkstream:
            args.append(b"MKSTREAM")
        if entries_read is not None:
            args.extend([b"ENTRIESREAD", str(entries_read).encode()])
        self._batch.custom_command(args)
        self._track(_ok_to_bool)
        return self

    def xgroup_destroy(self, key: Any, group: str) -> Self:
        self._batch.custom_command([b"XGROUP", b"DESTROY", key, _enc(group)])
        return self

    def xgroup_setid(self, key: Any, group: str, entry_id: str, *, entries_read: int | None = None) -> Self:
        args: list[Any] = [b"XGROUP", b"SETID", key, _enc(group), _enc(entry_id)]
        if entries_read is not None:
            args.extend([b"ENTRIESREAD", str(entries_read).encode()])
        self._batch.custom_command(args)
        self._track(_ok_to_bool)
        return self

    def xgroup_delconsumer(self, key: Any, group: str, consumer: str) -> Self:
        self._batch.custom_command([b"XGROUP", b"DELCONSUMER", key, _enc(group), _enc(consumer)])
        return self

    def xinfo_stream(self, key: Any, full: bool = False) -> Self:
        args: list[Any] = [b"XINFO", b"STREAM", key]
        if full:
            args.append(b"FULL")
        self._batch.custom_command(args)
        self._track(_decode_xinfo)
        return self

    def xinfo_groups(self, key: Any) -> Self:
        self._batch.custom_command([b"XINFO", b"GROUPS", key])
        self._track(lambda r: [_decode_xinfo(g) for g in (r or [])])
        return self

    def xinfo_consumers(self, key: Any, group: str) -> Self:
        self._batch.custom_command([b"XINFO", b"CONSUMERS", key, _enc(group)])
        self._track(lambda r: [_decode_xinfo(c) for c in (r or [])])
        return self

    # ---- raw ----
    def execute_command(self, *args: Any) -> Self:
        self._batch.custom_command(_enc_list(args))
        return self

    def __getattr__(self, name: str) -> Any:
        # RESP commands are single words, so an underscore is a lookup miss,
        # not a command: without this, ``deepcopy`` queued ``__DEEPCOPY__``.
        if "_" in name:
            raise AttributeError(name)
        cmd = name.upper()

        def call(*args: Any) -> ValkeyGlidePipelineAdapter:
            self._batch.custom_command([cmd, *_enc_list(args)])
            return self

        return call

    # ---- execution ----
    def execute(self) -> list[Any]:
        # Capture before resetting so the pipeline is reusable even if a
        # transform raises mid-decode.
        batch, post = self._batch, self._post
        self._batch = self._new_batch(atomic=batch.is_atomic)
        self._post = {}
        if not batch.commands:
            return []
        raw = self._client.exec(batch, raise_on_error=True) or []
        if not post:
            return list(raw)
        return [post[i](r) if i in post else r for i, r in enumerate(raw)]

    def reset(self) -> None:
        self._batch = self._new_batch(atomic=self._batch.is_atomic)
        self._post = {}

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class ValkeyGlideAsyncPipelineAdapter(ValkeyGlidePipelineAdapter, RespAsyncPipelineProtocol):
    """Async parallel of ``ValkeyGlidePipelineAdapter``.

    Conforms to :class:`RespAsyncPipelineProtocol` and holds a resolved
    ``AsyncGlideClient``, which is why the adapter's ``apipeline()`` is
    itself async.

    Inherits every queueing method from the sync adapter (they only
    mutate ``self._batch``) and overrides ``execute`` / ``reset`` to go
    through the async client. Don't redefine the queueing methods here:
    the sync surface already covers ``zrange(withscores=...)``,
    ``xrange(min=...)``, ``hset(mapping=...)`` and the rest, and a
    divergent copy would reject kwargs the cache layer sends.
    """

    def __init__(
        self,
        client: AsyncGlideClient,
        *,
        transaction: bool = False,
        batch_factory: Any = None,
    ) -> None:
        self._client = client
        self._new_batch = batch_factory or _new_batch
        self._batch = self._new_batch(atomic=transaction)
        self._post: dict[int, Any] = {}

    async def execute(self) -> list[Any]:  # type: ignore[override]
        # Capture before awaiting so a transform raising mid-decode doesn't
        # leave the next ``execute()`` replaying the same commands.
        batch, post = self._batch, self._post
        self._batch = self._new_batch(atomic=batch.is_atomic)
        self._post = {}
        if not batch.commands:
            return []
        raw = await self._client.exec(batch, raise_on_error=True) or []
        if not post:
            return list(raw)
        return [post[i](r) if i in post else r for i, r in enumerate(raw)]

    async def reset(self) -> None:  # type: ignore[override]
        self._batch = self._new_batch(atomic=self._batch.is_atomic)
        self._post = {}

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


# =============================================================================
# Cache client
# =============================================================================


class ValkeyGlideAdapter(RespAdapterProtocol):
    """Implements the cachex adapter surface against ``valkey-glide-sync``.

    Every operation method calls glide natively; none of the redis-py
    pool and parser machinery :class:`~django_cachex.adapters.valkey_py`
    builds on is involved.
    """

    def _batch_factory(self, *, atomic: bool = False) -> Any:
        """The batch flavor ``exec`` expects, overridden by the cluster adapter."""
        return Batch(is_atomic=atomic)

    # Matches ValkeyPyAdapter. Without it glide sends no COUNT and the server
    # default of 10 applies, which is ten times the round trips.
    _default_scan_itersize: int = 100

    # glide multiplexes: one connection carries every command of a client.
    multiplexed: bool = True

    def __init__(self, servers: list[str], **options: Any) -> None:
        _check_installed()
        if not servers:
            msg = (
                f"{type(self).__name__} requires at least one server URL. "
                f"Set the cache's LOCATION to a URL such as 'redis://127.0.0.1:6379/0' "
                f"(or a list of them, primary first)."
            )
            raise ImproperlyConfigured(msg)
        self._servers = servers
        self._options = options
        self._stampede_config: StampedeConfig | None = make_stampede_config(options.get("stampede_prevention"))
        self._config_key = _glide_config_key(servers, options)

    def resolve_stampede(self, stampede_prevention: bool | StampedeConfig | None = None) -> StampedeConfig | None:
        return resolve_stampede(self._stampede_config, stampede_prevention)

    def get_timeout_with_buffer(
        self,
        timeout: int | None,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> int | None:
        return get_timeout_with_buffer(timeout, self._stampede_config, stampede_prevention)

    # ---- client lifecycle ----
    def _client(self) -> GlideClient:
        client = _GLIDE_SYNC_CLIENTS.get(self._config_key)
        if client is not None:
            return client
        with _GLIDE_SYNC_LOCK:
            client = _GLIDE_SYNC_CLIENTS.get(self._config_key)
            if client is None:
                u = urlparse(self._servers[0])
                cfg = GlideClientConfiguration(
                    addresses=[NodeAddress(u.hostname or "localhost", u.port or 6379)],
                    **_glide_config_kwargs(self._servers, self._options, credentials_cls=ServerCredentials),
                )
                client = _WrongTypeClient(GlideClient.create(cfg))
                _GLIDE_SYNC_CLIENTS[self._config_key] = client
        return cast("GlideClient", client)

    def get_client(self, key: Any = None, *, write: bool = False) -> GlideClient:
        del key, write
        return self._client()

    @staticmethod
    def _async_registry() -> weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[tuple[Any, ...], Any]]:
        """The per-loop client registry for this topology, so ``aclose`` finds it."""
        return _GLIDE_ASYNC_CLIENTS

    @staticmethod
    def _async_locks() -> weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock]:
        """The per-loop create-locks for this topology, swept alongside the clients."""
        return _GLIDE_ASYNC_LOCKS

    @classmethod
    def _sweep_async_clients(cls) -> None:
        """Close and drop the clients of every event loop that has been closed.

        Glide's async client keeps a reference to the loop it was built on,
        which pins the ``WeakKeyDictionary`` key, so one ``asyncio.run()`` per
        request would otherwise grow the registry without bound.
        """
        registry = cls._async_registry()
        locks = cls._async_locks()
        with _GLIDE_ASYNC_REGISTRY_LOCK:
            for loop in [dead for dead in registry if dead.is_closed()]:
                for client in registry.pop(loop, {}).values():
                    # ``close()`` is a coroutine that never awaits, so one
                    # ``send`` runs it to completion; the dead loop can't.
                    closer = getattr(client, "close", None)
                    coro = closer() if closer is not None else None
                    if inspect.iscoroutine(coro):
                        with contextlib.suppress(ClosingError, StopIteration):
                            coro.send(None)
                        coro.close()
                locks.pop(loop, None)

    async def _create_async_client(self) -> Any:
        u = urlparse(self._servers[0])
        cfg = AsyncGlideClientConfiguration(
            addresses=[AsyncNodeAddress(u.hostname or "localhost", u.port or 6379)],
            **_glide_config_kwargs(self._servers, self._options, credentials_cls=AsyncServerCredentials),
        )
        return await AsyncGlideClient.create(cfg)

    async def get_async_client(self, key: Any = None, *, write: bool = False) -> AsyncGlideClient:
        """Lazy-init the async client for the running loop and config.

        ``async def`` because glide's ``AsyncGlideClient.create`` is itself
        an async constructor, unlike redis-py's where the sync helper
        gets us a Redis instance whose connection is opened on first use.
        """
        del key, write
        self._sweep_async_clients()
        registry = self._async_registry()
        locks = self._async_locks()
        loop = asyncio.get_running_loop()
        with _GLIDE_ASYNC_REGISTRY_LOCK:
            sub = registry.get(loop)
            if sub is None:
                sub = {}
                registry[loop] = sub
            lock = locks.get(loop)
            if lock is None:
                lock = asyncio.Lock()
                locks[loop] = lock
        client = sub.get(self._config_key)
        if client is not None:
            return client
        async with lock:
            client = sub.get(self._config_key)
            if client is None:
                client = _WrongTypeClient(await self._create_async_client())
                sub[self._config_key] = client
        return cast("AsyncGlideClient", client)

    def _cmd(self, args: list[Any], route: Any = None) -> Any:
        # Glide types every raw reply as one big union; each caller knows the
        # shape its own command returns.
        client: Any = self._client()
        return client.custom_command(args) if route is None else client.custom_command(args, route)

    async def _acmd(self, args: list[Any]) -> Any:
        client: Any = await self.get_async_client()
        return await client.custom_command(args)

    # =========================================================================
    # Sync core ops
    # =========================================================================

    def add(
        self,
        key: str,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> bool:
        client = self._client()
        actual_timeout = self.get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            result = client.set(
                key,
                _enc(value),
                conditional_set=ConditionalChange.ONLY_IF_DOES_NOT_EXIST,
            )
            if _ok_to_bool(result):
                client.delete([key])
                return True
            return False

        kw: dict[str, Any] = {"conditional_set": ConditionalChange.ONLY_IF_DOES_NOT_EXIST}
        if actual_timeout is not None:
            kw["expiry"] = ExpirySet(ExpiryType.SEC, actual_timeout)
        return _ok_to_bool(client.set(key, _enc(value), **kw))

    def get(self, key: str, *, stampede_prevention: bool | StampedeConfig | None = None) -> Any:
        client = self._client()
        val = client.get(key)
        if val is None:
            return None
        config = self.resolve_stampede(stampede_prevention)
        if config and isinstance(val, bytes):
            ttl = client.ttl(key)
            if ttl > 0 and should_recompute(ttl, config):
                return None
        return val

    def set(
        self,
        key: str,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> None:
        client = self._client()
        actual_timeout = self.get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            client.delete([key])
        elif actual_timeout is None:
            client.set(key, _enc(value))
        else:
            client.set(key, _enc(value), expiry=ExpirySet(ExpiryType.SEC, actual_timeout))

    def set_with_flags(
        self,
        key: str,
        value: Any,
        timeout: int | None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> bool | Any:
        client = self._client()
        actual_timeout = self.get_timeout_with_buffer(timeout, stampede_prevention)

        kw: dict[str, Any] = {}
        if nx:
            kw["conditional_set"] = ConditionalChange.ONLY_IF_DOES_NOT_EXIST
        elif xx:
            kw["conditional_set"] = ConditionalChange.ONLY_IF_EXISTS
        if get:
            kw["return_old_value"] = True

        if actual_timeout == 0:
            # timeout=0 means expire immediately: run the SET unexpired so
            # the nx/xx/get semantics still apply, then delete when it wrote.
            result = client.set(key, _enc(value), **kw)
            if get:
                executed = result is None if nx else (result is not None if xx else True)
            else:
                executed = _ok_to_bool(result)
            if executed:
                client.delete([key])
            return result if get else _ok_to_bool(result)

        if actual_timeout is not None:
            kw["expiry"] = ExpirySet(ExpiryType.SEC, actual_timeout)
        result = client.set(key, _enc(value), **kw)
        if get:
            return result
        return _ok_to_bool(result)

    def touch(self, key: str, timeout: int | None) -> bool:
        client = self._client()
        if timeout is None:
            return bool(client.persist(key))
        return bool(client.expire(key, timeout))

    def delete(self, key: str) -> bool:
        return bool(self._client().delete([key]))

    def get_many(
        self,
        keys: Iterable[str],
        *,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> dict[str, Any]:
        keys = list(keys)
        if not keys:
            return {}

        client = self._client()
        results = client.mget(list[Any](keys))
        found = {k: v for k, v in zip(keys, results, strict=False) if v is not None}

        config = self.resolve_stampede(stampede_prevention)
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

        return found

    def has_key(self, key: str) -> bool:
        return bool(self._client().exists([key]))

    def type(self, key: str) -> KeyType | None:
        result: Any = self._client().type(key)
        return _key_type(result.decode() if isinstance(result, bytes) else result)

    def incr(self, key: str, delta: int = 1) -> int:
        client = self._client()
        if delta == 1:
            return client.incr(key)
        return client.incrby(key, delta)

    def set_many(
        self,
        data: Mapping[str, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> list[Any]:
        if not data:
            return []
        client = self._client()
        prepared: dict[Any, Any] = {k: _enc(v) for k, v in data.items()}
        actual_timeout = self.get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            client.delete(list(prepared.keys()))
        elif actual_timeout is None:
            client.mset(prepared)
        else:
            # SET PX per key: MSET plus N EXPIREs would strand keys with no TTL
            # if the batch broke partway.
            expiry = ExpirySet(ExpiryType.MILLSEC, int(actual_timeout * 1000))
            batch = self._batch_factory(atomic=False)
            for key, value in prepared.items():
                batch.set(key, value, expiry=expiry)
            client.exec(batch, raise_on_error=True)
        return []

    def delete_many(self, keys: Sequence[str]) -> int:
        if not keys:
            return 0
        return self._client().delete(list(keys))

    def clear(self) -> bool:
        return self._client().flushdb(FlushMode.SYNC) == "OK"

    def close(self, **kwargs: Any) -> None:
        """Reap async clients whose event loop has been closed.

        Django fires ``cache.close()`` on every ``request_finished`` signal, so
        the sync client stays open (tearing it down would force a reconnect per
        request). Only the per-loop async clients nothing can await any more go.
        """
        del kwargs
        self._sweep_async_clients()

    # ---- TTL ----
    def ttl(self, key: str) -> int | None:
        return _normalize_ttl(self._client().ttl(key))

    def pttl(self, key: str) -> int | None:
        return _normalize_ttl(self._client().pttl(key))

    def persist(self, key: str) -> bool:
        return bool(self._client().persist(key))

    def expire(self, key: str, timeout: int | datetime.timedelta) -> bool:
        return bool(self._client().expire(key, _expire_arg(timeout)))

    def pexpire(self, key: str, timeout: int | datetime.timedelta) -> bool:
        return bool(self._client().pexpire(key, _expire_arg(timeout, milliseconds=True)))

    def expireat(self, key: str, when: int | datetime.datetime) -> bool:
        return bool(self._client().expireat(key, _to_unix(when)))

    def pexpireat(self, key: str, when: int | datetime.datetime) -> bool:
        return bool(self._client().pexpireat(key, _to_unix(when, milliseconds=True)))

    def expiretime(self, key: str) -> int | None:
        return _normalize_ttl(self._client().expiretime(key))

    def rename(self, src: str, dst: str) -> bool:
        try:
            return self._client().rename(src, dst) == "OK"
        except RequestError as exc:
            if "no such key" in str(exc).lower():
                raise KeyNotFoundError(src) from exc
            raise

    def renamenx(self, src: str, dst: str) -> bool:
        try:
            return bool(self._client().renamenx(src, dst))
        except RequestError as exc:
            if "no such key" in str(exc).lower():
                return False
            raise

    # ---- scan / keys ----
    def keys(self, pattern: str = "*") -> list[str]:
        result = self._cmd([b"KEYS", _enc(pattern)])
        return _dec_keys(result) if result else []

    def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        if count is None:
            count = self._default_scan_itersize
        result = self._client().scan(_enc(cursor), match=match, count=count, type=_object_type(_type))
        return int(_dec_str(result[0])), _dec_keys(result[1])

    def iter_keys(self, pattern: str, itersize: int | None = None) -> Iterator[str]:
        client = self._client()
        if itersize is None:
            itersize = self._default_scan_itersize
        cursor: Any = b"0"
        while True:
            result = client.scan(cursor, match=pattern, count=itersize)
            cursor, keys = cast("Any", result[0]), result[1]
            for k in keys:
                yield _dec_str(k)
            if cursor in (b"0", "0", 0):
                return

    def delete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        client = self._client()
        if itersize is None:
            itersize = self._default_scan_itersize
        deleted = 0
        for batch_keys in batched(self.iter_keys(pattern, itersize=itersize), itersize, strict=False):
            deleted += client.delete(list(batch_keys))
        return deleted

    # =========================================================================
    # Sync hashes
    # =========================================================================

    def hset(
        self,
        key: str,
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
            msg = "hset requires at least one field/value pair"
            raise ValueError(msg)
        return client.hset(key, m)

    def hsetnx(self, key: str, field: str, value: Any) -> bool:
        return self._client().hsetnx(key, field, _enc(value))

    def hget(self, key: str, field: str) -> Any | None:
        return self._client().hget(key, field)

    def hmget(self, key: str, *fields: str) -> list[Any]:
        if len(fields) == 1 and isinstance(fields[0], (list, tuple)):
            fields = tuple(fields[0])
        if not fields:
            # ``HMGET key`` with no fields is a syntax error on the wire.
            return []
        return list(self._client().hmget(key, list(fields)))

    def hgetall(self, key: str) -> dict[str, Any]:
        result = self._client().hgetall(key)
        return {k.decode() if isinstance(k, bytes) else k: v for k, v in result.items()}

    def hkeys(self, key: str) -> list[str]:
        return [k.decode() if isinstance(k, bytes) else k for k in self._client().hkeys(key)]

    def hvals(self, key: str) -> list[Any]:
        return list(self._client().hvals(key))

    def hlen(self, key: str) -> int:
        return self._client().hlen(key)

    def hexists(self, key: str, field: str) -> bool:
        return bool(self._client().hexists(key, field))

    def hdel(self, key: str, *fields: str) -> int:
        return self._client().hdel(key, list(fields))

    def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        return self._client().hincrby(key, field, amount)

    def hincrbyfloat(self, key: str, field: str, amount: float = 1.0) -> float:
        return self._client().hincrbyfloat(key, field, amount)

    # =========================================================================
    # Sync sets
    # =========================================================================

    def sadd(self, key: str, *members: Any) -> int:
        return self._client().sadd(key, [_enc(m) for m in members])

    def srem(self, key: str, *members: Any) -> int:
        return self._client().srem(key, [_enc(m) for m in members])

    def smembers(self, key: str) -> _set[Any]:
        return set(self._client().smembers(key))

    def sismember(self, key: str, member: Any) -> bool:
        return bool(self._client().sismember(key, _enc(member)))

    def smismember(self, key: str, *members: Any) -> list[bool]:
        return list(self._client().smismember(key, [_enc(m) for m in members]))

    def scard(self, key: str) -> int:
        return self._client().scard(key)

    def spop(self, key: str, count: int | None = None) -> Any:
        client = self._client()
        if count is None:
            return client.spop(key)
        return list(client.spop_count(key, count))

    def srandmember(self, key: str, count: int | None = None) -> Any:
        client = self._client()
        if count is None:
            return client.srandmember(key)
        return list(client.srandmember_count(key, count))

    def smove(self, src: str, dst: str, member: Any) -> bool:
        return bool(self._client().smove(src, dst, _enc(member)))

    def sinter(self, keys: Sequence[str]) -> _set[Any]:
        return set(self._client().sinter(list(keys)))

    def sunion(self, keys: Sequence[str]) -> _set[Any]:
        return set(self._client().sunion(list(keys)))

    def sdiff(self, keys: Sequence[str]) -> _set[Any]:
        return set(self._client().sdiff(list(keys)))

    def sinterstore(self, dst: str, keys: Sequence[str]) -> int:
        return self._client().sinterstore(dst, list(keys))

    def sunionstore(self, dst: str, keys: Sequence[str]) -> int:
        return self._client().sunionstore(dst, list(keys))

    def sdiffstore(self, dst: str, keys: Sequence[str]) -> int:
        return self._client().sdiffstore(dst, list(keys))

    def sscan(
        self,
        key: str,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, _set[Any]]:
        result = self._client().sscan(key, _enc(cursor), match=match, count=count)
        return int(_dec_str(result[0])), set(result[1])

    def sscan_iter(self, key: str, match: str | None = None, count: int | None = None) -> Iterator[bytes]:
        client = self._client()
        cursor: Any = b"0"
        while True:
            result = client.sscan(key, cursor, match=match, count=count)
            cursor, members = cast("Any", result[0]), result[1]
            yield from cast("list[bytes]", members)
            if cursor in (b"0", "0", 0):
                return

    # =========================================================================
    # Sync sorted sets
    # =========================================================================

    def zadd(self, key: str, mapping: Mapping[Any, float], **kwargs: Any) -> int:
        client = self._client()
        if kwargs:
            args: list[Any] = [b"ZADD", key]
            if kwargs.get("nx"):
                args.append(b"NX")
            elif kwargs.get("xx"):
                args.append(b"XX")
            if kwargs.get("gt"):
                args.append(b"GT")
            elif kwargs.get("lt"):
                args.append(b"LT")
            if kwargs.get("ch"):
                args.append(b"CH")
            if kwargs.get("incr"):
                args.append(b"INCR")
            for member, score in mapping.items():
                args.extend([_enc(score), _enc(member)])
            return self._cmd(args)
        return client.zadd(key, {_enc(m): float(s) for m, s in mapping.items()})

    def zrem(self, key: str, *members: Any) -> int:
        return self._client().zrem(key, [_enc(m) for m in members])

    def zscore(self, key: str, member: Any) -> float | None:
        return self._client().zscore(key, _enc(member))

    def zmscore(self, key: str, *members: Any) -> list[float | None]:
        return list(self._client().zmscore(key, [_enc(m) for m in members]))

    def zrank(self, key: str, member: Any) -> int | None:
        return self._client().zrank(key, _enc(member))

    def zrevrank(self, key: str, member: Any) -> int | None:
        return self._client().zrevrank(key, _enc(member))

    def zincrby(self, key: str, amount: float, member: Any) -> float:
        return self._client().zincrby(key, amount, _enc(member))

    def zremrangebyrank(self, key: str, start: int, end: int) -> int:
        return self._client().zremrangebyrank(key, start, end)

    def zremrangebyscore(self, key: str, mn: Any, mx: Any) -> int:
        return self._cmd([b"ZREMRANGEBYSCORE", key, _enc(mn), _enc(mx)])

    def zcard(self, key: str) -> int:
        return self._client().zcard(key)

    def zcount(self, key: str, mn: Any, mx: Any) -> int:
        return self._cmd([b"ZCOUNT", key, _enc(mn), _enc(mx)])

    def zrange(
        self,
        key: str,
        start: int,
        end: int,
        withscores: bool = False,
        desc: bool = False,
    ) -> list[Any]:
        args = [b"ZRANGE", key, str(start).encode(), str(end).encode()]
        if desc:
            args.append(b"REV")
        if withscores:
            args.append(b"WITHSCORES")
        result = self._cmd(args)
        return _decode_zrange(result, withscores=withscores)

    def zrevrange(
        self,
        key: str,
        start: int,
        end: int,
        withscores: bool = False,
    ) -> list[Any]:
        return self.zrange(key, start, end, withscores=withscores, desc=True)

    def zrangebyscore(
        self,
        key: str,
        mn: Any,
        mx: Any,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
    ) -> list[Any]:
        args = [b"ZRANGEBYSCORE", key, _enc(mn), _enc(mx)]
        if withscores:
            args.append(b"WITHSCORES")
        if start is not None and num is not None:
            args.extend([b"LIMIT", str(start).encode(), str(num).encode()])
        return _decode_zrange(self._cmd(args), withscores=withscores)

    def zrevrangebyscore(
        self,
        key: str,
        mx: Any,
        mn: Any,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
    ) -> list[Any]:
        args = [b"ZREVRANGEBYSCORE", key, _enc(mx), _enc(mn)]
        if withscores:
            args.append(b"WITHSCORES")
        if start is not None and num is not None:
            args.extend([b"LIMIT", str(start).encode(), str(num).encode()])
        return _decode_zrange(self._cmd(args), withscores=withscores)

    def zpopmin(self, key: str, count: int | None = None) -> list[tuple[Any, float]]:
        if count is None:
            count = 1
        return _decode_zpop(self._client().zpopmin(key, count))

    def zpopmax(self, key: str, count: int | None = None) -> list[tuple[Any, float]]:
        if count is None:
            count = 1
        return _decode_zpop(self._client().zpopmax(key, count))

    # =========================================================================
    # Sync lists
    # =========================================================================

    def lpush(self, key: str, *values: Any) -> int:
        return self._client().lpush(key, [_enc(v) for v in values])

    def rpush(self, key: str, *values: Any) -> int:
        return self._client().rpush(key, [_enc(v) for v in values])

    def lpop(self, key: str, count: int | None = None) -> Any:
        client = self._client()
        if count is None:
            return client.lpop(key)
        result = client.lpop_count(key, count)
        # A nil reply means the key is missing; keep it distinct from an empty
        # array so the cache layer can tell a miss from an empty pop.
        return list(result) if result is not None else None

    def rpop(self, key: str, count: int | None = None) -> Any:
        client = self._client()
        if count is None:
            return client.rpop(key)
        result = client.rpop_count(key, count)
        return list(result) if result is not None else None

    def lrange(self, key: str, start: int, end: int) -> list[Any]:
        return list(self._client().lrange(key, start, end))

    def ltrim(self, key: str, start: int, end: int) -> bool:
        return self._client().ltrim(key, start, end) == "OK"

    def llen(self, key: str) -> int:
        return self._client().llen(key)

    def lindex(self, key: str, index: int) -> Any:
        return self._client().lindex(key, index)

    def lset(self, key: str, index: int, value: Any) -> bool:
        return self._client().lset(key, index, _enc(value)) == "OK"

    def lrem(self, key: str, count: int, value: Any) -> int:
        return self._client().lrem(key, count, _enc(value))

    def linsert(self, key: str, where: str, pivot: Any, value: Any) -> int:
        return self._cmd(
            [
                b"LINSERT",
                key,
                _enc(where.upper()),
                _enc(pivot),
                _enc(value),
            ],
        )

    def lpos(
        self,
        key: str,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> Any:
        args: list[Any] = [b"LPOS", key, _enc(value)]
        if rank is not None:
            args.extend([b"RANK", str(rank).encode()])
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if maxlen is not None:
            args.extend([b"MAXLEN", str(maxlen).encode()])
        return self._cmd(args)

    def lmove(self, src: str, dst: str, wherefrom: str, whereto: str) -> Any:
        return self._cmd([b"LMOVE", src, dst, _enc(wherefrom.upper()), _enc(whereto.upper())])

    def blmove(self, src: str, dst: str, timeout: float, wherefrom: str = "LEFT", whereto: str = "RIGHT") -> Any:
        return self._cmd(
            [
                b"BLMOVE",
                src,
                dst,
                _enc(wherefrom.upper()),
                _enc(whereto.upper()),
                str(timeout).encode(),
            ],
        )

    def blpop(self, keys: Any, timeout: float = 0) -> Any:
        ks = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        result = self._cmd([b"BLPOP", *_enc_list(ks), str(timeout).encode()])
        if result is None:
            return None
        # Server returns [key, value]; cache layer expects (key: str, value: bytes).
        key, value = result[0], result[1]
        return (_dec_str(key), value)

    def brpop(self, keys: Any, timeout: float = 0) -> Any:
        ks = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        result = self._cmd([b"BRPOP", *_enc_list(ks), str(timeout).encode()])
        if result is None:
            return None
        key, value = result[0], result[1]
        return (_dec_str(key), value)

    # =========================================================================
    # Sync streams, mostly via custom_command
    # =========================================================================

    def xadd(
        self,
        key: str,
        fields: Mapping[Any, Any],
        entry_id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
    ) -> str:
        args = _xadd_args(
            key,
            fields,
            entry_id,
            maxlen=maxlen,
            approximate=approximate,
            nomkstream=nomkstream,
            minid=minid,
            limit=limit,
        )
        result = self._cmd(args)
        return result.decode() if isinstance(result, bytes) else result

    def xlen(self, key: str) -> int:
        return self._client().xlen(key)

    def xrange(
        self,
        key: str,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        args = [b"XRANGE", key, _enc(start), _enc(end)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        return _decode_stream_entries(self._cmd(args))

    def xrevrange(
        self,
        key: str,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        args = [b"XREVRANGE", key, _enc(end), _enc(start)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        return _decode_stream_entries(self._cmd(args))

    def xdel(self, key: str, *ids: Any) -> int:
        return self._cmd([b"XDEL", key, *_enc_list(ids)])

    def xtrim(
        self,
        key: str,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
    ) -> int:
        args: list[Any] = [b"XTRIM", key]
        if maxlen is not None:
            args.append(b"MAXLEN")
            if approximate:
                args.append(b"~")
            args.append(str(maxlen).encode())
        elif minid is not None:
            args.append(b"MINID")
            if approximate:
                args.append(b"~")
            args.append(_enc(minid))
        if limit is not None:
            args.extend([b"LIMIT", str(limit).encode()])
        return self._cmd(args)

    def xack(self, key: str, group: str, *ids: Any) -> int:
        return self._cmd([b"XACK", key, _enc(group), *_enc_list(ids)])

    def xclaim(
        self,
        key: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        entry_ids: Sequence[str],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
    ) -> Any:
        args: list[Any] = [
            b"XCLAIM",
            key,
            _enc(group),
            _enc(consumer),
            str(min_idle_time).encode(),
            *_enc_list(entry_ids),
        ]
        if idle is not None:
            args.extend([b"IDLE", str(idle).encode()])
        if time is not None:
            args.extend([b"TIME", str(time).encode()])
        if retrycount is not None:
            args.extend([b"RETRYCOUNT", str(retrycount).encode()])
        if force:
            args.append(b"FORCE")
        if justid:
            args.append(b"JUSTID")
        result = self._cmd(args)
        if justid:
            return _dec_keys(result or [])
        return _decode_stream_entries(result)

    def xautoclaim(
        self,
        key: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> tuple[str, Any, list[str]]:
        args: list[Any] = [
            b"XAUTOCLAIM",
            key,
            _enc(group),
            _enc(consumer),
            str(min_idle_time).encode(),
            _enc(start_id),
        ]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if justid:
            args.append(b"JUSTID")
        result = self._cmd(args)
        # Server returns [next_id, entries_or_ids, deleted_ids]
        next_id = _dec_str(result[0])
        if justid:
            claimed: Any = _dec_keys(result[1] or [])
        else:
            claimed = _decode_stream_entries(result[1])
        deleted = _dec_keys(result[2]) if len(result) > 2 and result[2] else []
        return (next_id, claimed, deleted)

    def xpending(
        self,
        key: str,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        _check_xpending_args(start, end, count, consumer, idle)
        args: list[Any] = [b"XPENDING", key, _enc(group)]
        if count is not None:
            # IDLE is only valid in the extended form, between the group
            # name and the start/end/count range.
            if idle is not None:
                args.extend([b"IDLE", str(idle).encode()])
            args.extend([_enc("-" if start is None else start), _enc("+" if end is None else end)])
            args.append(str(count).encode())
            if consumer is not None:
                args.append(_enc(consumer))
        result = self._cmd(args)
        if count is not None:
            return _decode_xpending_range(result)
        return _decode_xpending_summary(result)

    def xinfo_stream(self, key: str, full: bool = False) -> Any:
        args: list[Any] = [b"XINFO", b"STREAM", key]
        if full:
            args.append(b"FULL")
        return _decode_xinfo(self._cmd(args))

    def xinfo_groups(self, key: str) -> Any:
        result = self._cmd([b"XINFO", b"GROUPS", key])
        return [_decode_xinfo(g) for g in (result or [])]

    def xinfo_consumers(self, key: str, group: str) -> Any:
        result = self._cmd([b"XINFO", b"CONSUMERS", key, _enc(group)])
        return [_decode_xinfo(c) for c in (result or [])]

    def xgroup_create(
        self,
        key: str,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> bool:
        args: list[Any] = [b"XGROUP", b"CREATE", key, _enc(group), _enc(entry_id)]
        if mkstream:
            args.append(b"MKSTREAM")
        if entries_read is not None:
            args.extend([b"ENTRIESREAD", str(entries_read).encode()])
        return self._cmd(args) == "OK"

    def xgroup_destroy(self, key: str, group: str) -> int:
        return self._cmd([b"XGROUP", b"DESTROY", key, _enc(group)])

    def xgroup_setid(
        self,
        key: str,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
    ) -> bool:
        args: list[Any] = [b"XGROUP", b"SETID", key, _enc(group), _enc(entry_id)]
        if entries_read is not None:
            args.extend([b"ENTRIESREAD", str(entries_read).encode()])
        return self._cmd(args) == "OK"

    def xgroup_delconsumer(self, key: str, group: str, consumer: str) -> int:
        return self._cmd([b"XGROUP", b"DELCONSUMER", key, _enc(group), _enc(consumer)])

    def xread(
        self,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        args: list[Any] = [b"XREAD"]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if block is not None:
            args.extend([b"BLOCK", str(block).encode()])
        args.append(b"STREAMS")
        args.extend(_enc_list(streams.keys()))
        args.extend(_enc_list(streams.values()))
        return _decode_xread(self._cmd(args))

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        args: list[Any] = [b"XREADGROUP", b"GROUP", _enc(group), _enc(consumer)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if block is not None:
            args.extend([b"BLOCK", str(block).encode()])
        if noack:
            args.append(b"NOACK")
        args.append(b"STREAMS")
        args.extend(_enc_list(streams.keys()))
        args.extend(_enc_list(streams.values()))
        return _decode_xread(self._cmd(args))

    # =========================================================================
    # Sync scripting
    # =========================================================================

    def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        return self._cmd(
            [b"EVAL", _enc(script), str(numkeys).encode(), *_enc_list(keys_and_args)],
        )

    # =========================================================================
    # Sync server
    # =========================================================================

    def _info_args(self, section: str | None) -> list[bytes | str]:
        args: list[bytes | str] = [b"INFO"]
        if section:
            args.append(_enc(section))
        return args

    def info(self, section: str | None = None) -> dict[str, Any]:
        return _parse_info(self._cmd(self._info_args(section)))

    def slowlog_len(self) -> int:
        return self._cmd([b"SLOWLOG", b"LEN"])

    def slowlog_get(self, count: int = 10) -> list[dict[str, Any]]:
        raw = self._cmd([b"SLOWLOG", b"GET", str(count).encode()])
        # Reshape each row into the dict form the other adapters return.
        return [
            {
                "id": entry[0],
                "start_time": entry[1],
                "duration": entry[2],
                "command": [_dec_str(arg) for arg in (entry[3] or [])],
                "client_address": _dec_str(entry[4]) if len(entry) > 4 else None,
                "client_name": _dec_str(entry[5]) if len(entry) > 5 else None,
            }
            for entry in (raw or [])
            if isinstance(entry, (list, tuple)) and len(entry) >= 4
        ]

    # =========================================================================
    # Sync lock. Bespoke SET NX PX + Lua release, see ``_GlideLock``
    # =========================================================================

    def lock(
        self,
        key: str,
        lease: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        return _GlideLock(
            self._client(),
            key,
            lease=lease,
            sleep=sleep,
            blocking=blocking,
            timeout=timeout,
            thread_local=thread_local,
        )

    # =========================================================================
    # Sync pipeline
    # =========================================================================

    def _pipeline(self, *, transaction: bool = False) -> ValkeyGlidePipelineAdapter:
        return ValkeyGlidePipelineAdapter(self._client(), transaction=transaction, batch_factory=self._batch_factory)

    def pipeline(self, *, transaction: bool = True) -> ValkeyGlidePipelineAdapter:
        return self._pipeline(transaction=transaction)

    async def apipeline(self, *, transaction: bool = True) -> ValkeyGlideAsyncPipelineAdapter:
        client = await self.get_async_client()
        return ValkeyGlideAsyncPipelineAdapter(client, transaction=transaction, batch_factory=self._batch_factory)

    # =========================================================================
    # Async core ops
    # =========================================================================

    async def aadd(
        self,
        key: str,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> bool:
        client = await self.get_async_client()
        actual_timeout = self.get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            result = await client.set(
                key,
                _enc(value),
                conditional_set=ConditionalChange.ONLY_IF_DOES_NOT_EXIST,
            )
            if _ok_to_bool(result):
                await client.delete([key])
                return True
            return False

        kw: dict[str, Any] = {"conditional_set": ConditionalChange.ONLY_IF_DOES_NOT_EXIST}
        if actual_timeout is not None:
            kw["expiry"] = ExpirySet(ExpiryType.SEC, actual_timeout)
        return _ok_to_bool(await client.set(key, _enc(value), **kw))

    async def aget(self, key: str, *, stampede_prevention: bool | StampedeConfig | None = None) -> Any:
        client = await self.get_async_client()
        val = await client.get(key)
        if val is None:
            return None
        config = self.resolve_stampede(stampede_prevention)
        if config and isinstance(val, bytes):
            ttl = await client.ttl(key)
            if ttl > 0 and should_recompute(ttl, config):
                return None
        return val

    async def aset(
        self,
        key: str,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> None:
        client = await self.get_async_client()
        actual_timeout = self.get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            await client.delete([key])
        elif actual_timeout is None:
            await client.set(key, _enc(value))
        else:
            await client.set(key, _enc(value), expiry=ExpirySet(ExpiryType.SEC, actual_timeout))

    async def aset_with_flags(
        self,
        key: str,
        value: Any,
        timeout: int | None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> bool | Any:
        client = await self.get_async_client()
        actual_timeout = self.get_timeout_with_buffer(timeout, stampede_prevention)

        kw: dict[str, Any] = {}
        if nx:
            kw["conditional_set"] = ConditionalChange.ONLY_IF_DOES_NOT_EXIST
        elif xx:
            kw["conditional_set"] = ConditionalChange.ONLY_IF_EXISTS
        if get:
            kw["return_old_value"] = True

        if actual_timeout == 0:
            # timeout=0 means expire immediately: run the SET unexpired so
            # the nx/xx/get semantics still apply, then delete when it wrote.
            result = await client.set(key, _enc(value), **kw)
            if get:
                executed = result is None if nx else (result is not None if xx else True)
            else:
                executed = _ok_to_bool(result)
            if executed:
                await client.delete([key])
            return result if get else _ok_to_bool(result)

        if actual_timeout is not None:
            kw["expiry"] = ExpirySet(ExpiryType.SEC, actual_timeout)
        result = await client.set(key, _enc(value), **kw)
        if get:
            return result
        return _ok_to_bool(result)

    async def atouch(self, key: str, timeout: int | None) -> bool:
        client = await self.get_async_client()
        if timeout is None:
            return bool(await client.persist(key))
        return bool(await client.expire(key, timeout))

    async def adelete(self, key: str) -> bool:
        return bool(await (await self.get_async_client()).delete([key]))

    async def aget_many(
        self,
        keys: Iterable[str],
        *,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> dict[str, Any]:
        keys = list(keys)
        if not keys:
            return {}

        client = await self.get_async_client()
        results = await client.mget(list[Any](keys))
        found = {k: v for k, v in zip(keys, results, strict=False) if v is not None}

        config = self.resolve_stampede(stampede_prevention)
        if config and found:
            stampede_keys = [k for k, v in found.items() if isinstance(v, bytes)]
            if stampede_keys:
                batch = self._batch_factory(atomic=False)
                for k in stampede_keys:
                    batch.ttl(k)
                ttls = await client.exec(batch, raise_on_error=True) or []
                for k, ttl in zip(stampede_keys, ttls, strict=False):
                    if isinstance(ttl, int) and ttl > 0 and should_recompute(ttl, config):
                        del found[k]

        return found

    async def ahas_key(self, key: str) -> bool:
        return bool(await (await self.get_async_client()).exists([key]))

    async def atype(self, key: str) -> KeyType | None:
        result: Any = await (await self.get_async_client()).type(key)
        return _key_type(result.decode() if isinstance(result, bytes) else result)

    async def aincr(self, key: str, delta: int = 1) -> int:
        client = await self.get_async_client()
        if delta == 1:
            return await client.incr(key)
        return await client.incrby(key, delta)

    async def aset_many(
        self,
        data: Mapping[str, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | StampedeConfig | None = None,
    ) -> list[Any]:
        if not data:
            return []
        client = await self.get_async_client()
        prepared: dict[Any, Any] = {k: _enc(v) for k, v in data.items()}
        actual_timeout = self.get_timeout_with_buffer(timeout, stampede_prevention)

        if actual_timeout == 0:
            await client.delete(list(prepared.keys()))
        elif actual_timeout is None:
            await client.mset(prepared)
        else:
            # SET ... PX per key: see the note in the sync ``set_many``.
            expiry = ExpirySet(ExpiryType.MILLSEC, int(actual_timeout * 1000))
            batch = self._batch_factory(atomic=False)
            for key, value in prepared.items():
                batch.set(key, value, expiry=expiry)
            await client.exec(batch, raise_on_error=True)
        return []

    async def adelete_many(self, keys: Sequence[str]) -> int:
        if not keys:
            return 0
        return await (await self.get_async_client()).delete(list(keys))

    async def aclear(self) -> bool:
        return (await (await self.get_async_client()).flushdb(FlushMode.SYNC)) == "OK"

    async def aclose(self, **kwargs: Any) -> None:
        """Close and drop this config's async client for the running loop."""
        # Nothing fires this implicitly and glide clients have no ``__del__``,
        # so a loop discarded without it strands a socket and a Rust handle.
        del kwargs
        self._sweep_async_clients()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        with _GLIDE_ASYNC_REGISTRY_LOCK:
            sub = self._async_registry().get(loop)
            client = sub.pop(self._config_key, None) if sub else None
        if client is not None:
            await _aclose_glide_client(client)

    # ---- Async TTL ----
    async def attl(self, key: str) -> int | None:
        return _normalize_ttl(await (await self.get_async_client()).ttl(key))

    async def apttl(self, key: str) -> int | None:
        return _normalize_ttl(await (await self.get_async_client()).pttl(key))

    async def apersist(self, key: str) -> bool:
        return bool(await (await self.get_async_client()).persist(key))

    async def aexpire(self, key: str, timeout: int | datetime.timedelta) -> bool:
        return bool(await (await self.get_async_client()).expire(key, _expire_arg(timeout)))

    async def apexpire(self, key: str, timeout: int | datetime.timedelta) -> bool:
        return bool(await (await self.get_async_client()).pexpire(key, _expire_arg(timeout, milliseconds=True)))

    async def aexpireat(self, key: str, when: int | datetime.datetime) -> bool:
        return bool(await (await self.get_async_client()).expireat(key, _to_unix(when)))

    async def apexpireat(self, key: str, when: int | datetime.datetime) -> bool:
        return bool(await (await self.get_async_client()).pexpireat(key, _to_unix(when, milliseconds=True)))

    async def aexpiretime(self, key: str) -> int | None:
        return _normalize_ttl(await (await self.get_async_client()).expiretime(key))

    async def arename(self, src: str, dst: str) -> bool:
        try:
            return (await (await self.get_async_client()).rename(src, dst)) == "OK"
        except RequestError as exc:
            if "no such key" in str(exc).lower():
                raise KeyNotFoundError(src) from exc
            raise

    async def arenamenx(self, src: str, dst: str) -> bool:
        try:
            return bool(await (await self.get_async_client()).renamenx(src, dst))
        except RequestError as exc:
            if "no such key" in str(exc).lower():
                return False
            raise

    # ---- Async scan ----
    async def akeys(self, pattern: str = "*") -> list[str]:
        result = await self._acmd([b"KEYS", _enc(pattern)])
        return _dec_keys(result) if result else []

    async def ascan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        client = await self.get_async_client()
        if count is None:
            count = self._default_scan_itersize
        result = await client.scan(_enc(cursor), match=match, count=count, type=_object_type(_type))
        return int(_dec_str(result[0])), _dec_keys(result[1])

    async def aiter_keys(self, pattern: str, itersize: int | None = None):
        client = await self.get_async_client()
        if itersize is None:
            itersize = self._default_scan_itersize
        cursor: Any = b"0"
        while True:
            result = await client.scan(cursor, match=pattern, count=itersize)
            cursor, keys = cast("Any", result[0]), result[1]
            for k in keys:
                yield _dec_str(k)
            if cursor in (b"0", "0", 0):
                return

    async def adelete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        client = await self.get_async_client()
        if itersize is None:
            itersize = self._default_scan_itersize
        deleted = 0
        keys: list[Any] = []
        async for k in self.aiter_keys(pattern, itersize=itersize):
            keys.append(k)
            if len(keys) >= itersize:
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
        key: str,
        field: str | None = None,
        value: Any = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        client = await self.get_async_client()
        m: dict[Any, Any] = {}
        if field is not None:
            m[field] = _enc(value)
        if mapping:
            m.update({f: _enc(v) for f, v in mapping.items()})
        if items:
            for i in range(0, len(items), 2):
                m[items[i]] = _enc(items[i + 1])
        if not m:
            msg = "hset requires at least one field/value pair"
            raise ValueError(msg)
        return await client.hset(key, m)

    async def ahsetnx(self, key: str, field: str, value: Any) -> bool:
        return await (await self.get_async_client()).hsetnx(key, field, _enc(value))

    async def ahget(self, key: str, field: str) -> Any:
        return await (await self.get_async_client()).hget(key, field)

    async def ahmget(self, key: str, *fields: str) -> list[Any]:
        if len(fields) == 1 and isinstance(fields[0], (list, tuple)):
            fields = tuple(fields[0])
        if not fields:
            # ``HMGET key`` with no fields is a syntax error on the wire.
            return []
        return list(await (await self.get_async_client()).hmget(key, list(fields)))

    async def ahgetall(self, key: str) -> dict[str, Any]:
        result = await (await self.get_async_client()).hgetall(key)
        return {k.decode() if isinstance(k, bytes) else k: v for k, v in result.items()}

    async def ahkeys(self, key: str) -> list[str]:
        return [k.decode() if isinstance(k, bytes) else k for k in await (await self.get_async_client()).hkeys(key)]

    async def ahvals(self, key: str) -> list[Any]:
        return list(await (await self.get_async_client()).hvals(key))

    async def ahlen(self, key: str) -> int:
        return await (await self.get_async_client()).hlen(key)

    async def ahexists(self, key: str, field: str) -> bool:
        return bool(await (await self.get_async_client()).hexists(key, field))

    async def ahdel(self, key: str, *fields: str) -> int:
        return await (await self.get_async_client()).hdel(key, list(fields))

    async def ahincrby(self, key: str, field: str, amount: int = 1) -> int:
        return await (await self.get_async_client()).hincrby(key, field, amount)

    async def ahincrbyfloat(self, key: str, field: str, amount: float = 1.0) -> float:
        return await (await self.get_async_client()).hincrbyfloat(key, field, amount)

    # =========================================================================
    # Async sets
    # =========================================================================

    async def asadd(self, key: str, *members: Any) -> int:
        return await (await self.get_async_client()).sadd(key, [_enc(m) for m in members])

    async def asrem(self, key: str, *members: Any) -> int:
        return await (await self.get_async_client()).srem(key, [_enc(m) for m in members])

    async def asmembers(self, key: str) -> _set[Any]:
        return set(await (await self.get_async_client()).smembers(key))

    async def asismember(self, key: str, member: Any) -> bool:
        return bool(await (await self.get_async_client()).sismember(key, _enc(member)))

    async def asmismember(self, key: str, *members: Any) -> list[bool]:
        return list(await (await self.get_async_client()).smismember(key, [_enc(m) for m in members]))

    async def ascard(self, key: str) -> int:
        return await (await self.get_async_client()).scard(key)

    async def aspop(self, key: str, count: int | None = None) -> Any:
        client = await self.get_async_client()
        if count is None:
            return await client.spop(key)
        return list(await client.spop_count(key, count))

    async def asrandmember(self, key: str, count: int | None = None) -> Any:
        client = await self.get_async_client()
        if count is None:
            return await client.srandmember(key)
        return list(await client.srandmember_count(key, count))

    async def asmove(self, src: str, dst: str, member: Any) -> bool:
        return bool(await (await self.get_async_client()).smove(src, dst, _enc(member)))

    async def asinter(self, keys: Sequence[str]) -> _set[Any]:
        return set(await (await self.get_async_client()).sinter(list(keys)))

    async def asunion(self, keys: Sequence[str]) -> _set[Any]:
        return set(await (await self.get_async_client()).sunion(list(keys)))

    async def asdiff(self, keys: Sequence[str]) -> _set[Any]:
        return set(await (await self.get_async_client()).sdiff(list(keys)))

    async def asinterstore(self, dst: str, keys: Sequence[str]) -> int:
        return await (await self.get_async_client()).sinterstore(dst, list(keys))

    async def asunionstore(self, dst: str, keys: Sequence[str]) -> int:
        return await (await self.get_async_client()).sunionstore(dst, list(keys))

    async def asdiffstore(self, dst: str, keys: Sequence[str]) -> int:
        return await (await self.get_async_client()).sdiffstore(dst, list(keys))

    async def asscan(
        self,
        key: str,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, _set[Any]]:
        result = await (await self.get_async_client()).sscan(key, _enc(cursor), match=match, count=count)
        return int(_dec_str(result[0])), set(result[1])

    async def asscan_iter(self, key: str, match: str | None = None, count: int | None = None):
        client = await self.get_async_client()
        cursor: Any = b"0"
        while True:
            result = await client.sscan(key, cursor, match=match, count=count)
            cursor, members = cast("Any", result[0]), result[1]
            for m in members:
                yield m
            if cursor in (b"0", "0", 0):
                return

    # =========================================================================
    # Async sorted sets
    # =========================================================================

    async def azadd(self, key: str, mapping: Mapping[Any, float], **kwargs: Any) -> int:
        client = await self.get_async_client()
        if kwargs:
            args: list[Any] = [b"ZADD", key]
            if kwargs.get("nx"):
                args.append(b"NX")
            elif kwargs.get("xx"):
                args.append(b"XX")
            if kwargs.get("gt"):
                args.append(b"GT")
            elif kwargs.get("lt"):
                args.append(b"LT")
            if kwargs.get("ch"):
                args.append(b"CH")
            if kwargs.get("incr"):
                args.append(b"INCR")
            for member, score in mapping.items():
                args.extend([_enc(score), _enc(member)])
            return await self._acmd(args)
        return await client.zadd(key, {_enc(m): float(s) for m, s in mapping.items()})

    async def azrem(self, key: str, *members: Any) -> int:
        return await (await self.get_async_client()).zrem(key, [_enc(m) for m in members])

    async def azscore(self, key: str, member: Any) -> float | None:
        return await (await self.get_async_client()).zscore(key, _enc(member))

    async def azmscore(self, key: str, *members: Any) -> list[float | None]:
        return list(await (await self.get_async_client()).zmscore(key, [_enc(m) for m in members]))

    async def azrank(self, key: str, member: Any) -> int | None:
        return await (await self.get_async_client()).zrank(key, _enc(member))

    async def azrevrank(self, key: str, member: Any) -> int | None:
        return await (await self.get_async_client()).zrevrank(key, _enc(member))

    async def azincrby(self, key: str, amount: float, member: Any) -> float:
        return await (await self.get_async_client()).zincrby(key, amount, _enc(member))

    async def azremrangebyrank(self, key: str, start: int, end: int) -> int:
        return await (await self.get_async_client()).zremrangebyrank(key, start, end)

    async def azremrangebyscore(self, key: str, mn: Any, mx: Any) -> int:
        return await self._acmd([b"ZREMRANGEBYSCORE", key, _enc(mn), _enc(mx)])

    async def azcard(self, key: str) -> int:
        return await (await self.get_async_client()).zcard(key)

    async def azcount(self, key: str, mn: Any, mx: Any) -> int:
        return await self._acmd([b"ZCOUNT", key, _enc(mn), _enc(mx)])

    async def azrange(
        self,
        key: str,
        start: int,
        end: int,
        withscores: bool = False,
        desc: bool = False,
    ) -> list[Any]:
        args = [b"ZRANGE", key, str(start).encode(), str(end).encode()]
        if desc:
            args.append(b"REV")
        if withscores:
            args.append(b"WITHSCORES")
        return _decode_zrange(
            await self._acmd(args),
            withscores=withscores,
        )

    async def azrevrange(
        self,
        key: str,
        start: int,
        end: int,
        withscores: bool = False,
    ) -> list[Any]:
        return await self.azrange(key, start, end, withscores=withscores, desc=True)

    async def azrangebyscore(
        self,
        key: str,
        mn: Any,
        mx: Any,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
    ) -> list[Any]:
        args = [b"ZRANGEBYSCORE", key, _enc(mn), _enc(mx)]
        if withscores:
            args.append(b"WITHSCORES")
        if start is not None and num is not None:
            args.extend([b"LIMIT", str(start).encode(), str(num).encode()])
        return _decode_zrange(
            await self._acmd(args),
            withscores=withscores,
        )

    async def azrevrangebyscore(
        self,
        key: str,
        mx: Any,
        mn: Any,
        withscores: bool = False,
        start: int | None = None,
        num: int | None = None,
    ) -> list[Any]:
        args = [b"ZREVRANGEBYSCORE", key, _enc(mx), _enc(mn)]
        if withscores:
            args.append(b"WITHSCORES")
        if start is not None and num is not None:
            args.extend([b"LIMIT", str(start).encode(), str(num).encode()])
        return _decode_zrange(
            await self._acmd(args),
            withscores=withscores,
        )

    async def azpopmin(self, key: str, count: int | None = None) -> list[tuple[Any, float]]:
        if count is None:
            count = 1
        return _decode_zpop(await (await self.get_async_client()).zpopmin(key, count))

    async def azpopmax(self, key: str, count: int | None = None) -> list[tuple[Any, float]]:
        if count is None:
            count = 1
        return _decode_zpop(await (await self.get_async_client()).zpopmax(key, count))

    # =========================================================================
    # Async lists
    # =========================================================================

    async def alpush(self, key: str, *values: Any) -> int:
        return await (await self.get_async_client()).lpush(key, [_enc(v) for v in values])

    async def arpush(self, key: str, *values: Any) -> int:
        return await (await self.get_async_client()).rpush(key, [_enc(v) for v in values])

    async def alpop(self, key: str, count: int | None = None) -> Any:
        client = await self.get_async_client()
        if count is None:
            return await client.lpop(key)
        result = await client.lpop_count(key, count)
        # See the note in the sync ``lpop``.
        return list(result) if result is not None else None

    async def arpop(self, key: str, count: int | None = None) -> Any:
        client = await self.get_async_client()
        if count is None:
            return await client.rpop(key)
        result = await client.rpop_count(key, count)
        return list(result) if result is not None else None

    async def alrange(self, key: str, start: int, end: int) -> list[Any]:
        return list(await (await self.get_async_client()).lrange(key, start, end))

    async def altrim(self, key: str, start: int, end: int) -> bool:
        return (await (await self.get_async_client()).ltrim(key, start, end)) == "OK"

    async def allen(self, key: str) -> int:
        return await (await self.get_async_client()).llen(key)

    async def alindex(self, key: str, index: int) -> Any:
        return await (await self.get_async_client()).lindex(key, index)

    async def alset(self, key: str, index: int, value: Any) -> bool:
        return (await (await self.get_async_client()).lset(key, index, _enc(value))) == "OK"

    async def alrem(self, key: str, count: int, value: Any) -> int:
        return await (await self.get_async_client()).lrem(key, count, _enc(value))

    async def alinsert(self, key: str, where: str, pivot: Any, value: Any) -> int:
        return await self._acmd(
            [
                b"LINSERT",
                key,
                _enc(where.upper()),
                _enc(pivot),
                _enc(value),
            ],
        )

    async def alpos(
        self,
        key: str,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> Any:
        args: list[Any] = [b"LPOS", key, _enc(value)]
        if rank is not None:
            args.extend([b"RANK", str(rank).encode()])
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if maxlen is not None:
            args.extend([b"MAXLEN", str(maxlen).encode()])
        return await self._acmd(args)

    async def almove(self, src: str, dst: str, wherefrom: str, whereto: str) -> Any:
        return await self._acmd(
            [b"LMOVE", src, dst, _enc(wherefrom.upper()), _enc(whereto.upper())],
        )

    async def ablmove(
        self,
        src: str,
        dst: str,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
    ) -> Any:
        return await self._acmd(
            [
                b"BLMOVE",
                src,
                dst,
                _enc(wherefrom.upper()),
                _enc(whereto.upper()),
                str(timeout).encode(),
            ],
        )

    async def ablpop(self, keys: Any, timeout: float = 0) -> Any:
        ks = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        result = await self._acmd([b"BLPOP", *_enc_list(ks), str(timeout).encode()])
        if result is None:
            return None
        key, value = result[0], result[1]
        return (_dec_str(key), value)

    async def abrpop(self, keys: Any, timeout: float = 0) -> Any:
        ks = list(keys) if isinstance(keys, (list, tuple)) else [keys]
        result = await self._acmd([b"BRPOP", *_enc_list(ks), str(timeout).encode()])
        if result is None:
            return None
        key, value = result[0], result[1]
        return (_dec_str(key), value)

    # =========================================================================
    # Async streams, mostly via custom_command
    # =========================================================================

    async def axadd(
        self,
        key: str,
        fields: Mapping[Any, Any],
        entry_id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
    ) -> str:
        args = _xadd_args(
            key,
            fields,
            entry_id,
            maxlen=maxlen,
            approximate=approximate,
            nomkstream=nomkstream,
            minid=minid,
            limit=limit,
        )
        result = await self._acmd(args)
        return result.decode() if isinstance(result, bytes) else result

    async def axlen(self, key: str) -> int:
        return await (await self.get_async_client()).xlen(key)

    async def axrange(
        self,
        key: str,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        args = [b"XRANGE", key, _enc(start), _enc(end)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        return _decode_stream_entries(await self._acmd(args))

    async def axrevrange(
        self,
        key: str,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        args = [b"XREVRANGE", key, _enc(end), _enc(start)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        return _decode_stream_entries(await self._acmd(args))

    async def axdel(self, key: str, *ids: Any) -> int:
        return await self._acmd([b"XDEL", key, *_enc_list(ids)])

    async def axtrim(
        self,
        key: str,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
    ) -> int:
        args: list[Any] = [b"XTRIM", key]
        if maxlen is not None:
            args.append(b"MAXLEN")
            if approximate:
                args.append(b"~")
            args.append(str(maxlen).encode())
        elif minid is not None:
            args.append(b"MINID")
            if approximate:
                args.append(b"~")
            args.append(_enc(minid))
        if limit is not None:
            args.extend([b"LIMIT", str(limit).encode()])
        return await self._acmd(args)

    async def axack(self, key: str, group: str, *ids: Any) -> int:
        return await self._acmd([b"XACK", key, _enc(group), *_enc_list(ids)])

    async def axclaim(
        self,
        key: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        entry_ids: Sequence[str],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
    ) -> Any:
        args: list[Any] = [
            b"XCLAIM",
            key,
            _enc(group),
            _enc(consumer),
            str(min_idle_time).encode(),
            *_enc_list(entry_ids),
        ]
        if idle is not None:
            args.extend([b"IDLE", str(idle).encode()])
        if time is not None:
            args.extend([b"TIME", str(time).encode()])
        if retrycount is not None:
            args.extend([b"RETRYCOUNT", str(retrycount).encode()])
        if force:
            args.append(b"FORCE")
        if justid:
            args.append(b"JUSTID")
        result = await self._acmd(args)
        if justid:
            return _dec_keys(result or [])
        return _decode_stream_entries(result)

    async def axautoclaim(
        self,
        key: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> tuple[str, Any, list[str]]:
        args: list[Any] = [
            b"XAUTOCLAIM",
            key,
            _enc(group),
            _enc(consumer),
            str(min_idle_time).encode(),
            _enc(start_id),
        ]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if justid:
            args.append(b"JUSTID")
        result = await self._acmd(args)
        next_id = _dec_str(result[0])
        if justid:
            claimed: Any = _dec_keys(result[1] or [])
        else:
            claimed = _decode_stream_entries(result[1])
        deleted = _dec_keys(result[2]) if len(result) > 2 and result[2] else []
        return (next_id, claimed, deleted)

    async def axpending(
        self,
        key: str,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        _check_xpending_args(start, end, count, consumer, idle)
        args: list[Any] = [b"XPENDING", key, _enc(group)]
        if count is not None:
            # IDLE is only valid in the extended form, between the group
            # name and the start/end/count range.
            if idle is not None:
                args.extend([b"IDLE", str(idle).encode()])
            args.extend([_enc("-" if start is None else start), _enc("+" if end is None else end)])
            args.append(str(count).encode())
            if consumer is not None:
                args.append(_enc(consumer))
        result = await self._acmd(args)
        if count is not None:
            return _decode_xpending_range(result)
        return _decode_xpending_summary(result)

    async def axinfo_stream(self, key: str, full: bool = False) -> Any:
        args: list[Any] = [b"XINFO", b"STREAM", key]
        if full:
            args.append(b"FULL")
        return _decode_xinfo(await self._acmd(args))

    async def axinfo_groups(self, key: str) -> Any:
        result = await self._acmd([b"XINFO", b"GROUPS", key])
        return [_decode_xinfo(g) for g in (result or [])]

    async def axinfo_consumers(self, key: str, group: str) -> Any:
        result = await self._acmd([b"XINFO", b"CONSUMERS", key, _enc(group)])
        return [_decode_xinfo(c) for c in (result or [])]

    async def axgroup_create(
        self,
        key: str,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> bool:
        args: list[Any] = [b"XGROUP", b"CREATE", key, _enc(group), _enc(entry_id)]
        if mkstream:
            args.append(b"MKSTREAM")
        if entries_read is not None:
            args.extend([b"ENTRIESREAD", str(entries_read).encode()])
        return (await self._acmd(args)) == "OK"

    async def axgroup_destroy(self, key: str, group: str) -> int:
        return await self._acmd([b"XGROUP", b"DESTROY", key, _enc(group)])

    async def axgroup_setid(
        self,
        key: str,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
    ) -> bool:
        args: list[Any] = [b"XGROUP", b"SETID", key, _enc(group), _enc(entry_id)]
        if entries_read is not None:
            args.extend([b"ENTRIESREAD", str(entries_read).encode()])
        return (await self._acmd(args)) == "OK"

    async def axgroup_delconsumer(self, key: str, group: str, consumer: str) -> int:
        return await self._acmd(
            [b"XGROUP", b"DELCONSUMER", key, _enc(group), _enc(consumer)],
        )

    async def axread(
        self,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        args: list[Any] = [b"XREAD"]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if block is not None:
            args.extend([b"BLOCK", str(block).encode()])
        args.append(b"STREAMS")
        args.extend(_enc_list(streams.keys()))
        args.extend(_enc_list(streams.values()))
        return _decode_xread(await self._acmd(args))

    async def axreadgroup(
        self,
        group: str,
        consumer: str,
        streams: Mapping[str, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        args: list[Any] = [b"XREADGROUP", b"GROUP", _enc(group), _enc(consumer)]
        if count is not None:
            args.extend([b"COUNT", str(count).encode()])
        if block is not None:
            args.extend([b"BLOCK", str(block).encode()])
        if noack:
            args.append(b"NOACK")
        args.append(b"STREAMS")
        args.extend(_enc_list(streams.keys()))
        args.extend(_enc_list(streams.values()))
        return _decode_xread(await self._acmd(args))

    # =========================================================================
    # Async eval
    # =========================================================================

    async def aeval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        return await self._acmd(
            [b"EVAL", _enc(script), str(numkeys).encode(), *_enc_list(keys_and_args)],
        )

    # =========================================================================
    # Async lock
    # =========================================================================

    async def alock(
        self,
        key: str,
        lease: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        timeout: float | None = None,
        thread_local: bool = True,
    ) -> Any:
        return _AsyncGlideLock(
            self,
            key,
            lease=lease,
            sleep=sleep,
            blocking=blocking,
            timeout=timeout,
            thread_local=thread_local,
        )


# =============================================================================
# Cluster topology
# =============================================================================
# Separate from the standalone registries above: the two client flavors take
# different config classes and must not be mixed for the same address.
_GLIDE_SYNC_CLUSTER_CLIENTS: dict[tuple[Any, ...], Any] = {}
_GLIDE_SYNC_CLUSTER_LOCK = threading.Lock()
_GLIDE_ASYNC_CLUSTER_CLIENTS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[tuple[Any, ...], Any]] = (
    weakref.WeakKeyDictionary()
)
_GLIDE_ASYNC_CLUSTER_LOCKS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
    weakref.WeakKeyDictionary()
)


class ValkeyGlideClusterAdapter(ValkeyGlideAdapter):
    """Cluster-mode adapter, wraps ``GlideClusterClient`` instead of standalone.

    Inherits the full command surface from :class:`ValkeyGlideAdapter`.
    Only the client-construction hooks and SCAN change. Multi-key operations
    must hash to a single slot, use ``{tag}`` hash tags on related keys
    (matches :class:`~django_cachex.cache.resp.RespClusterCache` semantics
    for the other drivers).
    """

    def _batch_factory(self, *, atomic: bool = False) -> Any:
        return ClusterBatch(is_atomic=atomic)

    def pipeline(self, *, transaction: bool = True) -> ValkeyGlidePipelineAdapter:
        """Cluster pipelines can't be atomic across slots, force non-atomic batches."""
        del transaction
        return ValkeyGlidePipelineAdapter(self._client(), transaction=False, batch_factory=self._batch_factory)

    async def apipeline(self, *, transaction: bool = True) -> ValkeyGlideAsyncPipelineAdapter:
        """Async cluster pipelines can't be atomic across slots."""
        del transaction
        client = await self.get_async_client()
        return ValkeyGlideAsyncPipelineAdapter(client, transaction=False, batch_factory=self._batch_factory)

    def info(self, section: str | None = None) -> dict[str, Any]:
        """Ask one node for INFO instead of letting glide fan the command out."""
        # Unrouted, glide fans INFO out to all primaries and returns
        # ``{node_address: payload}`` instead of the single body we parse.
        return _parse_info(self._cmd(self._info_args(section), RandomNode()))

    # A ``ClusterScanCursor`` can't round-trip through the protocol's int cursor,
    # so drive the loop here and report one finished scan.

    def _scan_keys(self, match: str | None, count: int | None, _type: str | None) -> Iterator[str]:
        client = self._client()
        if count is None:
            count = self._default_scan_itersize
        cursor = ClusterScanCursor()
        object_type = _object_type(_type)
        while not cursor.is_finished():
            cursor, keys = client.scan(cursor, match=match, count=count, type=object_type)
            yield from _dec_keys(keys)

    def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        del cursor  # Only ever 0: the previous call consumed the whole keyspace.
        return 0, list(self._scan_keys(match, count, _type))

    def iter_keys(self, pattern: str, itersize: int | None = None) -> Iterator[str]:
        return self._scan_keys(pattern, itersize, None)

    async def _ascan_keys(self, match: str | None, count: int | None, _type: str | None):
        client: Any = await self.get_async_client()
        if count is None:
            count = self._default_scan_itersize
        cursor = AsyncClusterScanCursor()
        object_type = _object_type(_type)
        while not cursor.is_finished():
            cursor, keys = await client.scan(cursor, match=match, count=count, type=object_type)
            for key in keys:
                yield _dec_str(key)

    async def ascan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        del cursor
        return 0, [key async for key in self._ascan_keys(match, count, _type)]

    async def aiter_keys(self, pattern: str, itersize: int | None = None):
        async for key in self._ascan_keys(pattern, itersize, None):
            yield key

    def _client(self) -> Any:
        client = _GLIDE_SYNC_CLUSTER_CLIENTS.get(self._config_key)
        if client is not None:
            return client
        with _GLIDE_SYNC_CLUSTER_LOCK:
            client = _GLIDE_SYNC_CLUSTER_CLIENTS.get(self._config_key)
            if client is None:
                cfg = GlideClusterClientConfiguration(
                    addresses=self._cluster_addresses(),
                    **_glide_config_kwargs(
                        self._servers,
                        self._options,
                        credentials_cls=ServerCredentials,
                        include_database=False,
                    ),
                )
                client = _WrongTypeClient(GlideClusterClient.create(cfg))
                _GLIDE_SYNC_CLUSTER_CLIENTS[self._config_key] = client
        return client

    @staticmethod
    def _async_registry() -> weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[tuple[Any, ...], Any]]:
        return _GLIDE_ASYNC_CLUSTER_CLIENTS

    @staticmethod
    def _async_locks() -> weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock]:
        return _GLIDE_ASYNC_CLUSTER_LOCKS

    async def _create_async_client(self) -> Any:
        cfg = AsyncGlideClusterClientConfiguration(
            addresses=self._cluster_addresses_async(),
            **_glide_config_kwargs(
                self._servers,
                self._options,
                credentials_cls=AsyncServerCredentials,
                include_database=False,
            ),
        )
        return await AsyncGlideClusterClient.create(cfg)

    def _cluster_addresses(self) -> list[NodeAddress]:
        out: list[NodeAddress] = []
        for raw in self._servers:
            u = urlparse(raw)
            out.append(NodeAddress(u.hostname or "localhost", u.port or 6379))
        return out

    def _cluster_addresses_async(self) -> list[AsyncNodeAddress]:
        out: list[AsyncNodeAddress] = []
        for raw in self._servers:
            u = urlparse(raw)
            out.append(AsyncNodeAddress(u.hostname or "localhost", u.port or 6379))
        return out


# =============================================================================
# Helpers
# =============================================================================


def _decode_zrange(result: Any, *, withscores: bool) -> list[Any]:
    # Glide's shape varies by command: ZRANGE WITHSCORES gives a dict,
    # ZRANGEBYSCORE WITHSCORES gives a list of pairs.
    if not result:
        return []
    if not withscores:
        return list(result)
    if isinstance(result, dict):
        return [(m, float(s)) for m, s in result.items()]
    return [(item[0], float(item[1])) for item in result]


def _decode_zpop(result: Any) -> list[tuple[Any, float]]:
    """Decode ZPOPMIN/ZPOPMAX result into [(member, score), ...]."""
    if not result:
        return []
    if isinstance(result, dict):
        return [(m, float(s)) for m, s in result.items()]
    # list shape
    return [(m, float(s)) for m, s in result]


def _parse_info(raw: Any) -> dict[str, Any]:
    """Parse INFO output into a dict (mirrors redis-py's ``parse_info``)."""
    # A fanned-out cluster INFO answers ``{node_address: payload}``; flatten it
    # so callers see the same shape the standalone path returns.
    if isinstance(raw, dict):
        merged: dict[str, Any] = {}
        for payload in raw.values():
            merged.update(_parse_info(payload))
        return merged
    if isinstance(raw, (bytes, bytearray)):
        raw = bytes(raw).decode("utf-8", errors="replace")
    out: dict[str, Any] = {}
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[k] = _coerce_info_value(v)
    return out


def _coerce_info_value(v: str) -> Any:
    """Coerce one INFO value the way redis-py's ``parse_info`` does."""
    # Nested rows (``db0:keys=12,expires=3``) become dicts for the admin's
    # Keyspace panel; ``int`` is tried first so ``-1`` stays an ``int``.
    if "," in v and "=" in v:
        sub: dict[str, Any] = {}
        for item in v.split(","):
            if "=" not in item:
                continue
            k, val = item.rsplit("=", 1)
            sub[k] = _coerce_info_value(val)
        return sub
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        return v


# =============================================================================
# Locks (minimal SET NX PX + Lua release implementation)
# =============================================================================


_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

# Extend atomically, only while still owned by this token: adds ``ARGV[2]``
# seconds, or replaces the TTL with it. Returns 1 on success, 0 if not owned.
_EXTEND_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
if ARGV[3] == '1' then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then ttl = 0 end
return redis.call('PEXPIRE', KEYS[1], ttl + tonumber(ARGV[2]))
"""


class _GlideLock:
    """Sync distributed lock backed by SET NX PX + Lua release."""

    def __init__(
        self,
        client: GlideClient,
        key: str,
        *,
        lease: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        timeout: float | None = None,
        thread_local: bool = True,
    ) -> None:
        self._client = client
        self._key = key
        self._lease = lease
        self._sleep = sleep
        self._blocking = blocking
        self._timeout = timeout
        self._token_local: threading.local | None = threading.local() if thread_local else None
        self._token_shared: bytes | None = None

    @property
    def _token(self) -> bytes | None:
        if self._token_local is not None:
            return getattr(self._token_local, "token", None)
        return self._token_shared

    @_token.setter
    def _token(self, value: bytes | None) -> None:
        if self._token_local is not None:
            if value is None:
                if hasattr(self._token_local, "token"):
                    del self._token_local.token
            else:
                self._token_local.token = value
        else:
            self._token_shared = value

    def acquire(self, *, blocking: bool | None = None, timeout: float | None = None) -> bool:
        bl = self._blocking if blocking is None else blocking
        bt = self._timeout if timeout is None else timeout
        deadline = time.monotonic() + bt if bt is not None else None

        kw: dict[str, Any] = {"conditional_set": ConditionalChange.ONLY_IF_DOES_NOT_EXIST}
        if self._lease is not None:
            kw["expiry"] = ExpirySet(ExpiryType.MILLSEC, int(self._lease * 1000))

        while True:
            # Fresh token per attempt: a reused token would let a stale
            # holder release/extend a lock re-acquired by someone else.
            token = os.urandom(16).hex().encode()
            result = self._client.set(self._key, token, **kw)
            if _ok_to_bool(result):
                self._token = token
                return True
            if not bl:
                return False
            if deadline is None:
                time.sleep(self._sleep)
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(self._sleep, remaining))

    def release(self) -> None:
        from django_cachex.lock import LockError, LockNotOwnedError

        if self._token is None:
            msg = "Cannot release un-acquired lock"
            raise LockError(msg)
        result = self._client.custom_command(
            [b"EVAL", _RELEASE_LUA.encode(), b"1", _enc(self._key), self._token],
        )
        self._token = None
        if not result:
            msg = "Cannot release a lock that's no longer owned"
            raise LockNotOwnedError(msg)

    def extend(self, additional_time: float, *, replace_ttl: bool = False) -> bool:
        """Extend the lock's TTL by ``additional_time`` seconds (or replace it)."""
        from django_cachex.lock import LockError, LockNotOwnedError

        if self._token is None:
            msg = "Cannot extend un-acquired lock"
            raise LockError(msg)
        if self._lease is None:
            # PTTL is -1 without a lease and ``_EXTEND_LUA`` clamps it to 0, so
            # extending would make a permanent lock self-release.
            msg = "Cannot extend a lock with no lease"
            raise LockError(msg)
        added_ms = int(additional_time * 1000)
        result = self._client.custom_command(
            [
                b"EVAL",
                _EXTEND_LUA.encode(),
                b"1",
                _enc(self._key),
                self._token,
                str(added_ms).encode(),
                b"1" if replace_ttl else b"0",
            ],
        )
        if not result:
            msg = "Cannot extend a lock that's no longer owned"
            raise LockNotOwnedError(msg)
        return True

    def __enter__(self) -> Self:
        from django_cachex.lock import LockError

        if not self.acquire():
            msg = f"Could not acquire lock on {self._key}"
            raise LockError(msg)
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
        lease: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        timeout: float | None = None,
        thread_local: bool = True,
    ) -> None:
        self._adapter = adapter
        self._key = key
        self._lease = lease
        self._sleep = sleep
        self._blocking = blocking
        self._timeout = timeout
        self._token_local: threading.local | None = threading.local() if thread_local else None
        self._token_shared: bytes | None = None

    @property
    def _token(self) -> bytes | None:
        if self._token_local is not None:
            return getattr(self._token_local, "token", None)
        return self._token_shared

    @_token.setter
    def _token(self, value: bytes | None) -> None:
        if self._token_local is not None:
            if value is None:
                if hasattr(self._token_local, "token"):
                    del self._token_local.token
            else:
                self._token_local.token = value
        else:
            self._token_shared = value

    async def acquire(self, *, blocking: bool | None = None, timeout: float | None = None) -> bool:
        bl = self._blocking if blocking is None else blocking
        bt = self._timeout if timeout is None else timeout
        deadline = time.monotonic() + bt if bt is not None else None
        client = await self._adapter.get_async_client()

        kw: dict[str, Any] = {"conditional_set": ConditionalChange.ONLY_IF_DOES_NOT_EXIST}
        if self._lease is not None:
            kw["expiry"] = ExpirySet(ExpiryType.MILLSEC, int(self._lease * 1000))

        while True:
            # Fresh token per attempt: a reused token would let a stale
            # holder release/extend a lock re-acquired by someone else.
            token = os.urandom(16).hex().encode()
            result = await client.set(self._key, token, **kw)
            if _ok_to_bool(result):
                self._token = token
                return True
            if not bl:
                return False
            if deadline is None:
                await asyncio.sleep(self._sleep)
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(self._sleep, remaining))

    async def release(self) -> None:
        from django_cachex.lock import LockError, LockNotOwnedError

        if self._token is None:
            msg = "Cannot release un-acquired lock"
            raise LockError(msg)
        client = await self._adapter.get_async_client()
        result = await client.custom_command(
            [b"EVAL", _RELEASE_LUA.encode(), b"1", _enc(self._key), self._token],
        )
        self._token = None
        if not result:
            msg = "Cannot release a lock that's no longer owned"
            raise LockNotOwnedError(msg)

    async def extend(self, additional_time: float, *, replace_ttl: bool = False) -> bool:
        from django_cachex.lock import LockError, LockNotOwnedError

        if self._token is None:
            msg = "Cannot extend un-acquired lock"
            raise LockError(msg)
        if self._lease is None:
            # See the note in ``_GlideLock.extend``.
            msg = "Cannot extend a lock with no lease"
            raise LockError(msg)
        client = await self._adapter.get_async_client()
        added_ms = int(additional_time * 1000)
        result = await client.custom_command(
            [
                b"EVAL",
                _EXTEND_LUA.encode(),
                b"1",
                _enc(self._key),
                self._token,
                str(added_ms).encode(),
                b"1" if replace_ttl else b"0",
            ],
        )
        if not result:
            msg = "Cannot extend a lock that's no longer owned"
            raise LockNotOwnedError(msg)
        return True

    async def __aenter__(self) -> Self:
        from django_cachex.lock import LockError

        if not await self.acquire():
            msg = f"Could not acquire lock on {self._key}"
            raise LockError(msg)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._token is not None:
            await self.release()


__all__ = [
    "ValkeyGlideAdapter",
    "ValkeyGlideAsyncPipelineAdapter",
    "ValkeyGlideClusterAdapter",
    "ValkeyGlidePipelineAdapter",
]
