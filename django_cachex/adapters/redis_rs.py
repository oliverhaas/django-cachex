"""Cache client backed by the Rust ``RedisRsDriver``.

Subclass of :class:`BaseKeyValueAdapter`. Reuses the serializer/compressor
stack and stampede prevention logic from the base, but routes every I/O
call to the Rust driver from ``_redis_rs_clients`` instead of redis-py /
valkey-py. Each driver is process-shared via the registry; per-cache state
lives on the subclass instance.
"""

from itertools import batched
from typing import TYPE_CHECKING, Any, cast, override
from urllib.parse import parse_qs, urlparse

from django_cachex._redis_rs_clients import (
    get_driver_cluster,
    get_driver_sentinel,
    get_driver_standard,
)
from django_cachex.adapters.protocols import KeyValueAdapterProtocol
from django_cachex.lock import AsyncValkeyLock, ValkeyLock
from django_cachex.stampede import (
    StampedeConfig,
    get_timeout_with_buffer,
    make_stampede_config,
    resolve_stampede,
    should_recompute,
)
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence

    from django_cachex._driver import RedisRsDriver
    from django_cachex.adapters.pipeline import BaseKeyValuePipelineAdapter
    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT, _Set


# Subset of options that map onto driver-construction kwargs.
_DRIVER_KWARGS = frozenset(
    {
        "cache_max_size",
        "cache_ttl_secs",
        "ssl_ca_certs",
        "ssl_certfile",
        "ssl_keyfile",
    },
)


def _value_to_bytes(value: bytes | int) -> bytes:
    """Coerce an encoded value to bytes for the Rust driver.

    ``BaseKeyValueAdapter.encode()`` returns ``int`` for plain integers (so
    Redis can use them as counters); the driver only accepts ``&[u8]``, so
    we serialize integers to their decimal representation here. Decoding on
    the way out goes through ``decode()`` which already tries ``int()``
    first, so the round-trip preserves type.
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value).encode("ascii")
    if isinstance(value, bytes):
        return value
    # Bool values went through the serializer already and came out as bytes;
    # if we ever land here it's a programming error in the caller.
    msg = f"Cannot encode {type(value).__name__} for the Rust driver"
    raise TypeError(msg)


def _str_key(key: object) -> str:
    if isinstance(key, bytes):
        return key.decode("utf-8")
    return str(key)


def _coerce_keys_arg(keys: Any) -> list[str]:
    if isinstance(keys, (str, bytes)):
        return [_str_key(keys)]
    return [_str_key(k) for k in keys]


def _to_seconds(timeout: ExpiryT) -> int:
    if isinstance(timeout, int):
        return timeout
    return int(timeout.total_seconds())


def _to_unix(when: AbsExpiryT) -> int:
    if isinstance(when, int):
        return when
    return int(when.timestamp())


def _eval_arg(value: Any) -> bytes:
    """Encode a Lua/EVAL ARGV value the way redis-py would on the wire."""
    if isinstance(value, bytes):
        return value
    # Match redis-py: bool serializes as the integer 0/1, not "True"/"False".
    if isinstance(value, bool):
        return b"1" if value else b"0"
    if isinstance(value, int):
        return str(value).encode("ascii")
    return str(value).encode("utf-8")


def _eval_args(args: Sequence[Any]) -> list[bytes]:
    return [a if isinstance(a, bytes) else _eval_arg(a) for a in args]


def _zadd_flag_argv(*, nx: bool, xx: bool, gt: bool, lt: bool, ch: bool) -> list[bytes]:
    flags: list[bytes] = []
    if nx:
        flags.append(b"NX")
    if xx:
        flags.append(b"XX")
    if gt:
        flags.append(b"GT")
    if lt:
        flags.append(b"LT")
    if ch:
        flags.append(b"CH")
    return flags


def _set_with_flags_argv(
    nvalue: bytes,
    actual_timeout: int | None,
    nx: bool,
    xx: bool,
    get: bool,
) -> list[bytes]:
    return [
        nvalue,
        str(actual_timeout).encode("ascii") if actual_timeout else b"",
        b"1" if nx else b"0",
        b"1" if xx else b"0",
        b"1" if get else b"0",
    ]


def _xadd_argv(
    entry_id: str,
    encoded_fields: list[tuple[str, bytes]],
    maxlen: int | None,
    approximate: bool,
    nomkstream: bool,
    minid: str | None,
    limit: int | None,
) -> list[bytes]:
    argv: list[bytes] = []
    if nomkstream:
        argv.append(b"NOMKSTREAM")
    if maxlen is not None:
        argv.append(b"MAXLEN")
        if approximate:
            argv.append(b"~")
        argv.append(str(maxlen).encode("ascii"))
        if limit is not None:
            argv.extend([b"LIMIT", str(limit).encode("ascii")])
    elif minid is not None:
        argv.append(b"MINID")
        if approximate:
            argv.append(b"~")
        argv.append(minid.encode("ascii"))
    argv.append(entry_id.encode("ascii"))
    for f, v in encoded_fields:
        argv.append(f.encode("utf-8"))
        argv.append(v)
    return argv


def _decode_str(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


def _decode_zrange(raw: list[Any], *, withscores: bool) -> list[Any]:
    """Normalize ZRANGE-family results.

    Returns the raw list when ``withscores=False``. With scores, accepts
    either nested ``[[m, s], ...]`` (RESP3 / standard) or flat ``[m, s,
    m, s, ...]`` (RESP2 / cluster) and returns ``[(m, float(s)), ...]``.
    """
    if not withscores:
        return list(raw)
    if raw and isinstance(raw[0], (list, tuple)):
        return [(member, float(score)) for member, score in raw]
    out: list[tuple[Any, float]] = []
    it = iter(raw)
    for member, score in zip(it, it, strict=False):
        out.append((member, float(score)))
    return out


def _decode_zrevrangebyscore(raw: list, *, withscores: bool) -> list[Any]:
    # ZREVRANGEBYSCORE WITHSCORES returns a flat [m1, s1, m2, s2, ...].
    if not withscores:
        return list(raw)
    out: list[tuple[Any, float]] = []
    it = iter(raw)
    for member, score in zip(it, it, strict=False):
        out.append((member, float(score)))
    return out


def _decode_xrange(raw: list[Any] | None) -> list[tuple[str, dict[str, Any]]]:
    if not raw:
        return []
    out: list[tuple[str, dict[str, Any]]] = []
    for entry in raw:
        entry_id = entry[0]
        if isinstance(entry_id, bytes):
            entry_id = entry_id.decode()
        kv = entry[1]
        fields: dict[str, Any] = {}
        if isinstance(kv, list):
            it = iter(kv)
            for f, v in zip(it, it, strict=False):
                field_name = f.decode() if isinstance(f, bytes) else str(f)
                fields[field_name] = v
        elif isinstance(kv, dict):
            for f, v in kv.items():
                field_name = f.decode() if isinstance(f, bytes) else str(f)
                fields[field_name] = v
        out.append((entry_id, fields))
    return out


def _decode_xread(raw: Any) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {(k.decode() if isinstance(k, bytes) else k): _decode_xrange(v) for k, v in raw.items()}
    return {(item[0].decode() if isinstance(item[0], bytes) else item[0]): _decode_xrange(item[1]) for item in raw}


def _decode_xpending_summary(raw: Any) -> dict[str, Any]:
    # Summary form: ``[count, min_id, max_id, [[consumer, count], ...] | None]``.
    if not raw:
        return {"pending": 0, "min": None, "max": None, "consumers": []}
    count = int(raw[0]) if raw[0] is not None else 0
    min_id = _decode_str(raw[1]) if raw[1] is not None else None
    max_id = _decode_str(raw[2]) if raw[2] is not None else None
    consumers: list[dict[str, Any]] = []
    if len(raw) > 3 and raw[3]:
        consumers.extend({"name": _decode_str(entry[0]), "pending": int(entry[1])} for entry in raw[3])
    return {"pending": count, "min": min_id, "max": max_id, "consumers": consumers}


def _decode_xpending_range(raw: Any) -> list[dict[str, Any]]:
    # Range form: ``[[id, consumer, idle_ms, delivery_count], ...]``.
    if not raw:
        return []
    return [
        {
            "message_id": _decode_str(entry[0]),
            "consumer": _decode_str(entry[1]),
            "time_since_delivered": int(entry[2]),
            "times_delivered": int(entry[3]),
        }
        for entry in raw
    ]


def _decode_xautoclaim(
    raw: Any,
    *,
    justid: bool,
) -> tuple[str, list[tuple[str, dict[str, Any]]] | list[str], list[str]]:
    # XAUTOCLAIM returns ``[next_id, entries, deleted_ids]`` (Redis 7+).
    next_id = _decode_str(raw[0]) if raw and raw[0] is not None else "0-0"
    entries = raw[1] if len(raw) > 1 else []
    deleted_raw = raw[2] if len(raw) > 2 else []
    deleted = [_decode_str(d) for d in (deleted_raw or [])]
    if justid:
        claimed_ids = [_decode_str(eid) for eid in (entries or [])]
        return (next_id, claimed_ids, deleted)
    return (next_id, _decode_xrange(entries), deleted)


class RedisRsValkeyAdapter(KeyValueAdapterProtocol):
    """Base Rust-driver client. Subclasses choose the topology.

    Implements the cachex adapter surface against ``RedisRsDriver``. We
    don't run redis-py's pool/parser machinery — every I/O call routes
    through the Rust driver instead — so the redis-py-shaped class slots
    on :class:`~django_cachex.adapters.valkey_py.ValkeyPyAdapter` aren't
    relevant here.
    """

    # Option keys we recognize so the registry cache hits across cache
    # instances that share a driver but differ only in cosmetic options.
    _DRIVER_OPTION_KEYS = _DRIVER_KWARGS

    # Default scan iteration batch size
    _default_scan_itersize: int = 100

    def __init__(self, servers: list[str], **options: Any) -> None:
        self._servers = servers
        self._options = options
        self._stampede_config: StampedeConfig | None = make_stampede_config(options.get("stampede_prevention"))

    def _resolve_stampede(self, stampede_prevention: bool | dict | None = None) -> StampedeConfig | None:
        return resolve_stampede(self._stampede_config, stampede_prevention)

    def _get_timeout_with_buffer(
        self,
        timeout: int | None,
        stampede_prevention: bool | dict | None = None,
    ) -> int | None:
        return get_timeout_with_buffer(timeout, self._stampede_config, stampede_prevention)

    @staticmethod
    def _normalize_ttl(result: int) -> int | None:
        """Normalize Redis TTL/PTTL/EXPIRETIME results.

        -1 (no expiry) → None, -2 (key missing) → -2, positive → as-is.
        """
        if result == -1:
            return None
        return result

    # ------------------------------------------------------------------ hooks

    @property
    def _driver(self) -> RedisRsDriver:
        # Always go through the registry so its PID-check rebuilds drivers
        # in post-fork children — caching on the instance would defeat that.
        return self._connect()

    def _connect(self) -> RedisRsDriver:
        """Resolve a process-shared driver. Subclasses override."""
        return get_driver_standard(self._servers[0], **self._driver_kwargs())

    def _driver_kwargs(self) -> dict[str, Any]:
        return {k: v for k, v in self._options.items() if k in self._DRIVER_OPTION_KEYS}

    @override
    def get_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        return self._driver

    @override
    def get_async_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        return self._driver

    def get_raw_client(self) -> RedisRsDriver:
        """Expose the underlying Rust driver (use sparingly)."""
        return self._driver

    # ----------------------------------------------------------- core: get/set

    @override
    def add(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool:
        nvalue = _value_to_bytes(value)
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            if self._driver.set_nx(_str_key(key), nvalue, ttl=None):
                self._driver.delete(_str_key(key))
                return True
            return False
        return bool(self._driver.set_nx(_str_key(key), nvalue, ttl=actual_timeout))

    @override
    async def aadd(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool:
        nvalue = _value_to_bytes(value)
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            if await self._driver.aset_nx(_str_key(key), nvalue, ttl=None):
                await self._driver.adelete(_str_key(key))
                return True
            return False
        return bool(await self._driver.aset_nx(_str_key(key), nvalue, ttl=actual_timeout))

    @override
    def get(self, key: KeyT, *, stampede_prevention: bool | dict | None = None) -> Any:
        val = self._driver.get(_str_key(key))
        if val is None:
            return None
        config = self._resolve_stampede(stampede_prevention)
        if config and isinstance(val, bytes):
            ttl = self._driver.ttl(_str_key(key))
            if ttl > 0 and should_recompute(ttl, config):
                return None
        return val

    @override
    async def aget(self, key: KeyT, *, stampede_prevention: bool | dict | None = None) -> Any:
        val = await self._driver.aget(_str_key(key))
        if val is None:
            return None
        config = self._resolve_stampede(stampede_prevention)
        if config and isinstance(val, bytes):
            ttl = await self._driver.attl(_str_key(key))
            if ttl > 0 and should_recompute(ttl, config):
                return None
        return val

    @override
    def set(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> None:
        nvalue = _value_to_bytes(value)
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            self._driver.delete(_str_key(key))
        else:
            self._driver.set(_str_key(key), nvalue, ttl=actual_timeout)

    @override
    async def aset(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> None:
        nvalue = _value_to_bytes(value)
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            await self._driver.adelete(_str_key(key))
        else:
            await self._driver.aset(_str_key(key), nvalue, ttl=actual_timeout)

    # Builds a SET-with-flags Lua script. The driver only exposes
    # set/set_nx; xx/get/combinations go through this eval path so we keep
    # full atomicity (Redis 6.2+ supports the GET flag on SET natively).
    _SET_WITH_FLAGS_LUA = (
        "local args = {KEYS[1], ARGV[1]}; "
        "if ARGV[2] ~= '' then args[#args+1] = 'EX'; args[#args+1] = ARGV[2] end; "
        "if ARGV[3] == '1' then args[#args+1] = 'NX' end; "
        "if ARGV[4] == '1' then args[#args+1] = 'XX' end; "
        "if ARGV[5] == '1' then args[#args+1] = 'GET' end; "
        "return redis.call('SET', unpack(args))"
    )

    @override
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
        nvalue = _value_to_bytes(value)
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            return None if get else False
        result = self._driver.eval(
            self._SET_WITH_FLAGS_LUA,
            [_str_key(key)],
            _set_with_flags_argv(nvalue, actual_timeout, nx, xx, get),
        )
        if get:
            # SET ... GET returns the previous value (bytes) or nil.
            return None if result is None else result
        # Without GET: Redis's "OK" status surfaces from the driver as ``True``
        # (Value::Okay → Python bool); NX/XX rejection comes through as None.
        return result is True

    @override
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
        nvalue = _value_to_bytes(value)
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            return None if get else False
        result = await self._driver.aeval(
            self._SET_WITH_FLAGS_LUA,
            [_str_key(key)],
            _set_with_flags_argv(nvalue, actual_timeout, nx, xx, get),
        )
        if get:
            return None if result is None else result
        return result is True

    @override
    def touch(self, key: KeyT, timeout: int | None) -> bool:
        if timeout is None:
            return bool(self._driver.persist(_str_key(key)))
        return bool(self._driver.expire(_str_key(key), timeout))

    @override
    async def atouch(self, key: KeyT, timeout: int | None) -> bool:
        if timeout is None:
            return bool(await self._driver.apersist(_str_key(key)))
        return bool(await self._driver.aexpire(_str_key(key), timeout))

    @override
    def delete(self, key: KeyT) -> bool:
        return bool(self._driver.delete(_str_key(key)))

    @override
    async def adelete(self, key: KeyT) -> bool:
        return bool(await self._driver.adelete(_str_key(key)))

    @override
    def get_many(self, keys: Iterable[KeyT], *, stampede_prevention: bool | dict | None = None) -> dict[KeyT, Any]:
        keys_list = [_str_key(k) for k in keys]
        if not keys_list:
            return {}
        results = self._driver.mget(keys_list)
        found = {k: v for k, v in zip(keys_list, results, strict=False) if v is not None}
        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            stampede_keys = [k for k, v in found.items() if isinstance(v, bytes)]
            for k in stampede_keys:
                ttl = self._driver.ttl(k)
                if ttl > 0 and should_recompute(ttl, config):
                    del found[k]
        return dict(found.items())

    @override
    async def aget_many(
        self,
        keys: Iterable[KeyT],
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> dict[KeyT, Any]:
        keys_list = [_str_key(k) for k in keys]
        if not keys_list:
            return {}
        results = await self._driver.amget(keys_list)
        found = {k: v for k, v in zip(keys_list, results, strict=False) if v is not None}
        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            stampede_keys = [k for k, v in found.items() if isinstance(v, bytes)]
            for k in stampede_keys:
                ttl = await self._driver.attl(k)
                if ttl > 0 and should_recompute(ttl, config):
                    del found[k]
        return dict(found.items())

    @override
    def has_key(self, key: KeyT) -> bool:
        return bool(self._driver.exists(_str_key(key)))

    @override
    async def ahas_key(self, key: KeyT) -> bool:
        return bool(await self._driver.aexists(_str_key(key)))

    @override
    def type(self, key: KeyT) -> KeyType | None:
        result = self._driver.type(_str_key(key))
        return None if result == "none" else KeyType(result)

    @override
    async def atype(self, key: KeyT) -> KeyType | None:
        result = await self._driver.atype(_str_key(key))
        return None if result == "none" else KeyType(result)

    @override
    def incr(self, key: KeyT, delta: int = 1) -> int:
        return int(self._driver.incr_by(_str_key(key), delta))

    @override
    async def aincr(self, key: KeyT, delta: int = 1) -> int:
        return int(await self._driver.aincr_by(_str_key(key), delta))

    @override
    def set_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        if not data:
            return []
        prepared = [(_str_key(k), _value_to_bytes(v)) for k, v in data.items()]
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            self._driver.delete_many([k for k, _ in prepared])
        else:
            self._driver.pipeline_set(prepared, ttl=actual_timeout)
        return []

    @override
    async def aset_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list:
        if not data:
            return []
        prepared = [(_str_key(k), _value_to_bytes(v)) for k, v in data.items()]
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            await self._driver.adelete_many([k for k, _ in prepared])
        else:
            await self._driver.apipeline_set(prepared, ttl=actual_timeout)
        return []

    @override
    def delete_many(self, keys: Sequence[KeyT]) -> int:
        if not keys:
            return 0
        return int(self._driver.delete_many([_str_key(k) for k in keys]))

    @override
    async def adelete_many(self, keys: Sequence[KeyT]) -> int:
        if not keys:
            return 0
        return int(await self._driver.adelete_many([_str_key(k) for k in keys]))

    @override
    def clear(self) -> bool:
        self._driver.flushdb()
        return True

    @override
    async def aclear(self) -> bool:
        await self._driver.aflushdb()
        return True

    # ------------------------------------------------------------------- TTL

    @override
    def ttl(self, key: KeyT) -> int | None:
        return self._normalize_ttl(self._driver.ttl(_str_key(key)))

    @override
    def pttl(self, key: KeyT) -> int | None:
        return self._normalize_ttl(self._driver.pttl(_str_key(key)))

    @override
    def expire(self, key: KeyT, timeout: ExpiryT) -> bool:
        return bool(self._driver.expire(_str_key(key), _to_seconds(timeout)))

    @override
    def persist(self, key: KeyT) -> bool:
        return bool(self._driver.persist(_str_key(key)))

    @override
    async def attl(self, key: KeyT) -> int | None:
        return self._normalize_ttl(await self._driver.attl(_str_key(key)))

    @override
    async def apttl(self, key: KeyT) -> int | None:
        return self._normalize_ttl(await self._driver.apttl(_str_key(key)))

    @override
    async def aexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        return bool(await self._driver.aexpire(_str_key(key), _to_seconds(timeout)))

    @override
    async def apersist(self, key: KeyT) -> bool:
        return bool(await self._driver.apersist(_str_key(key)))

    # The driver currently exposes EXPIRE/PERSIST/TTL/PTTL only. The remaining
    # TTL ops (PEXPIRE / EXPIREAT / PEXPIREAT / EXPIRETIME) are implemented via
    # raw command via ``eval`` to keep parity with the redis-py surface.

    def _expire_via_eval(self, command: str, key: KeyT, value: int) -> bool:
        result = self._driver.eval(
            f"return redis.call('{command}', KEYS[1], ARGV[1])",
            [_str_key(key)],
            [str(int(value)).encode("ascii")],
        )
        return bool(result)

    async def _aexpire_via_eval(self, command: str, key: KeyT, value: int) -> bool:
        result = await self._driver.aeval(
            f"return redis.call('{command}', KEYS[1], ARGV[1])",
            [_str_key(key)],
            [str(int(value)).encode("ascii")],
        )
        return bool(result)

    @override
    def pexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        if isinstance(timeout, int):
            ms = timeout
        else:
            ms = int(timeout.total_seconds() * 1000)
        return self._expire_via_eval("PEXPIRE", key, ms)

    @override
    def expireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        return self._expire_via_eval("EXPIREAT", key, _to_unix(when))

    @override
    def pexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        if isinstance(when, int):
            ms = when
        else:
            ms = int(when.timestamp() * 1000)
        return self._expire_via_eval("PEXPIREAT", key, ms)

    @override
    def expiretime(self, key: KeyT) -> int | None:
        result = self._driver.eval(
            "return redis.call('EXPIRETIME', KEYS[1])",
            [_str_key(key)],
            [],
        )
        return self._normalize_ttl(int(result))

    @override
    async def apexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        if isinstance(timeout, int):
            ms = timeout
        else:
            ms = int(timeout.total_seconds() * 1000)
        return await self._aexpire_via_eval("PEXPIRE", key, ms)

    @override
    async def aexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        return await self._aexpire_via_eval("EXPIREAT", key, _to_unix(when))

    @override
    async def apexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        if isinstance(when, int):
            ms = when
        else:
            ms = int(when.timestamp() * 1000)
        return await self._aexpire_via_eval("PEXPIREAT", key, ms)

    @override
    async def aexpiretime(self, key: KeyT) -> int | None:
        result = await self._driver.aeval(
            "return redis.call('EXPIRETIME', KEYS[1])",
            [_str_key(key)],
            [],
        )
        return self._normalize_ttl(int(result))

    # ---------------------------------------------------------- pattern / scan

    @override
    def keys(self, pattern: str) -> list[str]:
        result = self._driver.keys(pattern)
        return [k.decode() if isinstance(k, bytes) else k for k in result]

    @override
    async def akeys(self, pattern: str) -> list[str]:
        result = await self._driver.akeys(pattern)
        return [k.decode() if isinstance(k, bytes) else k for k in result]

    @override
    def iter_keys(self, pattern: str, itersize: int | None = None) -> Iterator[str]:
        if itersize is None:
            itersize = self._default_scan_itersize
        keys = self._driver.scan(pattern, itersize)
        for k in keys:
            yield k.decode() if isinstance(k, bytes) else k

    @override
    async def aiter_keys(self, pattern: str, itersize: int | None = None) -> AsyncIterator[str]:
        if itersize is None:
            itersize = self._default_scan_itersize
        keys = await self._driver.ascan(pattern, itersize)
        for k in keys:
            yield k.decode() if isinstance(k, bytes) else k

    @override
    def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        # One SCAN iteration; caller drives the loop. Cluster mode falls
        # back to a single-shot KEYS call and always returns next_cursor=0.
        if count is None:
            count = self._default_scan_itersize
        next_cursor, keys = self._driver.scan_one(cursor, match or "*", count)
        return next_cursor, [k.decode() if isinstance(k, bytes) else k for k in keys]

    @override
    async def ascan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> tuple[int, list[str]]:
        if count is None:
            count = self._default_scan_itersize
        next_cursor, keys = await self._driver.ascan_one(cursor, match or "*", count)
        return next_cursor, [k.decode() if isinstance(k, bytes) else k for k in keys]

    @override
    def delete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        if itersize is None:
            itersize = self._default_scan_itersize
        count = 0
        for batch in batched(self.iter_keys(pattern, itersize=itersize), itersize, strict=False):
            count += int(self._driver.delete_many(list(batch)))
        return count

    @override
    async def adelete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        if itersize is None:
            itersize = self._default_scan_itersize
        count = 0
        batch: list[str] = []
        async for key in self.aiter_keys(pattern, itersize=itersize):
            batch.append(key)
            if len(batch) >= itersize:
                count += int(await self._driver.adelete_many(batch))
                batch = []
        if batch:
            count += int(await self._driver.adelete_many(batch))
        return count

    @override
    def rename(self, src: KeyT, dst: KeyT) -> bool:
        try:
            result = self._driver.eval(
                "return redis.call('RENAME', KEYS[1], KEYS[2])",
                [_str_key(src), _str_key(dst)],
                [],
            )
        except RuntimeError as e:
            if "no such key" in str(e).lower():
                raise ValueError(f"Key {src!r} not found") from None
            raise
        # The driver maps Redis's OK status to Python ``True``.
        return result is True

    @override
    def renamenx(self, src: KeyT, dst: KeyT) -> bool:
        try:
            result = self._driver.eval(
                "return redis.call('RENAMENX', KEYS[1], KEYS[2])",
                [_str_key(src), _str_key(dst)],
                [],
            )
        except RuntimeError as e:
            if "no such key" in str(e).lower():
                raise ValueError(f"Key {src!r} not found") from None
            raise
        return bool(result)

    @override
    async def arename(self, src: KeyT, dst: KeyT) -> bool:
        try:
            result = await self._driver.aeval(
                "return redis.call('RENAME', KEYS[1], KEYS[2])",
                [_str_key(src), _str_key(dst)],
                [],
            )
        except RuntimeError as e:
            if "no such key" in str(e).lower():
                raise ValueError(f"Key {src!r} not found") from None
            raise
        return result is True

    @override
    async def arenamenx(self, src: KeyT, dst: KeyT) -> bool:
        try:
            result = await self._driver.aeval(
                "return redis.call('RENAMENX', KEYS[1], KEYS[2])",
                [_str_key(src), _str_key(dst)],
                [],
            )
        except RuntimeError as e:
            if "no such key" in str(e).lower():
                raise ValueError(f"Key {src!r} not found") from None
            raise
        return bool(result)

    # ----------------------------------------------------------------- locks

    @override
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
        return ValkeyLock(
            self._driver,
            key,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    @override
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
        return AsyncValkeyLock(
            self._driver,
            key,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    # ----------------------------------------------------------------- lifecycle

    def close(self, **kwargs: Any) -> None:
        """No-op. The Rust driver lives in the shared registry, not this instance."""

    async def aclose(self, **kwargs: Any) -> None:
        """No-op. The Rust driver lives in the shared registry, not this instance."""

    # ----------------------------------------------------------------- admin

    @override
    def info(self, section: str | None = None) -> dict[str, Any]:
        # The driver fetches the full INFO bulk string; if a section was
        # requested, slice client-side using the "# <Section>" headers.
        raw = self._driver.info()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if section is not None:
            raw = _select_info_section(raw, section)
        return _parse_info(raw)

    # =========================================================================
    # Pipeline
    # =========================================================================

    @override
    def pipeline(self, *, transaction: bool = True) -> BaseKeyValuePipelineAdapter:
        from django_cachex.adapters.pipeline_redis_rs import RedisRsValkeyPipelineAdapter

        return RedisRsValkeyPipelineAdapter(self._driver, transaction=transaction)

    # =========================================================================
    # Hashes
    # =========================================================================

    @override
    def hset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        pairs: list[tuple[str, bytes]] = []
        if field is not None:
            pairs.append((str(field), _value_to_bytes(value)))
        if mapping:
            pairs.extend((str(k), _value_to_bytes(v)) for k, v in mapping.items())
        if items:
            it = iter(items)
            for f, v in zip(it, it, strict=False):
                pairs.append((str(f), _value_to_bytes(v)))
        if not pairs:
            return 0
        if len(pairs) == 1:
            return int(self._driver.hset(_str_key(key), pairs[0][0], pairs[0][1]))
        # HMSET returns OK, not a count. Match HSET semantics: number of new fields.
        before = self._driver.hlen(_str_key(key))
        self._driver.hmset(_str_key(key), pairs)
        after = self._driver.hlen(_str_key(key))
        return after - before

    @override
    async def ahset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        mapping: Mapping[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> int:
        pairs: list[tuple[str, bytes]] = []
        if field is not None:
            pairs.append((str(field), _value_to_bytes(value)))
        if mapping:
            pairs.extend((str(k), _value_to_bytes(v)) for k, v in mapping.items())
        if items:
            it = iter(items)
            for f, v in zip(it, it, strict=False):
                pairs.append((str(f), _value_to_bytes(v)))
        if not pairs:
            return 0
        if len(pairs) == 1:
            ret = await self._driver.ahset(_str_key(key), pairs[0][0], pairs[0][1])
            return int(ret)
        before = await self._driver.ahlen(_str_key(key))
        await self._driver.ahmset(_str_key(key), pairs)
        after = await self._driver.ahlen(_str_key(key))
        return int(after) - int(before)

    @override
    def hsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        # No native driver method — go via eval.
        result = self._driver.eval(
            "return redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])",
            [_str_key(key)],
            [str(field).encode("utf-8"), _value_to_bytes(value)],
        )
        return bool(result)

    @override
    async def ahsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        result = await self._driver.aeval(
            "return redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])",
            [_str_key(key)],
            [str(field).encode("utf-8"), _value_to_bytes(value)],
        )
        return bool(result)

    @override
    def hget(self, key: KeyT, field: str) -> Any | None:
        val = self._driver.hget(_str_key(key), str(field))
        return None if val is None else val

    @override
    async def ahget(self, key: KeyT, field: str) -> Any | None:
        val = await self._driver.ahget(_str_key(key), str(field))
        return None if val is None else val

    @override
    def hmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        if not fields:
            return []
        result = self._driver.hmget(_str_key(key), [str(f) for f in fields])
        return [None if v is None else v for v in result]

    @override
    async def ahmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        if not fields:
            return []
        result = await self._driver.ahmget(_str_key(key), [str(f) for f in fields])
        return [None if v is None else v for v in result]

    @override
    def hgetall(self, key: KeyT) -> dict[str, Any]:
        result = self._driver.hgetall(_str_key(key))
        return {(k.decode() if isinstance(k, bytes) else k): v for k, v in result.items()}

    @override
    async def ahgetall(self, key: KeyT) -> dict[str, Any]:
        result = await self._driver.ahgetall(_str_key(key))
        return {(k.decode() if isinstance(k, bytes) else k): v for k, v in result.items()}

    @override
    def hdel(self, key: KeyT, *fields: str) -> int:
        if not fields:
            return 0
        return int(self._driver.hdel(_str_key(key), [str(f) for f in fields]))

    @override
    async def ahdel(self, key: KeyT, *fields: str) -> int:
        if not fields:
            return 0
        return int(await self._driver.ahdel(_str_key(key), [str(f) for f in fields]))

    @override
    def hexists(self, key: KeyT, field: str) -> bool:
        return bool(self._driver.hexists(_str_key(key), str(field)))

    @override
    async def ahexists(self, key: KeyT, field: str) -> bool:
        return bool(await self._driver.ahexists(_str_key(key), str(field)))

    @override
    def hlen(self, key: KeyT) -> int:
        return int(self._driver.hlen(_str_key(key)))

    @override
    async def ahlen(self, key: KeyT) -> int:
        return int(await self._driver.ahlen(_str_key(key)))

    @override
    def hkeys(self, key: KeyT) -> list[str]:
        result = self._driver.hkeys(_str_key(key))
        return [k.decode() if isinstance(k, bytes) else k for k in result]

    @override
    async def ahkeys(self, key: KeyT) -> list[str]:
        result = await self._driver.ahkeys(_str_key(key))
        return [k.decode() if isinstance(k, bytes) else k for k in result]

    @override
    def hvals(self, key: KeyT) -> list[Any]:
        result = self._driver.hvals(_str_key(key))
        return list(result)

    @override
    async def ahvals(self, key: KeyT) -> list[Any]:
        result = await self._driver.ahvals(_str_key(key))
        return list(result)

    @override
    def hincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        return int(self._driver.hincrby(_str_key(key), str(field), amount))

    @override
    async def ahincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        return int(await self._driver.ahincrby(_str_key(key), str(field), amount))

    @override
    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0) -> float:
        result = self._driver.eval(
            "return redis.call('HINCRBYFLOAT', KEYS[1], ARGV[1], ARGV[2])",
            [_str_key(key)],
            [str(field).encode("utf-8"), str(amount).encode("utf-8")],
        )
        if isinstance(result, bytes):
            result = result.decode()
        return float(result)

    @override
    async def ahincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0) -> float:
        result = await self._driver.aeval(
            "return redis.call('HINCRBYFLOAT', KEYS[1], ARGV[1], ARGV[2])",
            [_str_key(key)],
            [str(field).encode("utf-8"), str(amount).encode("utf-8")],
        )
        if isinstance(result, bytes):
            result = result.decode()
        return float(result)

    # =========================================================================
    # Lists
    # =========================================================================

    @override
    def lpush(self, key: KeyT, *values: Any) -> int:
        if not values:
            return self.llen(key)
        return int(self._driver.lpush(_str_key(key), [_value_to_bytes(v) for v in values]))

    @override
    def rpush(self, key: KeyT, *values: Any) -> int:
        if not values:
            return self.llen(key)
        return int(self._driver.rpush(_str_key(key), [_value_to_bytes(v) for v in values]))

    @override
    async def alpush(self, key: KeyT, *values: Any) -> int:
        if not values:
            return await self.allen(key)
        return int(await self._driver.alpush(_str_key(key), [_value_to_bytes(v) for v in values]))

    @override
    async def arpush(self, key: KeyT, *values: Any) -> int:
        if not values:
            return await self.allen(key)
        return int(await self._driver.arpush(_str_key(key), [_value_to_bytes(v) for v in values]))

    # lpop / rpop / alpop / arpop with the optional ``count`` argument are
    # implemented in the eval-fallback section below.

    @override
    def llen(self, key: KeyT) -> int:
        return int(self._driver.llen(_str_key(key)))

    @override
    async def allen(self, key: KeyT) -> int:
        return int(await self._driver.allen(_str_key(key)))

    @override
    def lrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        result = self._driver.lrange(_str_key(key), start, end)
        return list(result)

    @override
    async def alrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        result = await self._driver.alrange(_str_key(key), start, end)
        return list(result)

    @override
    def lindex(self, key: KeyT, index: int) -> Any | None:
        val = self._driver.lindex(_str_key(key), index)
        return None if val is None else val

    @override
    async def alindex(self, key: KeyT, index: int) -> Any | None:
        val = await self._driver.alindex(_str_key(key), index)
        return None if val is None else val

    @override
    def lset(self, key: KeyT, index: int, value: Any) -> bool:
        self._driver.lset(_str_key(key), index, _value_to_bytes(value))
        return True

    @override
    async def alset(self, key: KeyT, index: int, value: Any) -> bool:
        await self._driver.alset(_str_key(key), index, _value_to_bytes(value))
        return True

    @override
    def lrem(self, key: KeyT, count: int, value: Any) -> int:
        return int(
            self._driver.lrem(_str_key(key), count, _value_to_bytes(value)),
        )

    @override
    async def alrem(self, key: KeyT, count: int, value: Any) -> int:
        return int(
            await self._driver.alrem(_str_key(key), count, _value_to_bytes(value)),
        )

    @override
    def ltrim(self, key: KeyT, start: int, end: int) -> bool:
        self._driver.ltrim(_str_key(key), start, end)
        return True

    @override
    async def altrim(self, key: KeyT, start: int, end: int) -> bool:
        await self._driver.altrim(_str_key(key), start, end)
        return True

    @override
    def linsert(
        self,
        key: KeyT,
        where: str,
        pivot: Any,
        value: Any,
    ) -> int:
        before = where.upper() == "BEFORE"
        return int(
            self._driver.linsert(
                _str_key(key),
                before=before,
                pivot=_value_to_bytes(pivot),
                value=_value_to_bytes(value),
            ),
        )

    @override
    async def alinsert(
        self,
        key: KeyT,
        where: str,
        pivot: Any,
        value: Any,
    ) -> int:
        before = where.upper() == "BEFORE"
        return int(
            await self._driver.alinsert(
                _str_key(key),
                before=before,
                pivot=_value_to_bytes(pivot),
                value=_value_to_bytes(value),
            ),
        )

    # =========================================================================
    # Sets
    # =========================================================================

    @override
    def sadd(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(self._driver.sadd(_str_key(key), [_value_to_bytes(m) for m in members]))

    @override
    async def asadd(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(await self._driver.asadd(_str_key(key), [_value_to_bytes(m) for m in members]))

    @override
    def srem(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(self._driver.srem(_str_key(key), [_value_to_bytes(m) for m in members]))

    @override
    async def asrem(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(await self._driver.asrem(_str_key(key), [_value_to_bytes(m) for m in members]))

    @override
    def smembers(self, key: KeyT) -> Any:
        result = self._driver.smembers(_str_key(key))
        return set(result)

    @override
    async def asmembers(self, key: KeyT) -> Any:
        result = await self._driver.asmembers(_str_key(key))
        return set(result)

    @override
    def sismember(self, key: KeyT, member: Any) -> bool:
        return bool(
            self._driver.sismember(_str_key(key), _value_to_bytes(member)),
        )

    @override
    async def asismember(self, key: KeyT, member: Any) -> bool:
        return bool(
            await self._driver.asismember(_str_key(key), _value_to_bytes(member)),
        )

    @override
    def scard(self, key: KeyT) -> int:
        return int(self._driver.scard(_str_key(key)))

    @override
    async def ascard(self, key: KeyT) -> int:
        return int(await self._driver.ascard(_str_key(key)))

    @override
    def sinter(self, keys: Any) -> Any:
        result = self._driver.sinter(_coerce_keys_arg(keys))
        return set(result)

    @override
    async def asinter(self, keys: Any) -> Any:
        result = await self._driver.asinter(_coerce_keys_arg(keys))
        return set(result)

    @override
    def sunion(self, keys: Any) -> Any:
        result = self._driver.sunion(_coerce_keys_arg(keys))
        return set(result)

    @override
    async def asunion(self, keys: Any) -> Any:
        result = await self._driver.asunion(_coerce_keys_arg(keys))
        return set(result)

    @override
    def sdiff(self, keys: Any) -> Any:
        result = self._driver.sdiff(_coerce_keys_arg(keys))
        return set(result)

    @override
    async def asdiff(self, keys: Any) -> Any:
        result = await self._driver.asdiff(_coerce_keys_arg(keys))
        return set(result)

    # =========================================================================
    # Sorted sets
    # =========================================================================

    # zadd / azadd with nx/xx/gt/lt/ch flags are implemented in the
    # eval-fallback section below.

    @override
    def zrem(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(self._driver.zrem(_str_key(key), [_value_to_bytes(m) for m in members]))

    @override
    async def azrem(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(await self._driver.azrem(_str_key(key), [_value_to_bytes(m) for m in members]))

    @override
    def zscore(self, key: KeyT, member: Any) -> float | None:
        return self._driver.zscore(_str_key(key), _value_to_bytes(member))

    @override
    async def azscore(self, key: KeyT, member: Any) -> float | None:
        return await self._driver.azscore(_str_key(key), _value_to_bytes(member))

    @override
    def zrank(self, key: KeyT, member: Any) -> int | None:
        result = self._driver.zrank(_str_key(key), _value_to_bytes(member))
        return None if result is None else int(result)

    @override
    async def azrank(self, key: KeyT, member: Any) -> int | None:
        result = await self._driver.azrank(_str_key(key), _value_to_bytes(member))
        return None if result is None else int(result)

    @override
    def zcard(self, key: KeyT) -> int:
        return int(self._driver.zcard(_str_key(key)))

    @override
    async def azcard(self, key: KeyT) -> int:
        return int(await self._driver.azcard(_str_key(key)))

    @override
    def zcount(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        return int(self._driver.zcount(_str_key(key), str(min_score), str(max_score)))

    @override
    async def azcount(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        return int(await self._driver.azcount(_str_key(key), str(min_score), str(max_score)))

    @override
    def zincrby(self, key: KeyT, amount: float, member: Any) -> float:
        return float(
            self._driver.zincrby(_str_key(key), _value_to_bytes(member), float(amount)),
        )

    @override
    async def azincrby(self, key: KeyT, amount: float, member: Any) -> float:
        return float(
            await self._driver.azincrby(_str_key(key), _value_to_bytes(member), float(amount)),
        )

    @override
    def zrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any]:
        raw = self._driver.zrange(_str_key(key), start, end, withscores)
        return _decode_zrange(raw, withscores=withscores)

    @override
    async def azrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any]:
        raw = await self._driver.azrange(_str_key(key), start, end, withscores)
        return _decode_zrange(raw, withscores=withscores)

    @override
    def zrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any]:
        raw = self._driver.zrevrange(_str_key(key), start, end, withscores)
        return _decode_zrange(raw, withscores=withscores)

    @override
    async def azrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any]:
        raw = await self._driver.azrevrange(_str_key(key), start, end, withscores)
        return _decode_zrange(raw, withscores=withscores)

    @override
    def zrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        *,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any]:
        raw = self._driver.zrangebyscore(
            _str_key(key),
            str(min_score),
            str(max_score),
            withscores,
        )
        decoded = _decode_zrange(raw, withscores=withscores)
        # Driver doesn't expose LIMIT — slice client-side for parity.
        if start is not None or num is not None:
            offset = start or 0
            decoded = decoded[offset : offset + num] if num is not None else decoded[offset:]
        return decoded

    @override
    async def azrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
        *,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any]:
        raw = await self._driver.azrangebyscore(
            _str_key(key),
            str(min_score),
            str(max_score),
            withscores,
        )
        decoded = _decode_zrange(raw, withscores=withscores)
        if start is not None or num is not None:
            offset = start or 0
            decoded = decoded[offset : offset + num] if num is not None else decoded[offset:]
        return decoded

    @override
    def zpopmin(self, key: KeyT, count: int | None = None) -> list[tuple[Any, float]]:
        # Rust driver wants a concrete i64; default-None semantics ("return one")
        # are encoded as count=1 here, mirroring redis-py's no-count call shape.
        raw = self._driver.zpopmin(_str_key(key), 1 if count is None else count)
        return [(m, float(s)) for m, s in raw]

    @override
    async def azpopmin(self, key: KeyT, count: int | None = None) -> list[tuple[Any, float]]:
        raw = await self._driver.azpopmin(_str_key(key), 1 if count is None else count)
        return [(m, float(s)) for m, s in raw]

    @override
    def zpopmax(self, key: KeyT, count: int | None = None) -> list[tuple[Any, float]]:
        raw = self._driver.zpopmax(_str_key(key), 1 if count is None else count)
        return [(m, float(s)) for m, s in raw]

    @override
    async def azpopmax(self, key: KeyT, count: int | None = None) -> list[tuple[Any, float]]:
        raw = await self._driver.azpopmax(_str_key(key), 1 if count is None else count)
        return [(m, float(s)) for m, s in raw]

    # =========================================================================
    # Scripts
    # =========================================================================

    @override
    def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        keys = [_str_key(k) for k in keys_and_args[:numkeys]]
        args = [_eval_arg(a) for a in keys_and_args[numkeys:]]
        return self._driver.eval(script, keys, args)

    @override
    async def aeval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        keys = [_str_key(k) for k in keys_and_args[:numkeys]]
        args = [_eval_arg(a) for a in keys_and_args[numkeys:]]
        return await self._driver.aeval(script, keys, args)

    # =========================================================================
    # Surface the driver doesn't expose natively — implemented via EVAL.
    # =========================================================================

    # Lua only expands the *last* multiret in a call expression — concatenate
    # KEYS and ARGV into a single table before unpacking.
    _EVAL_CALL_TEMPLATE = (
        "local args = {{}}; "
        "for _, v in ipairs(KEYS) do args[#args+1] = v end; "
        "for _, v in ipairs(ARGV) do args[#args+1] = v end; "
        "return redis.call('{cmd}', unpack(args))"
    )

    def _eval_call(self, command: str, keys: Sequence[KeyT], args: Sequence[Any]) -> Any:
        return self._driver.eval(
            self._EVAL_CALL_TEMPLATE.format(cmd=command),
            [_str_key(k) for k in keys],
            _eval_args(args),
        )

    async def _aeval_call(self, command: str, keys: Sequence[KeyT], args: Sequence[Any]) -> Any:
        return await self._driver.aeval(
            self._EVAL_CALL_TEMPLATE.format(cmd=command),
            [_str_key(k) for k in keys],
            _eval_args(args),
        )

    # ---- lpop/rpop with count ----

    @override
    def lpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is None:
            val = self._driver.lpop(_str_key(key))
            return None if val is None else val
        result = self._eval_call("LPOP", [key], [count])
        return None if result is None else list(result)

    @override
    def rpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is None:
            val = self._driver.rpop(_str_key(key))
            return None if val is None else val
        result = self._eval_call("RPOP", [key], [count])
        return None if result is None else list(result)

    @override
    async def alpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is None:
            val = await self._driver.alpop(_str_key(key))
            return None if val is None else val
        result = await self._aeval_call("LPOP", [key], [count])
        return None if result is None else list(result)

    @override
    async def arpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is None:
            val = await self._driver.arpop(_str_key(key))
            return None if val is None else val
        result = await self._aeval_call("RPOP", [key], [count])
        return None if result is None else list(result)

    # ---- zadd flags ----

    @override
    def zadd(
        self,
        key: KeyT,
        mapping: Mapping[Any, float],
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
    ) -> int:
        pairs = [(_value_to_bytes(m), float(s)) for m, s in mapping.items()]
        if not pairs:
            return 0
        if not (nx or xx or gt or lt or ch):
            return int(self._driver.zadd(_str_key(key), pairs))
        argv: list[bytes] = _zadd_flag_argv(nx=nx, xx=xx, gt=gt, lt=lt, ch=ch)
        for member, score in pairs:
            argv.append(str(score).encode("ascii"))
            argv.append(member)
        return int(self._eval_call("ZADD", [key], argv))

    @override
    async def azadd(
        self,
        key: KeyT,
        mapping: Mapping[Any, float],
        *,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
    ) -> int:
        pairs = [(_value_to_bytes(m), float(s)) for m, s in mapping.items()]
        if not pairs:
            return 0
        if not (nx or xx or gt or lt or ch):
            return int(await self._driver.azadd(_str_key(key), pairs))
        argv: list[bytes] = _zadd_flag_argv(nx=nx, xx=xx, gt=gt, lt=lt, ch=ch)
        for member, score in pairs:
            argv.append(str(score).encode("ascii"))
            argv.append(member)
        return int(await self._aeval_call("ZADD", [key], argv))

    # ---- zrevrank / zmscore / zremrangebyrank / zremrangebyscore / zrevrangebyscore ----

    @override
    def zrevrank(self, key: KeyT, member: Any) -> int | None:
        result = self._eval_call("ZREVRANK", [key], [_value_to_bytes(member)])
        return None if result is None else int(result)

    @override
    async def azrevrank(self, key: KeyT, member: Any) -> int | None:
        result = await self._aeval_call("ZREVRANK", [key], [_value_to_bytes(member)])
        return None if result is None else int(result)

    @override
    def zmscore(self, key: KeyT, *members: Any) -> list[float | None]:
        if not members:
            return []
        argv = [_value_to_bytes(m) for m in members]
        result = self._eval_call("ZMSCORE", [key], argv)
        return [None if s is None else float(s) for s in result]

    @override
    async def azmscore(self, key: KeyT, *members: Any) -> list[float | None]:
        if not members:
            return []
        argv = [_value_to_bytes(m) for m in members]
        result = await self._aeval_call("ZMSCORE", [key], argv)
        return [None if s is None else float(s) for s in result]

    @override
    def zremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        return int(self._eval_call("ZREMRANGEBYRANK", [key], [start, end]))

    @override
    async def azremrangebyrank(self, key: KeyT, start: int, end: int) -> int:
        return int(await self._aeval_call("ZREMRANGEBYRANK", [key], [start, end]))

    @override
    def zremrangebyscore(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        return int(self._eval_call("ZREMRANGEBYSCORE", [key], [str(min_score), str(max_score)]))

    @override
    async def azremrangebyscore(
        self,
        key: KeyT,
        min_score: float | str,
        max_score: float | str,
    ) -> int:
        return int(await self._aeval_call("ZREMRANGEBYSCORE", [key], [str(min_score), str(max_score)]))

    @override
    def zrevrangebyscore(
        self,
        key: KeyT,
        max_score: float | str,
        min_score: float | str,
        *,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any]:
        argv: list[Any] = [str(max_score), str(min_score)]
        if withscores:
            argv.append(b"WITHSCORES")
        if start is not None and num is not None:
            argv.extend([b"LIMIT", start, num])
        raw = self._eval_call("ZREVRANGEBYSCORE", [key], argv)
        return _decode_zrevrangebyscore(raw, withscores=withscores)

    @override
    async def azrevrangebyscore(
        self,
        key: KeyT,
        max_score: float | str,
        min_score: float | str,
        *,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[Any]:
        argv: list[Any] = [str(max_score), str(min_score)]
        if withscores:
            argv.append(b"WITHSCORES")
        if start is not None and num is not None:
            argv.extend([b"LIMIT", start, num])
        raw = await self._aeval_call("ZREVRANGEBYSCORE", [key], argv)
        return _decode_zrevrangebyscore(raw, withscores=withscores)

    # ---- set ops the driver doesn't expose ----

    @override
    def smove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        return bool(self._eval_call("SMOVE", [src, dst], [_value_to_bytes(member)]))

    @override
    async def asmove(self, src: KeyT, dst: KeyT, member: Any) -> bool:
        return bool(
            await self._aeval_call("SMOVE", [src, dst], [_value_to_bytes(member)]),
        )

    @override
    def smismember(self, key: KeyT, *members: Any) -> list[bool]:
        if not members:
            return []
        argv = [_value_to_bytes(m) for m in members]
        result = self._eval_call("SMISMEMBER", [key], argv)
        return [bool(r) for r in result]

    @override
    async def asmismember(self, key: KeyT, *members: Any) -> list[bool]:
        if not members:
            return []
        argv = [_value_to_bytes(m) for m in members]
        result = await self._aeval_call("SMISMEMBER", [key], argv)
        return [bool(r) for r in result]

    @override
    def spop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is None:
            val = self._eval_call("SPOP", [key], [])
            return None if val is None else val
        result = self._eval_call("SPOP", [key], [count])
        return None if result is None else list(result)

    @override
    async def aspop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is None:
            val = await self._aeval_call("SPOP", [key], [])
            return None if val is None else val
        result = await self._aeval_call("SPOP", [key], [count])
        return None if result is None else list(result)

    @override
    def srandmember(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is None:
            val = self._eval_call("SRANDMEMBER", [key], [])
            return None if val is None else val
        result = self._eval_call("SRANDMEMBER", [key], [count])
        return [] if result is None else list(result)

    @override
    async def asrandmember(
        self,
        key: KeyT,
        count: int | None = None,
    ) -> Any | list[Any] | None:
        if count is None:
            val = await self._aeval_call("SRANDMEMBER", [key], [])
            return None if val is None else val
        result = await self._aeval_call("SRANDMEMBER", [key], [count])
        return [] if result is None else list(result)

    @override
    def sdiffstore(self, dest: KeyT, keys: Any) -> int:
        return int(self._eval_call("SDIFFSTORE", [dest, *_coerce_keys_arg(keys)], []))

    @override
    async def asdiffstore(self, dest: KeyT, keys: Any) -> int:
        return int(
            await self._aeval_call("SDIFFSTORE", [dest, *_coerce_keys_arg(keys)], []),
        )

    @override
    def sinterstore(self, dest: KeyT, keys: Any) -> int:
        return int(self._eval_call("SINTERSTORE", [dest, *_coerce_keys_arg(keys)], []))

    @override
    async def asinterstore(self, dest: KeyT, keys: Any) -> int:
        return int(
            await self._aeval_call("SINTERSTORE", [dest, *_coerce_keys_arg(keys)], []),
        )

    @override
    def sunionstore(self, dest: KeyT, keys: Any) -> int:
        return int(self._eval_call("SUNIONSTORE", [dest, *_coerce_keys_arg(keys)], []))

    @override
    async def asunionstore(self, dest: KeyT, keys: Any) -> int:
        return int(
            await self._aeval_call("SUNIONSTORE", [dest, *_coerce_keys_arg(keys)], []),
        )

    # ---- list ops the driver doesn't expose ----

    @override
    def lmove(self, src: KeyT, dst: KeyT, wherefrom: str = "LEFT", whereto: str = "RIGHT") -> Any | None:
        val = self._eval_call("LMOVE", [src, dst], [wherefrom.encode(), whereto.encode()])
        return None if val is None else val

    @override
    async def almove(
        self,
        src: KeyT,
        dst: KeyT,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
    ) -> Any | None:
        val = await self._aeval_call("LMOVE", [src, dst], [wherefrom.encode(), whereto.encode()])
        return None if val is None else val

    @override
    def lpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> int | list[int] | None:
        argv: list[Any] = [_value_to_bytes(value)]
        if rank is not None:
            argv.extend([b"RANK", rank])
        if count is not None:
            argv.extend([b"COUNT", count])
        if maxlen is not None:
            argv.extend([b"MAXLEN", maxlen])
        result = self._eval_call("LPOS", [key], argv)
        if result is None or count is None:
            return None if result is None else int(result)
        return [int(p) for p in result]

    @override
    async def alpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> int | list[int] | None:
        argv: list[Any] = [_value_to_bytes(value)]
        if rank is not None:
            argv.extend([b"RANK", rank])
        if count is not None:
            argv.extend([b"COUNT", count])
        if maxlen is not None:
            argv.extend([b"MAXLEN", maxlen])
        result = await self._aeval_call("LPOS", [key], argv)
        if result is None or count is None:
            return None if result is None else int(result)
        return [int(p) for p in result]

    # ---- streams: xadd needs signature translation, others use eval ----

    @override
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
    ) -> str:
        encoded = [(str(k), _value_to_bytes(v)) for k, v in fields.items()]
        if maxlen is None and minid is None and not nomkstream and limit is None:
            return self._driver.xadd(_str_key(key), entry_id, encoded)
        argv = _xadd_argv(entry_id, encoded, maxlen, approximate, nomkstream, minid, limit)
        result = self._eval_call("XADD", [key], argv)
        return result.decode() if isinstance(result, bytes) else result

    @override
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
    ) -> str:
        encoded = [(str(k), _value_to_bytes(v)) for k, v in fields.items()]
        if maxlen is None and minid is None and not nomkstream and limit is None:
            return await self._driver.axadd(_str_key(key), entry_id, encoded)
        argv = _xadd_argv(entry_id, encoded, maxlen, approximate, nomkstream, minid, limit)
        result = await self._aeval_call("XADD", [key], argv)
        return result.decode() if isinstance(result, bytes) else result

    # xgroup_create: drop the entries_read kwarg (Redis 7.0+) which the driver
    # method doesn't accept.

    @override
    def xgroup_create(
        self,
        key: KeyT,
        group: str,
        identifier: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> bool:
        self._driver.xgroup_create(_str_key(key), group, identifier, mkstream=mkstream)
        return True

    @override
    async def axgroup_create(
        self,
        key: KeyT,
        group: str,
        identifier: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> bool:
        await self._driver.axgroup_create(_str_key(key), group, identifier, mkstream=mkstream)
        return True

    # ---- blocking list ops ----

    @override
    def blpop(self, keys: Any, timeout: float = 0) -> tuple[str, Any] | None:
        raw = self._driver.blpop(_coerce_keys_arg(keys), float(timeout))
        return None if raw is None else (raw[0], raw[1])

    @override
    def brpop(self, keys: Any, timeout: float = 0) -> tuple[str, Any] | None:
        raw = self._driver.brpop(_coerce_keys_arg(keys), float(timeout))
        return None if raw is None else (raw[0], raw[1])

    @override
    async def ablpop(self, keys: Any, timeout: float = 0) -> tuple[str, Any] | None:
        raw = await self._driver.ablpop(_coerce_keys_arg(keys), float(timeout))
        return None if raw is None else (raw[0], raw[1])

    @override
    async def abrpop(self, keys: Any, timeout: float = 0) -> tuple[str, Any] | None:
        raw = await self._driver.abrpop(_coerce_keys_arg(keys), float(timeout))
        return None if raw is None else (raw[0], raw[1])

    @override
    def blmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
    ) -> Any | None:
        # Driver positional order is (src, dst, wherefrom, whereto, timeout_secs).
        val = self._driver.blmove(_str_key(src), _str_key(dst), wherefrom, whereto, float(timeout))
        return None if val is None else val

    @override
    async def ablmove(
        self,
        src: KeyT,
        dst: KeyT,
        timeout: float,
        wherefrom: str = "LEFT",
        whereto: str = "RIGHT",
    ) -> Any | None:
        val = await self._driver.ablmove(_str_key(src), _str_key(dst), wherefrom, whereto, float(timeout))
        return None if val is None else val

    # ---- streams: range/read/trim signature translations ----

    @override
    def xrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        raw = self._driver.xrange(_str_key(key), start, end, count=count)
        return _decode_xrange(raw)

    @override
    async def axrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        raw = await self._driver.axrange(_str_key(key), start, end, count=count)
        return _decode_xrange(raw)

    @override
    def xrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        argv: list[Any] = [end, start]
        if count is not None:
            argv.extend([b"COUNT", count])
        raw = self._eval_call("XREVRANGE", [key], argv)
        return _decode_xrange(raw)

    @override
    async def axrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        argv: list[Any] = [end, start]
        if count is not None:
            argv.extend([b"COUNT", count])
        raw = await self._aeval_call("XREVRANGE", [key], argv)
        return _decode_xrange(raw)

    @override
    def xread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        keys = [_str_key(k) for k in streams]
        ids = list(streams.values())
        return _decode_xread(self._driver.xread(keys, ids, count=count))

    @override
    async def axread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        keys = [_str_key(k) for k in streams]
        ids = list(streams.values())
        raw = await self._driver.axread(keys, ids, count=count)
        return _decode_xread(raw)

    @override
    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        keys = [_str_key(k) for k in streams]
        ids = list(streams.values())
        return _decode_xread(
            self._driver.xreadgroup(group, consumer, keys, ids, count=count),
        )

    @override
    async def axreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        keys = [_str_key(k) for k in streams]
        ids = list(streams.values())
        raw = await self._driver.axreadgroup(group, consumer, keys, ids, count=count)
        return _decode_xread(raw)

    @override
    def xtrim(
        self,
        key: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
    ) -> int:
        if minid is not None:
            argv: list[Any] = [b"MINID"]
            if approximate:
                argv.append(b"~")
            argv.append(minid)
            return int(self._eval_call("XTRIM", [key], argv))
        return int(self._driver.xtrim(_str_key(key), maxlen or 0, approximate))

    @override
    async def axtrim(
        self,
        key: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
    ) -> int:
        if minid is not None:
            argv: list[Any] = [b"MINID"]
            if approximate:
                argv.append(b"~")
            argv.append(minid)
            return int(await self._aeval_call("XTRIM", [key], argv))
        return int(await self._driver.axtrim(_str_key(key), maxlen or 0, approximate))

    # ---- streams: ops with no driver method, all via eval ----

    @override
    def xdel(self, key: KeyT, *entry_ids: str) -> int:
        if not entry_ids:
            return 0
        return int(self._eval_call("XDEL", [key], list(entry_ids)))

    @override
    async def axdel(self, key: KeyT, *entry_ids: str) -> int:
        if not entry_ids:
            return 0
        return int(await self._aeval_call("XDEL", [key], list(entry_ids)))

    @override
    def xack(self, key: KeyT, group: str, *entry_ids: str) -> int:
        if not entry_ids:
            return 0
        return int(self._driver.xack(_str_key(key), group, list(entry_ids)))

    @override
    async def axack(self, key: KeyT, group: str, *entry_ids: str) -> int:
        if not entry_ids:
            return 0
        return int(await self._driver.axack(_str_key(key), group, list(entry_ids)))

    @override
    def xlen(self, key: KeyT) -> int:
        return int(self._driver.xlen(_str_key(key)))

    @override
    async def axlen(self, key: KeyT) -> int:
        return int(await self._driver.axlen(_str_key(key)))

    # Multi-word commands (XINFO STREAM, XGROUP DESTROY, ...) can't go through
    # ``_eval_call`` because Lua's ``redis.call`` treats the first arg as a
    # single command name. Use a custom script that splits subcommand off.
    _EVAL_SUBCALL_TEMPLATE = (
        "local args = {{ARGV[1]}}; "
        "for _, v in ipairs(KEYS) do args[#args+1] = v end; "
        "for i = 2, #ARGV do args[#args+1] = ARGV[i] end; "
        "return redis.call('{cmd}', unpack(args))"
    )

    def _eval_subcall(
        self,
        command: str,
        sub: str,
        keys: Sequence[KeyT],
        args: Sequence[Any],
    ) -> Any:
        return self._driver.eval(
            self._EVAL_SUBCALL_TEMPLATE.format(cmd=command),
            [_str_key(k) for k in keys],
            [sub.encode("ascii"), *_eval_args(args)],
        )

    async def _aeval_subcall(
        self,
        command: str,
        sub: str,
        keys: Sequence[KeyT],
        args: Sequence[Any],
    ) -> Any:
        return await self._driver.aeval(
            self._EVAL_SUBCALL_TEMPLATE.format(cmd=command),
            [_str_key(k) for k in keys],
            [sub.encode("ascii"), *_eval_args(args)],
        )

    @override
    def xinfo_stream(self, key: KeyT, full: bool = False) -> dict[str, Any]:
        argv = [b"FULL"] if full else []
        return _parse_xinfo_pairs(self._eval_subcall("XINFO", "STREAM", [key], argv))

    @override
    async def axinfo_stream(self, key: KeyT, full: bool = False) -> dict[str, Any]:
        argv = [b"FULL"] if full else []
        return _parse_xinfo_pairs(await self._aeval_subcall("XINFO", "STREAM", [key], argv))

    @override
    def xinfo_groups(self, key: KeyT) -> list[dict[str, Any]]:
        result = self._eval_subcall("XINFO", "GROUPS", [key], [])
        return [_parse_xinfo_pairs(item) for item in (result or [])]

    @override
    async def axinfo_groups(self, key: KeyT) -> list[dict[str, Any]]:
        result = await self._aeval_subcall("XINFO", "GROUPS", [key], [])
        return [_parse_xinfo_pairs(item) for item in (result or [])]

    @override
    def xinfo_consumers(self, key: KeyT, group: str) -> list[dict[str, Any]]:
        result = self._eval_subcall("XINFO", "CONSUMERS", [key], [group])
        return [_parse_xinfo_pairs(item) for item in (result or [])]

    @override
    async def axinfo_consumers(self, key: KeyT, group: str) -> list[dict[str, Any]]:
        result = await self._aeval_subcall("XINFO", "CONSUMERS", [key], [group])
        return [_parse_xinfo_pairs(item) for item in (result or [])]

    @override
    def xgroup_destroy(self, key: KeyT, group: str) -> int:
        return int(self._eval_subcall("XGROUP", "DESTROY", [key], [group]))

    @override
    async def axgroup_destroy(self, key: KeyT, group: str) -> int:
        return int(await self._aeval_subcall("XGROUP", "DESTROY", [key], [group]))

    @override
    def xgroup_setid(
        self,
        key: KeyT,
        group: str,
        identifier: str,
        entries_read: int | None = None,
    ) -> bool:
        return self._eval_subcall("XGROUP", "SETID", [key], [group, identifier]) is True

    @override
    async def axgroup_setid(
        self,
        key: KeyT,
        group: str,
        identifier: str,
        entries_read: int | None = None,
    ) -> bool:
        return await self._aeval_subcall("XGROUP", "SETID", [key], [group, identifier]) is True

    @override
    def xgroup_delconsumer(self, key: KeyT, group: str, consumer: str) -> int:
        return int(self._eval_subcall("XGROUP", "DELCONSUMER", [key], [group, consumer]))

    @override
    async def axgroup_delconsumer(self, key: KeyT, group: str, consumer: str) -> int:
        return int(await self._aeval_subcall("XGROUP", "DELCONSUMER", [key], [group, consumer]))

    # ---- xpending / xclaim / xautoclaim: driver-backed, decoded here ----

    @override
    def xpending(
        self,
        key: KeyT,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if start is not None and end is not None and count is not None:
            raw = self._driver.xpending_range(
                _str_key(key),
                group,
                start,
                end,
                count,
                consumer=consumer,
                idle=idle,
            )
            return _decode_xpending_range(raw)
        raw = self._driver.xpending(_str_key(key), group)
        return _decode_xpending_summary(raw)

    @override
    async def axpending(
        self,
        key: KeyT,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if start is not None and end is not None and count is not None:
            raw = await self._driver.axpending_range(
                _str_key(key),
                group,
                start,
                end,
                count,
                consumer=consumer,
                idle=idle,
            )
            return _decode_xpending_range(raw)
        raw = await self._driver.axpending(_str_key(key), group)
        return _decode_xpending_summary(raw)

    @override
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
    ) -> list[tuple[str, dict[str, Any]]] | list[str]:
        raw = self._driver.xclaim(
            _str_key(key),
            group,
            consumer,
            min_idle_time,
            list(entry_ids),
            idle=idle,
            time_ms=time,
            retrycount=retrycount,
            force=force,
            justid=justid,
        )
        if justid:
            return [_decode_str(eid) for eid in (raw or [])]
        return _decode_xrange(raw)

    @override
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
    ) -> list[tuple[str, dict[str, Any]]] | list[str]:
        raw = await self._driver.axclaim(
            _str_key(key),
            group,
            consumer,
            min_idle_time,
            list(entry_ids),
            idle=idle,
            time_ms=time,
            retrycount=retrycount,
            force=force,
            justid=justid,
        )
        if justid:
            return [_decode_str(eid) for eid in (raw or [])]
        return _decode_xrange(raw)

    @override
    def xautoclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]] | list[str], list[str]]:
        raw = self._driver.xautoclaim(
            _str_key(key),
            group,
            consumer,
            min_idle_time,
            start_id,
            count=count,
            justid=justid,
        )
        return _decode_xautoclaim(raw, justid=justid)

    @override
    async def axautoclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> tuple[str, list[tuple[str, dict[str, Any]]] | list[str], list[str]]:
        raw = await self._driver.axautoclaim(
            _str_key(key),
            group,
            consumer,
            min_idle_time,
            start_id,
            count=count,
            justid=justid,
        )
        return _decode_xautoclaim(raw, justid=justid)

    # ---- scan iterators: single-shot via driver, no cursor exposed ----

    @override
    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, _Set[Any]]:
        argv: list[Any] = [cursor]
        if match is not None:
            argv.extend([b"MATCH", match])
        if count is not None:
            argv.extend([b"COUNT", count])
        result = self._eval_call("SSCAN", [key], argv)
        next_cursor = int(result[0]) if isinstance(result[0], (int, str, bytes)) else 0
        return next_cursor, set(result[1])

    @override
    async def asscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, _Set[Any]]:
        argv: list[Any] = [cursor]
        if match is not None:
            argv.extend([b"MATCH", match])
        if count is not None:
            argv.extend([b"COUNT", count])
        result = await self._aeval_call("SSCAN", [key], argv)
        next_cursor = int(result[0]) if isinstance(result[0], (int, str, bytes)) else 0
        return next_cursor, set(result[1])

    @override
    def sscan_iter(self, key: KeyT, match: str | None = None, count: int | None = None) -> Iterator[Any]:
        cursor = 0
        while True:
            cursor, batch = self.sscan(key, cursor, match=match, count=count)
            yield from batch
            if cursor == 0:
                return

    @override
    async def asscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int | None = None,
    ) -> AsyncIterator[Any]:
        cursor = 0
        while True:
            cursor, batch = await self.asscan(key, cursor, match=match, count=count)
            for item in batch:
                yield item
            if cursor == 0:
                return


def _parse_xinfo_pairs(raw: Any) -> dict[str, Any]:
    """Convert a flat XINFO field-pair response into a dict.

    XINFO returns alternating ``[name, value, name, value, ...]`` arrays in
    RESP2; RESP3 servers may return a map directly. Handle both.
    """
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {(k.decode() if isinstance(k, bytes) else k): v for k, v in raw.items()}
    out: dict[str, Any] = {}
    it = iter(raw)
    for k, v in zip(it, it, strict=False):
        key = k.decode() if isinstance(k, bytes) else k
        out[key] = v
    return out


def _parse_info(raw: str) -> dict[str, Any]:
    """Parse a Redis ``INFO`` bulk-string response into a flat ``dict``.

    Mirrors redis-py's parser closely enough for ``cache.info()`` callers.
    """
    info: dict[str, Any] = {}
    for line in raw.splitlines():
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        # Coerce numeric-looking values; leave the rest as strings.
        try:
            info[name] = int(value)
        except ValueError:
            try:
                info[name] = float(value)
            except ValueError:
                info[name] = value
    return info


def _select_info_section(raw: str, section: str) -> str:
    """Return only the ``# <section>`` block from a full INFO response."""
    target = section.strip().lower()
    out: list[str] = []
    in_section = False
    for line in raw.splitlines():
        if line.startswith("#"):
            in_section = line[1:].strip().lower() == target
            continue
        if in_section:
            out.append(line)
    return "\n".join(out)


# =============================================================================
# Topology-specific subclasses
# =============================================================================


class RedisRsValkeyClusterAdapter(RedisRsValkeyAdapter):
    """Rust driver client for Valkey/Redis cluster mode."""

    @override
    def _connect(self) -> RedisRsDriver:
        # Cluster URLs may be a comma-joined string in Django LOCATION; the
        # base ``KeyValueCache`` already splits them into ``self._servers``.
        return get_driver_cluster(list(self._servers), **self._driver_kwargs())

    # Inherit ``lock``/``alock`` from the parent. The lock script keys to a
    # single slot, so the Rust cluster driver routes correctly without
    # special-casing here.


class RedisRsValkeySentinelAdapter(RedisRsValkeyAdapter):
    """Rust driver client for sentinel-managed Valkey/Redis topologies."""

    @override
    def _connect(self) -> RedisRsDriver:
        # Django LOCATION is a list of valkey://service-name?db=N URLs. We
        # need a service name + db plus a list of sentinel hosts/ports
        # (provided in OPTIONS["sentinels"] as [(host, port), ...]).
        sentinels = self._options.get("sentinels", [])
        if not sentinels:
            msg = "Sentinel client requires OPTIONS['sentinels'] = [(host, port), ...]"
            raise ValueError(msg)
        sentinel_urls = [f"redis://{host}:{port}" for host, port in sentinels]

        first_url = self._servers[0]
        parsed = urlparse(first_url)
        service_name = cast("str", parsed.hostname)
        db_values = parse_qs(parsed.query).get("db", ["0"])
        try:
            db = int(db_values[0])
        except ValueError:
            db = 0
        return get_driver_sentinel(sentinel_urls, service_name, db, **self._driver_kwargs())


__all__ = [
    "RedisRsValkeyAdapter",
    "RedisRsValkeyClusterAdapter",
    "RedisRsValkeySentinelAdapter",
]
