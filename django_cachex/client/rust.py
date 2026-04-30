"""Cache client backed by the Rust ``RustValkeyDriver``.

Subclass of :class:`KeyValueCacheClient`. Reuses the serializer/compressor
stack and stampede prevention logic from the base, but routes every I/O
call to the Rust driver from ``_rust_clients`` instead of redis-py /
valkey-py. Each driver is process-shared via the registry; per-cache state
lives on the subclass instance.
"""

from __future__ import annotations

from itertools import batched
from typing import TYPE_CHECKING, Any, cast, override
from urllib.parse import parse_qs, urlparse

from django_cachex._rust_clients import (
    get_driver_cluster,
    get_driver_sentinel,
    get_driver_standard,
)
from django_cachex.client.default import KeyValueCacheClient
from django_cachex.exceptions import NotSupportedError
from django_cachex.lock import ValkeyLock
from django_cachex.stampede import should_recompute
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence

    from django_cachex._driver import RustValkeyDriver  # ty: ignore[unresolved-import]
    from django_cachex.client.pipeline import Pipeline
    from django_cachex.types import AbsExpiryT, ExpiryT, KeyT


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

    ``KeyValueCacheClient.encode()`` returns ``int`` for plain integers (so
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


class RustKeyValueCacheClient(KeyValueCacheClient):
    """Base Rust-driver client. Subclasses choose the topology."""

    # The base class uses these to instantiate redis-py pools/parsers; we
    # have no use for them and leave them as None so the redis-py code
    # paths stay disabled.
    _lib: Any = None
    _client_class = None
    _pool_class = None
    _async_client_class = None
    _async_pool_class = None

    # Option keys we recognize so the registry cache hits across cache
    # instances that share a driver but differ only in cosmetic options.
    _DRIVER_OPTION_KEYS = _DRIVER_KWARGS

    def __init__(self, servers: list[str], **options: Any) -> None:
        # Honor the base init for serializers/compressors/stampede config,
        # but skip its pool plumbing — we don't construct redis-py pools.
        super().__init__(servers, **options)

    # ------------------------------------------------------------------ hooks

    @property
    def _driver(self) -> RustValkeyDriver:
        # Always go through the registry so its PID-check rebuilds drivers
        # in post-fork children — caching on the instance would defeat that.
        return self._connect()

    def _connect(self) -> RustValkeyDriver:
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

    def get_raw_client(self) -> RustValkeyDriver:
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
        nvalue = _value_to_bytes(self.encode(value))
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            if self._driver.set_nx_sync(_str_key(key), nvalue, ttl=None):
                self._driver.delete_sync(_str_key(key))
                return True
            return False
        return bool(self._driver.set_nx_sync(_str_key(key), nvalue, ttl=actual_timeout))

    @override
    async def aadd(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool:
        nvalue = _value_to_bytes(self.encode(value))
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            if await self._driver.set_nx(_str_key(key), nvalue, ttl=None):
                await self._driver.delete(_str_key(key))
                return True
            return False
        return bool(await self._driver.set_nx(_str_key(key), nvalue, ttl=actual_timeout))

    @override
    def get(self, key: KeyT, *, stampede_prevention: bool | dict | None = None) -> Any:
        val = self._driver.get_sync(_str_key(key))
        if val is None:
            return None
        config = self._resolve_stampede(stampede_prevention)
        if config and isinstance(val, bytes):
            ttl = self._driver.ttl_sync(_str_key(key))
            if ttl > 0 and should_recompute(ttl, config):
                return None
        return self.decode(val)

    @override
    async def aget(self, key: KeyT, *, stampede_prevention: bool | dict | None = None) -> Any:
        val = await self._driver.get(_str_key(key))
        if val is None:
            return None
        config = self._resolve_stampede(stampede_prevention)
        if config and isinstance(val, bytes):
            ttl = await self._driver.ttl(_str_key(key))
            if ttl > 0 and should_recompute(ttl, config):
                return None
        return self.decode(val)

    @override
    def set(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> None:
        nvalue = _value_to_bytes(self.encode(value))
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            self._driver.delete_sync(_str_key(key))
        else:
            self._driver.set_sync(_str_key(key), nvalue, ttl=actual_timeout)

    @override
    async def aset(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> None:
        nvalue = _value_to_bytes(self.encode(value))
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            await self._driver.delete(_str_key(key))
        else:
            await self._driver.set(_str_key(key), nvalue, ttl=actual_timeout)

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
        if xx or get:
            msg = "RustKeyValueCacheClient does not yet support xx/get flags on set"
            raise NotSupportedError(msg)
        nvalue = _value_to_bytes(self.encode(value))
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            return False
        if nx:
            return bool(self._driver.set_nx_sync(_str_key(key), nvalue, ttl=actual_timeout))
        self._driver.set_sync(_str_key(key), nvalue, ttl=actual_timeout)
        return True

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
        if xx or get:
            msg = "RustKeyValueCacheClient does not yet support xx/get flags on set"
            raise NotSupportedError(msg)
        nvalue = _value_to_bytes(self.encode(value))
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            return False
        if nx:
            return bool(await self._driver.set_nx(_str_key(key), nvalue, ttl=actual_timeout))
        await self._driver.set(_str_key(key), nvalue, ttl=actual_timeout)
        return True

    @override
    def touch(self, key: KeyT, timeout: int | None) -> bool:
        if timeout is None:
            return bool(self._driver.persist_sync(_str_key(key)))
        return bool(self._driver.expire_sync(_str_key(key), timeout))

    @override
    async def atouch(self, key: KeyT, timeout: int | None) -> bool:
        if timeout is None:
            return bool(await self._driver.persist(_str_key(key)))
        return bool(await self._driver.expire(_str_key(key), timeout))

    @override
    def delete(self, key: KeyT) -> bool:
        return bool(self._driver.delete_sync(_str_key(key)))

    @override
    async def adelete(self, key: KeyT) -> bool:
        return bool(await self._driver.delete(_str_key(key)))

    @override
    def get_many(self, keys: Iterable[KeyT], *, stampede_prevention: bool | dict | None = None) -> dict[KeyT, Any]:
        keys_list = [_str_key(k) for k in keys]
        if not keys_list:
            return {}
        results = self._driver.mget_sync(keys_list)
        found = {k: v for k, v in zip(keys_list, results, strict=False) if v is not None}
        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            stampede_keys = [k for k, v in found.items() if isinstance(v, bytes)]
            for k in stampede_keys:
                ttl = self._driver.ttl_sync(k)
                if ttl > 0 and should_recompute(ttl, config):
                    del found[k]
        return {k: self.decode(v) for k, v in found.items()}

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
        results = await self._driver.mget(keys_list)
        found = {k: v for k, v in zip(keys_list, results, strict=False) if v is not None}
        config = self._resolve_stampede(stampede_prevention)
        if config and found:
            stampede_keys = [k for k, v in found.items() if isinstance(v, bytes)]
            for k in stampede_keys:
                ttl = await self._driver.ttl(k)
                if ttl > 0 and should_recompute(ttl, config):
                    del found[k]
        return {k: self.decode(v) for k, v in found.items()}

    @override
    def has_key(self, key: KeyT) -> bool:
        return bool(self._driver.exists_sync(_str_key(key)))

    @override
    async def ahas_key(self, key: KeyT) -> bool:
        return bool(await self._driver.exists(_str_key(key)))

    @override
    def type(self, key: KeyT) -> KeyType | None:
        result = self._driver.type_sync(_str_key(key))
        return None if result == "none" else KeyType(result)

    @override
    async def atype(self, key: KeyT) -> KeyType | None:
        result = await self._driver.type(_str_key(key))
        return None if result == "none" else KeyType(result)

    @override
    def incr(self, key: KeyT, delta: int = 1) -> int:
        return int(self._driver.incr_by_sync(_str_key(key), delta))

    @override
    async def aincr(self, key: KeyT, delta: int = 1) -> int:
        return int(await self._driver.incr_by(_str_key(key), delta))

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
        prepared = [(_str_key(k), _value_to_bytes(self.encode(v))) for k, v in data.items()]
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            self._driver.delete_many_sync([k for k, _ in prepared])
        else:
            self._driver.pipeline_set_sync(prepared, ttl=actual_timeout)
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
        prepared = [(_str_key(k), _value_to_bytes(self.encode(v))) for k, v in data.items()]
        actual_timeout = self._get_timeout_with_buffer(timeout, stampede_prevention)
        if actual_timeout == 0:
            await self._driver.delete_many([k for k, _ in prepared])
        else:
            await self._driver.pipeline_set(prepared, ttl=actual_timeout)
        return []

    @override
    def delete_many(self, keys: Sequence[KeyT]) -> int:
        if not keys:
            return 0
        return int(self._driver.delete_many_sync([_str_key(k) for k in keys]))

    @override
    async def adelete_many(self, keys: Sequence[KeyT]) -> int:
        if not keys:
            return 0
        return int(await self._driver.delete_many([_str_key(k) for k in keys]))

    @override
    def clear(self) -> bool:
        self._driver.flushdb_sync()
        return True

    @override
    async def aclear(self) -> bool:
        await self._driver.flushdb()
        return True

    # ------------------------------------------------------------------- TTL

    @override
    def ttl(self, key: KeyT) -> int | None:
        return self._normalize_ttl(self._driver.ttl_sync(_str_key(key)))

    @override
    def pttl(self, key: KeyT) -> int | None:
        return self._normalize_ttl(self._driver.pttl_sync(_str_key(key)))

    @staticmethod
    def _to_seconds(timeout: ExpiryT) -> int:
        if isinstance(timeout, int):
            return timeout
        return int(timeout.total_seconds())

    @staticmethod
    def _to_unix(when: AbsExpiryT) -> int:
        if isinstance(when, int):
            return when
        return int(when.timestamp())

    @override
    def expire(self, key: KeyT, timeout: ExpiryT) -> bool:
        return bool(self._driver.expire_sync(_str_key(key), self._to_seconds(timeout)))

    @override
    def persist(self, key: KeyT) -> bool:
        return bool(self._driver.persist_sync(_str_key(key)))

    @override
    async def attl(self, key: KeyT) -> int | None:
        return self._normalize_ttl(await self._driver.ttl(_str_key(key)))

    @override
    async def apttl(self, key: KeyT) -> int | None:
        return self._normalize_ttl(await self._driver.pttl(_str_key(key)))

    @override
    async def aexpire(self, key: KeyT, timeout: ExpiryT) -> bool:
        return bool(await self._driver.expire(_str_key(key), self._to_seconds(timeout)))

    @override
    async def apersist(self, key: KeyT) -> bool:
        return bool(await self._driver.persist(_str_key(key)))

    # The driver currently exposes EXPIRE/PERSIST/TTL/PTTL only. The remaining
    # TTL ops (PEXPIRE / EXPIREAT / PEXPIREAT / EXPIRETIME) are implemented via
    # raw command via ``eval`` to keep parity with the redis-py surface.

    def _expire_via_eval(self, command: str, key: KeyT, value: int) -> bool:
        result = self._driver.eval_sync(
            f"return redis.call('{command}', KEYS[1], ARGV[1])",
            [_str_key(key)],
            [str(int(value)).encode("ascii")],
        )
        return bool(result)

    async def _aexpire_via_eval(self, command: str, key: KeyT, value: int) -> bool:
        result = await self._driver.eval(
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
        return self._expire_via_eval("EXPIREAT", key, self._to_unix(when))

    @override
    def pexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        if isinstance(when, int):
            ms = when
        else:
            ms = int(when.timestamp() * 1000)
        return self._expire_via_eval("PEXPIREAT", key, ms)

    @override
    def expiretime(self, key: KeyT) -> int | None:
        result = self._driver.eval_sync(
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
        return await self._aexpire_via_eval("EXPIREAT", key, self._to_unix(when))

    @override
    async def apexpireat(self, key: KeyT, when: AbsExpiryT) -> bool:
        if isinstance(when, int):
            ms = when
        else:
            ms = int(when.timestamp() * 1000)
        return await self._aexpire_via_eval("PEXPIREAT", key, ms)

    @override
    async def aexpiretime(self, key: KeyT) -> int | None:
        result = await self._driver.eval(
            "return redis.call('EXPIRETIME', KEYS[1])",
            [_str_key(key)],
            [],
        )
        return self._normalize_ttl(int(result))

    # ---------------------------------------------------------- pattern / scan

    @override
    def keys(self, pattern: str) -> list[str]:
        result = self._driver.keys_sync(pattern)
        return [k.decode() if isinstance(k, bytes) else k for k in result]

    @override
    async def akeys(self, pattern: str) -> list[str]:
        result = await self._driver.keys(pattern)
        return [k.decode() if isinstance(k, bytes) else k for k in result]

    @override
    def iter_keys(self, pattern: str, itersize: int | None = None) -> Iterator[str]:
        if itersize is None:
            itersize = self._default_scan_itersize
        keys = self._driver.scan_sync(pattern, itersize)
        for k in keys:
            yield k.decode() if isinstance(k, bytes) else k

    @override
    async def aiter_keys(self, pattern: str, itersize: int | None = None) -> AsyncIterator[str]:
        if itersize is None:
            itersize = self._default_scan_itersize
        keys = await self._driver.scan(pattern, itersize)
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
        # The driver collapses SCAN into a single batched call (no cursor
        # exposed). Returning cursor=0 signals "no more pages".
        if count is None:
            count = self._default_scan_itersize
        keys = self._driver.scan_sync(match or "*", count)
        return 0, [k.decode() if isinstance(k, bytes) else k for k in keys]

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
        keys = await self._driver.scan(match or "*", count)
        return 0, [k.decode() if isinstance(k, bytes) else k for k in keys]

    @override
    def delete_pattern(self, pattern: str, itersize: int | None = None) -> int:
        if itersize is None:
            itersize = self._default_scan_itersize
        count = 0
        for batch in batched(self.iter_keys(pattern, itersize=itersize), itersize, strict=False):
            count += int(self._driver.delete_many_sync(list(batch)))
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
                count += int(await self._driver.delete_many(batch))
                batch = []
        if batch:
            count += int(await self._driver.delete_many(batch))
        return count

    @override
    def rename(self, src: KeyT, dst: KeyT) -> bool:
        try:
            result = self._driver.eval_sync(
                "return redis.call('RENAME', KEYS[1], KEYS[2])",
                [_str_key(src), _str_key(dst)],
                [],
            )
        except RuntimeError as e:
            if "no such key" in str(e).lower():
                raise ValueError(f"Key {src!r} not found") from None
            raise
        if isinstance(result, bytes):
            result = result.decode()
        return result == "OK"

    @override
    def renamenx(self, src: KeyT, dst: KeyT) -> bool:
        try:
            result = self._driver.eval_sync(
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
            result = await self._driver.eval(
                "return redis.call('RENAME', KEYS[1], KEYS[2])",
                [_str_key(src), _str_key(dst)],
                [],
            )
        except RuntimeError as e:
            if "no such key" in str(e).lower():
                raise ValueError(f"Key {src!r} not found") from None
            raise
        if isinstance(result, bytes):
            result = result.decode()
        return result == "OK"

    @override
    async def arenamenx(self, src: KeyT, dst: KeyT) -> bool:
        try:
            result = await self._driver.eval(
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
        return ValkeyLock(
            self._driver,
            key,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    # ----------------------------------------------------------------- admin

    @override
    def info(self, section: str | None = None) -> dict[str, Any]:
        # The driver fetches the full INFO bulk string; if a section was
        # requested, slice client-side using the "# <Section>" headers.
        raw = self._driver.info_sync()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if section is not None:
            raw = _select_info_section(raw, section)
        return _parse_info(raw)

    # =========================================================================
    # Pipeline (deferred — driver-level batching needs a Python wrapper)
    # =========================================================================

    @override
    def pipeline(
        self,
        *,
        transaction: bool = True,
        version: int | None = None,
    ) -> Pipeline:
        msg = "RustKeyValueCacheClient does not yet support pipelines; use the redis-py-backed client."
        raise NotSupportedError(msg)

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
            pairs.append((str(field), _value_to_bytes(self.encode(value))))
        if mapping:
            pairs.extend((str(k), _value_to_bytes(self.encode(v))) for k, v in mapping.items())
        if items:
            it = iter(items)
            for f, v in zip(it, it, strict=False):
                pairs.append((str(f), _value_to_bytes(self.encode(v))))
        if not pairs:
            return 0
        if len(pairs) == 1:
            return int(self._driver.hset_sync(_str_key(key), pairs[0][0], pairs[0][1]))
        # HMSET returns OK, not a count. Match HSET semantics: number of new fields.
        before = self._driver.hlen_sync(_str_key(key))
        self._driver.hmset_sync(_str_key(key), pairs)
        after = self._driver.hlen_sync(_str_key(key))
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
            pairs.append((str(field), _value_to_bytes(self.encode(value))))
        if mapping:
            pairs.extend((str(k), _value_to_bytes(self.encode(v))) for k, v in mapping.items())
        if items:
            it = iter(items)
            for f, v in zip(it, it, strict=False):
                pairs.append((str(f), _value_to_bytes(self.encode(v))))
        if not pairs:
            return 0
        if len(pairs) == 1:
            ret = await self._driver.hset(_str_key(key), pairs[0][0], pairs[0][1])
            return int(ret)
        before = await self._driver.hlen(_str_key(key))
        await self._driver.hmset(_str_key(key), pairs)
        after = await self._driver.hlen(_str_key(key))
        return int(after) - int(before)

    @override
    def hsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        # No native driver method — go via eval.
        result = self._driver.eval_sync(
            "return redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])",
            [_str_key(key)],
            [str(field).encode("utf-8"), _value_to_bytes(self.encode(value))],
        )
        return bool(result)

    @override
    async def ahsetnx(self, key: KeyT, field: str, value: Any) -> bool:
        result = await self._driver.eval(
            "return redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])",
            [_str_key(key)],
            [str(field).encode("utf-8"), _value_to_bytes(self.encode(value))],
        )
        return bool(result)

    @override
    def hget(self, key: KeyT, field: str) -> Any | None:
        val = self._driver.hget_sync(_str_key(key), str(field))
        return None if val is None else self.decode(val)

    @override
    async def ahget(self, key: KeyT, field: str) -> Any | None:
        val = await self._driver.hget(_str_key(key), str(field))
        return None if val is None else self.decode(val)

    @override
    def hmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        if not fields:
            return []
        result = self._driver.hmget_sync(_str_key(key), [str(f) for f in fields])
        return [None if v is None else self.decode(v) for v in result]

    @override
    async def ahmget(self, key: KeyT, *fields: str) -> list[Any | None]:
        if not fields:
            return []
        result = await self._driver.hmget(_str_key(key), [str(f) for f in fields])
        return [None if v is None else self.decode(v) for v in result]

    @override
    def hgetall(self, key: KeyT) -> dict[str, Any]:
        result = self._driver.hgetall_sync(_str_key(key))
        return {(k.decode() if isinstance(k, bytes) else k): self.decode(v) for k, v in result.items()}

    @override
    async def ahgetall(self, key: KeyT) -> dict[str, Any]:
        result = await self._driver.hgetall(_str_key(key))
        return {(k.decode() if isinstance(k, bytes) else k): self.decode(v) for k, v in result.items()}

    @override
    def hdel(self, key: KeyT, *fields: str) -> int:
        if not fields:
            return 0
        return int(self._driver.hdel_sync(_str_key(key), [str(f) for f in fields]))

    @override
    async def ahdel(self, key: KeyT, *fields: str) -> int:
        if not fields:
            return 0
        return int(await self._driver.hdel(_str_key(key), [str(f) for f in fields]))

    @override
    def hexists(self, key: KeyT, field: str) -> bool:
        return bool(self._driver.hexists_sync(_str_key(key), str(field)))

    @override
    async def ahexists(self, key: KeyT, field: str) -> bool:
        return bool(await self._driver.hexists(_str_key(key), str(field)))

    @override
    def hlen(self, key: KeyT) -> int:
        return int(self._driver.hlen_sync(_str_key(key)))

    @override
    async def ahlen(self, key: KeyT) -> int:
        return int(await self._driver.hlen(_str_key(key)))

    @override
    def hkeys(self, key: KeyT) -> list[str]:
        result = self._driver.hkeys_sync(_str_key(key))
        return [k.decode() if isinstance(k, bytes) else k for k in result]

    @override
    async def ahkeys(self, key: KeyT) -> list[str]:
        result = await self._driver.hkeys(_str_key(key))
        return [k.decode() if isinstance(k, bytes) else k for k in result]

    @override
    def hvals(self, key: KeyT) -> list[Any]:
        result = self._driver.hvals_sync(_str_key(key))
        return [self.decode(v) for v in result]

    @override
    async def ahvals(self, key: KeyT) -> list[Any]:
        result = await self._driver.hvals(_str_key(key))
        return [self.decode(v) for v in result]

    @override
    def hincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        return int(self._driver.hincrby_sync(_str_key(key), str(field), amount))

    @override
    async def ahincrby(self, key: KeyT, field: str, amount: int = 1) -> int:
        return int(await self._driver.hincrby(_str_key(key), str(field), amount))

    @override
    def hincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0) -> float:
        result = self._driver.eval_sync(
            "return redis.call('HINCRBYFLOAT', KEYS[1], ARGV[1], ARGV[2])",
            [_str_key(key)],
            [str(field).encode("utf-8"), str(amount).encode("utf-8")],
        )
        if isinstance(result, bytes):
            result = result.decode()
        return float(result)

    @override
    async def ahincrbyfloat(self, key: KeyT, field: str, amount: float = 1.0) -> float:
        result = await self._driver.eval(
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
        return int(self._driver.lpush_sync(_str_key(key), [_value_to_bytes(self.encode(v)) for v in values]))

    @override
    def rpush(self, key: KeyT, *values: Any) -> int:
        if not values:
            return self.llen(key)
        return int(self._driver.rpush_sync(_str_key(key), [_value_to_bytes(self.encode(v)) for v in values]))

    @override
    async def alpush(self, key: KeyT, *values: Any) -> int:
        if not values:
            return await self.allen(key)
        return int(await self._driver.lpush(_str_key(key), [_value_to_bytes(self.encode(v)) for v in values]))

    @override
    async def arpush(self, key: KeyT, *values: Any) -> int:
        if not values:
            return await self.allen(key)
        return int(await self._driver.rpush(_str_key(key), [_value_to_bytes(self.encode(v)) for v in values]))

    @override
    def lpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is not None:
            msg = "RustKeyValueCacheClient.lpop does not support the count argument"
            raise NotSupportedError(msg)
        val = self._driver.lpop_sync(_str_key(key))
        return None if val is None else self.decode(val)

    @override
    def rpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is not None:
            msg = "RustKeyValueCacheClient.rpop does not support the count argument"
            raise NotSupportedError(msg)
        val = self._driver.rpop_sync(_str_key(key))
        return None if val is None else self.decode(val)

    @override
    async def alpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is not None:
            msg = "RustKeyValueCacheClient.alpop does not support the count argument"
            raise NotSupportedError(msg)
        val = await self._driver.lpop(_str_key(key))
        return None if val is None else self.decode(val)

    @override
    async def arpop(self, key: KeyT, count: int | None = None) -> Any | list[Any] | None:
        if count is not None:
            msg = "RustKeyValueCacheClient.arpop does not support the count argument"
            raise NotSupportedError(msg)
        val = await self._driver.rpop(_str_key(key))
        return None if val is None else self.decode(val)

    @override
    def llen(self, key: KeyT) -> int:
        return int(self._driver.llen_sync(_str_key(key)))

    @override
    async def allen(self, key: KeyT) -> int:
        return int(await self._driver.llen(_str_key(key)))

    @override
    def lrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        result = self._driver.lrange_sync(_str_key(key), start, end)
        return [self.decode(v) for v in result]

    @override
    async def alrange(self, key: KeyT, start: int, end: int) -> list[Any]:
        result = await self._driver.lrange(_str_key(key), start, end)
        return [self.decode(v) for v in result]

    @override
    def lindex(self, key: KeyT, index: int) -> Any | None:
        val = self._driver.lindex_sync(_str_key(key), index)
        return None if val is None else self.decode(val)

    @override
    async def alindex(self, key: KeyT, index: int) -> Any | None:
        val = await self._driver.lindex(_str_key(key), index)
        return None if val is None else self.decode(val)

    @override
    def lset(self, key: KeyT, index: int, value: Any) -> bool:
        self._driver.lset_sync(_str_key(key), index, _value_to_bytes(self.encode(value)))
        return True

    @override
    async def alset(self, key: KeyT, index: int, value: Any) -> bool:
        await self._driver.lset(_str_key(key), index, _value_to_bytes(self.encode(value)))
        return True

    @override
    def lrem(self, key: KeyT, count: int, value: Any) -> int:
        return int(
            self._driver.lrem_sync(_str_key(key), count, _value_to_bytes(self.encode(value))),
        )

    @override
    async def alrem(self, key: KeyT, count: int, value: Any) -> int:
        return int(
            await self._driver.lrem(_str_key(key), count, _value_to_bytes(self.encode(value))),
        )

    @override
    def ltrim(self, key: KeyT, start: int, end: int) -> bool:
        self._driver.ltrim_sync(_str_key(key), start, end)
        return True

    @override
    async def altrim(self, key: KeyT, start: int, end: int) -> bool:
        await self._driver.ltrim(_str_key(key), start, end)
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
            self._driver.linsert_sync(
                _str_key(key),
                before=before,
                pivot=_value_to_bytes(self.encode(pivot)),
                value=_value_to_bytes(self.encode(value)),
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
            await self._driver.linsert(
                _str_key(key),
                before=before,
                pivot=_value_to_bytes(self.encode(pivot)),
                value=_value_to_bytes(self.encode(value)),
            ),
        )

    # =========================================================================
    # Sets
    # =========================================================================

    @override
    def sadd(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(self._driver.sadd_sync(_str_key(key), [_value_to_bytes(self.encode(m)) for m in members]))

    @override
    async def asadd(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(await self._driver.sadd(_str_key(key), [_value_to_bytes(self.encode(m)) for m in members]))

    @override
    def srem(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(self._driver.srem_sync(_str_key(key), [_value_to_bytes(self.encode(m)) for m in members]))

    @override
    async def asrem(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(await self._driver.srem(_str_key(key), [_value_to_bytes(self.encode(m)) for m in members]))

    @override
    def smembers(self, key: KeyT) -> Any:
        result = self._driver.smembers_sync(_str_key(key))
        return {self.decode(m) for m in result}

    @override
    async def asmembers(self, key: KeyT) -> Any:
        result = await self._driver.smembers(_str_key(key))
        return {self.decode(m) for m in result}

    @override
    def sismember(self, key: KeyT, member: Any) -> bool:
        return bool(
            self._driver.sismember_sync(_str_key(key), _value_to_bytes(self.encode(member))),
        )

    @override
    async def asismember(self, key: KeyT, member: Any) -> bool:
        return bool(
            await self._driver.sismember(_str_key(key), _value_to_bytes(self.encode(member))),
        )

    @override
    def scard(self, key: KeyT) -> int:
        return int(self._driver.scard_sync(_str_key(key)))

    @override
    async def ascard(self, key: KeyT) -> int:
        return int(await self._driver.scard(_str_key(key)))

    @staticmethod
    def _coerce_keys_arg(keys: Any) -> list[str]:
        if isinstance(keys, (str, bytes)):
            return [_str_key(keys)]
        return [_str_key(k) for k in keys]

    @override
    def sinter(self, keys: Any) -> Any:
        result = self._driver.sinter_sync(self._coerce_keys_arg(keys))
        return {self.decode(m) for m in result}

    @override
    async def asinter(self, keys: Any) -> Any:
        result = await self._driver.sinter(self._coerce_keys_arg(keys))
        return {self.decode(m) for m in result}

    @override
    def sunion(self, keys: Any) -> Any:
        result = self._driver.sunion_sync(self._coerce_keys_arg(keys))
        return {self.decode(m) for m in result}

    @override
    async def asunion(self, keys: Any) -> Any:
        result = await self._driver.sunion(self._coerce_keys_arg(keys))
        return {self.decode(m) for m in result}

    @override
    def sdiff(self, keys: Any) -> Any:
        result = self._driver.sdiff_sync(self._coerce_keys_arg(keys))
        return {self.decode(m) for m in result}

    @override
    async def asdiff(self, keys: Any) -> Any:
        result = await self._driver.sdiff(self._coerce_keys_arg(keys))
        return {self.decode(m) for m in result}

    # =========================================================================
    # Sorted sets
    # =========================================================================

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
        if nx or xx or gt or lt or ch:
            msg = "RustKeyValueCacheClient.zadd does not yet support nx/xx/gt/lt/ch flags"
            raise NotSupportedError(msg)
        pairs = [(_value_to_bytes(self.encode(m)), float(s)) for m, s in mapping.items()]
        if not pairs:
            return 0
        return int(self._driver.zadd_sync(_str_key(key), pairs))

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
        if nx or xx or gt or lt or ch:
            msg = "RustKeyValueCacheClient.zadd does not yet support nx/xx/gt/lt/ch flags"
            raise NotSupportedError(msg)
        pairs = [(_value_to_bytes(self.encode(m)), float(s)) for m, s in mapping.items()]
        if not pairs:
            return 0
        return int(await self._driver.zadd(_str_key(key), pairs))

    @override
    def zrem(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(self._driver.zrem_sync(_str_key(key), [_value_to_bytes(self.encode(m)) for m in members]))

    @override
    async def azrem(self, key: KeyT, *members: Any) -> int:
        if not members:
            return 0
        return int(await self._driver.zrem(_str_key(key), [_value_to_bytes(self.encode(m)) for m in members]))

    @override
    def zscore(self, key: KeyT, member: Any) -> float | None:
        return self._driver.zscore_sync(_str_key(key), _value_to_bytes(self.encode(member)))

    @override
    async def azscore(self, key: KeyT, member: Any) -> float | None:
        return await self._driver.zscore(_str_key(key), _value_to_bytes(self.encode(member)))

    @override
    def zrank(self, key: KeyT, member: Any) -> int | None:
        result = self._driver.zrank_sync(_str_key(key), _value_to_bytes(self.encode(member)))
        return None if result is None else int(result)

    @override
    async def azrank(self, key: KeyT, member: Any) -> int | None:
        result = await self._driver.zrank(_str_key(key), _value_to_bytes(self.encode(member)))
        return None if result is None else int(result)

    @override
    def zcard(self, key: KeyT) -> int:
        return int(self._driver.zcard_sync(_str_key(key)))

    @override
    async def azcard(self, key: KeyT) -> int:
        return int(await self._driver.zcard(_str_key(key)))

    @override
    def zcount(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        return int(self._driver.zcount_sync(_str_key(key), str(min_score), str(max_score)))

    @override
    async def azcount(self, key: KeyT, min_score: float | str, max_score: float | str) -> int:
        return int(await self._driver.zcount(_str_key(key), str(min_score), str(max_score)))

    @override
    def zincrby(self, key: KeyT, amount: float, member: Any) -> float:
        return float(
            self._driver.zincrby_sync(_str_key(key), _value_to_bytes(self.encode(member)), float(amount)),
        )

    @override
    async def azincrby(self, key: KeyT, amount: float, member: Any) -> float:
        return float(
            await self._driver.zincrby(_str_key(key), _value_to_bytes(self.encode(member)), float(amount)),
        )

    def _decode_zrange(self, raw: list[Any], *, withscores: bool) -> list[Any]:
        if withscores:
            return [(self.decode(member), float(score)) for member, score in raw]
        return [self.decode(m) for m in raw]

    @override
    def zrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any]:
        raw = self._driver.zrange_sync(_str_key(key), start, end, withscores)
        return self._decode_zrange(raw, withscores=withscores)

    @override
    async def azrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any]:
        raw = await self._driver.zrange(_str_key(key), start, end, withscores)
        return self._decode_zrange(raw, withscores=withscores)

    @override
    def zrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any]:
        raw = self._driver.zrevrange_sync(_str_key(key), start, end, withscores)
        return self._decode_zrange(raw, withscores=withscores)

    @override
    async def azrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
    ) -> list[Any]:
        raw = await self._driver.zrevrange(_str_key(key), start, end, withscores)
        return self._decode_zrange(raw, withscores=withscores)

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
        raw = self._driver.zrangebyscore_sync(
            _str_key(key),
            str(min_score),
            str(max_score),
            withscores,
        )
        decoded = self._decode_zrange(raw, withscores=withscores)
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
        raw = await self._driver.zrangebyscore(
            _str_key(key),
            str(min_score),
            str(max_score),
            withscores,
        )
        decoded = self._decode_zrange(raw, withscores=withscores)
        if start is not None or num is not None:
            offset = start or 0
            decoded = decoded[offset : offset + num] if num is not None else decoded[offset:]
        return decoded

    @override
    def zpopmin(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        raw = self._driver.zpopmin_sync(_str_key(key), count)
        return [(self.decode(m), float(s)) for m, s in raw]

    @override
    async def azpopmin(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        raw = await self._driver.zpopmin(_str_key(key), count)
        return [(self.decode(m), float(s)) for m, s in raw]

    @override
    def zpopmax(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        raw = self._driver.zpopmax_sync(_str_key(key), count)
        return [(self.decode(m), float(s)) for m, s in raw]

    @override
    async def azpopmax(self, key: KeyT, count: int = 1) -> list[tuple[Any, float]]:
        raw = await self._driver.zpopmax(_str_key(key), count)
        return [(self.decode(m), float(s)) for m, s in raw]

    # =========================================================================
    # Scripts
    # =========================================================================

    @staticmethod
    def _eval_arg(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        # Match redis-py: bool serializes as the integer 0/1, not "True"/"False".
        if isinstance(value, bool):
            return b"1" if value else b"0"
        if isinstance(value, int):
            return str(value).encode("ascii")
        return str(value).encode("utf-8")

    @override
    def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        keys = [_str_key(k) for k in keys_and_args[:numkeys]]
        args = [self._eval_arg(a) for a in keys_and_args[numkeys:]]
        return self._driver.eval_sync(script, keys, args)

    @override
    async def aeval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        keys = [_str_key(k) for k in keys_and_args[:numkeys]]
        args = [self._eval_arg(a) for a in keys_and_args[numkeys:]]
        return await self._driver.eval(script, keys, args)


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


class RustValkeyClusterCacheClient(RustKeyValueCacheClient):
    """Rust driver client for Valkey/Redis cluster mode."""

    @override
    def _connect(self) -> RustValkeyDriver:
        # Cluster URLs may be a comma-joined string in Django LOCATION; the
        # base ``KeyValueCache`` already splits them into ``self._servers``.
        return get_driver_cluster(list(self._servers), **self._driver_kwargs())

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
        msg = "Distributed locks are not supported on cluster (Lua scripts route to a single slot)."
        raise NotImplementedError(msg)

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
        msg = "Distributed locks are not supported on cluster (Lua scripts route to a single slot)."
        raise NotImplementedError(msg)


class RustValkeySentinelCacheClient(RustKeyValueCacheClient):
    """Rust driver client for sentinel-managed Valkey/Redis topologies."""

    @override
    def _connect(self) -> RustValkeyDriver:
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


# Aliases — vendor names are interchangeable from the driver's perspective.
RustValkeyCacheClient = RustKeyValueCacheClient
RustRedisCacheClient = RustKeyValueCacheClient
RustRedisClusterCacheClient = RustValkeyClusterCacheClient
RustRedisSentinelCacheClient = RustValkeySentinelCacheClient


__all__ = [
    "RustKeyValueCacheClient",
    "RustRedisCacheClient",
    "RustRedisClusterCacheClient",
    "RustRedisSentinelCacheClient",
    "RustValkeyCacheClient",
    "RustValkeyClusterCacheClient",
    "RustValkeySentinelCacheClient",
]
