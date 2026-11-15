"""S3 cache backend for large values, on S3 Express One Zone or S3 Standard buckets.

Every cache key is one object under the ``LOCATION`` prefix. The body holds
the encoded value and nothing else; three user metadata fields describe it:

* ``cachex-expires``: the expiry as Unix seconds; absent when the value never expires.
* ``cachex-raw``: the value was ``bytes`` and skipped the serializer.
* ``cachex-compressed``: the configured compressor compressed the body.

S3 has no per-key TTL, so an object past ``cachex-expires`` reads as a miss
and stays in the bucket until a lifecycle rule or a later write removes it.
Every write is unconditional, so the last ``set()`` wins.
"""

import asyncio
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import TYPE_CHECKING, Any, override
from urllib.parse import urlsplit

from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.core.exceptions import ImproperlyConfigured

from django_cachex.cache._codec import ValueCodec
from django_cachex.cache.base import _USE_RESP, BaseCachex
from django_cachex.exceptions import CompressorError, NotSupportedError

try:
    import obstore
    from obstore.store import S3Store
except ImportError as _exc:
    _OBSTORE_IMPORT_ERROR: ImportError | None = _exc
else:
    _OBSTORE_IMPORT_ERROR = None

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping

    from obstore import GetResult

_EXPIRES = "cachex-expires"
_RAW = "cachex-raw"
_COMPRESSED = "cachex-compressed"

# S3 caps an object key, prefix included, at 1024 bytes of UTF-8.
_MAX_KEY_BYTES = 1024

# obstore rejects "/" segments that are empty, "." or "..", and control
# characters. Escaping "%" first keeps the mapping from cache keys injective.
_ESCAPES = {c: f"%{c:02X}" for c in (*range(0x20), ord("%"), ord("/"), 0x7F)}

_OPTION_DEFAULTS = {"serializer": None, "compressor": None, "max_concurrency": 64}

# obstore's own defaults (30 s per request, 10 retries within 3 minutes) would
# hold a web request for minutes while S3 is down. User entries win per key.
_CLIENT_DEFAULTS = {"timeout": "5s", "connect_timeout": "1s"}
_RETRY_DEFAULTS = {"max_retries": 2, "retry_timeout": timedelta(seconds=10)}

# obstore's runtime does not survive fork(): after this process has sent a
# request, every request in a forked child hangs, even on a new store.
_requesting_pid: int | None = None


def _check_fork() -> None:
    global _requesting_pid  # noqa: PLW0603
    pid = os.getpid()
    if _requesting_pid is None:
        _requesting_pid = pid
    elif _requesting_pid != pid:
        msg = (
            "S3Cache sent requests in a parent process before os.fork(); obstore hangs on every request "
            "in the child. Make no S3 cache calls before the fork (gunicorn --preload, Celery prefork)."
        )
        raise RuntimeError(msg)


def _expired(expires: float | None) -> bool:
    return expires is not None and expires <= time.time()


def _live(attributes: Mapping[str, Any]) -> bool:
    expires = attributes.get(_EXPIRES)
    return expires is None or float(expires) > time.time()


# One store per configuration, shared by the per-thread instances Django
# creates, so connections are reused.
_stores: dict[str, S3Store] = {}
_stores_lock = threading.Lock()


# One pool per calling thread and size. In a pool that threads share, a
# many-key call waits for the requests of every call before it while S3 stalls.
class _Pools(threading.local):
    def __init__(self) -> None:
        self.by_size: dict[int, ThreadPoolExecutor] = {}


_pools = _Pools()


def _shared_store(location: str, options: dict[str, Any]) -> S3Store:
    registry_key = repr((location, sorted(options.items())))
    with _stores_lock:
        store = _stores.get(registry_key)
        if store is None:
            if "s3_express" not in options and urlsplit(location).netloc.endswith("--x-s3"):
                options = {**options, "s3_express": True}
            try:
                store = S3Store.from_url(location, **options)
            except BaseException as e:
                # obstore panics on some misplaced entries, such as a top-level "allow_http",
                # and pyo3's PanicException derives from BaseException.
                if not isinstance(e, Exception) and type(e).__name__ != "PanicException":
                    raise
                msg = f"S3Cache could not create a store for LOCATION {location!r}: {e}"
                raise ImproperlyConfigured(msg) from e
            _stores[registry_key] = store
        return store


class S3Cache(ValueCodec, BaseCachex):
    """Django cache backend that stores each key as one object in an S3 bucket.

    ``LOCATION`` is ``s3://<bucket>/<prefix>``. ``OPTIONS`` takes
    ``serializer``, ``compressor`` and ``max_concurrency`` (the parallel
    requests of one ``get_many()`` or ``set_many()``); every other entry goes
    to obstore's ``S3Store.from_url()``.
    """

    def __init__(self, location: str, params: dict[str, Any]) -> None:
        super().__init__(params)
        if _OBSTORE_IMPORT_ERROR is not None:
            msg = "obstore is not installed. Install with the `s3` extra: pip install django-cachex[s3]"
            raise ImportError(msg) from _OBSTORE_IMPORT_ERROR
        if not isinstance(location, str) or not location:
            msg = "S3Cache requires a LOCATION such as 's3://my-bucket/cache'."
            raise ImproperlyConfigured(msg)
        options = dict(params.get("OPTIONS", {}))
        codec = {name: options.pop(name, default) for name, default in _OPTION_DEFAULTS.items()}
        max_concurrency = codec["max_concurrency"]
        if not isinstance(max_concurrency, int) or isinstance(max_concurrency, bool) or max_concurrency < 1:
            msg = f"S3Cache OPTIONS['max_concurrency'] must be a positive int, got {max_concurrency!r}"
            raise ImproperlyConfigured(msg)
        self._max_concurrency = max_concurrency
        self._serializers = self._create_serializers(codec["serializer"])
        self._compressors = self._create_compressors(codec["compressor"])
        options["client_options"] = {**_CLIENT_DEFAULTS, **options.get("client_options", {})}
        options["retry_config"] = {**_RETRY_DEFAULTS, **options.get("retry_config", {})}
        self._s3_store = _shared_store(location, options)
        prefix = self._s3_store.prefix
        self._max_path_bytes = _MAX_KEY_BYTES - (len(prefix.encode()) + 1 if prefix else 0)

    @property
    def _store(self) -> S3Store:
        _check_fork()
        return self._s3_store

    def _path(self, key: str, version: int | None) -> str:
        """The object path for ``key``, with ``%``, ``/``, control characters and all-dot keys percent-escaped."""
        key = self.make_key(key, version=version)
        path = key.replace(".", "%2E") if key.strip(".") == "" else key.translate(_ESCAPES)
        if not path:
            msg = "S3Cache cannot store an empty key"
            raise ValueError(msg)
        if len(path.encode("utf-8", "surrogatepass")) > self._max_path_bytes:
            msg = f"S3Cache key {key[:50]!r}... is longer than the {self._max_path_bytes} bytes S3 allows here"
            raise ValueError(msg)
        return path

    # The serializer runs on the calling thread only (_encode, _decode): pickling a lazy
    # value such as a QuerySet queries the database. The compressor runs with the request
    # (_put, _fetch), on a pool thread in a many-key call.
    def _encode(self, value: Any, expires: float | None) -> tuple[bytes, dict[str, str]]:
        """The serialized value and its attributes, not yet compressed."""
        attributes: dict[str, str] = {}
        if expires is not None:
            attributes[_EXPIRES] = f"{expires:.3f}"
        if type(value) is bytes:
            attributes[_RAW] = "1"
            return value, attributes
        return self._serializers[0].dumps(value), attributes

    def _decode(self, body: bytes, attributes: Mapping[str, Any]) -> Any:
        return body if _RAW in attributes else self._deserialize(body)

    def _compressed(self, payload: bytes, attributes: dict[str, str]) -> tuple[bytes, dict[str, str]]:
        if self._compressors:
            compressed = self._compressors[0].compress(payload)
            if compressed is not payload:
                return compressed, {**attributes, _COMPRESSED: "1"}
        return payload, attributes

    def _decompressed(self, body: bytes, attributes: Mapping[str, Any]) -> bytes:
        if _COMPRESSED not in attributes:
            return body
        decompressed = self._decompress(body)
        if decompressed is body:
            msg = f"No configured compressor could decompress the {len(body)}-byte value"
            raise CompressorError(msg)
        return decompressed

    def _put(self, path: str, payload: bytes, attributes: dict[str, str]) -> None:
        body, attributes = self._compressed(payload, attributes)
        obstore.put(self._store, path, body, attributes=attributes, use_multipart=False)

    async def _aput(self, path: str, payload: bytes, attributes: dict[str, str]) -> None:
        body, attributes = self._compressed(payload, attributes)
        await obstore.put_async(self._store, path, body, attributes=attributes, use_multipart=False)

    def _read(self, path: str, *, head: bool = False) -> GetResult | None:
        try:
            return obstore.get(self._store, path, options={"head": True} if head else None)
        except FileNotFoundError:
            return None

    async def _aread(self, path: str, *, head: bool = False) -> GetResult | None:
        try:
            return await obstore.get_async(self._store, path, options={"head": True} if head else None)
        except FileNotFoundError:
            return None

    def _map[T, R](self, fn: Callable[[T], R], items: list[T]) -> list[R]:
        # In a forked child the inherited pool has no threads, so the check runs here, not in a worker.
        _check_fork()
        if len(items) <= 1:
            return [fn(item) for item in items]
        pools = _pools.by_size
        if self._max_concurrency not in pools:
            pools[self._max_concurrency] = ThreadPoolExecutor(self._max_concurrency, thread_name_prefix="cachex-s3")
        return list(pools[self._max_concurrency].map(fn, items))

    async def _amap[T, R](self, fn: Callable[[T], Awaitable[R]], items: list[T]) -> list[R]:
        limit = asyncio.Semaphore(self._max_concurrency)

        async def limited(item: T) -> R:
            async with limit:
                return await fn(item)

        tasks = [asyncio.ensure_future(limited(item)) for item in items]
        try:
            return list(await asyncio.gather(*tasks))
        except BaseException:
            for task in tasks:
                task.cancel()
            raise

    @override
    def get(self, key: str, default: Any = None, version: int | None = None) -> Any:
        found = self._fetch(self._path(key, version))
        return default if found is None else self._decode(*found)

    def _fetch(self, path: str) -> tuple[bytes, Mapping[str, Any]] | None:
        """The decompressed body and the attributes of the live object at ``path``, or ``None``."""
        result = self._read(path)
        if result is None or not _live(result.attributes):
            return None
        return self._decompressed(bytes(result.bytes()), result.attributes), result.attributes

    @override
    async def aget(self, key: str, default: Any = None, version: int | None = None) -> Any:
        found = await self._afetch(self._path(key, version))
        return default if found is None else self._decode(*found)

    async def _afetch(self, path: str) -> tuple[bytes, Mapping[str, Any]] | None:
        result = await self._aread(path)
        if result is None or not _live(result.attributes):
            return None
        return self._decompressed(bytes(await result.bytes_async()), result.attributes), result.attributes

    @override
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
    ) -> None:
        if nx or xx or get:
            raise NotSupportedError("set with nx/xx/get", type(self).__name__, detail=_USE_RESP)
        path = self._path(key, version)
        expires = self.get_backend_timeout(timeout)
        if _expired(expires):
            obstore.delete(self._store, path)
        else:
            self._put(path, *self._encode(value, expires))

    @override
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
    ) -> None:
        if nx or xx or get:
            raise NotSupportedError("aset with nx/xx/get", type(self).__name__, detail=_USE_RESP)
        path = self._path(key, version)
        expires = self.get_backend_timeout(timeout)
        if _expired(expires):
            await obstore.delete_async(self._store, path)
        else:
            await self._aput(path, *self._encode(value, expires))

    @override
    def delete(self, key: str, version: int | None = None) -> bool:
        """Delete ``key``. Returns ``True`` also for a missing key: S3 does not say whether one was there."""
        obstore.delete(self._store, self._path(key, version))
        return True

    @override
    async def adelete(self, key: str, version: int | None = None) -> bool:
        await obstore.delete_async(self._store, self._path(key, version))
        return True

    @override
    def has_key(self, key: str, version: int | None = None) -> bool:
        current = self._read(self._path(key, version), head=True)
        return current is not None and _live(current.attributes)

    @override
    async def ahas_key(self, key: str, version: int | None = None) -> bool:
        current = await self._aread(self._path(key, version), head=True)
        return current is not None and _live(current.attributes)

    @override
    def get_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        paths = {self._path(key, version): key for key in keys}
        found = self._map(self._fetch, list(paths))
        return {key: self._decode(*hit) for key, hit in zip(paths.values(), found, strict=True) if hit is not None}

    @override
    async def aget_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        paths = {self._path(key, version): key for key in keys}
        found = await self._amap(self._afetch, list(paths))
        return {key: self._decode(*hit) for key, hit in zip(paths.values(), found, strict=True) if hit is not None}

    @override
    def set_many(
        self,
        data: Mapping[str, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[str]:
        items = {self._path(key, version): value for key, value in data.items()}
        expires = self.get_backend_timeout(timeout)
        if _expired(expires):
            if items:
                obstore.delete(self._store, list(items))
        else:
            puts = [(path, *self._encode(value, expires)) for path, value in items.items()]
            self._map(lambda put: self._put(*put), puts)
        return []

    @override
    async def aset_many(
        self,
        data: Mapping[str, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[str]:
        items = {self._path(key, version): value for key, value in data.items()}
        expires = self.get_backend_timeout(timeout)
        if _expired(expires):
            if items:
                await obstore.delete_async(self._store, list(items))
        else:
            puts = [(path, *self._encode(value, expires)) for path, value in items.items()]
            await self._amap(lambda put: self._aput(*put), puts)
        return []

    @override
    def delete_many(self, keys: Iterable[str], version: int | None = None) -> None:
        """Delete ``keys`` in one bulk request per 1000 keys."""
        paths = list(dict.fromkeys(self._path(key, version) for key in keys))
        if paths:
            obstore.delete(self._store, paths)

    @override
    async def adelete_many(self, keys: Iterable[str], version: int | None = None) -> None:
        paths = list(dict.fromkeys(self._path(key, version) for key in keys))
        if paths:
            await obstore.delete_async(self._store, paths)

    @override
    def clear(self) -> bool:  # type: ignore[override]
        """Delete every object under the ``LOCATION`` prefix, whichever cache wrote it."""
        store = self._store
        for batch in obstore.list(store, chunk_size=1000):
            obstore.delete(store, [meta["path"] for meta in batch])
        return True

    @override
    async def aclear(self) -> bool:  # type: ignore[override]
        store = self._store
        async for batch in obstore.list(store, chunk_size=1000):
            await obstore.delete_async(store, [meta["path"] for meta in batch])
        return True

    @override
    def incr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        raise NotSupportedError("incr", type(self).__name__, detail=_USE_RESP)

    @override
    def decr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        raise NotSupportedError("decr", type(self).__name__, detail=_USE_RESP)

    @override
    async def aincr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        raise NotSupportedError("aincr", type(self).__name__, detail=_USE_RESP)

    @override
    async def adecr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        raise NotSupportedError("adecr", type(self).__name__, detail=_USE_RESP)

    @override
    def add(self, key: str, value: Any, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        raise NotSupportedError("add", type(self).__name__, detail=_USE_RESP)

    @override
    async def aadd(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        raise NotSupportedError("aadd", type(self).__name__, detail=_USE_RESP)

    @override
    def get_or_set(
        self,
        key: str,
        default: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> Any:
        raise NotSupportedError("get_or_set", type(self).__name__, detail=_USE_RESP)

    @override
    async def aget_or_set(
        self,
        key: str,
        default: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> Any:
        raise NotSupportedError("aget_or_set", type(self).__name__, detail=_USE_RESP)

    @override
    def touch(self, key: str, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        raise NotSupportedError("touch", type(self).__name__, detail=_USE_RESP)

    @override
    async def atouch(self, key: str, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        raise NotSupportedError("atouch", type(self).__name__, detail=_USE_RESP)
