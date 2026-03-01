"""Stream-synchronized in-memory cache backend.

Reads are purely local (zero network). Writes go to a local dict and are
broadcast to a Redis Stream via XADD. A background daemon thread on each
pod consumes the stream via XREAD BLOCK and applies changes from all pods.

Suitable for read-heavy, write-light workloads (config, feature flags, etc.).

Configuration::

    CACHES = {
        "redis": {
            "BACKEND": "django_cachex.cache.RedisCache",
            "LOCATION": "redis://127.0.0.1:6379/0",
        },
        "default": {
            "BACKEND": "django_cachex.cache.SyncCache",
            "OPTIONS": {
                "TRANSPORT": "redis",
                "STREAM_KEY": "cache:sync",
                "MAXLEN": 10000,
                "BLOCK_TIMEOUT": 1000,
            },
        },
    }

``TRANSPORT`` is the alias of a Redis/Valkey cache used for stream I/O.
``STREAM_KEY`` is the Redis Stream key shared by all pods (default ``cache:sync``).
``MAXLEN`` caps stream length via approximate trimming (default 10000).
``BLOCK_TIMEOUT`` is the XREAD BLOCK timeout in milliseconds (default 1000).
"""

from __future__ import annotations

import fnmatch
import logging
import os
import pickle
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING, Any, ClassVar

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.core.exceptions import ImproperlyConfigured

if TYPE_CHECKING:
    from collections.abc import Iterable

    from django_cachex.types import KeyT

logger = logging.getLogger(__name__)

# Module-level globals for cross-thread sharing (same pattern as LocMemCache).
# Keyed by stream_key so multiple SyncCache instances on the same stream share
# one local store and different streams are isolated.
_caches: dict[str, OrderedDict] = {}
_expire_info: dict[str, dict[str, float | None]] = {}
_locks: dict[str, Lock] = {}


def _field(fields: dict, name: str) -> str:
    """Extract a string field from raw Redis stream entry (may be bytes or str)."""
    val = fields.get(name.encode(), fields.get(name, b""))
    return val.decode() if isinstance(val, bytes) else str(val) if val else ""


def _field_bytes(fields: dict, name: str) -> bytes:
    """Extract a bytes field from raw Redis stream entry."""
    val = fields.get(name.encode(), fields.get(name, b""))
    return val if isinstance(val, bytes) else val.encode() if val else b""


class SyncCache(BaseCache):
    """Stream-synchronized in-memory cache.

    All reads are local dict lookups. Writes update the local dict and publish
    to a Redis Stream. A daemon thread consumes the stream and applies remote
    changes. Eventual consistency across pods.
    """

    _cachex_support: str = "wrapped"

    def __init__(self, server: str, params: dict[str, Any]) -> None:
        super().__init__(params)
        options = params.get("OPTIONS", {})

        # Transport cache alias (required)
        self._transport_alias: str = options.get("TRANSPORT", "")
        if not self._transport_alias:
            msg = "SyncCache requires OPTIONS['TRANSPORT'] with a cache alias for Redis/Valkey stream transport."
            raise ImproperlyConfigured(msg)

        # Stream configuration
        self._stream_key: str = options.get("STREAM_KEY", "cache:sync")
        self._maxlen: int = options.get("MAXLEN", 10000)
        self._block_timeout: int = options.get("BLOCK_TIMEOUT", 1000)

        # Local storage (module-level globals, keyed by storage_key).
        # storage_key defaults to stream_key so all threads in a process share
        # one local dict. Tests can override via _STORAGE_KEY to simulate
        # separate pods in the same process.
        storage_key = options.get("_STORAGE_KEY", self._stream_key)
        self._storage_key: str = storage_key
        self._cache: OrderedDict = _caches.setdefault(storage_key, OrderedDict())
        self._expire_info: dict[str, float | None] = _expire_info.setdefault(
            storage_key,
            {},
        )
        self._lock: Lock = _locks.setdefault(storage_key, Lock())

        # Pod identity for self-message dedup
        self._pod_id: str = f"{os.getpid()}-{id(self)}-{uuid.uuid4().hex[:8]}"

        # Consumer thread state
        self._consumer_thread: Thread | None = None
        self._stop_event = Event()
        self._last_id: str = "$"
        self._initialized: bool = False
        self._init_lock = Lock()

        # Non-blocking publish: single-worker executor serializes XADD calls
        # without blocking the calling thread on network I/O.
        self._publish_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sync-pub")

    # -- Transport (lazy) --

    @cached_property
    def _transport(self) -> BaseCache:
        from django.core.cache import caches

        return caches[self._transport_alias]

    def _get_raw_client(self) -> Any:
        """Get raw Redis/Valkey client for stream operations."""
        return self._transport.get_client(write=True)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]

    # -- Local storage helpers --

    def _has_expired(self, key: str) -> bool:
        exp = self._expire_info.get(key, -1)
        return exp is not None and exp <= time.time()

    def _cull(self) -> None:
        if self._cull_frequency == 0:
            self._cache.clear()
            self._expire_info.clear()
        else:
            count = len(self._cache) // self._cull_frequency
            for _ in range(count):
                try:
                    key, _ = self._cache.popitem(last=True)
                except KeyError:
                    break
                self._expire_info.pop(key, None)

    def _local_set(self, key: str, pickled: bytes, exp_time: float | None) -> None:
        """Set in local dict. Caller holds self._lock."""
        if len(self._cache) >= self._max_entries:
            self._cull()
        self._cache[key] = pickled
        self._cache.move_to_end(key, last=False)
        self._expire_info[key] = exp_time

    def _local_delete(self, key: str) -> bool:
        """Delete from local dict. Caller holds self._lock."""
        existed = key in self._cache
        self._cache.pop(key, None)
        self._expire_info.pop(key, None)
        return existed

    def _local_clear(self) -> None:
        """Clear local dict. Caller holds self._lock."""
        self._cache.clear()
        self._expire_info.clear()

    # -- Stream publishing --

    def _publish(
        self,
        op: str,
        key: str = "",
        val: bytes = b"",
        exp: float | None = None,
        keys: str = "",
    ) -> None:
        """Publish a cache mutation to the Redis Stream (non-blocking, best-effort).

        Submits the XADD call to a single-worker ThreadPoolExecutor so the
        calling thread returns immediately without waiting for the network
        round-trip. The single worker serializes publishes, preserving stream
        order.
        """
        fields: dict[str, str | bytes] = {
            "op": op,
            "pod": self._pod_id,
            "key": key,
            "val": val,
            "exp": str(exp) if exp is not None else "",
        }
        if keys:
            fields["keys"] = keys
        self._publish_executor.submit(self._do_xadd, fields)

    def _do_xadd(self, fields: dict[str, str | bytes]) -> None:
        """Execute a single XADD. Runs in the publish executor thread."""
        try:
            client = self._get_raw_client()
            client.xadd(
                self._stream_key,
                fields,
                maxlen=self._maxlen,
                approximate=True,
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "SyncCache: Failed to publish %s to stream",
                fields.get("op", "?"),
                exc_info=True,
            )

    # -- Consumer thread --

    def _ensure_consumer(self) -> None:
        """Start the consumer thread on first access (double-checked locking)."""
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            self._start_consumer()
            self._initialized = True

    def _start_consumer(self) -> None:
        self._stop_event.clear()
        self._consumer_thread = Thread(
            target=self._consumer_loop,
            name=f"sync-cache-{self._stream_key}",
            daemon=True,
        )
        self._consumer_thread.start()

    def _consumer_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                client = self._get_raw_client()
                result = client.xread(
                    streams={self._stream_key: self._last_id},
                    count=100,
                    block=self._block_timeout,
                )
                if result is None:
                    continue
                # result is list of [stream_key, [(entry_id, fields), ...]]
                for _stream_key, entries in result:
                    with self._lock:
                        for entry_id, fields in entries:
                            self._apply_message(fields)
                            self._last_id = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
            except Exception:  # noqa: BLE001
                if not self._stop_event.is_set():
                    logger.warning(
                        "SyncCache: Consumer error, retrying in 1s",
                        exc_info=True,
                    )
                    self._stop_event.wait(1.0)

    def _apply_message(self, fields: dict) -> None:
        """Apply a single stream message to local cache. Caller holds self._lock."""
        op = _field(fields, "op")
        pod = _field(fields, "pod")

        # Skip self-messages (already applied locally by the writer)
        if pod == self._pod_id:
            return

        handler = self._MESSAGE_HANDLERS.get(op)
        if handler:
            handler(self, fields)

    def _handle_set(self, fields: dict) -> None:
        key = _field(fields, "key")
        val_bytes = _field_bytes(fields, "val")
        exp_str = _field(fields, "exp")
        exp_time = float(exp_str) if exp_str else None
        self._local_set(key, val_bytes, exp_time)

    def _handle_delete(self, fields: dict) -> None:
        self._local_delete(_field(fields, "key"))

    def _handle_delete_many(self, fields: dict) -> None:
        for key in _field(fields, "keys").split("\x00"):
            if key:
                self._local_delete(key)

    def _handle_clear(self, fields: dict) -> None:
        self._local_clear()

    def _handle_touch(self, fields: dict) -> None:
        key = _field(fields, "key")
        exp_str = _field(fields, "exp")
        exp_time = float(exp_str) if exp_str else None
        if key in self._cache:
            self._expire_info[key] = exp_time

    _MESSAGE_HANDLERS: ClassVar[dict[str, Any]] = {
        "set": _handle_set,
        "delete": _handle_delete,
        "delete_many": _handle_delete_many,
        "clear": _handle_clear,
        "touch": _handle_touch,
    }

    def _flush_publishes(self) -> None:
        """Block until all queued publishes have been sent. For testing only."""
        # Submit a no-op and wait for it — when it completes, all prior
        # submits have finished since the executor is single-threaded.
        self._publish_executor.submit(lambda: None).result(timeout=5.0)

    def _drain(self, timeout: float = 1.0) -> None:
        """Process all pending stream messages synchronously. For testing only.

        If the consumer hasn't consumed anything yet (``_last_id`` is still
        ``$``), this reads from the beginning of the stream so that messages
        published before the drain call are visible.
        """
        read_from = self._last_id if self._last_id != "$" else "0-0"
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                client = self._get_raw_client()
                result = client.xread(
                    streams={self._stream_key: read_from},
                    count=100,
                    block=50,
                )
                if result is None:
                    return
                for _stream_key, entries in result:
                    with self._lock:
                        for entry_id, fields in entries:
                            self._apply_message(fields)
                            eid = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
                            read_from = eid
                            self._last_id = eid
            except Exception:  # noqa: BLE001
                return

    def shutdown(self) -> None:
        """Stop the consumer thread and publish executor. For testing/cleanup only."""
        self._publish_executor.shutdown(wait=True, cancel_futures=False)
        if self._consumer_thread is not None:
            self._stop_event.set()
            self._consumer_thread.join(timeout=2.0)
            self._consumer_thread = None
            self._initialized = False
            self._stop_event.clear()

    # -- Standard Django cache interface --

    def get(self, key: KeyT, default: Any = None, version: int | None = None) -> Any:
        self._ensure_consumer()
        key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if self._has_expired(key):
                self._local_delete(key)
                return default
            pickled = self._cache.get(key)
            if pickled is None:
                return default
            self._cache.move_to_end(key, last=False)
        return pickle.loads(pickled)  # noqa: S301

    def set(  # type: ignore[override]
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        **kwargs: Any,
    ) -> bool:
        self._ensure_consumer()
        key = self.make_and_validate_key(key, version=version)
        exp_time = self.get_backend_timeout(timeout)
        pickled = pickle.dumps(value, pickle.HIGHEST_PROTOCOL)
        with self._lock:
            self._local_set(key, pickled, exp_time)
        self._publish("set", key=key, val=pickled, exp=exp_time)
        return True

    def add(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        self._ensure_consumer()
        key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if not self._has_expired(key) and key in self._cache:
                return False
            exp_time = self.get_backend_timeout(timeout)
            pickled = pickle.dumps(value, pickle.HIGHEST_PROTOCOL)
            self._local_set(key, pickled, exp_time)
        self._publish("set", key=key, val=pickled, exp=exp_time)
        return True

    def delete(self, key: KeyT, version: int | None = None) -> bool:
        self._ensure_consumer()
        key = self.make_and_validate_key(key, version=version)
        with self._lock:
            existed = self._local_delete(key)
        self._publish("delete", key=key)
        return existed

    def get_many(self, keys: Iterable[KeyT], version: int | None = None) -> dict[KeyT, Any]:
        self._ensure_consumer()
        result: dict[KeyT, Any] = {}
        for k in keys:
            val = self.get(k, default=self, version=version)
            if val is not self:
                result[k] = val
        return result

    def set_many(
        self,
        data: dict[KeyT, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[Any]:
        for key, value in data.items():
            self.set(key, value, timeout, version=version)
        return []

    def delete_many(self, keys: Iterable[KeyT], version: int | None = None) -> None:
        self._ensure_consumer()
        keys_list = list(keys)
        made_keys: list[str] = []
        with self._lock:
            for k in keys_list:
                mk = self.make_and_validate_key(k, version=version)
                made_keys.append(mk)
                self._local_delete(mk)
        if made_keys:
            self._publish("delete_many", keys="\x00".join(made_keys))

    def has_key(self, key: KeyT, version: int | None = None) -> bool:
        self._ensure_consumer()
        key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if self._has_expired(key):
                self._local_delete(key)
                return False
            return key in self._cache

    def incr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        self._ensure_consumer()
        key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if self._has_expired(key):
                self._local_delete(key)
                raise ValueError(f"Key '{key}' not found")
            pickled = self._cache.get(key)
            if pickled is None:
                raise ValueError(f"Key '{key}' not found")
            value = pickle.loads(pickled)  # noqa: S301
            new_value = value + delta
            new_pickled = pickle.dumps(new_value, pickle.HIGHEST_PROTOCOL)
            self._cache[key] = new_pickled
            self._cache.move_to_end(key, last=False)
            exp_time = self._expire_info.get(key)
        self._publish("set", key=key, val=new_pickled, exp=exp_time)
        return new_value

    def decr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        return self.incr(key, -delta, version=version)

    def touch(
        self,
        key: KeyT,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        self._ensure_consumer()
        key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if self._has_expired(key) or key not in self._cache:
                return False
            exp_time = self.get_backend_timeout(timeout)
            self._expire_info[key] = exp_time
        self._publish("touch", key=key, exp=exp_time)
        return True

    def clear(self) -> None:
        self._ensure_consumer()
        with self._lock:
            self._local_clear()
        self._publish("clear")

    def close(self, **kwargs: Any) -> None:
        """No-op: consumer thread persists across requests (daemon=True for cleanup)."""

    # -- Admin methods (local implementations for fast reads) --

    def make_key(self, key: str, version: int | None = None) -> str:
        """Construct the cache key with prefix and version."""
        v = version if version is not None else self.version
        prefix = self.key_prefix
        if not prefix:
            return f":{v}:{key}"
        return f"{prefix}:{v}:{key}"

    def reverse_key(self, key: str) -> str:
        """Strip prefix:version: to get the user-visible key."""
        prefix = self.key_prefix
        # Key format: ":version:user_key" or "prefix:version:user_key"
        expected_prefix = f":{self.version}:" if not prefix else f"{prefix}:{self.version}:"
        if key.startswith(expected_prefix):
            return key[len(expected_prefix) :]
        return key

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        self._ensure_consumer()
        user_keys: list[str] = []
        with self._lock:
            for internal_key in list(self._cache.keys()):
                if self._has_expired(internal_key):
                    continue
                user_key = self.reverse_key(internal_key)
                user_keys.append(user_key)
        if pattern and pattern != "*":
            user_keys = [k for k in user_keys if fnmatch.fnmatch(k, pattern)]
        user_keys.sort()
        return user_keys

    def ttl(self, key: KeyT, version: int | None = None) -> int | None:
        self._ensure_consumer()
        key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if key not in self._cache or self._has_expired(key):
                return 0
            exp = self._expire_info.get(key)
            if exp is None:
                return None
            remaining = int(exp - time.time())
            return max(0, remaining)

    def pttl(self, key: KeyT, version: int | None = None) -> int | None:
        self._ensure_consumer()
        key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if key not in self._cache or self._has_expired(key):
                return 0
            exp = self._expire_info.get(key)
            if exp is None:
                return None
            remaining = int((exp - time.time()) * 1000)
            return max(0, remaining)

    def persist(self, key: KeyT, version: int | None = None) -> bool:
        return self.touch(key, timeout=None, version=version)

    def expire(self, key: KeyT, timeout: int, version: int | None = None) -> bool:
        return self.touch(key, timeout=timeout, version=version)

    def type(self, key: KeyT, version: int | None = None) -> str:
        self._ensure_consumer()
        key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if key not in self._cache or self._has_expired(key):
                return "none"
        return "string"

    def info(self, section: str | None = None) -> dict[str, Any]:
        self._ensure_consumer()
        with self._lock:
            key_count = len(self._cache)
            now = time.time()
            expires_count = sum(1 for exp in self._expire_info.values() if exp is not None and exp > now)
        return {
            "server": {
                "redis_version": f"SyncCache (stream: {self._stream_key})",
                "transport": self._transport_alias,
            },
            "keyspace": {
                "db0": {
                    "keys": key_count,
                    "expires": expires_count,
                },
            },
        }

    def scan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]:
        """Scan local cache keys with cursor-based pagination."""
        self._ensure_consumer()
        all_keys = self.keys(pattern, version=version)
        if count is None:
            count = 10
        start = cursor
        end = min(start + count, len(all_keys))
        page = all_keys[start:end]
        next_cursor = end if end < len(all_keys) else 0
        return next_cursor, page

    def iter_keys(
        self,
        pattern: str,
        itersize: int | None = None,
    ) -> list[str]:
        return self.keys(pattern)

    def make_pattern(self, pattern: str, version: int | None = None) -> str:
        """Make a key pattern with the cache prefix."""
        prefix = self.key_prefix
        v = version if version is not None else self.version
        if not prefix:
            return f":{v}:{pattern}"
        return f"{prefix}:{v}:{pattern}"

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        matching = self.keys(pattern, version=version)
        for k in matching:
            self.delete(k, version=version)
        return len(matching)


__all__ = [
    "SyncCache",
]
