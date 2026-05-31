"""Stream-synchronized in-memory cache backend.

Extends Django's ``LocMemCache`` with cross-pod synchronization via Redis
Streams. Reads are purely local (zero network). Writes update the local
dict and are broadcast to a Redis Stream via XADD. A background daemon
thread on each pod consumes the stream via XREAD BLOCK and applies changes
from all pods.

Suitable for read-heavy, write-light workloads (config, feature flags, etc.).

Configuration::

    CACHES = {
        "redis": {
            "BACKEND": "django_cachex.cache.RedisCache",  # or RedisRsCache, etc.
            "LOCATION": "redis://127.0.0.1:6379/0",
        },
        "default": {
            "BACKEND": "django_cachex.cache.StreamCache",
            "OPTIONS": {
                "transport": "redis",
                "stream_key": "cache:sync",
                "maxlen": 10000,
                "block_timeout": 1000,
            },
        },
    }

``transport`` is the alias of any cachex ``RespCache`` subclass (pure-Python
or Rust-driver-backed) used for stream I/O.
``stream_key`` is the Redis Stream key shared by all pods (default ``cache:sync``).
``maxlen`` caps stream length via approximate trimming (default 10000).
``block_timeout`` is the XREAD BLOCK timeout in milliseconds (default 1000).
``replay`` is the number of recent stream entries to replay on startup to
warm the local cache (default 0 = no replay). Values up to ``maxlen`` work;
``1000`` replays the last 1000 mutations so a restarting pod doesn't start
with an empty cache.

Wire format: stream fields go through the transport cache's high-level
``xadd``/``xread``/``xrevrange`` methods, which apply its configured serializer
and compressor end-to-end. If the transport is configured with
``OPTIONS={"serializer": "msgpack", "compressor": "zstd"}``, stream entries
are msgpack-then-zstd. All pods sharing one ``stream_key`` must use the same
transport ``BACKEND`` + ``OPTIONS`` so their serializers agree.
"""

import contextlib
import fnmatch
import logging
import os
import pickle
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property
from threading import BoundedSemaphore, Event, Lock, Thread
from typing import TYPE_CHECKING, Any, ClassVar

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.core.cache.backends.locmem import LocMemCache
from django.core.exceptions import ImproperlyConfigured

from django_cachex.exceptions import NotSupportedError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from datetime import timedelta

    from django_cachex.cache.base import CachexSupportLevel

logger = logging.getLogger(__name__)


class StreamCache(LocMemCache):
    """Stream-synchronized in-memory cache.

    Extends Django's ``LocMemCache`` with cross-pod synchronization. Reads are
    local dict lookups (inherited from ``LocMemCache``). Writes update the
    local dict and publish to a Redis Stream via the transport cache's
    ``xadd``. A daemon thread consumes the stream via ``xread`` and applies
    remote changes. Supported operations are eventually consistent
    (last-writer-wins).

    ``add``, ``incr``, and ``decr`` raise ``NotSupportedError``: their
    semantics (atomic check-and-set, atomic increment) can't be provided
    with eventual consistency. Use the transport cache directly for these.

    The consumer thread is restarted if it dies; use ``info()["sync"]`` to
    monitor consumer health, last read age, and stream position.
    """

    _cachex_support: CachexSupportLevel = "cachex"

    # Type declarations for attributes and methods inherited from LocMemCache.
    if TYPE_CHECKING:
        from collections import OrderedDict
        from threading import Lock

        _cache: OrderedDict
        _expire_info: dict[str, float | None]
        _lock: Lock

        def _set(self, key: str, value: bytes, timeout: float | None = ...) -> None: ...
        def _delete(self, key: str) -> bool: ...
        def _has_expired(self, key: str) -> bool: ...
        def _cull(self) -> None: ...

    def __init__(self, server: str, params: dict[str, Any]) -> None:
        options = params.get("OPTIONS", {})

        self._transport_alias: str = options.get("transport", "")
        if not self._transport_alias:
            msg = "StreamCache requires OPTIONS['transport'] with a cache alias for stream transport."
            raise ImproperlyConfigured(msg)

        self._stream_key: str = options.get("stream_key", "cache:sync")
        self._maxlen: int = options.get("maxlen", 10000)
        self._block_timeout: int = options.get("block_timeout", 1000)
        self._replay_count: int = options.get("replay", 0)

        # LocMemCache uses its ``server`` argument to key its module-level
        # globals. Use Django's ``LOCATION`` to do the same; multiple
        # StreamCache aliases sharing one ``stream_key`` but distinct
        # ``LOCATION``s simulate separate pods within the same process,
        # which is how the test suite exercises the consumer side.
        storage_key = server or self._stream_key
        self._storage_key: str = storage_key
        super().__init__(storage_key, params)

        # Pod identity for self-message dedup
        self._pod_id: str = f"{os.getpid()}-{id(self)}-{uuid.uuid4().hex[:8]}"

        # Consumer thread state
        self._consumer_thread: Thread | None = None
        self._stop_event = Event()
        self._last_id: str = "$"
        self._last_read_time: float = 0.0
        self._initialized: bool = False
        self._init_lock = Lock()

        # Non-blocking publish: single-worker executor serializes XADD calls
        # without blocking the calling thread on network I/O.
        self._publish_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sync-pub")
        self._publish_executor_shutdown = False

        # Bound the publish backlog. ``ThreadPoolExecutor`` uses an unbounded
        # ``SimpleQueue``, so a writer outpacing XADD throughput would grow
        # memory until OOM. Gate ``submit`` on a ``BoundedSemaphore``; when
        # the budget is exhausted the publish is dropped with a warning
        # rather than blocking the caller. Default cap is 1000 outstanding
        # broadcasts; tune via OPTIONS["max_pending_publishes"].
        self._max_pending_publishes: int = options.get("max_pending_publishes", 1000)
        self._publish_budget = BoundedSemaphore(self._max_pending_publishes)
        # Shutdown grace for the publish executor. ``ThreadPoolExecutor.shutdown``
        # offers no native timeout in 3.14, so we cancel pending futures and
        # join the worker thread ourselves with this bound.
        self._publish_shutdown_timeout: float = options.get("publish_shutdown_timeout", 5.0)

        # Admin display: show stream key and transport alias as location
        self._cachex_location = f"stream:{self._stream_key} [transport: {self._transport_alias}]"

    def __del__(self) -> None:
        with contextlib.suppress(Exception):
            self.shutdown()

    # -- Transport (lazy) --

    @cached_property
    def _transport(self) -> BaseCache:
        from django.core.cache import caches

        return caches[self._transport_alias]

    # -- Consumer-side local storage helper --

    def _local_set(self, key: str, pickled: bytes, exp_time: float | None) -> None:
        """Set in local dict with absolute expiry. For consumer messages only.

        Unlike ``LocMemCache._set`` (which takes a relative timeout),
        this stores an absolute expiry timestamp received from the stream.
        Caller holds ``self._lock``.
        """
        if len(self._cache) >= self._max_entries:
            self._cull()
        self._cache[key] = pickled
        self._cache.move_to_end(key, last=False)
        self._expire_info[key] = exp_time

    # -- Stream publishing --

    def _publish(
        self,
        op: str,
        key: str = "",
        val: Any = None,
        exp: float | None = None,
        keys: str = "",
    ) -> None:
        """Publish a cache mutation to the stream (non-blocking, best-effort).

        ``val`` is the original Python value. The transport cache's serializer
        and compressor handle wire encoding. The single-worker executor
        preserves stream order while keeping the calling thread off the
        network round-trip.

        The pending-publish budget (``_publish_budget``) caps how many
        broadcasts may be queued at once. When the budget is exhausted the
        new publish is dropped with a warning, trading durability for
        bounded memory under sustained write bursts that outpace XADD.
        """
        if not self._publish_budget.acquire(blocking=False):
            logger.warning(
                "StreamCache: publish backlog full (cap=%d); dropping %s broadcast",
                self._max_pending_publishes,
                op,
            )
            return
        fields: dict[str, Any] = {
            "op": op,
            "pod": self._pod_id,
            "key": key,
            "val": val,
            "exp": str(exp) if exp is not None else "",
        }
        if keys:
            fields["keys"] = keys
        try:
            self._publish_executor.submit(self._do_xadd, fields)
        except RuntimeError:
            # Executor was shut down (likely from a prior ``__del__`` /
            # ``shutdown`` and a thread is racing the teardown). Drop the
            # publish; losing the broadcast is preferable to crashing the
            # caller. If the cache is still in active use ``_ensure_consumer``
            # will rebuild the executor on the next get/set.
            self._publish_budget.release()
            logger.warning(
                "StreamCache: publish executor closed; dropping %s broadcast",
                op,
            )

    def _do_xadd(self, fields: dict[str, Any]) -> None:
        """Execute a single XADD via the transport's high-level API."""
        try:
            self._transport.xadd(  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                self._stream_key,
                fields,
                maxlen=self._maxlen,
                approximate=True,
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "StreamCache: Failed to publish %s to stream",
                fields.get("op", "?"),
                exc_info=True,
            )
        finally:
            # Return budget once the broadcast has actually drained.
            with contextlib.suppress(ValueError):
                self._publish_budget.release()

    # -- Consumer thread --

    def _consumer_alive(self) -> bool:
        return self._consumer_thread is not None and self._consumer_thread.is_alive()

    def _ensure_consumer(self) -> None:
        """Start (or restart) the consumer thread.

        Uses double-checked locking. On every call, verifies the thread is
        actually alive. If it died (e.g. due to ``SystemExit`` or an
        unhandled ``BaseException``), it is automatically restarted so the
        pod doesn't silently fall out of sync.
        """
        if self._initialized and self._consumer_alive():
            return
        with self._init_lock:
            if self._initialized and self._consumer_alive():
                return
            if self._initialized and not self._consumer_alive():
                logger.warning(
                    "StreamCache: Consumer thread died, restarting (stream=%s)",
                    self._stream_key,
                )
            self._start_consumer()
            self._initialized = True

    def _start_consumer(self) -> None:
        # Recreate executor if it was shut down (e.g. after shutdown() + reuse)
        if self._publish_executor_shutdown:
            self._publish_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sync-pub")
            self._publish_executor_shutdown = False
            # Reset the publish budget too: the old semaphore may have been
            # drained by in-flight futures that ``cancel_futures=True``
            # rejected without running ``_do_xadd``'s release.
            self._publish_budget = BoundedSemaphore(self._max_pending_publishes)
        if self._replay_count > 0:
            self._replay_stream(self._replay_count)
        self._stop_event.clear()
        self._consumer_thread = Thread(
            target=self._consumer_loop,
            name=f"sync-cache-{self._stream_key}",
            daemon=True,
        )
        self._consumer_thread.start()

    def _replay_stream(self, count: int) -> None:
        """Replay the last ``count`` stream entries to warm the local cache.

        Called once at startup before the consumer thread begins. Reads
        recent entries via ``XREVRANGE``, applies them oldest-first, and
        sets ``_last_id`` so the consumer continues from where replay
        left off (no duplicates).
        """
        try:
            entries = self._transport.xrevrange(self._stream_key, count=count)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            if not entries:
                return
            with self._lock:
                for entry_id, fields in reversed(entries):
                    self._apply_message(fields)
                    self._last_id = entry_id
            logger.info(
                "StreamCache: Replayed %d entries from stream %s",
                len(entries),
                self._stream_key,
            )
        except Exception:  # noqa: BLE001
            logger.warning("StreamCache: stream replay failed", exc_info=True)

    def _consumer_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                result = self._transport.xread(  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                    streams={self._stream_key: self._last_id},
                    count=100,
                    block=self._block_timeout,
                )
                if not result:
                    continue
                self._last_read_time = time.time()
                for entries in result.values():
                    with self._lock:
                        for entry_id, fields in entries:
                            # Advance cursor BEFORE processing so a bad
                            # message is skipped, not retried forever.
                            self._last_id = entry_id
                            try:
                                self._apply_message(fields)
                            except Exception:  # noqa: BLE001
                                logger.warning(
                                    "StreamCache: Failed to apply message %s, skipping",
                                    self._last_id,
                                    exc_info=True,
                                )
            except Exception:  # noqa: BLE001
                if not self._stop_event.is_set():
                    logger.warning(
                        "StreamCache: Consumer error, retrying in 1s",
                        exc_info=True,
                    )
                    self._stop_event.wait(1.0)

    def _apply_message(self, fields: dict[str, Any]) -> None:
        """Apply a single stream message to local cache. Caller holds ``self._lock``."""
        op = fields.get("op", "")
        pod = fields.get("pod", "")

        # Skip self-messages (already applied locally by the writer)
        if pod == self._pod_id:
            return

        handler = self._MESSAGE_HANDLERS.get(op)
        if handler:
            handler(self, fields)

    def _handle_set(self, fields: dict[str, Any]) -> None:
        key = fields["key"]
        value = fields.get("val")
        exp_str = fields.get("exp", "")
        exp_time = float(exp_str) if exp_str else None
        pickled = pickle.dumps(value, self.pickle_protocol)
        self._local_set(key, pickled, exp_time)

    def _handle_delete(self, fields: dict[str, Any]) -> None:
        self._delete(fields["key"])

    def _handle_delete_many(self, fields: dict[str, Any]) -> None:
        for key in fields.get("keys", "").split("\x00"):
            if key:
                self._delete(key)

    def _handle_clear(self, fields: dict[str, Any]) -> None:
        self._cache.clear()
        self._expire_info.clear()

    def _handle_touch(self, fields: dict[str, Any]) -> None:
        key = fields["key"]
        exp_str = fields.get("exp", "")
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
        """Block until all queued publishes have been sent.

        Submits a no-op and waits; when it completes, all prior submits
        have finished since the executor is single-threaded.
        """
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
                result = self._transport.xread(  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                    streams={self._stream_key: read_from},
                    count=100,
                    block=50,
                )
                if not result:
                    return
                for entries in result.values():
                    with self._lock:
                        for entry_id, fields in entries:
                            self._apply_message(fields)
                            read_from = entry_id
                            self._last_id = entry_id
            except Exception:  # noqa: BLE001
                return

    def shutdown(self) -> None:
        """Stop the consumer thread and publish executor with bounded waits.

        The consumer is parked in ``XREAD BLOCK self._block_timeout`` (ms),
        so the join grace has to outlast one block window. We give it
        ``block_timeout + 1s`` (capped at 10s); if the thread is still
        alive after that it gets dropped, leaking the daemon thread is
        cheaper than blocking process shutdown.

        The publish executor is bounded by ``_publish_shutdown_timeout``.
        Pending futures are cancelled up front (``cancel_futures=True``)
        so a hung transport can't stall ``__del__`` indefinitely. The
        worker thread is then joined with a timeout; any in-flight XADD
        that doesn't drain in time is abandoned with a warning.
        """
        if self._consumer_thread is not None:
            self._stop_event.set()
            join_timeout = min(10.0, (self._block_timeout / 1000.0) + 1.0)
            self._consumer_thread.join(timeout=join_timeout)
            if self._consumer_thread.is_alive():
                logger.warning(
                    "StreamCache: consumer thread still alive after %.1fs; abandoning it",
                    join_timeout,
                )
            self._consumer_thread = None
            self._initialized = False
            self._stop_event.clear()
        # Drop pending broadcasts and start non-blocking shutdown.
        self._publish_executor.shutdown(wait=False, cancel_futures=True)
        self._publish_executor_shutdown = True
        # Bound the wait by joining the executor's worker threads ourselves
        # so a hung transport can't block forever. ``_threads`` is a private
        # attribute, but it's the only way to get a bounded join in 3.14.
        worker_threads = list(getattr(self._publish_executor, "_threads", ()) or ())
        deadline = time.time() + self._publish_shutdown_timeout
        for thread in worker_threads:
            remaining = max(0.0, deadline - time.time())
            thread.join(timeout=remaining)
        still_alive = [t for t in worker_threads if t.is_alive()]
        if still_alive:
            logger.warning(
                "StreamCache: publish worker(s) still alive after %.1fs; abandoning %d thread(s)",
                self._publish_shutdown_timeout,
                len(still_alive),
            )

    # -- Standard Django cache interface (LocMemCache + stream sync) --

    def get(self, key: str, default: Any = None, version: int | None = None) -> Any:
        self._ensure_consumer()
        return super().get(key, default=default, version=version)

    def set(  # type: ignore[override]
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        **kwargs: Any,
    ) -> bool:
        self._ensure_consumer()
        made_key = self.make_and_validate_key(key, version=version)
        pickled = pickle.dumps(value, self.pickle_protocol)
        with self._lock:
            self._set(made_key, pickled, timeout)
        exp_time = self._expire_info.get(made_key)
        self._publish("set", key=made_key, val=value, exp=exp_time)
        return True

    def add(self, key: str, value: Any, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        raise NotSupportedError("add", "StreamCache")

    def delete(self, key: str, version: int | None = None) -> bool:
        self._ensure_consumer()
        made_key = self.make_and_validate_key(key, version=version)
        with self._lock:
            existed = self._delete(made_key)
        self._publish("delete", key=made_key)
        return existed

    def get_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        self._ensure_consumer()
        result: dict[str, Any] = {}
        for k in keys:
            val = self.get(k, default=self, version=version)
            if val is not self:
                result[k] = val
        return result

    def set_many(
        self,
        data: dict[str, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[Any]:
        for key, value in data.items():
            self.set(key, value, timeout, version=version)
        return []

    def delete_many(self, keys: Iterable[str], version: int | None = None) -> int:  # type: ignore[override]
        self._ensure_consumer()
        keys_list = list(keys)
        made_keys: list[str] = []
        deleted = 0
        with self._lock:
            for k in keys_list:
                mk = self.make_and_validate_key(k, version=version)
                made_keys.append(mk)
                if self._delete(mk):
                    deleted += 1
        if made_keys:
            self._publish("delete_many", keys="\x00".join(made_keys))
        return deleted

    def has_key(self, key: str, version: int | None = None) -> bool:
        self._ensure_consumer()
        return super().has_key(key, version=version)

    def incr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        raise NotSupportedError("incr", "StreamCache")

    def decr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        raise NotSupportedError("decr", "StreamCache")

    def get_or_set(
        self,
        key: str,
        default: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> Any:
        """Get a value or set it if missing. Uses ``set`` (not ``add``)."""
        val = self.get(key, version=version)
        if val is None:
            if callable(default):
                default = default()
            if default is not None:
                self.set(key, default, timeout=timeout, version=version)
            return default
        return val

    def touch(
        self,
        key: str,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        self._ensure_consumer()
        made_key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if self._has_expired(made_key):
                return False
            exp_time = self.get_backend_timeout(timeout)
            self._expire_info[made_key] = exp_time
        self._publish("touch", key=made_key, exp=exp_time)
        return True

    def clear(self) -> bool:  # type: ignore[override]
        self._ensure_consumer()
        super().clear()
        self._publish("clear")
        return True

    def close(self, **kwargs: Any) -> None:
        """No-op. Use ``shutdown()`` to stop the consumer thread + publish executor."""

    # -- Admin methods (local implementations for fast reads) --

    def reverse_key(self, key: str) -> str:
        """Strip prefix:version: to get the user-visible key."""
        prefix = self.key_prefix
        # Key format: "prefix:version:user_key" or ":version:user_key"
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

    def ttl(self, key: str, version: int | None = None) -> int | None:
        self._ensure_consumer()
        made_key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if made_key not in self._cache or self._has_expired(made_key):
                return -2
            exp = self._expire_info.get(made_key)
            if exp is None:
                return -1
            remaining = int(exp - time.time())
            return max(0, remaining)

    def pttl(self, key: str, version: int | None = None) -> int | None:
        self._ensure_consumer()
        made_key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if made_key not in self._cache or self._has_expired(made_key):
                return -2
            exp = self._expire_info.get(made_key)
            if exp is None:
                return -1
            remaining = int((exp - time.time()) * 1000)
            return max(0, remaining)

    def persist(self, key: str, version: int | None = None) -> bool:
        return self.touch(key, timeout=None, version=version)

    def expire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        # ``timedelta`` collapses to seconds. ``touch()`` takes float | None.
        seconds: float = timeout if isinstance(timeout, int) else timeout.total_seconds()
        return self.touch(key, timeout=seconds, version=version)

    def type(self, key: str, version: int | None = None) -> KeyType | None:
        self._ensure_consumer()
        made_key = self.make_and_validate_key(key, version=version)
        with self._lock:
            if made_key not in self._cache or self._has_expired(made_key):
                return None
        return KeyType.STRING

    def info(self, section: str | None = None) -> dict[str, Any]:
        self._ensure_consumer()
        now = time.time()
        with self._lock:
            key_count = len(self._cache)
            expires_count = sum(1 for exp in self._expire_info.values() if exp is not None and exp > now)
        consumer_alive = self._consumer_alive()
        last_read_age = round(now - self._last_read_time, 1) if self._last_read_time else None
        return {
            "server": {
                "redis_version": f"StreamCache (stream: {self._stream_key})",
                "transport": self._transport_alias,
            },
            "keyspace": {
                "db0": {
                    "keys": key_count,
                    "expires": expires_count,
                },
            },
            "sync": {
                "consumer_alive": consumer_alive,
                "last_read_age_seconds": last_read_age,
                "last_stream_id": self._last_id,
                "pod_id": self._pod_id,
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
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]:
        yield from self.keys(pattern, version=version)

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
        return sum(1 for k in matching if self.delete(k, version=version))


__all__ = [
    "StreamCache",
]
