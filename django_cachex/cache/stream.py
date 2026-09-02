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
            "BACKEND": "django_cachex.cache.RedisCache",  # or ValkeyCache, etc.
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

``transport`` is the alias of any cachex ``RespCache`` subclass used for
stream I/O.
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
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from functools import cached_property
from itertools import count
from threading import BoundedSemaphore, Event, Lock, Thread
from typing import TYPE_CHECKING, Any, ClassVar

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.core.cache.backends.locmem import LocMemCache
from django.core.exceptions import ImproperlyConfigured

from django_cachex.cache.base import BaseCachex
from django_cachex.exceptions import NotSupportedError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from django_cachex.cache.base import CachexSupportLevel

logger = logging.getLogger(__name__)


class _StreamSync:
    """Pod identity, consumer thread and publisher shared by one storage key.

    Django's ``CacheHandler`` hands out one backend instance per thread and per
    async context, so per-instance sync state would mean a consumer and a
    publisher thread per ASGI request (neither collectable: the thread frame
    pins the instance) and a pod id per WSGI worker thread, which makes sibling
    consumers treat this process's own writes as remote. Instances share one of
    these instead, keyed in ``_SYNC_REGISTRY`` by the storage key
    ``LocMemCache`` also keys its dict, lock and expiry map with.
    """

    __slots__ = (
        "atexit_registered",
        "block_timeout",
        "consumer_thread",
        "initialized",
        "last_id",
        "last_read_time",
        "lock",
        "max_pending_publishes",
        "pending",
        "pid",
        "pod_id",
        "publish_budget",
        "publish_executor",
        "publish_executor_shutdown",
        "publish_shutdown_timeout",
        "seq_counter",
        "stop_event",
    )

    def __init__(
        self,
        *,
        block_timeout: int,
        max_pending_publishes: int,
        publish_shutdown_timeout: float,
    ) -> None:
        self.pid = os.getpid()
        self.pod_id = f"{self.pid}-{uuid.uuid4().hex[:12]}"
        self.block_timeout = block_timeout
        self.max_pending_publishes = max_pending_publishes
        self.publish_shutdown_timeout = publish_shutdown_timeout
        self.lock = Lock()
        self.stop_event = Event()
        self.consumer_thread: Thread | None = None
        self.initialized = False
        self.atexit_registered = False
        self.last_id = "$"
        self.last_read_time = 0.0
        self.seq_counter = count(1)
        # Made key -> sequence number of this pod's latest local mutation of
        # it, dropped once the matching broadcast has been consumed.
        self.pending: dict[str, int] = {}
        self.publish_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sync-pub")
        self.publish_executor_shutdown = False
        self.publish_budget = BoundedSemaphore(max_pending_publishes)

    def next_seq(self) -> int:
        return next(self.seq_counter)

    def shutdown(self) -> None:
        """Stop the consumer thread and the publish executor with bounded waits.

        The consumer is parked in ``XREAD BLOCK block_timeout`` (ms), so the
        join grace has to outlast one block window: ``block_timeout + 1s``,
        capped at 10s. A thread still alive after that is abandoned, which
        leaks a daemon thread but does not block process shutdown. Its stop
        event stays set, and the next start mints a fresh one, so it can never
        come back alongside its replacement with both advancing ``last_id``.

        The publish executor is bounded by ``publish_shutdown_timeout``.
        Pending futures are cancelled up front (``cancel_futures=True``), then
        the worker thread is joined with a timeout; an in-flight XADD that
        does not drain in time is abandoned with a warning.
        """
        with self.lock:
            thread = self.consumer_thread
            if thread is not None:
                self.stop_event.set()
                join_timeout = min(10.0, (self.block_timeout / 1000.0) + 1.0)
                thread.join(timeout=join_timeout)
                if thread.is_alive():
                    logger.warning(
                        "StreamCache: consumer thread still alive after %.1fs; abandoning it",
                        join_timeout,
                    )
                self.consumer_thread = None
                self.initialized = False
            executor = self.publish_executor
            self.publish_executor_shutdown = True
        executor.shutdown(wait=False, cancel_futures=True)
        # Executor.shutdown has no timeout; join its (private) worker threads with one.
        worker_threads = list(getattr(executor, "_threads", ()) or ())
        deadline = time.time() + self.publish_shutdown_timeout
        for worker in worker_threads:
            worker.join(timeout=max(0.0, deadline - time.time()))
        still_alive = [t for t in worker_threads if t.is_alive()]
        if still_alive:
            logger.warning(
                "StreamCache: publish worker(s) still alive after %.1fs; abandoning %d thread(s)",
                self.publish_shutdown_timeout,
                len(still_alive),
            )


_SYNC_REGISTRY: dict[str, _StreamSync] = {}
_MULTIPLEXED_POLL_INTERVAL = 0.025


def _newer_stream_id(entry_id: str, last_id: str) -> bool:
    """Whether ``entry_id`` lies past ``last_id``; ``$`` precedes every entry."""
    if last_id == "$":
        return True
    ms, _, seq = entry_id.partition("-")
    last_ms, _, last_seq = last_id.partition("-")
    return (int(ms), int(seq)) > (int(last_ms), int(last_seq))


_REGISTRY_LOCK = Lock()


class StreamCache(BaseCachex, LocMemCache):
    """Stream-synchronized in-memory cache.

    Extends Django's ``LocMemCache`` with cross-pod synchronization. Reads are
    local dict lookups (inherited from ``LocMemCache``). Writes update the
    local dict and publish to a Redis Stream via the transport cache's
    ``xadd``. A daemon thread consumes the stream via ``xread`` and applies
    changes. Supported operations are eventually consistent.

    ``add``, ``incr``, and ``decr`` raise ``NotSupportedError``: their
    semantics (atomic check-and-set, atomic increment) can't be provided
    with eventual consistency. Use the transport cache directly for these.
    The rest of the cachex surface (hashes, lists, sets, sorted sets, locks,
    pipelines, Lua) has no replicated equivalent either and raises
    ``NotSupportedError`` from :class:`~django_cachex.cache.base.BaseCachex`;
    key/TTL/type/info ops are implemented locally.

    Convergence: the stream is the one order every pod agrees on, and each
    entry carries the final value rather than a delta, so applying entries in
    stream order is idempotent and leaves every pod on the last entry written
    for a key. A pod applies its own entries too, which is what makes two pods
    writing the same key inside the propagation window converge instead of
    ending up holding each other's value. An entry is skipped only where a
    later local write to the same key (or a local ``clear``) has already
    replaced it, so a writer never reads back a value it has moved past.

    Write ordering: every mutating operation holds the local lock across both
    the local update and the enqueue of its broadcast, and a single-worker
    executor performs the XADDs in enqueue order, so a pod's entries appear in
    the order its local writes were applied. Broadcasts remain best-effort: an
    entry is dropped when the publish backlog is full or the transport errors,
    so the stream is a replication feed, not a durable log.

    The consumer thread is restarted if it dies; use ``info()["sync"]`` to
    monitor consumer health, last read age, and stream position.
    """

    _cachex_support: CachexSupportLevel = "cachex"

    # Type declarations for attributes and methods inherited from LocMemCache.
    if TYPE_CHECKING:
        from collections import OrderedDict
        from threading import Lock

        _cache: OrderedDict[str, bytes]
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

        # Aliases sharing a ``stream_key`` but not a ``LOCATION`` get separate
        # local storage and separate sync state, i.e. they act as separate pods.
        storage_key = server or self._stream_key
        super().__init__(storage_key, params)

        # A forked child inherits the registry but none of the parent's threads, and
        # reusing the parent's pod id would make it treat the parent's writes as its own.
        pid = os.getpid()
        with _REGISTRY_LOCK:
            state = _SYNC_REGISTRY.get(storage_key)
            if state is None or state.pid != pid:
                state = _StreamSync(
                    block_timeout=self._block_timeout,
                    max_pending_publishes=options.get("max_pending_publishes", 1000),
                    publish_shutdown_timeout=options.get("publish_shutdown_timeout", 5.0),
                )
                _SYNC_REGISTRY[storage_key] = state
        self._sync = state

        # Admin display: show stream key and transport alias as location
        self._cachex_location = f"stream:{self._stream_key} [transport: {self._transport_alias}]"

        self._register_interpreter_shutdown()

    def _register_interpreter_shutdown(self) -> None:
        """Run the bounded ``shutdown`` before the executor's unbounded join.

        ``ThreadPoolExecutor`` worker threads are non-daemon and joined with
        no timeout by ``concurrent.futures.thread._python_exit``, so a
        transport hung inside ``_do_xadd`` would stall interpreter shutdown
        forever. ``threading._register_atexit`` hooks run at the top of
        ``threading._shutdown()`` in reverse registration order, i.e. before
        that join, which ``atexit`` (which runs after it) cannot do. One hook
        per storage key: it holds the shared sync state, not a cache instance,
        so a discarded instance is still collectable.
        """
        state = self._sync
        with state.lock:
            if state.atexit_registered:
                return
            state.atexit_registered = True

        def _bounded_shutdown() -> None:
            with contextlib.suppress(Exception):
                state.shutdown()

        # AttributeError: no private hook on this interpreter. RuntimeError:
        # 3.14 refuses a registration once shutdown has already begun.
        with contextlib.suppress(AttributeError, RuntimeError):
            threading._register_atexit(_bounded_shutdown)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]

    # -- Transport (lazy) --

    @cached_property
    def _transport(self) -> BaseCache:
        from django.core.cache import caches

        return caches[self._transport_alias]

    @cached_property
    def _transport_is_multiplexed(self) -> bool:
        return bool(getattr(getattr(self._transport, "adapter", None), "multiplexed", False))

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
        keys: Sequence[str] | None = None,
    ) -> None:
        """Publish a cache mutation to the stream (non-blocking, best-effort).

        ``val`` is the original Python value. The transport cache's serializer
        and compressor handle wire encoding. The single-worker executor
        preserves stream order while keeping the calling thread off the
        network round-trip. Mutators call this while holding ``self._lock``
        so broadcasts are enqueued in local write order.

        The pending-publish budget caps how many broadcasts may be queued at
        once. When it is exhausted the new publish is dropped with a warning,
        trading durability for bounded memory under sustained write bursts
        that outpace XADD.
        """
        state = self._sync
        if not state.publish_budget.acquire(blocking=False):
            logger.warning(
                "StreamCache: publish backlog full (cap=%d); dropping %s broadcast",
                state.max_pending_publishes,
                op,
            )
            return
        seq = state.next_seq()
        fields: dict[str, Any] = {
            "op": op,
            "pod": state.pod_id,
            "seq": seq,
            "key": key,
            "val": val,
            "exp": str(exp) if exp is not None else "",
        }
        if keys:
            # A list, not a joined string: any separator is legal inside a
            # Django cache key.
            fields["keys"] = list(keys)
        try:
            state.publish_executor.submit(self._do_xadd, fields, state.publish_budget)
        except RuntimeError:
            # Executor was shut down and a thread is racing the teardown. Drop
            # the publish; losing the broadcast is preferable to crashing the
            # caller. If the cache is still in active use ``_ensure_consumer``
            # rebuilds the executor on the next get/set.
            state.publish_budget.release()
            logger.warning(
                "StreamCache: publish executor closed; dropping %s broadcast",
                op,
            )
            return
        # Record what this pod has moved past, under the same ``_lock`` hold
        # the caller mutated in, so the consumer cannot read a half-written map.
        if op == "clear":
            state.pending.clear()
        else:
            for made_key in keys or ((key,) if key else ()):
                state.pending[made_key] = seq

    def _do_xadd(self, fields: dict[str, Any], budget: BoundedSemaphore) -> None:
        """Execute a single XADD via the transport's high-level API."""
        try:
            self._transport.xadd(  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                self._stream_key,
                fields,
                maxlen=self._maxlen,
                approximate=True,
            )
        except Exception:
            logger.warning(
                "StreamCache: Failed to publish %s to stream",
                fields.get("op", "?"),
                exc_info=True,
            )
        finally:
            # Return the budget this publish took, not whichever semaphore is
            # current: a restart swaps in a fresh one.
            with contextlib.suppress(ValueError):
                budget.release()

    # -- Consumer thread --

    def _consumer_alive(self) -> bool:
        thread = self._sync.consumer_thread
        return thread is not None and thread.is_alive()

    def _ensure_consumer(self) -> None:
        """Start (or restart) the consumer thread for this storage key.

        Uses double-checked locking. On every call, verifies the thread is
        actually alive. If it died (e.g. due to ``SystemExit`` or an
        unhandled ``BaseException``), it is automatically restarted so the
        pod doesn't silently fall out of sync.
        """
        state = self._sync
        if state.initialized and self._consumer_alive():
            return
        with state.lock:
            if state.initialized and self._consumer_alive():
                return
            if state.initialized:
                logger.warning(
                    "StreamCache: Consumer thread died, restarting (stream=%s)",
                    self._stream_key,
                )
            self._start_consumer()
            state.initialized = True

    def _start_consumer(self) -> None:
        state = self._sync
        # Recreate executor if it was shut down (e.g. after shutdown() + reuse)
        if state.publish_executor_shutdown:
            state.publish_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sync-pub")
            state.publish_executor_shutdown = False
            # Reset the publish budget too: the old semaphore may have been
            # drained by in-flight futures that ``cancel_futures=True``
            # rejected without running ``_do_xadd``'s release.
            state.publish_budget = BoundedSemaphore(state.max_pending_publishes)
        if self._replay_count > 0 and state.last_id == "$":
            self._replay_stream(self._replay_count)
        # A fresh event per consumer: reusing one ``shutdown`` set would revive
        # a thread it abandoned in XREAD alongside its replacement.
        stop_event = Event()
        state.stop_event = stop_event
        state.consumer_thread = Thread(
            target=self._consumer_loop,
            args=(stop_event,),
            name=f"sync-cache-{self._stream_key}",
            daemon=True,
        )
        state.consumer_thread.start()

    def _replay_stream(self, count: int) -> None:
        """Replay the last ``count`` stream entries to warm the local cache.

        Called once at startup before the consumer thread begins. Reads
        recent entries via ``XREVRANGE``, applies them oldest-first, and
        sets ``last_id`` so the consumer continues from where replay
        left off (no duplicates).
        """
        state = self._sync
        try:
            entries = self._transport.xrevrange(self._stream_key, count=count)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            if not entries:
                return
            with self._lock:
                for entry_id, fields in reversed(entries):
                    self._apply_message(fields)
                    state.last_id = entry_id
            logger.info(
                "StreamCache: Replayed %d entries from stream %s",
                len(entries),
                self._stream_key,
            )
        except Exception:
            logger.warning("StreamCache: stream replay failed", exc_info=True)

    def _consumer_loop(self, stop_event: Event) -> None:
        state = self._sync
        # A multiplexed transport (valkey-glide) carries every command on one
        # connection, so parking in XREAD BLOCK would hold up each publish.
        block = None if self._transport_is_multiplexed else self._block_timeout
        while not stop_event.is_set():
            try:
                result = self._transport.xread(  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                    streams={self._stream_key: state.last_id},
                    count=100,
                    block=block,
                )
                # Stamp every poll: ``last_read_age_seconds`` tracks consumer
                # liveness, so an idle stream would otherwise look stalled.
                state.last_read_time = time.time()
                if not result:
                    if block is None:
                        stop_event.wait(_MULTIPLEXED_POLL_INTERVAL)
                    continue
                for entries in result.values():
                    self._apply_entries(entries)
            except Exception:
                if not stop_event.is_set():
                    logger.warning(
                        "StreamCache: Consumer error, retrying in 1s",
                        exc_info=True,
                    )
                    stop_event.wait(1.0)

    def _apply_entries(self, entries: Sequence[tuple[str, dict[str, Any]]]) -> None:
        """Apply a batch read from the stream, once per entry.

        The consumer thread and a test-side ``_drain`` can hold overlapping
        batches, so each entry is checked against the shared cursor under the
        lock and applied only by whichever reader gets there first.
        """
        state = self._sync
        with self._lock:
            for entry_id, fields in entries:
                if not _newer_stream_id(entry_id, state.last_id):
                    continue
                # Advance cursor BEFORE processing so a bad
                # message is skipped, not retried forever.
                state.last_id = entry_id
                try:
                    self._apply_message(fields)
                except Exception:
                    logger.warning(
                        "StreamCache: Failed to apply message %s, skipping",
                        state.last_id,
                        exc_info=True,
                    )

    def _apply_message(self, fields: dict[str, Any]) -> None:
        """Apply a single stream message to local cache. Caller holds ``self._lock``.

        A pod applies its own entries too, which is what makes two pods writing
        one key converge: a remote entry consumed since the local write may
        have overwritten it, and stream order is the only order both pods agree
        on. Keys a later local write already replaced are dropped from an own
        entry, so the writer never reads back a value it has moved past. An
        own ``clear`` still runs, since it has to undo remote entries applied
        between the local call and the entry coming back, but it spares the
        keys written locally after it.
        """
        op = fields.get("op", "")
        handler = self._MESSAGE_HANDLERS.get(op)
        if handler is None:
            return
        if fields.get("pod") == self._sync.pod_id:
            seq = int(fields.get("seq") or 0)
            pending = self._sync.pending
            keys = fields.get("keys")
            if op == "clear":
                fields = {**fields, "keep": {key for key, key_seq in pending.items() if key_seq > seq}}
            elif keys is None:
                key = fields.get("key", "")
                if pending.get(key) != seq:
                    return
                del pending[key]
            else:
                live = [key for key in keys if pending.get(key) == seq]
                if not live:
                    return
                for key in live:
                    del pending[key]
                fields = {**fields, "keys": live}
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
        for key in fields.get("keys") or ():
            if key:
                self._delete(key)

    def _handle_clear(self, fields: dict[str, Any]) -> None:
        keep = fields.get("keep") or ()
        if not keep:
            self._cache.clear()
            self._expire_info.clear()
            return
        for key in [key for key in self._cache if key not in keep]:
            del self._cache[key]
            self._expire_info.pop(key, None)

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
        self._sync.publish_executor.submit(lambda: None).result(timeout=5.0)

    def _drain(self, timeout: float = 1.0) -> None:
        """Process all pending stream messages synchronously. For testing only.

        If the consumer hasn't consumed anything yet (``last_id`` is still
        ``$``), this reads from the beginning of the stream so that messages
        published before the drain call are visible.
        """
        state = self._sync
        read_from = state.last_id if state.last_id != "$" else "0-0"
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
                    self._apply_entries(entries)
                    read_from = entries[-1][0]
            except Exception:  # noqa: BLE001
                return

    def shutdown(self) -> None:
        """Stop this storage key's consumer thread and publish executor."""
        self._sync.shutdown()

    # -- Standard Django cache interface (LocMemCache + stream sync) --

    def get(self, key: str, default: Any = None, version: int | None = None) -> Any:
        self._ensure_consumer()
        return super().get(key, default=default, version=version)

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
        # Conditional (nx/xx) and read-prior (get) writes need atomic
        # check-and-set, which eventual, last-writer-wins replication cannot
        # provide. Reject them rather than silently ignoring the flag, the same
        # way ``add``/``incr``/``decr`` do. Use the transport cache for these.
        for flag, requested in (("nx", nx), ("xx", xx), ("get", get)):
            if requested:
                raise NotSupportedError(f"set(..., {flag}=True)", "StreamCache")
        self._ensure_consumer()
        made_key = self.make_and_validate_key(key, version=version)
        pickled = pickle.dumps(value, self.pickle_protocol)
        # Publish under the lock: out-of-order broadcasts would make replaying
        # consumers converge to a stale value.
        with self._lock:
            self._set(made_key, pickled, timeout)
            exp_time = self._expire_info.get(made_key)
            self._publish("set", key=made_key, val=value, exp=exp_time)

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
        made_keys = [self.make_and_validate_key(k, version=version) for k in keys]
        with self._lock:
            return self._delete_many_locked(made_keys)

    def _delete_many_locked(self, made_keys: list[str]) -> int:
        """Delete already-made keys locally and broadcast them as one message.

        Caller holds ``self._lock``. One broadcast, not one per key: a
        per-key burst can exhaust the publish budget and drop deletes that
        other pods then never see.
        """
        deleted = sum(1 for made_key in made_keys if self._delete(made_key))
        if made_keys:
            self._publish("delete_many", keys=made_keys)
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
        """Get a value or set it if missing.

        Mirrors ``BaseCache.get_or_set`` but writes with ``set``: ``add``
        needs an atomic check-and-set this backend can't provide. A stored
        ``None`` therefore counts as a hit (the missing-key sentinel, not
        ``None``, decides), and the value is re-read after the write so
        concurrent callers converge on whatever landed last.
        """
        val = self.get(key, self._missing_key, version=version)
        if val is self._missing_key:
            if callable(default):
                default = default()
            self.set(key, default, timeout=timeout, version=version)
            return self.get(key, default, version=version)
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
        # Inlines ``LocMemCache.clear`` (which takes the non-reentrant lock
        # itself) so the broadcast is enqueued under the same lock hold.
        with self._lock:
            self._cache.clear()
            self._expire_info.clear()
            self._publish("clear")
        return True

    def close(self, **kwargs: Any) -> None:
        """No-op. Use ``shutdown()`` to stop the consumer thread + publish executor."""

    # -- Admin methods (local implementations for fast reads) --

    def reverse_key(self, key: str) -> str:
        """Strip the ``make_key`` prefix to get the user-visible key.

        The prefix is the one ``make_key`` builds for this cache's version,
        so keys round-trip whatever ``KEY_PREFIX``/``KEY_FUNCTION`` produce;
        anything not carrying it is returned unchanged.
        """
        return key.removeprefix(self.make_key(""))

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        """List user keys matching ``pattern`` (Redis-style glob).

        Internal keys are matched against the exact prefix ``make_key``
        produces for the requested ``version`` (default: this cache's
        ``self.version``) and stripped back to user keys, so results
        round-trip through the same key pipeline writes use, whatever
        ``KEY_PREFIX``/``KEY_FUNCTION`` produce. Expired but not-yet-culled
        entries are excluded.
        """
        self._ensure_consumer()
        prefix = self.make_key("", version=version)
        with self._lock:
            internal_keys = [k for k in self._cache if not self._has_expired(k)]
        user_keys = [
            internal_key.removeprefix(prefix) for internal_key in internal_keys if internal_key.startswith(prefix)
        ]
        if pattern and pattern != "*":
            # ``fnmatch`` normcases both sides, which would make patterns
            # case-insensitive on Windows only; Redis globs never are.
            user_keys = [k for k in user_keys if fnmatch.fnmatchcase(k, pattern)]
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
                return None
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
                return None
            remaining = int((exp - time.time()) * 1000)
            return max(0, remaining)

    def persist(self, key: str, version: int | None = None) -> bool:
        return self.touch(key, timeout=None, version=version)

    def expire(self, key: str, timeout: int | float | timedelta, version: int | None = None) -> bool:  # noqa: PYI041
        # ``timedelta`` collapses to seconds. ``touch()`` takes float | None.
        seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else float(timeout)
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
        state = self._sync
        now = time.time()
        with self._lock:
            key_count = len(self._cache)
            expires_count = sum(1 for exp in self._expire_info.values() if exp is not None and exp > now)
        consumer_alive = self._consumer_alive()
        last_read_age = round(now - state.last_read_time, 1) if state.last_read_time else None
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
                "last_stream_id": state.last_id,
                "pod_id": state.pod_id,
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
        """Scan local cache keys with cursor-based pagination.

        Every live key is opaque here, so ``key_type`` either passes
        everything through (``"string"``) or matches nothing.
        """
        self._ensure_consumer()
        all_keys = self.keys(pattern, version=version)
        if key_type is not None and key_type != KeyType.STRING:
            all_keys = []
        if count is None:
            count = 100
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

    # ``make_pattern`` stays unimplemented on purpose. ``keys``/``delete_pattern``
    # fnmatch prefix-stripped user keys, so a prefixed pattern is unusable here.

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        made_keys = [self.make_and_validate_key(k, version=version) for k in self.keys(pattern, version=version)]
        with self._lock:
            return self._delete_many_locked(made_keys)


__all__ = [
    "StreamCache",
]
