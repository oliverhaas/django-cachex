"""Local read cache kept coherent by ``CLIENT TRACKING BCAST`` invalidations from a Redis/Valkey transport."""

import logging
import os
import time
from collections import OrderedDict
from fnmatch import fnmatchcase
from functools import cached_property
from itertools import combinations
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING, Any

from django.core.cache.backends.base import DEFAULT_TIMEOUT, default_key_func
from django.core.exceptions import ImproperlyConfigured

from django_cachex.cache._delegation import DelegatingCacheMixin
from django_cachex.cache.base import BaseCachex, CachexSupportLevel
from django_cachex.cache.resp import RespCache
from django_cachex.exceptions import NotSupportedError
from django_cachex.stampede import should_recompute, should_recompute_remaining

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import timedelta

    from django_cachex.adapters.protocols import Invalidation, InvalidationListenerProtocol
    from django_cachex.stampede import StampedeConfig

logger = logging.getLogger(__name__)

_MISS = object()
# Per-command timeout for the listener's own connections (connect, CLIENT ID, PING).
_LISTENER_TIMEOUT = 5.0


class _TrackingState:
    """Per-process store, listener thread and counters shared by every instance of one storage key."""

    __slots__ = (
        "coherence",
        "connected",
        "flushes",
        "hits",
        "initialized",
        "invalidations",
        "last_message_time",
        "listener",
        "listener_thread",
        "lock",
        "max_entries",
        "misses",
        "pending",
        "pid",
        "poll_timeout",
        "reconnects",
        "start_lock",
        "stop_event",
        "store",
    )

    def __init__(self, *, max_entries: int, poll_timeout: float, coherence: str) -> None:
        self.pid = os.getpid()
        self.max_entries = max_entries
        self.poll_timeout = poll_timeout
        self.coherence = coherence
        # Guards store, pending, connected, listener and the counters.
        self.lock = Lock()
        # Guards listener_thread, stop_event and initialized; never held
        # together with ``lock`` while the transport is being called.
        self.start_lock = Lock()
        # Made key -> (encoded value, monotonic expiry or None); LRU by access.
        self.store: OrderedDict[str, tuple[Any, float | None]] = OrderedDict()
        # Made key -> token of the fetch in flight for it. An invalidation
        # drops the token so a reply that raced the write is never stored.
        self.pending: dict[str, object] = {}
        # TTL coherence needs no listener, so the store is live from the start.
        self.connected = coherence == "ttl"
        self.listener: InvalidationListenerProtocol | None = None
        self.listener_thread: Thread | None = None
        self.stop_event = Event()
        self.initialized = False
        self.hits = 0
        self.misses = 0
        self.invalidations = 0
        self.flushes = 0
        self.reconnects = 0
        self.last_message_time = 0.0

    # -- Local store --

    def local_get(self, made_key: str, now: float, stampede: StampedeConfig | None) -> Any:
        """Return the encoded value for ``made_key`` or ``_MISS``; every hit rolls XFetch's dice."""
        with self.lock:
            entry = self.store.get(made_key)
            if entry is None:
                return _MISS
            raw, expires_at = entry
            if expires_at is not None:
                remaining = expires_at - now
                if stampede is not None and isinstance(raw, bytes):
                    expired = should_recompute_remaining(remaining, stampede)
                else:
                    expired = remaining <= 0
                if expired:
                    del self.store[made_key]
                    return _MISS
            self.store.move_to_end(made_key)
            self.hits += 1
            return raw

    def begin_fetch(self, made_keys: list[str]) -> dict[str, object]:
        """Register fetches for ``made_keys``; no tokens while disconnected."""
        with self.lock:
            self.misses += len(made_keys)
            if not self.connected:
                return {}
            tokens: dict[str, object] = {}
            for made_key in made_keys:
                token = object()
                self.pending[made_key] = token
                tokens[made_key] = token
            return tokens

    def absorb(self, made_key: str, raw: Any, expires_at: float | None, token: object) -> None:
        """Store a fetched value unless its fetch was invalidated meanwhile."""
        with self.lock:
            if self.pending.get(made_key) is not token:
                return
            del self.pending[made_key]
            if not self.connected:
                return
            self.store[made_key] = (raw, expires_at)
            self.store.move_to_end(made_key)
            while len(self.store) > self.max_entries:
                self.store.popitem(last=False)

    def forget(self, made_key: str, token: object | None) -> None:
        """Drop the fetch token for ``made_key`` without storing anything."""
        if token is None:
            return
        with self.lock:
            if self.pending.get(made_key) is token:
                del self.pending[made_key]

    def discard(self, made_keys: Iterable[str]) -> None:
        with self.lock:
            for made_key in made_keys:
                self.store.pop(made_key, None)
                self.pending.pop(made_key, None)

    def flush(self) -> None:
        with self.lock:
            self._clear()
            self.flushes += 1

    def _clear(self) -> None:
        """Drop the store and every fetch in flight; the caller holds ``lock``."""
        self.store.clear()
        self.pending.clear()

    # -- Listener events --

    def apply(self, message: Invalidation) -> None:
        with self.lock:
            if message.keys is None:
                self._clear()
                self.flushes += 1
            else:
                for made_key in message.keys:
                    self.store.pop(made_key, None)
                    self.pending.pop(made_key, None)
                self.invalidations += len(message.keys)
            self.last_message_time = time.time()

    def on_connect(self, listener: InvalidationListenerProtocol, *, reconnect: bool) -> None:
        with self.lock:
            self._clear()
            self.listener = listener
            self.connected = True
            if reconnect:
                self.reconnects += 1

    def on_disconnect(self) -> None:
        with self.lock:
            self.connected = False
            self.listener = None
            self._clear()
            self.flushes += 1

    def shutdown(self) -> None:
        """Stop the listener thread with a bounded join and drop the local store."""
        with self.start_lock:
            thread = self.listener_thread
            if thread is not None:
                self.stop_event.set()
                join_timeout = min(10.0, self.poll_timeout + _LISTENER_TIMEOUT + 1.0)
                thread.join(timeout=join_timeout)
                if thread.is_alive():
                    logger.warning(
                        "TrackingCache: listener thread still alive after %.1fs; abandoning it",
                        join_timeout,
                    )
                self.listener_thread = None
            self.initialized = False
            with self.lock:
                listener = self.listener
                self.listener = None
                self.connected = self.coherence == "ttl"
                self._clear()
            if listener is not None:
                listener.close()


_TRACKING_REGISTRY: dict[str, _TrackingState] = {}
_REGISTRY_LOCK = Lock()


class TrackingCache(DelegatingCacheMixin, BaseCachex):
    """Read-through local cache over ``OPTIONS['transport']``.

    Invalidated by ``CLIENT TRACKING BCAST``; with ``coherence='ttl'`` no listener
    runs and ``local_timeout`` alone bounds staleness.
    """

    _cachex_support: CachexSupportLevel = "limited"

    def __init__(self, server: str, params: dict[str, Any]) -> None:
        super().__init__(params)
        options = params.get("OPTIONS", {})
        transport = options.get("transport")
        if not transport or not isinstance(transport, str):
            msg = f"TrackingCache requires OPTIONS['transport'] naming a Redis/Valkey cache alias. Got: {transport!r}"
            raise ImproperlyConfigured(msg)
        if "KEY_PREFIX" in options or params.get("KEY_PREFIX"):
            msg = (
                "TrackingCache does not apply KEY_PREFIX; keys are made by the transport. "
                "Set KEY_PREFIX on the transport cache alias instead."
            )
            raise ImproperlyConfigured(msg)
        self._transport_alias: str = transport
        self._storage_key: str = server or transport
        self._explicit_prefixes: tuple[str, ...] | None = self._validate_prefixes(options.get("prefixes"))
        local_timeout = options.get("local_timeout")
        self._local_timeout: float | None = None if local_timeout is None else float(local_timeout)
        self._poll_timeout: float = float(options.get("poll_timeout", 1.0))
        self._health_check_interval: float = float(options.get("health_check_interval", 15.0))
        self._reconnect_delay: float = float(options.get("reconnect_delay", 1.0))
        coherence = options.get("coherence", "tracking")
        if coherence not in ("tracking", "ttl"):
            msg = f"TrackingCache OPTIONS['coherence'] must be 'tracking' or 'ttl'. Got: {coherence!r}"
            raise ImproperlyConfigured(msg)
        if coherence == "ttl" and self._local_timeout is None:
            msg = "TrackingCache with coherence='ttl' requires OPTIONS['local_timeout']; nothing else bounds staleness."
            raise ImproperlyConfigured(msg)
        self._coherence: str = coherence

        # A forked child inherits the registry but none of the parent's threads.
        pid = os.getpid()
        with _REGISTRY_LOCK:
            state = _TRACKING_REGISTRY.get(self._storage_key)
            if state is None or state.pid != pid:
                state = _TrackingState(
                    max_entries=self._max_entries,
                    poll_timeout=self._poll_timeout,
                    coherence=coherence,
                )
                _TRACKING_REGISTRY[self._storage_key] = state
            elif state.coherence != coherence:
                msg = (
                    f"TrackingCache aliases sharing LOCATION {self._storage_key!r} disagree on coherence: "
                    f"{state.coherence!r} vs {coherence!r}."
                )
                raise ImproperlyConfigured(msg)
        self._state = state

        self._cachex_location = f"tracking:{self._storage_key} [transport: {self._transport_alias}]"

    @staticmethod
    def _validate_prefixes(prefixes: Any) -> tuple[str, ...] | None:
        if prefixes is None:
            return None
        if isinstance(prefixes, str) or not all(isinstance(p, str) for p in prefixes):
            msg = f"TrackingCache OPTIONS['prefixes'] must be a list of strings. Got: {prefixes!r}"
            raise ImproperlyConfigured(msg)
        resolved = tuple(prefixes)
        if not resolved:
            msg = "TrackingCache OPTIONS['prefixes'] must not be empty; use [''] to track every key."
            raise ImproperlyConfigured(msg)
        for first, second in combinations(resolved, 2):
            if first.startswith(second) or second.startswith(first):
                msg = (
                    f"TrackingCache OPTIONS['prefixes'] {first!r} and {second!r} overlap; "
                    "the server rejects overlapping BCAST prefixes."
                )
                raise ImproperlyConfigured(msg)
        return resolved

    # -- Transport --

    @cached_property
    def _transport(self) -> RespCache:
        from django.core.cache import caches

        transport = caches[self._transport_alias]
        if transport is self:
            msg = f"TrackingCache transport alias {self._transport_alias!r} resolves to the TrackingCache itself."
            raise ImproperlyConfigured(msg)
        if not isinstance(transport, RespCache):
            msg = (
                f"TrackingCache transport {self._transport_alias!r} must be a Redis/Valkey backend "
                f"(redis-py or valkey-py). Got: {type(transport).__name__}"
            )
            raise ImproperlyConfigured(msg)
        return transport

    @property
    def _delegation_target(self) -> RespCache:
        return self._transport

    @cached_property
    def _stampede(self) -> StampedeConfig | None:
        return self._transport.adapter.resolve_stampede(None)

    @cached_property
    def _prefixes(self) -> tuple[str, ...]:
        """Tracked key prefixes: explicit, or derived from the transport's key layout."""
        if self._explicit_prefixes is not None:
            return self._explicit_prefixes
        transport = self._transport
        if transport.key_func is default_key_func:
            return (f"{transport.key_prefix}:",)
        return ("",)

    def _local_key(self, key: str, version: int | None) -> str:
        """Make the key; under tracking, refuse one no tracked prefix covers: writes to it would never be seen."""
        made_key = self.make_and_validate_key(key, version=version)
        if self._coherence == "ttl":
            return made_key
        prefixes = self._prefixes
        if "" in prefixes or made_key.startswith(prefixes):
            return made_key
        msg = (
            f"TrackingCache key {made_key!r} lies outside every tracked prefix {list(prefixes)!r}. "
            "Set OPTIONS['prefixes'] to match the transport's key layout, or [''] to track every key."
        )
        raise ImproperlyConfigured(msg)

    # -- Listener lifecycle --

    def _listener_alive(self) -> bool:
        thread = self._state.listener_thread
        return thread is not None and thread.is_alive()

    def _ensure_listener(self) -> None:
        """Start (or restart) the listener thread with double-checked locking; TTL coherence has none."""
        if self._coherence == "ttl":
            return
        state = self._state
        if state.initialized and self._listener_alive():
            return
        with state.start_lock:
            if state.initialized and self._listener_alive():
                return
            if state.initialized:
                logger.warning("TrackingCache: listener thread died, restarting (%s)", self._storage_key)
            self._start_listener()
            state.initialized = True

    def _open_listener(self) -> InvalidationListenerProtocol:
        return self._transport.adapter.invalidation_listener(list(self._prefixes), timeout=_LISTENER_TIMEOUT)

    def _start_listener(self) -> None:
        state = self._state
        # The first connect runs in the caller's thread so a transport that
        # cannot host a listener fails loudly on first use.
        try:
            listener: InvalidationListenerProtocol | None = self._open_listener()
        except NotSupportedError as exc:
            msg = f"TrackingCache transport {self._transport_alias!r} cannot host an invalidation listener: {exc}"
            raise ImproperlyConfigured(msg) from exc
        except Exception:
            logger.warning(
                "TrackingCache: invalidation listener for %s failed to connect; "
                "serving from the transport until it does",
                self._storage_key,
                exc_info=True,
            )
            listener = None
        if listener is not None:
            state.on_connect(listener, reconnect=False)
        stop_event = Event()
        state.stop_event = stop_event
        thread = Thread(
            target=self._listener_loop,
            args=(stop_event, listener),
            name=f"tracking-cache-{self._storage_key}",
            daemon=True,
        )
        try:
            thread.start()
        except BaseException:
            # Nothing may look connected without a thread to keep it so.
            state.on_disconnect()
            if listener is not None:
                listener.close()
            raise
        state.listener_thread = thread

    def _listener_loop(self, stop_event: Event, listener: InvalidationListenerProtocol | None) -> None:
        state = self._state
        while not stop_event.is_set():
            if listener is None:
                if stop_event.wait(self._reconnect_delay):
                    break
                try:
                    listener = self._open_listener()
                except Exception:
                    logger.warning(
                        "TrackingCache: invalidation listener for %s failed to reconnect",
                        self._storage_key,
                        exc_info=True,
                    )
                    continue
                state.on_connect(listener, reconnect=True)
                logger.info("TrackingCache: invalidation listener for %s connected", self._storage_key)
            try:
                self._serve(stop_event, listener)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "TrackingCache: invalidation listener for %s lost (%r); local store flushed",
                    self._storage_key,
                    exc,
                )
            state.on_disconnect()
            listener.close()
            listener = None

    def _serve(self, stop_event: Event, listener: InvalidationListenerProtocol) -> None:
        """Pump invalidations until stopped; any exception means the listener is lost."""
        state = self._state
        idle_since = time.monotonic()
        while not stop_event.is_set():
            message = listener.poll(self._poll_timeout)
            if message is not None:
                state.apply(message)
                idle_since = time.monotonic()
                continue
            if time.monotonic() - idle_since >= self._health_check_interval:
                listener.ping()
                idle_since = time.monotonic()

    def shutdown(self) -> None:
        """Stop this storage key's listener thread and drop its local store."""
        self._state.shutdown()

    # -- Fetching --

    def _fetch(self, made_keys: list[str]) -> dict[str, Any]:
        """GET + PTTL each key on the transport; store what may be kept."""
        state = self._state
        tokens = state.begin_fetch(made_keys)
        try:
            pipe = self._transport.adapter.pipeline(transaction=False)
            for made_key in made_keys:
                pipe.get(made_key)
                pipe.pttl(made_key)
            results = pipe.execute()
        except BaseException:
            for made_key, token in tokens.items():
                state.forget(made_key, token)
            raise
        return self._absorb(made_keys, results, tokens)

    async def _afetch(self, made_keys: list[str]) -> dict[str, Any]:
        state = self._state
        tokens = state.begin_fetch(made_keys)
        try:
            pipe = await self._transport.adapter.apipeline(transaction=False)
            for made_key in made_keys:
                pipe.get(made_key)
                pipe.pttl(made_key)
            results = await pipe.execute()
        except BaseException:
            for made_key, token in tokens.items():
                state.forget(made_key, token)
            raise
        return self._absorb(made_keys, results, tokens)

    def _absorb(self, made_keys: list[str], results: list[Any], tokens: dict[str, object]) -> dict[str, Any]:
        """Apply the transport's stampede rule, bound the local lifetime, store."""
        state = self._state
        now = time.monotonic()
        config = self._stampede
        buffer_ms = config.buffer * 1000 if config else 0
        found: dict[str, Any] = {}
        for index, made_key in enumerate(made_keys):
            raw, pttl = results[2 * index], results[2 * index + 1]
            token = tokens.get(made_key)
            if raw is None:
                state.forget(made_key, token)
                continue
            # -1 (no expiry) is kept as is; -2 or an unexpected reply is served but not kept.
            keep = pttl == -1
            expires_at: float | None = None
            if isinstance(pttl, int) and pttl >= 0:
                ttl_s = (pttl + 500) // 1000
                if config and isinstance(raw, bytes) and ttl_s > 0 and should_recompute(ttl_s, config):
                    state.forget(made_key, token)
                    continue
                remaining = (pttl - buffer_ms) / 1000
                keep = remaining > 0
                expires_at = now + remaining
            if self._local_timeout is not None:
                cap = now + self._local_timeout
                expires_at = cap if expires_at is None else min(expires_at, cap)
            found[made_key] = raw
            if keep and token is not None:
                state.absorb(made_key, raw, expires_at, token)
            else:
                state.forget(made_key, token)
        return found

    def _evict(self, key: str, version: int | None) -> None:
        self._state.discard((self.make_key(key, version=version),))

    def _evict_many(self, keys: Iterable[str], version: int | None) -> None:
        self._state.discard(self.make_key(key, version=version) for key in keys)

    def _evict_pattern(self, pattern: str, version: int | None) -> None:
        transport = self._transport
        with self._state.lock:
            candidates = set(self._state.store) | set(self._state.pending)
        matching = []
        for made_key in candidates:
            original = transport.reverse_key(made_key)
            if fnmatchcase(original, pattern) and transport.make_key(original, version=version) == made_key:
                matching.append(made_key)
        self._state.discard(matching)

    # -- Reads --

    def get(self, key: str, default: Any = None, version: int | None = None) -> Any:
        self._ensure_listener()
        made_key = self._local_key(key, version)
        raw = self._state.local_get(made_key, time.monotonic(), self._stampede)
        if raw is _MISS:
            raw = self._fetch([made_key]).get(made_key, _MISS)
        if raw is _MISS:
            return default
        return self._transport.decode(raw)

    async def aget(self, key: str, default: Any = None, version: int | None = None) -> Any:
        self._ensure_listener()
        made_key = self._local_key(key, version)
        raw = self._state.local_get(made_key, time.monotonic(), self._stampede)
        if raw is _MISS:
            raw = (await self._afetch([made_key])).get(made_key, _MISS)
        if raw is _MISS:
            return default
        return self._transport.decode(raw)

    def get_or_set(
        self,
        key: str,
        default: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> Any:
        """Django's ``get_or_set``, writing with ``set`` rather than ``add`` when the transport prevents stampedes."""
        value = self.get(key, _MISS, version=version)
        if value is not _MISS:
            return value
        if callable(default):
            default = default()
        if self._stampede is not None:
            # The miss may be an early-recompute signal for a key that still
            # exists, which add (NX) would leave untouched.
            self.set(key, default, timeout=timeout, version=version)
        else:
            self.add(key, default, timeout=timeout, version=version)
        return self.get(key, default, version=version)

    async def aget_or_set(
        self,
        key: str,
        default: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> Any:
        """Async twin of :meth:`get_or_set`."""
        value = await self.aget(key, _MISS, version=version)
        if value is not _MISS:
            return value
        if callable(default):
            default = default()
        if self._stampede is not None:
            await self.aset(key, default, timeout=timeout, version=version)
        else:
            await self.aadd(key, default, timeout=timeout, version=version)
        return await self.aget(key, default, version=version)

    def _split_local(
        self,
        keys: Iterable[str],
        version: int | None,
    ) -> tuple[dict[str, str], dict[str, Any], list[str]]:
        """Map made keys to originals and serve what the local store has."""
        self._ensure_listener()
        now = time.monotonic()
        stampede = self._stampede
        key_map: dict[str, str] = {}
        local: dict[str, Any] = {}
        missing: list[str] = []
        for key in keys:
            made_key = self._local_key(key, version)
            if made_key in key_map:
                continue
            key_map[made_key] = key
            raw = self._state.local_get(made_key, now, stampede)
            if raw is _MISS:
                missing.append(made_key)
            else:
                local[made_key] = raw
        return key_map, local, missing

    def _merge(self, key_map: dict[str, str], *found: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for part in found:
            merged.update(part)
        decode = self._transport.decode
        return {key: decode(merged[made_key]) for made_key, key in key_map.items() if made_key in merged}

    def get_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        key_map, local, missing = self._split_local(keys, version)
        fetched = self._fetch(missing) if missing else {}
        return self._merge(key_map, local, fetched)

    async def aget_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        key_map, local, missing = self._split_local(keys, version)
        fetched = await self._afetch(missing) if missing else {}
        return self._merge(key_map, local, fetched)

    def has_key(self, key: str, version: int | None = None) -> bool:
        self._ensure_listener()
        made_key = self._local_key(key, version)
        if self._state.local_get(made_key, time.monotonic(), self._stampede) is not _MISS:
            return True
        return self._transport.has_key(key, version=version)

    async def ahas_key(self, key: str, version: int | None = None) -> bool:
        self._ensure_listener()
        made_key = self._local_key(key, version)
        if self._state.local_get(made_key, time.monotonic(), self._stampede) is not _MISS:
            return True
        return await self._transport.ahas_key(key, version=version)

    # -- Writes: transport first, then evict the local copy --

    def add(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        result = self._transport.add(key, value, timeout=timeout, version=version)
        self._evict(key, version)
        return result

    async def aadd(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        result = await self._transport.aadd(key, value, timeout=timeout, version=version)
        self._evict(key, version)
        return result

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
    ) -> Any:
        result = self._transport.set(key, value, timeout=timeout, version=version, nx=nx, xx=xx, get=get)
        self._evict(key, version)
        return result

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
    ) -> Any:
        result = await self._transport.aset(key, value, timeout=timeout, version=version, nx=nx, xx=xx, get=get)
        self._evict(key, version)
        return result

    def touch(self, key: str, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        result = self._transport.touch(key, timeout=timeout, version=version)
        self._evict(key, version)
        return result

    async def atouch(self, key: str, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        result = await self._transport.atouch(key, timeout=timeout, version=version)
        self._evict(key, version)
        return result

    def delete(self, key: str, version: int | None = None) -> bool:
        result = self._transport.delete(key, version=version)
        self._evict(key, version)
        return result

    async def adelete(self, key: str, version: int | None = None) -> bool:
        result = await self._transport.adelete(key, version=version)
        self._evict(key, version)
        return result

    def incr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        result = self._transport.incr(key, delta, version=version)
        self._evict(key, version)
        return result

    async def aincr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        result = await self._transport.aincr(key, delta, version=version)
        self._evict(key, version)
        return result

    def set_many(
        self,
        data: dict[str, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[str]:
        result = self._transport.set_many(data, timeout=timeout, version=version)
        self._evict_many(data, version)
        return result

    async def aset_many(
        self,
        data: dict[str, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[str]:
        result = await self._transport.aset_many(data, timeout=timeout, version=version)
        self._evict_many(data, version)
        return result

    def delete_many(self, keys: Iterable[str], version: int | None = None) -> Any:
        keys = list(keys)
        result = self._transport.delete_many(keys, version=version)
        self._evict_many(keys, version)
        return result

    async def adelete_many(self, keys: Iterable[str], version: int | None = None) -> Any:
        keys = list(keys)
        result = await self._transport.adelete_many(keys, version=version)
        self._evict_many(keys, version)
        return result

    def clear(self) -> bool:  # type: ignore[override]
        result = self._transport.clear()
        self._state.flush()
        return result

    async def aclear(self) -> bool:  # type: ignore[override]
        result = await self._transport.aclear()
        self._state.flush()
        return result

    def expire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        result = self._delegate("expire", key, timeout, version=version)
        self._evict(key, version)
        return result

    async def aexpire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        result = await self._adelegate("aexpire", key, timeout, version=version)
        self._evict(key, version)
        return result

    def persist(self, key: str, version: int | None = None) -> bool:
        result = self._delegate("persist", key, version=version)
        self._evict(key, version)
        return result

    async def apersist(self, key: str, version: int | None = None) -> bool:
        result = await self._adelegate("apersist", key, version=version)
        self._evict(key, version)
        return result

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        result = self._delegate("delete_pattern", pattern, version=version, itersize=itersize)
        self._evict_pattern(pattern, version)
        return result

    async def adelete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        result = await self._adelegate("adelete_pattern", pattern, version=version, itersize=itersize)
        self._evict_pattern(pattern, version)
        return result

    # -- Lifecycle and diagnostics --

    def close(self, **kwargs: Any) -> None:
        """No-op: the listener outlives requests. Use ``shutdown()`` to stop it."""

    async def aclose(self, **kwargs: Any) -> None:
        """No-op, see :meth:`close`."""

    def info(self, section: str | None = None) -> dict[str, Any]:
        self._ensure_listener()
        if section == "tracking":
            return {"tracking": self._tracking_info()}
        info = dict(self._delegate("info", section=section))
        if section is None:
            info["tracking"] = self._tracking_info()
        return info

    def _tracking_info(self) -> dict[str, Any]:
        state = self._state
        with state.lock:
            snapshot: dict[str, Any] = {
                "coherence": self._coherence,
                "connected": state.connected,
                "entries": len(state.store),
                "hits": state.hits,
                "misses": state.misses,
                "invalidations": state.invalidations,
                "flushes": state.flushes,
                "reconnects": state.reconnects,
            }
            last_message_time = state.last_message_time
        snapshot["listener_alive"] = self._listener_alive()
        snapshot["last_message_age_seconds"] = round(time.time() - last_message_time, 1) if last_message_time else None
        snapshot["prefixes"] = list(self._prefixes) if self._coherence == "tracking" else []
        return snapshot


__all__ = ["TrackingCache"]
