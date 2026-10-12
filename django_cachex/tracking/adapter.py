"""The adapter a cache with tracking uses: tracked reads, and every write drops the copies it touches."""

import functools
import inspect
from collections.abc import Callable, Hashable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Final

from django_cachex.tracking import tracker as _tracker

if TYPE_CHECKING:
    from django_cachex.tracking.config import TrackingConfig

# Adapter methods that send no command with the write flag in COMMAND INFO. They pass through.
READ_METHODS: Final = frozenset(
    {
        # Writes through the raw client these return skip the drop, a documented gap.
        "get_client",
        "get_async_client",
        "resolve_stampede",
        "get_timeout_with_buffer",
        "aget",
        "aget_many",
        "ahas_key",
        "close",
        "aclose",
        "info",
        "slowlog_get",
        "slowlog_len",
        "keys",
        "akeys",
        "iter_keys",
        "aiter_keys",
        "scan",
        "ascan",
        "sscan",
        "asscan",
        "sscan_iter",
        "asscan_iter",
        "ttl",
        "attl",
        "pttl",
        "apttl",
        "expiretime",
        "aexpiretime",
        "type",
        "atype",
        "memory_usage",
        "amemory_usage",
        "hget",
        "ahget",
        "hgetall",
        "ahgetall",
        "hmget",
        "ahmget",
        "hlen",
        "ahlen",
        "hkeys",
        "ahkeys",
        "hvals",
        "ahvals",
        "hexists",
        "ahexists",
        "hexpiretime",
        "ahexpiretime",
        "hpttl",
        "ahpttl",
        "httl",
        "ahttl",
        "lindex",
        "alindex",
        "llen",
        "allen",
        "lpos",
        "alpos",
        "lrange",
        "alrange",
        "scard",
        "ascard",
        "sismember",
        "asismember",
        "smismember",
        "asmismember",
        "smembers",
        "asmembers",
        "srandmember",
        "asrandmember",
        "sdiff",
        "asdiff",
        "sinter",
        "asinter",
        "sunion",
        "asunion",
        "zcard",
        "azcard",
        "zcount",
        "azcount",
        "zmscore",
        "azmscore",
        "zrange",
        "azrange",
        "zrangebyscore",
        "azrangebyscore",
        "zrank",
        "azrank",
        "zrevrange",
        "azrevrange",
        "zrevrangebyscore",
        "azrevrangebyscore",
        "zrevrank",
        "azrevrank",
        "zscore",
        "azscore",
        "xinfo_consumers",
        "axinfo_consumers",
        "xinfo_groups",
        "axinfo_groups",
        "xinfo_stream",
        "axinfo_stream",
        "xlen",
        "axlen",
        "xpending",
        "axpending",
        "xrange",
        "axrange",
        "xread",
        "axread",
        "xrevrange",
        "axrevrange",
        # The lock writes through its own client, a documented gap.
        "lock",
        "alock",
        "tracking_connection",
        "tracking_primary",
    },
)

# Adapter methods whose writes no key list describes. They flush every tracker.
FLUSH_METHODS: Final = frozenset({"clear", "aclear", "delete_pattern", "adelete_pattern"})

# Pipeline adapter methods that queue only read commands.
PIPELINE_READ_METHODS: Final = frozenset(
    {
        "exists",
        "expiretime",
        "get",
        "hexists",
        "hexpiretime",
        "hget",
        "hgetall",
        "hkeys",
        "hlen",
        "hmget",
        "hpttl",
        "httl",
        "hvals",
        "lindex",
        "llen",
        "lpos",
        "lrange",
        "memory_usage",
        "pttl",
        "scard",
        "sdiff",
        "sinter",
        "sismember",
        "smembers",
        "smismember",
        "srandmember",
        "sunion",
        "ttl",
        "type",
        "xinfo_consumers",
        "xinfo_groups",
        "xinfo_stream",
        "xlen",
        "xpending",
        "xpending_range",
        "xrange",
        "xread",
        "xrevrange",
        "zcard",
        "zcount",
        "zmscore",
        "zrange",
        "zrangebyscore",
        "zrank",
        "zrevrange",
        "zrevrangebyscore",
        "zrevrank",
        "zscore",
    },
)

# Commands a pipeline's execute_command() can queue without dropping anything.
READ_COMMANDS: Final = frozenset(
    {
        "EXISTS",
        "EXPIRETIME",
        "GET",
        "GETRANGE",
        "HEXISTS",
        "HGET",
        "HGETALL",
        "HKEYS",
        "HLEN",
        "HMGET",
        "HVALS",
        "LINDEX",
        "LLEN",
        "LRANGE",
        "MGET",
        "PEXPIRETIME",
        "PING",
        "PTTL",
        "SCARD",
        "SISMEMBER",
        "SMEMBERS",
        "SMISMEMBER",
        "STRLEN",
        "TTL",
        "TYPE",
        "XLEN",
        "XRANGE",
        "XREVRANGE",
        "ZCARD",
        "ZCOUNT",
        "ZMSCORE",
        "ZRANGE",
        "ZRANK",
        "ZREVRANK",
        "ZSCORE",
    },
)

# Commands that change keys no argument names.
FLUSH_COMMANDS: Final = frozenset({"FLUSHALL", "FLUSHDB", "SWAPDB"})

_CONTAINERS: Final = (list, tuple, set, frozenset)


def collect_strings(args: tuple[Any, ...], kwargs: dict[str, Any]) -> list[str | bytes] | None:
    """The str and bytes in the arguments, also inside lists, tuples, sets and dicts; None if an iterator can hide keys."""
    found: list[str | bytes] = []
    stack: list[Any] = [*args, *kwargs.values()]
    while stack:
        value = stack.pop()
        if isinstance(value, str | bytes):
            found.append(value)
        elif isinstance(value, bytearray | memoryview):
            found.append(bytes(value))
        elif isinstance(value, dict):
            stack.extend(value.keys())
            stack.extend(value.values())
        elif isinstance(value, _CONTAINERS):
            stack.extend(value)
        elif isinstance(value, Iterable):
            # A generator the call consumed, or a view: what it held cannot be read back.
            return None
    return found


def drop(args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
    """Drop the copies of every key among the arguments from every tracker of this process."""
    trackers = _tracker.trackers()
    if not trackers:
        return
    strings = collect_strings(args, kwargs)
    for tracker in trackers:
        if strings is None:
            tracker.flush()
        elif strings:
            tracker.discard(strings)


def flush_all() -> None:
    for tracker in _tracker.trackers():
        tracker.flush()


def _dropping(method: Callable[..., Any]) -> Callable[..., Any]:
    if inspect.iscoroutinefunction(method):

        @functools.wraps(method)
        async def adropping(*args: Any, **kwargs: Any) -> Any:
            try:
                return await method(*args, **kwargs)
            finally:
                drop(args, kwargs)

        return adropping

    @functools.wraps(method)
    def dropping(*args: Any, **kwargs: Any) -> Any:
        try:
            return method(*args, **kwargs)
        finally:
            drop(args, kwargs)

    return dropping


def _flushing(method: Callable[..., Any]) -> Callable[..., Any]:
    if inspect.iscoroutinefunction(method):

        @functools.wraps(method)
        async def aflushing(*args: Any, **kwargs: Any) -> Any:
            try:
                return await method(*args, **kwargs)
            finally:
                flush_all()

        return aflushing

    @functools.wraps(method)
    def flushing(*args: Any, **kwargs: Any) -> Any:
        try:
            return method(*args, **kwargs)
        finally:
            flush_all()

    return flushing


class TrackedAdapter:
    """Wrap a driver adapter: tracked reads use the local copies, and writes drop the copies of their keys."""

    def __init__(self, inner: Any, config: TrackingConfig, registry_key: Hashable) -> None:
        self._inner = inner
        self._config = config
        self._registry_key = registry_key

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if name.startswith("_") or not callable(attr):
            return attr
        if name in READ_METHODS:
            wrapped = attr
        elif name in FLUSH_METHODS:
            wrapped = _flushing(attr)
        else:
            wrapped = _dropping(attr)
        self.__dict__[name] = wrapped
        return wrapped

    def _get_tracker(self) -> _tracker.Tracker | None:
        """The tracker of this configuration; None when its thread cannot start, and reads go to the server."""
        return _tracker.get_tracker(self._registry_key, self._inner, self._config)

    def get(self, key: Any, *, stampede_prevention: Any = None) -> Any:
        tracker = self._get_tracker() if self._config.covers(key) else None
        if tracker is None:
            return self._inner.get(key, stampede_prevention=stampede_prevention)
        return tracker.get(self._inner, key, self._inner.resolve_stampede(stampede_prevention))

    def get_many(self, keys: Any, *, stampede_prevention: Any = None) -> dict[Any, Any]:
        keys = list(keys)
        covered = [key for key in keys if self._config.covers(key)]
        tracker = self._get_tracker() if covered else None
        if tracker is None:
            return self._inner.get_many(keys, stampede_prevention=stampede_prevention)
        if len(covered) < len(keys):
            others = [key for key in keys if not self._config.covers(key)]
            found = self._inner.get_many(others, stampede_prevention=stampede_prevention)
        else:
            found = {}
        stampede = self._inner.resolve_stampede(stampede_prevention)
        found.update(tracker.get_many(self._inner, covered, stampede))
        return found

    def has_key(self, key: Any, *, stampede_prevention: Any = None) -> bool:
        tracker = self._get_tracker() if self._config.covers(key) else None
        if tracker is None:
            return self._inner.has_key(key, stampede_prevention=stampede_prevention)
        return tracker.has_key(self._inner, key, self._inner.resolve_stampede(stampede_prevention))

    def read_many(self, keys: Sequence[Any]) -> list[Any]:
        """Raw values of ``keys`` in order, from the copies where they are current; no stampede rule."""
        tracker = self._get_tracker() if any(self._config.covers(key) for key in keys) else None
        if tracker is not None:
            return tracker.read_many(self._inner, keys)
        # No key can have a copy, or the tracker cannot start: the read goes to the server.
        if not keys:
            return []
        pipe = self._inner.pipeline(transaction=False)
        pipe.execute_command("MGET", *keys)
        return pipe.execute()[0]

    def tracking_info(self) -> dict[str, Any]:
        """The ``tracking`` section of ``info()``; creates no tracker."""
        tracker = _tracker.find_tracker(self._registry_key)
        if tracker is not None:
            return tracker.info()
        return {
            "connected": False,
            "fresh": False,
            "prefix": self._config.prefix,
            "max_staleness": self._config.max_staleness,
            "entries": 0,
            "bytes": 0,
            "max_bytes": self._config.max_bytes,
            "hits": 0,
            "misses": 0,
            "invalidations": 0,
            "flushes": 0,
            "reconnects": 0,
            "last_confirmed_age": None,
        }

    def pipeline(self, *args: Any, **kwargs: Any) -> RecordingPipeline:
        return RecordingPipeline(self._inner.pipeline(*args, **kwargs))

    async def apipeline(self, *args: Any, **kwargs: Any) -> AsyncRecordingPipeline:
        return AsyncRecordingPipeline(await self._inner.apipeline(*args, **kwargs))


class RecordingPipeline:
    """Wrap a pipeline adapter: ``execute()`` drops the copies of every key the queued writes named."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._strings: list[str | bytes] = []
        self._flush = False

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if name.startswith("_") or not callable(attr) or name in PIPELINE_READ_METHODS:
            return attr

        @functools.wraps(attr)
        def recording(*args: Any, **kwargs: Any) -> Any:
            self._record(collect_strings(args, kwargs))
            return attr(*args, **kwargs)

        self.__dict__[name] = recording
        return recording

    def execute_command(self, *args: Any, **options: Any) -> Any:
        if args:
            name = args[0].decode() if isinstance(args[0], bytes) else str(args[0])
            name = name.split(" ", 1)[0].upper()
            if name in FLUSH_COMMANDS:
                self._flush = True
            elif name not in READ_COMMANDS:
                self._record(collect_strings(args[1:], options))
        return self._inner.execute_command(*args, **options)

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return self._inner.execute(*args, **kwargs)
        finally:
            self._apply()

    def reset(self) -> None:
        self._inner.reset()
        self._strings.clear()
        self._flush = False

    def _record(self, strings: list[str | bytes] | None) -> None:
        if strings is None:
            self._flush = True
        else:
            self._strings += strings

    def _apply(self) -> None:
        strings, flush = self._strings, self._flush
        self._strings, self._flush = [], False
        if flush:
            flush_all()
        elif strings:
            drop(tuple(strings), {})


class AsyncRecordingPipeline(RecordingPipeline):
    """The async twin of :class:`RecordingPipeline`."""

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return await self._inner.execute(*args, **kwargs)
        finally:
            self._apply()

    async def reset(self) -> None:  # type: ignore[override]
        await self._inner.reset()
        self._strings.clear()
        self._flush = False
