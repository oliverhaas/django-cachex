"""Stores for cached query results, table generations and write leases.

"How invalidation works" in docs/user-guide/orm-cache.md describes the protocol.
"""

import itertools
import logging
import os
import pickle
import time
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any, Protocol

from django.core.cache import caches
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.core.cache.backends.locmem import LocMemCache

from django_cachex.cache.resp import RespCache
from django_cachex.cache.tracking import TrackingCache
from django_cachex.script import keys_only_pre

if TYPE_CHECKING:
    from collections.abc import Sequence

    from django.core.cache.backends.base import BaseCache


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Lookup:
    """A hit carries the result; a miss carries the generations to store the result against."""

    hit: bool = False
    value: Any = None
    # None when a write holds a lease on one of the tables: the result must not be stored.
    token: Any = None


BYPASS = Lookup()


class Store(Protocol):
    def ttl(self, timeout: Any) -> float | None:
        """Seconds a result stored with ``timeout`` lives, None for ever; nothing is stored at 0 or less."""
        ...

    def lookup(self, db_alias: str, query_key: str, table_keys: Sequence[str]) -> Lookup: ...

    def store(
        self,
        db_alias: str,
        query_key: str,
        table_keys: Sequence[str],
        token: Any,
        result: Any,
        timeout: Any,
    ) -> bool: ...

    def begin_write(self, db_alias: str, table_keys: Sequence[str], token: str, lease_timeout: float) -> None: ...

    def end_write(self, db_alias: str, table_keys: Sequence[str], token: str) -> None: ...

    def bump(self, db_alias: str, table_keys: Sequence[str]) -> None: ...

    def generations(self, db_alias: str, table_keys: Sequence[str]) -> list[str] | None: ...


def get_store(cache_alias: str) -> Store | None:
    """Return the store over cache ``cache_alias``, or None if its backend cannot hold one."""
    cache = caches[cache_alias]
    if isinstance(cache, TrackingCache):
        # This process's local results for the cache, bounded by its MAX_ENTRIES.
        with _LOCAL_RESULTS_LOCK:
            local = _LOCAL_RESULTS.get(cache._storage_key)
            if local is None or local.pid != os.getpid():
                local = _LOCAL_RESULTS[cache._storage_key] = _LocalResults(cache._max_entries)
        return RespStore(cache._transport, local)
    if isinstance(cache, RespCache):
        return RespStore(cache)
    if isinstance(cache, LocMemCache):
        return LocMemStore(cache)
    return None


def _ttl(cache: BaseCache, timeout: Any) -> float | None:
    return cache.default_timeout if timeout is DEFAULT_TIMEOUT else timeout


def _entry_key(db_alias: str, query_key: str) -> str:
    # The database alias is the hash tag, so the keys a script touches share a
    # cluster slot.
    return f"orm:{{{db_alias}}}:q:{query_key}"


def _generation_key(db_alias: str, table_key: str) -> str:
    return f"orm:{{{db_alias}}}:g:{table_key}"


def _lease_key(db_alias: str, table_key: str) -> str:
    return f"orm:{{{db_alias}}}:l:{table_key}"


# A new generation is the server time in microseconds times 1000, then only incremented: an
# evicted one comes back higher than it was (barring 1000 bumps a microsecond), so old results miss.
_LUA_PRELUDE = """
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local function fresh_generation()
  local t = redis.call('TIME')
  return t[1] .. string.format('%06d', tonumber(t[2])) .. '000'
end
local function leased(key, now)
  return redis.call('ZCOUNT', key, '(' .. now, '+inf') > 0
end
local function any_leased(first, n, now)
  for i = first, first + n - 1 do
    if leased(KEYS[i], now) then return true end
  end
  return false
end
local function current_generations(first, n)
  local generations = {}
  for i = first, first + n - 1 do
    local generation = redis.call('GET', KEYS[i])
    if not generation then
      generation = fresh_generation()
      redis.call('SET', KEYS[i], generation)
    end
    generations[#generations + 1] = generation
  end
  return generations
end
local function bump_existing(first, n)
  for i = first, first + n - 1 do
    if redis.call('EXISTS', KEYS[i]) == 1 then redis.call('INCR', KEYS[i]) end
  end
end
"""

# KEYS: entry, generations, leases. ARGV: table count, the local copy's generations or "".
# Returns {0} while leased, {1, payload, generations} on a hit, {2, generations} on a miss, {3} on a local hit.
_LOOKUP = (
    _LUA_PRELUDE
    + """
local n = tonumber(ARGV[1])
if any_leased(n + 2, n, now_ms()) then return {0} end
local generations = table.concat(current_generations(2, n), ':')
if redis.call('HGET', KEYS[1], 'g') ~= generations then return {2, generations} end
if ARGV[2] == generations then return {3} end
return {1, redis.call('HGET', KEYS[1], 'v'), generations}
"""
)

# KEYS: entry, generations, leases. ARGV: table count, generations the result
# was read under, payload, time to live in milliseconds (0 for ever).
_STORE = (
    _LUA_PRELUDE
    + """
local n = tonumber(ARGV[1])
if any_leased(n + 2, n, now_ms()) then return 0 end
local generations = redis.call('MGET', unpack(KEYS, 2, n + 1))
for i = 1, n do
  if not generations[i] then return 0 end
end
if table.concat(generations, ':') ~= ARGV[2] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1], 'g', ARGV[2], 'v', ARGV[3])
local ttl = tonumber(ARGV[4])
if ttl > 0 then redis.call('PEXPIRE', KEYS[1], ttl) end
return 1
"""
)

# KEYS: generations, leases. ARGV: table count, lease token, lease milliseconds. Lease keys
# never expire, which keeps volatile-* eviction off them; each lease's score is its expiry.
_BEGIN_WRITE = (
    _LUA_PRELUDE
    + """
local n = tonumber(ARGV[1])
local lease_ms = tonumber(ARGV[3])
local now = now_ms()
for i = n + 1, 2 * n do
  redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', now)
  redis.call('ZADD', KEYS[i], now + lease_ms, ARGV[2])
end
bump_existing(1, n)
return 1
"""
)

# KEYS: generations, leases. ARGV: table count, lease token.
_END_WRITE = (
    _LUA_PRELUDE
    + """
local n = tonumber(ARGV[1])
for i = n + 1, 2 * n do
  redis.call('ZREM', KEYS[i], ARGV[2])
end
bump_existing(1, n)
return 1
"""
)

# KEYS: generations.
_BUMP = (
    _LUA_PRELUDE
    + """
bump_existing(1, #KEYS)
return 1
"""
)

# KEYS: generations, leases. ARGV: table count.
_GENERATIONS = (
    _LUA_PRELUDE
    + """
local n = tonumber(ARGV[1])
if any_leased(n + 1, n, now_ms()) then return false end
return current_generations(1, n)
"""
)


class _LocalResults:
    """Encoded results kept in process, next to the generations they were stored under."""

    def __init__(self, max_entries: int) -> None:
        self.pid = os.getpid()
        self.max_entries = max_entries
        self.lock = Lock()
        self.entries: OrderedDict[str, tuple[bytes, Any]] = OrderedDict()

    def get(self, key: str) -> tuple[bytes, Any] | None:
        with self.lock:
            entry = self.entries.get(key)
            if entry is not None:
                self.entries.move_to_end(key)
            return entry

    def put(self, key: str, generations: bytes, payload: Any) -> None:
        with self.lock:
            self.entries[key] = (generations, payload)
            self.entries.move_to_end(key)
            while len(self.entries) > self.max_entries:
                self.entries.popitem(last=False)


_LOCAL_RESULTS: dict[str, _LocalResults] = {}
_LOCAL_RESULTS_LOCK = Lock()


class RespStore:
    """Results, generations and leases in Redis or Valkey, each operation one Lua script.

    With ``local``, results are also kept in process and served from there while the server holds them under
    the same generations: a hit costs a round trip but no transfer, and still expires with the server's copy.
    """

    def __init__(self, cache: RespCache, local: _LocalResults | None = None) -> None:
        self.cache = cache
        self.local = local

    def ttl(self, timeout: Any) -> float | None:
        return _ttl(self.cache, timeout)

    def _eval(self, script: str, keys: list[str], args: list[Any]) -> Any:
        return self.cache.eval_script(script, keys=keys, args=args, pre_hook=keys_only_pre)

    @staticmethod
    def _table_keys(db_alias: str, table_keys: Sequence[str]) -> list[str]:
        return [_generation_key(db_alias, k) for k in table_keys] + [_lease_key(db_alias, k) for k in table_keys]

    def lookup(self, db_alias: str, query_key: str, table_keys: Sequence[str]) -> Lookup:
        entry_key = _entry_key(db_alias, query_key)
        local = self.local.get(entry_key) if self.local is not None else None
        reply = self._eval(
            _LOOKUP,
            [entry_key, *self._table_keys(db_alias, table_keys)],
            [str(len(table_keys)), local[0] if local is not None else b""],
        )
        status = int(reply[0])
        if status == 0:
            return BYPASS
        if status == 3 and local is not None:
            # Only sent back when the local generations matched.
            generations, payload = local
        elif status == 1:
            payload, generations = reply[1], reply[2]
        else:
            return Lookup(token=reply[1])
        try:
            value = self.cache.decode(payload)
        except Exception:
            # Written by another serializer, say during a deploy: run the query
            # and store its result over this one.
            logger.warning("Ignoring an ORM cache entry that does not decode.", exc_info=True)
            return Lookup(token=generations)
        if status == 1 and self.local is not None:
            self.local.put(entry_key, generations, payload)
        return Lookup(hit=True, value=value)

    def store(
        self,
        db_alias: str,
        query_key: str,
        table_keys: Sequence[str],
        token: Any,
        result: Any,
        timeout: Any,
    ) -> bool:
        ttl = self.ttl(timeout)
        if ttl is not None and ttl <= 0:
            return False
        payload = self.cache.encode(result)
        if isinstance(payload, int):
            payload = str(payload).encode()
        entry_key = _entry_key(db_alias, query_key)
        stored = self._eval(
            _STORE,
            [entry_key, *self._table_keys(db_alias, table_keys)],
            [str(len(table_keys)), token, payload, "0" if ttl is None else str(max(1, int(ttl * 1000)))],
        )
        if stored and self.local is not None:
            self.local.put(entry_key, token, payload)
        return bool(stored)

    def begin_write(self, db_alias: str, table_keys: Sequence[str], token: str, lease_timeout: float) -> None:
        self._eval(
            _BEGIN_WRITE,
            self._table_keys(db_alias, table_keys),
            [str(len(table_keys)), token, str(max(1, int(lease_timeout * 1000)))],
        )

    def end_write(self, db_alias: str, table_keys: Sequence[str], token: str) -> None:
        self._eval(_END_WRITE, self._table_keys(db_alias, table_keys), [str(len(table_keys)), token])

    def bump(self, db_alias: str, table_keys: Sequence[str]) -> None:
        self._eval(_BUMP, [_generation_key(db_alias, k) for k in table_keys], [])

    def generations(self, db_alias: str, table_keys: Sequence[str]) -> list[str] | None:
        reply = self._eval(_GENERATIONS, self._table_keys(db_alias, table_keys), [str(len(table_keys))])
        return None if reply is None else [g.decode() if isinstance(g, bytes) else str(g) for g in reply]


# Generations in a local memory cache are (epoch, count) pairs. A new generation takes a new
# epoch, so it never matches a result stored before the process last saw the table.
_EPOCHS = itertools.count(1)


class _LocMemState:
    """Generations and leases of one local memory cache, guarded by the cache's own lock."""

    def __init__(self) -> None:
        self.generations: dict[tuple[str, str], tuple[int, int]] = {}
        # Table -> lease token -> time.monotonic() the lease expires at.
        self.leases: dict[tuple[str, str], dict[str, float]] = {}

    def any_leased(self, keys: Sequence[tuple[str, str]], now: float) -> bool:
        for key in keys:
            leases = self.leases.get(key)
            if leases is None:
                continue
            for token in [t for t, expires_at in leases.items() if expires_at <= now]:
                del leases[token]
            if leases:
                return True
            del self.leases[key]
        return False

    def current(self, keys: Sequence[tuple[str, str]]) -> tuple[tuple[int, int], ...]:
        generations = self.generations
        for key in keys:
            if key not in generations:
                generations[key] = (next(_EPOCHS), 0)
        return tuple(generations[key] for key in keys)

    def bump(self, keys: Sequence[tuple[str, str]]) -> None:
        generations = self.generations
        for key in keys:
            generation = generations.get(key)
            if generation is not None:
                generations[key] = (generation[0], generation[1] + 1)


# Keyed by the id of the cache's shared OrderedDict, which Django keeps alive
# for the life of the process.
_LOCMEM_STATES: dict[int, _LocMemState] = {}
_LOCMEM_STATES_LOCK = Lock()


class LocMemStore:
    """Results in a local memory cache; generations and leases next to it, never culled."""

    def __init__(self, cache: LocMemCache) -> None:
        self.cache = cache
        internals: Any = cache
        with _LOCMEM_STATES_LOCK:
            self.state = _LOCMEM_STATES.setdefault(id(internals._cache), _LocMemState())

    def ttl(self, timeout: Any) -> float | None:
        return _ttl(self.cache, timeout)

    @staticmethod
    def _keys(db_alias: str, table_keys: Sequence[str]) -> list[tuple[str, str]]:
        return [(db_alias, k) for k in table_keys]

    def _entry_key(self, db_alias: str, query_key: str) -> str:
        return self.cache.make_and_validate_key(_entry_key(db_alias, query_key))

    def lookup(self, db_alias: str, query_key: str, table_keys: Sequence[str]) -> Lookup:
        cache: Any = self.cache
        entry_key = self._entry_key(db_alias, query_key)
        keys = self._keys(db_alias, table_keys)
        with cache._lock:
            if self.state.any_leased(keys, time.monotonic()):
                return BYPASS
            token = self.state.current(keys)
            raw = cache._cache.get(entry_key)
            if raw is not None:
                if cache._has_expired(entry_key):
                    cache._delete(entry_key)
                    raw = None
                else:
                    cache._cache.move_to_end(entry_key, last=False)
        if raw is not None:
            try:
                stored_under, result = pickle.loads(raw)  # noqa: S301
            except Exception:
                logger.warning("Ignoring an ORM cache entry that does not unpickle.", exc_info=True)
            else:
                if stored_under == token:
                    return Lookup(hit=True, value=result)
        return Lookup(token=token)

    def store(
        self,
        db_alias: str,
        query_key: str,
        table_keys: Sequence[str],
        token: Any,
        result: Any,
        timeout: Any,
    ) -> bool:
        ttl = self.ttl(timeout)
        if ttl is not None and ttl <= 0:
            return False
        cache: Any = self.cache
        raw = pickle.dumps((token, result), pickle.HIGHEST_PROTOCOL)
        entry_key = self._entry_key(db_alias, query_key)
        keys = self._keys(db_alias, table_keys)
        with cache._lock:
            if self.state.any_leased(keys, time.monotonic()):
                return False
            if any(key not in self.state.generations for key in keys) or self.state.current(keys) != token:
                return False
            cache._set(entry_key, raw, ttl)
        return True

    def begin_write(self, db_alias: str, table_keys: Sequence[str], token: str, lease_timeout: float) -> None:
        cache: Any = self.cache
        keys = self._keys(db_alias, table_keys)
        with cache._lock:
            expires_at = time.monotonic() + lease_timeout
            for key in keys:
                self.state.leases.setdefault(key, {})[token] = expires_at
            self.state.bump(keys)

    def end_write(self, db_alias: str, table_keys: Sequence[str], token: str) -> None:
        cache: Any = self.cache
        keys = self._keys(db_alias, table_keys)
        with cache._lock:
            for key in keys:
                leases = self.state.leases.get(key)
                if leases is not None:
                    leases.pop(token, None)
                    if not leases:
                        del self.state.leases[key]
            self.state.bump(keys)

    def bump(self, db_alias: str, table_keys: Sequence[str]) -> None:
        cache: Any = self.cache
        with cache._lock:
            self.state.bump(self._keys(db_alias, table_keys))

    def generations(self, db_alias: str, table_keys: Sequence[str]) -> list[str] | None:
        cache: Any = self.cache
        keys = self._keys(db_alias, table_keys)
        with cache._lock:
            if self.state.any_leased(keys, time.monotonic()):
                return None
            return [f"{epoch}.{count}" for epoch, count in self.state.current(keys)]
