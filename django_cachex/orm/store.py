"""Stores for cached query results and table generations.

"How invalidation works" in docs/user-guide/orm-cache.md describes the protocol.
"""

import itertools
import logging
import pickle
import secrets
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any, Protocol

from django.core.cache import caches
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.core.cache.backends.locmem import LocMemCache

from django_cachex.cache.resp import RespCache
from django_cachex.cache.tracking import TrackingCache

if TYPE_CHECKING:
    from collections.abc import Sequence

    from django.core.cache.backends.base import BaseCache


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Lookup:
    """A hit carries the result; a miss carries the generations to store the result under."""

    hit: bool = False
    value: Any = None
    # None when the generations cannot be read: the result must not be stored.
    token: Any = None


BYPASS = Lookup()


class Store(Protocol):
    def ttl(self, timeout: Any) -> float | None:
        """Seconds a result stored with ``timeout`` lives, None for ever; nothing is stored at 0 or less."""
        ...

    def lookup(self, db_alias: str, query_key: str, table_keys: Sequence[str]) -> Lookup: ...

    def store(self, db_alias: str, query_key: str, token: Any, result: Any, timeout: Any) -> bool: ...

    def bump(self, db_alias: str, table_keys: Sequence[str]) -> None: ...

    def generations(self, db_alias: str, table_keys: Sequence[str]) -> list[str] | None: ...


def get_store(cache_alias: str) -> Store | None:
    """Return the store over cache ``cache_alias``, or None if its backend cannot hold one."""
    cache = caches[cache_alias]
    if isinstance(cache, TrackingCache):
        return RespStore(cache._transport)
    if isinstance(cache, RespCache):
        return RespStore(cache)
    if isinstance(cache, LocMemCache):
        return LocMemStore(cache)
    return None


def _ttl(cache: BaseCache, timeout: Any) -> float | None:
    return cache.default_timeout if timeout is DEFAULT_TIMEOUT else timeout


def _entry_key(db_alias: str, query_key: str) -> str:
    # The database alias is the hash tag, so the keys one command reads share a
    # cluster slot.
    return f"orm:{{{db_alias}}}:q:{query_key}"


def _generation_key(db_alias: str, table_key: str) -> str:
    return f"orm:{{{db_alias}}}:g:{table_key}"


def _new_generation() -> str:
    # Random, so an evicted generation never comes back to a value results are still stored under. 62 bits
    # leave room in a signed 64-bit integer for the INCR of an older version running during an upgrade.
    return str(secrets.randbits(62))


class RespStore:
    """Results and generations in Redis or Valkey: a lookup is one MGET, a bump one MSET."""

    def __init__(self, cache: RespCache) -> None:
        self.cache = cache

    def ttl(self, timeout: Any) -> float | None:
        return _ttl(self.cache, timeout)

    def _run(self, *commands: tuple[Any, ...]) -> list[Any]:
        # A pipeline runs on the primary, since a replica can lag behind a bump. Only a redis-py or valkey-py
        # cluster set to read from replicas sends it to one.
        pipe = self.cache.adapter.pipeline(transaction=False)
        for command in commands:
            pipe.execute_command(*command)
        return pipe.execute()

    def _generation_keys(self, db_alias: str, table_keys: Sequence[str]) -> list[str]:
        return [self.cache.make_and_validate_key(_generation_key(db_alias, k)) for k in table_keys]

    def _token(self, generation_keys: list[str], generations: list[Any]) -> str | None:
        """The generations as one string, after creating the missing ones; None if they cannot be read."""
        if None in generations:
            # Another process can create a generation first, so all of them are read again.
            created = [
                ("SET", key, _new_generation(), "NX")
                for key, generation in zip(generation_keys, generations, strict=True)
                if generation is None
            ]
            generations = self._run(*created, ("MGET", *generation_keys))[-1]
            if None in generations:
                return None
        return b":".join(generations).decode()

    def lookup(self, db_alias: str, query_key: str, table_keys: Sequence[str]) -> Lookup:
        generation_keys = self._generation_keys(db_alias, table_keys)
        entry_key = self.cache.make_and_validate_key(_entry_key(db_alias, query_key))
        *generations, entry = self._run(("MGET", *generation_keys, entry_key))[0]
        token = self._token(generation_keys, generations)
        if token is None:
            return BYPASS
        if entry is not None:
            try:
                stored_under, value = self.cache.decode(entry)
            except Exception:
                # Written by another serializer, say during a deploy: run the query
                # and store its result over this one.
                logger.warning("Ignoring an ORM cache entry that does not decode.", exc_info=True)
            else:
                if stored_under == token:
                    return Lookup(hit=True, value=value)
        return Lookup(token=token)

    def store(self, db_alias: str, query_key: str, token: Any, result: Any, timeout: Any) -> bool:
        ttl = self.ttl(timeout)
        if ttl is not None and ttl <= 0:
            return False
        # Stored whatever the generations are now: one bumped since the lookup only leaves the result unserved.
        command: tuple[Any, ...] = (
            "SET",
            self.cache.make_and_validate_key(_entry_key(db_alias, query_key)),
            self.cache.encode((token, result)),
        )
        if ttl is not None:
            command += ("PX", max(1, int(ttl * 1000)))
        self._run(command)
        return True

    def bump(self, db_alias: str, table_keys: Sequence[str]) -> None:
        # MSET also creates a missing generation, which no stored result can match.
        pairs = [(key, _new_generation()) for key in self._generation_keys(db_alias, table_keys)]
        self._run(("MSET", *itertools.chain.from_iterable(pairs)))

    def generations(self, db_alias: str, table_keys: Sequence[str]) -> list[str] | None:
        generation_keys = self._generation_keys(db_alias, table_keys)
        token = self._token(generation_keys, self._run(("MGET", *generation_keys))[0])
        return None if token is None else token.split(":")


# Generations in a local memory cache are (epoch, count) pairs. A new generation takes a new
# epoch, so it never matches a result stored before the process last saw the table.
_EPOCHS = itertools.count(1)


class _LocMemState:
    """Generations of one local memory cache, guarded by the cache's own lock."""

    def __init__(self) -> None:
        self.generations: dict[tuple[str, str], tuple[int, int]] = {}

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
    """Results in a local memory cache; generations next to it, never culled."""

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

    def store(self, db_alias: str, query_key: str, token: Any, result: Any, timeout: Any) -> bool:
        ttl = self.ttl(timeout)
        if ttl is not None and ttl <= 0:
            return False
        cache: Any = self.cache
        raw = pickle.dumps((token, result), pickle.HIGHEST_PROTOCOL)
        entry_key = self._entry_key(db_alias, query_key)
        with cache._lock:
            cache._set(entry_key, raw, ttl)
        return True

    def bump(self, db_alias: str, table_keys: Sequence[str]) -> None:
        cache: Any = self.cache
        with cache._lock:
            self.state.bump(self._keys(db_alias, table_keys))

    def generations(self, db_alias: str, table_keys: Sequence[str]) -> list[str]:
        cache: Any = self.cache
        with cache._lock:
            return [f"{epoch}.{count}" for epoch, count in self.state.current(self._keys(db_alias, table_keys))]
