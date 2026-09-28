"""What the current transaction of a connection wrote and cached, and how it isolates its reads."""

import logging
import pickle
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from django.db.backends.base.base import BaseDatabaseWrapper

logger = logging.getLogger(__name__)

# Each statement reads what was committed when it started (PostgreSQL READ COMMITTED, SQLite
# with a rollback journal): tables the transaction wrote bypass the cache, the others share it.
SHARED = "shared"
# The transaction reads a snapshot from its first query (PostgreSQL REPEATABLE READ and
# SERIALIZABLE, SQLite in WAL mode), maybe older than the cache: results stay per transaction.
SNAPSHOT = "snapshot"

_STATE = "_cachex_orm_state"
# Bytes of pickled results a transaction caches for itself; past them its queries run against the database.
_CACHE_BUDGET = 16 * 1024 * 1024


class _Layer:
    """What a transaction did since a savepoint, or since it began for the first layer."""

    __slots__ = ("entries", "sid", "size", "written")

    def __init__(self, sid: str | None) -> None:
        self.sid = sid
        self.written: set[str] = set()
        # Query key -> (tables read, pickled result).
        self.entries: dict[str, tuple[frozenset[str], bytes]] = {}
        # Bytes of the pickled results in ``entries``.
        self.size = 0


class _State:
    __slots__ = ("isolation", "isolation_connection", "layers", "reread_isolation")

    def __init__(self) -> None:
        self.layers: list[_Layer] = []
        self.isolation: str | None = None
        # The DB-API connection ``isolation`` was read from.
        self.isolation_connection: Any = None
        # The session default changed during the transaction: read it again once the transaction ends.
        self.reread_isolation = False


def _state(connection: BaseDatabaseWrapper) -> _State:
    state = getattr(connection, _STATE, None)
    if state is None:
        state = _State()
        setattr(connection, _STATE, state)
    return state


def _top_layer(state: _State) -> _Layer:
    if not state.layers:
        state.layers.append(_Layer(None))
    return state.layers[-1]


def _find(state: _State, sid: str) -> int | None:
    for index, layer in enumerate(state.layers):
        if layer.sid == sid:
            return index
    return None


def in_transaction(connection: BaseDatabaseWrapper) -> bool:
    """Whether statements on ``connection`` run in a transaction rather than commit one by one."""
    if connection.in_atomic_block:
        return True
    if connection.connection is None:
        # Not connected yet: the first statement opens a transaction unless
        # the connection is set to autocommit.
        return not connection.settings_dict["AUTOCOMMIT"]
    return not connection.autocommit


def written(connection: BaseDatabaseWrapper) -> set[str]:
    """Tables the current transaction has written."""
    state = getattr(connection, _STATE, None)
    if state is None:
        return set()
    return set().union(*(layer.written for layer in state.layers))


def mark_written(connection: BaseDatabaseWrapper, tables: Iterable[str]) -> None:
    """Record a write to ``tables`` and drop the results the transaction cached from them."""
    tables = frozenset(tables)
    state = _state(connection)
    _top_layer(state).written.update(tables)
    for layer in state.layers:
        for query_key in [key for key, (read, _) in layer.entries.items() if not read.isdisjoint(tables)]:
            layer.size -= len(layer.entries.pop(query_key)[1])


def cached(connection: BaseDatabaseWrapper, query_key: str) -> tuple[bool, Any]:
    """Return ``(True, result)`` if the transaction cached a result for ``query_key``, else ``(False, None)``."""
    state = getattr(connection, _STATE, None)
    if state is not None:
        for layer in reversed(state.layers):
            entry = layer.entries.get(query_key)
            if entry is not None:
                return True, pickle.loads(entry[1])  # noqa: S301
    return False, None


def cache(connection: BaseDatabaseWrapper, query_key: str, tables: Iterable[str], result: Any) -> None:
    """Cache ``result`` for the rest of the transaction if it fits in the budget."""
    # Pickled so a caller that changes the result cannot change the cached copy.
    pickled = pickle.dumps(result, pickle.HIGHEST_PROTOCOL)
    state = _state(connection)
    if sum(layer.size for layer in state.layers) + len(pickled) > _CACHE_BUDGET:
        return
    layer = _top_layer(state)
    layer.entries[query_key] = (frozenset(tables), pickled)
    layer.size += len(pickled)


def reset(connection: BaseDatabaseWrapper) -> None:
    """Forget the transaction: it was committed or rolled back, or the connection closed."""
    state = getattr(connection, _STATE, None)
    if state is not None:
        state.layers.clear()
        if state.reread_isolation:
            state.isolation, state.reread_isolation = None, False


def savepoint_created(connection: BaseDatabaseWrapper, sid: str) -> None:
    state = _state(connection)
    _top_layer(state)
    state.layers.append(_Layer(sid))


def savepoint_rolled_back(connection: BaseDatabaseWrapper, sid: str) -> None:
    # The savepoint survives a rollback to it, with nothing done since.
    state = _state(connection)
    index = _find(state, sid)
    if index is not None:
        del state.layers[index:]
        state.layers.append(_Layer(sid))


def savepoint_released(connection: BaseDatabaseWrapper, sid: str) -> None:
    state = _state(connection)
    index = _find(state, sid)
    if not index:
        return
    parent = state.layers[index - 1]
    for layer in state.layers[index:]:
        parent.written.update(layer.written)
        parent.entries.update(layer.entries)
        parent.size += layer.size
    del state.layers[index:]


def isolation(connection: BaseDatabaseWrapper) -> str:
    """Return SHARED or SNAPSHOT for the transactions on ``connection``, read once per database connection."""
    state = _state(connection)
    if state.isolation is not None and state.isolation_connection is connection.connection:
        return state.isolation
    try:
        connection.ensure_connection()
        kind = _read_isolation(connection)
    except Exception:
        logger.warning(
            "Could not read the transaction isolation of database %r; caching results per transaction.",
            connection.alias,
            exc_info=True,
        )
        return SNAPSHOT
    state.isolation, state.isolation_connection = kind, connection.connection
    return kind


def _read_isolation(connection: BaseDatabaseWrapper) -> str:
    raw: Any = connection.connection
    if connection.vendor == "postgresql":
        if "isolation_level" in connection.settings_dict["OPTIONS"]:
            # Importable only with a PostgreSQL driver installed.
            from django.db.backends.postgresql.psycopg_any import IsolationLevel

            level = getattr(connection, "isolation_level", None)
            return SHARED if level in {IsolationLevel.READ_UNCOMMITTED, IsolationLevel.READ_COMMITTED} else SNAPSHOT
        # On the database connection itself, so it is not logged as a query.
        with raw.cursor() as cursor:
            cursor.execute("SHOW default_transaction_isolation")
            (name,) = cursor.fetchone()
        return SHARED if name in {"read uncommitted", "read committed"} else SNAPSHOT
    # SQLite, the only other vendor the ORM cache caches.
    (mode,) = raw.execute("PRAGMA journal_mode").fetchone()
    return SNAPSHOT if str(mode).lower() == "wal" else SHARED


def isolation_changed(connection: BaseDatabaseWrapper, *, known: bool) -> None:
    """Raw SQL changed the isolation: read it again if ``known``, else assume SNAPSHOT until reconnecting."""
    state = _state(connection)
    if not known:
        state.isolation, state.isolation_connection = SNAPSHOT, connection.connection
        state.reread_isolation = False
    elif in_transaction(connection):
        # A new session default applies from the next transaction, and SET LOCAL or a rollback undoes it.
        if state.isolation is None or state.isolation_connection is not connection.connection:
            state.isolation, state.isolation_connection = SNAPSHOT, connection.connection
        state.reread_isolation = True
    else:
        state.isolation = None
