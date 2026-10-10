"""What the current transaction of a connection wrote."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from django.db.backends.base.base import BaseDatabaseWrapper

_LAYERS = "_cachex_orm_layers"


class _Layer:
    """The tables a transaction wrote since a savepoint, or since it began for the first layer."""

    __slots__ = ("sid", "written")

    def __init__(self, sid: str | None) -> None:
        self.sid = sid
        self.written: set[str] = set()


def _layers(connection: BaseDatabaseWrapper) -> list[_Layer]:
    layers = getattr(connection, _LAYERS, None)
    if layers is None:
        layers = []
        setattr(connection, _LAYERS, layers)
    return layers


def _top_layer(layers: list[_Layer]) -> _Layer:
    if not layers:
        layers.append(_Layer(None))
    return layers[-1]


def _find(layers: list[_Layer], sid: str) -> int | None:
    for index, layer in enumerate(layers):
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
    return set().union(*(layer.written for layer in getattr(connection, _LAYERS, ())))


def mark_written(connection: BaseDatabaseWrapper, tables: Iterable[str]) -> None:
    """Record a write to ``tables``."""
    _top_layer(_layers(connection)).written.update(tables)


def reset(connection: BaseDatabaseWrapper) -> None:
    """Forget the transaction: it was committed or rolled back, or the connection closed."""
    layers = getattr(connection, _LAYERS, None)
    if layers is not None:
        layers.clear()


def savepoint_created(connection: BaseDatabaseWrapper, sid: str) -> None:
    layers = _layers(connection)
    _top_layer(layers)
    layers.append(_Layer(sid))


def savepoint_rolled_back(connection: BaseDatabaseWrapper, sid: str) -> None:
    # The savepoint survives a rollback to it, with nothing done since.
    layers = _layers(connection)
    index = _find(layers, sid)
    if index is not None:
        del layers[index:]
        layers.append(_Layer(sid))


def savepoint_released(connection: BaseDatabaseWrapper, sid: str) -> None:
    layers = _layers(connection)
    index = _find(layers, sid)
    if not index:
        return
    parent = layers[index - 1]
    for layer in layers[index:]:
        parent.written.update(layer.written)
    del layers[index:]
