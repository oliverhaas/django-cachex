"""Type protocols for the adapter layer.

These ``typing.Protocol`` classes formalize the contracts that
``KeyValueCache`` and ``Pipeline`` rely on. Concrete adapters don't need
to inherit from them — structural typing checks whether the concrete
class has compatible method signatures.

Scope is intentionally focused on the *core* surface: get/set/delete,
batch ops, TTL, scan, eval, pipeline, lock, sync + async pairs. Less
common ops (full hash/set/zset/list/stream surfaces) aren't enumerated
here; concrete adapters expose them and callers rely on duck typing.

The ``BaseKeyValueAdapter`` ABC in ``adapter/default.py`` provides the
default implementation that satisfies this Protocol.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from django_cachex.types import KeyT


@runtime_checkable
class KeyValuePipelineProtocol(Protocol):
    """Pipeline-like object: collects ops and runs them as one wire round-trip."""

    def execute(self) -> list[Any]: ...
    def reset(self) -> None: ...

    # Adapters' raw pipelines support the redis-py-shaped op surface
    # (set/get/delete/mget/mset/incrby/expire/ttl/...). Not enumerated here;
    # ``Pipeline`` (django_cachex.adapter.pipeline) calls them dynamically.


@runtime_checkable
class KeyValueAdapterProtocol(Protocol):
    """Adapter contract: what ``KeyValueCache`` calls on its underlying adapter.

    Only the most-used core surface is declared. Concrete adapters expose
    the full ~200-method cachex surface (hash/set/zset/list/stream + async
    twins); type-checkers can verify those structurally on a per-call basis.
    """

    # ---- core sync ----
    def get(self, key: KeyT, *, stampede_prevention: bool | dict | None = None) -> Any: ...
    def set(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> None: ...
    def add(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool: ...
    def delete(self, key: KeyT) -> bool: ...
    def get_many(
        self,
        keys: Iterable[KeyT],
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> dict[KeyT, Any]: ...
    def set_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list[Any]: ...
    def delete_many(self, keys: Sequence[KeyT]) -> int: ...
    def has_key(self, key: KeyT) -> bool: ...
    def incr(self, key: KeyT, delta: int = 1) -> int: ...
    def clear(self) -> bool: ...
    def close(self, **kwargs: Any) -> None: ...

    # ---- core async ----
    async def aget(
        self,
        key: KeyT,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> Any: ...
    async def aset(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> None: ...
    async def aadd(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> bool: ...
    async def adelete(self, key: KeyT) -> bool: ...
    async def aget_many(
        self,
        keys: Iterable[KeyT],
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> dict[KeyT, Any]: ...
    async def aset_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: int | None,
        *,
        stampede_prevention: bool | dict | None = None,
    ) -> list[Any]: ...
    async def adelete_many(self, keys: Sequence[KeyT]) -> int: ...
    async def aincr(self, key: KeyT, delta: int = 1) -> int: ...
    async def aclear(self) -> bool: ...

    # ---- pipeline + lock ----
    def pipeline(self, *, transaction: bool = True, version: int | None = None) -> Any: ...
    def lock(self, key: str, timeout: float | None = None, **kwargs: Any) -> Any: ...
    def alock(self, key: str, timeout: float | None = None, **kwargs: Any) -> Any: ...

    # ---- escape hatch ----
    def get_raw_client(self, key: KeyT | None = None, *, write: bool = False) -> Any:
        """Return the underlying lib's connection object.

        Off-contract — what gets returned differs per adapter (redis.Redis,
        our Rust driver, glide GlideClient). Use only for read-only
        inspection or features django-cachex doesn't expose.
        """
        ...


@runtime_checkable
class CachexProtocol(Protocol):
    """The cachex extension surface, structurally typed.

    Both ``KeyValueCache`` (Redis-shaped, native via adapter) and
    ``LocMemCache`` (in-process, native against a dict) satisfy this
    Protocol. The ``CachexCompat`` mixin can also satisfy it via emulated
    impls — restricted to admin views since compound ops are racy
    without backend-specific atomicity primitives.

    Method names mirror the Redis surface (``hget``, ``zadd``, etc.) and
    each method has an ``aXXX`` async twin omitted from this declaration
    for compactness. See concrete classes for the full surface.
    """

    # User-callable BaseCache surface that cachex extends
    def get(self, key: KeyT, default: Any = None, version: int | None = None) -> Any: ...
    def set(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = ...,
        version: int | None = None,
    ) -> None: ...
    def delete(self, key: KeyT, version: int | None = None) -> bool: ...

    # Cachex extensions — the Redis-shaped surface
    def hget(self, key: KeyT, field: str, version: int | None = None) -> Any: ...
    def hset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        mapping: Mapping[str, Any] | None = None,
        version: int | None = None,
    ) -> int: ...
    def zadd(
        self,
        key: KeyT,
        mapping: Mapping[Any, float],
        version: int | None = None,
        **kwargs: Any,
    ) -> int: ...
    def lpush(self, key: KeyT, *values: Any, version: int | None = None) -> int: ...
    def sadd(self, key: KeyT, *members: Any, version: int | None = None) -> int: ...

    def lock(self, key: str, timeout: float | None = None, **kwargs: Any) -> Any: ...
    def pipeline(self, *, transaction: bool = True, version: int | None = None) -> Any: ...


__all__ = [
    "CachexProtocol",
    "KeyValueAdapterProtocol",
    "KeyValuePipelineProtocol",
]
