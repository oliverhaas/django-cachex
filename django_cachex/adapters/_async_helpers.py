"""Awaitable-wrapping helpers for the Rust ``RedisRsAdapter``.

The Rust ``#[pyclass]`` adapter has folded almost all of its post-await
transforms into the ``AwaitTransform`` enum (see ``async_bridge.rs``).
What remains here are the coroutines/generators that need genuine
multi-step Python control flow — async generators, error-translation
re-raises, and the empty short-circuit that needs a ready-resolved
coroutine.

Keep this module ``Any``-typed and lean.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable


async def constant(value: Any) -> Any:
    """Return a literal value as a coroutine result.

    Used for empty-args short-circuits where the adapter returns ``0`` /
    ``[]`` synchronously but the caller does ``await ...``.
    """
    return value


# =========================================================================
# Rename error-translation coroutines
#
# Redis surfaces ``ERR no such key`` as a generic error; cachex's contract
# is to raise ``ValueError(f"Key {src!r} not found")`` so callers can
# disambiguate "missing" from "transport failure."
# =========================================================================


async def rename_after(awaitable: Awaitable[Any], src: Any) -> bool:
    try:
        await awaitable
    except RuntimeError as e:
        if "no such key" in str(e).lower():
            raise ValueError(f"Key {src!r} not found") from None
        raise
    return True


async def renamenx_after(awaitable: Awaitable[Any], src: Any) -> bool:
    try:
        result = await awaitable
    except RuntimeError as e:
        if "no such key" in str(e).lower():
            raise ValueError(f"Key {src!r} not found") from None
        raise
    return bool(result)


# =========================================================================
# Iterator generators
#
# The protocol exposes async generators (``aiter_keys``, ``asscan_iter``,
# ``adelete_pattern``) that the Rust adapter can't express directly —
# PyO3 doesn't have a clean async-generator facility. These helpers do
# the iteration on the Python side; Rust constructs the generator and
# returns it. The ``adapter`` argument is the Rust ``RedisRsAdapter``;
# the methods called below match the adapter's public surface
# (``scan``, ``ascan``, ``sscan``, ``asscan``, ``adelete_many``).
# =========================================================================


def sscan_iter_loop(
    adapter: Any,
    key: str,
    match: str | None,
    count: int | None,
) -> Any:
    """Drive the SSCAN cursor loop synchronously, yielding each member."""
    cursor = 0
    while True:
        cursor, batch = adapter.sscan(key, cursor, match=match, count=count)
        yield from batch
        if cursor == 0:
            return


async def asscan_iter_loop(
    adapter: Any,
    key: str,
    match: str | None,
    count: int | None,
) -> Any:
    """Drive the SSCAN cursor loop asynchronously, yielding each member."""
    cursor = 0
    while True:
        cursor, batch = await adapter.asscan(key, cursor, match=match, count=count)
        for item in batch:
            yield item
        if cursor == 0:
            return


def scan_iter_loop(adapter: Any, pattern: str, count: int) -> Any:
    """Drive the SCAN cursor loop synchronously, yielding each key.

    Streams via SCAN cursor batches so a million-key keyspace doesn't
    materialize as a single Vec.
    """
    cursor = 0
    while True:
        cursor, batch = adapter.scan(cursor, pattern, count, None)
        yield from batch
        if cursor == 0:
            return


async def ascan_iter_loop(adapter: Any, pattern: str, count: int) -> Any:
    """Drive the SCAN cursor loop asynchronously, yielding each key."""
    cursor = 0
    while True:
        cursor, batch = await adapter.ascan(cursor, pattern, count, None)
        for item in batch:
            yield item
        if cursor == 0:
            return


async def adelete_pattern_loop(
    adapter: Any,
    pattern: str,
    itersize: int,
) -> int:
    """Stream the SCAN cursor and ``DEL`` matched keys in itersize batches.

    Cursor-based iteration so the keyspace is never materialized as a
    single list — important for keyspaces with millions of matches.
    """
    total = 0
    cursor = 0
    batch: list[str] = []
    while True:
        cursor, keys = await adapter.ascan(cursor, pattern, itersize, None)
        for key in keys:
            batch.append(key)
            if len(batch) >= itersize:
                total += int(await adapter.adelete_many(batch))
                batch = []
        if cursor == 0:
            break
    if batch:
        total += int(await adapter.adelete_many(batch))
    return total
