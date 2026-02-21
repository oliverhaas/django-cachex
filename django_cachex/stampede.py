"""Cache stampede prevention via XFetch probabilistic early expiration.

Implements the XFetch algorithm (Vattani et al., VLDB 2015) for optimal
cache stampede prevention. Values are wrapped in a lightweight binary
envelope containing logical expiry and recomputation time metadata.

The envelope wraps *after* serialization/compression, so the encode/decode
pipeline is unchanged and integer values (used by incr/decr) pass through
without wrapping.
"""

from __future__ import annotations

import math
import random
import struct
import time
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django_cachex.types import KeyT

# Envelope marker: 4 bytes chosen to be invalid as the first byte of all
# supported serialization formats (pickle=\x80, JSON=ASCII, msgpack=\x80+)
# and all supported compression formats (gzip=\x1f, zlib=\x78, lz4=\x04,
# lzma=\xfd, zstd=\x28).
ENVELOPE_MARKER = b"\x00CX\x01"
_HEADER_SIZE = 4 + 8 + 4  # marker + expiry_ms(Q) + delta_ms(I) = 16 bytes
_HEADER_STRUCT = struct.Struct(">QI")

# Thread/task-safe storage for pending recomputation timestamps.
# ContextVar is per-thread in sync (WSGI) and per-task in async (ASGI).
_pending_recomputations: ContextVar[dict[KeyT, float]] = ContextVar(
    "_pending_recomputations",
)


@dataclass(frozen=True, slots=True)
class StampedeConfig:
    """Configuration for stampede prevention."""

    buffer: int = 60  # extra TTL seconds added to Redis expiry
    beta: float = 1.0  # XFetch beta parameter (higher = earlier recompute)
    default_delta: float = 1.0  # default recomputation time estimate in seconds


def wrap_envelope(
    value_bytes: bytes,
    timeout: int,
    config: StampedeConfig,
    delta_seconds: float | None = None,
) -> bytes:
    """Wrap encoded value bytes in a stampede prevention envelope.

    Args:
        value_bytes: Already-encoded (serialized + compressed) value bytes.
        timeout: Logical timeout in seconds (must be > 0).
        config: Stampede prevention configuration.
        delta_seconds: Measured recomputation time. None uses config.default_delta.
    """
    now_ms = int(time.time() * 1000)
    logical_expiry_ms = now_ms + (timeout * 1000)
    delta_ms = int((delta_seconds if delta_seconds is not None else config.default_delta) * 1000)

    header = _HEADER_STRUCT.pack(logical_expiry_ms, delta_ms)
    return ENVELOPE_MARKER + header + value_bytes


def unwrap_envelope(
    raw: bytes,
    config: StampedeConfig,
) -> tuple[bytes, bool]:
    """Unwrap an envelope, returning the value bytes and whether to recompute.

    Args:
        raw: Raw bytes from Redis (may or may not be an envelope).
        config: Stampede prevention configuration.

    Returns:
        Tuple of (value_bytes, should_recompute).
        If not an envelope, returns (raw, False).
    """
    if not raw.startswith(ENVELOPE_MARKER):
        return raw, False

    if len(raw) < _HEADER_SIZE:
        return raw, False

    logical_expiry_ms, delta_ms = _HEADER_STRUCT.unpack(raw[4:_HEADER_SIZE])
    value_bytes = raw[_HEADER_SIZE:]

    now_ms = int(time.time() * 1000)
    remaining_ms = logical_expiry_ms - now_ms

    if remaining_ms <= 0:
        return value_bytes, True

    if delta_ms > 0:
        threshold = delta_ms * config.beta * math.log(random.random())  # noqa: S311
        if remaining_ms + threshold <= 0:
            return value_bytes, True

    return value_bytes, False


def record_recomputation_start(key: KeyT) -> None:
    """Record that we're triggering a recomputation for this key."""
    try:
        pending = _pending_recomputations.get()
    except LookupError:
        pending = {}
        _pending_recomputations.set(pending)
    pending[key] = time.time()


def get_recomputation_delta(key: KeyT) -> float | None:
    """Get and clear the measured recomputation time for a key.

    Returns delta in seconds, or None if no pending recomputation was recorded.
    """
    try:
        pending = _pending_recomputations.get()
    except LookupError:
        return None
    start = pending.pop(key, None)
    if start is not None:
        return time.time() - start
    return None
