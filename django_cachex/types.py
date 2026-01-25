"""Type aliases for django-cachex.

These types are designed to be 100% compatible with:
- redis-py's type system (redis.typing)
- valkey-py's type system (valkey.typing)
- Django's cache backend types

By defining these ourselves, we avoid a runtime dependency on redis-py/valkey-py
just for type annotations.
"""

from datetime import datetime, timedelta
from typing import Any, Protocol, TypeVar

# =============================================================================
# Types matching redis-py / valkey-py (for compatibility with their APIs)
# =============================================================================

# Key types - matches redis.typing.KeyT and valkey.typing.KeyT
type KeyT = bytes | str | memoryview

# Pattern types - matches redis.typing.PatternT
type PatternT = bytes | str | memoryview

# Encodable value types - matches redis.typing.EncodableT
# Note: redis-py includes bytearray, valkey-py doesn't - we include it for broader compat
type EncodableT = bytes | bytearray | memoryview | str | int | float

# Expiry types (relative timeout) - matches redis.typing.ExpiryT
type ExpiryT = int | timedelta

# Absolute expiry types - matches redis.typing.AbsExpiryT
type AbsExpiryT = int | datetime

# =============================================================================
# Our own types (not from redis-py/valkey-py)
# =============================================================================

# Timeout type for Django cache interface (allows float seconds and None)
type TimeoutT = float | int | timedelta | None

# Value types for cache operations
CacheValueT = TypeVar("CacheValueT")

# Encoded value type (after serialization, before storage)
type EncodedT = bytes | int


class SerializerProtocol(Protocol):
    """Protocol for cache value serializers."""

    def dumps(self, value: Any) -> bytes:
        """Serialize a value to bytes."""
        ...

    def loads(self, value: bytes) -> Any:
        """Deserialize bytes to a value."""
        ...


class CompressorProtocol(Protocol):
    """Protocol for cache value compressors."""

    min_length: int

    def compress(self, value: bytes) -> bytes:
        """Compress bytes."""
        ...

    def decompress(self, value: bytes) -> bytes:
        """Decompress bytes."""
        ...
