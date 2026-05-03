"""Type aliases for django-cachex.

Compatible with redis-py and valkey-py type systems, defined locally
to avoid a runtime dependency on either library for type annotations.
"""

from datetime import datetime, timedelta
from enum import StrEnum

# Key types - matches redis.typing.KeyT and valkey.typing.KeyT
type KeyT = bytes | str | memoryview

# Expiry types (relative timeout) - matches redis.typing.ExpiryT
type ExpiryT = int | timedelta

# Absolute expiry types - matches redis.typing.AbsExpiryT
type AbsExpiryT = int | datetime


class KeyType(StrEnum):
    """Redis key data types."""

    STRING = "string"
    LIST = "list"
    SET = "set"
    HASH = "hash"
    ZSET = "zset"
    STREAM = "stream"


# Alias to avoid shadowing by .set() methods
_Set = set
