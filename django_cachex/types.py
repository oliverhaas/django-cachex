"""Type definitions for django-cachex."""

from enum import StrEnum


class KeyType(StrEnum):
    """Redis key data types."""

    STRING = "string"
    LIST = "list"
    SET = "set"
    HASH = "hash"
    ZSET = "zset"
    STREAM = "stream"
