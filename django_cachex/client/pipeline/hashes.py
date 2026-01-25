from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from django_cachex.types import KeyT


class HashPipelineMixin:
    """Pipeline mixin providing Redis hash operations."""

    # Type hints for base class attributes
    _pipeline: Any
    _decoders: list
    _client: Any
    _noop: Any
    _decode_single: Any
    _make_key: Any
    _encode: Any

    def _decode_hash_keys(self, value: list[bytes]) -> list[str]:
        """Decode hash field names (keys are not serialized, just bytes)."""
        return [k.decode() for k in value]

    def _decode_hash_values(self, value: list[bytes | None]) -> list[Any]:
        """Decode hash values (may contain None for missing fields)."""
        return [self._client.decode(v) if v is not None else None for v in value]

    def _decode_hash_dict(self, value: dict[bytes, bytes]) -> dict[str, Any]:
        """Decode a full hash (keys are strings, values are decoded)."""
        return {k.decode(): self._client.decode(v) for k, v in value.items()}

    def hset(
        self,
        key: KeyT,
        field: str,
        value: Any,
        version: int | None = None,
    ) -> Self:
        """Queue HSET command (set field value)."""
        nkey = self._make_key(key, version)
        nvalue = self._encode(value)
        self._pipeline.hset(nkey, field, nvalue)
        self._decoders.append(self._noop)  # Returns count of fields added
        return self

    def hmset(
        self,
        key: KeyT,
        mapping: dict[str, Any],
        version: int | None = None,
    ) -> Self:
        """Queue HSET with mapping (set multiple fields)."""
        nkey = self._make_key(key, version)
        encoded_mapping = {field: self._encode(value) for field, value in mapping.items()}
        self._pipeline.hset(nkey, mapping=encoded_mapping)
        self._decoders.append(self._noop)  # Returns count of fields added
        return self

    def hdel(
        self,
        key: KeyT,
        field: str,
        version: int | None = None,
    ) -> Self:
        """Queue HDEL command (delete field)."""
        nkey = self._make_key(key, version)
        self._pipeline.hdel(nkey, field)
        self._decoders.append(self._noop)  # Returns count deleted
        return self

    def hlen(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue HLEN command (get number of fields)."""
        nkey = self._make_key(key, version)
        self._pipeline.hlen(nkey)
        self._decoders.append(self._noop)  # Returns int
        return self

    def hkeys(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue HKEYS command (get all field names)."""
        nkey = self._make_key(key, version)
        self._pipeline.hkeys(nkey)
        self._decoders.append(self._decode_hash_keys)
        return self

    def hexists(
        self,
        key: KeyT,
        field: str,
        version: int | None = None,
    ) -> Self:
        """Queue HEXISTS command (check if field exists)."""
        nkey = self._make_key(key, version)
        self._pipeline.hexists(nkey, field)
        self._decoders.append(lambda x: bool(x))
        return self

    def hget(
        self,
        key: KeyT,
        field: str,
        version: int | None = None,
    ) -> Self:
        """Queue HGET command (get field value)."""
        nkey = self._make_key(key, version)
        self._pipeline.hget(nkey, field)
        self._decoders.append(self._decode_single)
        return self

    def hgetall(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue HGETALL command (get all fields and values)."""
        nkey = self._make_key(key, version)
        self._pipeline.hgetall(nkey)
        self._decoders.append(self._decode_hash_dict)
        return self

    def hmget(
        self,
        key: KeyT,
        *fields: str,
        version: int | None = None,
    ) -> Self:
        """Queue HMGET command (get multiple field values)."""
        nkey = self._make_key(key, version)
        self._pipeline.hmget(nkey, fields)
        self._decoders.append(self._decode_hash_values)
        return self

    def hincrby(
        self,
        key: KeyT,
        field: str,
        amount: int = 1,
        version: int | None = None,
    ) -> Self:
        """Queue HINCRBY command (increment integer field)."""
        nkey = self._make_key(key, version)
        self._pipeline.hincrby(nkey, field, amount)
        self._decoders.append(self._noop)  # Returns new value
        return self

    def hincrbyfloat(
        self,
        key: KeyT,
        field: str,
        amount: float = 1.0,
        version: int | None = None,
    ) -> Self:
        """Queue HINCRBYFLOAT command (increment float field)."""
        nkey = self._make_key(key, version)
        self._pipeline.hincrbyfloat(nkey, field, amount)
        self._decoders.append(self._noop)  # Returns new value
        return self

    def hsetnx(
        self,
        key: KeyT,
        field: str,
        value: Any,
        version: int | None = None,
    ) -> Self:
        """Queue HSETNX command (set field only if not exists)."""
        nkey = self._make_key(key, version)
        nvalue = self._encode(value)
        self._pipeline.hsetnx(nkey, field, nvalue)
        self._decoders.append(lambda x: bool(x))
        return self

    def hvals(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue HVALS command (get all values)."""
        nkey = self._make_key(key, version)
        self._pipeline.hvals(nkey)
        self._decoders.append(lambda x: [self._client.decode(v) for v in x])
        return self
