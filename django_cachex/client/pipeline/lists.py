from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from django_cachex.types import KeyT


class ListPipelineMixin:
    """Pipeline mixin providing Redis list operations.

    Note: Blocking operations (blpop, brpop, blmove) are not supported
    in pipelines since blocking doesn't make sense in batched execution.
    """

    # Type hints for base class attributes
    _pipeline: Any
    _decoders: list
    _noop: Any
    _decode_single: Any
    _decode_list: Any
    _decode_single_or_list: Any
    _make_key: Any
    _encode: Any

    def lpush(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
    ) -> Self:
        """Queue LPUSH command (insert at head)."""
        nkey = self._make_key(key, version)
        encoded_values = [self._encode(value) for value in values]
        self._pipeline.lpush(nkey, *encoded_values)
        self._decoders.append(self._noop)  # Returns count
        return self

    def rpush(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
    ) -> Self:
        """Queue RPUSH command (insert at tail)."""
        nkey = self._make_key(key, version)
        encoded_values = [self._encode(value) for value in values]
        self._pipeline.rpush(nkey, *encoded_values)
        self._decoders.append(self._noop)  # Returns count
        return self

    def lpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue LPOP command (remove from head)."""
        nkey = self._make_key(key, version)
        self._pipeline.lpop(nkey, count=count)
        self._decoders.append(self._decode_single_or_list)
        return self

    def rpop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue RPOP command (remove from tail)."""
        nkey = self._make_key(key, version)
        self._pipeline.rpop(nkey, count=count)
        self._decoders.append(self._decode_single_or_list)
        return self

    def lrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        version: int | None = None,
    ) -> Self:
        """Queue LRANGE command (get range of elements)."""
        nkey = self._make_key(key, version)
        self._pipeline.lrange(nkey, start, end)
        self._decoders.append(self._decode_list)
        return self

    def lindex(
        self,
        key: KeyT,
        index: int,
        version: int | None = None,
    ) -> Self:
        """Queue LINDEX command (get element at index)."""
        nkey = self._make_key(key, version)
        self._pipeline.lindex(nkey, index)
        self._decoders.append(self._decode_single)
        return self

    def llen(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue LLEN command (get list length)."""
        nkey = self._make_key(key, version)
        self._pipeline.llen(nkey)
        self._decoders.append(self._noop)  # Returns int
        return self

    def lrem(
        self,
        key: KeyT,
        count: int,
        value: Any,
        version: int | None = None,
    ) -> Self:
        """Queue LREM command (remove elements)."""
        nkey = self._make_key(key, version)
        encoded_value = self._encode(value)
        self._pipeline.lrem(nkey, count, encoded_value)
        self._decoders.append(self._noop)  # Returns count removed
        return self

    def ltrim(
        self,
        key: KeyT,
        start: int,
        end: int,
        version: int | None = None,
    ) -> Self:
        """Queue LTRIM command (trim list to range)."""
        nkey = self._make_key(key, version)
        self._pipeline.ltrim(nkey, start, end)
        self._decoders.append(self._noop)  # Returns bool
        return self

    def lset(
        self,
        key: KeyT,
        index: int,
        value: Any,
        version: int | None = None,
    ) -> Self:
        """Queue LSET command (set element at index)."""
        nkey = self._make_key(key, version)
        encoded_value = self._encode(value)
        self._pipeline.lset(nkey, index, encoded_value)
        self._decoders.append(self._noop)  # Returns bool
        return self

    def linsert(
        self,
        key: KeyT,
        where: str,
        pivot: Any,
        value: Any,
        version: int | None = None,
    ) -> Self:
        """Queue LINSERT command (insert before/after pivot)."""
        nkey = self._make_key(key, version)
        encoded_pivot = self._encode(pivot)
        encoded_value = self._encode(value)
        self._pipeline.linsert(nkey, where, encoded_pivot, encoded_value)
        self._decoders.append(self._noop)  # Returns new length or -1
        return self

    def lpos(
        self,
        key: KeyT,
        value: Any,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue LPOS command (find position of element)."""
        nkey = self._make_key(key, version)
        encoded_value = self._encode(value)
        self._pipeline.lpos(nkey, encoded_value, rank=rank, count=count, maxlen=maxlen)
        self._decoders.append(self._noop)  # Returns int, list[int], or None
        return self

    def lmove(
        self,
        source: KeyT,
        destination: KeyT,
        src_direction: str = "LEFT",
        dest_direction: str = "RIGHT",
        version: int | None = None,
    ) -> Self:
        """Queue LMOVE command (move element between lists)."""
        nsrc = self._make_key(source, version)
        ndst = self._make_key(destination, version)
        self._pipeline.lmove(nsrc, ndst, src_direction, dest_direction)
        self._decoders.append(self._decode_single)
        return self
