from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from django_cachex.types import KeyT


class SetPipelineMixin:
    """Pipeline mixin providing Redis set operations.

    Note: sscan_iter is not supported in pipelines since iterators
    don't make sense in batched execution.
    """

    # Type hints for base class attributes
    _pipeline: Any
    _decoders: list
    _client: Any
    _noop: Any
    _decode_single: Any
    _decode_list: Any
    _make_key: Any
    _encode: Any

    def _decode_set(self, value: set[bytes]) -> set[Any]:
        """Decode a set of values."""
        return {self._client.decode(item) for item in value}

    def _decode_set_or_single(self, value: set[bytes] | bytes | None) -> set[Any] | Any:
        """Decode spop/srandmember result (set, single value, or None)."""
        if value is None:
            return None
        if isinstance(value, (set, list)):
            return {self._client.decode(item) for item in value}
        return self._client.decode(value)

    def sadd(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
    ) -> Self:
        """Queue SADD command (add members to set)."""
        nkey = self._make_key(key, version)
        encoded_values = [self._encode(value) for value in values]
        self._pipeline.sadd(nkey, *encoded_values)
        self._decoders.append(self._noop)  # Returns count added
        return self

    def scard(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue SCARD command (get set cardinality)."""
        nkey = self._make_key(key, version)
        self._pipeline.scard(nkey)
        self._decoders.append(self._noop)  # Returns int
        return self

    def sdiff(
        self,
        *keys: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue SDIFF command (set difference)."""
        nkeys = [self._make_key(key, version) for key in keys]
        self._pipeline.sdiff(*nkeys)
        self._decoders.append(self._decode_set)
        return self

    def sdiffstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> Self:
        """Queue SDIFFSTORE command (store set difference)."""
        ndest = self._make_key(dest, version_dest)
        nkeys = [self._make_key(key, version_keys) for key in keys]
        self._pipeline.sdiffstore(ndest, *nkeys)
        self._decoders.append(self._noop)  # Returns count
        return self

    def sinter(
        self,
        *keys: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue SINTER command (set intersection)."""
        nkeys = [self._make_key(key, version) for key in keys]
        self._pipeline.sinter(*nkeys)
        self._decoders.append(self._decode_set)
        return self

    def sinterstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue SINTERSTORE command (store set intersection)."""
        ndest = self._make_key(dest, version)
        nkeys = [self._make_key(key, version) for key in keys]
        self._pipeline.sinterstore(ndest, *nkeys)
        self._decoders.append(self._noop)  # Returns count
        return self

    def sismember(
        self,
        key: KeyT,
        member: Any,
        version: int | None = None,
    ) -> Self:
        """Queue SISMEMBER command (check membership)."""
        nkey = self._make_key(key, version)
        nmember = self._encode(member)
        self._pipeline.sismember(nkey, nmember)
        self._decoders.append(lambda x: bool(x))  # Returns bool
        return self

    def smismember(
        self,
        key: KeyT,
        *members: Any,
        version: int | None = None,
    ) -> Self:
        """Queue SMISMEMBER command (check multiple memberships)."""
        nkey = self._make_key(key, version)
        encoded_members = [self._encode(member) for member in members]
        self._pipeline.smismember(nkey, *encoded_members)
        self._decoders.append(lambda x: [bool(v) for v in x])  # Returns list[bool]
        return self

    def smembers(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue SMEMBERS command (get all members)."""
        nkey = self._make_key(key, version)
        self._pipeline.smembers(nkey)
        self._decoders.append(self._decode_set)
        return self

    def smove(
        self,
        source: KeyT,
        destination: KeyT,
        member: Any,
        version: int | None = None,
    ) -> Self:
        """Queue SMOVE command (move member between sets)."""
        nsource = self._make_key(source, version)
        ndestination = self._make_key(destination, version)
        nmember = self._encode(member)
        self._pipeline.smove(nsource, ndestination, nmember)
        self._decoders.append(lambda x: bool(x))  # Returns bool
        return self

    def spop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue SPOP command (remove and return random member(s))."""
        nkey = self._make_key(key, version)
        self._pipeline.spop(nkey, count)
        self._decoders.append(self._decode_set_or_single)
        return self

    def srandmember(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue SRANDMEMBER command (get random member(s))."""
        nkey = self._make_key(key, version)
        self._pipeline.srandmember(nkey, count)
        # Returns list when count is specified, single value otherwise
        self._decoders.append(
            lambda x: (
                [self._client.decode(item) for item in x]
                if isinstance(x, list)
                else (self._client.decode(x) if x is not None else None)
            ),
        )
        return self

    def srem(
        self,
        key: KeyT,
        *members: Any,
        version: int | None = None,
    ) -> Self:
        """Queue SREM command (remove members)."""
        nkey = self._make_key(key, version)
        nmembers = [self._encode(member) for member in members]
        self._pipeline.srem(nkey, *nmembers)
        self._decoders.append(self._noop)  # Returns count removed
        return self

    def sunion(
        self,
        *keys: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue SUNION command (set union)."""
        nkeys = [self._make_key(key, version) for key in keys]
        self._pipeline.sunion(*nkeys)
        self._decoders.append(self._decode_set)
        return self

    def sunionstore(
        self,
        destination: KeyT,
        *keys: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue SUNIONSTORE command (store set union)."""
        ndestination = self._make_key(destination, version)
        nkeys = [self._make_key(key, version) for key in keys]
        self._pipeline.sunionstore(ndestination, *nkeys)
        self._decoders.append(self._noop)  # Returns count
        return self
