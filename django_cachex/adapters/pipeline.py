"""Pipeline layer.

:class:`Pipeline` is the user-facing wrapper that handles key prefixing,
value serialization, and result decoding. It delegates all queueing to a
concrete :class:`~django_cachex.adapters.protocols.RespPipelineProtocol`
implementation — one per driver:

- :class:`~django_cachex.adapters.valkey_py.ValkeyPyPipelineAdapter` /
  :class:`~django_cachex.adapters.redis_py.RedisPyPipelineAdapter` — wrap a
  redis-py / valkey-py / cluster ``Pipeline`` object.
- :class:`~django_cachex.adapters.redis_rs.RedisRsPipelineAdapter` —
  buffers RESP wire commands for the Rust driver's ``pipeline_exec``.
- :class:`~django_cachex.adapters.valkey_glide.ValkeyGlidePipelineAdapter` —
  drives ``valkey-glide``'s ``Batch``.

Each adapter's ``RespAdapterProtocol.pipeline()`` factory constructs the right
concrete pipeline adapter and the cache layer wraps it in a :class:`Pipeline`.
"""

from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime, timedelta

    from django_cachex.adapters.protocols import RespAsyncPipelineProtocol, RespPipelineProtocol
    from django_cachex.types import KeyT

from django_cachex.script import ScriptHelpers

# Aliases for builtins shadowed by `set`/`type` methods (PEP 649 defers
# annotations at runtime, but type checkers still resolve them in class scope).
_set = set
_type = type

# Type aliases matching django_cachex.types for convenience
type ExpiryT = int | timedelta
type AbsExpiryT = int | datetime


class Pipeline:
    """Pipeline wrapper that handles key prefixing and value serialization.

    Queues cachex ops on a :class:`RespPipelineProtocol` implementation,
    then decodes the raw results when ``execute()`` is called.
    """

    def __init__(
        self,
        cache: Any,
        pipeline_adapter: RespPipelineProtocol,
        version: int | None = None,
    ) -> None:
        """Initialize the wrapped pipeline."""
        self._cache = cache
        # Adapter back-reference for timeout / stampede helpers that still
        # live on the adapter layer (``_get_timeout_with_buffer`` /
        # ``_resolve_stampede``). Encoding/decoding lives on the cache.
        self._adapter = cache.adapter
        self._pipeline_adapter = pipeline_adapter
        self._version = version
        self._key_func: Callable[..., str] | None = None
        self._cache_version: int | None = None
        self._decoders: list[Callable[[Any], Any]] = []

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(self, *args: object) -> None:
        """Exit context manager, resetting the underlying pipeline."""
        self._pipeline_adapter.reset()
        self._decoders.clear()

    def execute(self) -> list[Any]:
        """Execute all queued commands and decode the results."""
        results = self._pipeline_adapter.execute()
        decoders = self._decoders
        self._decoders = []
        decoded = []
        for result, decoder in zip(results, decoders, strict=True):
            decoded.append(decoder(result))
        return decoded

    # -------------------------------------------------------------------------
    # Decoder helpers
    # -------------------------------------------------------------------------

    def _noop(self, value: Any) -> Any:
        """Return value unchanged (for int, bool, etc.)."""
        return value

    def _decode_single(self, value: bytes | None) -> Any:
        """Decode a single value, returning None if None.

        Pipelines always serve values as-is (stale serving) — no stampede TTL checks.
        """
        if value is None:
            return None
        return self._cache.decode(value)

    def _decode_list(self, value: list[bytes | None]) -> list[Any]:
        """Decode a list of values."""
        return [self._cache.decode(item) if item is not None else None for item in value]

    def _decode_single_or_list(self, value: bytes | list[bytes | None] | None) -> Any:
        """Decode value that may be single item, list, or None (lpop/rpop with count)."""
        if value is None:
            return None
        if isinstance(value, list):
            return [self._cache.decode(item) if item is not None else None for item in value]
        return self._cache.decode(value)

    def _decode_set(self, value: _set[bytes]) -> _set[Any]:
        """Decode a set of values."""
        return {self._cache.decode(item) for item in value}

    def _decode_set_or_single(self, value: _set[bytes] | bytes | None) -> _set[Any] | Any:
        """Decode spop/srandmember result (set, single value, or None)."""
        if value is None:
            return None
        if isinstance(value, (set, list)):
            return {self._cache.decode(item) for item in value}
        return self._cache.decode(value)

    def _decode_hash_keys(self, value: list[bytes]) -> list[str]:
        """Decode hash field names (keys are not serialized, just bytes)."""
        return [k.decode() for k in value]

    def _decode_hash_values(self, value: list[bytes | None]) -> list[Any]:
        """Decode hash values (may contain None for missing fields)."""
        return [self._cache.decode(v) if v is not None else None for v in value]

    def _decode_hash_dict(self, value: dict[bytes, bytes]) -> dict[str, Any]:
        """Decode a full hash (keys are strings, values are decoded)."""
        return {k.decode(): self._cache.decode(v) for k, v in value.items()}

    def _decode_zset_members(self, value: list[bytes]) -> list[Any]:
        """Decode sorted set members (without scores)."""
        return [self._cache.decode(member) for member in value]

    def _decode_zset_with_scores(self, value: list[tuple[bytes, float]]) -> list[tuple[Any, float]]:
        """Decode sorted set members with scores."""
        return [(self._cache.decode(member), score) for member, score in value]

    def _make_zset_decoder(self, *, withscores: bool) -> Callable[[list[tuple[bytes, float]]], list]:
        """Create decoder based on whether scores are included."""
        if withscores:
            return self._decode_zset_with_scores
        return self._decode_zset_members  # type: ignore[return-value]  # ty: ignore[invalid-return-type]

    def _decode_zpop(self, value: list[tuple[bytes, float]]) -> list[tuple[Any, float]]:
        """Decode zpopmin/zpopmax result."""
        if not value:
            return []
        return [(self._cache.decode(member), score) for member, score in value]

    def _decode_type(self, value: bytes | str) -> str:
        """Decode TYPE result to string."""
        return value.decode() if isinstance(value, bytes) else value

    def _decode_entry_id(self, value: bytes | str) -> str:
        """Decode stream entry ID."""
        return value.decode() if isinstance(value, bytes) else value

    def _decode_stream_entries(self, results: list[tuple[Any, dict[Any, Any]]]) -> list[tuple[str, dict[str, Any]]]:
        """Decode raw stream entries from Redis."""
        return [
            (
                entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                {k.decode() if isinstance(k, bytes) else k: self._cache.decode(v) for k, v in fields.items()},
            )
            for entry_id, fields in results
        ]

    def _decode_stream_results(
        self,
        results: list[tuple[Any, list[tuple[Any, dict[Any, Any]]]]] | None,
    ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
        """Decode multi-stream results (xread/xreadgroup)."""
        if results is None:
            return None
        return {
            (stream_key.decode() if isinstance(stream_key, bytes) else stream_key): self._decode_stream_entries(
                entries,
            )
            for stream_key, entries in results
        }

    def _make_stream_key_decoder(
        self,
        key_map: dict[str, Any],
    ) -> Callable[[Any], dict[str, list[tuple[str, dict[str, Any]]]] | None]:
        """Create a decoder that un-prefixes stream keys in xread/xreadgroup results."""

        def decode(
            results: list[tuple[Any, list[tuple[Any, dict[Any, Any]]]]] | None,
        ) -> dict[str, list[tuple[str, dict[str, Any]]]] | None:
            if results is None:
                return None
            decoded: dict[str, list[tuple[str, dict[str, Any]]]] = {}
            for stream_key, entries in results:
                sk = stream_key.decode() if isinstance(stream_key, bytes) else str(stream_key)
                original = key_map.get(sk, sk)
                decoded[str(original)] = self._decode_stream_entries(entries)
            return decoded

        return decode

    # -------------------------------------------------------------------------
    # Key/value helpers
    # -------------------------------------------------------------------------

    def _make_key(self, key: KeyT, version: int | None = None) -> KeyT:
        """Create a prefixed key."""
        v = version if version is not None else self._version
        if self._key_func is not None:
            return self._key_func(key, version=v)
        return self._cache.make_and_validate_key(key, version=v)

    def _encode(self, value: Any) -> bytes | int:
        """Encode a value for storage."""
        return self._cache.encode(value)

    # -------------------------------------------------------------------------
    # Core cache operations
    # -------------------------------------------------------------------------

    def set(
        self,
        key: KeyT,
        value: Any,
        timeout: int | None = None,
        version: int | None = None,
        *,
        nx: bool = False,
        xx: bool = False,
        stampede_prevention: bool | dict | None = None,
    ) -> Self:
        """Queue a SET command."""
        nkey = self._make_key(key, version)
        nvalue = self._encode(value)
        actual_timeout = self._adapter._get_timeout_with_buffer(timeout, stampede_prevention)

        # timeout=0 means "expire immediately" (Django convention) — queue a DELETE
        if actual_timeout == 0:
            self._pipeline_adapter.delete(nkey)
            self._decoders.append(bool)
            return self

        kwargs: dict[str, Any] = {}
        if actual_timeout is not None:
            kwargs["ex"] = actual_timeout
        if nx:
            kwargs["nx"] = True
        if xx:
            kwargs["xx"] = True

        self._pipeline_adapter.set(nkey, nvalue, **kwargs)
        # SET returns OK/True on success, None on failure (with NX/XX)
        # We return True for success, None for failure
        self._decoders.append(lambda x: True if (x is not None and x != b"" and x is not False) else None)
        return self

    def get(self, key: KeyT, version: int | None = None) -> Self:
        """Queue a GET command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.get(nkey)
        self._decoders.append(self._decode_single)
        return self

    def delete(self, key: KeyT, version: int | None = None) -> Self:
        """Queue a DELETE command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.delete(nkey)
        # DEL returns count of deleted keys, convert to bool
        self._decoders.append(bool)
        return self

    def exists(self, key: KeyT, version: int | None = None) -> Self:
        """Queue an EXISTS command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.exists(nkey)
        # EXISTS returns count, convert to bool
        self._decoders.append(bool)
        return self

    def expire(
        self,
        key: KeyT,
        timeout: int,
        version: int | None = None,
    ) -> Self:
        """Queue an EXPIRE command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.expire(nkey, timeout)
        self._decoders.append(self._noop)
        return self

    def ttl(self, key: KeyT, version: int | None = None) -> Self:
        """Queue a TTL command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.ttl(nkey)
        self._decoders.append(self._noop)
        return self

    def incr(
        self,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
    ) -> Self:
        """Queue an INCRBY command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.incrby(nkey, delta)
        self._decoders.append(self._noop)
        return self

    def decr(
        self,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
    ) -> Self:
        """Queue a DECRBY command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.decrby(nkey, delta)
        self._decoders.append(self._noop)
        return self

    def persist(self, key: KeyT, version: int | None = None) -> Self:
        """Queue a PERSIST command (remove expiry)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.persist(nkey)
        self._decoders.append(bool)
        return self

    def pttl(self, key: KeyT, version: int | None = None) -> Self:
        """Queue a PTTL command (TTL in milliseconds)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.pttl(nkey)
        self._decoders.append(self._noop)
        return self

    def expireat(
        self,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
    ) -> Self:
        """Queue an EXPIREAT command (set expiry to absolute time)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.expireat(nkey, when)
        self._decoders.append(bool)
        return self

    def pexpire(
        self,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
    ) -> Self:
        """Queue a PEXPIRE command (set expiry in milliseconds)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.pexpire(nkey, timeout)
        self._decoders.append(bool)
        return self

    def pexpireat(
        self,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
    ) -> Self:
        """Queue a PEXPIREAT command (set expiry to absolute time, ms precision)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.pexpireat(nkey, when)
        self._decoders.append(bool)
        return self

    def expiretime(self, key: KeyT, version: int | None = None) -> Self:
        """Queue an EXPIRETIME command (get absolute expiry timestamp)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.expiretime(nkey)
        self._decoders.append(self._noop)
        return self

    def type(self, key: KeyT, version: int | None = None) -> Self:
        """Queue a TYPE command (get key data type)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.type(nkey)
        self._decoders.append(self._decode_type)
        return self

    def rename(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Self:
        """Queue a RENAME command."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        nsrc = self._make_key(src, src_ver)
        ndst = self._make_key(dst, dst_ver)
        self._pipeline_adapter.rename(nsrc, ndst)
        self._decoders.append(self._noop)
        return self

    def renamenx(
        self,
        src: KeyT,
        dst: KeyT,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Self:
        """Queue a RENAMENX command (rename only if dest doesn't exist)."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        nsrc = self._make_key(src, src_ver)
        ndst = self._make_key(dst, dst_ver)
        self._pipeline_adapter.renamenx(nsrc, ndst)
        self._decoders.append(bool)
        return self

    # -------------------------------------------------------------------------
    # List operations
    # -------------------------------------------------------------------------

    def lpush(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
    ) -> Self:
        """Queue LPUSH command (insert at head)."""
        nkey = self._make_key(key, version)
        encoded_values = [self._encode(value) for value in values]
        self._pipeline_adapter.lpush(nkey, *encoded_values)
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
        self._pipeline_adapter.rpush(nkey, *encoded_values)
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
        self._pipeline_adapter.lpop(nkey, count=count)
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
        self._pipeline_adapter.rpop(nkey, count=count)
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
        self._pipeline_adapter.lrange(nkey, start, end)
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
        self._pipeline_adapter.lindex(nkey, index)
        self._decoders.append(self._decode_single)
        return self

    def llen(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue LLEN command (get list length)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.llen(nkey)
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
        self._pipeline_adapter.lrem(nkey, count, encoded_value)
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
        self._pipeline_adapter.ltrim(nkey, start, end)
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
        self._pipeline_adapter.lset(nkey, index, encoded_value)
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
        self._pipeline_adapter.linsert(nkey, where, encoded_pivot, encoded_value)
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
        self._pipeline_adapter.lpos(nkey, encoded_value, rank=rank, count=count, maxlen=maxlen)
        self._decoders.append(self._noop)  # Returns int, list[int], or None
        return self

    def lmove(
        self,
        source: KeyT,
        destination: KeyT,
        src_direction: str = "LEFT",
        dest_direction: str = "RIGHT",
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Self:
        """Queue LMOVE command (move element between lists)."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        nsrc = self._make_key(source, src_ver)
        ndst = self._make_key(destination, dst_ver)
        self._pipeline_adapter.lmove(nsrc, ndst, src_direction, dest_direction)
        self._decoders.append(self._decode_single)
        return self

    # -------------------------------------------------------------------------
    # Set operations
    # -------------------------------------------------------------------------

    def sadd(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
    ) -> Self:
        """Queue SADD command (add members to set)."""
        nkey = self._make_key(key, version)
        encoded_values = [self._encode(value) for value in values]
        self._pipeline_adapter.sadd(nkey, *encoded_values)
        self._decoders.append(self._noop)  # Returns count added
        return self

    def scard(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue SCARD command (get set cardinality)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.scard(nkey)
        self._decoders.append(self._noop)  # Returns int
        return self

    def sdiff(
        self,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
    ) -> Self:
        """Queue SDIFF command (set difference)."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self._make_key(key, version) for key in keys]
        self._pipeline_adapter.sdiff(*nkeys)
        self._decoders.append(self._decode_set)
        return self

    def sdiffstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> Self:
        """Queue SDIFFSTORE command (store set difference)."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        ndest = self._make_key(dest, dest_ver)
        nkeys = [self._make_key(key, keys_ver) for key in keys]
        self._pipeline_adapter.sdiffstore(ndest, *nkeys)
        self._decoders.append(self._noop)  # Returns count
        return self

    def sinter(
        self,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
    ) -> Self:
        """Queue SINTER command (set intersection)."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self._make_key(key, version) for key in keys]
        self._pipeline_adapter.sinter(*nkeys)
        self._decoders.append(self._decode_set)
        return self

    def sinterstore(
        self,
        dest: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> Self:
        """Queue SINTERSTORE command (store set intersection)."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        ndest = self._make_key(dest, dest_ver)
        nkeys = [self._make_key(key, keys_ver) for key in keys]
        self._pipeline_adapter.sinterstore(ndest, *nkeys)
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
        self._pipeline_adapter.sismember(nkey, nmember)
        self._decoders.append(bool)  # Returns bool
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
        self._pipeline_adapter.smismember(nkey, *encoded_members)
        self._decoders.append(lambda x: [bool(v) for v in x])  # Returns list[bool]
        return self

    def smembers(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue SMEMBERS command (get all members)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.smembers(nkey)
        self._decoders.append(self._decode_set)
        return self

    def smove(
        self,
        source: KeyT,
        destination: KeyT,
        member: Any,
        version: int | None = None,
        version_src: int | None = None,
        version_dst: int | None = None,
    ) -> Self:
        """Queue SMOVE command (move member between sets)."""
        src_ver = version_src if version_src is not None else version
        dst_ver = version_dst if version_dst is not None else version
        nsource = self._make_key(source, src_ver)
        ndestination = self._make_key(destination, dst_ver)
        nmember = self._encode(member)
        self._pipeline_adapter.smove(nsource, ndestination, nmember)
        self._decoders.append(bool)  # Returns bool
        return self

    def spop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue SPOP command (remove and return random member(s))."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.spop(nkey, count)
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
        self._pipeline_adapter.srandmember(nkey, count)
        # Returns list when count is specified, single value otherwise
        self._decoders.append(
            lambda x: (
                [self._cache.decode(item) for item in x]
                if isinstance(x, list)
                else (self._cache.decode(x) if x is not None else None)
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
        self._pipeline_adapter.srem(nkey, *nmembers)
        self._decoders.append(self._noop)  # Returns count removed
        return self

    def sunion(
        self,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
    ) -> Self:
        """Queue SUNION command (set union)."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        nkeys = [self._make_key(key, version) for key in keys]
        self._pipeline_adapter.sunion(*nkeys)
        self._decoders.append(self._decode_set)
        return self

    def sunionstore(
        self,
        destination: KeyT,
        keys: KeyT | Sequence[KeyT],
        version: int | None = None,
        version_dest: int | None = None,
        version_keys: int | None = None,
    ) -> Self:
        """Queue SUNIONSTORE command (store set union)."""
        keys = [keys] if isinstance(keys, (str, bytes, memoryview)) else keys
        dest_ver = version_dest if version_dest is not None else version
        keys_ver = version_keys if version_keys is not None else version
        ndestination = self._make_key(destination, dest_ver)
        nkeys = [self._make_key(key, keys_ver) for key in keys]
        self._pipeline_adapter.sunionstore(ndestination, *nkeys)
        self._decoders.append(self._noop)  # Returns count
        return self

    # -------------------------------------------------------------------------
    # Hash operations
    # -------------------------------------------------------------------------

    def hset(
        self,
        key: KeyT,
        field: str | None = None,
        value: Any = None,
        version: int | None = None,
        mapping: dict[str, Any] | None = None,
        items: list[Any] | None = None,
    ) -> Self:
        """Queue HSET command. Use field/value, mapping, or items (flat key-value pairs)."""
        nkey = self._make_key(key, version)
        nvalue = self._encode(value) if field is not None else None
        nmapping = {f: self._encode(v) for f, v in mapping.items()} if mapping else None
        nitems = [self._encode(v) if i % 2 else v for i, v in enumerate(items)] if items else None
        self._pipeline_adapter.hset(nkey, field, nvalue, mapping=nmapping, items=nitems)
        self._decoders.append(self._noop)  # Returns count of fields added
        return self

    def hdel(
        self,
        key: KeyT,
        *fields: str,
        version: int | None = None,
    ) -> Self:
        """Queue HDEL command (delete one or more fields)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.hdel(nkey, *fields)
        self._decoders.append(self._noop)  # Returns count deleted
        return self

    def hlen(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue HLEN command (get number of fields)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.hlen(nkey)
        self._decoders.append(self._noop)  # Returns int
        return self

    def hkeys(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue HKEYS command (get all field names)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.hkeys(nkey)
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
        self._pipeline_adapter.hexists(nkey, field)
        self._decoders.append(bool)
        return self

    def hget(
        self,
        key: KeyT,
        field: str,
        version: int | None = None,
    ) -> Self:
        """Queue HGET command (get field value)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.hget(nkey, field)
        self._decoders.append(self._decode_single)
        return self

    def hgetall(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue HGETALL command (get all fields and values)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.hgetall(nkey)
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
        self._pipeline_adapter.hmget(nkey, fields)
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
        self._pipeline_adapter.hincrby(nkey, field, amount)
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
        self._pipeline_adapter.hincrbyfloat(nkey, field, amount)
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
        self._pipeline_adapter.hsetnx(nkey, field, nvalue)
        self._decoders.append(bool)
        return self

    def hvals(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue HVALS command (get all values)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.hvals(nkey)
        self._decoders.append(lambda x: [self._cache.decode(v) for v in x])
        return self

    # -------------------------------------------------------------------------
    # Sorted set operations
    # -------------------------------------------------------------------------

    def zadd(
        self,
        key: KeyT,
        mapping: dict[Any, float],
        *,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
        gt: bool = False,
        lt: bool = False,
        version: int | None = None,
    ) -> Self:
        """Queue ZADD command (add members with scores)."""
        nkey = self._make_key(key, version)
        # Encode members but NOT scores
        encoded_mapping = {self._encode(member): score for member, score in mapping.items()}
        self._pipeline_adapter.zadd(
            nkey,
            encoded_mapping,
            nx=nx,
            xx=xx,
            ch=ch,
            incr=incr,
            gt=gt,
            lt=lt,
        )
        self._decoders.append(self._noop)  # Returns count added
        return self

    def zcard(
        self,
        key: KeyT,
        version: int | None = None,
    ) -> Self:
        """Queue ZCARD command (get cardinality)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.zcard(nkey)
        self._decoders.append(self._noop)  # Returns int
        return self

    def zcount(
        self,
        key: KeyT,
        min: float | str,
        max: float | str,
        version: int | None = None,
    ) -> Self:
        """Queue ZCOUNT command (count members in score range)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.zcount(nkey, min, max)
        self._decoders.append(self._noop)  # Returns int
        return self

    def zincrby(
        self,
        key: KeyT,
        amount: float,
        value: Any,
        version: int | None = None,
    ) -> Self:
        """Queue ZINCRBY command (increment member's score)."""
        nkey = self._make_key(key, version)
        encoded_value = self._encode(value)
        self._pipeline_adapter.zincrby(nkey, amount, encoded_value)
        self._decoders.append(self._noop)  # Returns new score
        return self

    def zpopmax(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue ZPOPMAX command (pop highest scoring members)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.zpopmax(nkey, count)
        self._decoders.append(self._decode_zpop)
        return self

    def zpopmin(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue ZPOPMIN command (pop lowest scoring members)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.zpopmin(nkey, count)
        self._decoders.append(self._decode_zpop)
        return self

    def zrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: _type = float,
        version: int | None = None,
    ) -> Self:
        """Queue ZRANGE command (get members by index range)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.zrange(
            nkey,
            start,
            end,
            desc=desc,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )
        self._decoders.append(self._make_zset_decoder(withscores=withscores))
        return self

    def zrangebyscore(
        self,
        key: KeyT,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
        *,
        withscores: bool = False,
        score_cast_func: _type = float,
        version: int | None = None,
    ) -> Self:
        """Queue ZRANGEBYSCORE command (get members by score range)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.zrangebyscore(
            nkey,
            min,
            max,
            start=start,
            num=num,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )
        self._decoders.append(self._make_zset_decoder(withscores=withscores))
        return self

    def zrank(
        self,
        key: KeyT,
        value: Any,
        version: int | None = None,
    ) -> Self:
        """Queue ZRANK command (get rank, low to high)."""
        nkey = self._make_key(key, version)
        encoded_value = self._encode(value)
        self._pipeline_adapter.zrank(nkey, encoded_value)
        self._decoders.append(self._noop)  # Returns int or None
        return self

    def zrem(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
    ) -> Self:
        """Queue ZREM command (remove members)."""
        nkey = self._make_key(key, version)
        encoded_values = [self._encode(value) for value in values]
        self._pipeline_adapter.zrem(nkey, *encoded_values)
        self._decoders.append(self._noop)  # Returns count removed
        return self

    def zremrangebyscore(
        self,
        key: KeyT,
        min: float | str,
        max: float | str,
        version: int | None = None,
    ) -> Self:
        """Queue ZREMRANGEBYSCORE command (remove by score range)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.zremrangebyscore(nkey, min, max)
        self._decoders.append(self._noop)  # Returns count removed
        return self

    def zremrangebyrank(
        self,
        key: KeyT,
        start: int,
        end: int,
        version: int | None = None,
    ) -> Self:
        """Queue ZREMRANGEBYRANK command (remove by rank range)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.zremrangebyrank(nkey, start, end)
        self._decoders.append(self._noop)  # Returns count removed
        return self

    def zrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        *,
        withscores: bool = False,
        score_cast_func: _type = float,
        version: int | None = None,
    ) -> Self:
        """Queue ZREVRANGE command (get members by index, high to low)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.zrevrange(
            nkey,
            start,
            end,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )
        self._decoders.append(self._make_zset_decoder(withscores=withscores))
        return self

    def zrevrangebyscore(
        self,
        key: KeyT,
        max: float | str,
        min: float | str,
        start: int | None = None,
        num: int | None = None,
        *,
        withscores: bool = False,
        score_cast_func: _type = float,
        version: int | None = None,
    ) -> Self:
        """Queue ZREVRANGEBYSCORE command (get by score, high to low)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.zrevrangebyscore(
            nkey,
            max,
            min,
            start=start,
            num=num,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )
        self._decoders.append(self._make_zset_decoder(withscores=withscores))
        return self

    def zscore(
        self,
        key: KeyT,
        value: Any,
        version: int | None = None,
    ) -> Self:
        """Queue ZSCORE command (get member's score)."""
        nkey = self._make_key(key, version)
        encoded_value = self._encode(value)
        self._pipeline_adapter.zscore(nkey, encoded_value)
        self._decoders.append(self._noop)  # Returns float or None
        return self

    def zrevrank(
        self,
        key: KeyT,
        value: Any,
        version: int | None = None,
    ) -> Self:
        """Queue ZREVRANK command (get rank, high to low)."""
        nkey = self._make_key(key, version)
        encoded_value = self._encode(value)
        self._pipeline_adapter.zrevrank(nkey, encoded_value)
        self._decoders.append(self._noop)  # Returns int or None
        return self

    def zmscore(
        self,
        key: KeyT,
        *members: Any,
        version: int | None = None,
    ) -> Self:
        """Queue ZMSCORE command (get multiple members' scores)."""
        nkey = self._make_key(key, version)
        encoded_members = [self._encode(member) for member in members]
        self._pipeline_adapter.zmscore(nkey, encoded_members)
        self._decoders.append(self._noop)  # Returns list[float | None]
        return self

    # -------------------------------------------------------------------------
    # Stream operations
    # -------------------------------------------------------------------------

    def xadd(
        self,
        key: KeyT,
        fields: dict[str, Any],
        entry_id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue XADD command (add entry to stream)."""
        nkey = self._make_key(key, version)
        encoded_fields = {k: self._encode(v) for k, v in fields.items()}
        self._pipeline_adapter.xadd(
            nkey,
            encoded_fields,
            id=entry_id,
            maxlen=maxlen,
            approximate=approximate,
            nomkstream=nomkstream,
            minid=minid,
            limit=limit,
        )
        self._decoders.append(self._decode_entry_id)
        return self

    def xlen(self, key: KeyT, version: int | None = None) -> Self:
        """Queue XLEN command (get stream length)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xlen(nkey)
        self._decoders.append(self._noop)
        return self

    def xrange(
        self,
        key: KeyT,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue XRANGE command (get entries in ascending order)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xrange(nkey, min=start, max=end, count=count)
        self._decoders.append(self._decode_stream_entries)
        return self

    def xrevrange(
        self,
        key: KeyT,
        end: str = "+",
        start: str = "-",
        count: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue XREVRANGE command (get entries in descending order)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xrevrange(nkey, max=end, min=start, count=count)
        self._decoders.append(self._decode_stream_entries)
        return self

    def xread(
        self,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue XREAD command (read from streams)."""
        key_map: dict[str, KeyT] = {}
        nstreams: dict[KeyT, str] = {}
        for k, v in streams.items():
            nk = self._make_key(k, version)
            key_map[nk if isinstance(nk, str) else str(nk)] = k
            nstreams[nk] = v
        self._pipeline_adapter.xread(nstreams, count=count, block=block)
        self._decoders.append(self._make_stream_key_decoder(key_map))
        return self

    def xtrim(
        self,
        key: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: str | None = None,
        limit: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue XTRIM command (trim stream)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xtrim(nkey, maxlen=maxlen, approximate=approximate, minid=minid, limit=limit)
        self._decoders.append(self._noop)
        return self

    def xdel(self, key: KeyT, *entry_ids: str, version: int | None = None) -> Self:
        """Queue XDEL command (delete stream entries)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xdel(nkey, *entry_ids)
        self._decoders.append(self._noop)
        return self

    def xinfo_stream(self, key: KeyT, full: bool = False, version: int | None = None) -> Self:
        """Queue XINFO STREAM command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xinfo_stream(nkey, full=full)
        self._decoders.append(self._noop)
        return self

    def xinfo_groups(self, key: KeyT, version: int | None = None) -> Self:
        """Queue XINFO GROUPS command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xinfo_groups(nkey)
        self._decoders.append(self._noop)
        return self

    def xinfo_consumers(self, key: KeyT, group: str, version: int | None = None) -> Self:
        """Queue XINFO CONSUMERS command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xinfo_consumers(nkey, group)
        self._decoders.append(self._noop)
        return self

    def xgroup_create(
        self,
        key: KeyT,
        group: str,
        entry_id: str = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue XGROUP CREATE command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xgroup_create(nkey, group, entry_id, mkstream=mkstream, entries_read=entries_read)
        self._decoders.append(self._noop)
        return self

    def xgroup_destroy(self, key: KeyT, group: str, version: int | None = None) -> Self:
        """Queue XGROUP DESTROY command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xgroup_destroy(nkey, group)
        self._decoders.append(self._noop)
        return self

    def xgroup_setid(
        self,
        key: KeyT,
        group: str,
        entry_id: str,
        entries_read: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue XGROUP SETID command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xgroup_setid(nkey, group, entry_id, entries_read=entries_read)
        self._decoders.append(self._noop)
        return self

    def xgroup_delconsumer(self, key: KeyT, group: str, consumer: str, version: int | None = None) -> Self:
        """Queue XGROUP DELCONSUMER command."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xgroup_delconsumer(nkey, group, consumer)
        self._decoders.append(self._noop)
        return self

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[KeyT, str],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
        version: int | None = None,
    ) -> Self:
        """Queue XREADGROUP command (read as consumer group member)."""
        key_map: dict[str, KeyT] = {}
        nstreams: dict[KeyT, str] = {}
        for k, v in streams.items():
            nk = self._make_key(k, version)
            key_map[nk if isinstance(nk, str) else str(nk)] = k
            nstreams[nk] = v
        self._pipeline_adapter.xreadgroup(group, consumer, nstreams, count=count, block=block, noack=noack)
        self._decoders.append(self._make_stream_key_decoder(key_map))
        return self

    def xack(self, key: KeyT, group: str, *entry_ids: str, version: int | None = None) -> Self:
        """Queue XACK command (acknowledge messages)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xack(nkey, group, *entry_ids)
        self._decoders.append(self._noop)
        return self

    def xpending(
        self,
        key: KeyT,
        group: str,
        start: str | None = None,
        end: str | None = None,
        count: int | None = None,
        consumer: str | None = None,
        idle: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue XPENDING command (get pending entries info)."""
        nkey = self._make_key(key, version)
        if start is not None and end is not None and count is not None:
            kwargs: dict[str, Any] = {}
            if consumer is not None:
                kwargs["consumername"] = consumer
            if idle is not None:
                kwargs["idle"] = idle
            self._pipeline_adapter.xpending_range(nkey, group, min=start, max=end, count=count, **kwargs)
        else:
            self._pipeline_adapter.xpending(nkey, group)
        self._decoders.append(self._noop)
        return self

    def xclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        entry_ids: list[str],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
        version: int | None = None,
    ) -> Self:
        """Queue XCLAIM command (claim pending messages)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xclaim(
            nkey,
            group,
            consumer,
            min_idle_time,
            entry_ids,
            idle=idle,
            time=time,
            retrycount=retrycount,
            force=force,
            justid=justid,
        )
        if justid:
            self._decoders.append(self._noop)
        else:
            self._decoders.append(self._decode_stream_entries)
        return self

    def xautoclaim(
        self,
        key: KeyT,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
        version: int | None = None,
    ) -> Self:
        """Queue XAUTOCLAIM command (auto-claim idle messages)."""
        nkey = self._make_key(key, version)
        self._pipeline_adapter.xautoclaim(
            nkey,
            group,
            consumer,
            min_idle_time,
            start_id=start_id,
            count=count,
            justid=justid,
        )
        self._decoders.append(self._make_xautoclaim_decoder(justid=justid))
        return self

    def _make_xautoclaim_decoder(
        self,
        *,
        justid: bool,
    ) -> Callable[[Any], tuple[str, list, list[str]]]:
        """Create decoder for XAUTOCLAIM result.

        redis-py returns different formats based on justid:
        - justid=False: [next_id, [[id, fields], ...], [deleted]] (3-tuple)
        - justid=True:  [id1, id2, ...] (flat list of claimed IDs)
        """

        def decode(result: Any) -> tuple[str, list, list[str]]:
            if justid:
                # redis-py returns flat list of claimed IDs (strips next_id/deleted)
                claimed = [r.decode() if isinstance(r, bytes) else r for r in result]
                return ("", claimed, [])

            next_id = result[0].decode() if isinstance(result[0], bytes) else result[0]
            deleted = [d.decode() if isinstance(d, bytes) else d for d in result[2]] if len(result) > 2 else []
            return (next_id, self._decode_stream_entries(result[1]), deleted)

        return decode

    # -------------------------------------------------------------------------
    # Lua Script Operations
    # -------------------------------------------------------------------------

    def eval_script(
        self,
        script: str,
        *,
        keys: Sequence[Any] = (),
        args: Sequence[Any] = (),
        pre_hook: Callable[[ScriptHelpers, Sequence[Any], Sequence[Any]], tuple[list[Any], list[Any]]] | None = None,
        post_hook: Callable[[ScriptHelpers, Any], Any] | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue a Lua script for pipelined execution."""
        # Determine version for key prefixing
        v = version if version is not None else self._version
        if v is None:
            v = getattr(self, "_cache_version", None)

        # Create helpers for pre/post processing
        helpers = ScriptHelpers(
            make_key=self._make_key,
            encode=self._cache.encode,
            decode=self._cache.decode,
            version=v,
        )

        proc_keys: list[Any] = list(keys)
        proc_args: list[Any] = list(args)
        if pre_hook is not None:
            proc_keys, proc_args = pre_hook(helpers, proc_keys, proc_args)

        # Queue EVAL command using execute_command for cluster compatibility
        # Note: We use EVAL instead of EVALSHA in pipelines because:
        # 1. EVALSHA is blocked in Redis Cluster mode pipelines
        # 2. ClusterPipeline.eval() has a different signature than regular Pipeline.eval()
        # 3. execute_command works uniformly across both pipeline types
        self._pipeline_adapter.execute_command("EVAL", script, len(proc_keys), *proc_keys, *proc_args)

        if post_hook is not None:

            def make_decoder(ph: Any, h: ScriptHelpers) -> Any:
                def decoder(result: Any) -> Any:
                    return ph(h, result)

                return decoder

            self._decoders.append(make_decoder(post_hook, helpers))
        else:
            self._decoders.append(self._noop)

        return self


class AsyncPipeline(Pipeline):
    """Async sibling of :class:`Pipeline` — same chainable API, awaitable ``execute()``.

    Driver async pipelines (``redis.asyncio`` ``Pipeline``, glide async ``Batch``,
    redis-rs ``apipeline_exec``) all expose the same chainable command surface
    as their sync counterparts; only execution does I/O. So this subclass
    inherits every queueing method from :class:`Pipeline` and only overrides
    the lifecycle (``__aenter__/__aexit__``) and ``execute()`` to be async.
    """

    if TYPE_CHECKING:
        # Narrow the parent's ``_pipeline_adapter`` to the async protocol so
        # type checkers know ``await adapter.execute()`` / ``adapter.reset()``
        # are valid. The runtime attribute is set by the parent ``__init__``.
        _pipeline_adapter: RespAsyncPipelineProtocol  # type: ignore[assignment]

    def __init__(
        self,
        cache: Any,
        pipeline_adapter: RespAsyncPipelineProtocol,
        version: int | None = None,
    ) -> None:
        """Initialize the wrapped async pipeline."""
        super().__init__(cache, pipeline_adapter, version=version)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

    async def __aenter__(self) -> Self:
        """Enter async context manager."""
        return self

    async def __aexit__(self, *args: object) -> None:
        """Exit async context manager, resetting the underlying pipeline."""
        await self._pipeline_adapter.reset()
        self._decoders.clear()

    async def execute(self) -> list[Any]:  # type: ignore[override]
        """Execute all queued commands asynchronously and decode the results."""
        results = await self._pipeline_adapter.execute()
        decoders = self._decoders
        self._decoders = []
        return [decoder(result) for result, decoder in zip(results, decoders, strict=True)]


__all__ = ["Pipeline"]
