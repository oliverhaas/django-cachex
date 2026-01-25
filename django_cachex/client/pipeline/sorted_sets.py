from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from django_cachex.types import KeyT


class SortedSetPipelineMixin:
    """Pipeline mixin providing Redis sorted set (ZSET) operations."""

    # Type hints for base class attributes
    _pipeline: Any
    _decoders: list
    _client: Any
    _noop: Any
    _make_key: Any
    _encode: Any

    def _decode_zset_members(self, value: list[bytes]) -> list[Any]:
        """Decode sorted set members (without scores)."""
        return [self._client.decode(member) for member in value]

    def _decode_zset_with_scores(self, value: list[tuple[bytes, float]]) -> list[tuple[Any, float]]:
        """Decode sorted set members with scores."""
        return [(self._client.decode(member), score) for member, score in value]

    def _make_zset_decoder(self, withscores: bool):
        """Create decoder based on whether scores are included."""
        if withscores:
            return self._decode_zset_with_scores
        return self._decode_zset_members

    def _decode_zpop(self, value: list[tuple[bytes, float]], count: int | None) -> Any:
        """Decode zpopmin/zpopmax result."""
        if not value:
            return None if count is None else []
        decoded = [(self._client.decode(member), score) for member, score in value]
        if count is None:
            return decoded[0] if decoded else None
        return decoded

    def zadd(
        self,
        key: KeyT,
        mapping: dict[Any, float],
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
        self._pipeline.zadd(
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
        self._pipeline.zcard(nkey)
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
        self._pipeline.zcount(nkey, min, max)
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
        self._pipeline.zincrby(nkey, amount, encoded_value)
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
        self._pipeline.zpopmax(nkey, count)
        # Capture count for decoder
        self._decoders.append(lambda x, c=count: self._decode_zpop(x, c))
        return self

    def zpopmin(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
    ) -> Self:
        """Queue ZPOPMIN command (pop lowest scoring members)."""
        nkey = self._make_key(key, version)
        self._pipeline.zpopmin(nkey, count)
        # Capture count for decoder
        self._decoders.append(lambda x, c=count: self._decode_zpop(x, c))
        return self

    def zrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: type = float,
        version: int | None = None,
    ) -> Self:
        """Queue ZRANGE command (get members by index range)."""
        nkey = self._make_key(key, version)
        self._pipeline.zrange(
            nkey,
            start,
            end,
            desc=desc,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )
        self._decoders.append(self._make_zset_decoder(withscores))
        return self

    def zrangebyscore(
        self,
        key: KeyT,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
        score_cast_func: type = float,
        version: int | None = None,
    ) -> Self:
        """Queue ZRANGEBYSCORE command (get members by score range)."""
        nkey = self._make_key(key, version)
        self._pipeline.zrangebyscore(
            nkey,
            min,
            max,
            start=start,
            num=num,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )
        self._decoders.append(self._make_zset_decoder(withscores))
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
        self._pipeline.zrank(nkey, encoded_value)
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
        self._pipeline.zrem(nkey, *encoded_values)
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
        self._pipeline.zremrangebyscore(nkey, min, max)
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
        self._pipeline.zremrangebyrank(nkey, start, end)
        self._decoders.append(self._noop)  # Returns count removed
        return self

    def zrevrange(
        self,
        key: KeyT,
        start: int,
        end: int,
        withscores: bool = False,
        score_cast_func: type = float,
        version: int | None = None,
    ) -> Self:
        """Queue ZREVRANGE command (get members by index, high to low)."""
        nkey = self._make_key(key, version)
        self._pipeline.zrevrange(
            nkey,
            start,
            end,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )
        self._decoders.append(self._make_zset_decoder(withscores))
        return self

    def zrevrangebyscore(
        self,
        key: KeyT,
        max: float | str,
        min: float | str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
        score_cast_func: type = float,
        version: int | None = None,
    ) -> Self:
        """Queue ZREVRANGEBYSCORE command (get by score, high to low)."""
        nkey = self._make_key(key, version)
        self._pipeline.zrevrangebyscore(
            nkey,
            max,
            min,
            start=start,
            num=num,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )
        self._decoders.append(self._make_zset_decoder(withscores))
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
        self._pipeline.zscore(nkey, encoded_value)
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
        self._pipeline.zrevrank(nkey, encoded_value)
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
        self._pipeline.zmscore(nkey, encoded_members)
        self._decoders.append(self._noop)  # Returns list[float | None]
        return self
