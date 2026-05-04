"""Response shape parsers for the Rust pipeline class.

The driver returns raw ``redis::Value`` already converted to Python natives
(bytes / int / list / dict). The pipeline class applies one of these
parsers per command to normalize the shape — folding RESP3 maps and RESP2
flat-pair-lists into a consistent representation, coercing bytes scores to
floats, etc.

These functions are public-by-name (Rust references them via getattr at
queue time). Module-level so they survive process forks without
re-resolving.
"""

from typing import Any


def to_float_or_none(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, (bytes, bytearray, str)):
        return float(v)
    return v


def list_to_float_or_none(v: Any) -> list[float | None]:
    return [to_float_or_none(x) for x in v]


def bytes_or_none_to_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bytes):
        return v.decode()
    return v


def hgetall(v: Any) -> dict[bytes, bytes]:
    """RESP3 HGETALL is a Map; older protocols send a flat array. Normalize to dict."""
    if isinstance(v, dict):
        return v
    if v is None:
        return {}
    items = list(v)
    return {items[i]: items[i + 1] for i in range(0, len(items), 2)}


def zset_with_scores(v: Any) -> list[tuple[bytes, float]]:
    """ZRANGE/ZREVRANGE WITHSCORES — handle RESP3 nested arrays and RESP2 flat lists."""
    if not v:
        return []
    if isinstance(v, dict):
        return [(member, float(score)) for member, score in v.items()]
    first = v[0]
    if isinstance(first, (list, tuple)):
        return [(pair[0], float(pair[1])) for pair in v]
    return [(v[i], float(v[i + 1])) for i in range(0, len(v), 2)]


def stream_entry(entry: Any) -> tuple[str, dict[str, bytes]]:
    """One stream entry: [id, [k1,v1,k2,v2,...]] → (id_str, {k_str: v_bytes})."""
    eid = entry[0]
    if isinstance(eid, bytes):
        eid = eid.decode()
    fields = entry[1]
    if isinstance(fields, dict):
        d = {(k.decode() if isinstance(k, bytes) else k): v for k, v in fields.items()}
    elif fields is None:
        d = {}
    else:
        flat = list(fields)
        d = {(flat[i].decode() if isinstance(flat[i], bytes) else flat[i]): flat[i + 1] for i in range(0, len(flat), 2)}
    return (eid, d)


def stream_entries(v: Any) -> list[tuple[str, dict[str, bytes]]]:
    if v is None:
        return []
    return [stream_entry(e) for e in v]


def stream_read(v: Any) -> list[tuple[Any, list[tuple[str, dict[str, bytes]]]]] | None:
    """XREAD/XREADGROUP — list of (stream_key, entries). RESP3 may yield a Map."""
    if v is None or v in ([], {}):
        return None
    if isinstance(v, dict):
        return [(k, stream_entries(entries)) for k, entries in v.items()]
    return [(stream[0], stream_entries(stream[1])) for stream in v]


def xpending_range(v: Any) -> list[list[Any]]:
    """Range form: list of [id, consumer, idle_ms, deliveries]."""
    if v is None:
        return []
    return [list(entry) for entry in v]


def xinfo_dict(v: Any) -> dict[str, Any]:
    """XINFO STREAM — RESP3 Map, RESP2 flat list of alternating k/v."""
    if isinstance(v, dict):
        return {(k.decode() if isinstance(k, bytes) else k): val for k, val in v.items()}
    if v is None:
        return {}
    flat = list(v)
    return {(flat[i].decode() if isinstance(flat[i], bytes) else flat[i]): flat[i + 1] for i in range(0, len(flat), 2)}


def xinfo_dict_list(v: Any) -> list[dict[str, Any]]:
    if v is None:
        return []
    return [xinfo_dict(item) for item in v]


def xautoclaim(v: Any) -> list[Any]:
    """XAUTOCLAIM returns [next_id, [entries], [deleted_ids]]."""
    out: list[Any] = []
    if v is None:
        return out
    items = list(v)
    if items:
        out.append(items[0])
    if len(items) > 1:
        out.append(stream_entries(items[1]))
    if len(items) > 2:
        out.append(items[2])
    return out


async def apipeline_execute(awaitable: Any, parsers: list[Any]) -> list[Any]:
    """Await ``apipeline_exec`` then apply per-command parsers element-wise."""
    raw = await awaitable
    out: list[Any] = []
    for value, parser in zip(raw, parsers, strict=True):
        out.append(parser(value) if parser is not None else value)
    return out
