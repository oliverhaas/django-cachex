"""An incremental RESP3 parser: a frame split across reads resumes where it stopped."""

from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from collections.abc import Callable

# Longest header or simple line before its CRLF. Real ones are a few bytes.
_MAX_LINE: Final = 64 * 1024


class ProtocolError(Exception):
    """The stream is not valid RESP3."""


class SimpleString(bytes):
    """A ``+`` reply, such as ``+OK`` or ``+PONG``."""


class ErrorReply(bytes):
    """A ``-`` or ``!`` reply."""


class Push(list[Any]):
    """A ``>`` frame, such as ``>2 invalidate [keys]``."""


class Map(list[Any]):
    """A ``%`` frame, as a list of ``(key, value)`` pairs."""


class Set(list[Any]):
    """A ``~`` frame."""


NEED_MORE: Final = object()
_SKIP: Final = object()


def _null(line: bytes) -> None:
    if line:
        raise ValueError(line)


def _boolean(line: bytes) -> bool:
    if line not in {b"t", b"f"}:
        raise ValueError(line)
    return line == b"t"


_SCALARS: Final[dict[bytes, Callable[[bytes], Any]]] = {
    b"+": SimpleString,
    b"-": ErrorReply,
    b":": int,
    b"_": _null,
    b"#": _boolean,
    b",": float,
    b"(": int,
}
_BLOBS: Final = frozenset({b"$", b"=", b"!"})
_AGGREGATES: Final = frozenset({b"*", b">", b"~", b"%", b"|"})


def _scalar(kind: bytes, line: bytes) -> Any:
    convert = _SCALARS.get(kind)
    if convert is None:
        msg = f"Unknown RESP3 type {kind!r}"
        raise ProtocolError(msg)
    return convert(line)


class _Aggregate:
    __slots__ = ("items", "kind", "remaining")

    def __init__(self, kind: bytes, remaining: int) -> None:
        self.kind = kind
        self.remaining = remaining
        self.items: list[Any] = []

    def build(self) -> Any:
        items = self.items
        if self.kind == b"*":
            return items
        if self.kind == b">":
            return Push(items)
        if self.kind == b"~":
            return Set(items)
        if self.kind == b"%":
            return Map(zip(items[::2], items[1::2], strict=True))
        # An attribute describes the frame after it, and nothing here reads it.
        return _SKIP


class Parser:
    """Parse frames from bytes fed in any chunks."""

    def __init__(self) -> None:
        self._buf = bytearray()
        self._pos = 0
        # Where the line that _line() read ends.
        self._next = 0
        # Aggregates still waiting for elements, innermost last.
        self._stack: list[_Aggregate] = []

    def feed(self, data: bytes) -> None:
        self._buf += data

    def gets(self) -> Any:
        """Return the next complete frame, or :data:`NEED_MORE`."""
        while True:
            item = self._read_item()
            if item is NEED_MORE:
                del self._buf[: self._pos]
                self._pos = 0
                return NEED_MORE
            item = self._attach(item)
            if item is not NEED_MORE and item is not _SKIP:
                return item

    def _attach(self, item: Any) -> Any:
        """Add ``item`` to the open aggregates; return a completed top-level frame or NEED_MORE."""
        while True:
            if item is _SKIP:
                return NEED_MORE if self._stack else _SKIP
            if not self._stack:
                return item
            top = self._stack[-1]
            top.items.append(item)
            top.remaining -= 1
            if top.remaining:
                return NEED_MORE
            self._stack.pop()
            item = top.build()

    def _line(self) -> bytes | None:
        buf = self._buf
        end = buf.find(b"\r\n", self._pos)
        if end < 0:
            if len(buf) - self._pos > _MAX_LINE:
                msg = "RESP3 line too long"
                raise ProtocolError(msg)
            return None
        line = bytes(buf[self._pos + 1 : end])
        self._next = end + 2
        return line

    def _read_item(self) -> Any:
        if self._pos >= len(self._buf):
            return NEED_MORE
        kind = bytes(self._buf[self._pos : self._pos + 1])
        line = self._line()
        if line is None:
            return NEED_MORE
        try:
            if kind in _BLOBS:
                return self._read_blob(kind, int(line))
            value = self._open(kind, int(line)) if kind in _AGGREGATES else _scalar(kind, line)
        except ValueError as e:
            msg = f"Bad RESP3 frame {kind!r}{line!r}"
            raise ProtocolError(msg) from e
        self._pos = self._next
        return value

    def _open(self, kind: bytes, count: int) -> Any:
        if count < 0:
            if count == -1 and kind == b"*":
                return None
            raise ValueError(count)
        if kind in b"%|":
            count *= 2
        aggregate = _Aggregate(kind, count)
        if not count:
            return aggregate.build()
        self._stack.append(aggregate)
        return _SKIP

    def _read_blob(self, kind: bytes, length: int) -> Any:
        if length < 0:
            if length == -1 and kind == b"$":
                self._pos = self._next
                return None
            raise ValueError(length)
        start = self._next
        end = start + length
        if len(self._buf) < end + 2:
            return NEED_MORE
        if self._buf[end : end + 2] != b"\r\n":
            msg = "RESP3 blob without its CRLF"
            raise ProtocolError(msg)
        data = bytes(self._buf[start:end])
        self._pos = end + 2
        if kind == b"!":
            return ErrorReply(data)
        if kind == b"=":
            # A verbatim string starts with a format such as "txt:".
            return data[4:]
        return data
