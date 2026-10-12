"""The tracking connection: its socket IO, the PINGs that confirm it, the liveness gate and its thread."""

import logging
import os
import random
import selectors
import socket
import ssl
import threading
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Final

from django_cachex.tracking.resp3 import NEED_MORE, ErrorReply, Parser, ProtocolError, Push, SimpleString
from django_cachex.tracking.store import clock

if TYPE_CHECKING:
    from django_cachex.tracking.store import LocalStore

logger = logging.getLogger("django_cachex.tracking")

_RECV_SIZE: Final = 65536
# Bytes one drain reads before it lets go of the IO lock.
_DRAIN_CAP: Final = 65536
# Seconds a PING can stay unanswered before the connection counts as dead.
_PING_TIMEOUT: Final = 5.0
_BACKOFF_MIN: Final = 0.1
_BACKOFF_MAX: Final = 5.0
_PING: Final = b"*1\r\n$4\r\nPING\r\n"
_WOULD_BLOCK: Final = (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError)


def _command(*args: bytes) -> bytes:
    parts = [b"*%d\r\n" % len(args)]
    for arg in args:
        parts += (b"$%d\r\n" % len(arg), arg, b"\r\n")
    return b"".join(parts)


def _backoff(delay: float) -> float:
    return min(max(delay * 2, _BACKOFF_MIN), _BACKOFF_MAX)


def _expect_ok(sock: socket.socket, parser: Parser) -> None:
    """Read the reply to CLIENT TRACKING with ``parser``, and raise unless it is +OK."""
    while (reply := parser.gets()) is NEED_MORE:
        data = sock.recv(_RECV_SIZE)
        if not data:
            msg = "The server closed the tracking connection."
            raise ConnectionError(msg)
        parser.feed(data)
    if not (isinstance(reply, SimpleString) and reply == b"OK"):
        msg = f"CLIENT TRACKING failed: {reply!r}"
        raise ConnectionError(msg)


class Listener:
    """One tracking connection, read by tracked reads and by its own thread."""

    def __init__(self, adapter: Any, prefix: str, max_staleness: float, store: LocalStore) -> None:
        self._adapter = adapter
        self._tracking_command = _command(
            b"CLIENT",
            b"TRACKING",
            b"ON",
            b"BCAST",
            b"PREFIX",
            prefix.encode("utf-8", "surrogateescape"),
        )
        self._max_staleness = max_staleness
        self._store = store

        # The IO lock guards the connection, the parser, the output buffer and the gate.
        self._io_lock = threading.Lock()
        self._conn: Any = None
        self._sock: Any = None
        self._parser = Parser()
        self._outbuf = bytearray()
        # Send time of the outstanding PING.
        self._ping_at: float | None = None
        # Send time of the newest PING, answered or not, or of CLIENT TRACKING.
        self._last_ping = 0.0
        # Send time of the newest PING answered with +PONG, or of CLIENT TRACKING.
        self._confirmed_at: float | None = None
        self._broken = False

        # Only the thread touches these.
        self._address: tuple[Any, Any] | None = None
        self._watched: Any = None
        self._watched_events = 0
        self._next_primary_check = 0.0
        self._primary_warned = False
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_w.setblocking(False)
        self._selector = selectors.DefaultSelector()
        self._selector.register(self._wake_r, selectors.EVENT_READ)
        self._stopping = False
        self._thread = threading.Thread(target=self._run, name="django-cachex-tracking", daemon=True)
        self.reconnects = 0
        self._handshakes = 0

    def drain(self) -> bool:
        """Process the frames that arrived; True when copies can be served now."""
        with self._io_lock:
            if self._sock is None or self._broken:
                return False
            had_ping = self._ping_at is not None
            try:
                capped = self._receive()
            except Exception:
                logger.debug("The tracking connection failed in a read.", exc_info=True)
                self._mark_broken()
                return False
            except BaseException:
                self._mark_broken()
                raise
            # Wake the thread for bytes a capped read left, or to schedule the next PING after a PONG read here.
            if capped or (had_ping and self._ping_at is None):
                self._wake()
            if capped:
                return False
            return self._confirmed_at is not None and clock() - self._confirmed_at <= self._max_staleness

    def status(self) -> tuple[bool, float | None]:
        """Whether the connection is up, and the time it was last confirmed."""
        with self._io_lock:
            return self._sock is not None and not self._broken, self._confirmed_at

    def _receive(self) -> bool:
        """Read and process until the socket would block; True when the cap stopped it first."""
        sock = self._sock
        total = 0
        while total < _DRAIN_CAP:
            try:
                data = sock.recv(min(_RECV_SIZE, _DRAIN_CAP - total))
            except _WOULD_BLOCK:
                return False
            if not data:
                msg = "The server closed the tracking connection."
                raise ConnectionError(msg)
            total += len(data)
            self._parser.feed(data)
            self._process()
        return True

    def _process(self) -> None:
        store = self._store
        while (frame := self._parser.gets()) is not NEED_MORE:
            if isinstance(frame, Push):
                if len(frame) == 2 and frame[0] == b"invalidate":
                    if frame[1] is None:
                        store.flush()
                    else:
                        store.invalidate([key.decode("utf-8", "surrogateescape") for key in frame[1]])
                continue
            if self._ping_at is None:
                msg = f"Unexpected frame on the tracking connection: {frame!r}"
                raise ProtocolError(msg)
            # An error reply, such as -BUSY or -LOADING, answers the PING without confirming it.
            if isinstance(frame, SimpleString) and frame == b"PONG":
                self._confirmed_at = self._ping_at
            elif not isinstance(frame, ErrorReply):
                msg = f"Unexpected reply to PING: {frame!r}"
                raise ProtocolError(msg)
            self._ping_at = None

    def _send(self) -> None:
        while self._outbuf:
            try:
                sent = self._sock.send(self._outbuf)
            except _WOULD_BLOCK:
                return
            del self._outbuf[:sent]

    def _mark_broken(self) -> None:
        self._broken = True
        self._store.deactivate()
        self._wake()

    def _wake(self) -> None:
        with suppress(OSError):
            self._wake_w.send(b"\0")

    def start(self) -> None:
        """Start the thread; if it cannot start, close the wake sockets and the selector and raise."""
        try:
            self._thread.start()
        except BaseException:
            self.stop()
            raise

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the thread and close the connection."""
        self._stopping = True
        self._wake()
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout)
        self._close()
        with suppress(Exception):
            self._selector.close()
        for sock in (self._wake_r, self._wake_w):
            with suppress(OSError):
                sock.close()

    def discard_in_child(self) -> None:
        """Let go of the parent's descriptors in a forked child, touching neither the connection nor a lock."""
        self._stopping = True
        sock = self._sock
        if sock is not None:
            # No shutdown() and no TLS close_notify: the parent still reads this connection.
            with suppress(OSError):
                os.close(sock.detach())
        self._conn = self._sock = None
        # Never unregister: the epoll instance is shared with the parent.
        with suppress(Exception):
            self._selector.close()
        for wake in (self._wake_r, self._wake_w):
            with suppress(OSError):
                wake.close()

    def _run(self) -> None:  # noqa: C901
        delay = 0.0
        failing = False
        connected_at: float | None = None
        while not self._stopping:
            if self._conn is None:
                if delay and self._sleep(delay * random.uniform(0.5, 1.0)):  # noqa: S311
                    break
                try:
                    connected_at = self._connect()
                except Exception as e:  # noqa: BLE001
                    if not failing:
                        logger.warning("Client tracking cannot connect, reads go to the server: %s", e)
                        failing = True
                    delay = _backoff(delay)
                continue
            try:
                self._step()
            except Exception:
                logger.warning("The client tracking thread failed, reconnecting.", exc_info=True)
                with self._io_lock:
                    self._mark_broken()
            # A connection counts as recovered when a PING confirms it, not at its handshake.
            # So a server that drops each new connection is retried with backoff.
            confirmed = self._confirmed_at != connected_at
            if self._broken:
                self._close()
                self._clear_wake()
                if not failing:
                    logger.warning("Client tracking lost its connection, reads go to the server until it reconnects.")
                    failing = True
                delay = 0.0 if confirmed else _backoff(delay)
            elif confirmed and (failing or delay):
                if failing:
                    logger.info("Client tracking reconnected.")
                failing = False
                delay = 0.0
        self._close()

    def _connect(self) -> float:
        """Open a connection, turn tracking on and install it; return the CLIENT TRACKING send time."""
        conn = self._adapter.tracking_connection()
        try:
            sock = conn._sock
            parser = Parser()
            sent_at = clock()
            sock.sendall(self._tracking_command)
            # Read with this parser, so a push that shares a segment with the +OK cannot stay in the driver's.
            _expect_ok(sock, parser)
            sock.setblocking(False)
        except BaseException:
            with suppress(Exception):
                conn.disconnect()
            raise
        with self._io_lock:
            self._conn, self._sock, self._parser = conn, sock, parser
            self._outbuf.clear()
            self._ping_at = None
            self._confirmed_at = self._last_ping = sent_at
            self._broken = False
            self._store.activate()
            try:
                self._process()
            except Exception:
                logger.debug("The tracking connection sent a bad frame.", exc_info=True)
                self._mark_broken()
        self._address = (getattr(conn, "host", None), getattr(conn, "port", None))
        self._next_primary_check = clock() + self._max_staleness
        self._handshakes += 1
        if self._handshakes > 1:
            self.reconnects += 1
        return sent_at

    def _step(self) -> None:
        """PING when due, check the primary when due, wait for the socket, and drain it."""
        now = clock()
        with self._io_lock:
            if self._broken:
                return
            if self._ping_at is not None:
                if now - self._ping_at > _PING_TIMEOUT:
                    logger.debug("A PING on the tracking connection went unanswered.")
                    self._mark_broken()
                    return
                due = self._ping_at + _PING_TIMEOUT
            else:
                # Paced by the last PING, not the last PONG, so error replies cannot make PINGs back to back.
                due = self._last_ping + self._max_staleness / 2
                if now >= due:
                    self._ping_at = self._last_ping = now
                    self._outbuf += _PING
                    due = now + _PING_TIMEOUT
            try:
                self._send()
            except Exception:
                logger.debug("The tracking connection failed in a write.", exc_info=True)
                self._mark_broken()
                return
            sock = self._sock
            writing = bool(self._outbuf)
        if now >= self._next_primary_check:
            self._next_primary_check = now + self._max_staleness
            self._check_primary()
        self._watch(sock, writing=writing)
        timeout = max(0.0, min(due, self._next_primary_check) - clock())
        for key, _ in self._selector.select(timeout):
            if key.fileobj is self._wake_r:
                self._clear_wake()
        self._drain()

    def _drain(self) -> None:
        while True:
            with self._io_lock:
                if self._sock is None or self._broken:
                    return
                try:
                    capped = self._receive()
                    self._send()
                except Exception:
                    logger.debug("The tracking connection failed.", exc_info=True)
                    self._mark_broken()
                    return
            if not capped:
                return

    def _check_primary(self) -> None:
        try:
            primary = self._adapter.tracking_primary()
        except Exception as e:  # noqa: BLE001
            # Sentinel unreachable: the connection can still be good, so keep it.
            if not self._primary_warned:
                logger.warning("Client tracking cannot ask Sentinel for the primary: %s", e)
                self._primary_warned = True
            return
        self._primary_warned = False
        if primary is not None and tuple(primary) != self._address:
            logger.info("Sentinel names %s as the primary, reconnecting client tracking.", primary)
            with self._io_lock:
                self._mark_broken()

    def _watch(self, sock: Any, *, writing: bool) -> None:
        events = selectors.EVENT_READ | (selectors.EVENT_WRITE if writing else 0)
        if self._watched is not sock:
            self._unwatch()
            self._selector.register(sock, events)
            self._watched, self._watched_events = sock, events
        elif events != self._watched_events:
            self._selector.modify(sock, events)
            self._watched_events = events

    def _unwatch(self) -> None:
        if self._watched is not None:
            with suppress(Exception):
                self._selector.unregister(self._watched)
            self._watched = None

    def _sleep(self, seconds: float) -> bool:
        """Wait ``seconds`` or until woken; True when stopping."""
        with suppress(OSError):
            self._selector.select(seconds)
        self._clear_wake()
        return self._stopping

    def _clear_wake(self) -> None:
        with suppress(OSError):
            while self._wake_r.recv(4096):
                pass

    def _close(self) -> None:
        with self._io_lock:
            conn = self._conn
            self._conn = self._sock = None
            self._broken = False
            self._parser = Parser()
            self._outbuf.clear()
            self._ping_at = None
            self._store.deactivate()
        self._unwatch()
        if conn is not None:
            with suppress(Exception):
                conn.disconnect()
