"""Helpers shared by the cache-layer test modules."""

import math
import os
import signal
import socket
import threading
import time
from collections import defaultdict
from contextlib import suppress
from typing import TYPE_CHECKING, Any, NamedTuple

import pytest

from django_cachex.cache import RedisCache

if TYPE_CHECKING:
    from collections.abc import Callable


def make_cache(*, key_prefix: str = "", **options: Any) -> RedisCache:
    """Build a :class:`RedisCache` from ``OPTIONS`` kwargs; ``adapter`` is lazy, so it never connects."""
    return RedisCache(
        server="redis://localhost:6379/0",
        params={
            "OPTIONS": {name: value for name, value in options.items() if value is not None},
            "KEY_PREFIX": key_prefix,
        },
    )


def run_forked_while_held(lock: Any, call: Callable[[], object]) -> None:
    """Fork while another thread holds ``lock``, and fail unless ``call`` returns in the child.

    Only the forking thread survives a fork, so the lock stays held in the child
    unless the code under test replaces it there.
    """
    holding, release = threading.Event(), threading.Event()

    def hold_the_lock() -> None:
        with lock:
            holding.set()
            release.wait(10)

    holder = threading.Thread(target=hold_the_lock, daemon=True)
    holder.start()
    assert holding.wait(5)
    pid = os.fork()
    if pid == 0:
        code = 1
        try:
            call()
            code = 0
        finally:
            os._exit(code)
    release.set()
    holder.join(5)
    deadline = time.monotonic() + 10
    while (waited := os.waitpid(pid, os.WNOHANG)) == (0, 0) and time.monotonic() < deadline:
        time.sleep(0.05)
    if waited == (0, 0):
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        pytest.fail("the forked child deadlocked on a lock held at the fork")
    assert os.waitstatus_to_exitcode(waited[1]) == 0


def wait_until(condition: Callable[[], object], timeout: float = 5.0) -> None:
    """Poll ``condition`` until it holds, and fail the test if it does not within ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail(f"the condition did not hold within {timeout} s")
        time.sleep(0.01)


class TcpProxy:
    """Forward connections to a server, and hold up, cut or slow them down on demand.

    ``reply_delay`` is the seconds each chunk of replies waits, on every connection but a tracking one.
    """

    def __init__(self, host: str, port: int, *, reply_delay: float = 0.0) -> None:
        self._target = (host, port)
        self._reply_delay = reply_delay
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port: int = self._listener.getsockname()[1]
        self._flowing = threading.Event()
        self._flowing.set()
        self._refusing = threading.Event()
        self._sockets: list[socket.socket] = []
        threading.Thread(target=self._accept, daemon=True).start()

    def pause(self) -> None:
        """Hold the bytes of tracking connections in both directions until :meth:`resume`."""
        self._flowing.clear()

    def resume(self) -> None:
        self._flowing.set()

    def cut(self) -> None:
        """Close every connection, and each new one as it opens, until :meth:`restore`."""
        self._refusing.set()
        for sock in tuple(self._sockets):
            with suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)

    def restore(self) -> None:
        self._refusing.clear()

    def close(self) -> None:
        self._flowing.set()
        for sock in (self._listener, *self._sockets):
            with suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            sock.close()

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            if self._refusing.is_set():
                client.close()
                continue
            try:
                upstream = socket.create_connection(self._target)
            except OSError:
                client.close()
                continue
            self._sockets += (client, upstream)
            tracking = threading.Event()
            threading.Thread(target=self._pump, args=(client, upstream, tracking, True), daemon=True).start()
            threading.Thread(target=self._pump, args=(upstream, client, tracking, False), daemon=True).start()

    def _pump(self, source: socket.socket, sink: socket.socket, tracking: threading.Event, upstream: bool) -> None:
        with suppress(OSError):
            while data := source.recv(65536):
                if upstream and b"TRACKING" in data:
                    tracking.set()
                if tracking.is_set():
                    self._flowing.wait()
                elif not upstream and self._reply_delay:
                    time.sleep(self._reply_delay)
                sink.sendall(data)
        for sock in (source, sink):
            with suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)


class Op(NamedTuple):
    """One read or write of one key, timed with ``time.monotonic()``.

    ``value`` is a unique int, None for a missing key, or the bool of ``has_key()``. ``own`` marks a write
    through the cache under test, which that cache's reads have to see as soon as it has returned.
    """

    key: str
    value: Any
    start: float
    end: float
    own: bool = False


_BEFORE_THE_FIRST_WRITE = Op("", None, -math.inf, -math.inf)


def stale_reads(writes: list[Op], reads: list[Op], max_staleness: float) -> list[Op]:
    """The reads with a value no write can have left, or one that a write they had to see overwrote.

    A read has to see the writes through the same cache that returned before it started, and all writes that
    returned more than ``max_staleness`` before it started. A write overwrites those that returned before it started.
    """
    by_key: defaultdict[str, list[Op]] = defaultdict(list)
    for write in writes:
        by_key[write.key].append(write)
    origins = {(write.key, write.value): write for write in writes if write.value is not None}
    stale = []
    for read in reads:
        history = by_key[read.key]
        bound = max(
            (w.start for w in history if w.end < read.start - max_staleness or (w.own and w.end < read.start)),
            default=-math.inf,
        )
        if read.value is True:
            sources = [w for w in history if w.value is not None]
        elif read.value is None or read.value is False:
            sources = [_BEFORE_THE_FIRST_WRITE, *(w for w in history if w.value is None)]
        else:
            sources = [origins[read.key, read.value]] if (read.key, read.value) in origins else []
        ends = [w.end for w in sources if w.start <= read.end]
        if not ends or max(ends) < bound:
            stale.append(read)
    return stale
