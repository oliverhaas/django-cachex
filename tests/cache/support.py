"""Helpers shared by the cache-layer test modules."""

import os
import signal
import threading
import time
from typing import TYPE_CHECKING, Any

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
