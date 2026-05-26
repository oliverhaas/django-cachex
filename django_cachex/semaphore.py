"""Weighted semaphores for django-cachex.

Two implementations share a common API:

- ``Semaphore`` / ``AsyncSemaphore`` (in-process, used by ``LocMemCache``):
  FIFO deque + ``threading.Lock`` for state.
- ``RedisSemaphore`` / ``RedisAsyncSemaphore`` (added in later tasks):
  Lua-script-via-EVAL on any RESP adapter.

The public top-level names ``Semaphore`` / ``AsyncSemaphore`` resolve to the
in-process classes; Redis-backed instances are constructed by the cache
factory methods (``cache.semaphore(...)`` / ``cache.asemaphore(...)``) and
are not exposed by name.
"""

from __future__ import annotations


class SemaphoreError(Exception):
    """Raised when a semaphore operation fails."""


class SemaphoreTimeoutError(SemaphoreError):
    """Raised when ``timeout`` elapses before the caller could acquire."""


class Semaphore:
    """Local in-process weighted semaphore. Implementation lands in Task 3."""


class AsyncSemaphore:
    """Local in-process async weighted semaphore. Implementation lands in Task 7."""


__all__ = ["AsyncSemaphore", "Semaphore", "SemaphoreError", "SemaphoreTimeoutError"]
