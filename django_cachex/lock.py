"""Distributed lock backed by the Rust driver's ``lock_*`` primitives.

A token-scoped Redis lock: SET NX with a TTL plus Lua-script release/extend
that compare the stored token before mutating. Cluster mode is rejected
because the underlying Lua scripts route to a single hash slot.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from types import TracebackType

    from django_cachex._driver import RustValkeyDriver


class LockError(Exception):
    """Raised when a lock operation fails."""


class LockNotOwnedError(LockError):
    """Raised when releasing or extending a lock the caller no longer owns."""


class ValkeyLock:
    """Token-scoped distributed lock on top of the Rust driver.

    Each instance is single-use per ``acquire``: the token is rotated on each
    call. ``thread_local=True`` (the default) keeps the active token per
    thread so the same ``ValkeyLock`` instance can be shared between threads
    without one releasing another's lock.
    """

    def __init__(
        self,
        driver: RustValkeyDriver,
        name: str,
        timeout: float | None = None,
        sleep: float = 0.1,
        *,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        thread_local: bool = True,
    ) -> None:
        if sleep <= 0:
            msg = "sleep must be positive"
            raise ValueError(msg)
        self._driver = driver
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.thread_local = thread_local

        self._token_local: threading.local | None = threading.local() if thread_local else None
        self._token_shared: bytes | None = None

    # ------------------------------------------------------------------ token

    @property
    def token(self) -> bytes | None:
        if self._token_local is not None:
            return getattr(self._token_local, "token", None)
        return self._token_shared

    @token.setter
    def token(self, value: bytes | None) -> None:
        if self._token_local is not None:
            if value is None:
                if hasattr(self._token_local, "token"):
                    del self._token_local.token
            else:
                self._token_local.token = value
        else:
            self._token_shared = value

    @staticmethod
    def _new_token() -> bytes:
        return uuid.uuid4().hex.encode("ascii")

    @staticmethod
    def _ttl_ms(timeout: float | None) -> int | None:
        if timeout is None:
            return None
        return max(1, int(timeout * 1000))

    # ---------------------------------------------------------------- helpers

    def locked(self) -> bool:
        """Return True if the lock key exists. Doesn't check ownership."""
        return self._driver.exists_sync(self.name)

    async def alocked(self) -> bool:
        return await self._driver.exists(self.name)

    def owned(self) -> bool:
        """Return True if this client holds the lock (token still matches)."""
        token = self.token
        if token is None:
            return False
        stored = self._driver.get_sync(self.name)
        return stored == token

    async def aowned(self) -> bool:
        token = self.token
        if token is None:
            return False
        stored = await self._driver.get(self.name)
        return stored == token

    # ----------------------------------------------------------------- sync

    def acquire(
        self,
        *,
        blocking: bool | None = None,
        blocking_timeout: float | None = None,
        token: bytes | str | None = None,
    ) -> bool:
        if blocking is None:
            blocking = self.blocking
        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout

        new_token = self._coerce_token(token) if token is not None else self._new_token()
        ttl_ms = self._ttl_ms(self.timeout)

        deadline = time.monotonic() + blocking_timeout if blocking_timeout is not None else None
        while True:
            if self._driver.lock_acquire_sync(self.name, new_token.decode("ascii"), ttl_ms):
                self.token = new_token
                return True
            if not blocking:
                return False
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(self.sleep)

    def release(self) -> None:
        token = self.token
        if token is None:
            msg = "Cannot release an unlocked lock"
            raise LockError(msg)
        result = self._driver.lock_release_sync(self.name, token.decode("ascii"))
        self.token = None
        if result == 0:
            msg = "Cannot release a lock that's no longer owned"
            raise LockNotOwnedError(msg)

    def extend(self, additional_time: float, *, replace_ttl: bool = False) -> bool:
        if replace_ttl:
            # The driver's lock_extend script always adds to the existing TTL
            # rather than replacing it; flag the divergence loudly instead of
            # silently doing the wrong thing.
            msg = "ValkeyLock.extend(replace_ttl=True) is not supported by the Rust driver"
            raise NotImplementedError(msg)
        token = self.token
        if token is None:
            msg = "Cannot extend an unlocked lock"
            raise LockError(msg)
        additional_ms = max(1, int(additional_time * 1000))
        result = self._driver.lock_extend_sync(self.name, token.decode("ascii"), additional_ms)
        if result == 0:
            msg = "Cannot extend a lock that's no longer owned"
            raise LockNotOwnedError(msg)
        return True

    # ---------------------------------------------------------------- async

    async def aacquire(
        self,
        *,
        blocking: bool | None = None,
        blocking_timeout: float | None = None,
        token: bytes | str | None = None,
    ) -> bool:
        if blocking is None:
            blocking = self.blocking
        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout

        new_token = self._coerce_token(token) if token is not None else self._new_token()
        ttl_ms = self._ttl_ms(self.timeout)

        deadline = time.monotonic() + blocking_timeout if blocking_timeout is not None else None
        while True:
            ok = await self._driver.lock_acquire(self.name, new_token.decode("ascii"), ttl_ms)
            if ok:
                self.token = new_token
                return True
            if not blocking:
                return False
            if deadline is not None and time.monotonic() >= deadline:
                return False
            await asyncio.sleep(self.sleep)

    async def arelease(self) -> None:
        token = self.token
        if token is None:
            msg = "Cannot release an unlocked lock"
            raise LockError(msg)
        result = await self._driver.lock_release(self.name, token.decode("ascii"))
        self.token = None
        if result == 0:
            msg = "Cannot release a lock that's no longer owned"
            raise LockNotOwnedError(msg)

    async def aextend(self, additional_time: float, *, replace_ttl: bool = False) -> bool:
        if replace_ttl:
            msg = "ValkeyLock.aextend(replace_ttl=True) is not supported by the Rust driver"
            raise NotImplementedError(msg)
        token = self.token
        if token is None:
            msg = "Cannot extend an unlocked lock"
            raise LockError(msg)
        additional_ms = max(1, int(additional_time * 1000))
        result = await self._driver.lock_extend(self.name, token.decode("ascii"), additional_ms)
        if result == 0:
            msg = "Cannot extend a lock that's no longer owned"
            raise LockNotOwnedError(msg)
        return True

    # ---------------------------------------------------------- ctx managers

    def __enter__(self) -> Self:
        if self.acquire():
            return self
        msg = f"Could not acquire lock {self.name!r}"
        raise LockError(msg)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()

    async def __aenter__(self) -> Self:
        if await self.aacquire():
            return self
        msg = f"Could not acquire lock {self.name!r}"
        raise LockError(msg)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.arelease()

    # ---------------------------------------------------------------- helpers

    @staticmethod
    def _coerce_token(token: bytes | str | Any) -> bytes:
        if isinstance(token, bytes):
            return token
        if isinstance(token, str):
            return token.encode("ascii")
        msg = f"token must be bytes or str, got {type(token).__name__}"
        raise TypeError(msg)


class AsyncValkeyLock(ValkeyLock):
    """Async-flavored ValkeyLock. ``acquire``/``release``/``extend`` are
    coroutines (matching redis-py's async ``Lock``)."""

    async def acquire(  # type: ignore[override]
        self,
        *,
        blocking: bool | None = None,
        blocking_timeout: float | None = None,
        token: bytes | str | None = None,
    ) -> bool:
        return await self.aacquire(
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            token=token,
        )

    async def release(self) -> None:  # type: ignore[override]
        await self.arelease()

    async def extend(self, additional_time: float, *, replace_ttl: bool = False) -> bool:  # type: ignore[override]
        return await self.aextend(additional_time, replace_ttl=replace_ttl)

    async def locked(self) -> bool:  # type: ignore[override]
        return await self.alocked()

    async def owned(self) -> bool:  # type: ignore[override]
        return await self.aowned()

    async def __aenter__(self) -> Self:
        if await self.aacquire():
            return self
        msg = f"Could not acquire lock {self.name!r}"
        raise LockError(msg)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.arelease()

    # Sync ctx manager intentionally absent on the async variant.


__all__ = ["AsyncValkeyLock", "LockError", "LockNotOwnedError", "ValkeyLock"]
