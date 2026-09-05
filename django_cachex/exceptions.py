# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause
#
# django-redis was used as inspiration for this project.

"""Exceptions for django-cachex."""

import socket
from typing import Any


def _build_main_exceptions() -> tuple[type[Exception], ...]:
    """Network/server-side errors the client layer treats as transient."""
    # Built on first access: importing ``CachexError`` must not drag in a
    # driver that a LocMem or Database user never installed.
    found: list[type[Exception]] = [socket.timeout]

    try:
        from redis.exceptions import ConnectionError as RedisConnectionError
        from redis.exceptions import RedisClusterException
        from redis.exceptions import ResponseError as RedisResponseError
        from redis.exceptions import TimeoutError as RedisTimeoutError

        found.extend([RedisConnectionError, RedisTimeoutError, RedisResponseError, RedisClusterException])
    except ImportError:
        pass

    try:
        from valkey.exceptions import ConnectionError as ValkeyConnectionError
        from valkey.exceptions import ResponseError as ValkeyResponseError
        from valkey.exceptions import TimeoutError as ValkeyTimeoutError
        from valkey.exceptions import ValkeyClusterException

        found.extend([ValkeyConnectionError, ValkeyTimeoutError, ValkeyResponseError, ValkeyClusterException])
    except ImportError:
        pass

    return tuple(found)


def __getattr__(name: str) -> Any:
    if name == "_main_exceptions":
        value = _build_main_exceptions()
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class CachexError(Exception):
    """Base class for every exception raised by django-cachex.

    Catch this to handle any library error in one place; catch a specific
    subclass when you need to react to one failure mode.
    """


class CompressorError(CachexError):
    """Raised when compression or decompression fails. Triggers the client's compressor fallback."""


class SerializerError(CachexError):
    """Raised when serialization or deserialization fails. Triggers the client's serializer fallback."""


class NotSupportedError(CachexError):
    """Raised when an operation is not supported by the cache backend."""

    def __init__(self, operation: str, backend: str | None = None) -> None:
        self.operation = operation
        self.backend = backend
        msg = f"Operation '{operation}' is not supported"
        if backend:
            msg += f" by {backend}"
        super().__init__(msg)


class KeyNotFoundError(CachexError, ValueError):
    """Raised when an operation needs a key that does not exist.

    Mirrors Redis ``ERR no such key``. Subclasses both :class:`CachexError`
    and :class:`ValueError`, so ``except CachexError`` and existing
    ``except ValueError`` callers both keep working. The missing key is
    available as ``key``.
    """

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"Key {key!r} not found")


class WrongTypeError(CachexError, TypeError):
    """Raised when an operation is applied to a key holding the wrong RESP type.

    Mirrors Redis ``WRONGTYPE Operation against a key holding the wrong kind
    of value``. Subclasses both :class:`CachexError` and :class:`TypeError`,
    so ``except CachexError`` and existing ``except TypeError`` callers both
    keep working while new code can catch this specifically across backends
    (LocMem, redis-py, valkey-py, valkey-glide).
    """


def maybe_wrap_wrongtype(exc: BaseException) -> BaseException:
    """Return :class:`WrongTypeError` if ``exc`` is a backend WRONGTYPE response.

    Each RESP client surfaces ``WRONGTYPE`` differently. redis-py and
    valkey-py raise their own ``ResponseError`` subclasses, valkey-glide
    raises ``RequestError``. All backends carry the literal ``WRONGTYPE``
    token in the message; this helper inspects the message and returns a
    uniform :class:`WrongTypeError` (preserving the original as
    ``__cause__``) so callers can catch a single exception across backends.
    """
    if isinstance(exc, WrongTypeError):
        return exc
    msg = str(exc)
    if "WRONGTYPE" in msg:
        wrapped = WrongTypeError(msg)
        wrapped.__cause__ = exc
        return wrapped
    return exc
