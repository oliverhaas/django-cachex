# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause
#
# django-redis was used as inspiration for this project.

"""Exceptions for django-cachex."""

import socket

# Network/server-side errors the client layer treats as transient or
# backend-specific failures. Each block is best-effort: redis-py and
# valkey-py are both optional installs.
_exception_list: list[type[Exception]] = [socket.timeout]

try:
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import RedisClusterException
    from redis.exceptions import ResponseError as RedisResponseError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    _exception_list.extend([RedisConnectionError, RedisTimeoutError, RedisResponseError, RedisClusterException])
except ImportError:
    pass

try:
    from valkey.exceptions import ConnectionError as ValkeyConnectionError
    from valkey.exceptions import ResponseError as ValkeyResponseError
    from valkey.exceptions import TimeoutError as ValkeyTimeoutError
    from valkey.exceptions import ValkeyClusterException

    _exception_list.extend([ValkeyConnectionError, ValkeyTimeoutError, ValkeyResponseError, ValkeyClusterException])
except ImportError:
    pass

_main_exceptions = tuple(_exception_list)


class CompressorError(Exception):
    """Raised when compression or decompression fails. Triggers the client's compressor fallback."""


class SerializerError(Exception):
    """Raised when serialization or deserialization fails. Triggers the client's serializer fallback."""


class NotSupportedError(Exception):
    """Raised when an operation is not supported by the cache backend."""

    def __init__(self, operation: str, backend: str | None = None) -> None:
        self.operation = operation
        self.backend = backend
        msg = f"Operation '{operation}' is not supported"
        if backend:
            msg += f" by {backend}"
        super().__init__(msg)


class WrongTypeError(TypeError):
    """Raised when an operation is applied to a key holding the wrong RESP type.

    Mirrors Redis ``WRONGTYPE Operation against a key holding the wrong kind
    of value``. Subclasses :class:`TypeError` so existing ``except TypeError``
    callers keep working while new code can catch this specifically across
    backends (LocMem, redis-py, valkey-py, valkey-glide, redis-rs).
    """


def maybe_wrap_wrongtype(exc: BaseException) -> BaseException:
    """Return :class:`WrongTypeError` if ``exc`` is a backend WRONGTYPE response.

    Each RESP client surfaces ``WRONGTYPE`` differently — redis-py and
    valkey-py raise their own ``ResponseError`` subclasses, valkey-glide
    raises ``RequestError``, and the Rust adapter currently raises
    ``RuntimeError``. They all carry the literal ``WRONGTYPE`` token in the
    message. This helper inspects the message and returns a uniform
    :class:`WrongTypeError` (preserving the original as ``__cause__``) so
    callers can catch a single exception across backends.
    """
    if isinstance(exc, WrongTypeError):
        return exc
    msg = str(exc)
    if "WRONGTYPE" in msg:
        wrapped = WrongTypeError(msg)
        wrapped.__cause__ = exc
        return wrapped
    return exc
