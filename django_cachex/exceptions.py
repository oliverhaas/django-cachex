# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause
#
# django-redis was used as inspiration for this project.

"""Exceptions for django-cachex."""

import socket

# Build exception tuples from available libraries (redis-py / valkey-py).
# These are used by omit_exception and the client layer.
_exception_list: list[type[Exception]] = [socket.timeout]
_RedisResponseError: type[Exception] | None = None
_ValkeyResponseError: type[Exception] | None = None

try:
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import RedisClusterException
    from redis.exceptions import ResponseError as RedisResponseError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    _RedisResponseError = RedisResponseError
    _exception_list.extend([RedisConnectionError, RedisTimeoutError, RedisResponseError, RedisClusterException])
except ImportError:
    pass

try:
    from valkey.exceptions import ConnectionError as ValkeyConnectionError
    from valkey.exceptions import ResponseError as ValkeyResponseError
    from valkey.exceptions import TimeoutError as ValkeyTimeoutError

    _ValkeyResponseError = ValkeyResponseError
    _exception_list.extend([ValkeyConnectionError, ValkeyTimeoutError, ValkeyResponseError])
except ImportError:
    pass

_main_exceptions = tuple(_exception_list)

_response_errors: list[type[Exception]] = []
if _RedisResponseError is not None:
    _response_errors.append(_RedisResponseError)
if _ValkeyResponseError is not None:
    _response_errors.append(_ValkeyResponseError)
_ResponseError = tuple(_response_errors) if _response_errors else (Exception,)


class CompressorError(Exception):
    """Raised when compression or decompression fails.

    Caught by the client's fallback logic to try the next compressor in the chain.
    """


class SerializerError(Exception):
    """Raised when serialization or deserialization fails.

    Caught by the client's fallback logic to try the next serializer in the chain.
    """


class NotSupportedError(Exception):
    """Raised when an operation is not supported by the cache backend."""

    def __init__(self, operation: str, backend: str | None = None) -> None:
        self.operation = operation
        self.backend = backend
        msg = f"Operation '{operation}' is not supported"
        if backend:
            msg += f" by {backend}"
        super().__init__(msg)

    def __str__(self) -> str:
        msg = f"Operation '{self.operation}' is not supported"
        if self.backend:
            msg += f" by {self.backend}"
        return msg
