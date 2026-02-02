# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause
#
# django-redis was used as inspiration for this project.

"""Exceptions for django-cachex.

This module defines exceptions that may be raised during cache operations.
Users can catch these to handle specific error conditions.
"""

from typing import Any


class ConnectionInterruptedError(Exception):
    """Raised when a connection to Redis/Valkey is interrupted.

    This exception wraps underlying connection errors from the redis-py
    or valkey-py libraries. The original exception is available via
    the ``__cause__`` attribute.

    Attributes:
        connection: The connection object that was interrupted.

    Example:
        Handling connection errors::

            from django.core.cache import cache
            from django_cachex.exceptions import ConnectionInterruptedError

            try:
                cache.set("key", "value")
            except ConnectionInterruptedError as e:
                logger.error(f"Cache unavailable: {e}")
                # Fall back to database or other storage
    """

    def __init__(self, connection: Any, parent: Any = None) -> None:
        self.connection = connection

    def __str__(self) -> str:
        error_type = type(self.__cause__).__name__
        error_msg = str(self.__cause__)
        return f"Redis {error_type}: {error_msg}"


# Backwards compatibility alias
ConnectionInterrupted = ConnectionInterruptedError


class CompressorError(Exception):
    """Raised when compression or decompression fails.

    This can occur when:
    - The data is corrupted and cannot be decompressed
    - The compressor is misconfigured
    - The compression library encounters an error

    When using compressor fallback, this error triggers fallback to the
    next compressor in the list.
    """


class SerializerError(Exception):
    """Raised when serialization or deserialization fails.

    This can occur when:
    - The data format doesn't match the expected serializer format
    - The data is corrupted
    - The serializer encounters an incompatible type

    When using serializer fallback, this error triggers fallback to the
    next serializer in the list, enabling safe migrations between formats.
    """


class ScriptNotRegisteredError(KeyError):
    """Raised when eval_script is called with an unregistered script name.

    Attributes:
        name: The script name that was not found.

    Example:
        Handling missing scripts::

            from django.core.cache import cache
            from django_cachex.exceptions import ScriptNotRegisteredError

            try:
                cache.eval_script("unknown_script", keys=["key1"])
            except ScriptNotRegisteredError as e:
                logger.error(f"Script not registered: {e.name}")
    """

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"Script '{name}' is not registered")

    def __str__(self) -> str:
        return f"Script '{self.name}' is not registered"


class NotSupportedError(Exception):
    """Raised when an operation is not supported by the cache backend.

    This exception is used by the cache admin interface when a cache
    backend (or its wrapper) does not support a particular operation.
    Views catch this exception to display user-friendly warning messages.

    Attributes:
        operation: The operation that is not supported.
        backend: Optional name of the backend that doesn't support it.

    Example:
        Handling unsupported operations::

            from django_cachex.exceptions import NotSupportedError

            try:
                service.ttl(key)
            except NotSupportedError:
                messages.warning(request, "TTL not supported for this backend.")
    """

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
