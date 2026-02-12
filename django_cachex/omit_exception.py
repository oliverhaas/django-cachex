from __future__ import annotations

import asyncio
import functools
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

from django_cachex.exceptions import _main_exceptions


def omit_exception(
    method: Callable | None = None,
    return_value: Any | None = None,
) -> Callable:
    """Decorator that catches backend errors (connection, timeout, response) based on _ignore_exceptions setting.

    Supports both sync and async methods. When ignoring, returns ``return_value``.

    Usage::

        @omit_exception
        def set(self, key, value): ...

        @omit_exception(return_value={})
        async def aget_many(self, keys): ...
    """
    if method is None:
        return functools.partial(omit_exception, return_value=return_value)

    def _handle_exception(self: Any, exc: Exception) -> Any:
        if self._ignore_exceptions:
            if self._log_ignored_exceptions:
                self._logger.exception("Exception ignored")
            return return_value
        raise exc

    if asyncio.iscoroutinefunction(method):

        @functools.wraps(method)
        async def _async_decorator(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return await method(self, *args, **kwargs)
            except _main_exceptions as e:
                return _handle_exception(self, e)

        return _async_decorator

    @functools.wraps(method)
    def _sync_decorator(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return method(self, *args, **kwargs)
        except _main_exceptions as e:
            return _handle_exception(self, e)

    return _sync_decorator
