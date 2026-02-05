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
    """Decorator that intercepts connection errors and ignores them if configured.

    When applied to a cache method (sync or async), this decorator catches
    connection and timeout errors from the underlying library (redis-py or
    valkey-py) and either ignores them (returning return_value) or re-raises,
    depending on the cache's _ignore_exceptions setting.

    Args:
        method: The method to wrap (when used without parentheses)
        return_value: Value to return when exception is ignored (default: None)

    Usage:
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
