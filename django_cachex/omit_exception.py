from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

from django_cachex.exceptions import ConnectionInterruptedError


def omit_exception(
    method: Callable | None = None,
    return_value: Any | None = None,
) -> Callable:
    """Decorator that intercepts connection errors and ignores them if configured.

    When applied to a cache method, this decorator catches ConnectionInterruptedError
    and either ignores it (returning return_value) or re-raises the underlying cause,
    depending on the cache's _ignore_exceptions setting.

    Args:
        method: The method to wrap (when used without parentheses)
        return_value: Value to return when exception is ignored (default: None)

    Usage:
        @omit_exception
        def set(self, key, value): ...

        @omit_exception(return_value={})
        def get_many(self, keys): ...
    """
    if method is None:
        return functools.partial(omit_exception, return_value=return_value)

    @functools.wraps(method)
    def _decorator(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return method(self, *args, **kwargs)
        except ConnectionInterruptedError as e:
            if self._ignore_exceptions:
                if self._log_ignored_exceptions:
                    self._logger.exception("Exception ignored")
                return return_value
            if e.__cause__ is not None:
                raise e.__cause__  # noqa: B904
            raise  # Fallback if __cause__ is somehow None

    return _decorator


def aomit_exception(
    method: Callable | None = None,
    return_value: Any | None = None,
) -> Callable:
    """Async version of omit_exception decorator.

    Works identically to omit_exception but for async methods.

    Args:
        method: The async method to wrap (when used without parentheses)
        return_value: Value to return when exception is ignored (default: None)

    Usage:
        @aomit_exception
        async def aset(self, key, value): ...

        @aomit_exception(return_value={})
        async def aget_many(self, keys): ...
    """
    if method is None:
        return functools.partial(aomit_exception, return_value=return_value)

    @functools.wraps(method)
    async def _decorator(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return await method(self, *args, **kwargs)
        except ConnectionInterruptedError as e:
            if self._ignore_exceptions:
                if self._log_ignored_exceptions:
                    self._logger.exception("Exception ignored")
                return return_value
            if e.__cause__ is not None:
                raise e.__cause__  # noqa: B904
            raise  # Fallback if __cause__ is somehow None

    return _decorator
