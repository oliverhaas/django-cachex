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
    """Simple decorator that intercepts connection
    errors and ignores these if settings specify this.
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
                    self.logger.exception("Exception ignored")

                return return_value
            if e.__cause__ is not None:
                raise e.__cause__  # noqa: B904
            raise  # Fallback if __cause__ is somehow None

    return _decorator
