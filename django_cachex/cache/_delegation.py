"""Delegation helpers shared by composite backends that front another cache."""

from typing import TYPE_CHECKING, Any

from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from django.core.cache.backends.base import BaseCache


class DelegatingCacheMixin:
    """Forward the admin/metadata surface to ``_delegation_target``.

    A ``NotSupportedError`` raised by the target is re-raised as this
    backend's own so callers see the alias they actually used.
    """

    @property
    def _delegation_target(self) -> BaseCache:
        raise NotImplementedError

    def _delegated_method(self, method: str) -> Any:
        """Look up ``method`` on the target, raising NotSupportedError if it has none."""
        fn = getattr(self._delegation_target, method, None)
        if fn is None:
            raise NotSupportedError(method, type(self).__name__)
        return fn

    def _delegate(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Call ``method`` on the target, translating only its ``NotSupportedError``.

        An ``AttributeError`` raised inside the target's implementation is a
        bug there and propagates unchanged.
        """
        fn = self._delegated_method(method)
        try:
            return fn(*args, **kwargs)
        except NotSupportedError as exc:
            raise NotSupportedError(method, type(self).__name__) from exc

    async def _adelegate(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Async twin of :meth:`_delegate`."""
        fn = self._delegated_method(method)
        try:
            return await fn(*args, **kwargs)
        except NotSupportedError as exc:
            raise NotSupportedError(method, type(self).__name__) from exc

    def _wrap_iter(self, method: str, it: Iterator[str]) -> Iterator[str]:
        """Re-raise a lazily surfaced NotSupportedError as this backend's.

        A generator function returns without running its body, so the
        target's ``NotSupportedError`` escapes :meth:`_delegate` at
        iteration time.
        """
        try:
            yield from it
        except NotSupportedError as exc:
            raise NotSupportedError(method, type(self).__name__) from exc

    async def _awrap_iter(self, method: str, it: AsyncIterator[str]) -> AsyncIterator[str]:
        """Async twin of :meth:`_wrap_iter`."""
        try:
            async for key in it:
                yield key
        except NotSupportedError as exc:
            raise NotSupportedError(method, type(self).__name__) from exc

    # -- Key helpers --

    def make_key(self, key: str, version: int | None = None) -> str:
        return self._delegate("make_key", key, version=version)

    def reverse_key(self, key: str) -> str:
        return self._delegate("reverse_key", key)

    def make_pattern(self, pattern: str, version: int | None = None) -> str:
        return self._delegate("make_pattern", pattern, version=version)

    # -- Admin metadata --

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        return self._delegate("keys", pattern, version=version)

    def iter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> Iterator[str]:
        return self._wrap_iter("iter_keys", self._delegate("iter_keys", pattern, version=version, itersize=itersize))

    def scan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]:
        return self._delegate(
            "scan",
            cursor=cursor,
            pattern=pattern,
            count=count,
            version=version,
            key_type=key_type,
        )

    def ttl(self, key: str, version: int | None = None) -> int | None:
        return self._delegate("ttl", key, version=version)

    def pttl(self, key: str, version: int | None = None) -> int | None:
        return self._delegate("pttl", key, version=version)

    def type(self, key: str, version: int | None = None) -> Any:
        return self._delegate("type", key, version=version)

    def info(self, section: str | None = None) -> dict[str, Any]:
        return self._delegate("info", section=section)

    def persist(self, key: str, version: int | None = None) -> bool:
        return self._delegate("persist", key, version=version)

    async def akeys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        return await self._adelegate("akeys", pattern, version=version)

    def aiter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> AsyncIterator[str]:
        # Not ``async def``: the target's ``aiter_keys`` is itself a plain
        # method returning an async iterator, matching ``BaseCachex``.
        fn = self._delegated_method("aiter_keys")
        try:
            it = fn(pattern, version=version, itersize=itersize)
        except NotSupportedError as exc:
            raise NotSupportedError("aiter_keys", type(self).__name__) from exc
        return self._awrap_iter("aiter_keys", it)

    async def ascan(
        self,
        cursor: int = 0,
        pattern: str = "*",
        count: int | None = None,
        version: int | None = None,
        key_type: str | None = None,
    ) -> tuple[int, list[str]]:
        return await self._adelegate(
            "ascan",
            cursor=cursor,
            pattern=pattern,
            count=count,
            version=version,
            key_type=key_type,
        )

    async def attl(self, key: str, version: int | None = None) -> int | None:
        return await self._adelegate("attl", key, version=version)

    async def apttl(self, key: str, version: int | None = None) -> int | None:
        return await self._adelegate("apttl", key, version=version)

    async def atype(self, key: str, version: int | None = None) -> Any:
        return await self._adelegate("atype", key, version=version)

    async def apersist(self, key: str, version: int | None = None) -> bool:
        return await self._adelegate("apersist", key, version=version)


__all__ = ["DelegatingCacheMixin"]
