"""Delegation helpers for a composite backend that fronts another cache."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from django.core.cache.backends.base import BaseCache


class DelegatingCacheMixin:
    """Forward the admin/metadata surface to ``_delegation_target``.

    The target's errors propagate unchanged: a ``NotSupportedError`` it
    raises already names the operation and, when the server refused the
    command, why.
    """

    @property
    def _delegation_target(self) -> BaseCache:
        raise NotImplementedError

    def _delegate(self, method: str, *args: Any, **kwargs: Any) -> Any:
        return getattr(self._delegation_target, method)(*args, **kwargs)

    async def _adelegate(self, method: str, *args: Any, **kwargs: Any) -> Any:
        return await getattr(self._delegation_target, method)(*args, **kwargs)

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
        return self._delegate("iter_keys", pattern, version=version, itersize=itersize)

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

    def memory_usage(self, key: str, version: int | None = None, *, samples: int | None = None) -> int | None:
        return self._delegate("memory_usage", key, version=version, samples=samples)

    def largest_keys(
        self,
        pattern: str = "*",
        count: int = 10,
        version: int | None = None,
        *,
        samples: int | None = None,
        itersize: int | None = None,
    ) -> list[tuple[str, int]]:
        return self._delegate("largest_keys", pattern, count, version=version, samples=samples, itersize=itersize)

    def slowlog_get(self, count: int = 10) -> list[Any]:
        return self._delegate("slowlog_get", count)

    def slowlog_len(self) -> int:
        return self._delegate("slowlog_len")

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
        return self._delegate("aiter_keys", pattern, version=version, itersize=itersize)

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

    async def amemory_usage(self, key: str, version: int | None = None, *, samples: int | None = None) -> int | None:
        return await self._adelegate("amemory_usage", key, version=version, samples=samples)

    async def alargest_keys(
        self,
        pattern: str = "*",
        count: int = 10,
        version: int | None = None,
        *,
        samples: int | None = None,
        itersize: int | None = None,
    ) -> list[tuple[str, int]]:
        return await self._adelegate(
            "alargest_keys",
            pattern,
            count,
            version=version,
            samples=samples,
            itersize=itersize,
        )


__all__ = ["DelegatingCacheMixin"]
