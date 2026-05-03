"""Two-tiered cache backend using Django's CACHES setting.

References two other cache backends (L1 and L2) from the CACHES setting.
Hot reads are served from L1 (typically LocMemCache), falling through to
L2 (typically Redis/Valkey) on miss. L1 TTL is capped to prevent serving
stale data.

Only the standard Django cache interface is supported. For advanced
features (data structures, pipelines, etc.), use the tier caches directly.

Configuration::

    CACHES = {
        "l1": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "OPTIONS": {"MAX_ENTRIES": 1000},
        },
        "l2": {
            "BACKEND": "django_cachex.cache.RedisCache",
            "LOCATION": "redis://127.0.0.1:6379/0",
        },
        "default": {
            "BACKEND": "django_cachex.cache.TieredCache",
            "OPTIONS": {
                "TIERS": ["l1", "l2"],
                "L1_TIMEOUT": 5,
            },
        },
    }

``L1_TIMEOUT`` caps how long entries live in L1. If omitted, falls back
to L1's own ``TIMEOUT`` setting.
"""

from functools import cached_property
from typing import TYPE_CHECKING, Any

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.core.exceptions import ImproperlyConfigured

from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from django_cachex.types import ExpiryT, KeyT

# Sentinel to distinguish "not in L1" from a stored None
_L1_MISS = object()


class TieredCache(BaseCache):
    """Two-tiered cache referencing other CACHES entries as L1 and L2.

    L1 is checked first on reads; on miss, L2 is queried and L1 is populated.
    L1 TTL is capped by min(L1_TIMEOUT, L2's remaining TTL).
    """

    _cachex_support: str = "cachex"

    def __init__(self, server: str, params: dict[str, Any]) -> None:
        super().__init__(params)
        options = params.get("OPTIONS", {})
        tiers = options.get("TIERS")
        if not tiers or len(tiers) != 2:
            msg = (
                f"TieredCache requires OPTIONS['TIERS'] with exactly 2 cache aliases, e.g. ['l1', 'l2']. Got: {tiers!r}"
            )
            raise ImproperlyConfigured(msg)
        self._l1_alias: str = tiers[0]
        self._l2_alias: str = tiers[1]
        # L1 TTL cap: explicit option or fall back to L1's own default_timeout
        self._l1_max_timeout: float | None = options.get("L1_TIMEOUT")

    @cached_property
    def _l1(self) -> BaseCache:
        from django.core.cache import caches

        return caches[self._l1_alias]

    @cached_property
    def _l2(self) -> BaseCache:
        from django.core.cache import caches

        return caches[self._l2_alias]

    @property
    def _l1_cap(self) -> float | None:
        """L1 TTL cap: explicit L1_TIMEOUT option, or L1's own default_timeout."""
        cap = self._l1_max_timeout
        return cap if cap is not None else self._l1.default_timeout

    def _l1_timeout(self, l2_ttl: int | None = None) -> float | None:
        """Calculate L1 TTL: min(L1 cap, L2's remaining TTL)."""
        cap = self._l1_cap
        if l2_ttl is not None and l2_ttl > 0:
            if cap is not None:
                return min(cap, l2_ttl)
            return l2_ttl
        return cap

    def _l1_timeout_for_set(self, timeout: float | None) -> float | None:
        """Calculate L1 TTL for a set operation given the user-specified timeout."""
        cap = self._l1_cap
        if timeout is None or timeout is DEFAULT_TIMEOUT:
            return cap
        if timeout <= 0:
            return timeout  # 0 means delete immediately
        if cap is not None:
            return min(cap, timeout)
        return timeout

    def _get_l2_ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Try to get L2's remaining TTL for a key. Returns None if unsupported."""
        try:
            ttl = self._l2.ttl(key, version=version)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            return ttl if isinstance(ttl, int) and ttl > 0 else None
        except AttributeError, NotSupportedError, TypeError:
            return None

    async def _aget_l2_ttl(self, key: KeyT, version: int | None = None) -> int | None:
        """Try to get L2's remaining TTL for a key asynchronously."""
        try:
            ttl = await self._l2.attl(key, version=version)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            return ttl if isinstance(ttl, int) and ttl > 0 else None
        except AttributeError, NotSupportedError, TypeError:
            return None

    # =========================================================================
    # Standard Django cache interface
    # =========================================================================

    def get(self, key: KeyT, default: Any = None, version: int | None = None) -> Any:
        val = self._l1.get(key, _L1_MISS, version=version)
        if val is not _L1_MISS:
            return val
        val = self._l2.get(key, _L1_MISS, version=version)
        if val is _L1_MISS:
            return default
        l2_ttl = self._get_l2_ttl(key, version=version)
        self._l1.set(key, val, self._l1_timeout(l2_ttl), version=version)
        return val

    async def aget(self, key: KeyT, default: Any = None, version: int | None = None) -> Any:
        val = self._l1.get(key, _L1_MISS, version=version)
        if val is not _L1_MISS:
            return val
        val = await self._l2.aget(key, _L1_MISS, version=version)
        if val is _L1_MISS:
            return default
        l2_ttl = await self._aget_l2_ttl(key, version=version)
        self._l1.set(key, val, self._l1_timeout(l2_ttl), version=version)
        return val

    def set(  # type: ignore[override]
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        **kwargs: Any,
    ) -> bool:
        # Django stubs annotate BaseCache.set as returning None; cachex backends
        # actually return bool. bool(None) is False, matching "didn't happen".
        result = self._l2.set(key, value, timeout, version=version, **kwargs)  # type: ignore[func-returns-value]
        if result or not (kwargs.get("nx") or kwargs.get("xx")):
            self._l1.set(key, value, self._l1_timeout_for_set(timeout), version=version)
        return bool(result)

    async def aset(  # type: ignore[override]
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        **kwargs: Any,
    ) -> bool:
        result = await self._l2.aset(key, value, timeout, version=version, **kwargs)  # type: ignore[func-returns-value]
        if result or not (kwargs.get("nx") or kwargs.get("xx")):
            self._l1.set(key, value, self._l1_timeout_for_set(timeout), version=version)
        return bool(result)

    def add(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        result = self._l2.add(key, value, timeout, version=version)
        if result:
            self._l1.set(key, value, self._l1_timeout_for_set(timeout), version=version)
        return result

    async def aadd(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        result = await self._l2.aadd(key, value, timeout, version=version)
        if result:
            self._l1.set(key, value, self._l1_timeout_for_set(timeout), version=version)
        return result

    def delete(self, key: KeyT, version: int | None = None) -> bool:
        self._l1.delete(key, version=version)
        return self._l2.delete(key, version=version)

    async def adelete(self, key: KeyT, version: int | None = None) -> bool:
        self._l1.delete(key, version=version)
        return await self._l2.adelete(key, version=version)

    def get_many(self, keys: Iterable[KeyT], version: int | None = None) -> dict[KeyT, Any]:
        l1_results: dict[KeyT, Any] = {}
        missed_keys: list[KeyT] = []
        for key in keys:
            val = self._l1.get(key, _L1_MISS, version=version)
            if val is not _L1_MISS:
                l1_results[key] = val
            else:
                missed_keys.append(key)
        if not missed_keys:
            return l1_results
        l2_results = self._l2.get_many(missed_keys, version=version)
        for key, val in l2_results.items():
            l2_ttl = self._get_l2_ttl(key, version=version)
            self._l1.set(key, val, self._l1_timeout(l2_ttl), version=version)
        l1_results.update(l2_results)
        return l1_results

    async def aget_many(self, keys: Iterable[KeyT], version: int | None = None) -> dict[KeyT, Any]:
        l1_results: dict[KeyT, Any] = {}
        missed_keys: list[KeyT] = []
        for key in keys:
            val = self._l1.get(key, _L1_MISS, version=version)
            if val is not _L1_MISS:
                l1_results[key] = val
            else:
                missed_keys.append(key)
        if not missed_keys:
            return l1_results
        l2_results = await self._l2.aget_many(missed_keys, version=version)
        for key, val in l2_results.items():
            l2_ttl = await self._aget_l2_ttl(key, version=version)
            self._l1.set(key, val, self._l1_timeout(l2_ttl), version=version)
        l1_results.update(l2_results)
        return l1_results

    def set_many(
        self,
        data: dict[KeyT, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[Any]:
        result = self._l2.set_many(data, timeout, version=version)
        l1_timeout = self._l1_timeout_for_set(timeout)
        for key, value in data.items():
            self._l1.set(key, value, l1_timeout, version=version)
        return result

    async def aset_many(
        self,
        data: dict[KeyT, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[Any]:
        result = await self._l2.aset_many(data, timeout, version=version)
        l1_timeout = self._l1_timeout_for_set(timeout)
        for key, value in data.items():
            self._l1.set(key, value, l1_timeout, version=version)
        return result

    def delete_many(self, keys: Iterable[KeyT], version: int | None = None) -> None:
        keys = list(keys)
        for key in keys:
            self._l1.delete(key, version=version)
        self._l2.delete_many(keys, version=version)

    async def adelete_many(self, keys: Iterable[KeyT], version: int | None = None) -> None:
        keys = list(keys)
        for key in keys:
            self._l1.delete(key, version=version)
        await self._l2.adelete_many(keys, version=version)

    def has_key(self, key: KeyT, version: int | None = None) -> bool:
        if self._l1.has_key(key, version=version):
            return True
        return self._l2.has_key(key, version=version)

    async def ahas_key(self, key: KeyT, version: int | None = None) -> bool:
        if self._l1.has_key(key, version=version):
            return True
        return await self._l2.ahas_key(key, version=version)

    def incr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        self._l1.delete(key, version=version)
        return self._l2.incr(key, delta, version=version)

    async def aincr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        self._l1.delete(key, version=version)
        return await self._l2.aincr(key, delta, version=version)

    def decr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        self._l1.delete(key, version=version)
        return self._l2.decr(key, delta, version=version)

    async def adecr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        self._l1.delete(key, version=version)
        return await self._l2.adecr(key, delta, version=version)

    def touch(self, key: KeyT, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        result = self._l2.touch(key, timeout, version=version)
        if result:
            self._l1.touch(key, self._l1_timeout_for_set(timeout), version=version)
        return result

    async def atouch(self, key: KeyT, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        result = await self._l2.atouch(key, timeout, version=version)
        if result:
            self._l1.touch(key, self._l1_timeout_for_set(timeout), version=version)
        return result

    def clear(self) -> None:
        self._l1.clear()
        self._l2.clear()

    async def aclear(self) -> None:
        self._l1.clear()
        await self._l2.aclear()

    def close(self, **kwargs: Any) -> None:
        self._l1.close(**kwargs)
        self._l2.close(**kwargs)

    async def aclose(self, **kwargs: Any) -> None:
        await self._l1.aclose(**kwargs)
        await self._l2.aclose(**kwargs)

    # =========================================================================
    # Admin delegation methods (delegate to L2)
    # =========================================================================

    def _delegate(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Delegate a method call to L2, raising NotSupportedError if unavailable."""
        fn = getattr(self._l2, method, None)
        if fn is None:
            raise NotSupportedError(method, "TieredCache")
        try:
            return fn(*args, **kwargs)
        except AttributeError, NotSupportedError:
            raise NotSupportedError(method, "TieredCache") from None

    def make_key(self, key: str, version: int | None = None) -> str:
        return self._delegate("make_key", key, version=version)

    def reverse_key(self, key: str) -> str:
        return self._delegate("reverse_key", key)

    def make_pattern(self, pattern: str, version: int | None = None) -> str:
        return self._delegate("make_pattern", pattern, version=version)

    def keys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        return self._delegate("keys", pattern, version=version)

    def iter_keys(self, pattern: str, itersize: int | None = None) -> Iterator[str]:
        return self._delegate("iter_keys", pattern, itersize=itersize)

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

    def ttl(self, key: KeyT, version: int | None = None) -> int:
        return self._delegate("ttl", key, version=version)

    def pttl(self, key: KeyT, version: int | None = None) -> int:
        return self._delegate("pttl", key, version=version)

    def type(self, key: KeyT, version: int | None = None) -> str:
        return self._delegate("type", key, version=version)

    def info(self, section: str | None = None) -> dict[str, Any]:
        return self._delegate("info", section=section)

    def persist(self, key: KeyT, version: int | None = None) -> bool:
        return self._delegate("persist", key, version=version)

    def expire(self, key: KeyT, timeout: ExpiryT, version: int | None = None) -> bool:
        self._l1.delete(key, version=version)
        return self._delegate("expire", key, timeout, version=version)

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        self._l1.clear()
        return self._delegate("delete_pattern", pattern, version=version, itersize=itersize)


__all__ = [
    "TieredCache",
]
