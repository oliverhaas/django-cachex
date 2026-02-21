"""Two-tiered cache backends with L1 in-process memory + L2 Redis/Valkey.

Adds a fast in-process memory layer (Django's LocMemCache) on top of any
django-cachex Redis/Valkey backend. Hot reads are served from local memory,
falling through to the remote backend on miss. Staleness is bounded by a
short L1 TTL (default 5 seconds).

Configuration::

    CACHES = {
        "default": {
            "BACKEND": "django_cachex.cache.RedisTieredCache",
            "LOCATION": "redis://127.0.0.1:6379/0",
            "OPTIONS": {
                "TIERED_L1_TIMEOUT": 5,        # L1 TTL in seconds
                "TIERED_L1_MAX_ENTRIES": 1000,  # L1 max entries before LRU cull
            }
        }
    }
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.core.cache.backends.locmem import LocMemCache

if TYPE_CHECKING:
    from collections.abc import Mapping

    from django_cachex.types import KeyT

# Sentinel to distinguish "not in L1" from a stored None
_L1_MISS = object()


class TieredCacheMixin:
    """Mixin that adds L1 in-process caching to any KeyValueCache backend.

    Must appear before the cache backend in MRO::

        class RedisTieredCache(TieredCacheMixin, RedisCache):
            pass

    Core operations (get/set/delete/get_many/set_many/delete_many/has_key/
    incr/touch/clear) check L1 first and fall through to L2 on miss.
    Data structure operations (hashes, lists, sets, sorted sets, streams)
    bypass L1 and go directly to the remote backend.
    """

    _l1: LocMemCache
    _l1_timeout: int

    def __init__(self, server: str, params: dict[str, Any]) -> None:
        options = params.get("OPTIONS", {})
        l1_timeout = options.pop("TIERED_L1_TIMEOUT", 5)
        l1_max_entries = options.pop("TIERED_L1_MAX_ENTRIES", 1000)
        super().__init__(server, params)  # type: ignore[call-arg]  # ty: ignore[too-many-positional-arguments]
        self._l1_timeout = l1_timeout
        self._l1 = LocMemCache(
            f"django-cachex-tiered-{id(self)}",
            {"TIMEOUT": l1_timeout, "OPTIONS": {"MAX_ENTRIES": l1_max_entries}},
        )

    # =========================================================================
    # Core operations — L1 + L2
    # =========================================================================

    def get(self, key: KeyT, default: Any = None, version: int | None = None) -> Any:
        val = self._l1.get(key, _L1_MISS, version=version)
        if val is not _L1_MISS:
            return val
        val = super().get(key, _L1_MISS, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        if val is not _L1_MISS:
            self._l1.set(key, val, self._l1_timeout, version=version)
            return val
        return default

    async def aget(self, key: KeyT, default: Any = None, version: int | None = None) -> Any:
        val = self._l1.get(key, _L1_MISS, version=version)
        if val is not _L1_MISS:
            return val
        val = await super().aget(key, _L1_MISS, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        if val is not _L1_MISS:
            self._l1.set(key, val, self._l1_timeout, version=version)
            return val
        return default

    def set(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        **kwargs: Any,
    ) -> Any:
        result = super().set(key, value, timeout, version=version, **kwargs)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        nx = kwargs.get("nx", False)
        xx = kwargs.get("xx", False)
        get = kwargs.get("get", False)
        if nx or xx:
            # With nx/xx + get, the return value is ambiguous — skip L1
            if not get and result:
                self._l1.set(key, value, self._l1_timeout, version=version)
        else:
            self._l1.set(key, value, self._l1_timeout, version=version)
        return result

    async def aset(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        **kwargs: Any,
    ) -> Any:
        result = await super().aset(key, value, timeout, version=version, **kwargs)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        nx = kwargs.get("nx", False)
        xx = kwargs.get("xx", False)
        get = kwargs.get("get", False)
        if nx or xx:
            if not get and result:
                self._l1.set(key, value, self._l1_timeout, version=version)
        else:
            self._l1.set(key, value, self._l1_timeout, version=version)
        return result

    def add(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        result = super().add(key, value, timeout, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        if result:
            self._l1.set(key, value, self._l1_timeout, version=version)
        return result

    async def aadd(
        self,
        key: KeyT,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        result = await super().aadd(key, value, timeout, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        if result:
            self._l1.set(key, value, self._l1_timeout, version=version)
        return result

    def delete(self, key: KeyT, version: int | None = None) -> bool:
        self._l1.delete(key, version=version)
        return super().delete(key, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    async def adelete(self, key: KeyT, version: int | None = None) -> bool:
        self._l1.delete(key, version=version)
        return await super().adelete(key, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    def get_many(self, keys: list[KeyT], version: int | None = None) -> dict[KeyT, Any]:
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

        l2_results = super().get_many(missed_keys, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        for key, val in l2_results.items():
            self._l1.set(key, val, self._l1_timeout, version=version)

        l1_results.update(l2_results)
        return l1_results

    async def aget_many(self, keys: list[KeyT], version: int | None = None) -> dict[KeyT, Any]:
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

        l2_results = await super().aget_many(missed_keys, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        for key, val in l2_results.items():
            self._l1.set(key, val, self._l1_timeout, version=version)

        l1_results.update(l2_results)
        return l1_results

    def set_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list:
        for key, value in data.items():
            self._l1.set(key, value, self._l1_timeout, version=version)
        return super().set_many(data, timeout, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    async def aset_many(
        self,
        data: Mapping[KeyT, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list:
        for key, value in data.items():
            self._l1.set(key, value, self._l1_timeout, version=version)
        return await super().aset_many(data, timeout, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    def delete_many(self, keys: list[KeyT], version: int | None = None) -> int:
        for key in keys:
            self._l1.delete(key, version=version)
        return super().delete_many(keys, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    async def adelete_many(self, keys: list[KeyT], version: int | None = None) -> int:
        for key in keys:
            self._l1.delete(key, version=version)
        return await super().adelete_many(keys, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    def has_key(self, key: KeyT, version: int | None = None) -> bool:
        if self._l1.has_key(key, version=version):
            return True
        return super().has_key(key, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    async def ahas_key(self, key: KeyT, version: int | None = None) -> bool:
        if self._l1.has_key(key, version=version):
            return True
        return await super().ahas_key(key, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    def incr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        self._l1.delete(key, version=version)
        return super().incr(key, delta, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    async def aincr(self, key: KeyT, delta: int = 1, version: int | None = None) -> int:
        self._l1.delete(key, version=version)
        return await super().aincr(key, delta, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    def touch(self, key: KeyT, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        result = super().touch(key, timeout, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        if result:
            self._l1.touch(key, self._l1_timeout, version=version)
        return result

    async def atouch(self, key: KeyT, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        result = await super().atouch(key, timeout, version=version)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        if result:
            self._l1.touch(key, self._l1_timeout, version=version)
        return result

    # =========================================================================
    # Bulk / pattern operations — clear L1 entirely
    # =========================================================================

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        self._l1.clear()
        return super().delete_pattern(pattern, version=version, itersize=itersize)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    async def adelete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        self._l1.clear()
        return await super().adelete_pattern(pattern, version=version, itersize=itersize)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    def flush_db(self) -> bool:
        self._l1.clear()
        return super().flush_db()  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    async def aflush_db(self) -> bool:
        self._l1.clear()
        return await super().aflush_db()  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    def close(self, **kwargs: Any) -> None:
        self._l1.close()
        super().close(**kwargs)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    async def aclose(self, **kwargs: Any) -> None:
        self._l1.close()
        await super().aclose(**kwargs)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]


# =============================================================================
# Concrete tiered cache classes
# =============================================================================

from django_cachex.cache.default import RedisCache, ValkeyCache  # noqa: E402


class RedisTieredCache(TieredCacheMixin, RedisCache):  # type: ignore[misc]
    """Two-tiered cache: L1 in-process memory + L2 Redis."""


class ValkeyTieredCache(TieredCacheMixin, ValkeyCache):  # type: ignore[misc]
    """Two-tiered cache: L1 in-process memory + L2 Valkey."""


# Cluster variants
from django_cachex.cache.cluster import RedisClusterCache, ValkeyClusterCache  # noqa: E402


class RedisClusterTieredCache(TieredCacheMixin, RedisClusterCache):  # type: ignore[misc]
    """Two-tiered cache: L1 in-process memory + L2 Redis Cluster."""


class ValkeyClusterTieredCache(TieredCacheMixin, ValkeyClusterCache):  # type: ignore[misc]
    """Two-tiered cache: L1 in-process memory + L2 Valkey Cluster."""


# Sentinel variants
from django_cachex.cache.sentinel import RedisSentinelCache, ValkeySentinelCache  # noqa: E402


class RedisSentinelTieredCache(TieredCacheMixin, RedisSentinelCache):  # type: ignore[misc]
    """Two-tiered cache: L1 in-process memory + L2 Redis Sentinel."""


class ValkeySentinelTieredCache(TieredCacheMixin, ValkeySentinelCache):  # type: ignore[misc]
    """Two-tiered cache: L1 in-process memory + L2 Valkey Sentinel."""


__all__ = [
    "RedisClusterTieredCache",
    "RedisSentinelTieredCache",
    "RedisTieredCache",
    "TieredCacheMixin",
    "ValkeyClusterTieredCache",
    "ValkeySentinelTieredCache",
    "ValkeyTieredCache",
]
