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
            "BACKEND": "django_cachex.cache.LocMemCache",
            "OPTIONS": {"MAX_ENTRIES": 1000},
        },
        "l2": {
            "BACKEND": "django_cachex.cache.RedisCache",
            "LOCATION": "redis://127.0.0.1:6379/0",
        },
        "default": {
            "BACKEND": "django_cachex.cache.TieredCache",
            "OPTIONS": {
                "tiers": ["l1", "l2"],
                "l1_timeout": 5,
            },
        },
    }

``l1_timeout`` caps how long entries live in L1. If omitted, falls back
to L1's own ``TIMEOUT`` setting.

Use ``django_cachex.cache.LocMemCache`` (not Django's stock
``django.core.cache.backends.locmem.LocMemCache``) as L1 if you ever call
``delete_pattern``: targeted L1 invalidation needs ``delete_pattern`` or
``iter_keys`` on L1, and an L1 with neither leaves ``clear()`` as the only
way to keep the tiers coherent, evicting every cached entry.
"""

from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.core.exceptions import ImproperlyConfigured

from django_cachex.cache.base import BaseCachex, CachexSupportLevel
from django_cachex.exceptions import NotSupportedError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator
    from datetime import timedelta

# Sentinel to distinguish "not in L1" from a stored None
_L1_MISS = object()


class TieredCache(BaseCachex):
    """Two-tiered cache referencing other CACHES entries as L1 and L2.

    L1 is checked first on reads; on miss, L2 is queried and L1 is populated.
    L1 TTL is capped by min(L1_TIMEOUT, L2's remaining TTL). Only the standard
    Django cache surface plus admin metadata (``keys``, ``ttl``, ``type``,
    ``info``, etc.) is delegated to L2; data-structure ops (``lpush``,
    ``hset``, ``zadd`` …) raise :class:`NotSupportedError`.
    """

    _cachex_support: CachexSupportLevel = "limited"

    def __init__(self, server: str, params: dict[str, Any]) -> None:
        super().__init__(params)
        options = params.get("OPTIONS", {})
        tiers = options.get("tiers")
        if not tiers or len(tiers) != 2:
            msg = (
                f"TieredCache requires OPTIONS['tiers'] with exactly 2 cache aliases, e.g. ['l1', 'l2']. Got: {tiers!r}"
            )
            raise ImproperlyConfigured(msg)
        if "KEY_PREFIX" in options:
            msg = (
                "TieredCache does not apply OPTIONS['KEY_PREFIX']; each tier "
                "prefixes its own keys. Set KEY_PREFIX on the tier cache aliases instead."
            )
            raise ImproperlyConfigured(msg)
        if params.get("KEY_PREFIX"):
            msg = (
                "TieredCache does not apply KEY_PREFIX; each tier prefixes its "
                "own keys. Set KEY_PREFIX on the tier cache aliases instead."
            )
            raise ImproperlyConfigured(msg)
        if tiers[0] == tiers[1]:
            msg = f"TieredCache requires two distinct cache aliases in OPTIONS['tiers']. Got: {tiers!r}"
            raise ImproperlyConfigured(msg)
        self._l1_alias: str = tiers[0]
        self._l2_alias: str = tiers[1]
        # L1 TTL cap: explicit option or fall back to L1's own default_timeout
        self._l1_max_timeout: float | None = options.get("l1_timeout")

    def _resolve_tier(self, alias: str) -> BaseCache:
        """Look up a tier by alias, rejecting a tier that is this cache itself.

        The alias TieredCache is registered under isn't passed to ``__init__``,
        so a self-referencing tier can only be caught here, on first use.
        Resolving it would otherwise recurse until ``RecursionError``.
        """
        from django.core.cache import caches

        tier = caches[alias]
        if tier is self:
            msg = (
                f"TieredCache tier alias {alias!r} resolves to the TieredCache itself. "
                f"OPTIONS['tiers'] must name two other cache aliases."
            )
            raise ImproperlyConfigured(msg)
        return tier

    @cached_property
    def _l1(self) -> BaseCache:
        return self._resolve_tier(self._l1_alias)

    @cached_property
    def _l2(self) -> BaseCachex:
        # Cast the ``BaseCache``-typed registry entry to the cachex shape;
        # cachex-only APIs still guard at runtime, so a stock L2 works.
        return cast("BaseCachex", self._resolve_tier(self._l2_alias))

    @property
    def _l1_cap(self) -> float | None:
        """L1 TTL cap: explicit ``l1_timeout`` option, or L1's own default_timeout."""
        cap = self._l1_max_timeout
        return cap if cap is not None else self._l1.default_timeout

    def _l1_timeout(self, l2_ttl: int | None = None) -> float | None:
        """Calculate L1 TTL: min(L1 cap, L2's remaining TTL).

        When ``l2_ttl`` is unavailable (e.g. ``ttl`` raised ``NotSupported``),
        fall back to ``min(L1 cap, L2.default_timeout)`` so a long-lived L1
        cap can't outlive an L2 entry written under L2's own default.
        """
        cap = self._l1_cap
        if l2_ttl is not None and l2_ttl > 0:
            if cap is not None:
                return min(cap, l2_ttl)
            return l2_ttl
        l2_default = self._l2.default_timeout
        if l2_default is None:
            return cap
        if cap is None:
            return l2_default
        return min(cap, l2_default)

    def _l1_timeout_for_set(self, timeout: float | None) -> float | None:  # noqa: PLR0911
        """Calculate L1 TTL for a set operation given the user-specified timeout.

        When ``timeout is DEFAULT_TIMEOUT``, L2 resolves the effective TTL
        against its own ``default_timeout``. L1 must clamp to that bound
        too; otherwise an L1 with a longer ``default_timeout`` than L2
        would outlive its L2 entry and serve stale.

        ``timeout is None`` (persistent) is safe: L1 capped finite, L2
        persistent => L2 always outlives L1.
        """
        cap = self._l1_cap
        if timeout is DEFAULT_TIMEOUT:
            l2_default = self._l2.default_timeout
            if l2_default is None:
                return cap
            if cap is None:
                return l2_default
            return min(cap, l2_default)
        if timeout is None:
            return cap
        if timeout <= 0:
            return timeout  # 0 means delete immediately
        if cap is not None:
            return min(cap, timeout)
        return timeout

    def _get_l2_ttl(self, key: str, version: int | None = None) -> int | None:
        """Try to get L2's remaining TTL for a key. Returns None if unsupported."""
        try:
            ttl = self._l2.ttl(key, version=version)
            return ttl if isinstance(ttl, int) and ttl > 0 else None
        except AttributeError, NotSupportedError, TypeError:
            return None

    async def _aget_l2_ttl(self, key: str, version: int | None = None) -> int | None:
        """Try to get L2's remaining TTL for a key asynchronously."""
        try:
            ttl = await self._l2.attl(key, version=version)
            return ttl if isinstance(ttl, int) and ttl > 0 else None
        except AttributeError, NotSupportedError, TypeError:
            return None

    @staticmethod
    def _normalize_ttls(keys: list[str], results: list[Any]) -> dict[str, int | None] | None:
        """Pair pipelined TTL results with their keys, or ``None`` if they don't line up."""
        if len(results) != len(keys):
            return None
        return {key: ttl if isinstance(ttl, int) and ttl > 0 else None for key, ttl in zip(keys, results, strict=True)}

    def _get_l2_ttls(self, keys: list[str], version: int | None = None) -> dict[str, int | None]:
        """Get L2's remaining TTL for several keys, batched into one round trip.

        ``get_many`` would otherwise pay one ``TTL`` call per key on top of the
        ``MGET``, which is the opposite of what a tier is for. Pipelining is
        best-effort: an L2 without it (stock Django backends, LocMem) falls
        back to the per-key path.
        """
        pipeline = getattr(self._l2, "pipeline", None)
        if callable(pipeline):
            try:
                with pipeline() as pipe:
                    for key in keys:
                        pipe.ttl(key, version=version)
                    results = pipe.execute()
            except AttributeError, NotSupportedError, TypeError:
                pass
            else:
                ttls = self._normalize_ttls(keys, results)
                if ttls is not None:
                    return ttls
        return {key: self._get_l2_ttl(key, version=version) for key in keys}

    async def _aget_l2_ttls(self, keys: list[str], version: int | None = None) -> dict[str, int | None]:
        """Async twin of :meth:`_get_l2_ttls`."""
        apipeline = getattr(self._l2, "apipeline", None)
        if callable(apipeline):
            try:
                async with await apipeline() as pipe:
                    for key in keys:
                        pipe.ttl(key, version=version)
                    results = await pipe.execute()
            except AttributeError, NotSupportedError, TypeError:
                pass
            else:
                ttls = self._normalize_ttls(keys, results)
                if ttls is not None:
                    return ttls
        return {key: await self._aget_l2_ttl(key, version=version) for key in keys}

    # =========================================================================
    # Standard Django cache interface
    # =========================================================================

    def get(self, key: str, default: Any = None, version: int | None = None) -> Any:
        val = self._l1.get(key, _L1_MISS, version=version)
        if val is not _L1_MISS:
            return val
        val = self._l2.get(key, _L1_MISS, version=version)
        if val is _L1_MISS:
            return default
        l2_ttl = self._get_l2_ttl(key, version=version)
        self._l1.set(key, val, self._l1_timeout(l2_ttl), version=version)
        return val

    async def aget(self, key: str, default: Any = None, version: int | None = None) -> Any:
        val = await self._l1.aget(key, _L1_MISS, version=version)
        if val is not _L1_MISS:
            return val
        val = await self._l2.aget(key, _L1_MISS, version=version)
        if val is _L1_MISS:
            return default
        l2_ttl = await self._aget_l2_ttl(key, version=version)
        await self._l1.aset(key, val, self._l1_timeout(l2_ttl), version=version)
        return val

    @staticmethod
    def _l2_write_happened(result: Any, *, nx: bool, xx: bool, get: bool) -> bool:
        """Whether the L2 ``set`` definitely wrote, given its return value.

        Without ``get``, ``nx``/``xx`` return a success bool and a plain set
        always writes. With ``get`` the L2 return is the prior value, not a
        success flag, so success is inferred from the conditional: an
        unconditional ``get`` always writes; ``nx`` writes only when the key was
        absent and ``xx`` only when it existed. A ``None`` prior under ``nx``/
        ``xx`` is ambiguous (an absent key and a cached ``None`` both decode to
        ``None``); :meth:`_sync_l1_after_set` handles that case separately, so
        this predicate may assume a non-``None`` prior there.
        """
        if get:
            if nx:
                return result is None
            if xx:
                return result is not None
            return True
        if nx or xx:
            return bool(result)
        return True

    def _sync_l1_after_set(
        self,
        key: str,
        value: Any,
        timeout: float | None,
        version: int | None,
        result: Any,
        *,
        nx: bool,
        xx: bool,
        get: bool,
    ) -> None:
        """Reconcile L1 with the outcome of an L2 ``set``.

        Mirror the new value into L1 when the write definitely landed. For a
        conditional write with ``get=True`` whose returned prior value is
        ``None`` we cannot tell an absent key from a key holding a cached
        ``None``, so we invalidate L1 instead of guessing, forcing a coherent
        re-read from L2.
        """
        if get and (nx or xx) and result is None:
            self._l1.delete(key, version=version)
        elif self._l2_write_happened(result, nx=nx, xx=xx, get=get):
            self._l1.set(key, value, self._l1_timeout_for_set(timeout), version=version)

    async def _async_l1_after_set(
        self,
        key: str,
        value: Any,
        timeout: float | None,
        version: int | None,
        result: Any,
        *,
        nx: bool,
        xx: bool,
        get: bool,
    ) -> None:
        """Async twin of :meth:`_sync_l1_after_set`."""
        if get and (nx or xx) and result is None:
            await self._l1.adelete(key, version=version)
        elif self._l2_write_happened(result, nx=nx, xx=xx, get=get):
            await self._l1.aset(key, value, self._l1_timeout_for_set(timeout), version=version)

    def _l2_set(
        self,
        key: str,
        value: Any,
        timeout: float | None,
        version: int | None,
        *,
        nx: bool,
        xx: bool,
        get: bool,
    ) -> Any:
        """Run the L2 write, degrading when L2 is a stock Django backend.

        Stock ``BaseCache.set`` accepts no ``nx``/``xx``/``get`` kwargs, so
        forwarding them would raise ``TypeError``: ``nx`` is emulated via
        ``add``, ``xx``/``get`` raise, and a plain set drops the flag kwargs.
        """
        if isinstance(self._l2, BaseCachex):
            return self._l2.set(key, value, timeout, version=version, nx=nx, xx=xx, get=get)
        if xx or get:
            raise NotSupportedError("set with xx/get", type(self._l2).__name__)
        if nx:
            return self._l2.add(key, value, timeout, version=version)
        return self._l2.set(key, value, timeout, version=version)

    async def _al2_set(
        self,
        key: str,
        value: Any,
        timeout: float | None,
        version: int | None,
        *,
        nx: bool,
        xx: bool,
        get: bool,
    ) -> Any:
        """Async twin of :meth:`_l2_set`."""
        if isinstance(self._l2, BaseCachex):
            return await self._l2.aset(key, value, timeout, version=version, nx=nx, xx=xx, get=get)
        if xx or get:
            raise NotSupportedError("aset with xx/get", type(self._l2).__name__)
        if nx:
            return await self._l2.aadd(key, value, timeout, version=version)
        return await self._l2.aset(key, value, timeout, version=version)

    def set(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
    ) -> Any:
        # Proxy the L2 return verbatim (prior value when ``get=True``, else the
        # nx/xx success bool, or ``None`` for a plain set) so the ``get=`` flag
        # contract is honored, then reconcile L1 with the actual write outcome.
        result = self._l2_set(key, value, timeout, version, nx=nx, xx=xx, get=get)
        self._sync_l1_after_set(key, value, timeout, version, result, nx=nx, xx=xx, get=get)
        return result

    async def aset(
        self,
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
    ) -> Any:
        result = await self._al2_set(key, value, timeout, version, nx=nx, xx=xx, get=get)
        await self._async_l1_after_set(key, value, timeout, version, result, nx=nx, xx=xx, get=get)
        return result

    def add(
        self,
        key: str,
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
        key: str,
        value: Any,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> bool:
        result = await self._l2.aadd(key, value, timeout, version=version)
        if result:
            await self._l1.aset(key, value, self._l1_timeout_for_set(timeout), version=version)
        return result

    # Mutate L2 before invalidating L1: the other order lets a concurrent read
    # repopulate L1 from the pre-mutation L2 value.

    def delete(self, key: str, version: int | None = None) -> bool:
        result = self._l2.delete(key, version=version)
        self._l1.delete(key, version=version)
        return result

    async def adelete(self, key: str, version: int | None = None) -> bool:
        result = await self._l2.adelete(key, version=version)
        await self._l1.adelete(key, version=version)
        return result

    def get_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        l1_results: dict[str, Any] = {}
        missed_keys: list[str] = []
        for key in keys:
            val = self._l1.get(key, _L1_MISS, version=version)
            if val is not _L1_MISS:
                l1_results[key] = val
            else:
                missed_keys.append(key)
        if not missed_keys:
            return l1_results
        l2_results = self._l2.get_many(missed_keys, version=version)
        l2_ttls = self._get_l2_ttls(list(l2_results), version=version)
        for key, val in l2_results.items():
            self._l1.set(key, val, self._l1_timeout(l2_ttls.get(key)), version=version)
        l1_results.update(l2_results)
        return l1_results

    async def aget_many(self, keys: Iterable[str], version: int | None = None) -> dict[str, Any]:
        l1_results: dict[str, Any] = {}
        missed_keys: list[str] = []
        for key in keys:
            val = await self._l1.aget(key, _L1_MISS, version=version)
            if val is not _L1_MISS:
                l1_results[key] = val
            else:
                missed_keys.append(key)
        if not missed_keys:
            return l1_results
        l2_results = await self._l2.aget_many(missed_keys, version=version)
        l2_ttls = await self._aget_l2_ttls(list(l2_results), version=version)
        for key, val in l2_results.items():
            await self._l1.aset(key, val, self._l1_timeout(l2_ttls.get(key)), version=version)
        l1_results.update(l2_results)
        return l1_results

    def set_many(
        self,
        data: dict[str, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[Any]:
        result = self._l2.set_many(data, timeout, version=version)
        # ``set_many`` returns the list of keys that L2 failed to write.
        # Don't propagate those to L1, otherwise readers would hit L1 and
        # get values that are missing from L2.
        failed = set(result or ())
        l1_timeout = self._l1_timeout_for_set(timeout)
        for key, value in data.items():
            if key not in failed:
                self._l1.set(key, value, l1_timeout, version=version)
        return result

    async def aset_many(
        self,
        data: dict[str, Any],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> list[Any]:
        result = await self._l2.aset_many(data, timeout, version=version)
        failed = set(result or ())
        l1_timeout = self._l1_timeout_for_set(timeout)
        for key, value in data.items():
            if key not in failed:
                await self._l1.aset(key, value, l1_timeout, version=version)
        return result

    # Django's ``BaseCache.delete_many``/``clear`` return ``None``; ``RespCache``
    # extends both to return ``int`` / ``bool``. A stock-Django ``None`` means
    # "done", not "failed", so it maps to success here rather than 0 / False.
    def delete_many(self, keys: Iterable[str], version: int | None = None) -> int:  # type: ignore[override]
        keys = list(keys)
        result: Any = self._l2.delete_many(keys, version=version)  # type: ignore[func-returns-value]
        for key in keys:
            self._l1.delete(key, version=version)
        return len(keys) if result is None else result

    async def adelete_many(self, keys: Iterable[str], version: int | None = None) -> int:  # type: ignore[override]
        keys = list(keys)
        result: Any = await self._l2.adelete_many(keys, version=version)  # type: ignore[func-returns-value]
        for key in keys:
            await self._l1.adelete(key, version=version)
        return len(keys) if result is None else result

    def has_key(self, key: str, version: int | None = None) -> bool:
        if self._l1.has_key(key, version=version):
            return True
        return self._l2.has_key(key, version=version)

    async def ahas_key(self, key: str, version: int | None = None) -> bool:
        if await self._l1.ahas_key(key, version=version):
            return True
        return await self._l2.ahas_key(key, version=version)

    def incr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        result = self._l2.incr(key, delta, version=version)
        self._l1.delete(key, version=version)
        return result

    async def aincr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        result = await self._l2.aincr(key, delta, version=version)
        await self._l1.adelete(key, version=version)
        return result

    def decr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        result = self._l2.decr(key, delta, version=version)
        self._l1.delete(key, version=version)
        return result

    async def adecr(self, key: str, delta: int = 1, version: int | None = None) -> int:
        result = await self._l2.adecr(key, delta, version=version)
        await self._l1.adelete(key, version=version)
        return result

    def touch(self, key: str, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        result = self._l2.touch(key, timeout, version=version)
        if result:
            self._l1.touch(key, self._l1_timeout_for_set(timeout), version=version)
        return result

    async def atouch(self, key: str, timeout: float | None = DEFAULT_TIMEOUT, version: int | None = None) -> bool:
        result = await self._l2.atouch(key, timeout, version=version)
        if result:
            await self._l1.atouch(key, self._l1_timeout_for_set(timeout), version=version)
        return result

    def clear(self) -> bool:  # type: ignore[override]
        result: Any = self._l2.clear()  # type: ignore[func-returns-value]
        self._l1.clear()
        return True if result is None else bool(result)

    async def aclear(self) -> bool:  # type: ignore[override]
        result: Any = await self._l2.aclear()  # type: ignore[func-returns-value]
        await self._l1.aclear()
        return True if result is None else bool(result)

    def _resolved_tiers(self) -> list[BaseCache]:
        """Tiers already resolved by :meth:`_resolve_tier`.

        Closing must not resolve a tier that was never used: building it can
        fail (a misconfigured alias) and there is nothing of ours to close.
        """
        return [tier for tier in (self.__dict__.get("_l1"), self.__dict__.get("_l2")) if tier is not None]

    def close(self, **kwargs: Any) -> None:
        for tier in self._resolved_tiers():
            tier.close(**kwargs)

    async def aclose(self, **kwargs: Any) -> None:
        for tier in self._resolved_tiers():
            await tier.aclose(**kwargs)

    # =========================================================================
    # Admin delegation methods (delegate to L2)
    # =========================================================================

    def _l2_method(self, method: str) -> Any:
        """Look up ``method`` on L2, raising NotSupportedError if it has none."""
        fn = getattr(self._l2, method, None)
        if fn is None:
            raise NotSupportedError(method, "TieredCache")
        return fn

    def _delegate(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Delegate a method call to L2, raising NotSupportedError if unavailable.

        Only the attribute lookup and an explicit ``NotSupportedError`` from L2
        are translated; an ``AttributeError`` raised inside L2's implementation
        is a bug there and propagates unchanged.
        """
        fn = self._l2_method(method)
        try:
            return fn(*args, **kwargs)
        except NotSupportedError as exc:
            raise NotSupportedError(method, "TieredCache") from exc

    async def _adelegate(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Async twin of :meth:`_delegate`."""
        fn = self._l2_method(method)
        try:
            return await fn(*args, **kwargs)
        except NotSupportedError as exc:
            raise NotSupportedError(method, "TieredCache") from exc

    @staticmethod
    def _wrap_iter(method: str, it: Iterator[str]) -> Iterator[str]:
        """Re-raise a lazily surfaced NotSupportedError as TieredCache's.

        A generator function returns without running its body, so L2's
        ``NotSupportedError`` escapes :meth:`_delegate` at iteration time.
        """
        try:
            yield from it
        except NotSupportedError as exc:
            raise NotSupportedError(method, "TieredCache") from exc

    @staticmethod
    async def _awrap_iter(method: str, it: AsyncIterator[str]) -> AsyncIterator[str]:
        """Async twin of :meth:`_wrap_iter`."""
        try:
            async for key in it:
                yield key
        except NotSupportedError as exc:
            raise NotSupportedError(method, "TieredCache") from exc

    def make_key(self, key: str, version: int | None = None) -> str:
        return self._delegate("make_key", key, version=version)

    def reverse_key(self, key: str) -> str:
        return self._delegate("reverse_key", key)

    def make_pattern(self, pattern: str, version: int | None = None) -> str:
        return self._delegate("make_pattern", pattern, version=version)

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

    def expire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        result = self._delegate("expire", key, timeout, version=version)
        self._l1.delete(key, version=version)
        return result

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        # Targeted L1 invalidation: clearing all of L1 for any pattern is a
        # footgun (a single ``delete_pattern("user:42:*")`` would evict every
        # cached entry). Prefer L1.delete_pattern; fall back to iter_keys +
        # delete; clear() only as the last resort if L1 supports neither.
        result = self._delegate("delete_pattern", pattern, version=version, itersize=itersize)
        self._invalidate_l1_by_pattern(pattern, version=version)
        return result

    def _invalidate_l1_by_pattern(self, pattern: str, version: int | None) -> None:
        """Remove L1 entries matching ``pattern`` without clearing the whole cache."""
        l1_delete_pattern = getattr(self._l1, "delete_pattern", None)
        if callable(l1_delete_pattern):
            try:
                l1_delete_pattern(pattern, version=version)
                return
            except NotSupportedError:
                pass
        if self._l1_targeted_delete(pattern, version=version):
            return
        # Last resort: L1 supports neither delete_pattern nor iter_keys.
        self._l1.clear()

    def _l1_targeted_delete(self, pattern: str, version: int | None) -> bool:
        """Iterate L1 keys matching ``pattern`` and delete each.

        Returns ``True`` if L1 supported ``iter_keys`` (regardless of how
        many keys matched), ``False`` if the caller should fall back to
        ``clear()``.
        """
        iter_keys = getattr(self._l1, "iter_keys", None)
        if not callable(iter_keys):
            return False
        try:
            keys = list(iter_keys(pattern, version=version))
        except NotSupportedError:
            return False
        for k in keys:
            self._l1.delete(k, version=version)
        return True

    async def _ainvalidate_l1_by_pattern(self, pattern: str, version: int | None) -> None:
        """Async twin of :meth:`_invalidate_l1_by_pattern`."""
        l1_delete_pattern = getattr(self._l1, "adelete_pattern", None)
        if callable(l1_delete_pattern):
            try:
                await l1_delete_pattern(pattern, version=version)
                return
            except NotSupportedError:
                pass
        if await self._al1_targeted_delete(pattern, version=version):
            return
        # Last resort: L1 supports neither adelete_pattern nor aiter_keys.
        await self._l1.aclear()

    async def _al1_targeted_delete(self, pattern: str, version: int | None) -> bool:
        """Async twin of :meth:`_l1_targeted_delete`."""
        aiter_keys = getattr(self._l1, "aiter_keys", None)
        if not callable(aiter_keys):
            return False
        try:
            keys = [k async for k in aiter_keys(pattern, version=version)]
        except NotSupportedError:
            return False
        for k in keys:
            await self._l1.adelete(k, version=version)
        return True

    async def akeys(self, pattern: str = "*", version: int | None = None) -> list[str]:
        return await self._adelegate("akeys", pattern, version=version)

    def aiter_keys(
        self,
        pattern: str = "*",
        version: int | None = None,
        itersize: int | None = None,
    ) -> AsyncIterator[str]:
        # Not ``async def``: L2's ``aiter_keys`` is itself a plain method
        # returning an async iterator, matching ``BaseCachex``.
        fn = self._l2_method("aiter_keys")
        try:
            it = fn(pattern, version=version, itersize=itersize)
        except NotSupportedError as exc:
            raise NotSupportedError("aiter_keys", "TieredCache") from exc
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

    async def aexpire(self, key: str, timeout: int | timedelta, version: int | None = None) -> bool:
        result = await self._adelegate("aexpire", key, timeout, version=version)
        await self._l1.adelete(key, version=version)
        return result

    async def adelete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        itersize: int | None = None,
    ) -> int:
        # See :meth:`delete_pattern` for why L1 is invalidated by pattern
        # rather than cleared.
        result = await self._adelegate("adelete_pattern", pattern, version=version, itersize=itersize)
        await self._ainvalidate_l1_by_pattern(pattern, version=version)
        return result


__all__ = [
    "TieredCache",
]
