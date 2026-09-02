"""Helpers shared by the cache-layer test modules."""

from typing import Any

from django_cachex.cache import RedisCache


def make_cache(*, key_prefix: str = "", **options: Any) -> RedisCache:
    """Build a :class:`RedisCache` from ``OPTIONS`` kwargs; ``adapter`` is lazy, so it never connects."""
    return RedisCache(
        server="redis://localhost:6379/0",
        params={
            "OPTIONS": {name: value for name, value in options.items() if value is not None},
            "KEY_PREFIX": key_prefix,
        },
    )
