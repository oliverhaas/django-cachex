"""Per-cache-test-suite fixtures.

Backend-specific cache fixtures (TieredCache, SyncCache) live here so they
can be shared between sync and async test modules without re-importing
across files. The general-purpose ``cache`` fixture lives in
``tests/conftest.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.test import override_settings

from tests.fixtures.cache import BACKENDS, _get_client_library_options

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django.core.cache.backends.base import BaseCache

    from tests.fixtures.containers import RedisContainerInfo

L1_TIMEOUT = 2  # seconds — short L1 cap for testing


def _build_tiered_config(host: str, port: int, client_library: str = "redis") -> dict:
    """Build CACHES config with l1 (LocMemCache), l2 (Redis/Valkey), and tiered."""
    options = _get_client_library_options(client_library)
    location = f"redis://{host}:{port}?db=1"
    backend_class = BACKENDS[("default", client_library, "py")]

    return {
        "l1": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "OPTIONS": {"MAX_ENTRIES": 100},
        },
        "l2": {
            "BACKEND": backend_class,
            "LOCATION": location,
            "OPTIONS": options,
        },
        "default": {
            "BACKEND": "django_cachex.cache.TieredCache",
            "OPTIONS": {
                "TIERS": ["l1", "l2"],
                "L1_TIMEOUT": L1_TIMEOUT,
            },
        },
    }


@pytest.fixture
def tiered_cache(redis_container: RedisContainerInfo) -> Iterator[BaseCache]:
    """Tiered cache fixture with short L1 TTL for testing."""
    config = _build_tiered_config(
        redis_container.host,
        redis_container.port,
        client_library=redis_container.client_library,
    )

    with override_settings(CACHES=config):
        cache = caches["default"]
        cache.clear()
        yield cache
        cache.clear()
