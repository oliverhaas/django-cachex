"""Shared fixtures for rust driver tests."""

import pytest

# Skip the whole directory when the optional django-cachex-redis-rs package
# isn't installed (e.g. running against the pure wheel only).
pytest.importorskip("django_cachex._driver")

from django_cachex._driver import RedisRsDriver  # ty: ignore[unresolved-import]


@pytest.fixture
def driver(redis_container) -> RedisRsDriver:
    d = RedisRsDriver.connect_standard(
        f"redis://{redis_container.host}:{redis_container.port}/0",
    )
    d.flushdb()
    return d
