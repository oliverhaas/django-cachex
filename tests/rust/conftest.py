"""Shared fixtures for rust driver tests."""

from __future__ import annotations

import pytest

from django_cachex._driver import RustValkeyDriver  # ty: ignore[unresolved-import]


@pytest.fixture
def driver(redis_container) -> RustValkeyDriver:
    d = RustValkeyDriver.connect_standard(
        f"redis://{redis_container.host}:{redis_container.port}/0",
    )
    d.flushdb()
    return d
