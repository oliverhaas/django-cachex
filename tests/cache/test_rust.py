"""Tests that exercise driver-specific behavior of the Rust extension.

The basic CRUD / hash / list / set / zset / TTL surface is already covered
by the parametrized ``cache`` fixture (which runs every test in
``test_basic.py`` & friends against ``driver=rust``). This file is
limited to behavior that's specific to the Rust driver: lazy connection,
the ``RedisRsDriver`` raw client object, and regression tests for bugs
that only surfaced in the Rust path.
"""

from typing import TYPE_CHECKING

import pytest
from django.test import override_settings

from django_cachex._redis_rs_clients import _reset_for_tests

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django_cachex.cache import RespCache
    from tests.fixtures.containers import RedisContainerInfo


@pytest.fixture(autouse=True)
def _clear_rust_registry() -> Iterator[None]:
    """Driver registry leaks between tests if not cleared (per-test flushdb)."""
    _reset_for_tests()
    yield
    _reset_for_tests()


@pytest.fixture
def rust_cache(redis_container: RedisContainerInfo) -> Iterator[RespCache]:
    location = f"redis://{redis_container.host}:{redis_container.port}/0"
    caches = {
        "default": {
            "BACKEND": "django_cachex.cache.RedisRsCache",
            "LOCATION": location,
            "OPTIONS": {},
        },
    }
    with override_settings(CACHES=caches):
        from django.core.cache import cache

        cache.flush_db()
        yield cache  # type: ignore[misc]
        cache.flush_db()


# --------------------------------------------------------------- raw client


def test_get_raw_client_returns_driver(rust_cache):
    from django_cachex._driver import RedisRsDriver  # ty: ignore[unresolved-import]

    raw = rust_cache.adapter.get_raw_client()
    assert isinstance(raw, RedisRsDriver)
    raw.set("rawkey", b"rawval")
    assert raw.get("rawkey") == b"rawval"


# ------------------------------------------------------- lazy connection


def test_unreachable_server_does_not_raise_at_construction(redis_container):
    """Driver must connect lazily so IGNORE_EXCEPTIONS-style wrappers can catch errors."""
    caches = {
        "default": {
            "BACKEND": "django_cachex.cache.RedisRsCache",
            "LOCATION": "redis://127.0.0.1:1/0",
            "OPTIONS": {},
        },
    }
    with override_settings(CACHES=caches):
        from django.core.cache import cache

        client = cache.adapter
        assert client is not None
        with pytest.raises(ConnectionError):
            cache.get("k")


# -------------------------------------------------------- driver-specific lock


def test_lock_extend_replace_ttl_raises(rust_cache):
    """``replace_ttl=True`` is not implementable on the current driver — must raise loudly."""
    lock = rust_cache.lock("mylock", timeout=5)
    lock.acquire()
    try:
        with pytest.raises(NotImplementedError):
            lock.extend(10, replace_ttl=True)
    finally:
        lock.release()


# ----------------------------------------------------------- regression cases


def test_info_section_filter(rust_cache):
    """``cache.info(section=...)`` must restrict the response to that section."""
    full = rust_cache.adapter.info()
    section = rust_cache.adapter.info(section="server")
    # ``redis_version`` lives in the ``server`` section; ``role`` lives in
    # ``replication``. The filtered view must contain the former and not the latter.
    assert "redis_version" in section
    assert "role" not in section
    assert "redis_version" in full
    assert "role" in full


def test_rename_missing_source_raises_valueerror(rust_cache):
    """RENAME on a missing key must surface as ValueError, not RuntimeError."""
    with pytest.raises(ValueError, match="not found"):
        rust_cache.adapter.rename("missing-src", "dst")


def test_renamenx_missing_source_raises_valueerror(rust_cache):
    with pytest.raises(ValueError, match="not found"):
        rust_cache.adapter.renamenx("missing-src", "dst")


def test_eval_bool_arg_encoded_as_int(rust_cache):
    """Bools must marshal to 0/1, matching redis-py's ARGV semantics."""
    assert rust_cache.adapter.eval("return ARGV[1]", 0, True) == b"1"
    assert rust_cache.adapter.eval("return ARGV[1]", 0, False) == b"0"


def test_hincrbyfloat_returns_running_total(rust_cache):
    """HINCRBYFLOAT goes via Lua eval — the running total must accumulate."""
    assert rust_cache.hincrbyfloat("h", "f", 1.5) == 1.5
    assert rust_cache.hincrbyfloat("h", "f", 2.25) == 3.75
