"""Process-wide registry of Rust driver instances.

One driver per (URL, options) tuple, shared across cache instances. PID-checked
so post-fork children rebuild their own connection pools instead of inheriting
the parent's tokio runtime / sockets.

The ``_driver`` extension lives in the optional ``django-cachex-rust``
companion package. Importing this module is supported without it — the
first attempt to construct a driver raises ``ImportError`` with an
install hint.
"""

from __future__ import annotations

import os
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable

    from django_cachex._driver import RustValkeyDriver


def _get_driver_class() -> type[RustValkeyDriver]:
    try:
        from django_cachex._driver import RustValkeyDriver
    except ImportError as e:
        raise ImportError(
            "django-cachex Rust driver is not available. Install with the "
            "`redis-rs` extra to pull in the django-cachex-rust binary "
            "package: pip install django-cachex[redis-rs]. Prebuilt wheels "
            "are published for Linux x86_64 (cp314, cp314t); on other "
            "platforms pip will try to build from source via the Rust "
            "toolchain.",
        ) from e
    return RustValkeyDriver


_CLIENTS: dict[Hashable, Any] = {}
_PID = os.getpid()
_LOCK = threading.Lock()


def _get_or_create(key: Hashable, factory: Callable[[], RustValkeyDriver]) -> RustValkeyDriver:
    # Lock wraps both the PID check and the dict ops so a concurrent fork-detect
    # under free-threaded 3.14t can't race two clears or skip an in-flight insert.
    # `factory()` is a blocking connect(); double-checked locking would let two
    # callers race the same connect.
    global _PID  # noqa: PLW0603
    with _LOCK:
        pid = os.getpid()
        if pid != _PID:
            _CLIENTS.clear()
            _PID = pid
        driver = _CLIENTS.get(key)
        if driver is None:
            driver = factory()
            _CLIENTS[key] = driver
        return driver


def get_driver_standard(
    url: str,
    *,
    cache_max_size: int | None = None,
    cache_ttl_secs: int | None = None,
    ssl_ca_certs: str | None = None,
    ssl_certfile: str | None = None,
    ssl_keyfile: str | None = None,
) -> RustValkeyDriver:
    driver_cls = _get_driver_class()
    key = ("standard", url, cache_max_size, cache_ttl_secs, ssl_ca_certs, ssl_certfile, ssl_keyfile)
    return _get_or_create(
        key,
        lambda: driver_cls.connect_standard(
            url,
            cache_max_size=cache_max_size,
            cache_ttl_secs=cache_ttl_secs,
            ssl_ca_certs=ssl_ca_certs,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
        ),
    )


def get_driver_cluster(
    urls: list[str],
    *,
    ssl_ca_certs: str | None = None,
    ssl_certfile: str | None = None,
    ssl_keyfile: str | None = None,
) -> RustValkeyDriver:
    driver_cls = _get_driver_class()
    key = ("cluster", tuple(urls), ssl_ca_certs, ssl_certfile, ssl_keyfile)
    return _get_or_create(
        key,
        lambda: driver_cls.connect_cluster(
            list(urls),
            ssl_ca_certs=ssl_ca_certs,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
        ),
    )


def get_driver_sentinel(
    sentinel_urls: list[str],
    service_name: str,
    db: int,
    *,
    cache_max_size: int | None = None,
    cache_ttl_secs: int | None = None,
    ssl_ca_certs: str | None = None,
    ssl_certfile: str | None = None,
    ssl_keyfile: str | None = None,
) -> RustValkeyDriver:
    # Sentinel nodes are equivalent — sort so reordered lists hit the same driver.
    # (Cluster URLs are NOT sorted: node order can affect initial topology probe.)
    driver_cls = _get_driver_class()
    key = (
        "sentinel",
        tuple(sorted(sentinel_urls)),
        service_name,
        db,
        cache_max_size,
        cache_ttl_secs,
        ssl_ca_certs,
        ssl_certfile,
        ssl_keyfile,
    )
    return _get_or_create(
        key,
        lambda: driver_cls.connect_sentinel(
            list(sentinel_urls),
            service_name,
            db,
            cache_max_size=cache_max_size,
            cache_ttl_secs=cache_ttl_secs,
            ssl_ca_certs=ssl_ca_certs,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
        ),
    )


def _reset_for_tests() -> None:
    """Clear the registry. Test-only — production code must not call this."""
    with _LOCK:
        _CLIENTS.clear()
