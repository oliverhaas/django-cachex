"""Process-wide registry of Rust driver instances.

One driver per (URL, options) tuple, shared across cache instances. PID-checked
so post-fork children rebuild their own connection pools instead of inheriting
the parent's tokio runtime / sockets.
"""

from __future__ import annotations

import os
import threading
from typing import TYPE_CHECKING

from django_cachex._driver import RustValkeyDriver  # ty: ignore[unresolved-import]

if TYPE_CHECKING:
    from collections.abc import Hashable

_CLIENTS: dict[Hashable, RustValkeyDriver] = {}
_PID = os.getpid()
_LOCK = threading.Lock()


def _check_pid() -> None:
    global _PID  # noqa: PLW0603
    pid = os.getpid()
    if pid != _PID:
        _CLIENTS.clear()
        _PID = pid


def _get_or_create(key: Hashable, factory) -> RustValkeyDriver:  # noqa: ANN001
    _check_pid()
    with _LOCK:
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
    key = ("standard", url, cache_max_size, cache_ttl_secs, ssl_ca_certs, ssl_certfile, ssl_keyfile)
    return _get_or_create(
        key,
        lambda: RustValkeyDriver.connect_standard(
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
    key = ("cluster", tuple(urls), ssl_ca_certs, ssl_certfile, ssl_keyfile)
    return _get_or_create(
        key,
        lambda: RustValkeyDriver.connect_cluster(
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
    key = (
        "sentinel",
        tuple(sentinel_urls),
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
        lambda: RustValkeyDriver.connect_sentinel(
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
