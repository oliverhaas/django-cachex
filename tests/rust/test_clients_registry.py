import os
from unittest.mock import patch

from django_cachex import _rust_clients
from django_cachex._rust_clients import (
    _CLIENTS,
    _reset_for_tests,
    get_driver_cluster,
    get_driver_sentinel,
    get_driver_standard,
)


def setup_function():
    _reset_for_tests()


def test_same_url_returns_same_driver(redis_container):
    url = f"redis://{redis_container.host}:{redis_container.port}/0"
    a = get_driver_standard(url)
    b = get_driver_standard(url)
    assert a is b


def test_different_options_get_different_drivers(redis_container):
    url = f"redis://{redis_container.host}:{redis_container.port}/0"
    a = get_driver_standard(url)
    b = get_driver_standard(url, cache_max_size=128)
    assert a is not b


def test_cluster_key_uses_url_tuple(cluster_container):
    host, port = cluster_container
    urls = [f"redis://{host}:{port + i}" for i in range(6)]
    a = get_driver_cluster(urls)
    b = get_driver_cluster(urls)
    assert a is b
    # Same set in different order is treated as a different key (intentional).
    c = get_driver_cluster(list(reversed(urls)))
    assert c is not a


def test_sentinel_key_includes_service_name_and_db(sentinel_container):
    urls = [f"redis://{sentinel_container.host}:{sentinel_container.port}"]
    a = get_driver_sentinel(urls, "mymaster", 0)
    b = get_driver_sentinel(urls, "mymaster", 0)
    assert a is b


def test_pid_change_clears_registry(redis_container):
    url = f"redis://{redis_container.host}:{redis_container.port}/0"
    first = get_driver_standard(url)
    assert _CLIENTS  # populated

    with patch.object(_rust_clients, "_PID", os.getpid() + 1):
        second = get_driver_standard(url)

    assert second is not first
    assert len(_CLIENTS) == 1  # registry was cleared, then repopulated with `second`
