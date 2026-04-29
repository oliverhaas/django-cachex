from django_cachex._driver import RustValkeyDriver


def test_connect_and_round_trip(redis_container):
    driver = RustValkeyDriver.connect_standard(f"redis://{redis_container.host}:{redis_container.port}/0")
    driver.flushdb_sync()

    driver.set_sync("k", b"v")
    assert driver.get_sync("k") == b"v"


def test_get_returns_none_for_missing_key(redis_container):
    driver = RustValkeyDriver.connect_standard(f"redis://{redis_container.host}:{redis_container.port}/0")
    driver.flushdb_sync()

    assert driver.get_sync("nope") is None


def test_set_with_ttl(redis_container):
    driver = RustValkeyDriver.connect_standard(f"redis://{redis_container.host}:{redis_container.port}/0")
    driver.flushdb_sync()

    driver.set_sync("k", b"v", ttl=100)
    assert driver.get_sync("k") == b"v"


def test_connection_error_on_bad_host():
    import pytest

    with pytest.raises(ConnectionError):
        RustValkeyDriver.connect_standard("redis://127.0.0.1:1/0")
