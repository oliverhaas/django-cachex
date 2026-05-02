from django_cachex._driver import RedisRsDriver


def test_connect_and_round_trip(redis_container):
    driver = RedisRsDriver.connect_standard(f"redis://{redis_container.host}:{redis_container.port}/0")
    driver.flushdb()

    driver.set("k", b"v")
    assert driver.get("k") == b"v"


def test_get_returns_none_for_missing_key(redis_container):
    driver = RedisRsDriver.connect_standard(f"redis://{redis_container.host}:{redis_container.port}/0")
    driver.flushdb()

    assert driver.get("nope") is None


def test_set_with_ttl(redis_container):
    driver = RedisRsDriver.connect_standard(f"redis://{redis_container.host}:{redis_container.port}/0")
    driver.flushdb()

    driver.set("k", b"v", ttl=100)
    assert driver.get("k") == b"v"


def test_connection_error_on_bad_host():
    import pytest

    with pytest.raises(ConnectionError):
        RedisRsDriver.connect_standard("redis://127.0.0.1:1/0")
