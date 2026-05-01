from django_cachex._driver import RustValkeyDriver


def test_connect_sentinel_and_round_trip(sentinel_container):
    driver = RustValkeyDriver.connect_sentinel(
        [f"redis://{sentinel_container.host}:{sentinel_container.port}"],
        "mymaster",
        0,
    )
    driver.flushdb()

    driver.set("k", b"v")
    assert driver.get("k") == b"v"


def test_sentinel_with_no_quorum_fails(sentinel_container):
    import pytest

    with pytest.raises(ConnectionError):
        RustValkeyDriver.connect_sentinel(
            [f"redis://{sentinel_container.host}:{sentinel_container.port}"],
            "no-such-service",
            0,
        )
