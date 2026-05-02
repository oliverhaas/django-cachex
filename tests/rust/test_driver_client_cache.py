from django_cachex._driver import RedisRsDriver


def test_cache_statistics_disabled_by_default(redis_container):
    driver = RedisRsDriver.connect_standard(f"redis://{redis_container.host}:{redis_container.port}/0")
    assert driver.cache_statistics() is None


def test_client_side_cache_records_hits(redis_container):
    driver = RedisRsDriver.connect_standard(
        f"redis://{redis_container.host}:{redis_container.port}/0",
        cache_max_size=128,
        cache_ttl_secs=60,
    )
    driver.flushdb()
    driver.set("k", b"v")

    # First read populates the client cache (miss).
    assert driver.get("k") == b"v"
    # Subsequent reads hit the local cache.
    for _ in range(5):
        assert driver.get("k") == b"v"

    stats = driver.cache_statistics()
    assert stats is not None
    hits, _misses, _invalidates = stats
    assert hits >= 1


def test_client_side_cache_invalidates_on_write(redis_container):
    driver = RedisRsDriver.connect_standard(
        f"redis://{redis_container.host}:{redis_container.port}/0",
        cache_max_size=128,
        cache_ttl_secs=60,
    )
    driver.flushdb()

    driver.set("k", b"v1")
    assert driver.get("k") == b"v1"  # populates cache
    assert driver.get("k") == b"v1"  # hit

    # Write the same key from a separate connection (server pushes invalidation).
    other = RedisRsDriver.connect_standard(f"redis://{redis_container.host}:{redis_container.port}/0")
    other.set("k", b"v2")

    # Read until we observe the new value (gives the invalidation push time to land).
    # 5s deadline tolerates slow CI runners; on a healthy box the push lands in <100ms.
    import time

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if driver.get("k") == b"v2":
            break
        time.sleep(0.02)
    assert driver.get("k") == b"v2"

    stats = driver.cache_statistics()
    assert stats is not None
    _hits, _misses, invalidates = stats
    assert invalidates >= 1
