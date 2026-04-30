def test_get_set_basic(driver):
    driver.set_sync("k", b"v")
    assert driver.get_sync("k") == b"v"


def test_get_missing(driver):
    assert driver.get_sync("nope") is None


def test_set_with_ttl_and_pttl(driver):
    driver.set_sync("k", b"v", ttl=100)
    assert driver.ttl_sync("k") in range(95, 101)  # account for clock skew
    assert driver.pttl_sync("k") <= 100_000


def test_set_nx_only_when_absent(driver):
    assert driver.set_nx_sync("k", b"v1") is True
    assert driver.set_nx_sync("k", b"v2") is False
    assert driver.get_sync("k") == b"v1"


def test_delete_returns_count(driver):
    driver.set_sync("k", b"v")
    assert driver.delete_sync("k") == 1
    assert driver.delete_sync("k") == 0


def test_delete_many(driver):
    driver.set_sync("a", b"1")
    driver.set_sync("b", b"2")
    driver.set_sync("c", b"3")
    assert driver.delete_many_sync(["a", "b", "missing", "c"]) == 3


def test_mget(driver):
    driver.set_sync("a", b"1")
    driver.set_sync("b", b"2")
    assert driver.mget_sync(["a", "missing", "b"]) == [b"1", None, b"2"]


def test_pipeline_set(driver):
    driver.pipeline_set_sync([("a", b"1"), ("b", b"2"), ("c", b"3")])
    assert driver.mget_sync(["a", "b", "c"]) == [b"1", b"2", b"3"]


def test_pipeline_set_with_ttl(driver):
    driver.pipeline_set_sync([("a", b"1"), ("b", b"2")], ttl=100)
    assert driver.ttl_sync("a") in range(95, 101)


def test_incr_by(driver):
    assert driver.incr_by_sync("counter", 5) == 5
    assert driver.incr_by_sync("counter", 3) == 8
    assert driver.incr_by_sync("counter", -2) == 6


def test_exists(driver):
    assert driver.exists_sync("k") is False
    driver.set_sync("k", b"v")
    assert driver.exists_sync("k") is True


def test_expire_and_persist(driver):
    driver.set_sync("k", b"v")
    assert driver.expire_sync("k", 50) is True
    assert driver.ttl_sync("k") in range(45, 51)
    assert driver.persist_sync("k") is True
    assert driver.ttl_sync("k") == -1  # no expiry


def test_ttl_codes(driver):
    assert driver.ttl_sync("missing") == -2  # key doesn't exist
    driver.set_sync("k", b"v")
    assert driver.ttl_sync("k") == -1  # no expiry


def test_keys_pattern(driver):
    driver.set_sync("u:1", b"a")
    driver.set_sync("u:2", b"b")
    driver.set_sync("other", b"c")
    keys = sorted(driver.keys_sync("u:*"))
    assert keys == ["u:1", "u:2"]


def test_scan(driver):
    for i in range(20):
        driver.set_sync(f"k:{i}", b"x")
    keys = driver.scan_sync("k:*", 10)
    assert len(keys) == 20


def test_type(driver):
    driver.set_sync("s", b"v")
    driver.lpush_sync("l", [b"x"])
    assert driver.type_sync("s") == "string"
    assert driver.type_sync("l") == "list"
    assert driver.type_sync("missing") == "none"
