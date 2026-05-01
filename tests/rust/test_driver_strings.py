def test_get_set_basic(driver):
    driver.set("k", b"v")
    assert driver.get("k") == b"v"


def test_get_missing(driver):
    assert driver.get("nope") is None


def test_set_with_ttl_and_pttl(driver):
    driver.set("k", b"v", ttl=100)
    assert driver.ttl("k") in range(95, 101)  # account for clock skew
    assert driver.pttl("k") <= 100_000


def test_set_nx_only_when_absent(driver):
    assert driver.set_nx("k", b"v1") is True
    assert driver.set_nx("k", b"v2") is False
    assert driver.get("k") == b"v1"


def test_delete_returns_count(driver):
    driver.set("k", b"v")
    assert driver.delete("k") == 1
    assert driver.delete("k") == 0


def test_delete_many(driver):
    driver.set("a", b"1")
    driver.set("b", b"2")
    driver.set("c", b"3")
    assert driver.delete_many(["a", "b", "missing", "c"]) == 3


def test_mget(driver):
    driver.set("a", b"1")
    driver.set("b", b"2")
    assert driver.mget(["a", "missing", "b"]) == [b"1", None, b"2"]


def test_pipeline_set(driver):
    driver.pipeline_set([("a", b"1"), ("b", b"2"), ("c", b"3")])
    assert driver.mget(["a", "b", "c"]) == [b"1", b"2", b"3"]


def test_pipeline_set_with_ttl(driver):
    driver.pipeline_set([("a", b"1"), ("b", b"2")], ttl=100)
    assert driver.ttl("a") in range(95, 101)


def test_incr_by(driver):
    assert driver.incr_by("counter", 5) == 5
    assert driver.incr_by("counter", 3) == 8
    assert driver.incr_by("counter", -2) == 6


def test_exists(driver):
    assert driver.exists("k") is False
    driver.set("k", b"v")
    assert driver.exists("k") is True


def test_expire_and_persist(driver):
    driver.set("k", b"v")
    assert driver.expire("k", 50) is True
    assert driver.ttl("k") in range(45, 51)
    assert driver.persist("k") is True
    assert driver.ttl("k") == -1  # no expiry


def test_ttl_codes(driver):
    assert driver.ttl("missing") == -2  # key doesn't exist
    driver.set("k", b"v")
    assert driver.ttl("k") == -1  # no expiry


def test_keys_pattern(driver):
    driver.set("u:1", b"a")
    driver.set("u:2", b"b")
    driver.set("other", b"c")
    keys = sorted(driver.keys("u:*"))
    assert keys == ["u:1", "u:2"]


def test_scan(driver):
    for i in range(20):
        driver.set(f"k:{i}", b"x")
    keys = driver.scan("k:*", 10)
    assert len(keys) == 20


def test_type(driver):
    driver.set("s", b"v")
    driver.lpush("l", [b"x"])
    assert driver.type("s") == "string"
    assert driver.type("l") == "list"
    assert driver.type("missing") == "none"
