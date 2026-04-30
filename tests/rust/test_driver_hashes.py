def test_hset_hget(driver):
    driver.hset_sync("h", "f", b"v")
    assert driver.hget_sync("h", "f") == b"v"
    assert driver.hget_sync("h", "missing") is None


def test_hgetall(driver):
    driver.hset_sync("h", "a", b"1")
    driver.hset_sync("h", "b", b"2")
    result = driver.hgetall_sync("h")
    assert result == {b"a": b"1", b"b": b"2"}


def test_hdel(driver):
    driver.hset_sync("h", "a", b"1")
    driver.hset_sync("h", "b", b"2")
    driver.hset_sync("h", "c", b"3")
    assert driver.hdel_sync("h", ["a", "b", "missing"]) == 2
    assert driver.hgetall_sync("h") == {b"c": b"3"}


def test_hincrby(driver):
    assert driver.hincrby_sync("h", "n", 5) == 5
    assert driver.hincrby_sync("h", "n", 3) == 8
    assert driver.hincrby_sync("h", "n", -2) == 6


def test_hkeys_hvals(driver):
    driver.hset_sync("h", "a", b"1")
    driver.hset_sync("h", "b", b"2")
    assert sorted(driver.hkeys_sync("h")) == ["a", "b"]
    assert sorted(driver.hvals_sync("h")) == [b"1", b"2"]


def test_hexists(driver):
    driver.hset_sync("h", "f", b"v")
    assert driver.hexists_sync("h", "f") is True
    assert driver.hexists_sync("h", "missing") is False


def test_hlen(driver):
    driver.hset_sync("h", "a", b"1")
    driver.hset_sync("h", "b", b"2")
    assert driver.hlen_sync("h") == 2


def test_hmget(driver):
    driver.hset_sync("h", "a", b"1")
    driver.hset_sync("h", "b", b"2")
    assert driver.hmget_sync("h", ["a", "missing", "b"]) == [b"1", None, b"2"]


def test_hmset(driver):
    driver.hmset_sync("h", [("a", b"1"), ("b", b"2"), ("c", b"3")])
    assert driver.hgetall_sync("h") == {b"a": b"1", b"b": b"2", b"c": b"3"}
