def test_hset_hget(driver):
    driver.hset("h", "f", b"v")
    assert driver.hget("h", "f") == b"v"
    assert driver.hget("h", "missing") is None


def test_hgetall(driver):
    driver.hset("h", "a", b"1")
    driver.hset("h", "b", b"2")
    result = driver.hgetall("h")
    assert result == {b"a": b"1", b"b": b"2"}


def test_hdel(driver):
    driver.hset("h", "a", b"1")
    driver.hset("h", "b", b"2")
    driver.hset("h", "c", b"3")
    assert driver.hdel("h", ["a", "b", "missing"]) == 2
    assert driver.hgetall("h") == {b"c": b"3"}


def test_hincrby(driver):
    assert driver.hincrby("h", "n", 5) == 5
    assert driver.hincrby("h", "n", 3) == 8
    assert driver.hincrby("h", "n", -2) == 6


def test_hkeys_hvals(driver):
    driver.hset("h", "a", b"1")
    driver.hset("h", "b", b"2")
    assert sorted(driver.hkeys("h")) == ["a", "b"]
    assert sorted(driver.hvals("h")) == [b"1", b"2"]


def test_hexists(driver):
    driver.hset("h", "f", b"v")
    assert driver.hexists("h", "f") is True
    assert driver.hexists("h", "missing") is False


def test_hlen(driver):
    driver.hset("h", "a", b"1")
    driver.hset("h", "b", b"2")
    assert driver.hlen("h") == 2


def test_hmget(driver):
    driver.hset("h", "a", b"1")
    driver.hset("h", "b", b"2")
    assert driver.hmget("h", ["a", "missing", "b"]) == [b"1", None, b"2"]


def test_hmset(driver):
    driver.hmset("h", [("a", b"1"), ("b", b"2"), ("c", b"3")])
    assert driver.hgetall("h") == {b"a": b"1", b"b": b"2", b"c": b"3"}
