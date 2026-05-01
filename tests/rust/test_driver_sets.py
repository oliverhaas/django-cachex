def test_sadd_smembers(driver):
    assert driver.sadd("s", [b"a", b"b", b"c"]) == 3
    assert sorted(driver.smembers("s")) == [b"a", b"b", b"c"]


def test_sadd_dedup(driver):
    driver.sadd("s", [b"a", b"b"])
    assert driver.sadd("s", [b"a", b"c"]) == 1  # only "c" is new


def test_srem(driver):
    driver.sadd("s", [b"a", b"b", b"c"])
    assert driver.srem("s", [b"a", b"c", b"missing"]) == 2
    assert driver.smembers("s") == [b"b"]


def test_sismember(driver):
    driver.sadd("s", [b"a", b"b"])
    assert driver.sismember("s", b"a") is True
    assert driver.sismember("s", b"c") is False


def test_scard(driver):
    driver.sadd("s", [b"a", b"b", b"c"])
    assert driver.scard("s") == 3


def test_sinter(driver):
    driver.sadd("s1", [b"a", b"b", b"c"])
    driver.sadd("s2", [b"b", b"c", b"d"])
    assert sorted(driver.sinter(["s1", "s2"])) == [b"b", b"c"]


def test_sunion(driver):
    driver.sadd("s1", [b"a", b"b"])
    driver.sadd("s2", [b"b", b"c"])
    assert sorted(driver.sunion(["s1", "s2"])) == [b"a", b"b", b"c"]


def test_sdiff(driver):
    driver.sadd("s1", [b"a", b"b", b"c"])
    driver.sadd("s2", [b"b"])
    assert sorted(driver.sdiff(["s1", "s2"])) == [b"a", b"c"]
