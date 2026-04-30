def test_sadd_smembers(driver):
    assert driver.sadd_sync("s", [b"a", b"b", b"c"]) == 3
    assert sorted(driver.smembers_sync("s")) == [b"a", b"b", b"c"]


def test_sadd_dedup(driver):
    driver.sadd_sync("s", [b"a", b"b"])
    assert driver.sadd_sync("s", [b"a", b"c"]) == 1  # only "c" is new


def test_srem(driver):
    driver.sadd_sync("s", [b"a", b"b", b"c"])
    assert driver.srem_sync("s", [b"a", b"c", b"missing"]) == 2
    assert driver.smembers_sync("s") == [b"b"]


def test_sismember(driver):
    driver.sadd_sync("s", [b"a", b"b"])
    assert driver.sismember_sync("s", b"a") is True
    assert driver.sismember_sync("s", b"c") is False


def test_scard(driver):
    driver.sadd_sync("s", [b"a", b"b", b"c"])
    assert driver.scard_sync("s") == 3


def test_sinter(driver):
    driver.sadd_sync("s1", [b"a", b"b", b"c"])
    driver.sadd_sync("s2", [b"b", b"c", b"d"])
    assert sorted(driver.sinter_sync(["s1", "s2"])) == [b"b", b"c"]


def test_sunion(driver):
    driver.sadd_sync("s1", [b"a", b"b"])
    driver.sadd_sync("s2", [b"b", b"c"])
    assert sorted(driver.sunion_sync(["s1", "s2"])) == [b"a", b"b", b"c"]


def test_sdiff(driver):
    driver.sadd_sync("s1", [b"a", b"b", b"c"])
    driver.sadd_sync("s2", [b"b"])
    assert sorted(driver.sdiff_sync(["s1", "s2"])) == [b"a", b"c"]
