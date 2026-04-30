def test_lpush_rpush_lrange(driver):
    driver.rpush_sync("l", [b"a", b"b", b"c"])
    driver.lpush_sync("l", [b"z"])
    assert driver.lrange_sync("l", 0, -1) == [b"z", b"a", b"b", b"c"]


def test_lpop_rpop(driver):
    driver.rpush_sync("l", [b"a", b"b", b"c"])
    assert driver.lpop_sync("l") == b"a"
    assert driver.rpop_sync("l") == b"c"
    assert driver.lrange_sync("l", 0, -1) == [b"b"]


def test_lpop_empty_returns_none(driver):
    assert driver.lpop_sync("nope") is None


def test_llen(driver):
    driver.rpush_sync("l", [b"a", b"b", b"c"])
    assert driver.llen_sync("l") == 3


def test_lrem(driver):
    driver.rpush_sync("l", [b"a", b"x", b"a", b"x", b"a"])
    assert driver.lrem_sync("l", 2, b"a") == 2
    assert driver.lrange_sync("l", 0, -1) == [b"x", b"x", b"a"]


def test_lindex(driver):
    driver.rpush_sync("l", [b"a", b"b", b"c"])
    assert driver.lindex_sync("l", 0) == b"a"
    assert driver.lindex_sync("l", -1) == b"c"
    assert driver.lindex_sync("l", 99) is None


def test_lset(driver):
    driver.rpush_sync("l", [b"a", b"b", b"c"])
    driver.lset_sync("l", 1, b"B")
    assert driver.lrange_sync("l", 0, -1) == [b"a", b"B", b"c"]


def test_linsert(driver):
    driver.rpush_sync("l", [b"a", b"c"])
    assert driver.linsert_sync("l", before=True, pivot=b"c", value=b"b") == 3
    assert driver.lrange_sync("l", 0, -1) == [b"a", b"b", b"c"]


def test_ltrim(driver):
    driver.rpush_sync("l", [b"a", b"b", b"c", b"d", b"e"])
    driver.ltrim_sync("l", 1, 3)
    assert driver.lrange_sync("l", 0, -1) == [b"b", b"c", b"d"]


def test_blmove_returns_value_when_source_has_data(driver):
    driver.rpush_sync("src", [b"hello"])
    # Short timeout — value is already there so no blocking happens.
    assert driver.blmove_sync("src", "dst", "LEFT", "RIGHT", 0.1) == b"hello"
    assert driver.lrange_sync("dst", 0, -1) == [b"hello"]


def test_blmove_returns_none_on_empty(driver):
    # Source is empty, brief timeout so we don't hang.
    assert driver.blmove_sync("src", "dst", "LEFT", "RIGHT", 0.1) is None


def test_blmpop_returns_key_and_values(driver):
    driver.rpush_sync("a", [b"1", b"2", b"3"])
    result = driver.blmpop_sync(0.1, ["a", "b"], "LEFT", 2)
    assert result == ("a", [b"1", b"2"])


def test_blmpop_returns_none_on_empty(driver):
    assert driver.blmpop_sync(0.1, ["a", "b"], "LEFT", 1) is None
