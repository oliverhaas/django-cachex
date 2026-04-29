def test_xadd_xlen(driver):
    id1 = driver.xadd_sync("s", "*", [("field", b"v1")])
    id2 = driver.xadd_sync("s", "*", [("field", b"v2")])
    assert driver.xlen_sync("s") == 2
    assert id1 < id2


def test_xrange(driver):
    driver.xadd_sync("s", "*", [("field", b"v1")])
    driver.xadd_sync("s", "*", [("field", b"v2")])
    entries = driver.xrange_sync("s", "-", "+")
    assert len(entries) == 2


def test_xrange_with_count(driver):
    for i in range(5):
        driver.xadd_sync("s", "*", [("i", str(i).encode())])
    entries = driver.xrange_sync("s", "-", "+", count=2)
    assert len(entries) == 2


def test_xread(driver):
    driver.xadd_sync("s", "*", [("field", b"v1")])
    result = driver.xread_sync(["s"], ["0"])
    assert result is not None
    assert len(result) == 1


def test_xtrim(driver):
    for i in range(10):
        driver.xadd_sync("s", "*", [("i", str(i).encode())])
    driver.xtrim_sync("s", max_len=3)
    assert driver.xlen_sync("s") == 3


def test_xgroup_create_and_xreadgroup(driver):
    driver.xadd_sync("s", "*", [("field", b"v1")])
    driver.xgroup_create_sync("s", "g1", "0", mkstream=False)
    result = driver.xreadgroup_sync("g1", "consumer1", ["s"], [">"])
    assert result is not None


def test_xack(driver):
    driver.xadd_sync("s", "*", [("field", b"v1")])
    driver.xgroup_create_sync("s", "g1", "0", mkstream=False)
    result = driver.xreadgroup_sync("g1", "consumer1", ["s"], [">"])
    # RESP3 returns XREADGROUP as a map: {stream: [[id, [field, value]]]}
    entries = result[b"s"]
    msg_id = entries[0][0].decode()
    assert driver.xack_sync("s", "g1", [msg_id]) == 1


def test_xpending(driver):
    driver.xadd_sync("s", "*", [("field", b"v1")])
    driver.xgroup_create_sync("s", "g1", "0", mkstream=False)
    driver.xreadgroup_sync("g1", "consumer1", ["s"], [">"])
    result = driver.xpending_sync("s", "g1")
    assert result[0] == 1  # 1 pending message


def test_xgroup_create_mkstream(driver):
    # Stream doesn't exist yet — mkstream=True creates it.
    driver.xgroup_create_sync("new_stream", "g", "0", mkstream=True)
    assert driver.xlen_sync("new_stream") == 0
