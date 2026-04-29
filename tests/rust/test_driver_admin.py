def test_dbsize(driver):
    assert driver.dbsize_sync() == 0
    driver.set_sync("a", b"1")
    driver.set_sync("b", b"2")
    assert driver.dbsize_sync() == 2


def test_info(driver):
    result = driver.info_sync()
    # Info returns a bulk string keyed by section; here we get the full text.
    assert result is not None


def test_client_list(driver):
    result = driver.client_list_sync()
    assert result is not None


def test_config_get(driver):
    result = driver.config_get_sync("maxmemory-policy")
    # Older versions return a 2-element array, newer ones a map. Just verify shape isn't empty.
    assert result is not None


def test_object_encoding(driver):
    driver.set_sync("k", b"v")
    encoding = driver.object_encoding_sync("k")
    # Encoding depends on Redis version (embstr/raw); just verify it's not None.
    assert encoding is not None


def test_object_idletime(driver):
    driver.set_sync("k", b"v")
    idle = driver.object_idletime_sync("k")
    assert idle is not None
    assert idle >= 0


def test_memory_usage(driver):
    driver.set_sync("k", b"some value here")
    usage = driver.memory_usage_sync("k")
    assert usage is not None
    assert usage > 0
