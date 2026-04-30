def test_dbsize(driver):
    assert driver.dbsize_sync() == 0
    driver.set_sync("a", b"1")
    driver.set_sync("b", b"2")
    assert driver.dbsize_sync() == 2


def test_info(driver):
    result = driver.info_sync()
    # INFO returns a bulk string with sections; redis_version is universal.
    assert b"redis_version" in result


def test_client_list(driver):
    result = driver.client_list_sync()
    assert b"id=" in result  # each client entry has an id= field


def test_config_get(driver):
    # RESP3 returns a Map (Python dict); we force RESP3 on every connection.
    result = driver.config_get_sync("maxmemory-policy")
    assert isinstance(result, dict)
    assert b"maxmemory-policy" in result


def test_object_encoding(driver):
    driver.set_sync("k", b"some longer value")
    encoding = driver.object_encoding_sync("k")
    # Encoding is one of the known string encodings.
    assert encoding in {"embstr", "raw", "int"}


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
