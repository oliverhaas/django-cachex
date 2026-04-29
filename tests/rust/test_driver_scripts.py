SCRIPT = "return redis.call('SET', KEYS[1], ARGV[1])"
RETURN_INT = "return tonumber(ARGV[1])"
RETURN_LIST = "return {1, 2, 'three'}"


def test_eval_set(driver):
    driver.eval_sync(SCRIPT, ["k"], [b"v"])
    assert driver.get_sync("k") == b"v"


def test_eval_returns_int(driver):
    assert driver.eval_sync(RETURN_INT, [], [b"42"]) == 42


def test_eval_returns_list(driver):
    result = driver.eval_sync(RETURN_LIST, [], [])
    assert result[0] == 1
    assert result[1] == 2
    assert result[2] == b"three"


def test_script_load_and_evalsha(driver):
    sha = driver.script_load_sync(SCRIPT)
    driver.evalsha_sync(sha, ["k"], [b"v"])
    assert driver.get_sync("k") == b"v"


def test_script_exists(driver):
    sha = driver.script_load_sync(SCRIPT)
    assert driver.script_exists_sync(sha) is True
    assert driver.script_exists_sync("0" * 40) is False
