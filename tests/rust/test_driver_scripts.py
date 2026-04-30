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


def test_evalsha_unknown_raises_runtime_error(driver):
    """Server-side errors (NOSCRIPT) classify as PyRuntimeError, NOT PyConnectionError.

    This locks in the boundary between IGNORE_EXCEPTIONS-swallowable connection
    errors and propagated server errors per #66's locked decisions.
    """
    import pytest

    with pytest.raises(RuntimeError):
        driver.evalsha_sync("0" * 40, [], [])


def test_eval_returning_string_renders_as_bytes(driver):
    # Lua return type 'string' arrives as RESP bulk-string → Python bytes.
    result = driver.eval_sync("return 'hello'", [], [])
    assert result == b"hello"
