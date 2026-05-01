SCRIPT = "return redis.call('SET', KEYS[1], ARGV[1])"
RETURN_INT = "return tonumber(ARGV[1])"
RETURN_LIST = "return {1, 2, 'three'}"


def test_eval_set(driver):
    driver.eval(SCRIPT, ["k"], [b"v"])
    assert driver.get("k") == b"v"


def test_eval_returns_int(driver):
    assert driver.eval(RETURN_INT, [], [b"42"]) == 42


def test_eval_returns_list(driver):
    result = driver.eval(RETURN_LIST, [], [])
    assert result[0] == 1
    assert result[1] == 2
    assert result[2] == b"three"


def test_script_load_and_evalsha(driver):
    sha = driver.script_load(SCRIPT)
    driver.evalsha(sha, ["k"], [b"v"])
    assert driver.get("k") == b"v"


def test_script_exists(driver):
    sha = driver.script_load(SCRIPT)
    assert driver.script_exists(sha) is True
    assert driver.script_exists("0" * 40) is False


def test_evalsha_unknown_raises_runtime_error(driver):
    """Server-side errors (NOSCRIPT) classify as PyRuntimeError, NOT PyConnectionError.

    This locks in the boundary between IGNORE_EXCEPTIONS-swallowable connection
    errors and propagated server errors per #66's locked decisions.
    """
    import pytest

    with pytest.raises(RuntimeError):
        driver.evalsha("0" * 40, [], [])


def test_eval_returning_string_renders_as_bytes(driver):
    # Lua return type 'string' arrives as RESP bulk-string → Python bytes.
    result = driver.eval("return 'hello'", [], [])
    assert result == b"hello"
