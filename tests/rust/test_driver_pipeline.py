def test_pipeline_exec_executes_commands_in_order(driver):
    result = driver.pipeline_exec_sync(
        [
            ("SET", [b"a", b"1"]),
            ("SET", [b"b", b"2"]),
            ("GET", [b"a"]),
            ("GET", [b"b"]),
        ],
    )
    # SET returns OK (rendered as True via redis::Value::Okay), GET returns the bytes.
    assert result[0] is True
    assert result[1] is True
    assert result[2] == b"1"
    assert result[3] == b"2"


def test_pipeline_exec_with_increment(driver):
    result = driver.pipeline_exec_sync(
        [
            ("INCR", [b"counter"]),
            ("INCR", [b"counter"]),
            ("INCR", [b"counter"]),
        ],
    )
    assert result == [1, 2, 3]


def test_pipeline_exec_empty_raises(driver):
    """redis-rs rejects empty pipelines — surface that as a RuntimeError."""
    import pytest

    with pytest.raises(RuntimeError):
        driver.pipeline_exec_sync([])
