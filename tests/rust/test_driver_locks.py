def test_lock_acquire_release(driver):
    assert driver.lock_acquire("lock", "tok-1", timeout_ms=5000) is True
    assert driver.lock_acquire("lock", "tok-2", timeout_ms=5000) is False
    assert driver.lock_release("lock", "tok-1") == 1


def test_lock_release_with_wrong_token(driver):
    driver.lock_acquire("lock", "tok-1", timeout_ms=5000)
    assert driver.lock_release("lock", "wrong-tok") == 0
    # Real owner can still release
    assert driver.lock_release("lock", "tok-1") == 1


def test_lock_extend(driver):
    driver.lock_acquire("lock", "tok", timeout_ms=1000)
    assert driver.lock_extend("lock", "tok", 5000) == 1
    assert driver.pttl("lock") > 1000


def test_lock_extend_with_wrong_token(driver):
    driver.lock_acquire("lock", "tok", timeout_ms=5000)
    assert driver.lock_extend("lock", "wrong-tok", 1000) == 0
