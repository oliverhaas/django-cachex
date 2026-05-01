def test_zadd_zrange(driver):
    assert driver.zadd("z", [(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)]) == 3
    assert driver.zrange("z", 0, -1) == [b"a", b"b", b"c"]


def test_zrange_with_scores(driver):
    driver.zadd("z", [(b"a", 1.0), (b"b", 2.0)])
    assert driver.zrange("z", 0, -1, with_scores=True) == [
        [b"a", 1.0],
        [b"b", 2.0],
    ]


def test_zrem(driver):
    driver.zadd("z", [(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)])
    assert driver.zrem("z", [b"a", b"missing"]) == 1
    assert driver.zrange("z", 0, -1) == [b"b", b"c"]


def test_zrangebyscore(driver):
    driver.zadd("z", [(b"a", 1.0), (b"b", 5.0), (b"c", 10.0)])
    assert driver.zrangebyscore("z", "2", "8") == [b"b"]
    assert driver.zrangebyscore("z", "-inf", "+inf") == [b"a", b"b", b"c"]


def test_zrevrange(driver):
    driver.zadd("z", [(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)])
    assert driver.zrevrange("z", 0, -1) == [b"c", b"b", b"a"]


def test_zincrby(driver):
    driver.zadd("z", [(b"a", 1.0)])
    assert driver.zincrby("z", b"a", 2.5) == 3.5
    assert driver.zscore("z", b"a") == 3.5


def test_zcard(driver):
    driver.zadd("z", [(b"a", 1.0), (b"b", 2.0)])
    assert driver.zcard("z") == 2


def test_zscore(driver):
    driver.zadd("z", [(b"a", 1.5)])
    assert driver.zscore("z", b"a") == 1.5
    assert driver.zscore("z", b"missing") is None


def test_zrank(driver):
    driver.zadd("z", [(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)])
    assert driver.zrank("z", b"a") == 0
    assert driver.zrank("z", b"c") == 2
    assert driver.zrank("z", b"missing") is None


def test_zcount(driver):
    driver.zadd("z", [(b"a", 1.0), (b"b", 5.0), (b"c", 10.0)])
    assert driver.zcount("z", "2", "8") == 1
    assert driver.zcount("z", "-inf", "+inf") == 3


def test_zpopmin(driver):
    driver.zadd("z", [(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)])
    assert driver.zpopmin("z") == [(b"a", 1.0)]
    assert driver.zpopmin("z", count=2) == [(b"b", 2.0), (b"c", 3.0)]


def test_zpopmax(driver):
    driver.zadd("z", [(b"a", 1.0), (b"b", 2.0), (b"c", 3.0)])
    assert driver.zpopmax("z") == [(b"c", 3.0)]
