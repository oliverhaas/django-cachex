"""Tests for increment and decrement operations."""

import pytest

from django_cachex.cache import RedisCache


class TestIncrDecrOperations:
    def test_incr(self, cache: RedisCache):
        cache.set("num", 1)

        cache.incr("num")
        res = cache.get("num")
        assert res == 2

        cache.incr("num", 10)
        res = cache.get("num")
        assert res == 12

        # max 64 bit signed int
        cache.set("num", 9223372036854775807)

        cache.incr("num")
        res = cache.get("num")
        assert res == 9223372036854775808

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 9223372036854775810

        cache.set("num", 3)

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 5

    def test_incr_no_timeout(self, cache: RedisCache):
        cache.set("num", 1, timeout=None)

        cache.incr("num")
        res = cache.get("num")
        assert res == 2

        cache.incr("num", 10)
        res = cache.get("num")
        assert res == 12

        # max 64 bit signed int
        cache.set("num", 9223372036854775807, timeout=None)

        cache.incr("num")
        res = cache.get("num")
        assert res == 9223372036854775808

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 9223372036854775810

        cache.set("num", 3, timeout=None)

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 5

    def test_incr_error(self, cache: RedisCache):
        with pytest.raises(ValueError):
            # key does not exist
            cache.incr("numnum")

    def test_decr(self, cache: RedisCache):
        cache.set("num", 20)

        cache.decr("num")
        res = cache.get("num")
        assert res == 19

        cache.decr("num", 20)
        res = cache.get("num")
        assert res == -1

        cache.decr("num", 2)
        res = cache.get("num")
        assert res == -3

        cache.set("num", 20)

        cache.decr("num")
        res = cache.get("num")
        assert res == 19

        # max 64 bit signed int + 1
        cache.set("num", 9223372036854775808)

        cache.decr("num")
        res = cache.get("num")
        assert res == 9223372036854775807

        cache.decr("num", 2)
        res = cache.get("num")
        assert res == 9223372036854775805
