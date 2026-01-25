"""Tests for timeout and TTL operations."""

import datetime
import time
from datetime import timedelta

import pytest
from django.core.cache.backends.base import DEFAULT_TIMEOUT

from django_cachex.cache import RedisCache


class TestTimeoutOperations:
    def test_setnx_timeout(self, cache: RedisCache):
        # test that timeout still works for nx=True
        res = cache.set("test_key_nx", 1, timeout=2, nx=True)
        assert res is True
        time.sleep(3)
        res = cache.get("test_key_nx")
        assert res is None

        # test that timeout will not affect key, if it was there
        cache.set("test_key_nx", 1)
        res = cache.set("test_key_nx", 2, timeout=2, nx=True)
        assert res is False
        time.sleep(3)
        res = cache.get("test_key_nx")
        assert res == 1

        cache.delete("test_key_nx")
        res = cache.get("test_key_nx")
        assert res is None

    def test_timeout(self, cache: RedisCache):
        cache.set("test_key", 222, timeout=3)
        time.sleep(4)

        res = cache.get("test_key")
        assert res is None

    def test_timeout_0(self, cache: RedisCache):
        cache.set("test_key", 222, timeout=0)
        res = cache.get("test_key")
        assert res is None

    def test_timeout_parameter_as_positional_argument(self, cache: RedisCache):
        cache.set("test_key", 222, -1)
        res = cache.get("test_key")
        assert res is None

        cache.set("test_key", 222, 1)
        res1 = cache.get("test_key")
        time.sleep(2)
        res2 = cache.get("test_key")
        assert res1 == 222
        assert res2 is None

        # nx=True should not overwrite expire of key already in db
        cache.set("test_key", 222, None)
        cache.set("test_key", 222, -1, nx=True)
        res = cache.get("test_key")
        assert res == 222

    def test_timeout_negative(self, cache: RedisCache):
        cache.set("test_key", 222, timeout=-1)
        res = cache.get("test_key")
        assert res is None

        cache.set("test_key", 222, timeout=None)
        cache.set("test_key", 222, timeout=-1)
        res = cache.get("test_key")
        assert res is None

        # nx=True should not overwrite expire of key already in db
        cache.set("test_key", 222, timeout=None)
        cache.set("test_key", 222, timeout=-1, nx=True)
        res = cache.get("test_key")
        assert res == 222

    def test_timeout_tiny(self, cache: RedisCache):
        cache.set("test_key", 222, timeout=0.00001)
        res = cache.get("test_key")
        assert res in (None, 222)


class TestTTLOperations:
    def test_ttl(self, cache: RedisCache):
        cache.set("foo", "bar", 10)
        ttl = cache.ttl("foo")
        assert pytest.approx(ttl) == 10

        # Test ttl None
        cache.set("foo", "foo", timeout=None)
        ttl = cache.ttl("foo")
        assert ttl is None

        # Test ttl with expired key
        cache.set("foo", "foo", timeout=-1)
        ttl = cache.ttl("foo")
        assert ttl == 0

        # Test ttl with not existent key
        ttl = cache.ttl("not-existent-key")
        assert ttl == 0

    def test_pttl(self, cache: RedisCache):
        # Test pttl
        cache.set("foo", "bar", 10)
        ttl = cache.pttl("foo")

        # delta is set to 10 as precision error causes tests to fail
        assert pytest.approx(ttl, 10) == 10000

        # Test pttl with float value
        cache.set("foo", "bar", 5.5)
        ttl = cache.pttl("foo")
        assert pytest.approx(ttl, 10) == 5500

        # Test pttl None
        cache.set("foo", "foo", timeout=None)
        ttl = cache.pttl("foo")
        assert ttl is None

        # Test pttl with expired key
        cache.set("foo", "foo", timeout=-1)
        ttl = cache.pttl("foo")
        assert ttl == 0

        # Test pttl with not existent key
        ttl = cache.pttl("not-existent-key")
        assert ttl == 0

    def test_persist(self, cache: RedisCache):
        cache.set("foo", "bar", timeout=20)
        assert cache.persist("foo") is True

        ttl = cache.ttl("foo")
        assert ttl is None
        assert cache.persist("not-existent-key") is False

    def test_expire(self, cache: RedisCache):
        cache.set("foo", "bar", timeout=None)
        assert cache.expire("foo", 20) is True
        ttl = cache.ttl("foo")
        assert pytest.approx(ttl) == 20
        assert cache.expire("not-existent-key", 20) is False

    def test_expire_with_default_timeout(self, cache: RedisCache):
        cache.set("foo", "bar", timeout=None)
        assert cache.expire("foo", DEFAULT_TIMEOUT) is True
        assert cache.expire("not-existent-key", DEFAULT_TIMEOUT) is False

    def test_pexpire(self, cache: RedisCache):
        cache.set("foo", "bar", timeout=None)
        assert cache.pexpire("foo", 20500) is True
        ttl = cache.pttl("foo")
        # delta is set to 10 as precision error causes tests to fail
        assert pytest.approx(ttl, 10) == 20500
        assert cache.pexpire("not-existent-key", 20500) is False

    def test_pexpire_with_default_timeout(self, cache: RedisCache):
        cache.set("foo", "bar", timeout=None)
        assert cache.pexpire("foo", DEFAULT_TIMEOUT) is True
        assert cache.pexpire("not-existent-key", DEFAULT_TIMEOUT) is False

    def test_pexpire_at(self, cache: RedisCache):
        # Test settings expiration time 1 hour ahead by datetime.
        cache.set("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=1)
        assert cache.pexpire_at("foo", expiration_time) is True
        ttl = cache.pttl("foo")
        assert pytest.approx(ttl, 10) == timedelta(hours=1).total_seconds()

        # Test settings expiration time 1 hour ahead by Unix timestamp.
        cache.set("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert cache.pexpire_at("foo", int(expiration_time.timestamp() * 1000)) is True
        ttl = cache.pttl("foo")
        assert pytest.approx(ttl, 10) == timedelta(hours=2).total_seconds() * 1000

        # Test settings expiration time 1 hour in past, which effectively
        # deletes the key.
        expiration_time = datetime.datetime.now() - timedelta(hours=2)
        assert cache.pexpire_at("foo", expiration_time) is True
        value = cache.get("foo")
        assert value is None

        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert cache.pexpire_at("not-existent-key", expiration_time) is False

    def test_expire_at(self, cache: RedisCache):
        # Test settings expiration time 1 hour ahead by datetime.
        cache.set("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=1)
        assert cache.expire_at("foo", expiration_time) is True
        ttl = cache.ttl("foo")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds()

        # Test settings expiration time 1 hour ahead by Unix timestamp.
        cache.set("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert cache.expire_at("foo", int(expiration_time.timestamp())) is True
        ttl = cache.ttl("foo")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds() * 2

        # Test settings expiration time 1 hour in past, which effectively
        # deletes the key.
        expiration_time = datetime.datetime.now() - timedelta(hours=2)
        assert cache.expire_at("foo", expiration_time) is True
        value = cache.get("foo")
        assert value is None

        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert cache.expire_at("not-existent-key", expiration_time) is False


class TestTouchOperations:
    def test_touch_zero_timeout(self, cache: RedisCache):
        cache.set("test_key", 222, timeout=10)

        assert cache.touch("test_key", 0) is True
        res = cache.get("test_key")
        assert res is None

    def test_touch_positive_timeout(self, cache: RedisCache):
        cache.set("test_key", 222, timeout=10)

        assert cache.touch("test_key", 2) is True
        assert cache.get("test_key") == 222
        time.sleep(3)
        assert cache.get("test_key") is None

    def test_touch_negative_timeout(self, cache: RedisCache):
        cache.set("test_key", 222, timeout=10)

        assert cache.touch("test_key", -1) is True
        res = cache.get("test_key")
        assert res is None

    def test_touch_missed_key(self, cache: RedisCache):
        assert cache.touch("test_key_does_not_exist", 1) is False

    def test_touch_forever(self, cache: RedisCache):
        cache.set("test_key", "foo", timeout=1)
        result = cache.touch("test_key", None)
        assert result is True
        assert cache.ttl("test_key") is None
        time.sleep(2)
        assert cache.get("test_key") == "foo"

    def test_touch_forever_nonexistent(self, cache: RedisCache):
        result = cache.touch("test_key_does_not_exist", None)
        assert result is False

    def test_touch_default_timeout(self, cache: RedisCache):
        cache.set("test_key", "foo", timeout=1)
        result = cache.touch("test_key")
        assert result is True
        time.sleep(2)
        assert cache.get("test_key") == "foo"
