"""Tests for increment and decrement operations."""

import pytest

from django_cachex.cache import KeyValueCache


class TestIncrementOperations:
    """Tests for the incr() method."""

    def test_increment_by_one(self, cache: KeyValueCache):
        cache.set("counter", 5)
        cache.incr("counter")
        assert cache.get("counter") == 6

    def test_increment_by_custom_amount(self, cache: KeyValueCache):
        cache.set("counter", 10)
        cache.incr("counter", 7)
        assert cache.get("counter") == 17

    def test_increment_large_number(self, cache: KeyValueCache):
        # Max signed 64-bit integer
        cache.set("big_num", 9223372036854775807)
        cache.incr("big_num")
        assert cache.get("big_num") == 9223372036854775808

    def test_increment_chain(self, cache: KeyValueCache):
        cache.set("chain", 0)
        cache.incr("chain")
        cache.incr("chain", 4)
        cache.incr("chain", 5)
        assert cache.get("chain") == 10


class TestIncrementWithoutTimeout:
    """Tests for incr() on keys without expiration."""

    def test_increment_persistent_key(self, cache: KeyValueCache):
        cache.set("persistent", 100, timeout=None)
        cache.incr("persistent")
        assert cache.get("persistent") == 101

    def test_increment_persistent_by_amount(self, cache: KeyValueCache):
        cache.set("persistent2", 50, timeout=None)
        cache.incr("persistent2", 25)
        assert cache.get("persistent2") == 75

    def test_increment_persistent_large(self, cache: KeyValueCache):
        cache.set("persistent_big", 9223372036854775807, timeout=None)
        cache.incr("persistent_big", 3)
        assert cache.get("persistent_big") == 9223372036854775810


class TestIncrementErrors:
    """Tests for error conditions in incr()."""

    def test_increment_missing_key_raises(self, cache: KeyValueCache):
        cache.delete("nonexistent_counter")
        with pytest.raises(ValueError):
            cache.incr("nonexistent_counter")


class TestDecrementOperations:
    """Tests for the decr() method."""

    def test_decrement_by_one(self, cache: KeyValueCache):
        cache.set("countdown", 10)
        cache.decr("countdown")
        assert cache.get("countdown") == 9

    def test_decrement_by_custom_amount(self, cache: KeyValueCache):
        cache.set("countdown2", 50)
        cache.decr("countdown2", 15)
        assert cache.get("countdown2") == 35

    def test_decrement_to_negative(self, cache: KeyValueCache):
        cache.set("neg_test", 5)
        cache.decr("neg_test", 10)
        assert cache.get("neg_test") == -5

    def test_decrement_chain(self, cache: KeyValueCache):
        cache.set("chain_dec", 100)
        cache.decr("chain_dec")
        cache.decr("chain_dec", 9)
        cache.decr("chain_dec", 40)
        assert cache.get("chain_dec") == 50

    def test_decrement_large_number(self, cache: KeyValueCache):
        # Slightly above max signed 64-bit
        cache.set("big_dec", 9223372036854775808)
        cache.decr("big_dec")
        assert cache.get("big_dec") == 9223372036854775807

    def test_decrement_multiple_times(self, cache: KeyValueCache):
        cache.set("multi_dec", 9223372036854775808)
        cache.decr("multi_dec")
        cache.decr("multi_dec", 3)
        assert cache.get("multi_dec") == 9223372036854775804
