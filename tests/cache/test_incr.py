"""Tests for increment and decrement operations."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


def test_increment_by_one(cache: RespCache):
    cache.set("counter", 5)
    cache.incr("counter")
    assert cache.get("counter") == 6


def test_increment_by_custom_amount(cache: RespCache):
    cache.set("counter", 10)
    cache.incr("counter", 7)
    assert cache.get("counter") == 17


def test_increment_chain(cache: RespCache):
    cache.set("chain", 0)
    cache.incr("chain")
    cache.incr("chain", 4)
    cache.incr("chain", 5)
    assert cache.get("chain") == 10


def test_increment_keeps_a_persistent_key_persistent(cache: RespCache):
    cache.set("persistent", 100, timeout=None)
    cache.incr("persistent", 25)
    assert cache.get("persistent") == 125
    assert cache.ttl("persistent") is None


# Redis rounds TTL to the nearest second, so a slow round trip may read 299.
def test_increment_keeps_the_ttl(cache: RespCache):
    cache.set("ttl_counter", 5, timeout=300)
    cache.incr("ttl_counter")
    assert cache.ttl("ttl_counter") in (299, 300)


def test_decrement_keeps_the_ttl(cache: RespCache):
    cache.set("ttl_countdown", 5, timeout=300)
    cache.decr("ttl_countdown")
    assert cache.ttl("ttl_countdown") in (299, 300)


def test_increment_missing_key_creates_it(cache: RespCache):
    cache.delete("nonexistent_counter")
    result = cache.incr("nonexistent_counter")
    assert result == 1


def test_decrement_by_one(cache: RespCache):
    cache.set("countdown", 10)
    cache.decr("countdown")
    assert cache.get("countdown") == 9


def test_decrement_by_custom_amount(cache: RespCache):
    cache.set("countdown2", 50)
    cache.decr("countdown2", 15)
    assert cache.get("countdown2") == 35


def test_decrement_to_negative(cache: RespCache):
    cache.set("neg_test", 5)
    cache.decr("neg_test", 10)
    assert cache.get("neg_test") == -5


def test_decrement_chain(cache: RespCache):
    cache.set("chain_dec", 100)
    cache.decr("chain_dec")
    cache.decr("chain_dec", 9)
    cache.decr("chain_dec", 40)
    assert cache.get("chain_dec") == 50
