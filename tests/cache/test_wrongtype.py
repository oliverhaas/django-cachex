"""WRONGTYPE normalization across RESP backends.

Real Redis/Valkey raise ``WRONGTYPE`` (as a server-side response error)
when a command is applied to a key holding the wrong type. Each adapter
surfaces this differently (redis-py and valkey-py via ``ResponseError``,
the Rust adapter via ``RuntimeError``), so callers historically had no
single exception to catch. The adapter layer now normalizes those into
:class:`django_cachex.exceptions.WrongTypeError` (a :class:`TypeError`
subclass) so user code can ``except WrongTypeError`` portably.
"""

from typing import TYPE_CHECKING

import pytest

from django_cachex.exceptions import WrongTypeError

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


class TestWrongTypeNormalization:
    def test_get_after_lpush_raises_wrongtype(self, cache: RespCache):
        cache.lpush("k", "x")
        with pytest.raises(WrongTypeError):
            cache.get("k")

    def test_lpush_on_string_raises_wrongtype(self, cache: RespCache):
        cache.set("k", "abc")
        with pytest.raises(WrongTypeError):
            cache.lpush("k", "x")

    def test_hset_on_string_raises_wrongtype(self, cache: RespCache):
        cache.set("k", "abc")
        with pytest.raises(WrongTypeError):
            cache.hset("k", "f", "v")

    def test_sadd_on_string_raises_wrongtype(self, cache: RespCache):
        cache.set("k", "abc")
        with pytest.raises(WrongTypeError):
            cache.sadd("k", "m")

    def test_zadd_on_string_raises_wrongtype(self, cache: RespCache):
        cache.set("k", "abc")
        with pytest.raises(WrongTypeError):
            cache.zadd("k", {"m": 1.0})

    def test_set_overwrites_collection(self, cache: RespCache):
        cache.lpush("k", "x")
        cache.set("k", "abc")
        assert cache.get("k") == "abc"

    def test_wrongtype_is_typeerror_subclass(self, cache: RespCache):
        cache.set("k", "abc")
        with pytest.raises(TypeError):  # WrongTypeError ⊂ TypeError
            cache.lpush("k", "x")
