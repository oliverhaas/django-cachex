"""Tests for the RESP emulation helpers in ``django_cachex.utils``."""

import math
from typing import TYPE_CHECKING

import pytest

from django_cachex.utils import _apply_zrange_limit, _as_incremented_score, _as_score, _glob_to_regex

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


@pytest.mark.parametrize("value", [math.nan, "nan", "NaN", " -nan "])
def test_as_score_rejects_nan(value):
    with pytest.raises(ValueError, match="value is not a valid float"):
        _as_score(value)


@pytest.mark.parametrize("value", ["abc", None, b"1x"])
def test_as_score_rejects_a_non_numeric_value(value):
    with pytest.raises(ValueError, match="value is not a valid float"):
        _as_score(value)


def test_as_score_accepts_infinities():
    assert _as_score("inf") == math.inf
    assert _as_score("-inf") == -math.inf
    assert _as_score(b"2.5") == 2.5


def test_as_incremented_score_adds():
    assert _as_incremented_score(1.5, 2.0) == 3.5
    assert _as_incremented_score(math.inf, 1.0) == math.inf


def test_as_incremented_score_rejects_a_nan_sum():
    with pytest.raises(ValueError, match=r"resulting score is not a number \(NaN\)"):
        _as_incremented_score(math.inf, -math.inf)


@pytest.mark.parametrize(("start", "num"), [(-1, 5), (-1, -1), (-3, 1)])
def test_apply_zrange_limit_negative_offset_returns_nothing(start, num):
    assert _apply_zrange_limit(["a", "b", "c"], start, num) == []


@pytest.mark.parametrize(("start", "num"), [(0, 2), (1, -1), (2, 5), (5, 1), (-1, 5), (-2, -1)])
def test_apply_zrange_limit_matches_the_server(cache: RespCache, start, num):
    cache.zadd("z", {"a": 1, "b": 2, "c": 3})
    served = cache.zrangebyscore("z", "-inf", "+inf", start=start, num=num)
    assert served == _apply_zrange_limit(["a", "b", "c"], start, num)


def test_score_errors_match_the_server(cache: RespCache):
    with pytest.raises(Exception, match="value is not a valid float"):
        cache.zadd("z", {"a": math.nan})
    cache.zadd("z", {"a": math.inf})
    with pytest.raises(Exception, match=r"resulting score is not a number \(NaN\)"):
        cache.zincrby("z", -math.inf, "a")


@pytest.mark.parametrize(
    ("pattern", "key", "expected"),
    [
        ("k[a-]1", "ka1", False),
        ("k[a-]1", "k1", True),
        ("k[a-]1", "k_", True),
        (r"k[a-\]]", "k_]", True),
        (r"k[a-\]]", "k]", False),
        ("k[ab-]z", "kz", True),
        ("k[ab-]z", "kaz", False),
    ],
)
def test_glob_class_range_reads_its_end_like_the_server(pattern, key, expected):
    assert bool(_glob_to_regex(pattern).match(key)) is expected
