"""Pickle and copy round trips for the exception classes in ``django_cachex.exceptions``."""

import copy
import inspect
import pickle
from typing import Any

import pytest

from django_cachex import exceptions

# Constructor calls per class; classes not listed take a single message.
_SAMPLE_CALLS: dict[type[Exception], list[tuple[tuple[Any, ...], dict[str, Any]]]] = {
    exceptions.NotSupportedError: [
        (("hset",), {}),
        (("hset", "LocMemCache"), {}),
        (("hexpire",), {"detail": "the server does not know this command (requires Redis 7.4+)"}),
        (("set with nx/xx/get", "StreamCache"), {"detail": "no atomic check-and-set"}),
    ],
    exceptions.KeyNotFoundError: [(("missing-key",), {})],
}


def _public_exception_classes() -> list[type[Exception]]:
    return [
        obj
        for name, obj in vars(exceptions).items()
        if not name.startswith("_")
        and inspect.isclass(obj)
        and issubclass(obj, exceptions.CachexError)
        and obj.__module__ == exceptions.__name__
    ]


def _instances(cls: type[Exception]) -> list[Exception]:
    return [cls(*args, **kwargs) for args, kwargs in _SAMPLE_CALLS.get(cls, [(("boom",), {})])]


def _pickle_round_trip(exc: Exception) -> Exception:
    return pickle.loads(pickle.dumps(exc))


_ROUND_TRIPS = [_pickle_round_trip, copy.copy, copy.deepcopy]


def test_module_scan_finds_the_exception_classes():
    # The parametrization below iterates the module so new classes are covered automatically.
    found = _public_exception_classes()
    assert exceptions.CachexError in found
    assert exceptions.NotSupportedError in found
    assert exceptions.KeyNotFoundError in found
    assert exceptions.WrongTypeError in found


@pytest.mark.parametrize("cls", _public_exception_classes(), ids=lambda cls: cls.__name__)
@pytest.mark.parametrize("round_trip", _ROUND_TRIPS, ids=lambda fn: fn.__name__.strip("_"))
def test_round_trip_keeps_message_and_attributes(cls, round_trip):
    # Regression: ``BaseException.__reduce__`` re-runs ``__init__`` with ``args``,
    # so classes formatting their message in ``__init__`` nested it and lost fields.
    for original in _instances(cls):
        original.add_note("kept")
        restored = round_trip(original)
        assert type(restored) is cls
        assert str(restored) == str(original)
        assert restored.args == original.args
        assert vars(restored) == vars(original)


@pytest.mark.parametrize("round_trip", _ROUND_TRIPS, ids=lambda fn: fn.__name__.strip("_"))
def test_not_supported_error_fields_survive(round_trip):
    restored = round_trip(exceptions.NotSupportedError("hexpire", "RedisCache", detail="requires Redis 7.4+"))
    assert (restored.operation, restored.backend, restored.detail) == ("hexpire", "RedisCache", "requires Redis 7.4+")
    assert str(restored) == "Operation 'hexpire' is not supported by RedisCache: requires Redis 7.4+"


@pytest.mark.parametrize("round_trip", _ROUND_TRIPS, ids=lambda fn: fn.__name__.strip("_"))
def test_key_not_found_error_key_survives(round_trip):
    restored = round_trip(exceptions.KeyNotFoundError("missing-key"))
    assert restored.key == "missing-key"
    assert str(restored) == "Key 'missing-key' not found"
