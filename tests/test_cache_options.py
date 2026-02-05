import copy
from collections.abc import Iterable
from typing import cast

import pytest
from django.core.cache import caches
from pytest import LogCaptureFixture
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisClusterException

from django_cachex.cache import KeyValueCache


def make_key(key: str, prefix: str, version: str) -> str:
    return f"{prefix}#{version}#{key}"


def reverse_key(key: str) -> str:
    return key.split("#", 2)[2]


@pytest.fixture
def ignore_exceptions_cache(settings) -> KeyValueCache:
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["doesnotexist"].setdefault("OPTIONS", {})["ignore_exceptions"] = True
    caches_setting["doesnotexist"]["OPTIONS"]["log_ignored_exceptions"] = True
    settings.CACHES = caches_setting
    return cast("KeyValueCache", caches["doesnotexist"])


def test_get_django_omit_exceptions_many_returns_default_arg(
    ignore_exceptions_cache: KeyValueCache,
):
    assert ignore_exceptions_cache._ignore_exceptions is True
    assert ignore_exceptions_cache.get_many(["key1", "key2", "key3"]) == {}


def test_get_django_omit_exceptions(
    caplog: LogCaptureFixture,
    ignore_exceptions_cache: KeyValueCache,
):
    assert ignore_exceptions_cache._ignore_exceptions is True
    assert ignore_exceptions_cache._log_ignored_exceptions is True

    assert ignore_exceptions_cache.get("key") is None
    assert ignore_exceptions_cache.get("key", "default") == "default"
    assert ignore_exceptions_cache.get("key", default="default") == "default"

    assert len(caplog.records) == 3
    assert all(record.levelname == "ERROR" and record.msg == "Exception ignored" for record in caplog.records)


def test_ignore_exceptions_enabled(settings):
    """Test that ignore_exceptions=True returns None instead of raising."""
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["doesnotexist"].setdefault("OPTIONS", {})["ignore_exceptions"] = True
    settings.CACHES = caches_setting
    cache = cast("KeyValueCache", caches["doesnotexist"])
    assert cache._ignore_exceptions is True
    assert cache.get("key") is None


def test_ignore_exceptions_disabled(settings):
    """Test that ignore_exceptions=False raises ConnectionError."""
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["doesnotexist"].setdefault("OPTIONS", {})["ignore_exceptions"] = False
    settings.CACHES = caches_setting
    cache = cast("KeyValueCache", caches["doesnotexist"])
    assert cache._ignore_exceptions is False
    with pytest.raises((RedisConnectionError, RedisClusterException)):
        cache.get("key")


@pytest.fixture
def key_prefix_cache(cache: KeyValueCache, settings) -> KeyValueCache:
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"]["KEY_PREFIX"] = "*"
    settings.CACHES = caches_setting
    return cache


@pytest.fixture
def with_prefix_cache() -> Iterable[KeyValueCache]:
    with_prefix = cast("KeyValueCache", caches["with_prefix"])
    yield with_prefix
    with_prefix.clear()


class TestDjangoKeyValueCacheEscapePrefix:
    def test_delete_pattern(
        self,
        key_prefix_cache: KeyValueCache,
        with_prefix_cache: KeyValueCache,
    ):
        key_prefix_cache.set("a", "1")
        with_prefix_cache.set("b", "2")
        key_prefix_cache.delete_pattern("*")
        assert key_prefix_cache.has_key("a") is False
        assert with_prefix_cache.get("b") == "2"

    def test_iter_keys(
        self,
        key_prefix_cache: KeyValueCache,
        with_prefix_cache: KeyValueCache,
    ):
        key_prefix_cache.set("a", "1")
        with_prefix_cache.set("b", "2")
        assert list(key_prefix_cache.iter_keys("*")) == ["a"]

    def test_keys(self, key_prefix_cache: KeyValueCache, with_prefix_cache: KeyValueCache):
        key_prefix_cache.set("a", "1")
        with_prefix_cache.set("b", "2")
        keys = key_prefix_cache.keys("*")
        assert "a" in keys
        assert "b" not in keys


def test_custom_key_function(cache: KeyValueCache, settings):
    from redis.cluster import RedisCluster

    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"]["KEY_FUNCTION"] = "test_cache_options.make_key"
    caches_setting["default"]["OPTIONS"]["reverse_key_function"] = "test_cache_options.reverse_key"
    settings.CACHES = caches_setting

    for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
        cache.set(key, "foo")

    res = cache.delete_pattern("*foo-a*")
    assert bool(res) is True

    keys = cache.keys("foo*")
    assert set(keys) == {"foo-bb", "foo-bc"}
    # ensure our custom function was actually called
    client = cache.get_client(write=False)
    if isinstance(client, RedisCluster):
        # In cluster mode, query all primary nodes
        raw_keys = client.keys("*", target_nodes=RedisCluster.PRIMARIES)
    else:
        raw_keys = client.keys("*")
    assert {k.decode() for k in raw_keys} == {"#1#foo-bc", "#1#foo-bb"}  # type: ignore[union-attr]
