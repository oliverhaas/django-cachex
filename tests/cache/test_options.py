import copy
from typing import TYPE_CHECKING, cast

import pytest
from django.core.cache import caches

if TYPE_CHECKING:
    from collections.abc import Iterable

    from django_cachex.cache import KeyValueCache


def make_key(key: str, prefix: str, version: str) -> str:
    return f"{prefix}#{version}#{key}"


def reverse_key(key: str) -> str:
    return key.split("#", 2)[2]


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
    caches_setting["default"]["KEY_FUNCTION"] = "tests.cache.test_options.make_key"
    caches_setting["default"]["OPTIONS"]["reverse_key_function"] = "tests.cache.test_options.reverse_key"
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
    # redis-py returns bytes; the Rust driver returns str. Normalize.
    decoded = {k.decode() if isinstance(k, bytes) else k for k in raw_keys}  # type: ignore[union-attr]
    assert decoded == {"#1#foo-bc", "#1#foo-bb"}
