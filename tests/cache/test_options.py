import copy
from typing import TYPE_CHECKING, cast

import pytest
from django.core.cache import caches

from tests.cache.support import make_cache

if TYPE_CHECKING:
    from collections.abc import Iterable

    from django_cachex.cache import RespCache


def make_key(key: str, prefix: str, version: str) -> str:
    return f"{prefix}#{version}#{key}"


def reverse_key(key: str) -> str:
    return key.split("#", 2)[2]


@pytest.fixture
def key_prefix_cache(cache: RespCache, settings) -> RespCache:
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"]["KEY_PREFIX"] = "*"
    settings.CACHES = caches_setting
    return cache


@pytest.fixture
def with_prefix_cache() -> Iterable[RespCache]:
    with_prefix = cast("RespCache", caches["with_prefix"])
    yield with_prefix
    with_prefix.clear()


class TestDjangoRespCacheEscapePrefix:
    def test_delete_pattern(
        self,
        key_prefix_cache: RespCache,
        with_prefix_cache: RespCache,
    ):
        key_prefix_cache.set("a", "1")
        with_prefix_cache.set("b", "2")
        key_prefix_cache.delete_pattern("*")
        assert key_prefix_cache.has_key("a") is False
        assert with_prefix_cache.get("b") == "2"

    def test_iter_keys(
        self,
        key_prefix_cache: RespCache,
        with_prefix_cache: RespCache,
    ):
        key_prefix_cache.set("a", "1")
        with_prefix_cache.set("b", "2")
        assert list(key_prefix_cache.iter_keys("*")) == ["a"]

    def test_keys(self, key_prefix_cache: RespCache, with_prefix_cache: RespCache):
        key_prefix_cache.set("a", "1")
        with_prefix_cache.set("b", "2")
        keys = key_prefix_cache.keys("*")
        assert "a" in keys
        assert "b" not in keys


def test_custom_key_function(cache: RespCache, settings):
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"]["KEY_FUNCTION"] = "tests.cache.test_options.make_key"
    caches_setting["default"]["REVERSE_KEY_FUNCTION"] = "tests.cache.test_options.reverse_key"
    settings.CACHES = caches_setting

    for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
        cache.set(key, "foo")

    res = cache.delete_pattern("*foo-a*")
    assert bool(res) is True

    keys = cache.keys("foo*")
    assert set(keys) == {"foo-bb", "foo-bc"}
    # The adapter's own ``keys()`` avoids the raw client, which needs target_nodes on cluster.
    raw_keys = cache.adapter.keys("*")
    decoded = {k.decode() if isinstance(k, bytes) else k for k in raw_keys}
    assert decoded == {"#1#foo-bc", "#1#foo-bb"}


class TestDefaultReverseKey:
    def test_basic_key_reversal(self):
        cache = make_cache(key_prefix="myprefix")
        assert cache.reverse_key(cache.make_key("mykey")) == "mykey"

    def test_key_with_colons(self):
        cache = make_cache(key_prefix="prefix")
        assert cache.reverse_key("prefix:1:key:with:colons") == "key:with:colons"

    def test_empty_prefix(self):
        cache = make_cache()
        assert cache.reverse_key(":1:mykey") == "mykey"
        assert cache.reverse_key(cache.make_key("mykey")) == "mykey"

    def test_colon_in_key_prefix(self):
        cache = make_cache(key_prefix="app:v2")
        assert cache.make_key("foo") == "app:v2:1:foo"
        assert cache.reverse_key(cache.make_key("foo")) == "foo"
        assert cache.reverse_key(cache.make_key("key:with:colons")) == "key:with:colons"

    def test_unmatched_prefix_returned_unchanged(self):
        # A key made by some other cache must not lose its leading segments.
        cache = make_cache(key_prefix="myprefix")
        assert cache.reverse_key("otherprefix:1:mykey") == "otherprefix:1:mykey"

    def test_key_without_layout_returned_unchanged(self):
        cache = make_cache(key_prefix="myprefix")
        assert cache.reverse_key("plainkey") == "plainkey"
        # Prefix matches but there is no version:key remainder.
        assert cache.reverse_key("myprefix:1") == "myprefix:1"
