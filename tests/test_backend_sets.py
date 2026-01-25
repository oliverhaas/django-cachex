"""Tests for set operations."""

import pytest

from django_cachex.cache import RedisCache


class TestSetOperations:
    def test_sadd(self, cache: RedisCache):
        assert cache.sadd("foo", "bar") == 1
        assert cache.smembers("foo") == {"bar"}

    def test_scard(self, cache: RedisCache):
        cache.sadd("foo", "bar", "bar2")
        assert cache.scard("foo") == 2

    def test_sdiff(self, cache: RedisCache):
        # Use hash tags {foo} to ensure keys are on same cluster slot
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sdiff("{foo}1", "{foo}2") == {"bar1"}

    def test_sdiffstore(self, cache: RedisCache):
        # Use hash tags {foo} to ensure keys are on same cluster slot
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sdiffstore("{foo}3", "{foo}1", "{foo}2") == 1
        assert cache.smembers("{foo}3") == {"bar1"}

    def test_sdiffstore_with_keys_version(self, cache: RedisCache):
        # Use hash tags {foo} to ensure keys are on same cluster slot
        cache.sadd("{foo}1", "bar1", "bar2", version=2)
        cache.sadd("{foo}2", "bar2", "bar3", version=2)
        assert cache.sdiffstore("{foo}3", "{foo}1", "{foo}2", version_keys=2) == 1
        assert cache.smembers("{foo}3") == {"bar1"}

    def test_sdiffstore_with_different_keys_versions_without_initial_set_in_version(
        self,
        cache: RedisCache,
    ):
        # Use hash tags {foo} to ensure keys are on same cluster slot
        cache.sadd("{foo}1", "bar1", "bar2", version=1)
        cache.sadd("{foo}2", "bar2", "bar3", version=2)
        assert cache.sdiffstore("{foo}3", "{foo}1", "{foo}2", version_keys=2) == 0

    def test_sdiffstore_with_different_keys_versions_with_initial_set_in_version(
        self,
        cache: RedisCache,
    ):
        # Use hash tags {foo} to ensure keys are on same cluster slot
        cache.sadd("{foo}1", "bar1", "bar2", version=2)
        cache.sadd("{foo}2", "bar2", "bar3", version=1)
        assert cache.sdiffstore("{foo}3", "{foo}1", "{foo}2", version_keys=2) == 2

    def test_sinter(self, cache: RedisCache):
        # Use hash tags {foo} to ensure keys are on same cluster slot
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sinter("{foo}1", "{foo}2") == {"bar2"}

    def test_interstore(self, cache: RedisCache):
        # Use hash tags {foo} to ensure keys are on same cluster slot
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sinterstore("{foo}3", "{foo}1", "{foo}2") == 1
        assert cache.smembers("{foo}3") == {"bar2"}

    def test_sismember(self, cache: RedisCache):
        cache.sadd("foo", "bar")
        assert cache.sismember("foo", "bar") is True
        assert cache.sismember("foo", "bar2") is False

    def test_smove(self, cache: RedisCache):
        # Use hash tags {foo} to ensure keys are on same cluster slot
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.smove("{foo}1", "{foo}2", "bar1") is True
        assert cache.smove("{foo}1", "{foo}2", "bar4") is False
        assert cache.smembers("{foo}1") == {"bar2"}
        assert cache.smembers("{foo}2") == {"bar1", "bar2", "bar3"}

    def test_spop_default_count(self, cache: RedisCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.spop("foo") in {"bar1", "bar2"}
        assert cache.smembers("foo") in [{"bar1"}, {"bar2"}]

    def test_spop(self, cache: RedisCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.spop("foo", 1) in [{"bar1"}, {"bar2"}]
        assert cache.smembers("foo") in [{"bar1"}, {"bar2"}]

    def test_srandmember_default_count(self, cache: RedisCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.srandmember("foo") in {"bar1", "bar2"}

    def test_srandmember(self, cache: RedisCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.srandmember("foo", 1) in [["bar1"], ["bar2"]]

    def test_srem(self, cache: RedisCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.srem("foo", "bar1") == 1
        assert cache.srem("foo", "bar3") == 0

    def test_sscan(self, cache: RedisCache):
        cache.sadd("foo", "bar1", "bar2")
        items = cache.sscan("foo")
        assert items == {"bar1", "bar2"}

    def test_sscan_with_match(self, cache: RedisCache):
        if cache._has_compression_enabled():
            pytest.skip("Compression is enabled, sscan with match is not supported")
        cache.sadd("foo", "bar1", "bar2", "zoo")
        items = cache.sscan("foo", match="zoo")
        assert items == {"zoo"}

    def test_sscan_iter(self, cache: RedisCache):
        cache.sadd("foo", "bar1", "bar2")
        items = cache.sscan_iter("foo")
        assert set(items) == {"bar1", "bar2"}

    def test_sscan_iter_with_match(self, cache: RedisCache):
        if cache._has_compression_enabled():
            pytest.skip(
                "Compression is enabled, sscan_iter with match is not supported",
            )
        cache.sadd("foo", "bar1", "bar2", "zoo")
        items = cache.sscan_iter("foo", match="bar*")
        assert set(items) == {"bar1", "bar2"}

    def test_smismember(self, cache: RedisCache):
        cache.sadd("foo", "bar1", "bar2", "bar3")
        assert cache.smismember("foo", "bar1", "bar2", "xyz") == [True, True, False]

    def test_sunion(self, cache: RedisCache):
        # Use hash tags {foo} to ensure keys are on same cluster slot
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sunion("{foo}1", "{foo}2") == {"bar1", "bar2", "bar3"}

    def test_sunionstore(self, cache: RedisCache):
        # Use hash tags {foo} to ensure keys are on same cluster slot
        cache.sadd("{foo}1", "bar1", "bar2")
        cache.sadd("{foo}2", "bar2", "bar3")
        assert cache.sunionstore("{foo}3", "{foo}1", "{foo}2") == 3
        assert cache.smembers("{foo}3") == {"bar1", "bar2", "bar3"}
