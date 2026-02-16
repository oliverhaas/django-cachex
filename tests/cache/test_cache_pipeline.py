"""Tests for pipeline operations."""

import pytest

from django_cachex.cache import KeyValueCache


class TestPipelineBasic:
    """Test basic pipeline functionality."""

    def test_pipeline_returns_pipeline_object(self, cache: KeyValueCache):
        """Test that pipeline() returns a Pipeline object."""
        from django_cachex.client.pipeline import Pipeline

        pipe = cache.pipeline()
        assert isinstance(pipe, Pipeline)

    def test_pipeline_manual_execute(self, cache: KeyValueCache):
        """Test pipeline with manual execute."""
        cache.set("manual_key", "manual_value")

        pipe = cache.pipeline()
        pipe.get("manual_key")
        pipe.set("new_key", "new_value")
        pipe.get("new_key")
        results = pipe.execute()

        assert results[0] == "manual_value"
        assert results[1] is True
        assert results[2] == "new_value"

    def test_pipeline_empty_execute(self, cache: KeyValueCache):
        """Test executing an empty pipeline."""
        pipe = cache.pipeline()
        results = pipe.execute()
        assert results == []

    def test_pipeline_chaining(self, cache: KeyValueCache):
        """Test that pipeline methods return self for chaining."""
        pipe = cache.pipeline()
        result = pipe.set("chain1", "a").set("chain2", "b").get("chain1").get("chain2")
        assert result is pipe

        results = pipe.execute()
        assert results == [True, True, "a", "b"]

    def test_pipeline_transaction(self, cache: KeyValueCache):
        """Test pipeline with transaction=True (default)."""
        pipe = cache.pipeline(transaction=True)
        pipe.set("tx_key", "tx_value")
        pipe.get("tx_key")
        results = pipe.execute()
        assert results == [True, "tx_value"]

    def test_pipeline_no_transaction(self, cache: KeyValueCache):
        """Test pipeline with transaction=False."""
        pipe = cache.pipeline(transaction=False)
        pipe.set("notx_key", "notx_value")
        pipe.get("notx_key")
        results = pipe.execute()
        assert results == [True, "notx_value"]


class TestPipelineCacheOperations:
    """Test standard cache operations in pipeline."""

    def test_pipeline_get_set(self, cache: KeyValueCache):
        """Test get/set operations in pipeline."""
        pipe = cache.pipeline()
        pipe.set("key1", "value1")
        pipe.set("key2", {"nested": "dict"})
        pipe.get("key1")
        pipe.get("key2")
        pipe.get("nonexistent")
        results = pipe.execute()

        assert results[0] is True  # set returns True
        assert results[1] is True
        assert results[2] == "value1"
        assert results[3] == {"nested": "dict"}
        assert results[4] is None

    def test_pipeline_delete(self, cache: KeyValueCache):
        """Test delete in pipeline."""
        cache.set("{pipe_del}key1", "a")
        cache.set("{pipe_del}key2", "b")

        pipe = cache.pipeline()
        pipe.delete("{pipe_del}key1")
        pipe.delete("{pipe_del}key2")
        pipe.delete("{pipe_del}nonexistent")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is True
        assert results[2] is False

    def test_pipeline_exists(self, cache: KeyValueCache):
        """Test exists in pipeline."""
        cache.set("{pipe_ex}key", "value")

        pipe = cache.pipeline()
        pipe.exists("{pipe_ex}key")
        pipe.exists("{pipe_ex}nonexistent")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is False

    def test_pipeline_expire_ttl(self, cache: KeyValueCache):
        """Test expire and ttl in pipeline."""
        cache.set("expire_key", "value")

        pipe = cache.pipeline()
        pipe.expire("expire_key", 100)
        pipe.ttl("expire_key")
        results = pipe.execute()

        assert results[0] is True
        assert 0 < results[1] <= 100

    def test_pipeline_incr_decr(self, cache: KeyValueCache):
        """Test incr/decr in pipeline."""
        cache.set("counter", 10)

        pipe = cache.pipeline()
        pipe.incr("counter")
        pipe.incr("counter", 5)
        pipe.decr("counter")
        pipe.decr("counter", 3)
        pipe.get("counter")
        results = pipe.execute()

        assert results[0] == 11
        assert results[1] == 16
        assert results[2] == 15
        assert results[3] == 12
        assert results[4] == 12


class TestPipelineListOperations:
    """Test list operations in pipeline."""

    def test_pipeline_lpush_rpush(self, cache: KeyValueCache):
        """Test lpush/rpush in pipeline."""
        pipe = cache.pipeline()
        pipe.rpush("pipe_list", "a", "b", "c")
        pipe.lpush("pipe_list", "x", "y")
        pipe.lrange("pipe_list", 0, -1)
        results = pipe.execute()

        assert results[0] == 3  # rpush returns new length
        assert results[1] == 5  # lpush returns new length
        assert results[2] == ["y", "x", "a", "b", "c"]

    def test_pipeline_lpop_rpop(self, cache: KeyValueCache):
        """Test lpop/rpop in pipeline."""
        cache.rpush("pipe_list2", "a", "b", "c", "d")

        pipe = cache.pipeline()
        pipe.lpop("pipe_list2")
        pipe.rpop("pipe_list2")
        pipe.lpop("pipe_list2", count=2)
        results = pipe.execute()

        assert results[0] == "a"
        assert results[1] == "d"
        assert results[2] == ["b", "c"]

    def test_pipeline_llen_lindex(self, cache: KeyValueCache):
        """Test llen/lindex in pipeline."""
        cache.rpush("pipe_list3", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.llen("pipe_list3")
        pipe.lindex("pipe_list3", 0)
        pipe.lindex("pipe_list3", -1)
        results = pipe.execute()

        assert results[0] == 3
        assert results[1] == "a"
        assert results[2] == "c"

    def test_pipeline_lset_lrem(self, cache: KeyValueCache):
        """Test lset/lrem in pipeline."""
        cache.rpush("pipe_list4", "a", "b", "a", "c")

        pipe = cache.pipeline()
        pipe.lset("pipe_list4", 1, "B")
        pipe.lrem("pipe_list4", 1, "a")
        pipe.lrange("pipe_list4", 0, -1)
        results = pipe.execute()

        assert results[0] is True
        assert results[1] == 1
        assert results[2] == ["B", "a", "c"]

    def test_pipeline_ltrim(self, cache: KeyValueCache):
        """Test ltrim in pipeline."""
        cache.rpush("pipe_list5", "a", "b", "c", "d", "e")

        pipe = cache.pipeline()
        pipe.ltrim("pipe_list5", 1, 3)
        pipe.lrange("pipe_list5", 0, -1)
        results = pipe.execute()

        assert results[0] is True
        assert results[1] == ["b", "c", "d"]

    def test_pipeline_linsert(self, cache: KeyValueCache):
        """Test linsert in pipeline."""
        cache.rpush("pipe_list6", "a", "c")

        pipe = cache.pipeline()
        pipe.linsert("pipe_list6", "BEFORE", "c", "b")
        pipe.linsert("pipe_list6", "AFTER", "c", "d")
        pipe.lrange("pipe_list6", 0, -1)
        results = pipe.execute()

        assert results[0] == 3
        assert results[1] == 4
        assert results[2] == ["a", "b", "c", "d"]

    def test_pipeline_lpos(self, cache: KeyValueCache):
        """Test lpos in pipeline."""
        cache.rpush("pipe_list7", "a", "b", "c", "b", "d")

        pipe = cache.pipeline()
        pipe.lpos("pipe_list7", "b")
        pipe.lpos("pipe_list7", "b", rank=2)
        pipe.lpos("pipe_list7", "z")
        results = pipe.execute()

        assert results[0] == 1
        assert results[1] == 3
        assert results[2] is None

    def test_pipeline_lmove(self, cache: KeyValueCache):
        """Test lmove in pipeline."""
        # Use hash tags to ensure keys are on same cluster slot
        cache.rpush("{pipe}src", "a", "b", "c")
        cache.rpush("{pipe}dst", "x")

        pipe = cache.pipeline()
        pipe.lmove("{pipe}src", "{pipe}dst", "LEFT", "RIGHT")
        pipe.lrange("{pipe}src", 0, -1)
        pipe.lrange("{pipe}dst", 0, -1)
        results = pipe.execute()

        assert results[0] == "a"
        assert results[1] == ["b", "c"]
        assert results[2] == ["x", "a"]


class TestPipelineSetOperations:
    """Test set operations in pipeline."""

    def test_pipeline_sadd_smembers(self, cache: KeyValueCache):
        """Test sadd/smembers in pipeline."""
        pipe = cache.pipeline()
        pipe.sadd("pipe_set", "a", "b", "c")
        pipe.smembers("pipe_set")
        results = pipe.execute()

        assert results[0] == 3
        assert results[1] == {"a", "b", "c"}

    def test_pipeline_scard_sismember(self, cache: KeyValueCache):
        """Test scard/sismember in pipeline."""
        cache.sadd("pipe_set2", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.scard("pipe_set2")
        pipe.sismember("pipe_set2", "b")
        pipe.sismember("pipe_set2", "z")
        results = pipe.execute()

        assert results[0] == 3
        assert results[1] is True
        assert results[2] is False

    def test_pipeline_srem(self, cache: KeyValueCache):
        """Test srem in pipeline."""
        cache.sadd("pipe_set3", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.srem("pipe_set3", "b", "c")
        pipe.smembers("pipe_set3")
        results = pipe.execute()

        assert results[0] == 2
        assert results[1] == {"a"}

    def test_pipeline_sdiff_sinter_sunion(self, cache: KeyValueCache, client_class: str):
        """Test set operations in pipeline."""
        # Redis Cluster doesn't support sdiff/sinter/sunion in pipeline mode
        if client_class == "cluster":
            pytest.skip("sdiff/sinter/sunion blocked in cluster pipeline mode")

        # Use hash tags for cluster compatibility
        cache.sadd("{pipe_set}1", "a", "b", "c")
        cache.sadd("{pipe_set}2", "b", "c", "d")

        pipe = cache.pipeline()
        pipe.sdiff(["{pipe_set}1", "{pipe_set}2"])
        pipe.sinter(["{pipe_set}1", "{pipe_set}2"])
        pipe.sunion(["{pipe_set}1", "{pipe_set}2"])
        results = pipe.execute()

        assert results[0] == {"a"}
        assert results[1] == {"b", "c"}
        assert results[2] == {"a", "b", "c", "d"}

    def test_pipeline_spop(self, cache: KeyValueCache):
        """Test spop in pipeline."""
        cache.sadd("pipe_set4", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.spop("pipe_set4")
        pipe.scard("pipe_set4")
        results = pipe.execute()

        assert results[0] in {"a", "b", "c"}
        assert results[1] == 2

    def test_pipeline_smismember(self, cache: KeyValueCache):
        """Test smismember in pipeline."""
        cache.sadd("pipe_set5", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.smismember("pipe_set5", "a", "z", "b")
        results = pipe.execute()

        assert results[0] == [True, False, True]

    def test_pipeline_smove(self, cache: KeyValueCache, client_class: str):
        """Test smove in pipeline."""
        # Redis Cluster doesn't support smove in pipeline mode
        if client_class == "cluster":
            pytest.skip("smove blocked in cluster pipeline mode")

        # Use hash tags for cluster compatibility
        cache.sadd("{pipe_smove}src", "a", "b")
        cache.sadd("{pipe_smove}dst", "x")

        pipe = cache.pipeline()
        pipe.smove("{pipe_smove}src", "{pipe_smove}dst", "a")
        pipe.smembers("{pipe_smove}src")
        pipe.smembers("{pipe_smove}dst")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] == {"b"}
        assert results[2] == {"x", "a"}


class TestPipelineHashOperations:
    """Test hash operations in pipeline."""

    def test_pipeline_hset_hget(self, cache: KeyValueCache):
        """Test hset/hget in pipeline."""
        pipe = cache.pipeline()
        pipe.hset("pipe_hash", "field1", "value1")
        pipe.hset("pipe_hash", "field2", {"nested": "value"})
        pipe.hget("pipe_hash", "field1")
        pipe.hget("pipe_hash", "field2")
        results = pipe.execute()

        assert results[0] == 1  # 1 field added
        assert results[1] == 1
        assert results[2] == "value1"
        assert results[3] == {"nested": "value"}

    def test_pipeline_hset_mapping_hmget(self, cache: KeyValueCache):
        """Test hset with mapping / hmget in pipeline."""
        pipe = cache.pipeline()
        pipe.hset("pipe_hash2", mapping={"f1": "v1", "f2": "v2", "f3": "v3"})
        pipe.hmget("pipe_hash2", "f1", "f3", "nonexistent")
        results = pipe.execute()

        assert results[0] == 3  # fields added
        assert results[1] == ["v1", "v3", None]

    def test_pipeline_hgetall(self, cache: KeyValueCache):
        """Test hgetall in pipeline."""
        cache.hset("pipe_hash3", mapping={"a": "1", "b": "2"})

        pipe = cache.pipeline()
        pipe.hgetall("pipe_hash3")
        results = pipe.execute()

        assert results[0] == {"a": "1", "b": "2"}

    def test_pipeline_hdel_hlen(self, cache: KeyValueCache):
        """Test hdel/hlen in pipeline."""
        cache.hset("pipe_hash4", mapping={"a": "1", "b": "2", "c": "3"})

        pipe = cache.pipeline()
        pipe.hdel("pipe_hash4", "b")
        pipe.hlen("pipe_hash4")
        results = pipe.execute()

        assert results[0] == 1
        assert results[1] == 2

    def test_pipeline_hkeys_hvals(self, cache: KeyValueCache):
        """Test hkeys/hvals in pipeline."""
        cache.hset("pipe_hash5", mapping={"a": "1", "b": "2"})

        pipe = cache.pipeline()
        pipe.hkeys("pipe_hash5")
        pipe.hvals("pipe_hash5")
        results = pipe.execute()

        assert set(results[0]) == {"a", "b"}
        assert set(results[1]) == {"1", "2"}

    def test_pipeline_hexists(self, cache: KeyValueCache):
        """Test hexists in pipeline."""
        cache.hset("pipe_hash6", "field", "value")

        pipe = cache.pipeline()
        pipe.hexists("pipe_hash6", "field")
        pipe.hexists("pipe_hash6", "nonexistent")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is False

    def test_pipeline_hincrby(self, cache: KeyValueCache):
        """Test hincrby/hincrbyfloat in pipeline."""
        cache.hset("pipe_hash7", "count", 10)

        pipe = cache.pipeline()
        pipe.hincrby("pipe_hash7", "count", 5)
        pipe.hincrby("pipe_hash7", "count", -3)
        pipe.hincrbyfloat("pipe_hash7", "float_val", 1.5)
        results = pipe.execute()

        assert results[0] == 15
        assert results[1] == 12
        assert results[2] == 1.5

    def test_pipeline_hsetnx(self, cache: KeyValueCache):
        """Test hsetnx in pipeline."""
        cache.hset("pipe_hash8", "existing", "value")

        pipe = cache.pipeline()
        pipe.hsetnx("pipe_hash8", "existing", "new_value")
        pipe.hsetnx("pipe_hash8", "new_field", "new_value")
        pipe.hget("pipe_hash8", "existing")
        pipe.hget("pipe_hash8", "new_field")
        results = pipe.execute()

        assert results[0] is False  # not set, field exists
        assert results[1] is True  # set, new field
        assert results[2] == "value"  # unchanged
        assert results[3] == "new_value"


class TestPipelineSortedSetOperations:
    """Test sorted set operations in pipeline."""

    def test_pipeline_zadd_zrange(self, cache: KeyValueCache):
        """Test zadd/zrange in pipeline."""
        pipe = cache.pipeline()
        pipe.zadd("pipe_zset", {"a": 1, "b": 2, "c": 3})
        pipe.zrange("pipe_zset", 0, -1)
        pipe.zrange("pipe_zset", 0, -1, withscores=True)
        results = pipe.execute()

        assert results[0] == 3
        assert results[1] == ["a", "b", "c"]
        assert results[2] == [("a", 1.0), ("b", 2.0), ("c", 3.0)]

    def test_pipeline_zcard_zcount(self, cache: KeyValueCache):
        """Test zcard/zcount in pipeline."""
        cache.zadd("pipe_zset2", {"a": 1, "b": 2, "c": 3, "d": 4})

        pipe = cache.pipeline()
        pipe.zcard("pipe_zset2")
        pipe.zcount("pipe_zset2", 2, 3)
        results = pipe.execute()

        assert results[0] == 4
        assert results[1] == 2  # b and c

    def test_pipeline_zincrby(self, cache: KeyValueCache):
        """Test zincrby in pipeline."""
        cache.zadd("pipe_zset3", {"item": 10})

        pipe = cache.pipeline()
        pipe.zincrby("pipe_zset3", 5, "item")
        pipe.zincrby("pipe_zset3", -3, "item")
        pipe.zscore("pipe_zset3", "item")
        results = pipe.execute()

        assert results[0] == 15.0
        assert results[1] == 12.0
        assert results[2] == 12.0

    def test_pipeline_zrank_zrevrank(self, cache: KeyValueCache):
        """Test zrank/zrevrank in pipeline."""
        cache.zadd("pipe_zset4", {"a": 1, "b": 2, "c": 3})

        pipe = cache.pipeline()
        pipe.zrank("pipe_zset4", "b")
        pipe.zrevrank("pipe_zset4", "b")
        pipe.zrank("pipe_zset4", "nonexistent")
        results = pipe.execute()

        assert results[0] == 1  # 0-indexed
        assert results[1] == 1  # reversed: c=0, b=1, a=2
        assert results[2] is None

    def test_pipeline_zrem(self, cache: KeyValueCache):
        """Test zrem in pipeline."""
        cache.zadd("pipe_zset5", {"a": 1, "b": 2, "c": 3})

        pipe = cache.pipeline()
        pipe.zrem("pipe_zset5", "b")
        pipe.zrange("pipe_zset5", 0, -1)
        results = pipe.execute()

        assert results[0] == 1
        assert results[1] == ["a", "c"]

    def test_pipeline_zpopmin_zpopmax(self, cache: KeyValueCache):
        """Test zpopmin/zpopmax in pipeline."""
        cache.zadd("pipe_zset6", {"a": 1, "b": 2, "c": 3})

        pipe = cache.pipeline()
        pipe.zpopmin("pipe_zset6")
        pipe.zpopmax("pipe_zset6")
        pipe.zrange("pipe_zset6", 0, -1)
        results = pipe.execute()

        assert results[0] == ("a", 1.0)
        assert results[1] == ("c", 3.0)
        assert results[2] == ["b"]

    def test_pipeline_zrangebyscore(self, cache: KeyValueCache):
        """Test zrangebyscore/zrevrangebyscore in pipeline."""
        cache.zadd("pipe_zset7", {"a": 1, "b": 2, "c": 3, "d": 4})

        pipe = cache.pipeline()
        pipe.zrangebyscore("pipe_zset7", 2, 3)
        pipe.zrevrangebyscore("pipe_zset7", 3, 2)
        results = pipe.execute()

        assert results[0] == ["b", "c"]
        assert results[1] == ["c", "b"]

    def test_pipeline_zremrangebyscore(self, cache: KeyValueCache):
        """Test zremrangebyscore in pipeline."""
        cache.zadd("pipe_zset8", {"a": 1, "b": 2, "c": 3, "d": 4})

        pipe = cache.pipeline()
        pipe.zremrangebyscore("pipe_zset8", 2, 3)
        pipe.zrange("pipe_zset8", 0, -1)
        results = pipe.execute()

        assert results[0] == 2  # removed b and c
        assert results[1] == ["a", "d"]

    def test_pipeline_zremrangebyrank(self, cache: KeyValueCache):
        """Test zremrangebyrank in pipeline."""
        cache.zadd("pipe_zset9", {"a": 1, "b": 2, "c": 3, "d": 4})

        pipe = cache.pipeline()
        pipe.zremrangebyrank("pipe_zset9", 1, 2)
        pipe.zrange("pipe_zset9", 0, -1)
        results = pipe.execute()

        assert results[0] == 2  # removed b and c (indexes 1 and 2)
        assert results[1] == ["a", "d"]

    def test_pipeline_zmscore(self, cache: KeyValueCache):
        """Test zmscore in pipeline."""
        cache.zadd("pipe_zset10", {"a": 1, "b": 2, "c": 3})

        pipe = cache.pipeline()
        pipe.zmscore("pipe_zset10", "a", "b", "nonexistent")
        results = pipe.execute()

        assert results[0] == [1.0, 2.0, None]


class TestPipelineVersionSupport:
    """Test version parameter support in pipeline."""

    def test_pipeline_with_version(self, cache: KeyValueCache):
        """Test pipeline operations with version parameter."""
        pipe = cache.pipeline(version=1)
        pipe.set("versioned_key", "v1_value")
        pipe.get("versioned_key")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] == "v1_value"

        # Different version should not see the key
        assert cache.get("versioned_key", version=2) is None
        # Same version should see it
        assert cache.get("versioned_key", version=1) == "v1_value"

    def test_pipeline_version_override(self, cache: KeyValueCache):
        """Test that individual operations can override pipeline version."""
        pipe = cache.pipeline(version=1)
        pipe.set("key_v1", "value1")
        pipe.set("key_v2", "value2", version=2)  # Override version
        pipe.get("key_v1")
        pipe.get("key_v2", version=2)
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is True
        assert results[2] == "value1"
        assert results[3] == "value2"

        # Verify different versions
        assert cache.get("key_v1", version=1) == "value1"
        assert cache.get("key_v1", version=2) is None
        assert cache.get("key_v2", version=1) is None
        assert cache.get("key_v2", version=2) == "value2"


class TestPipelineMixedOperations:
    """Test mixing different operation types in a single pipeline."""

    def test_mixed_data_structures(self, cache: KeyValueCache):
        """Test using different data structures in one pipeline."""
        pipe = cache.pipeline()
        # Cache operations
        pipe.set("string_key", "string_value")
        # List operations
        pipe.rpush("list_key", "a", "b", "c")
        # Set operations
        pipe.sadd("set_key", "x", "y", "z")
        # Hash operations
        pipe.hset("hash_key", "field", "value")
        # Sorted set operations
        pipe.zadd("zset_key", {"member": 1.0})

        # Read them all back
        pipe.get("string_key")
        pipe.lrange("list_key", 0, -1)
        pipe.smembers("set_key")
        pipe.hget("hash_key", "field")
        pipe.zrange("zset_key", 0, -1)

        results = pipe.execute()

        # Write results
        assert results[0] is True  # set
        assert results[1] == 3  # rpush
        assert results[2] == 3  # sadd
        assert results[3] == 1  # hset
        assert results[4] == 1  # zadd

        # Read results
        assert results[5] == "string_value"
        assert results[6] == ["a", "b", "c"]
        assert results[7] == {"x", "y", "z"}
        assert results[8] == "value"
        assert results[9] == ["member"]


class TestPipelineMissingOperations:
    """Test operations that were missing from initial coverage."""

    def test_pipeline_set_with_timeout(self, cache: KeyValueCache):
        """Test set with timeout parameter."""
        pipe = cache.pipeline()
        pipe.set("timeout_key", "value", timeout=100)
        pipe.ttl("timeout_key")
        results = pipe.execute()

        assert results[0] is True
        assert 0 < results[1] <= 100

    def test_pipeline_set_nx(self, cache: KeyValueCache):
        """Test set with nx (only if not exists)."""
        cache.set("nx_existing", "original")

        pipe = cache.pipeline()
        pipe.set("nx_existing", "new_value", nx=True)  # Should fail
        pipe.set("nx_new", "new_value", nx=True)  # Should succeed
        pipe.get("nx_existing")
        pipe.get("nx_new")
        results = pipe.execute()

        assert results[0] is None  # nx failed, key existed
        assert results[1] is True  # nx succeeded
        assert results[2] == "original"  # unchanged
        assert results[3] == "new_value"

    def test_pipeline_set_xx(self, cache: KeyValueCache):
        """Test set with xx (only if exists)."""
        cache.set("xx_existing", "original")

        pipe = cache.pipeline()
        pipe.set("xx_existing", "updated", xx=True)  # Should succeed
        pipe.set("xx_new", "value", xx=True)  # Should fail
        pipe.get("xx_existing")
        pipe.exists("xx_new")
        results = pipe.execute()

        assert results[0] is True  # xx succeeded
        assert results[1] is None  # xx failed, key didn't exist
        assert results[2] == "updated"
        assert results[3] is False  # key wasn't created

    def test_pipeline_srandmember(self, cache: KeyValueCache):
        """Test srandmember in pipeline."""
        cache.sadd("srand_set", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.srandmember("srand_set")  # Single random member
        pipe.srandmember("srand_set", count=2)  # Multiple random members
        results = pipe.execute()

        assert results[0] in {"a", "b", "c"}
        assert len(results[1]) == 2
        assert all(m in {"a", "b", "c"} for m in results[1])

    def test_pipeline_sdiffstore(self, cache: KeyValueCache, client_class: str):
        """Test sdiffstore in pipeline."""
        # Redis Cluster doesn't support sdiffstore in pipeline mode
        if client_class == "cluster":
            pytest.skip("sdiffstore blocked in cluster pipeline mode")

        # Use hash tags for cluster compatibility
        cache.sadd("{sdiff}set1", "a", "b", "c")
        cache.sadd("{sdiff}set2", "b", "c", "d")

        pipe = cache.pipeline()
        pipe.sdiffstore("{sdiff}dest", ["{sdiff}set1", "{sdiff}set2"])
        pipe.smembers("{sdiff}dest")
        results = pipe.execute()

        assert results[0] == 1  # Count of elements in result
        assert results[1] == {"a"}

    def test_pipeline_sinterstore(self, cache: KeyValueCache, client_class: str):
        """Test sinterstore in pipeline."""
        # Redis Cluster doesn't support sinterstore in pipeline mode
        if client_class == "cluster":
            pytest.skip("sinterstore blocked in cluster pipeline mode")

        # Use hash tags for cluster compatibility
        cache.sadd("{sinter}set1", "a", "b", "c")
        cache.sadd("{sinter}set2", "b", "c", "d")

        pipe = cache.pipeline()
        pipe.sinterstore("{sinter}dest", ["{sinter}set1", "{sinter}set2"])
        pipe.smembers("{sinter}dest")
        results = pipe.execute()

        assert results[0] == 2  # Count of elements in result
        assert results[1] == {"b", "c"}

    def test_pipeline_sunionstore(self, cache: KeyValueCache, client_class: str):
        """Test sunionstore in pipeline."""
        # Redis Cluster doesn't support sunionstore in pipeline mode
        if client_class == "cluster":
            pytest.skip("sunionstore blocked in cluster pipeline mode")

        # Use hash tags for cluster compatibility
        cache.sadd("{sunion}set1", "a", "b")
        cache.sadd("{sunion}set2", "c", "d")

        pipe = cache.pipeline()
        pipe.sunionstore("{sunion}dest", ["{sunion}set1", "{sunion}set2"])
        pipe.smembers("{sunion}dest")
        results = pipe.execute()

        assert results[0] == 4  # Count of elements in result
        assert results[1] == {"a", "b", "c", "d"}

    def test_pipeline_zrevrange(self, cache: KeyValueCache):
        """Test zrevrange in pipeline."""
        cache.zadd("zrev_set", {"a": 1, "b": 2, "c": 3, "d": 4})

        pipe = cache.pipeline()
        pipe.zrevrange("zrev_set", 0, -1)
        pipe.zrevrange("zrev_set", 0, 1)
        pipe.zrevrange("zrev_set", 0, -1, withscores=True)
        results = pipe.execute()

        assert results[0] == ["d", "c", "b", "a"]
        assert results[1] == ["d", "c"]
        assert results[2] == [("d", 4.0), ("c", 3.0), ("b", 2.0), ("a", 1.0)]


class TestPipelineCommandCombinations:
    """Test various combinations of commands to verify chaining works correctly."""

    def test_combination_counter_pattern(self, cache: KeyValueCache):
        """Test counter pattern: set initial, incr multiple times, get final."""
        pipe = cache.pipeline()
        pipe.set("{combo1}counter", 0)
        pipe.incr("{combo1}counter")
        pipe.incr("{combo1}counter", 5)
        pipe.incr("{combo1}counter", 10)
        pipe.decr("{combo1}counter", 3)
        pipe.get("{combo1}counter")
        results = pipe.execute()

        assert results[0] is True  # set
        assert results[1] == 1  # 0 + 1
        assert results[2] == 6  # 1 + 5
        assert results[3] == 16  # 6 + 10
        assert results[4] == 13  # 16 - 3
        assert results[5] == 13  # final value

    def test_combination_list_queue_pattern(self, cache: KeyValueCache):
        """Test queue pattern: push items, check length, pop items."""
        pipe = cache.pipeline()
        pipe.rpush("{combo2}queue", "task1", "task2", "task3")
        pipe.llen("{combo2}queue")
        pipe.lpop("{combo2}queue")
        pipe.llen("{combo2}queue")
        pipe.lpop("{combo2}queue")
        pipe.lrange("{combo2}queue", 0, -1)
        results = pipe.execute()

        assert results[0] == 3  # rpush count
        assert results[1] == 3  # llen
        assert results[2] == "task1"  # first pop
        assert results[3] == 2  # llen after pop
        assert results[4] == "task2"  # second pop
        assert results[5] == ["task3"]  # remaining

    def test_combination_hash_user_profile(self, cache: KeyValueCache):
        """Test user profile pattern: set fields, check existence, get all."""
        pipe = cache.pipeline()
        pipe.hset("{combo3}user:1", "name", "Alice")
        pipe.hset("{combo3}user:1", "email", "alice@example.com")
        pipe.hincrby("{combo3}user:1", "login_count", 1)
        pipe.hexists("{combo3}user:1", "name")
        pipe.hexists("{combo3}user:1", "phone")
        pipe.hgetall("{combo3}user:1")
        results = pipe.execute()

        assert results[0] == 1  # hset name
        assert results[1] == 1  # hset email
        assert results[2] == 1  # hincrby
        assert results[3] is True  # name exists
        assert results[4] is False  # phone doesn't exist
        assert results[5]["name"] == "Alice"
        assert results[5]["email"] == "alice@example.com"
        assert results[5]["login_count"] == 1

    def test_combination_sorted_set_leaderboard(self, cache: KeyValueCache):
        """Test leaderboard pattern: add scores, get rankings, update scores."""
        pipe = cache.pipeline()
        pipe.zadd("{combo4}leaderboard", {"alice": 100, "bob": 85, "charlie": 92})
        pipe.zrevrange("{combo4}leaderboard", 0, 2, withscores=True)  # Top 3
        pipe.zincrby("{combo4}leaderboard", 20, "bob")  # Bob gets bonus
        pipe.zrevrange("{combo4}leaderboard", 0, 0)  # New leader
        pipe.zrank("{combo4}leaderboard", "charlie")  # Charlie's rank (0-indexed, low to high)
        results = pipe.execute()

        assert results[0] == 3  # zadd count
        assert results[1][0][0] == "alice"  # alice was #1
        assert results[2] == 105.0  # bob's new score
        assert results[3] == ["bob"]  # bob is now #1
        assert results[4] == 0  # charlie is lowest (rank 0 in ascending order)
