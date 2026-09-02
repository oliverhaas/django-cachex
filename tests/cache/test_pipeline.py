"""Tests for pipeline operations."""

import time
import warnings
from typing import TYPE_CHECKING

import pytest

from django_cachex.adapters.pipeline import AsyncPipeline, Pipeline
from django_cachex.exceptions import NotSupportedError
from django_cachex.types import KeyType

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


class TestPipelineBasic:
    def test_pipeline_returns_pipeline_object(self, cache: RespCache):
        pipe = cache.pipeline()
        assert isinstance(pipe, Pipeline)

    def test_pipeline_manual_execute(self, cache: RespCache):
        cache.set("manual_key", "manual_value")

        pipe = cache.pipeline()
        pipe.get("manual_key")
        pipe.set("new_key", "new_value")
        pipe.get("new_key")
        results = pipe.execute()

        assert results[0] == "manual_value"
        assert results[1] is True
        assert results[2] == "new_value"

    def test_pipeline_empty_execute(self, cache: RespCache):
        pipe = cache.pipeline()
        results = pipe.execute()
        assert results == []

    def test_pipeline_chaining(self, cache: RespCache):
        pipe = cache.pipeline()
        result = pipe.set("chain1", "a").set("chain2", "b").get("chain1").get("chain2")
        assert result is pipe

        results = pipe.execute()
        assert results == [True, True, "a", "b"]

    def test_pipeline_transaction(
        self,
        cache: RespCache,
        client_class: str,
        sentinel_mode: str | bool,
    ):
        if client_class == "cluster" and not sentinel_mode:
            with pytest.raises(NotSupportedError):
                cache.pipeline(transaction=True)
            return
        pipe = cache.pipeline(transaction=True)
        pipe.set("tx_key", "tx_value")
        pipe.get("tx_key")
        results = pipe.execute()
        assert results == [True, "tx_value"]

    def test_pipeline_no_transaction(self, cache: RespCache):
        pipe = cache.pipeline(transaction=False)
        pipe.set("notx_key", "notx_value")
        pipe.get("notx_key")
        results = pipe.execute()
        assert results == [True, "notx_value"]


class TestPipelineCacheOperations:
    def test_pipeline_get_set(self, cache: RespCache):
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

    def test_pipeline_delete(self, cache: RespCache):
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

    def test_pipeline_exists(self, cache: RespCache):
        cache.set("{pipe_ex}key", "value")

        pipe = cache.pipeline()
        pipe.exists("{pipe_ex}key")
        pipe.exists("{pipe_ex}nonexistent")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is False

    def test_pipeline_expire_ttl(self, cache: RespCache):
        cache.set("expire_key", "value")

        pipe = cache.pipeline()
        pipe.expire("expire_key", 100)
        pipe.ttl("expire_key")
        results = pipe.execute()

        assert results[0] is True
        assert 0 < results[1] <= 100

    def test_pipeline_incr_decr(self, cache: RespCache):
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
    def test_pipeline_lpush_rpush(self, cache: RespCache):
        pipe = cache.pipeline()
        pipe.rpush("pipe_list", "a", "b", "c")
        pipe.lpush("pipe_list", "x", "y")
        pipe.lrange("pipe_list", 0, -1)
        results = pipe.execute()

        assert results[0] == 3  # rpush returns new length
        assert results[1] == 5  # lpush returns new length
        assert results[2] == ["y", "x", "a", "b", "c"]

    def test_pipeline_lpop_rpop(self, cache: RespCache):
        cache.rpush("pipe_list2", "a", "b", "c", "d")

        pipe = cache.pipeline()
        pipe.lpop("pipe_list2")
        pipe.rpop("pipe_list2")
        pipe.lpop("pipe_list2", count=2)
        results = pipe.execute()

        assert results[0] == "a"
        assert results[1] == "d"
        assert results[2] == ["b", "c"]

    def test_pipeline_llen_lindex(self, cache: RespCache):
        cache.rpush("pipe_list3", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.llen("pipe_list3")
        pipe.lindex("pipe_list3", 0)
        pipe.lindex("pipe_list3", -1)
        results = pipe.execute()

        assert results[0] == 3
        assert results[1] == "a"
        assert results[2] == "c"

    def test_pipeline_lset_lrem(self, cache: RespCache):
        cache.rpush("pipe_list4", "a", "b", "a", "c")

        pipe = cache.pipeline()
        pipe.lset("pipe_list4", 1, "B")
        pipe.lrem("pipe_list4", 1, "a")
        pipe.lrange("pipe_list4", 0, -1)
        results = pipe.execute()

        assert results[0] is True
        assert results[1] == 1
        assert results[2] == ["B", "a", "c"]

    def test_pipeline_ltrim(self, cache: RespCache):
        cache.rpush("pipe_list5", "a", "b", "c", "d", "e")

        pipe = cache.pipeline()
        pipe.ltrim("pipe_list5", 1, 3)
        pipe.lrange("pipe_list5", 0, -1)
        results = pipe.execute()

        assert results[0] is True
        assert results[1] == ["b", "c", "d"]

    def test_pipeline_linsert(self, cache: RespCache):
        cache.rpush("pipe_list6", "a", "c")

        pipe = cache.pipeline()
        pipe.linsert("pipe_list6", "BEFORE", "c", "b")
        pipe.linsert("pipe_list6", "AFTER", "c", "d")
        pipe.lrange("pipe_list6", 0, -1)
        results = pipe.execute()

        assert results[0] == 3
        assert results[1] == 4
        assert results[2] == ["a", "b", "c", "d"]

    def test_pipeline_lpos(self, cache: RespCache):
        cache.rpush("pipe_list7", "a", "b", "c", "b", "d")

        pipe = cache.pipeline()
        pipe.lpos("pipe_list7", "b")
        pipe.lpos("pipe_list7", "b", rank=2)
        pipe.lpos("pipe_list7", "z")
        results = pipe.execute()

        assert results[0] == 1
        assert results[1] == 3
        assert results[2] is None

    def test_pipeline_lmove(self, cache: RespCache):
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
    def test_pipeline_sadd_smembers(self, cache: RespCache):
        pipe = cache.pipeline()
        pipe.sadd("pipe_set", "a", "b", "c")
        pipe.smembers("pipe_set")
        results = pipe.execute()

        assert results[0] == 3
        assert results[1] == {"a", "b", "c"}

    def test_pipeline_scard_sismember(self, cache: RespCache):
        cache.sadd("pipe_set2", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.scard("pipe_set2")
        pipe.sismember("pipe_set2", "b")
        pipe.sismember("pipe_set2", "z")
        results = pipe.execute()

        assert results[0] == 3
        assert results[1] is True
        assert results[2] is False

    def test_pipeline_srem(self, cache: RespCache):
        cache.sadd("pipe_set3", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.srem("pipe_set3", "b", "c")
        pipe.smembers("pipe_set3")
        results = pipe.execute()

        assert results[0] == 2
        assert results[1] == {"a"}

    def test_pipeline_sdiff_sinter_sunion(self, cache: RespCache, client_class: str, sentinel_mode: str | bool):
        if client_class == "cluster" and not sentinel_mode:
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

    def test_pipeline_spop(self, cache: RespCache):
        cache.sadd("pipe_set4", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.spop("pipe_set4")
        pipe.scard("pipe_set4")
        results = pipe.execute()

        assert results[0] in {"a", "b", "c"}
        assert results[1] == 2

    def test_pipeline_smismember(self, cache: RespCache):
        cache.sadd("pipe_set5", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.smismember("pipe_set5", "a", "z", "b")
        results = pipe.execute()

        assert results[0] == [True, False, True]

    def test_pipeline_smove(self, cache: RespCache, client_class: str, sentinel_mode: str | bool):
        if client_class == "cluster" and not sentinel_mode:
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
    def test_pipeline_hset_hget(self, cache: RespCache):
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

    def test_pipeline_hset_mapping_hmget(self, cache: RespCache):
        pipe = cache.pipeline()
        pipe.hset("pipe_hash2", mapping={"f1": "v1", "f2": "v2", "f3": "v3"})
        pipe.hmget("pipe_hash2", "f1", "f3", "nonexistent")
        results = pipe.execute()

        assert results[0] == 3  # fields added
        assert results[1] == ["v1", "v3", None]

    def test_pipeline_hgetall(self, cache: RespCache):
        cache.hset("pipe_hash3", mapping={"a": "1", "b": "2"})

        pipe = cache.pipeline()
        pipe.hgetall("pipe_hash3")
        results = pipe.execute()

        assert results[0] == {"a": "1", "b": "2"}

    def test_pipeline_hdel_hlen(self, cache: RespCache):
        cache.hset("pipe_hash4", mapping={"a": "1", "b": "2", "c": "3"})

        pipe = cache.pipeline()
        pipe.hdel("pipe_hash4", "b")
        pipe.hlen("pipe_hash4")
        results = pipe.execute()

        assert results[0] == 1
        assert results[1] == 2

    def test_pipeline_hkeys_hvals(self, cache: RespCache):
        cache.hset("pipe_hash5", mapping={"a": "1", "b": "2"})

        pipe = cache.pipeline()
        pipe.hkeys("pipe_hash5")
        pipe.hvals("pipe_hash5")
        results = pipe.execute()

        assert set(results[0]) == {"a", "b"}
        assert set(results[1]) == {"1", "2"}

    def test_pipeline_hexists(self, cache: RespCache):
        cache.hset("pipe_hash6", "field", "value")

        pipe = cache.pipeline()
        pipe.hexists("pipe_hash6", "field")
        pipe.hexists("pipe_hash6", "nonexistent")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is False

    def test_pipeline_hincrby(self, cache: RespCache):
        cache.hset("pipe_hash7", "count", 10)

        pipe = cache.pipeline()
        pipe.hincrby("pipe_hash7", "count", 5)
        pipe.hincrby("pipe_hash7", "count", -3)
        pipe.hincrbyfloat("pipe_hash7", "float_val", 1.5)
        results = pipe.execute()

        assert results[0] == 15
        assert results[1] == 12
        assert results[2] == 1.5

    def test_pipeline_hsetnx(self, cache: RespCache):
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
    def test_pipeline_zadd_zrange(self, cache: RespCache):
        pipe = cache.pipeline()
        pipe.zadd("pipe_zset", {"a": 1, "b": 2, "c": 3})
        pipe.zrange("pipe_zset", 0, -1)
        pipe.zrange("pipe_zset", 0, -1, withscores=True)
        results = pipe.execute()

        assert results[0] == 3
        assert results[1] == ["a", "b", "c"]
        assert results[2] == [("a", 1.0), ("b", 2.0), ("c", 3.0)]

    def test_pipeline_zcard_zcount(self, cache: RespCache):
        cache.zadd("pipe_zset2", {"a": 1, "b": 2, "c": 3, "d": 4})

        pipe = cache.pipeline()
        pipe.zcard("pipe_zset2")
        pipe.zcount("pipe_zset2", 2, 3)
        results = pipe.execute()

        assert results[0] == 4
        assert results[1] == 2  # b and c

    def test_pipeline_zincrby(self, cache: RespCache):
        cache.zadd("pipe_zset3", {"item": 10})

        pipe = cache.pipeline()
        pipe.zincrby("pipe_zset3", 5, "item")
        pipe.zincrby("pipe_zset3", -3, "item")
        pipe.zscore("pipe_zset3", "item")
        results = pipe.execute()

        assert results[0] == 15.0
        assert results[1] == 12.0
        assert results[2] == 12.0

    def test_pipeline_zrank_zrevrank(self, cache: RespCache):
        cache.zadd("pipe_zset4", {"a": 1, "b": 2, "c": 3})

        pipe = cache.pipeline()
        pipe.zrank("pipe_zset4", "b")
        pipe.zrevrank("pipe_zset4", "b")
        pipe.zrank("pipe_zset4", "nonexistent")
        results = pipe.execute()

        assert results[0] == 1  # 0-indexed
        assert results[1] == 1  # reversed: c=0, b=1, a=2
        assert results[2] is None

    def test_pipeline_zrem(self, cache: RespCache):
        cache.zadd("pipe_zset5", {"a": 1, "b": 2, "c": 3})

        pipe = cache.pipeline()
        pipe.zrem("pipe_zset5", "b")
        pipe.zrange("pipe_zset5", 0, -1)
        results = pipe.execute()

        assert results[0] == 1
        assert results[1] == ["a", "c"]

    def test_pipeline_zpopmin_zpopmax(self, cache: RespCache):
        cache.zadd("pipe_zset6", {"a": 1, "b": 2, "c": 3})

        pipe = cache.pipeline()
        pipe.zpopmin("pipe_zset6")
        pipe.zpopmax("pipe_zset6")
        pipe.zrange("pipe_zset6", 0, -1)
        results = pipe.execute()

        assert results[0] == [("a", 1.0)]
        assert results[1] == [("c", 3.0)]
        assert results[2] == ["b"]

    def test_pipeline_zrangebyscore(self, cache: RespCache):
        cache.zadd("pipe_zset7", {"a": 1, "b": 2, "c": 3, "d": 4})

        pipe = cache.pipeline()
        pipe.zrangebyscore("pipe_zset7", 2, 3)
        pipe.zrevrangebyscore("pipe_zset7", 3, 2)
        results = pipe.execute()

        assert results[0] == ["b", "c"]
        assert results[1] == ["c", "b"]

    def test_pipeline_zremrangebyscore(self, cache: RespCache):
        cache.zadd("pipe_zset8", {"a": 1, "b": 2, "c": 3, "d": 4})

        pipe = cache.pipeline()
        pipe.zremrangebyscore("pipe_zset8", 2, 3)
        pipe.zrange("pipe_zset8", 0, -1)
        results = pipe.execute()

        assert results[0] == 2  # removed b and c
        assert results[1] == ["a", "d"]

    def test_pipeline_zremrangebyrank(self, cache: RespCache):
        cache.zadd("pipe_zset9", {"a": 1, "b": 2, "c": 3, "d": 4})

        pipe = cache.pipeline()
        pipe.zremrangebyrank("pipe_zset9", 1, 2)
        pipe.zrange("pipe_zset9", 0, -1)
        results = pipe.execute()

        assert results[0] == 2  # removed b and c (indexes 1 and 2)
        assert results[1] == ["a", "d"]

    def test_pipeline_zmscore(self, cache: RespCache):
        cache.zadd("pipe_zset10", {"a": 1, "b": 2, "c": 3})

        pipe = cache.pipeline()
        pipe.zmscore("pipe_zset10", "a", "b", "nonexistent")
        results = pipe.execute()

        assert results[0] == [1.0, 2.0, None]


class TestPipelineVersionSupport:
    def test_pipeline_with_version(self, cache: RespCache):
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

    def test_pipeline_version_override(self, cache: RespCache):
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

        assert cache.get("key_v1", version=1) == "value1"
        assert cache.get("key_v1", version=2) is None
        assert cache.get("key_v2", version=1) is None
        assert cache.get("key_v2", version=2) == "value2"


class TestPipelineMixedOperations:
    def test_mixed_data_structures(self, cache: RespCache):
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


class TestPipelineSetFlagsAndStoreOperations:
    def test_pipeline_set_with_timeout(self, cache: RespCache):
        pipe = cache.pipeline()
        pipe.set("timeout_key", "value", timeout=100)
        pipe.ttl("timeout_key")
        results = pipe.execute()

        assert results[0] is True
        assert 0 < results[1] <= 100

    def test_pipeline_set_nx(self, cache: RespCache):
        cache.set("nx_existing", "original")

        pipe = cache.pipeline()
        pipe.set("nx_existing", "new_value", nx=True)  # Should fail
        pipe.set("nx_new", "new_value", nx=True)  # Should succeed
        pipe.get("nx_existing")
        pipe.get("nx_new")
        results = pipe.execute()

        assert results[0] is False  # nx failed, key existed
        assert results[1] is True  # nx succeeded
        assert results[2] == "original"  # unchanged
        assert results[3] == "new_value"

    def test_pipeline_set_xx(self, cache: RespCache):
        cache.set("xx_existing", "original")

        pipe = cache.pipeline()
        pipe.set("xx_existing", "updated", xx=True)  # Should succeed
        pipe.set("xx_new", "value", xx=True)  # Should fail
        pipe.get("xx_existing")
        pipe.exists("xx_new")
        results = pipe.execute()

        assert results[0] is True  # xx succeeded
        assert results[1] is False  # xx failed, key didn't exist
        assert results[2] == "updated"
        assert results[3] is False  # key wasn't created

    def test_pipeline_srandmember(self, cache: RespCache):
        cache.sadd("srand_set", "a", "b", "c")

        pipe = cache.pipeline()
        pipe.srandmember("srand_set")  # Single random member
        pipe.srandmember("srand_set", count=2)  # Multiple random members
        results = pipe.execute()

        assert results[0] in {"a", "b", "c"}
        assert len(results[1]) == 2
        assert all(m in {"a", "b", "c"} for m in results[1])

    def test_pipeline_sdiffstore(self, cache: RespCache, client_class: str, sentinel_mode: str | bool):
        if client_class == "cluster" and not sentinel_mode:
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

    def test_pipeline_sinterstore(self, cache: RespCache, client_class: str, sentinel_mode: str | bool):
        if client_class == "cluster" and not sentinel_mode:
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

    def test_pipeline_sunionstore(self, cache: RespCache, client_class: str, sentinel_mode: str | bool):
        if client_class == "cluster" and not sentinel_mode:
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

    def test_pipeline_zrevrange(self, cache: RespCache):
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
    def test_combination_counter_pattern(self, cache: RespCache):
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

    def test_combination_list_queue_pattern(self, cache: RespCache):
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

    def test_combination_hash_user_profile(self, cache: RespCache):
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

    def test_combination_sorted_set_leaderboard(self, cache: RespCache):
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


class TestPipelineStreamOps:
    def test_pipeline_xpending_range(self, cache: RespCache):
        """Pipeline xpending with range params must use xpending_range."""
        cache.xadd("pipe_pend", {"msg": "test"})
        cache.xgroup_create("pipe_pend", "grp", entry_id="0")
        cache.xreadgroup("grp", "c1", {"pipe_pend": ">"})

        pipe = cache.pipeline()
        pipe.xpending("pipe_pend", "grp", start="-", end="+", count=10)
        results = pipe.execute()

        assert isinstance(results[0], list)
        assert len(results[0]) == 1


class TestPipelineReuse:
    def test_multiple_execute_calls(self, cache: RespCache):
        """Pipeline can be executed multiple times without decoder misalignment."""
        pipe = cache.pipeline()

        # First batch
        pipe.set("reuse_a", "val1")
        pipe.get("reuse_a")
        results1 = pipe.execute()
        assert results1 == [True, "val1"]

        # Second batch on the same pipeline must not crash
        pipe.set("reuse_b", "val2")
        pipe.get("reuse_b")
        results2 = pipe.execute()
        assert results2 == [True, "val2"]

    def test_context_manager_resets_decoders(self, cache: RespCache):
        """After context manager exit, pipeline decoders are cleared."""
        pipe = cache.pipeline()

        with pipe:
            pipe.set("ctx_key", "ctx_val")
            pipe.get("ctx_key")
            results = pipe.execute()
            assert results == [True, "ctx_val"]

        # After __exit__, decoders should be cleared.
        # Queuing new commands and executing should work without length mismatch.
        pipe.set("ctx_key2", "ctx_val2")
        pipe.get("ctx_key2")
        results2 = pipe.execute()
        assert results2 == [True, "ctx_val2"]

    def test_execute_empty_after_previous(self, cache: RespCache):
        """Executing with no commands after a previous execute returns empty list."""
        pipe = cache.pipeline()
        pipe.set("empty_test", "val")
        pipe.execute()
        # No new commands queued
        assert pipe.execute() == []


class TestPipelineXreadKeyUnprefixing:
    """Test that pipeline xread/xreadgroup return user-facing keys, not prefixed ones."""

    def test_pipeline_xread_returns_original_keys(self, cache: RespCache):
        cache.xadd("pipe_xread_stream", {"msg": "hello"})

        pipe = cache.pipeline()
        pipe.xread({"pipe_xread_stream": "0-0"}, count=10)
        results = pipe.execute()

        assert results[0] is not None
        # The key in the response must be the user's original key, not the prefixed one
        assert "pipe_xread_stream" in results[0]
        assert len(results[0]["pipe_xread_stream"]) == 1

    def test_pipeline_xreadgroup_returns_original_keys(self, cache: RespCache):
        cache.xadd("pipe_xrg_stream", {"msg": "world"})
        cache.xgroup_create("pipe_xrg_stream", "pipe_grp", entry_id="0")

        pipe = cache.pipeline()
        pipe.xreadgroup("pipe_grp", "consumer1", {"pipe_xrg_stream": ">"}, count=10)
        results = pipe.execute()

        assert results[0] is not None
        assert "pipe_xrg_stream" in results[0]
        assert len(results[0]["pipe_xrg_stream"]) == 1


class TestPipelineNoSpuriousWarnings:
    def test_default_pipeline_no_warnings(self, cache: RespCache):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            pipe = cache.pipeline()
            pipe.set("nowarn_key", "val")
            pipe.execute()


class TestAsyncPipeline:
    """End-to-end checks for the async pipeline factory and wrapper."""

    @pytest.mark.asyncio
    async def test_apipeline_returns_async_pipeline(self, cache: RespCache):
        pipe = await cache.apipeline()
        assert isinstance(pipe, AsyncPipeline)

    @pytest.mark.asyncio
    async def test_apipeline_basic_set_get(self, cache: RespCache):
        pipe = await cache.apipeline()
        pipe.set("apipe_key", "hello")
        pipe.get("apipe_key")
        results = await pipe.execute()
        assert results[0] is True
        assert results[1] == "hello"

    @pytest.mark.asyncio
    async def test_apipeline_chaining(self, cache: RespCache):
        pipe = await cache.apipeline()
        result = pipe.set("apipe_chain1", "a").set("apipe_chain2", "b").get("apipe_chain1")
        assert result is pipe
        results = await pipe.execute()
        assert results == [True, True, "a"]

    @pytest.mark.asyncio
    async def test_apipeline_empty_execute(self, cache: RespCache):
        pipe = await cache.apipeline()
        results = await pipe.execute()
        assert results == []

    @pytest.mark.asyncio
    async def test_apipeline_async_context_manager(self, cache: RespCache):
        async with await cache.apipeline() as pipe:
            pipe.set("apipe_ctx", "v")
            results = await pipe.execute()
        assert results == [True]

    @pytest.mark.asyncio
    async def test_apipeline_sync_with_raises_type_error_on_enter(self, cache: RespCache):
        pipe = await cache.apipeline()
        with pytest.raises(TypeError, match="async with"):
            pipe.__enter__()

    @pytest.mark.asyncio
    async def test_apipeline_failed_execute_does_not_leak_decoders(self, cache: RespCache):
        pipe = await cache.apipeline()
        pipe.set("adecoder_leak", "v1")
        real_adapter = pipe._pipeline_adapter

        class FailingAdapter:
            async def execute(self) -> list:
                # The driver pipeline discards its queue when execute fails.
                await real_adapter.reset()
                msg = "simulated execute failure"
                raise RuntimeError(msg)

        pipe._pipeline_adapter = FailingAdapter()
        with pytest.raises(RuntimeError, match="simulated execute failure"):
            await pipe.execute()

        pipe._pipeline_adapter = real_adapter
        pipe.set("adecoder_leak_after", "ok")
        assert await pipe.execute() == [True]
        assert await cache.aget("adecoder_leak_after") == "ok"


class TestPipelineErrorRecovery:
    """The wrapper stays usable after a failed execute()."""

    def test_pipeline_failed_execute_does_not_leak_decoders(self, cache: RespCache):
        pipe = cache.pipeline()
        pipe.set("decoder_leak", "v1")
        real_adapter = pipe._pipeline_adapter

        class FailingAdapter:
            def execute(self) -> list:
                # The driver pipeline discards its queue when execute fails.
                real_adapter.reset()
                msg = "simulated execute failure"
                raise RuntimeError(msg)

        pipe._pipeline_adapter = FailingAdapter()
        with pytest.raises(RuntimeError, match="simulated execute failure"):
            pipe.execute()

        pipe._pipeline_adapter = real_adapter
        pipe.set("decoder_leak_after", "ok")
        assert pipe.execute() == [True]
        assert cache.get("decoder_leak_after") == "ok"


class TestPipelineSetTimeoutSemantics:
    """``pipe.set()`` must resolve timeouts exactly like ``cache.set()``."""

    def test_default_timeout_sentinel_applies_backend_timeout(self, cache: RespCache, monkeypatch):
        monkeypatch.setattr(cache, "default_timeout", 123)

        pipe = cache.pipeline()
        pipe.set("pipe_default_to", "value")
        pipe.ttl("pipe_default_to")
        results = pipe.execute()

        assert results[0] is True
        assert 0 < results[1] <= 123

    def test_explicit_none_timeout_persists(self, cache: RespCache, monkeypatch):
        monkeypatch.setattr(cache, "default_timeout", 123)

        pipe = cache.pipeline()
        pipe.set("pipe_none_to", "value", timeout=None)
        pipe.ttl("pipe_none_to")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is None

    def test_negative_timeout_deletes_instead_of_erroring(self, cache: RespCache):
        cache.set("pipe_neg_to", "old")

        pipe = cache.pipeline()
        pipe.set("pipe_neg_to", "value", -1)
        pipe.exists("pipe_neg_to")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is False

    def test_float_timeout_is_truncated(self, cache: RespCache):
        pipe = cache.pipeline()
        pipe.set("pipe_float_trunc", "value", 0.5)
        pipe.exists("pipe_float_trunc")
        pipe.set("pipe_float_keep", "value", 100.9)
        pipe.ttl("pipe_float_keep")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is False
        assert results[2] is True
        assert 0 < results[3] <= 100

    def test_zero_timeout_reports_success_for_absent_key(self, cache: RespCache):
        pipe = cache.pipeline()
        pipe.set("pipe_zero_absent", "value", 0)
        pipe.exists("pipe_zero_absent")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] is False

    def test_matches_cache_set_for_the_same_timeouts(self, cache: RespCache, monkeypatch):
        monkeypatch.setattr(cache, "default_timeout", 123)

        for timeout, suffix in ((-1, "neg"), (0, "zero"), (0.5, "frac")):
            cache.set(f"pipe_parity_direct_{suffix}", "value", timeout)
            pipe = cache.pipeline()
            pipe.set(f"pipe_parity_pipe_{suffix}", "value", timeout)
            pipe.execute()
            assert cache.has_key(f"pipe_parity_pipe_{suffix}") == cache.has_key(f"pipe_parity_direct_{suffix}")

        cache.set("pipe_parity_direct_default", "value")
        pipe = cache.pipeline()
        pipe.set("pipe_parity_pipe_default", "value")
        pipe.execute()
        assert cache.ttl("pipe_parity_pipe_default") == cache.ttl("pipe_parity_direct_default")


class TestPipelineTtlNormalization:
    """``ttl``/``pttl``/``expiretime`` report "no expiry" as None, like the cache does."""

    def test_persistent_key_reports_none(self, cache: RespCache):
        cache.set("pipe_ttl_persist", "value", timeout=None)

        pipe = cache.pipeline()
        pipe.ttl("pipe_ttl_persist")
        pipe.pttl("pipe_ttl_persist")
        pipe.expiretime("pipe_ttl_persist")
        results = pipe.execute()

        assert results == [None, None, None]
        assert cache.ttl("pipe_ttl_persist") is None

    def test_missing_key_keeps_minus_two(self, cache: RespCache):
        pipe = cache.pipeline()
        pipe.ttl("pipe_ttl_missing")
        pipe.pttl("pipe_ttl_missing")
        pipe.expiretime("pipe_ttl_missing")
        results = pipe.execute()

        assert results == [-2, -2, -2]


class TestPipelineRenameResult:
    """RENAME reports a driver-independent bool."""

    def test_rename_returns_true(self, cache: RespCache, client_class: str, sentinel_mode: str | bool):
        if client_class == "cluster" and not sentinel_mode:
            pytest.skip("rename blocked in cluster pipeline mode")

        cache.set("{piperen}src", "value")

        pipe = cache.pipeline()
        pipe.rename("{piperen}src", "{piperen}dst")
        pipe.get("{piperen}dst")
        results = pipe.execute()

        assert results[0] is True
        assert results[1] == "value"


class TestPipelineStreamResultParity:
    """Stream replies decode to the same shapes as the non-pipelined path."""

    def test_xclaim_justid_returns_str_ids(self, cache: RespCache):
        cache.xadd("pipe_claim_jid", {"msg": "test"})
        cache.xgroup_create("pipe_claim_jid", "grp", entry_id="0")
        entries = cache.xreadgroup("grp", "c1", {"pipe_claim_jid": ">"})
        entry_id = entries["pipe_claim_jid"][0][0]

        pipe = cache.pipeline()
        pipe.xclaim("pipe_claim_jid", "grp", "c2", 0, [entry_id], justid=True)
        results = pipe.execute()

        assert results[0] == [entry_id]
        assert all(isinstance(claimed, str) for claimed in results[0])

    def test_xpending_rejects_range_and_filters_without_count(self, cache: RespCache):
        cache.xadd("pipe_pend_guard", {"msg": "test"})
        cache.xgroup_create("pipe_pend_guard", "grp", entry_id="0")

        pipe = cache.pipeline()
        for kwargs in ({"idle": 1000}, {"consumer": "c1"}, {"start": "-"}, {"end": "+"}):
            with pytest.raises(ValueError, match="requires count"):
                pipe.xpending("pipe_pend_guard", "grp", **kwargs)

    def test_xpending_takes_count_without_a_range(self, cache: RespCache):
        cache.xadd("pipe_pend_count", {"msg": "test"})
        cache.xgroup_create("pipe_pend_count", "grp", entry_id="0")
        cache.xreadgroup("grp", "c1", {"pipe_pend_count": ">"})

        pipe = cache.pipeline()
        pipe.xpending("pipe_pend_count", "grp", count=10)
        results = pipe.execute()

        assert isinstance(results[0], list)
        assert len(results[0]) == 1


class TestPipelineSetImmediateExpiryWithFlags:
    """A non-positive timeout must honour nx/xx the way ``set_with_flags`` does."""

    @pytest.mark.parametrize("flag", ["nx", "xx"])
    @pytest.mark.parametrize("preexisting", [True, False], ids=["existing", "absent"])
    def test_matches_cache_set(self, cache: RespCache, flag: str, preexisting: bool):
        state = "existing" if preexisting else "absent"
        direct = f"pipe_zeroflag_direct_{flag}_{state}"
        piped = f"pipe_zeroflag_pipe_{flag}_{state}"
        for key in (direct, piped):
            cache.delete(key)
            if preexisting:
                cache.set(key, "original")

        expected = cache.set(direct, "new", 0, **{flag: True})

        pipe = cache.pipeline()
        pipe.set(piped, "new", 0, **{flag: True})

        assert pipe.execute() == [expected]
        assert cache.has_key(piped) == cache.has_key(direct)

    def test_nx_leaves_an_existing_value_untouched(self, cache: RespCache):
        cache.set("pipe_nx_zero_keep", "original")

        pipe = cache.pipeline()
        pipe.set("pipe_nx_zero_keep", "new", 0, nx=True)
        pipe.get("pipe_nx_zero_keep")

        assert pipe.execute() == [False, "original"]

    def test_xx_deletes_only_a_key_that_exists(self, cache: RespCache):
        cache.set("pipe_xx_zero_hit", "original")
        cache.delete("pipe_xx_zero_miss")

        pipe = cache.pipeline()
        pipe.set("pipe_xx_zero_hit", "new", 0, xx=True)
        pipe.exists("pipe_xx_zero_hit")
        pipe.set("pipe_xx_zero_miss", "new", 0, xx=True)

        assert pipe.execute() == [True, False, False]


class TestPipelineStampedeExpireFamily:
    """``pipe.expire()`` and friends buffer the timeout the way ``cache.expire()`` does."""

    def test_expire_keeps_the_value_readable(self, stampede_cache: RespCache):
        stampede_cache.set("sp_pipe_exp", "val", timeout=300)

        pipe = stampede_cache.pipeline()
        pipe.expire("sp_pipe_exp", 30)

        assert pipe.execute() == [True]
        assert stampede_cache.get("sp_pipe_exp") == "val"
        assert stampede_cache.ttl("sp_pipe_exp") == pytest.approx(30, abs=2)

    def test_pexpire_keeps_the_value_readable(self, stampede_cache: RespCache):
        stampede_cache.set("sp_pipe_pexp", "val", timeout=300)

        pipe = stampede_cache.pipeline()
        pipe.pexpire("sp_pipe_pexp", 30_000)

        assert pipe.execute() == [True]
        assert stampede_cache.get("sp_pipe_pexp") == "val"

    def test_expireat_keeps_the_value_readable(self, stampede_cache: RespCache):
        stampede_cache.set("sp_pipe_expat", "val", timeout=300)
        when = int(time.time()) + 30

        pipe = stampede_cache.pipeline()
        pipe.expireat("sp_pipe_expat", when)

        assert pipe.execute() == [True]
        assert stampede_cache.get("sp_pipe_expat") == "val"
        assert stampede_cache.expiretime("sp_pipe_expat") == pytest.approx(when, abs=2)

    def test_pexpireat_keeps_the_value_readable(self, stampede_cache: RespCache):
        stampede_cache.set("sp_pipe_pexpat", "val", timeout=300)

        pipe = stampede_cache.pipeline()
        pipe.pexpireat("sp_pipe_pexpat", int(time.time() * 1000) + 30_000)

        assert pipe.execute() == [True]
        assert stampede_cache.get("sp_pipe_pexpat") == "val"

    def test_expire_opts_out_of_the_buffer(self, stampede_cache: RespCache):
        stampede_cache.set("sp_pipe_exp_raw", "val", timeout=300)

        pipe = stampede_cache.pipeline()
        pipe.expire("sp_pipe_exp_raw", 30, stampede_prevention=False)
        pipe.execute()

        assert stampede_cache.ttl("sp_pipe_exp_raw", stampede_prevention=False) == pytest.approx(30, abs=2)


class TestPipelineStampedeTtl:
    """``pipe.ttl()`` reports the logical TTL ``cache.ttl()`` reports."""

    def test_ttl_strips_the_buffer(self, stampede_cache: RespCache):
        stampede_cache.set("sp_pipe_ttl", "val", timeout=300)

        pipe = stampede_cache.pipeline()
        pipe.ttl("sp_pipe_ttl")
        pipe.pttl("sp_pipe_ttl")
        pipe.expiretime("sp_pipe_ttl")
        pipe.ttl("sp_pipe_ttl", stampede_prevention=False)
        results = pipe.execute()

        assert results[0] == pytest.approx(stampede_cache.ttl("sp_pipe_ttl"), abs=2)
        assert results[0] == pytest.approx(300, abs=2)
        assert results[1] == pytest.approx(300_000, abs=2000)
        assert results[2] == pytest.approx(stampede_cache.expiretime("sp_pipe_ttl"), abs=2)
        assert results[3] == pytest.approx(360, abs=2)

    def test_sentinels_pass_through(self, stampede_cache: RespCache):
        stampede_cache.set("sp_pipe_ttl_persist", "val", timeout=None)
        stampede_cache.delete("sp_pipe_ttl_missing")

        pipe = stampede_cache.pipeline()
        pipe.ttl("sp_pipe_ttl_persist")
        pipe.ttl("sp_pipe_ttl_missing")

        assert pipe.execute() == [None, -2]


class TestPipelineTypeParity:
    """``pipe.type()`` reports the KeyType surface ``cache.type()`` reports."""

    def test_type_matches_the_cache(self, cache: RespCache):
        cache.set("pipe_type_str", "value")
        cache.rpush("pipe_type_list", "a")
        cache.delete("pipe_type_missing")

        pipe = cache.pipeline()
        pipe.type("pipe_type_str")
        pipe.type("pipe_type_list")
        pipe.type("pipe_type_missing")
        results = pipe.execute()

        assert results == [KeyType.STRING, KeyType.LIST, None]
        assert results[0] == cache.type("pipe_type_str")
        assert results[2] == cache.type("pipe_type_missing")

    def test_unmodelled_type_becomes_unknown(self, cache: RespCache):
        assert cache.pipeline()._decode_type(b"ReJSON-RL") is KeyType.UNKNOWN


class TestPipelineZsetSignatureParity:
    """``pipe.zadd``/``pipe.zrange`` take what the cache takes, and nothing more."""

    def test_zadd_has_no_incr_flag(self, cache: RespCache):
        pipe = cache.pipeline()
        with pytest.raises(TypeError, match="incr"):
            pipe.zadd("pipe_zadd_incr", {"a": 1.0}, incr=True)

    def test_zrange_has_no_desc_flag(self, cache: RespCache):
        pipe = cache.pipeline()
        with pytest.raises(TypeError, match="desc"):
            pipe.zrange("pipe_zrange_desc", 0, -1, desc=True)
