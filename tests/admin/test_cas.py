"""Tests for admin CAS (compare-and-swap) operations."""

from typing import TYPE_CHECKING

import pytest

from django_cachex.admin.cas import (
    cas_rename_hash_field,
    cas_update_hash_field,
    cas_update_list_element,
    cas_update_string,
    cas_update_zset_score,
    get_hash_field_sha1s_for,
    get_list_sha1s_range,
    get_string_sha1,
    supports_cas,
)

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


class TestStringSHA1:
    def test_get_sha1(self, test_cache: RespCache):
        test_cache.set("str_key", "hello")
        sha1 = get_string_sha1(test_cache, "str_key")
        assert sha1 is not None
        assert len(sha1) == 40

    def test_get_sha1_nonexistent(self, test_cache: RespCache):
        sha1 = get_string_sha1(test_cache, "nonexistent")
        assert sha1 is None

    def test_sha1_changes_with_value(self, test_cache: RespCache):
        test_cache.set("str_key", "value1")
        sha1_a = get_string_sha1(test_cache, "str_key")

        test_cache.set("str_key", "value2")
        sha1_b = get_string_sha1(test_cache, "str_key")

        assert sha1_a != sha1_b

    def test_sha1_stable_for_same_value(self, test_cache: RespCache):
        test_cache.set("str_key", "same")
        sha1_a = get_string_sha1(test_cache, "str_key")
        sha1_b = get_string_sha1(test_cache, "str_key")
        assert sha1_a == sha1_b


class TestSupportsCAS:
    def test_resp_cache_supports_cas(self, test_cache: RespCache):
        assert supports_cas(test_cache) is True

    def test_locmem_does_not_support_cas(self, test_cache: RespCache):
        del test_cache
        from django.core.cache import caches

        assert supports_cas(caches["local"]) is False


class TestCASStringUpdate:
    def test_success(self, test_cache: RespCache):
        test_cache.set("cas_str", "original")
        sha1 = get_string_sha1(test_cache, "cas_str")

        result = cas_update_string(test_cache, "cas_str", sha1, "updated")
        assert result == 1
        assert test_cache.get("cas_str") == "updated"

    def test_conflict(self, test_cache: RespCache):
        test_cache.set("cas_str", "original")
        sha1 = get_string_sha1(test_cache, "cas_str")

        # Another process modifies the value
        test_cache.set("cas_str", "modified_by_other")

        result = cas_update_string(test_cache, "cas_str", sha1, "my_update")
        assert result == 0
        assert test_cache.get("cas_str") == "modified_by_other"

    def test_key_gone(self, test_cache: RespCache):
        test_cache.set("cas_str", "original")
        sha1 = get_string_sha1(test_cache, "cas_str")

        test_cache.delete("cas_str")

        result = cas_update_string(test_cache, "cas_str", sha1, "updated")
        assert result == -1

    def test_complex_value(self, test_cache: RespCache):
        original = {"key": "value", "nested": [1, 2, 3]}
        test_cache.set("cas_complex", original)
        sha1 = get_string_sha1(test_cache, "cas_complex")

        new_value = {"key": "updated", "nested": [4, 5, 6]}
        result = cas_update_string(test_cache, "cas_complex", sha1, new_value)
        assert result == 1
        assert test_cache.get("cas_complex") == new_value


class TestCASHashFieldUpdate:
    def test_success(self, test_cache: RespCache):
        test_cache.hset("cas_hash", "field1", "original")
        sha1s = get_hash_field_sha1s_for(test_cache, "cas_hash", test_cache.hkeys("cas_hash"))

        result = cas_update_hash_field(test_cache, "cas_hash", "field1", sha1s["field1"], "updated")
        assert result == 1
        assert test_cache.hget("cas_hash", "field1") == "updated"

    def test_conflict(self, test_cache: RespCache):
        test_cache.hset("cas_hash", "field1", "original")
        sha1s = get_hash_field_sha1s_for(test_cache, "cas_hash", test_cache.hkeys("cas_hash"))

        # Another process modifies the field
        test_cache.hset("cas_hash", "field1", "modified_by_other")

        result = cas_update_hash_field(test_cache, "cas_hash", "field1", sha1s["field1"], "my_update")
        assert result == 0
        assert test_cache.hget("cas_hash", "field1") == "modified_by_other"

    def test_field_gone(self, test_cache: RespCache):
        test_cache.hset("cas_hash", "field1", "original")
        sha1s = get_hash_field_sha1s_for(test_cache, "cas_hash", test_cache.hkeys("cas_hash"))

        test_cache.hdel("cas_hash", "field1")

        result = cas_update_hash_field(test_cache, "cas_hash", "field1", sha1s["field1"], "updated")
        assert result == -1

    def test_other_fields_unaffected(self, test_cache: RespCache):
        test_cache.hset("cas_hash", "f1", "v1")
        test_cache.hset("cas_hash", "f2", "v2")
        sha1s = get_hash_field_sha1s_for(test_cache, "cas_hash", test_cache.hkeys("cas_hash"))

        result = cas_update_hash_field(test_cache, "cas_hash", "f1", sha1s["f1"], "new_v1")
        assert result == 1
        assert test_cache.hget("cas_hash", "f2") == "v2"


class TestCASHashFieldRename:
    """Redis has no HRENAME, so the script does HSET plus HDEL atomically."""

    def test_success(self, test_cache: RespCache):
        test_cache.hset("cas_rename", "old", "original")
        sha1s = get_hash_field_sha1s_for(test_cache, "cas_rename", test_cache.hkeys("cas_rename"))

        result = cas_rename_hash_field(test_cache, "cas_rename", "old", "new", sha1s["old"], "updated")
        assert result == 1
        assert test_cache.hgetall("cas_rename") == {"new": "updated"}

    def test_conflict(self, test_cache: RespCache):
        test_cache.hset("cas_rename", "old", "original")
        sha1s = get_hash_field_sha1s_for(test_cache, "cas_rename", test_cache.hkeys("cas_rename"))

        test_cache.hset("cas_rename", "old", "modified_by_other")

        result = cas_rename_hash_field(test_cache, "cas_rename", "old", "new", sha1s["old"], "mine")
        assert result == 0
        assert test_cache.hgetall("cas_rename") == {"old": "modified_by_other"}

    def test_field_gone(self, test_cache: RespCache):
        test_cache.hset("cas_rename", "old", "original")
        sha1s = get_hash_field_sha1s_for(test_cache, "cas_rename", test_cache.hkeys("cas_rename"))

        test_cache.hdel("cas_rename", "old")

        result = cas_rename_hash_field(test_cache, "cas_rename", "old", "new", sha1s["old"], "updated")
        assert result == -1

    def test_target_name_taken(self, test_cache: RespCache):
        test_cache.hset("cas_rename", "old", "original")
        test_cache.hset("cas_rename", "taken", "keep me")
        sha1s = get_hash_field_sha1s_for(test_cache, "cas_rename", test_cache.hkeys("cas_rename"))

        result = cas_rename_hash_field(test_cache, "cas_rename", "old", "taken", sha1s["old"], "updated")
        assert result == -2
        assert test_cache.hgetall("cas_rename") == {"old": "original", "taken": "keep me"}

    def test_other_fields_unaffected(self, test_cache: RespCache):
        test_cache.hset("cas_rename", "old", "v1")
        test_cache.hset("cas_rename", "f2", "v2")
        sha1s = get_hash_field_sha1s_for(test_cache, "cas_rename", test_cache.hkeys("cas_rename"))

        result = cas_rename_hash_field(test_cache, "cas_rename", "old", "new", sha1s["old"], "v1")
        assert result == 1
        assert test_cache.hgetall("cas_rename") == {"new": "v1", "f2": "v2"}


class TestCASZsetScoreUpdate:
    def test_success(self, test_cache: RespCache):
        test_cache.zadd("cas_zset", {"member1": 10.0})
        score = test_cache.zscore("cas_zset", "member1")

        result = cas_update_zset_score(test_cache, "cas_zset", "member1", str(score), 20.0)
        assert result == 1
        assert test_cache.zscore("cas_zset", "member1") == 20.0

    def test_conflict(self, test_cache: RespCache):
        test_cache.zadd("cas_zset", {"member1": 10.0})
        score = test_cache.zscore("cas_zset", "member1")

        # Another process changes the score
        test_cache.zadd("cas_zset", {"member1": 99.0})

        result = cas_update_zset_score(test_cache, "cas_zset", "member1", str(score), 20.0)
        assert result == 0
        assert test_cache.zscore("cas_zset", "member1") == 99.0

    def test_member_gone(self, test_cache: RespCache):
        test_cache.zadd("cas_zset", {"member1": 10.0})
        score = test_cache.zscore("cas_zset", "member1")

        test_cache.zrem("cas_zset", "member1")

        result = cas_update_zset_score(test_cache, "cas_zset", "member1", str(score), 20.0)
        assert result == -1


class TestCASListElementUpdate:
    def test_success(self, test_cache: RespCache):
        test_cache.rpush("cas_list", "a", "b", "c")
        sha1s = get_list_sha1s_range(test_cache, "cas_list", 0, -1)

        result = cas_update_list_element(test_cache, "cas_list", 1, sha1s[1], "B")
        assert result == 1
        assert test_cache.lindex("cas_list", 1) == "B"

    def test_conflict_value_changed(self, test_cache: RespCache):
        test_cache.rpush("cas_list", "a", "b", "c")
        sha1s = get_list_sha1s_range(test_cache, "cas_list", 0, -1)

        # Another process modifies element at index 1
        test_cache.lset("cas_list", 1, "modified")

        result = cas_update_list_element(test_cache, "cas_list", 1, sha1s[1], "my_update")
        assert result == 0
        assert test_cache.lindex("cas_list", 1) == "modified"

    def test_conflict_index_shifted(self, test_cache: RespCache):
        test_cache.rpush("cas_list", "a", "b", "c")
        sha1s = get_list_sha1s_range(test_cache, "cas_list", 0, -1)

        # Another process inserts at head, shifting indices
        test_cache.lpush("cas_list", "z")

        # Index 1 now has "a" (was "b"), SHA1 won't match
        result = cas_update_list_element(test_cache, "cas_list", 1, sha1s[1], "my_update")
        assert result == 0

    @pytest.mark.parametrize("count", [0, 1])
    def test_index_gone(self, test_cache: RespCache, count: int):
        test_cache.rpush("cas_list", *["x"] * (count + 1))
        sha1s = get_list_sha1s_range(test_cache, "cas_list", 0, -1)

        test_cache.delete("cas_list")

        result = cas_update_list_element(test_cache, "cas_list", count, sha1s[count], "updated")
        assert result == -1

    def test_first_and_last_elements(self, test_cache: RespCache):
        test_cache.rpush("cas_list", "first", "middle", "last")
        sha1s = get_list_sha1s_range(test_cache, "cas_list", 0, -1)

        result = cas_update_list_element(test_cache, "cas_list", 0, sha1s[0], "FIRST")
        assert result == 1

        result = cas_update_list_element(test_cache, "cas_list", 2, sha1s[2], "LAST")
        assert result == 1

        assert test_cache.lrange("cas_list", 0, -1) == ["FIRST", "middle", "LAST"]


class TestListSHA1sRange:
    def test_range_matches_full(self, test_cache: RespCache):
        test_cache.rpush("range_list", "a", "b", "c", "d", "e")
        full = get_list_sha1s_range(test_cache, "range_list", 0, -1)
        subset = get_list_sha1s_range(test_cache, "range_list", 1, 3)
        assert subset == full[1:4]

    def test_range_first_page(self, test_cache: RespCache):
        test_cache.rpush("range_list", "a", "b", "c", "d", "e")
        sha1s = get_list_sha1s_range(test_cache, "range_list", 0, 2)
        assert len(sha1s) == 3
        assert all(len(s) == 40 for s in sha1s)

    def test_range_last_elements(self, test_cache: RespCache):
        test_cache.rpush("range_list", "a", "b", "c")
        sha1s = get_list_sha1s_range(test_cache, "range_list", 2, 2)
        assert len(sha1s) == 1

    def test_range_empty(self, test_cache: RespCache):
        sha1s = get_list_sha1s_range(test_cache, "nonexistent", 0, 99)
        assert sha1s == []

    def test_range_beyond_length(self, test_cache: RespCache):
        test_cache.rpush("range_list", "a", "b")
        sha1s = get_list_sha1s_range(test_cache, "range_list", 0, 99)
        assert len(sha1s) == 2


class TestHashFieldSHA1sFor:
    def test_specific_fields(self, test_cache: RespCache):
        test_cache.hset("hash_key", "f1", "v1")
        test_cache.hset("hash_key", "f2", "v2")
        test_cache.hset("hash_key", "f3", "v3")
        sha1s = get_hash_field_sha1s_for(test_cache, "hash_key", ["f1", "f3"])
        assert len(sha1s) == 2
        assert "f1" in sha1s
        assert "f3" in sha1s
        assert "f2" not in sha1s

    def test_sha1_changes_with_field_value(self, test_cache: RespCache):
        test_cache.hset("hash_key", "f1", "v1")
        sha1_a = get_hash_field_sha1s_for(test_cache, "hash_key", ["f1"])["f1"]

        test_cache.hset("hash_key", "f1", "v2")
        sha1_b = get_hash_field_sha1s_for(test_cache, "hash_key", ["f1"])["f1"]

        assert sha1_a != sha1_b

    def test_nonexistent_fields(self, test_cache: RespCache):
        test_cache.hset("hash_key", "f1", "v1")
        sha1s = get_hash_field_sha1s_for(test_cache, "hash_key", ["missing"])
        assert sha1s == {}

    def test_empty_fields_list(self, test_cache: RespCache):
        # Empty fields list short-circuits regardless of hash content; this
        # test exists to lock that in (NOT to test against a missing key.
        # See test_nonexistent_key for that).
        test_cache.hset("hash_key", "f1", "v1")
        sha1s = get_hash_field_sha1s_for(test_cache, "hash_key", [])
        assert sha1s == {}

    def test_nonexistent_key(self, test_cache: RespCache):
        sha1s = get_hash_field_sha1s_for(test_cache, "nonexistent", ["f1"])
        assert sha1s == {}
