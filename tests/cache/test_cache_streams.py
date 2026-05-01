"""Tests for stream operations."""

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django_cachex.cache import KeyValueCache


class TestStreamBasicOps:
    """Tests for xadd, xlen, xrange, xrevrange, xdel."""

    def test_xadd_and_xlen(self, cache: KeyValueCache):
        entry_id = cache.xadd("stream1", {"name": "Alice", "score": 100})
        assert isinstance(entry_id, str)
        assert "-" in entry_id
        assert cache.xlen("stream1") == 1

        cache.xadd("stream1", {"name": "Bob", "score": 200})
        assert cache.xlen("stream1") == 2

    def test_xadd_with_maxlen(self, cache: KeyValueCache):
        for i in range(10):
            cache.xadd("stream_maxlen", {"i": i}, maxlen=5, approximate=False)
        assert cache.xlen("stream_maxlen") == 5

    def test_xrange(self, cache: KeyValueCache):
        cache.xadd("stream_range", {"a": 1})
        cache.xadd("stream_range", {"b": 2})
        cache.xadd("stream_range", {"c": 3})

        entries = cache.xrange("stream_range")
        assert len(entries) == 3
        # Each entry is (entry_id, fields_dict)
        assert entries[0][1]["a"] == 1
        assert entries[1][1]["b"] == 2
        assert entries[2][1]["c"] == 3

    def test_xrange_with_count(self, cache: KeyValueCache):
        for i in range(5):
            cache.xadd("stream_range_cnt", {"i": i})

        entries = cache.xrange("stream_range_cnt", count=2)
        assert len(entries) == 2

    def test_xrevrange(self, cache: KeyValueCache):
        cache.xadd("stream_revrange", {"a": 1})
        cache.xadd("stream_revrange", {"b": 2})
        cache.xadd("stream_revrange", {"c": 3})

        entries = cache.xrevrange("stream_revrange")
        assert len(entries) == 3
        assert entries[0][1]["c"] == 3
        assert entries[2][1]["a"] == 1

    def test_xdel(self, cache: KeyValueCache):
        eid = cache.xadd("stream_del", {"data": "test"})
        assert cache.xlen("stream_del") == 1
        deleted = cache.xdel("stream_del", eid)
        assert deleted == 1

    def test_xtrim(self, cache: KeyValueCache):
        for i in range(10):
            cache.xadd("stream_trim", {"i": i})
        trimmed = cache.xtrim("stream_trim", maxlen=3, approximate=False)
        assert trimmed == 7
        assert cache.xlen("stream_trim") == 3


class TestStreamRead:
    """Tests for xread."""

    def test_xread(self, cache: KeyValueCache):
        cache.xadd("stream_read", {"a": 1})
        cache.xadd("stream_read", {"b": 2})

        result = cache.xread({"stream_read": "0-0"}, count=10)
        assert result is not None
        # result keys must be the original user key, not the prefixed Redis key
        assert "stream_read" in result
        assert len(result["stream_read"]) == 2


class TestStreamInfo:
    """Tests for xinfo_stream, xinfo_groups."""

    def test_xinfo_stream(self, cache: KeyValueCache):
        cache.xadd("stream_info", {"data": "test"})

        info = cache.xinfo_stream("stream_info")
        assert isinstance(info, dict)

    def test_xinfo_groups_empty(self, cache: KeyValueCache):
        cache.xadd("stream_info_groups", {"data": "test"})

        groups = cache.xinfo_groups("stream_info_groups")
        assert groups == []


class TestStreamConsumerGroups:
    """Tests for xgroup_create, xgroup_destroy, xreadgroup, xack, xpending."""

    def test_xgroup_create_and_destroy(self, cache: KeyValueCache):
        cache.xadd("stream_grp", {"data": "test"})

        result = cache.xgroup_create("stream_grp", "mygroup", entry_id="0")
        assert result is True

        groups = cache.xinfo_groups("stream_grp")
        assert len(groups) == 1

        destroyed = cache.xgroup_destroy("stream_grp", "mygroup")
        assert destroyed == 1

    def test_xgroup_create_mkstream(self, cache: KeyValueCache):
        result = cache.xgroup_create("stream_grp_mk", "mygroup", entry_id="$", mkstream=True)
        assert result is True
        assert cache.xlen("stream_grp_mk") == 0

    def test_xreadgroup(self, cache: KeyValueCache):
        cache.xadd("stream_rg", {"msg": "hello"})
        cache.xadd("stream_rg", {"msg": "world"})
        cache.xgroup_create("stream_rg", "readers", entry_id="0")

        result = cache.xreadgroup("readers", "consumer1", {"stream_rg": ">"}, count=10)
        assert result is not None
        # result keys must be the original user key, not the prefixed Redis key
        assert "stream_rg" in result
        assert len(result["stream_rg"]) == 2

    def test_xack(self, cache: KeyValueCache):
        eid = cache.xadd("stream_ack", {"msg": "test"})
        cache.xgroup_create("stream_ack", "ack_grp", entry_id="0")
        cache.xreadgroup("ack_grp", "consumer1", {"stream_ack": ">"})

        acked = cache.xack("stream_ack", "ack_grp", eid)
        assert acked == 1

    def test_xpending_summary(self, cache: KeyValueCache):
        cache.xadd("stream_pend", {"msg": "test"})
        cache.xgroup_create("stream_pend", "pend_grp", entry_id="0")
        cache.xreadgroup("pend_grp", "consumer1", {"stream_pend": ">"})

        pending = cache.xpending("stream_pend", "pend_grp")
        assert isinstance(pending, dict)

    def test_xgroup_setid(self, cache: KeyValueCache):
        cache.xadd("stream_setid", {"data": "test"})
        cache.xgroup_create("stream_setid", "setid_grp", entry_id="0")
        result = cache.xgroup_setid("stream_setid", "setid_grp", "$")
        assert result is True

    def test_xgroup_delconsumer(self, cache: KeyValueCache):
        cache.xadd("stream_delc", {"data": "test"})
        cache.xgroup_create("stream_delc", "delc_grp", entry_id="0")
        cache.xreadgroup("delc_grp", "consumer1", {"stream_delc": ">"})

        result = cache.xgroup_delconsumer("stream_delc", "delc_grp", "consumer1")
        assert isinstance(result, int)

    def test_xinfo_consumers(self, cache: KeyValueCache):
        cache.xadd("stream_infoc", {"data": "test"})
        cache.xgroup_create("stream_infoc", "infoc_grp", entry_id="0")
        cache.xreadgroup("infoc_grp", "consumer1", {"stream_infoc": ">"})

        consumers = cache.xinfo_consumers("stream_infoc", "infoc_grp")
        assert len(consumers) == 1

    def test_xclaim(self, cache: KeyValueCache):
        eid = cache.xadd("stream_claim", {"msg": "claimable"})
        cache.xgroup_create("stream_claim", "claim_grp", entry_id="0")
        cache.xreadgroup("claim_grp", "consumer1", {"stream_claim": ">"})

        # Small delay so the message has some idle time
        time.sleep(0.01)
        claimed = cache.xclaim("stream_claim", "claim_grp", "consumer2", 0, [eid])
        assert len(claimed) == 1
        assert claimed[0][0] == eid

    def test_xclaim_justid(self, cache: KeyValueCache):
        eid = cache.xadd("stream_claim_jid", {"msg": "claimable"})
        cache.xgroup_create("stream_claim_jid", "claim_jid_grp", entry_id="0")
        cache.xreadgroup("claim_jid_grp", "consumer1", {"stream_claim_jid": ">"})

        time.sleep(0.01)
        claimed = cache.xclaim("stream_claim_jid", "claim_jid_grp", "consumer2", 0, [eid], justid=True)
        assert eid in claimed

    def test_xautoclaim(self, cache: KeyValueCache):
        cache.xadd("stream_autoclaim", {"msg": "auto"})
        cache.xgroup_create("stream_autoclaim", "auto_grp", entry_id="0")
        cache.xreadgroup("auto_grp", "consumer1", {"stream_autoclaim": ">"})

        time.sleep(0.01)
        next_id, claimed, _deleted = cache.xautoclaim("stream_autoclaim", "auto_grp", "consumer2", 0)
        assert isinstance(next_id, str)
        assert len(claimed) >= 1

    def test_xautoclaim_justid(self, cache: KeyValueCache):
        eid = cache.xadd("stream_autoclaim_jid", {"msg": "auto"})
        cache.xgroup_create("stream_autoclaim_jid", "ac_jid_grp", entry_id="0")
        cache.xreadgroup("ac_jid_grp", "consumer1", {"stream_autoclaim_jid": ">"})

        time.sleep(0.01)
        _next_id, claimed, _deleted = cache.xautoclaim(
            "stream_autoclaim_jid",
            "ac_jid_grp",
            "consumer2",
            0,
            justid=True,
        )
        assert isinstance(claimed, list)
        assert eid in claimed

    def test_xpending_range(self, cache: KeyValueCache):
        cache.xadd("stream_pend_range", {"msg": "test"})
        cache.xgroup_create("stream_pend_range", "pr_grp", entry_id="0")
        cache.xreadgroup("pr_grp", "consumer1", {"stream_pend_range": ">"})

        result = cache.xpending("stream_pend_range", "pr_grp", start="-", end="+", count=10)
        assert isinstance(result, list)
        assert len(result) == 1
