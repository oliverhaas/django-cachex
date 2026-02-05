"""Tests for stream operations (sync and async)."""

import time

import pytest

from django_cachex.cache import KeyValueCache


@pytest.fixture
def mk(cache: KeyValueCache):
    """Create a prefixed key for direct client testing."""
    return lambda key, version=None: cache.make_and_validate_key(key, version=version)


# =============================================================================
# Sync stream tests
# =============================================================================


class TestStreamBasicOps:
    """Tests for xadd, xlen, xrange, xrevrange, xdel."""

    def test_xadd_and_xlen(self, cache: KeyValueCache, mk):
        key = mk("stream1")
        entry_id = cache._cache.xadd(key, {"name": "Alice", "score": 100})
        assert isinstance(entry_id, str)
        assert "-" in entry_id
        assert cache._cache.xlen(key) == 1

        cache._cache.xadd(key, {"name": "Bob", "score": 200})
        assert cache._cache.xlen(key) == 2

    def test_xadd_with_maxlen(self, cache: KeyValueCache, mk):
        key = mk("stream_maxlen")
        for i in range(10):
            cache._cache.xadd(key, {"i": i}, maxlen=5, approximate=False)
        assert cache._cache.xlen(key) == 5

    def test_xrange(self, cache: KeyValueCache, mk):
        key = mk("stream_range")
        cache._cache.xadd(key, {"a": 1})
        cache._cache.xadd(key, {"b": 2})
        cache._cache.xadd(key, {"c": 3})

        entries = cache._cache.xrange(key)
        assert len(entries) == 3
        # Each entry is (entry_id, fields_dict)
        assert entries[0][1]["a"] == 1
        assert entries[1][1]["b"] == 2
        assert entries[2][1]["c"] == 3

    def test_xrange_with_count(self, cache: KeyValueCache, mk):
        key = mk("stream_range_cnt")
        for i in range(5):
            cache._cache.xadd(key, {"i": i})

        entries = cache._cache.xrange(key, count=2)
        assert len(entries) == 2

    def test_xrevrange(self, cache: KeyValueCache, mk):
        key = mk("stream_revrange")
        cache._cache.xadd(key, {"a": 1})
        cache._cache.xadd(key, {"b": 2})
        cache._cache.xadd(key, {"c": 3})

        entries = cache._cache.xrevrange(key)
        assert len(entries) == 3
        assert entries[0][1]["c"] == 3
        assert entries[2][1]["a"] == 1

    def test_xdel(self, cache: KeyValueCache, mk):
        key = mk("stream_del")
        eid = cache._cache.xadd(key, {"data": "test"})
        assert cache._cache.xlen(key) == 1
        deleted = cache._cache.xdel(key, eid)
        assert deleted == 1

    def test_xtrim(self, cache: KeyValueCache, mk):
        key = mk("stream_trim")
        for i in range(10):
            cache._cache.xadd(key, {"i": i})
        trimmed = cache._cache.xtrim(key, maxlen=3, approximate=False)
        assert trimmed == 7
        assert cache._cache.xlen(key) == 3


class TestStreamRead:
    """Tests for xread."""

    def test_xread(self, cache: KeyValueCache, mk):
        key = mk("stream_read")
        cache._cache.xadd(key, {"a": 1})
        cache._cache.xadd(key, {"b": 2})

        result = cache._cache.xread({key: "0-0"}, count=10)
        assert result is not None
        # result is dict: {stream_key: [(entry_id, fields), ...]}
        stream_entries = list(result.values())[0]
        assert len(stream_entries) == 2


class TestStreamInfo:
    """Tests for xinfo_stream, xinfo_groups."""

    def test_xinfo_stream(self, cache: KeyValueCache, mk):
        key = mk("stream_info")
        cache._cache.xadd(key, {"data": "test"})

        info = cache._cache.xinfo_stream(key)
        assert isinstance(info, dict)

    def test_xinfo_groups_empty(self, cache: KeyValueCache, mk):
        key = mk("stream_info_groups")
        cache._cache.xadd(key, {"data": "test"})

        groups = cache._cache.xinfo_groups(key)
        assert groups == []


class TestStreamConsumerGroups:
    """Tests for xgroup_create, xgroup_destroy, xreadgroup, xack, xpending."""

    def test_xgroup_create_and_destroy(self, cache: KeyValueCache, mk):
        key = mk("stream_grp")
        cache._cache.xadd(key, {"data": "test"})

        result = cache._cache.xgroup_create(key, "mygroup", entry_id="0")
        assert result is True

        groups = cache._cache.xinfo_groups(key)
        assert len(groups) == 1

        destroyed = cache._cache.xgroup_destroy(key, "mygroup")
        assert destroyed == 1

    def test_xgroup_create_mkstream(self, cache: KeyValueCache, mk):
        key = mk("stream_grp_mk")
        result = cache._cache.xgroup_create(key, "mygroup", entry_id="$", mkstream=True)
        assert result is True
        assert cache._cache.xlen(key) == 0

    def test_xreadgroup(self, cache: KeyValueCache, mk):
        key = mk("stream_rg")
        cache._cache.xadd(key, {"msg": "hello"})
        cache._cache.xadd(key, {"msg": "world"})
        cache._cache.xgroup_create(key, "readers", entry_id="0")

        result = cache._cache.xreadgroup("readers", "consumer1", {key: ">"}, count=10)
        assert result is not None
        entries = list(result.values())[0]
        assert len(entries) == 2

    def test_xack(self, cache: KeyValueCache, mk):
        key = mk("stream_ack")
        eid = cache._cache.xadd(key, {"msg": "test"})
        cache._cache.xgroup_create(key, "ack_grp", entry_id="0")
        cache._cache.xreadgroup("ack_grp", "consumer1", {key: ">"})

        acked = cache._cache.xack(key, "ack_grp", eid)
        assert acked == 1

    def test_xpending_summary(self, cache: KeyValueCache, mk):
        key = mk("stream_pend")
        cache._cache.xadd(key, {"msg": "test"})
        cache._cache.xgroup_create(key, "pend_grp", entry_id="0")
        cache._cache.xreadgroup("pend_grp", "consumer1", {key: ">"})

        pending = cache._cache.xpending(key, "pend_grp")
        assert isinstance(pending, dict)

    def test_xgroup_setid(self, cache: KeyValueCache, mk):
        key = mk("stream_setid")
        cache._cache.xadd(key, {"data": "test"})
        cache._cache.xgroup_create(key, "setid_grp", entry_id="0")
        result = cache._cache.xgroup_setid(key, "setid_grp", "$")
        assert result is True

    def test_xgroup_delconsumer(self, cache: KeyValueCache, mk):
        key = mk("stream_delc")
        cache._cache.xadd(key, {"data": "test"})
        cache._cache.xgroup_create(key, "delc_grp", entry_id="0")
        cache._cache.xreadgroup("delc_grp", "consumer1", {key: ">"})

        result = cache._cache.xgroup_delconsumer(key, "delc_grp", "consumer1")
        assert isinstance(result, int)

    def test_xinfo_consumers(self, cache: KeyValueCache, mk):
        key = mk("stream_infoc")
        cache._cache.xadd(key, {"data": "test"})
        cache._cache.xgroup_create(key, "infoc_grp", entry_id="0")
        cache._cache.xreadgroup("infoc_grp", "consumer1", {key: ">"})

        consumers = cache._cache.xinfo_consumers(key, "infoc_grp")
        assert len(consumers) == 1

    def test_xclaim(self, cache: KeyValueCache, mk):
        key = mk("stream_claim")
        eid = cache._cache.xadd(key, {"msg": "claimable"})
        cache._cache.xgroup_create(key, "claim_grp", entry_id="0")
        cache._cache.xreadgroup("claim_grp", "consumer1", {key: ">"})

        # Small delay so the message has some idle time
        time.sleep(0.01)
        claimed = cache._cache.xclaim(key, "claim_grp", "consumer2", 0, [eid])
        assert len(claimed) == 1
        assert claimed[0][0] == eid

    def test_xclaim_justid(self, cache: KeyValueCache, mk):
        key = mk("stream_claim_jid")
        eid = cache._cache.xadd(key, {"msg": "claimable"})
        cache._cache.xgroup_create(key, "claim_jid_grp", entry_id="0")
        cache._cache.xreadgroup("claim_jid_grp", "consumer1", {key: ">"})

        time.sleep(0.01)
        claimed = cache._cache.xclaim(key, "claim_jid_grp", "consumer2", 0, [eid], justid=True)
        assert eid in claimed

    def test_xautoclaim(self, cache: KeyValueCache, mk):
        key = mk("stream_autoclaim")
        cache._cache.xadd(key, {"msg": "auto"})
        cache._cache.xgroup_create(key, "auto_grp", entry_id="0")
        cache._cache.xreadgroup("auto_grp", "consumer1", {key: ">"})

        time.sleep(0.01)
        next_id, claimed, _deleted = cache._cache.xautoclaim(key, "auto_grp", "consumer2", 0)
        assert isinstance(next_id, str)
        assert len(claimed) >= 1

    def test_xpending_range(self, cache: KeyValueCache, mk):
        key = mk("stream_pend_range")
        cache._cache.xadd(key, {"msg": "test"})
        cache._cache.xgroup_create(key, "pr_grp", entry_id="0")
        cache._cache.xreadgroup("pr_grp", "consumer1", {key: ">"})

        result = cache._cache.xpending(key, "pr_grp", start="-", end="+", count=10)
        assert isinstance(result, list)
        assert len(result) == 1


# =============================================================================
# Async stream tests
# =============================================================================


class TestAsyncStreamBasicOps:
    """Tests for axadd, axlen, axrange, axrevrange, axdel."""

    @pytest.mark.asyncio
    async def test_axadd_and_axlen(self, cache: KeyValueCache, mk):
        key = mk("astream1")
        entry_id = await cache._cache.axadd(key, {"name": "Alice", "score": 100})
        assert isinstance(entry_id, str)
        assert "-" in entry_id
        assert await cache._cache.axlen(key) == 1

        await cache._cache.axadd(key, {"name": "Bob", "score": 200})
        assert await cache._cache.axlen(key) == 2

    @pytest.mark.asyncio
    async def test_axadd_with_maxlen(self, cache: KeyValueCache, mk):
        key = mk("astream_maxlen")
        for i in range(10):
            await cache._cache.axadd(key, {"i": i}, maxlen=5, approximate=False)
        assert await cache._cache.axlen(key) == 5

    @pytest.mark.asyncio
    async def test_axrange(self, cache: KeyValueCache, mk):
        key = mk("astream_range")
        await cache._cache.axadd(key, {"a": 1})
        await cache._cache.axadd(key, {"b": 2})
        await cache._cache.axadd(key, {"c": 3})

        entries = await cache._cache.axrange(key)
        assert len(entries) == 3
        assert entries[0][1]["a"] == 1
        assert entries[1][1]["b"] == 2
        assert entries[2][1]["c"] == 3

    @pytest.mark.asyncio
    async def test_axrange_with_count(self, cache: KeyValueCache, mk):
        key = mk("astream_range_cnt")
        for i in range(5):
            await cache._cache.axadd(key, {"i": i})

        entries = await cache._cache.axrange(key, count=2)
        assert len(entries) == 2

    @pytest.mark.asyncio
    async def test_axrevrange(self, cache: KeyValueCache, mk):
        key = mk("astream_revrange")
        await cache._cache.axadd(key, {"a": 1})
        await cache._cache.axadd(key, {"b": 2})
        await cache._cache.axadd(key, {"c": 3})

        entries = await cache._cache.axrevrange(key)
        assert len(entries) == 3
        assert entries[0][1]["c"] == 3
        assert entries[2][1]["a"] == 1

    @pytest.mark.asyncio
    async def test_axdel(self, cache: KeyValueCache, mk):
        key = mk("astream_del")
        eid = await cache._cache.axadd(key, {"data": "test"})
        assert await cache._cache.axlen(key) == 1
        deleted = await cache._cache.axdel(key, eid)
        assert deleted == 1

    @pytest.mark.asyncio
    async def test_axtrim(self, cache: KeyValueCache, mk):
        key = mk("astream_trim")
        for i in range(10):
            await cache._cache.axadd(key, {"i": i})
        trimmed = await cache._cache.axtrim(key, maxlen=3, approximate=False)
        assert trimmed == 7
        assert await cache._cache.axlen(key) == 3


class TestAsyncStreamRead:
    """Tests for axread."""

    @pytest.mark.asyncio
    async def test_axread(self, cache: KeyValueCache, mk):
        key = mk("astream_read")
        await cache._cache.axadd(key, {"a": 1})
        await cache._cache.axadd(key, {"b": 2})

        result = await cache._cache.axread({key: "0-0"}, count=10)
        assert result is not None
        stream_entries = list(result.values())[0]
        assert len(stream_entries) == 2


class TestAsyncStreamInfo:
    """Tests for axinfo_stream, axinfo_groups."""

    @pytest.mark.asyncio
    async def test_axinfo_stream(self, cache: KeyValueCache, mk):
        key = mk("astream_info")
        await cache._cache.axadd(key, {"data": "test"})

        info = await cache._cache.axinfo_stream(key)
        assert isinstance(info, dict)

    @pytest.mark.asyncio
    async def test_axinfo_groups_empty(self, cache: KeyValueCache, mk):
        key = mk("astream_info_groups")
        await cache._cache.axadd(key, {"data": "test"})

        groups = await cache._cache.axinfo_groups(key)
        assert groups == []


class TestAsyncStreamConsumerGroups:
    """Tests for axgroup_create, axgroup_destroy, axreadgroup, axack, axpending."""

    @pytest.mark.asyncio
    async def test_axgroup_create_and_destroy(self, cache: KeyValueCache, mk):
        key = mk("astream_grp")
        await cache._cache.axadd(key, {"data": "test"})

        result = await cache._cache.axgroup_create(key, "mygroup", entry_id="0")
        assert result is True

        groups = await cache._cache.axinfo_groups(key)
        assert len(groups) == 1

        destroyed = await cache._cache.axgroup_destroy(key, "mygroup")
        assert destroyed == 1

    @pytest.mark.asyncio
    async def test_axgroup_create_mkstream(self, cache: KeyValueCache, mk):
        key = mk("astream_grp_mk")
        result = await cache._cache.axgroup_create(key, "mygroup", entry_id="$", mkstream=True)
        assert result is True
        assert await cache._cache.axlen(key) == 0

    @pytest.mark.asyncio
    async def test_axreadgroup(self, cache: KeyValueCache, mk):
        key = mk("astream_rg")
        await cache._cache.axadd(key, {"msg": "hello"})
        await cache._cache.axadd(key, {"msg": "world"})
        await cache._cache.axgroup_create(key, "readers", entry_id="0")

        result = await cache._cache.axreadgroup("readers", "consumer1", {key: ">"}, count=10)
        assert result is not None
        entries = list(result.values())[0]
        assert len(entries) == 2

    @pytest.mark.asyncio
    async def test_axack(self, cache: KeyValueCache, mk):
        key = mk("astream_ack")
        eid = await cache._cache.axadd(key, {"msg": "test"})
        await cache._cache.axgroup_create(key, "ack_grp", entry_id="0")
        await cache._cache.axreadgroup("ack_grp", "consumer1", {key: ">"})

        acked = await cache._cache.axack(key, "ack_grp", eid)
        assert acked == 1

    @pytest.mark.asyncio
    async def test_axpending_summary(self, cache: KeyValueCache, mk):
        key = mk("astream_pend")
        await cache._cache.axadd(key, {"msg": "test"})
        await cache._cache.axgroup_create(key, "pend_grp", entry_id="0")
        await cache._cache.axreadgroup("pend_grp", "consumer1", {key: ">"})

        pending = await cache._cache.axpending(key, "pend_grp")
        assert isinstance(pending, dict)

    @pytest.mark.asyncio
    async def test_axpending_range(self, cache: KeyValueCache, mk):
        key = mk("astream_pend_range")
        await cache._cache.axadd(key, {"msg": "test"})
        await cache._cache.axgroup_create(key, "pr_grp", entry_id="0")
        await cache._cache.axreadgroup("pr_grp", "consumer1", {key: ">"})

        result = await cache._cache.axpending(key, "pr_grp", start="-", end="+", count=10)
        assert isinstance(result, list)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_axgroup_setid(self, cache: KeyValueCache, mk):
        key = mk("astream_setid")
        await cache._cache.axadd(key, {"data": "test"})
        await cache._cache.axgroup_create(key, "setid_grp", entry_id="0")
        result = await cache._cache.axgroup_setid(key, "setid_grp", "$")
        assert result is True

    @pytest.mark.asyncio
    async def test_axgroup_delconsumer(self, cache: KeyValueCache, mk):
        key = mk("astream_delc")
        await cache._cache.axadd(key, {"data": "test"})
        await cache._cache.axgroup_create(key, "delc_grp", entry_id="0")
        await cache._cache.axreadgroup("delc_grp", "consumer1", {key: ">"})

        result = await cache._cache.axgroup_delconsumer(key, "delc_grp", "consumer1")
        assert isinstance(result, int)

    @pytest.mark.asyncio
    async def test_axinfo_consumers(self, cache: KeyValueCache, mk):
        key = mk("astream_infoc")
        await cache._cache.axadd(key, {"data": "test"})
        await cache._cache.axgroup_create(key, "infoc_grp", entry_id="0")
        await cache._cache.axreadgroup("infoc_grp", "consumer1", {key: ">"})

        consumers = await cache._cache.axinfo_consumers(key, "infoc_grp")
        assert len(consumers) == 1

    @pytest.mark.asyncio
    async def test_axclaim(self, cache: KeyValueCache, mk):
        key = mk("astream_claim")
        eid = await cache._cache.axadd(key, {"msg": "claimable"})
        await cache._cache.axgroup_create(key, "claim_grp", entry_id="0")
        await cache._cache.axreadgroup("claim_grp", "consumer1", {key: ">"})

        import asyncio

        await asyncio.sleep(0.01)
        claimed = await cache._cache.axclaim(key, "claim_grp", "consumer2", 0, [eid])
        assert len(claimed) == 1
        assert claimed[0][0] == eid

    @pytest.mark.asyncio
    async def test_axclaim_justid(self, cache: KeyValueCache, mk):
        key = mk("astream_claim_jid")
        eid = await cache._cache.axadd(key, {"msg": "claimable"})
        await cache._cache.axgroup_create(key, "claim_jid_grp", entry_id="0")
        await cache._cache.axreadgroup("claim_jid_grp", "consumer1", {key: ">"})

        import asyncio

        await asyncio.sleep(0.01)
        claimed = await cache._cache.axclaim(key, "claim_jid_grp", "consumer2", 0, [eid], justid=True)
        assert eid in claimed

    @pytest.mark.asyncio
    async def test_axautoclaim(self, cache: KeyValueCache, mk):
        key = mk("astream_autoclaim")
        await cache._cache.axadd(key, {"msg": "auto"})
        await cache._cache.axgroup_create(key, "auto_grp", entry_id="0")
        await cache._cache.axreadgroup("auto_grp", "consumer1", {key: ">"})

        import asyncio

        await asyncio.sleep(0.01)
        next_id, claimed, _deleted = await cache._cache.axautoclaim(key, "auto_grp", "consumer2", 0)
        assert isinstance(next_id, str)
        assert len(claimed) >= 1
