"""Tests for list operations."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from django_cachex.cache import KeyValueCache


class TestListOperations:
    def test_lpush_rpush(self, cache: KeyValueCache):
        # lpush adds to head
        cache.lpush("mylist", "world")
        cache.lpush("mylist", "hello")
        # rpush adds to tail
        cache.rpush("mylist", "!")

        result = cache.lrange("mylist", 0, -1)
        assert result == ["hello", "world", "!"]

    def test_lpush_multiple(self, cache: KeyValueCache):
        count = cache.lpush("mylist2", "a", "b", "c")
        assert count == 3
        # When pushing multiple, they're pushed in order, so last one ends up at head
        result = cache.lrange("mylist2", 0, -1)
        assert result == ["c", "b", "a"]

    def test_rpush_multiple(self, cache: KeyValueCache):
        count = cache.rpush("mylist3", "a", "b", "c")
        assert count == 3
        result = cache.lrange("mylist3", 0, -1)
        assert result == ["a", "b", "c"]

    def test_lpop(self, cache: KeyValueCache):
        cache.rpush("mylist4", "a", "b", "c")

        # Pop single element from head
        result = cache.lpop("mylist4")
        assert result == "a"

        # Pop multiple elements
        result = cache.lpop("mylist4", count=2)
        assert result == ["b", "c"]

        # Pop from empty list
        result = cache.lpop("mylist4")
        assert result is None

    def test_rpop(self, cache: KeyValueCache):
        cache.rpush("mylist5", "a", "b", "c")

        # Pop single element from tail
        result = cache.rpop("mylist5")
        assert result == "c"

        # Pop multiple elements
        result = cache.rpop("mylist5", count=2)
        assert result == ["b", "a"]

        # Pop from empty list
        result = cache.rpop("mylist5")
        assert result is None

    def test_lrange(self, cache: KeyValueCache):
        cache.rpush("mylist6", "a", "b", "c", "d", "e")

        # Get all elements
        assert cache.lrange("mylist6", 0, -1) == ["a", "b", "c", "d", "e"]

        # Get first 3
        assert cache.lrange("mylist6", 0, 2) == ["a", "b", "c"]

        # Get last 2
        assert cache.lrange("mylist6", -2, -1) == ["d", "e"]

        # Empty range
        assert cache.lrange("nonexistent", 0, -1) == []

    def test_lindex(self, cache: KeyValueCache):
        cache.rpush("mylist7", "a", "b", "c")

        assert cache.lindex("mylist7", 0) == "a"
        assert cache.lindex("mylist7", 1) == "b"
        assert cache.lindex("mylist7", -1) == "c"
        assert cache.lindex("mylist7", 100) is None
        assert cache.lindex("nonexistent", 0) is None

    def test_llen(self, cache: KeyValueCache):
        assert cache.llen("mylist8") == 0

        cache.rpush("mylist8", "a", "b", "c")
        assert cache.llen("mylist8") == 3

    def test_lrem(self, cache: KeyValueCache):
        cache.rpush("mylist9", "a", "b", "a", "c", "a")

        # Remove 2 occurrences from head
        removed = cache.lrem("mylist9", 2, "a")
        assert removed == 2
        assert cache.lrange("mylist9", 0, -1) == ["b", "c", "a"]

    def test_lrem_from_tail(self, cache: KeyValueCache):
        cache.rpush("mylist10", "a", "b", "a", "c", "a")

        # Remove 2 occurrences from tail (negative count)
        removed = cache.lrem("mylist10", -2, "a")
        assert removed == 2
        assert cache.lrange("mylist10", 0, -1) == ["a", "b", "c"]

    def test_lrem_all(self, cache: KeyValueCache):
        cache.rpush("mylist11", "a", "b", "a", "c", "a")

        # Remove all occurrences (count=0)
        removed = cache.lrem("mylist11", 0, "a")
        assert removed == 3
        assert cache.lrange("mylist11", 0, -1) == ["b", "c"]

    def test_ltrim(self, cache: KeyValueCache):
        cache.rpush("mylist12", "a", "b", "c", "d", "e")

        result = cache.ltrim("mylist12", 1, 3)
        assert result is True
        assert cache.lrange("mylist12", 0, -1) == ["b", "c", "d"]

    def test_lset(self, cache: KeyValueCache):
        cache.rpush("mylist13", "a", "b", "c")

        result = cache.lset("mylist13", 1, "B")
        assert result is True
        assert cache.lrange("mylist13", 0, -1) == ["a", "B", "c"]

    def test_linsert(self, cache: KeyValueCache):
        cache.rpush("mylist14", "a", "c")

        # Insert before
        length = cache.linsert("mylist14", "BEFORE", "c", "b")
        assert length == 3
        assert cache.lrange("mylist14", 0, -1) == ["a", "b", "c"]

        # Insert after
        length = cache.linsert("mylist14", "AFTER", "c", "d")
        assert length == 4
        assert cache.lrange("mylist14", 0, -1) == ["a", "b", "c", "d"]

        # Pivot not found
        length = cache.linsert("mylist14", "BEFORE", "z", "x")
        assert length == -1

    def test_list_with_complex_values(self, cache: KeyValueCache):
        cache.rpush("mylist15", {"name": "Alice"}, {"name": "Bob"})

        result = cache.lrange("mylist15", 0, -1)
        assert result == [{"name": "Alice"}, {"name": "Bob"}]

        popped = cache.lpop("mylist15")
        assert popped == {"name": "Alice"}

    def test_list_version_support(self, cache: KeyValueCache):
        cache.rpush("mylist", "v1_a", "v1_b", version=1)
        cache.rpush("mylist", "v2_a", version=2)

        assert cache.llen("mylist", version=1) == 2
        assert cache.llen("mylist", version=2) == 1

        assert cache.lrange("mylist", 0, -1, version=1) == ["v1_a", "v1_b"]
        assert cache.lrange("mylist", 0, -1, version=2) == ["v2_a"]

    def test_lpos_basic(self, cache: KeyValueCache):
        cache.rpush("mylist_lpos", "a", "b", "c", "b", "d")

        # Find first occurrence
        assert cache.lpos("mylist_lpos", "b") == 1

        # Element not found
        assert cache.lpos("mylist_lpos", "z") is None

    def test_lpos_with_rank(self, cache: KeyValueCache):
        cache.rpush("mylist_lpos2", "a", "b", "c", "b", "d", "b")

        # Find second occurrence (rank=2)
        assert cache.lpos("mylist_lpos2", "b", rank=2) == 3

        # Find from the end (negative rank)
        assert cache.lpos("mylist_lpos2", "b", rank=-1) == 5

    def test_lpos_with_count(self, cache: KeyValueCache):
        cache.rpush("mylist_lpos3", "a", "b", "c", "b", "d", "b")

        # Find all occurrences
        result = cache.lpos("mylist_lpos3", "b", count=0)
        assert result == [1, 3, 5]

        # Find first 2 occurrences
        result = cache.lpos("mylist_lpos3", "b", count=2)
        assert result == [1, 3]

    def test_lmove_basic(self, cache: KeyValueCache):
        # Use hash tags {list} to ensure keys are on same cluster slot
        cache.rpush("{list}src", "a", "b", "c")
        cache.rpush("{list}dst", "x", "y")

        # Move from src LEFT to dst RIGHT
        result = cache.lmove("{list}src", "{list}dst", "LEFT", "RIGHT")
        assert result == "a"

        assert cache.lrange("{list}src", 0, -1) == ["b", "c"]
        assert cache.lrange("{list}dst", 0, -1) == ["x", "y", "a"]

    def test_lmove_directions(self, cache: KeyValueCache):
        # Use hash tags {list} to ensure keys are on same cluster slot
        cache.rpush("{list}src2", "1", "2", "3")
        cache.rpush("{list}dst2", "a")

        # RIGHT to LEFT (rpoplpush equivalent)
        result = cache.lmove("{list}src2", "{list}dst2", "RIGHT", "LEFT")
        assert result == "3"
        assert cache.lrange("{list}src2", 0, -1) == ["1", "2"]
        assert cache.lrange("{list}dst2", 0, -1) == ["3", "a"]

    def test_lmove_empty_source(self, cache: KeyValueCache):
        # Use hash tags {list} to ensure keys are on same cluster slot
        cache.rpush("{list}dst3", "x")

        result = cache.lmove("{list}empty_src", "{list}dst3", "LEFT", "RIGHT")
        assert result is None
        assert cache.lrange("{list}dst3", 0, -1) == ["x"]

    def test_blpop_immediate(self, cache: KeyValueCache):
        cache.rpush("blpop_list", "a", "b", "c")

        result = cache.blpop("blpop_list", timeout=1)
        assert result is not None
        key, value = result
        assert "blpop_list" in key  # Key includes prefix/version
        assert value == "a"
        assert cache.lrange("blpop_list", 0, -1) == ["b", "c"]

    def test_blpop_timeout(self, cache: KeyValueCache):
        """Test blpop returns None after timeout on empty list."""
        result = cache.blpop("blpop_empty", timeout=0.1)
        assert result is None

    def test_blpop_multiple_keys(self, cache: KeyValueCache):
        # Use hash tags to ensure keys are on same cluster slot
        cache.rpush("{blpop}list2", "x", "y")

        result = cache.blpop(["{blpop}list1", "{blpop}list2"], timeout=1)
        assert result is not None
        key, value = result
        assert "{blpop}list2" in key
        assert value == "x"

    def test_brpop_immediate(self, cache: KeyValueCache):
        cache.rpush("brpop_list", "a", "b", "c")

        result = cache.brpop("brpop_list", timeout=1)
        assert result is not None
        key, value = result
        assert "brpop_list" in key
        assert value == "c"
        assert cache.lrange("brpop_list", 0, -1) == ["a", "b"]

    def test_brpop_timeout(self, cache: KeyValueCache):
        """Test brpop returns None after timeout on empty list."""
        result = cache.brpop("brpop_empty", timeout=0.1)
        assert result is None

    def test_blmove_immediate(self, cache: KeyValueCache):
        # Use hash tags to ensure keys are on same cluster slot
        cache.rpush("{blmove}src", "a", "b", "c")
        cache.rpush("{blmove}dst", "x")

        result = cache.blmove("{blmove}src", "{blmove}dst", timeout=1, wherefrom="LEFT", whereto="RIGHT")
        assert result == "a"
        assert cache.lrange("{blmove}src", 0, -1) == ["b", "c"]
        assert cache.lrange("{blmove}dst", 0, -1) == ["x", "a"]

    def test_blmove_timeout(self, cache: KeyValueCache):
        """Test blmove returns None after timeout on empty source."""
        # Use hash tags to ensure keys are on same cluster slot
        cache.rpush("{blmove_empty}dst", "x")

        result = cache.blmove("{blmove_empty}src", "{blmove_empty}dst", timeout=0.1)
        assert result is None

    def test_blpop_with_complex_values(self, cache: KeyValueCache):
        cache.rpush("blpop_complex", {"name": "Alice"}, {"name": "Bob"})

        result = cache.blpop("blpop_complex", timeout=1)
        assert result is not None
        _key, value = result
        assert value == {"name": "Alice"}


class TestVersionSrcDst:
    """Tests for version_src/version_dst on lmove and blmove."""

    def test_lmove_version_src_dst(self, cache: KeyValueCache):
        cache.rpush("{vs}:lsrc", "a", "b", version=1)
        cache.rpush("{vs}:ldst", "x", version=2)

        result = cache.lmove("{vs}:lsrc", "{vs}:ldst", "LEFT", "RIGHT", version_src=1, version_dst=2)
        assert result == "a"
        assert cache.lrange("{vs}:lsrc", 0, -1, version=1) == ["b"]
        assert cache.lrange("{vs}:ldst", 0, -1, version=2) == ["x", "a"]

    def test_blmove_version_src_dst(self, cache: KeyValueCache):
        cache.rpush("{vs}:blsrc", "a", "b", version=1)
        cache.rpush("{vs}:bldst", "x", version=2)

        result = cache.blmove(
            "{vs}:blsrc",
            "{vs}:bldst",
            timeout=1,
            wherefrom="LEFT",
            whereto="RIGHT",
            version_src=1,
            version_dst=2,
        )
        assert result == "a"
        assert cache.lrange("{vs}:blsrc", 0, -1, version=1) == ["b"]
        assert cache.lrange("{vs}:bldst", 0, -1, version=2) == ["x", "a"]


class TestAsyncListPushPop:
    """Tests for alpush, arpush, alpop, arpop."""

    @pytest.mark.asyncio
    async def test_alpush_arpush(self, cache: KeyValueCache):
        await cache.alpush("amylist", "world")
        await cache.alpush("amylist", "hello")
        await cache.arpush("amylist", "!")

        result = cache.lrange("amylist", 0, -1)
        assert result == ["hello", "world", "!"]

    @pytest.mark.asyncio
    async def test_alpush_multiple(self, cache: KeyValueCache):
        count = await cache.alpush("amylist2", "a", "b", "c")
        assert count == 3
        result = cache.lrange("amylist2", 0, -1)
        assert result == ["c", "b", "a"]

    @pytest.mark.asyncio
    async def test_arpush_multiple(self, cache: KeyValueCache):
        count = await cache.arpush("amylist3", "a", "b", "c")
        assert count == 3
        result = cache.lrange("amylist3", 0, -1)
        assert result == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_alpop(self, cache: KeyValueCache):
        cache.rpush("amylist4", "a", "b", "c")

        result = await cache.alpop("amylist4")
        assert result == "a"

        result = await cache.alpop("amylist4", count=2)
        assert result == ["b", "c"]

        result = await cache.alpop("amylist4")
        assert result is None

    @pytest.mark.asyncio
    async def test_arpop(self, cache: KeyValueCache):
        cache.rpush("amylist5", "a", "b", "c")

        result = await cache.arpop("amylist5")
        assert result == "c"

        result = await cache.arpop("amylist5", count=2)
        assert result == ["b", "a"]

        result = await cache.arpop("amylist5")
        assert result is None


class TestAsyncListRange:
    """Tests for alrange, alindex."""

    @pytest.mark.asyncio
    async def test_alrange(self, cache: KeyValueCache):
        cache.rpush("amylist6", "a", "b", "c", "d", "e")

        assert await cache.alrange("amylist6", 0, -1) == ["a", "b", "c", "d", "e"]
        assert await cache.alrange("amylist6", 0, 2) == ["a", "b", "c"]
        assert await cache.alrange("amylist6", -2, -1) == ["d", "e"]
        assert await cache.alrange("anonexistent", 0, -1) == []

    @pytest.mark.asyncio
    async def test_alindex(self, cache: KeyValueCache):
        cache.rpush("amylist7", "a", "b", "c")

        assert await cache.alindex("amylist7", 0) == "a"
        assert await cache.alindex("amylist7", 1) == "b"
        assert await cache.alindex("amylist7", -1) == "c"
        assert await cache.alindex("amylist7", 100) is None
        assert await cache.alindex("anonexistent", 0) is None


class TestAsyncListLength:
    """Tests for allen."""

    @pytest.mark.asyncio
    async def test_allen(self, cache: KeyValueCache):
        assert await cache.allen("amylist8") == 0
        cache.rpush("amylist8", "a", "b", "c")
        assert await cache.allen("amylist8") == 3


class TestAsyncListModify:
    """Tests for alrem, altrim, alset, alinsert."""

    @pytest.mark.asyncio
    async def test_alrem(self, cache: KeyValueCache):
        cache.rpush("amylist9", "a", "b", "a", "c", "a")
        removed = await cache.alrem("amylist9", 2, "a")
        assert removed == 2
        assert cache.lrange("amylist9", 0, -1) == ["b", "c", "a"]

    @pytest.mark.asyncio
    async def test_alrem_from_tail(self, cache: KeyValueCache):
        cache.rpush("amylist10", "a", "b", "a", "c", "a")
        removed = await cache.alrem("amylist10", -2, "a")
        assert removed == 2
        assert cache.lrange("amylist10", 0, -1) == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_alrem_all(self, cache: KeyValueCache):
        cache.rpush("amylist11", "a", "b", "a", "c", "a")
        removed = await cache.alrem("amylist11", 0, "a")
        assert removed == 3
        assert cache.lrange("amylist11", 0, -1) == ["b", "c"]

    @pytest.mark.asyncio
    async def test_altrim(self, cache: KeyValueCache):
        cache.rpush("amylist12", "a", "b", "c", "d", "e")
        result = await cache.altrim("amylist12", 1, 3)
        assert result is True
        assert cache.lrange("amylist12", 0, -1) == ["b", "c", "d"]

    @pytest.mark.asyncio
    async def test_alset(self, cache: KeyValueCache):
        cache.rpush("amylist13", "a", "b", "c")
        result = await cache.alset("amylist13", 1, "B")
        assert result is True
        assert cache.lrange("amylist13", 0, -1) == ["a", "B", "c"]

    @pytest.mark.asyncio
    async def test_alinsert(self, cache: KeyValueCache):
        cache.rpush("amylist14", "a", "c")

        length = await cache.alinsert("amylist14", "BEFORE", "c", "b")
        assert length == 3
        assert cache.lrange("amylist14", 0, -1) == ["a", "b", "c"]

        length = await cache.alinsert("amylist14", "AFTER", "c", "d")
        assert length == 4
        assert cache.lrange("amylist14", 0, -1) == ["a", "b", "c", "d"]

        length = await cache.alinsert("amylist14", "BEFORE", "z", "x")
        assert length == -1


class TestAsyncListPos:
    """Tests for alpos."""

    @pytest.mark.asyncio
    async def test_alpos_basic(self, cache: KeyValueCache):
        cache.rpush("amylist_lpos", "a", "b", "c", "b", "d")

        assert await cache.alpos("amylist_lpos", "b") == 1
        assert await cache.alpos("amylist_lpos", "z") is None

    @pytest.mark.asyncio
    async def test_alpos_with_rank(self, cache: KeyValueCache):
        cache.rpush("amylist_lpos2", "a", "b", "c", "b", "d", "b")

        assert await cache.alpos("amylist_lpos2", "b", rank=2) == 3
        assert await cache.alpos("amylist_lpos2", "b", rank=-1) == 5

    @pytest.mark.asyncio
    async def test_alpos_with_count(self, cache: KeyValueCache):
        cache.rpush("amylist_lpos3", "a", "b", "c", "b", "d", "b")

        result = await cache.alpos("amylist_lpos3", "b", count=0)
        assert result == [1, 3, 5]

        result = await cache.alpos("amylist_lpos3", "b", count=2)
        assert result == [1, 3]


class TestAsyncListMove:
    """Tests for almove."""

    @pytest.mark.asyncio
    async def test_almove_basic(self, cache: KeyValueCache):
        cache.rpush("{alist}src", "a", "b", "c")
        cache.rpush("{alist}dst", "x", "y")

        result = await cache.almove("{alist}src", "{alist}dst", "LEFT", "RIGHT")
        assert result == "a"

        assert cache.lrange("{alist}src", 0, -1) == ["b", "c"]
        assert cache.lrange("{alist}dst", 0, -1) == ["x", "y", "a"]

    @pytest.mark.asyncio
    async def test_almove_empty_source(self, cache: KeyValueCache):
        cache.rpush("{alist}dst3", "x")

        result = await cache.almove("{alist}empty_src", "{alist}dst3", "LEFT", "RIGHT")
        assert result is None


class TestAsyncBlockingOps:
    """Tests for ablpop, abrpop, ablmove."""

    @pytest.mark.asyncio
    async def test_ablpop_immediate(self, cache: KeyValueCache):
        cache.rpush("ablpop_list", "a", "b", "c")

        result = await cache.ablpop("ablpop_list", timeout=1)
        assert result is not None
        rkey, value = result
        assert rkey == "ablpop_list"
        assert value == "a"

    @pytest.mark.asyncio
    async def test_ablpop_timeout(self, cache: KeyValueCache):
        result = await cache.ablpop("ablpop_empty", timeout=0.1)
        assert result is None

    @pytest.mark.asyncio
    async def test_abrpop_immediate(self, cache: KeyValueCache):
        cache.rpush("abrpop_list", "a", "b", "c")

        result = await cache.abrpop("abrpop_list", timeout=1)
        assert result is not None
        rkey, value = result
        assert rkey == "abrpop_list"
        assert value == "c"

    @pytest.mark.asyncio
    async def test_abrpop_timeout(self, cache: KeyValueCache):
        result = await cache.abrpop("abrpop_empty", timeout=0.1)
        assert result is None

    @pytest.mark.asyncio
    async def test_ablmove_immediate(self, cache: KeyValueCache):
        cache.rpush("{ablmove}src", "a", "b", "c")
        cache.rpush("{ablmove}dst", "x")

        result = await cache.ablmove(
            "{ablmove}src",
            "{ablmove}dst",
            timeout=1,
            wherefrom="LEFT",
            whereto="RIGHT",
        )
        assert result == "a"
        assert cache.lrange("{ablmove}src", 0, -1) == ["b", "c"]
        assert cache.lrange("{ablmove}dst", 0, -1) == ["x", "a"]

    @pytest.mark.asyncio
    async def test_ablmove_timeout(self, cache: KeyValueCache):
        cache.rpush("{ablmove_empty}dst", "x")

        result = await cache.ablmove(
            "{ablmove_empty}src",
            "{ablmove_empty}dst",
            timeout=0.1,
        )
        assert result is None


class TestAsyncVersionSrcDst:
    """Tests for version_src/version_dst on almove and ablmove."""

    @pytest.mark.asyncio
    async def test_almove_version_src_dst(self, cache: KeyValueCache):
        cache.rpush("{vs}:alsrc", "a", "b", version=1)
        cache.rpush("{vs}:aldst", "x", version=2)

        result = await cache.almove("{vs}:alsrc", "{vs}:aldst", "LEFT", "RIGHT", version_src=1, version_dst=2)
        assert result == "a"
        assert cache.lrange("{vs}:alsrc", 0, -1, version=1) == ["b"]
        assert cache.lrange("{vs}:aldst", 0, -1, version=2) == ["x", "a"]

    @pytest.mark.asyncio
    async def test_ablmove_version_src_dst(self, cache: KeyValueCache):
        cache.rpush("{vs}:ablsrc", "a", "b", version=1)
        cache.rpush("{vs}:abldst", "x", version=2)

        result = await cache.ablmove(
            "{vs}:ablsrc",
            "{vs}:abldst",
            timeout=1,
            wherefrom="LEFT",
            whereto="RIGHT",
            version_src=1,
            version_dst=2,
        )
        assert result == "a"
        assert cache.lrange("{vs}:ablsrc", 0, -1, version=1) == ["b"]
        assert cache.lrange("{vs}:abldst", 0, -1, version=2) == ["x", "a"]


class TestAsyncListSerialization:
    """Tests for serialization of complex values into list operations."""

    @pytest.mark.asyncio
    async def test_alist_with_complex_values(self, cache: KeyValueCache):
        await cache.arpush("amylist15", {"name": "Alice"}, {"name": "Bob"})

        result = cache.lrange("amylist15", 0, -1)
        assert result == [{"name": "Alice"}, {"name": "Bob"}]

        popped = await cache.alpop("amylist15")
        assert popped == {"name": "Alice"}


class TestAsyncListVersioning:
    """Tests for version parameter on async list operations."""

    @pytest.mark.asyncio
    async def test_alist_version_support(self, cache: KeyValueCache):
        await cache.arpush("amylist", "v1_a", "v1_b", version=1)
        await cache.arpush("amylist", "v2_a", version=2)

        assert cache.llen("amylist", version=1) == 2
        assert cache.llen("amylist", version=2) == 1

        assert cache.lrange("amylist", 0, -1, version=1) == ["v1_a", "v1_b"]
        assert cache.lrange("amylist", 0, -1, version=2) == ["v2_a"]


class TestAsyncBlockingPopExtra:
    """Additional ablpop coverage."""

    @pytest.mark.asyncio
    async def test_ablpop_multiple_keys(self, cache: KeyValueCache):
        cache.rpush("{ablpop}list2", "x", "y")

        result = await cache.ablpop(["{ablpop}list1", "{ablpop}list2"], timeout=1)
        assert result is not None
        key, value = result
        assert "{ablpop}list2" in key
        assert value == "x"

    @pytest.mark.asyncio
    async def test_ablpop_with_complex_values(self, cache: KeyValueCache):
        cache.rpush("ablpop_complex", {"name": "Alice"}, {"name": "Bob"})

        result = await cache.ablpop("ablpop_complex", timeout=1)
        assert result is not None
        _key, value = result
        assert value == {"name": "Alice"}
