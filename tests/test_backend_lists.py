"""Tests for list operations."""

from django_cachex.cache import RedisCache


class TestListOperations:
    def test_lpush_rpush(self, cache: RedisCache):
        # lpush adds to head
        cache.lpush("mylist", "world")
        cache.lpush("mylist", "hello")
        # rpush adds to tail
        cache.rpush("mylist", "!")

        result = cache.lrange("mylist", 0, -1)
        assert result == ["hello", "world", "!"]

    def test_lpush_multiple(self, cache: RedisCache):
        count = cache.lpush("mylist2", "a", "b", "c")
        assert count == 3
        # When pushing multiple, they're pushed in order, so last one ends up at head
        result = cache.lrange("mylist2", 0, -1)
        assert result == ["c", "b", "a"]

    def test_rpush_multiple(self, cache: RedisCache):
        count = cache.rpush("mylist3", "a", "b", "c")
        assert count == 3
        result = cache.lrange("mylist3", 0, -1)
        assert result == ["a", "b", "c"]

    def test_lpop(self, cache: RedisCache):
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

    def test_rpop(self, cache: RedisCache):
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

    def test_lrange(self, cache: RedisCache):
        cache.rpush("mylist6", "a", "b", "c", "d", "e")

        # Get all elements
        assert cache.lrange("mylist6", 0, -1) == ["a", "b", "c", "d", "e"]

        # Get first 3
        assert cache.lrange("mylist6", 0, 2) == ["a", "b", "c"]

        # Get last 2
        assert cache.lrange("mylist6", -2, -1) == ["d", "e"]

        # Empty range
        assert cache.lrange("nonexistent", 0, -1) == []

    def test_lindex(self, cache: RedisCache):
        cache.rpush("mylist7", "a", "b", "c")

        assert cache.lindex("mylist7", 0) == "a"
        assert cache.lindex("mylist7", 1) == "b"
        assert cache.lindex("mylist7", -1) == "c"
        assert cache.lindex("mylist7", 100) is None
        assert cache.lindex("nonexistent", 0) is None

    def test_llen(self, cache: RedisCache):
        assert cache.llen("mylist8") == 0

        cache.rpush("mylist8", "a", "b", "c")
        assert cache.llen("mylist8") == 3

    def test_lrem(self, cache: RedisCache):
        cache.rpush("mylist9", "a", "b", "a", "c", "a")

        # Remove 2 occurrences from head
        removed = cache.lrem("mylist9", 2, "a")
        assert removed == 2
        assert cache.lrange("mylist9", 0, -1) == ["b", "c", "a"]

    def test_lrem_from_tail(self, cache: RedisCache):
        cache.rpush("mylist10", "a", "b", "a", "c", "a")

        # Remove 2 occurrences from tail (negative count)
        removed = cache.lrem("mylist10", -2, "a")
        assert removed == 2
        assert cache.lrange("mylist10", 0, -1) == ["a", "b", "c"]

    def test_lrem_all(self, cache: RedisCache):
        cache.rpush("mylist11", "a", "b", "a", "c", "a")

        # Remove all occurrences (count=0)
        removed = cache.lrem("mylist11", 0, "a")
        assert removed == 3
        assert cache.lrange("mylist11", 0, -1) == ["b", "c"]

    def test_ltrim(self, cache: RedisCache):
        cache.rpush("mylist12", "a", "b", "c", "d", "e")

        result = cache.ltrim("mylist12", 1, 3)
        assert result is True
        assert cache.lrange("mylist12", 0, -1) == ["b", "c", "d"]

    def test_lset(self, cache: RedisCache):
        cache.rpush("mylist13", "a", "b", "c")

        result = cache.lset("mylist13", 1, "B")
        assert result is True
        assert cache.lrange("mylist13", 0, -1) == ["a", "B", "c"]

    def test_linsert(self, cache: RedisCache):
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

    def test_list_with_complex_values(self, cache: RedisCache):
        """Test that lists work with complex serialized values."""
        cache.rpush("mylist15", {"name": "Alice"}, {"name": "Bob"})

        result = cache.lrange("mylist15", 0, -1)
        assert result == [{"name": "Alice"}, {"name": "Bob"}]

        popped = cache.lpop("mylist15")
        assert popped == {"name": "Alice"}

    def test_list_version_support(self, cache: RedisCache):
        """Test that version parameter works correctly."""
        cache.rpush("mylist", "v1_a", "v1_b", version=1)
        cache.rpush("mylist", "v2_a", version=2)

        assert cache.llen("mylist", version=1) == 2
        assert cache.llen("mylist", version=2) == 1

        assert cache.lrange("mylist", 0, -1, version=1) == ["v1_a", "v1_b"]
        assert cache.lrange("mylist", 0, -1, version=2) == ["v2_a"]

    def test_lpos_basic(self, cache: RedisCache):
        """Test finding element position in list."""
        cache.rpush("mylist_lpos", "a", "b", "c", "b", "d")

        # Find first occurrence
        assert cache.lpos("mylist_lpos", "b") == 1

        # Element not found
        assert cache.lpos("mylist_lpos", "z") is None

    def test_lpos_with_rank(self, cache: RedisCache):
        """Test lpos with rank parameter."""
        cache.rpush("mylist_lpos2", "a", "b", "c", "b", "d", "b")

        # Find second occurrence (rank=2)
        assert cache.lpos("mylist_lpos2", "b", rank=2) == 3

        # Find from the end (negative rank)
        assert cache.lpos("mylist_lpos2", "b", rank=-1) == 5

    def test_lpos_with_count(self, cache: RedisCache):
        """Test lpos with count parameter returns multiple indices."""
        cache.rpush("mylist_lpos3", "a", "b", "c", "b", "d", "b")

        # Find all occurrences
        result = cache.lpos("mylist_lpos3", "b", count=0)
        assert result == [1, 3, 5]

        # Find first 2 occurrences
        result = cache.lpos("mylist_lpos3", "b", count=2)
        assert result == [1, 3]

    def test_lmove_basic(self, cache: RedisCache):
        """Test moving element between lists."""
        # Use hash tags {list} to ensure keys are on same cluster slot
        cache.rpush("{list}src", "a", "b", "c")
        cache.rpush("{list}dst", "x", "y")

        # Move from src LEFT to dst RIGHT
        result = cache.lmove("{list}src", "{list}dst", "LEFT", "RIGHT")
        assert result == "a"

        assert cache.lrange("{list}src", 0, -1) == ["b", "c"]
        assert cache.lrange("{list}dst", 0, -1) == ["x", "y", "a"]

    def test_lmove_directions(self, cache: RedisCache):
        """Test different lmove directions."""
        # Use hash tags {list} to ensure keys are on same cluster slot
        cache.rpush("{list}src2", "1", "2", "3")
        cache.rpush("{list}dst2", "a")

        # RIGHT to LEFT (rpoplpush equivalent)
        result = cache.lmove("{list}src2", "{list}dst2", "RIGHT", "LEFT")
        assert result == "3"
        assert cache.lrange("{list}src2", 0, -1) == ["1", "2"]
        assert cache.lrange("{list}dst2", 0, -1) == ["3", "a"]

    def test_lmove_empty_source(self, cache: RedisCache):
        """Test lmove on empty source list."""
        # Use hash tags {list} to ensure keys are on same cluster slot
        cache.rpush("{list}dst3", "x")

        result = cache.lmove("{list}empty_src", "{list}dst3", "LEFT", "RIGHT")
        assert result is None
        assert cache.lrange("{list}dst3", 0, -1) == ["x"]

    def test_blpop_immediate(self, cache: RedisCache):
        """Test blpop returns immediately when data exists."""
        cache.rpush("blpop_list", "a", "b", "c")

        result = cache.blpop("blpop_list", timeout=1)
        assert result is not None
        key, value = result
        assert "blpop_list" in key  # Key includes prefix/version
        assert value == "a"
        assert cache.lrange("blpop_list", 0, -1) == ["b", "c"]

    def test_blpop_timeout(self, cache: RedisCache):
        """Test blpop returns None after timeout on empty list."""
        result = cache.blpop("blpop_empty", timeout=0.1)
        assert result is None

    def test_blpop_multiple_keys(self, cache: RedisCache):
        """Test blpop with multiple keys returns first available."""
        # Use hash tags to ensure keys are on same cluster slot
        cache.rpush("{blpop}list2", "x", "y")

        result = cache.blpop("{blpop}list1", "{blpop}list2", timeout=1)
        assert result is not None
        key, value = result
        assert "{blpop}list2" in key
        assert value == "x"

    def test_brpop_immediate(self, cache: RedisCache):
        """Test brpop returns immediately when data exists."""
        cache.rpush("brpop_list", "a", "b", "c")

        result = cache.brpop("brpop_list", timeout=1)
        assert result is not None
        key, value = result
        assert "brpop_list" in key
        assert value == "c"
        assert cache.lrange("brpop_list", 0, -1) == ["a", "b"]

    def test_brpop_timeout(self, cache: RedisCache):
        """Test brpop returns None after timeout on empty list."""
        result = cache.brpop("brpop_empty", timeout=0.1)
        assert result is None

    def test_blmove_immediate(self, cache: RedisCache):
        """Test blmove returns immediately when data exists."""
        # Use hash tags to ensure keys are on same cluster slot
        cache.rpush("{blmove}src", "a", "b", "c")
        cache.rpush("{blmove}dst", "x")

        result = cache.blmove("{blmove}src", "{blmove}dst", timeout=1, src_direction="LEFT", dest_direction="RIGHT")
        assert result == "a"
        assert cache.lrange("{blmove}src", 0, -1) == ["b", "c"]
        assert cache.lrange("{blmove}dst", 0, -1) == ["x", "a"]

    def test_blmove_timeout(self, cache: RedisCache):
        """Test blmove returns None after timeout on empty source."""
        # Use hash tags to ensure keys are on same cluster slot
        cache.rpush("{blmove_empty}dst", "x")

        result = cache.blmove("{blmove_empty}src", "{blmove_empty}dst", timeout=0.1)
        assert result is None

    def test_blpop_with_complex_values(self, cache: RedisCache):
        """Test blpop works with serialized complex values."""
        cache.rpush("blpop_complex", {"name": "Alice"}, {"name": "Bob"})

        result = cache.blpop("blpop_complex", timeout=1)
        assert result is not None
        _key, value = result
        assert value == {"name": "Alice"}
