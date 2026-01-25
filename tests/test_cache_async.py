"""Tests for async cache operations."""

import pytest

from django_cachex.cache import KeyValueCache


class TestAsyncAdd:
    """Tests for the aadd() method."""

    @pytest.mark.asyncio
    async def test_aadd_succeeds_for_new_key(self, cache: KeyValueCache):
        """Test aadd creates new key."""
        cache.delete("async_add_new")
        result = await cache.aadd("async_add_new", "new_value")
        assert result is True
        assert cache.get("async_add_new") == "new_value"

    @pytest.mark.asyncio
    async def test_aadd_fails_for_existing_key(self, cache: KeyValueCache):
        """Test aadd does not overwrite existing key."""
        cache.set("async_add_existing", "original")
        result = await cache.aadd("async_add_existing", "new_value")
        assert result is False
        assert cache.get("async_add_existing") == "original"

    @pytest.mark.asyncio
    async def test_aadd_with_timeout(self, cache: KeyValueCache):
        """Test aadd with explicit timeout."""
        cache.delete("async_add_timeout")
        result = await cache.aadd("async_add_timeout", "timed_value", timeout=60)
        assert result is True
        ttl = cache.ttl("async_add_timeout")
        assert ttl is not None and ttl > 0


class TestAsyncSet:
    """Tests for the aset() method."""

    @pytest.mark.asyncio
    async def test_aset_and_get(self, cache: KeyValueCache):
        """Test aset stores value that can be retrieved."""
        await cache.aset("async_set_key", "async_set_value")
        result = cache.get("async_set_key")
        assert result == "async_set_value"

    @pytest.mark.asyncio
    async def test_aset_with_timeout(self, cache: KeyValueCache):
        """Test aset with explicit timeout."""
        await cache.aset("async_timeout_key", "timeout_value", timeout=60)
        result = cache.get("async_timeout_key")
        assert result == "timeout_value"
        # Verify TTL was set
        ttl = cache.ttl("async_timeout_key")
        assert ttl is not None and ttl > 0

    @pytest.mark.asyncio
    async def test_aset_overwrites_existing(self, cache: KeyValueCache):
        """Test aset overwrites existing value."""
        cache.set("async_overwrite", "original")
        await cache.aset("async_overwrite", "updated")
        result = cache.get("async_overwrite")
        assert result == "updated"

    @pytest.mark.asyncio
    async def test_aset_complex_value(self, cache: KeyValueCache):
        """Test aset with complex data types."""
        data = {"items": [1, 2, 3], "nested": {"key": "value"}}
        await cache.aset("async_complex_set", data)
        result = cache.get("async_complex_set")
        assert result == data

    @pytest.mark.asyncio
    async def test_aset_with_version(self, cache: KeyValueCache):
        """Test aset respects version parameter."""
        await cache.aset("versioned_set", "v1_data", version=1)
        await cache.aset("versioned_set", "v2_data", version=2)

        result_v1 = cache.get("versioned_set", version=1)
        result_v2 = cache.get("versioned_set", version=2)

        assert result_v1 == "v1_data"
        assert result_v2 == "v2_data"


class TestAsyncDelete:
    """Tests for the adelete() method."""

    @pytest.mark.asyncio
    async def test_adelete_existing_key(self, cache: KeyValueCache):
        """Test adelete removes an existing key."""
        cache.set("async_delete_key", "to_delete")
        assert cache.get("async_delete_key") == "to_delete"

        result = await cache.adelete("async_delete_key")
        assert result is True
        assert cache.get("async_delete_key") is None

    @pytest.mark.asyncio
    async def test_adelete_nonexistent_key(self, cache: KeyValueCache):
        """Test adelete returns False for nonexistent key."""
        cache.delete("nonexistent_async_key")
        result = await cache.adelete("nonexistent_async_key")
        assert result is False

    @pytest.mark.asyncio
    async def test_adelete_with_version(self, cache: KeyValueCache):
        """Test adelete respects version parameter."""
        cache.set("versioned_delete", "v1_data", version=1)
        cache.set("versioned_delete", "v2_data", version=2)

        await cache.adelete("versioned_delete", version=1)

        assert cache.get("versioned_delete", version=1) is None
        assert cache.get("versioned_delete", version=2) == "v2_data"


class TestAsyncTouch:
    """Tests for the atouch() method."""

    @pytest.mark.asyncio
    async def test_atouch_updates_timeout(self, cache: KeyValueCache):
        """Test atouch updates the timeout."""
        cache.set("async_touch_key", "value", timeout=10)
        result = await cache.atouch("async_touch_key", timeout=60)
        assert result is True
        ttl = cache.ttl("async_touch_key")
        assert ttl is not None and ttl > 10

    @pytest.mark.asyncio
    async def test_atouch_nonexistent_key(self, cache: KeyValueCache):
        """Test atouch returns False for nonexistent key."""
        cache.delete("async_touch_missing")
        result = await cache.atouch("async_touch_missing", timeout=60)
        assert result is False


class TestAsyncHasKey:
    """Tests for the ahas_key() method."""

    @pytest.mark.asyncio
    async def test_ahas_key_exists(self, cache: KeyValueCache):
        """Test ahas_key returns True for existing key."""
        cache.set("async_has_key", "value")
        result = await cache.ahas_key("async_has_key")
        assert result is True

    @pytest.mark.asyncio
    async def test_ahas_key_missing(self, cache: KeyValueCache):
        """Test ahas_key returns False for missing key."""
        cache.delete("async_missing_key")
        result = await cache.ahas_key("async_missing_key")
        assert result is False


class TestAsyncIncr:
    """Tests for the aincr() and adecr() methods."""

    @pytest.mark.asyncio
    async def test_aincr_increments(self, cache: KeyValueCache):
        """Test aincr increments value."""
        cache.set("async_counter", 10)
        result = await cache.aincr("async_counter")
        assert result == 11
        assert cache.get("async_counter") == 11

    @pytest.mark.asyncio
    async def test_aincr_by_amount(self, cache: KeyValueCache):
        """Test aincr increments by specified amount."""
        cache.set("async_counter2", 5)
        result = await cache.aincr("async_counter2", 10)
        assert result == 15

    @pytest.mark.asyncio
    async def test_aincr_missing_key_raises(self, cache: KeyValueCache):
        """Test aincr raises ValueError for missing key."""
        cache.delete("async_missing_counter")
        with pytest.raises(ValueError):
            await cache.aincr("async_missing_counter")

    @pytest.mark.asyncio
    async def test_adecr_decrements(self, cache: KeyValueCache):
        """Test adecr decrements value."""
        cache.set("async_decr", 10)
        result = await cache.adecr("async_decr")
        assert result == 9


class TestAsyncGetMany:
    """Tests for the aget_many() method."""

    @pytest.mark.asyncio
    async def test_aget_many_retrieves_multiple(self, cache: KeyValueCache):
        """Test aget_many retrieves multiple values."""
        cache.set("async_many_a", "value_a")
        cache.set("async_many_b", "value_b")
        cache.set("async_many_c", "value_c")

        result = await cache.aget_many(["async_many_a", "async_many_b", "async_many_c"])
        assert result == {
            "async_many_a": "value_a",
            "async_many_b": "value_b",
            "async_many_c": "value_c",
        }

    @pytest.mark.asyncio
    async def test_aget_many_partial_match(self, cache: KeyValueCache):
        """Test aget_many only returns existing keys."""
        cache.set("async_partial_a", "a")
        cache.delete("async_partial_b")

        result = await cache.aget_many(["async_partial_a", "async_partial_b"])
        assert result == {"async_partial_a": "a"}


class TestAsyncSetMany:
    """Tests for the aset_many() method."""

    @pytest.mark.asyncio
    async def test_aset_many_stores_multiple(self, cache: KeyValueCache):
        """Test aset_many stores multiple values."""
        await cache.aset_many(
            {
                "async_set_many_x": 1,
                "async_set_many_y": 2,
                "async_set_many_z": 3,
            },
        )

        assert cache.get("async_set_many_x") == 1
        assert cache.get("async_set_many_y") == 2
        assert cache.get("async_set_many_z") == 3


class TestAsyncDeleteMany:
    """Tests for the adelete_many() method."""

    @pytest.mark.asyncio
    async def test_adelete_many_removes_multiple(self, cache: KeyValueCache):
        """Test adelete_many removes multiple keys."""
        cache.set_many({"async_del_1": 1, "async_del_2": 2, "async_del_3": 3})

        result = await cache.adelete_many(["async_del_1", "async_del_2"])
        assert result == 2

        assert cache.get("async_del_1") is None
        assert cache.get("async_del_2") is None
        assert cache.get("async_del_3") == 3


class TestAsyncClear:
    """Tests for the aclear() method."""

    @pytest.mark.asyncio
    async def test_aclear_removes_all(self, cache: KeyValueCache):
        """Test aclear removes all keys."""
        cache.set("async_clear_key", "value")
        assert cache.get("async_clear_key") == "value"

        result = await cache.aclear()
        assert result is True
        assert cache.get("async_clear_key") is None


class TestAsyncGet:
    """Tests for the aget() method."""

    @pytest.mark.asyncio
    async def test_aget_existing_key(self, cache: KeyValueCache):
        """Test aget retrieves an existing value."""
        cache.set("async_key", "async_value")
        result = await cache.aget("async_key")
        assert result == "async_value"

    @pytest.mark.asyncio
    async def test_aget_missing_key_returns_default(self, cache: KeyValueCache):
        """Test aget returns default for missing key."""
        cache.delete("missing_async_key")
        result = await cache.aget("missing_async_key")
        assert result is None

    @pytest.mark.asyncio
    async def test_aget_missing_key_with_custom_default(self, cache: KeyValueCache):
        """Test aget returns custom default for missing key."""
        cache.delete("missing_async_key2")
        result = await cache.aget("missing_async_key2", default="fallback")
        assert result == "fallback"

    @pytest.mark.asyncio
    async def test_aget_complex_value(self, cache: KeyValueCache):
        """Test aget with complex data types."""
        data = {"user": "alice", "scores": [10, 20, 30], "active": True}
        cache.set("async_complex", data)
        result = await cache.aget("async_complex")
        assert result == data

    @pytest.mark.asyncio
    async def test_aget_with_version(self, cache: KeyValueCache):
        """Test aget respects version parameter."""
        cache.set("versioned_key", "v1_value", version=1)
        cache.set("versioned_key", "v2_value", version=2)

        result_v1 = await cache.aget("versioned_key", version=1)
        result_v2 = await cache.aget("versioned_key", version=2)

        assert result_v1 == "v1_value"
        assert result_v2 == "v2_value"
