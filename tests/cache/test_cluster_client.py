"""Tests for RedisPyClusterAdapter."""

import asyncio
import pickle
import threading
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.cluster import RedisCluster, key_slot

from django_cachex.adapters import RedisPyClusterAdapter


def setup_cluster_client(mock_cluster_cls=None):
    """Build a RedisPyClusterAdapter for testing.

    Each call gets isolated cluster registries so concurrent tests don't
    share cached cluster instances and don't poison the process-wide
    real-driver caches.
    """
    client = RedisPyClusterAdapter.__new__(RedisPyClusterAdapter)
    client._servers = ["redis://localhost:7000"]
    client._options = {}
    client._stampede_config = None
    client._clusters = {}
    client._clusters_lock = threading.Lock()
    client._async_clusters = weakref.WeakKeyDictionary()
    if mock_cluster_cls is not None:
        client._cluster_class = mock_cluster_cls
    else:
        client._cluster_class = RedisCluster
    client._key_slot_func = key_slot
    return client


class TestRedisClusterAdapter:
    def test_get_client_creates_cluster(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)

        result = client.get_client()

        assert result == mock_cluster
        mock_cluster_cls.from_url.assert_called_once()

    def test_get_client_caches_cluster(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)

        result1 = client.get_client()
        result2 = client.get_client()

        assert result1 is result2
        assert mock_cluster_cls.from_url.call_count == 1

    def test_group_keys_by_slot(self):
        client = setup_cluster_client()

        keys = ["{user}:1", "{user}:2", "{user}:3"]
        slots = client._group_keys_by_slot(keys)

        assert len(slots) == 1
        slot_keys = list(slots.values())[0]
        assert len(slot_keys) == 3

    def test_group_keys_by_slot_different_slots(self):
        client = setup_cluster_client()

        # {a} -> slot 15495, {b} -> slot 3300, {c} -> slot 7365
        keys = ["{a}key1", "{b}key2", "{c}key3"]
        slots = client._group_keys_by_slot(keys)

        assert len(slots) == 3
        total_keys = sum(len(v) for v in slots.values())
        assert total_keys == 3

    def test_get_many_uses_mget_nonatomic(self):
        """Test get_many uses mget_nonatomic for cross-slot keys."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)
        client._serializers = [MagicMock()]
        client._compressors = [MagicMock()]

        client.key_func = lambda k, p, v: k

        client._serializers[0].loads.side_effect = pickle.loads
        client._compressors[0].decompress.side_effect = lambda x: x

        mock_cluster.mget_nonatomic.return_value = [
            pickle.dumps("value_a"),
            pickle.dumps("value_b"),
            pickle.dumps("value_c"),
        ]

        result = client.get_many(["{a}key1", "{b}key2", "{c}key3"])

        # Adapter returns raw bytes (cache layer is responsible for decoding).
        assert len(result) == 3
        mock_cluster.mget_nonatomic.assert_called_once()
        assert pickle.dumps("value_a") in result.values()
        assert pickle.dumps("value_b") in result.values()
        assert pickle.dumps("value_c") in result.values()

    def test_get_many_empty_keys(self):
        client = setup_cluster_client()

        result = client.get_many([])
        assert result == {}

    def test_delete_many_groups_by_slot(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)

        client.key_func = lambda k, p, v: k

        mock_cluster.delete.return_value = 1

        # {a} -> slot 15495, {b} -> slot 3300, {c} -> slot 7365
        client.delete_many(["{a}key1", "{b}key2", "{c}key3"])

        assert mock_cluster.delete.call_count == 3

    def test_delete_many_empty_keys(self):
        client = setup_cluster_client()

        client.delete_many([])

    def test_delete_many_same_slot(self):
        """Test delete_many with keys in the same slot uses single delete."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)

        client.key_func = lambda k, p, v: k

        mock_cluster.delete.return_value = 3

        client.delete_many(["{user}key1", "{user}key2", "{user}key3"])

        mock_cluster.delete.assert_called_once()

    def test_clear_flushes_all_primaries(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)

        # Low-level clear() still calls flushdb (used by RespCache.flush_db())
        client.clear()

        mock_cluster.flushdb.assert_called_once_with(target_nodes="primaries")

    def test_keys_scans_all_primaries(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)

        client.key_func = lambda k, p, v: f"{p}:{v}:{k}"

        mock_cluster.keys.return_value = [
            b"prefix:1:foo_1",
            b"prefix:1:foo_2",
            b"prefix:1:foo_3",
        ]

        result = client.keys("foo_*")

        mock_cluster.keys.assert_called_once()
        call_kwargs = mock_cluster.keys.call_args.kwargs
        assert call_kwargs.get("target_nodes") == "primaries"

        # Full keys: the cache backend, not the adapter, strips the prefix.
        assert len(result) == 3
        assert "prefix:1:foo_1" in result
        assert "prefix:1:foo_2" in result
        assert "prefix:1:foo_3" in result

    def test_keys_empty_result(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)

        client.key_func = lambda k, p, v: k

        mock_cluster.keys.return_value = []

        result = client.keys("nonexistent_*")

        assert result == []
        mock_cluster.keys.assert_called_once()

    def test_iter_keys_scans_all_primaries(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)

        client._default_scan_itersize = 10
        client.key_func = lambda k, p, v: f"{p}:{v}:{k}"

        mock_cluster.scan_iter.return_value = iter(
            [
                b"prefix:1:bar_1",
                b"prefix:1:bar_2",
                b"prefix:1:bar_3",
            ],
        )

        result = list(client.iter_keys("bar_*"))

        mock_cluster.scan_iter.assert_called_once()
        call_kwargs = mock_cluster.scan_iter.call_args.kwargs
        assert call_kwargs.get("target_nodes") == "primaries"

        # Full keys: the cache backend, not the adapter, strips the prefix.
        assert len(result) == 3
        assert "prefix:1:bar_1" in result
        assert "prefix:1:bar_2" in result
        assert "prefix:1:bar_3" in result

    def test_iter_keys_with_itersize(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)

        client._default_scan_itersize = 10
        client.key_func = lambda k, p, v: k

        mock_cluster.scan_iter.return_value = iter([])

        list(client.iter_keys("*", itersize=500))

        call_kwargs = mock_cluster.scan_iter.call_args.kwargs
        assert call_kwargs.get("count") == 500
        assert call_kwargs.get("target_nodes") == "primaries"

    def test_delete_pattern_deletes_across_primaries(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)

        client._default_scan_itersize = 10
        client.key_func = lambda k, p, v: f"{p}:{v}:{k}"

        mock_cluster.scan_iter.return_value = iter(
            [
                b"prefix:1:temp_1",
                b"prefix:1:temp_2",
                b"prefix:1:temp_3",
            ],
        )
        mock_cluster.delete.return_value = 1

        result = client.delete_pattern("temp_*")

        mock_cluster.scan_iter.assert_called_once()
        call_kwargs = mock_cluster.scan_iter.call_args.kwargs
        assert call_kwargs.get("target_nodes") == "primaries"

        assert result == 3

    def test_delete_pattern_empty_result(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)

        client._default_scan_itersize = 10
        client.key_func = lambda k, p, v: k

        mock_cluster.scan_iter.return_value = iter([])

        result = client.delete_pattern("nonexistent_*")

        assert result == 0
        mock_cluster.delete.assert_not_called()

    def test_delete_pattern_groups_by_slot(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)

        client._default_scan_itersize = 10
        client.key_func = lambda k, p, v: k

        # {a} -> slot 15495, {b} -> slot 3300
        mock_cluster.scan_iter.return_value = iter(
            [
                b"{a}key1",
                b"{a}key2",
                b"{b}key3",
            ],
        )
        mock_cluster.delete.return_value = 2  # First call deletes 2 keys
        mock_cluster.delete.side_effect = [2, 1]  # 2 keys in slot a, 1 in slot b

        result = client.delete_pattern("*")

        assert mock_cluster.delete.call_count == 2
        assert result == 3  # 2 + 1 = 3 total deleted

    def test_close_keeps_the_sync_cluster(self):
        """The sync cluster client is shared process-wide, so close() must leave it alone."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)
        client.get_client()

        client.close()

        assert len(client._clusters) == 1
        mock_cluster.close.assert_not_called()

    def test_close_drops_clusters_of_closed_loops(self):
        client = setup_cluster_client(MagicMock())
        loop = asyncio.new_event_loop()
        client._async_clusters[loop] = {("key",): AsyncMock()}
        loop.close()

        client.close()

        assert loop not in client._async_clusters

    @pytest.mark.asyncio
    async def test_aclose_closes_the_running_loops_cluster(self):
        mock_async_cluster = AsyncMock()
        client = setup_cluster_client(MagicMock())
        client._async_cluster_class = MagicMock()
        client._async_cluster_class.from_url.return_value = mock_async_cluster

        assert await client.get_async_client() is mock_async_cluster
        loop = asyncio.get_running_loop()
        assert len(client._async_clusters[loop]) == 1

        await client.aclose()

        assert loop not in client._async_clusters
        mock_async_cluster.aclose.assert_awaited_once()
