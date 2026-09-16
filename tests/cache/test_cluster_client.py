"""Tests for RedisPyClusterAdapter."""

import asyncio
import pickle
import threading
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.cluster import RedisCluster

from django_cachex.adapters import RedisPyClusterAdapter
from django_cachex.cache import RedisClusterCache
from django_cachex.exceptions import NotSupportedError


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

    def test_get_many_uses_mget_nonatomic(self):
        """Test get_many uses mget_nonatomic for cross-slot keys."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)

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

    def test_delete_many_is_one_unlink(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)

        mock_cluster.unlink.return_value = 3

        client.delete_many(["{a}key1", "{b}key2", "{c}key3"])

        mock_cluster.unlink.assert_called_once_with("{a}key1", "{b}key2", "{c}key3")

    def test_delete_many_empty_keys(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)

        client.delete_many([])

        mock_cluster.unlink.assert_not_called()

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

        mock_cluster.scan_iter.return_value = iter(
            [
                b"prefix:1:temp_1",
                b"prefix:1:temp_2",
                b"prefix:1:temp_3",
            ],
        )
        mock_cluster.unlink.return_value = 3

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

        mock_cluster.scan_iter.return_value = iter([])

        result = client.delete_pattern("nonexistent_*")

        assert result == 0
        mock_cluster.unlink.assert_not_called()

    def test_delete_pattern_is_one_unlink_per_batch(self):
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.from_url.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)

        client._default_scan_itersize = 10

        mock_cluster.scan_iter.return_value = iter(
            [
                b"{a}key1",
                b"{a}key2",
                b"{b}key3",
            ],
        )
        mock_cluster.unlink.return_value = 3

        result = client.delete_pattern("*")

        mock_cluster.unlink.assert_called_once_with(b"{a}key1", b"{a}key2", b"{b}key3")
        assert result == 3

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

        assert client._async_clusters[loop] == {}
        mock_async_cluster.aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aclose_leaves_another_aliases_cluster_open(self):
        # Regression: the whole loop slot was popped, so closing one alias
        # dropped the cluster client another alias was still using.
        registry: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
        cluster_class = MagicMock()
        clusters = [AsyncMock(), AsyncMock()]
        clients = []
        for cluster, port in zip(clusters, (7000, 7001), strict=True):
            client = setup_cluster_client(cluster_class)
            client._servers = [f"redis://localhost:{port}"]
            client._async_clusters = registry
            client._async_cluster_class = MagicMock()
            client._async_cluster_class.from_url.return_value = cluster
            clients.append(client)

        for client in clients:
            await client.get_async_client()
        await clients[0].aclose()

        clusters[0].aclose.assert_awaited_once()
        clusters[1].aclose.assert_not_awaited()
        assert await clients[1].get_async_client() is clusters[1]


def _version_in_tag(key: str, key_prefix: str, version: int) -> str:
    """A KEY_FUNCTION that hash-tags the version, so v1 and v2 of a key land in different slots."""
    return f"{{{key_prefix}:{version}}}:{key}"


def setup_cluster_cache(**params):
    """Build a RedisClusterCache over a mock adapter, so the hash-tag check runs without a cluster."""
    cache = RedisClusterCache("redis://localhost:7000", params)
    cache.__dict__["adapter"] = MagicMock(rename=MagicMock(), arename=AsyncMock())
    return cache


class TestClusterVersionRename:
    """incr_version/decr_version only RENAME when both versions share a hash tag."""

    def test_incr_version_renames_inside_the_tag(self):
        cache = setup_cluster_cache()
        assert cache.incr_version("{user}:k") == 2
        cache.adapter.rename.assert_called_once_with(":1:{user}:k", ":2:{user}:k")

    def test_key_prefix_tag_colocates_versions(self):
        cache = setup_cluster_cache(KEY_PREFIX="{app}")
        assert cache.incr_version("k", delta=2, version=3) == 5
        cache.adapter.rename.assert_called_once_with("{app}:3:k", "{app}:5:k")

    def test_incr_version_without_tag_is_rejected(self):
        cache = setup_cluster_cache()
        with pytest.raises(NotSupportedError, match="hash tag"):
            cache.incr_version("plain")
        cache.adapter.rename.assert_not_called()

    def test_tag_that_includes_the_version_is_rejected(self):
        # Regression: any {...} in the made key used to pass, but "{:1}:k" and
        # "{:2}:k" hash to different slots, so the RENAME failed with CROSSSLOT.
        cache = setup_cluster_cache(KEY_FUNCTION=_version_in_tag)
        with pytest.raises(NotSupportedError, match=r"'\{:1\}:k' and '\{:2\}:k'"):
            cache.incr_version("k")
        cache.adapter.rename.assert_not_called()

    def test_decr_version_error_names_decr_version(self):
        cache = setup_cluster_cache()
        with pytest.raises(NotSupportedError, match="decr_version"):
            cache.decr_version("plain")
        cache.adapter.rename.assert_not_called()

    @pytest.mark.asyncio
    async def test_aincr_version_checks_both_versions(self):
        cache = setup_cluster_cache(KEY_FUNCTION=_version_in_tag)
        with pytest.raises(NotSupportedError, match="hash tag"):
            await cache.aincr_version("k")
        cache.adapter.arename.assert_not_awaited()

        cache = setup_cluster_cache()
        assert await cache.adecr_version("{user}:k", version=2) == 1
        cache.adapter.arename.assert_awaited_once_with(":2:{user}:k", ":1:{user}:k")
