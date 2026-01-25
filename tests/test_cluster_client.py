"""Tests for RedisClusterCacheClient."""

from unittest.mock import MagicMock

from redis.cluster import RedisCluster, key_slot

from django_cachex.client import RedisClusterCacheClient


def setup_cluster_client(mock_cluster_cls=None):
    """Helper to create a properly configured RedisClusterCacheClient for testing."""
    client = RedisClusterCacheClient.__new__(RedisClusterCacheClient)
    client._servers = ["redis://localhost:7000"]
    client._options = {}
    client._clusters = {}
    # Set class attributes that are normally set via class definition
    if mock_cluster_cls is not None:
        client._cluster_class = mock_cluster_cls
    else:
        client._cluster_class = RedisCluster
    client._key_slot_func = key_slot
    return client


class TestRedisClusterCacheClient:
    """Tests for RedisClusterCacheClient."""

    def test_get_client_creates_cluster(self):
        """Test get_client creates a RedisCluster instance."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)

        result = client.get_client()

        assert result == mock_cluster
        mock_cluster_cls.assert_called_once()

    def test_get_client_caches_cluster(self):
        """Test get_client caches cluster instances."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)

        result1 = client.get_client()
        result2 = client.get_client()

        assert result1 is result2
        assert mock_cluster_cls.call_count == 1

    def test_group_keys_by_slot(self):
        """Test _group_keys_by_slot groups keys correctly."""
        client = setup_cluster_client()

        # Keys with same hash tag should be in same slot
        keys = ["{user}:1", "{user}:2", "{user}:3"]
        slots = client._group_keys_by_slot(keys)

        # All keys with {user} hash tag should be in the same slot
        assert len(slots) == 1
        slot_keys = list(slots.values())[0]
        assert len(slot_keys) == 3

    def test_group_keys_by_slot_different_slots(self):
        """Test _group_keys_by_slot separates keys in different slots."""
        client = setup_cluster_client()

        # Use hash tags that are guaranteed to hash to different slots:
        # {a} -> slot 15495, {b} -> slot 3300, {c} -> slot 7365
        keys = ["{a}key1", "{b}key2", "{c}key3"]
        slots = client._group_keys_by_slot(keys)

        # Should have exactly 3 slots (one per hash tag)
        assert len(slots) == 3
        total_keys = sum(len(v) for v in slots.values())
        assert total_keys == 3

    def test_get_many_uses_mget_nonatomic(self):
        """Test get_many uses mget_nonatomic for cross-slot keys."""
        import pickle

        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)
        client._serializers = [MagicMock()]
        client._compressors = [MagicMock()]
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        client.key_prefix = ""
        client.version = 1
        client.key_func = lambda k, p, v: k

        # Setup mocks for encoding/decoding
        client._serializers[0].loads.side_effect = lambda x: pickle.loads(x)
        client._compressors[0].decompress.side_effect = lambda x: x
        client._compressors[0].__class__.__name__ = "IdentityCompressor"

        # Mock mget_nonatomic response with pickled data
        mock_cluster.mget_nonatomic.return_value = [
            pickle.dumps("value_a"),
            pickle.dumps("value_b"),
            pickle.dumps("value_c"),
        ]

        # Using hash tags that hash to different slots
        result = client.get_many(["{a}key1", "{b}key2", "{c}key3"])

        # Should return decoded values
        assert len(result) == 3
        # mget_nonatomic is called once with all keys
        mock_cluster.mget_nonatomic.assert_called_once()
        assert "value_a" in result.values()
        assert "value_b" in result.values()
        assert "value_c" in result.values()

    def test_get_many_empty_keys(self):
        """Test get_many with empty keys list."""
        client = setup_cluster_client()
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None

        result = client.get_many([])
        assert result == {}

    def test_delete_many_groups_by_slot(self):
        """Test delete_many handles cross-slot keys."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        client.key_prefix = ""
        client.version = 1
        client.key_func = lambda k, p, v: k

        # Mock delete to return count of deleted keys per call
        mock_cluster.delete.return_value = 1

        # Using hash tags that hash to different slots:
        # {a} -> slot 15495, {b} -> slot 3300, {c} -> slot 7365
        client.delete_many(["{a}key1", "{b}key2", "{c}key3"])

        # delete should be called once per slot (3 different slots)
        assert mock_cluster.delete.call_count == 3

    def test_delete_many_empty_keys(self):
        """Test delete_many with empty keys list."""
        client = setup_cluster_client()
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None

        client.delete_many([])
        # Should not raise any errors

    def test_delete_many_same_slot(self):
        """Test delete_many with keys in the same slot uses single delete."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster

        client = setup_cluster_client(mock_cluster_cls)
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        client.key_prefix = ""
        client.version = 1
        client.key_func = lambda k, p, v: k

        # Mock delete to return count of deleted keys
        mock_cluster.delete.return_value = 3

        # All keys have same hash tag {user} -> same slot
        client.delete_many(["{user}key1", "{user}key2", "{user}key3"])

        # delete should be called only once (all keys in same slot)
        mock_cluster.delete.assert_called_once()

    def test_clear_flushes_all_primaries(self):
        """Test clear flushes all primary nodes."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None

        client.clear()

        # Should call flushdb with target_nodes=PRIMARIES
        mock_cluster.flushdb.assert_called_once_with(target_nodes="primaries")

    def test_keys_scans_all_primaries(self):
        """Test keys() returns keys from all primary nodes."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        client.key_prefix = "prefix"
        client.version = 1
        client.key_func = lambda k, p, v: f"{p}:{v}:{k}"
        client._reverse_key = lambda k: k.split(":")[-1]

        # Mock keys() to return keys from across the cluster
        mock_cluster.keys.return_value = [
            b"prefix:1:foo_1",
            b"prefix:1:foo_2",
            b"prefix:1:foo_3",
        ]

        result = client.keys("foo_*")

        # Should call keys with target_nodes=PRIMARIES
        mock_cluster.keys.assert_called_once()
        call_kwargs = mock_cluster.keys.call_args.kwargs
        assert call_kwargs.get("target_nodes") == "primaries"

        # Should return decoded keys
        assert len(result) == 3
        assert "foo_1" in result
        assert "foo_2" in result
        assert "foo_3" in result

    def test_keys_empty_result(self):
        """Test keys() with no matching keys."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        client.key_prefix = ""
        client.version = 1
        client.key_func = lambda k, p, v: k
        client._reverse_key = lambda k: k

        mock_cluster.keys.return_value = []

        result = client.keys("nonexistent_*")

        assert result == []
        mock_cluster.keys.assert_called_once()

    def test_iter_keys_scans_all_primaries(self):
        """Test iter_keys() iterates keys from all primary nodes."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        client.key_prefix = "prefix"
        client.version = 1
        client._default_scan_itersize = 10
        client.key_func = lambda k, p, v: f"{p}:{v}:{k}"
        client._reverse_key = lambda k: k.split(":")[-1]

        # Mock scan_iter to return keys from across the cluster
        mock_cluster.scan_iter.return_value = iter(
            [
                b"prefix:1:bar_1",
                b"prefix:1:bar_2",
                b"prefix:1:bar_3",
            ],
        )

        result = list(client.iter_keys("bar_*"))

        # Should call scan_iter with target_nodes=PRIMARIES
        mock_cluster.scan_iter.assert_called_once()
        call_kwargs = mock_cluster.scan_iter.call_args.kwargs
        assert call_kwargs.get("target_nodes") == "primaries"

        # Should return decoded keys
        assert len(result) == 3
        assert "bar_1" in result
        assert "bar_2" in result
        assert "bar_3" in result

    def test_iter_keys_with_itersize(self):
        """Test iter_keys() passes itersize to scan_iter."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        client.key_prefix = ""
        client.version = 1
        client._default_scan_itersize = 10
        client.key_func = lambda k, p, v: k
        client._reverse_key = lambda k: k

        mock_cluster.scan_iter.return_value = iter([])

        list(client.iter_keys("*", itersize=500))

        call_kwargs = mock_cluster.scan_iter.call_args.kwargs
        assert call_kwargs.get("count") == 500
        assert call_kwargs.get("target_nodes") == "primaries"

    def test_delete_pattern_deletes_across_primaries(self):
        """Test delete_pattern() deletes keys from all primary nodes."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        client.key_prefix = "prefix"
        client.version = 1
        client._default_scan_itersize = 10
        client.key_func = lambda k, p, v: f"{p}:{v}:{k}"

        # Mock scan_iter to return keys
        mock_cluster.scan_iter.return_value = iter(
            [
                b"prefix:1:temp_1",
                b"prefix:1:temp_2",
                b"prefix:1:temp_3",
            ],
        )
        mock_cluster.delete.return_value = 1

        result = client.delete_pattern("temp_*")

        # Should scan with target_nodes=PRIMARIES
        mock_cluster.scan_iter.assert_called_once()
        call_kwargs = mock_cluster.scan_iter.call_args.kwargs
        assert call_kwargs.get("target_nodes") == "primaries"

        # Should delete 3 keys (called once per slot group, but we'll verify total)
        assert result == 3

    def test_delete_pattern_empty_result(self):
        """Test delete_pattern() with no matching keys."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        client.key_prefix = ""
        client.version = 1
        client._default_scan_itersize = 10
        client.key_func = lambda k, p, v: k

        mock_cluster.scan_iter.return_value = iter([])

        result = client.delete_pattern("nonexistent_*")

        assert result == 0
        # delete should not be called if no keys found
        mock_cluster.delete.assert_not_called()

    def test_delete_pattern_groups_by_slot(self):
        """Test delete_pattern() groups keys by slot for deletion."""
        mock_cluster_cls = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster_cls.return_value = mock_cluster
        mock_cluster_cls.PRIMARIES = "primaries"

        client = setup_cluster_client(mock_cluster_cls)
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        client.key_prefix = ""
        client.version = 1
        client._default_scan_itersize = 10
        client.key_func = lambda k, p, v: k

        # Keys with different hash tags go to different slots
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

        # Should call delete twice (once per slot)
        assert mock_cluster.delete.call_count == 2
        assert result == 3  # 2 + 1 = 3 total deleted

    def test_close_closes_cluster(self):
        """Test close() closes the cluster connection when close_connection=True."""
        mock_cluster = MagicMock()

        # Create and populate instance
        client = setup_cluster_client()
        client._options = {"close_connection": True}
        client._clusters = {"redis://localhost:7000": mock_cluster}

        client.close()

        mock_cluster.close.assert_called_once()
        assert "redis://localhost:7000" not in client._clusters
