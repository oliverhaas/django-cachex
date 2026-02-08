from unittest.mock import Mock, patch

import pytest

from django_cachex.cache import KeyValueCache
from django_cachex.client import KeyValueCacheClient, RedisCacheClient


@pytest.fixture
def cache_client(cache: KeyValueCache):
    """Fixture that returns the internal cache client for testing client-level behavior."""
    cache.set("TestClientClose", 0)
    yield cache._cache  # Return the internal client
    cache.delete("TestClientClose")


class TestClientClose:
    def test_close_is_noop(self, cache_client: KeyValueCacheClient):
        """Test close() is a no-op — pools persist after close."""
        # Create a pool first by accessing it
        pool = cache_client._get_connection_pool(write=True)
        assert 0 in cache_client._pools

        # close() should NOT clear pools
        cache_client.close()
        assert 0 in cache_client._pools
        assert cache_client._pools[0] is pool


class TestRedisCacheClient:
    @patch("tests.cache.test_client.RedisCacheClient.get_client")
    @patch("tests.cache.test_client.RedisCacheClient.__init__", return_value=None)
    def test_delete_pattern_calls_get_client_given_no_client(
        self,
        init_mock,
        get_client_mock,
    ):
        mock_client = Mock()
        mock_client.scan_iter.return_value = []
        get_client_mock.return_value = mock_client

        client = RedisCacheClient.__new__(RedisCacheClient)
        client._default_scan_itersize = 10
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None

        client.delete_pattern(pattern="foo*")
        get_client_mock.assert_called_once_with(write=True)

    @patch("tests.cache.test_client.RedisCacheClient.get_client")
    @patch("tests.cache.test_client.RedisCacheClient.__init__", return_value=None)
    def test_delete_pattern_calls_scan_iter_with_pattern(
        self,
        init_mock,
        get_client_mock,
    ):
        """Test that delete_pattern passes the pattern directly to scan_iter."""
        mock_client = Mock()
        mock_client.scan_iter.return_value = []
        get_client_mock.return_value = mock_client

        client = RedisCacheClient.__new__(RedisCacheClient)
        client._default_scan_itersize = 10
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None

        client.delete_pattern(pattern="prefix:1:foo*")

        mock_client.scan_iter.assert_called_once_with(
            count=10,
            match="prefix:1:foo*",
        )

    @patch("tests.cache.test_client.RedisCacheClient.get_client")
    @patch("tests.cache.test_client.RedisCacheClient.__init__", return_value=None)
    def test_delete_pattern_calls_scan_iter_with_count_if_itersize_given(
        self,
        init_mock,
        get_client_mock,
    ):
        mock_client = Mock()
        mock_client.scan_iter.return_value = []
        get_client_mock.return_value = mock_client

        client = RedisCacheClient.__new__(RedisCacheClient)
        client._default_scan_itersize = 10
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None

        client.delete_pattern(pattern="prefix:1:foo*", itersize=90210)

        mock_client.scan_iter.assert_called_once_with(
            count=90210,
            match="prefix:1:foo*",
        )

    @patch("tests.cache.test_client.RedisCacheClient.get_client")
    @patch("tests.cache.test_client.RedisCacheClient.__init__", return_value=None)
    def test_delete_pattern_deletes_found_keys(
        self,
        init_mock,
        get_client_mock,
    ):
        """Test that delete_pattern deletes all keys found by scan_iter."""
        mock_client = Mock()
        mock_client.scan_iter.return_value = [":1:foo", ":1:foo-a"]
        mock_client.delete.return_value = 2
        get_client_mock.return_value = mock_client

        client = RedisCacheClient.__new__(RedisCacheClient)
        client._default_scan_itersize = 10
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None

        result = client.delete_pattern(pattern="prefix:1:foo*")

        # delete is called once with all keys
        mock_client.delete.assert_called_once_with(":1:foo", ":1:foo-a")
        assert result == 2
