from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest

from django_cachex.adapters import RedisPyAdapter, RespAdapterProtocol

if TYPE_CHECKING:
    from django_cachex.cache import RespCache


@pytest.fixture
def cache_client(cache: RespCache):
    """Fixture that returns the internal cache client for testing client-level behavior."""
    cache.set("TestClientClose", 0)
    yield cache.adapter  # Return the internal client
    cache.delete("TestClientClose")


class TestClientClose:
    def test_close_is_noop(self, cache_client: RespAdapterProtocol):
        """Test close() is a no-op — pools persist after close."""
        # Create a pool first by accessing it
        pool = cache_client._get_connection_pool(write=True)
        assert 0 in cache_client._pools

        # close() should NOT clear pools
        cache_client.close()
        assert 0 in cache_client._pools
        assert cache_client._pools[0] is pool


class TestRedisPyAdapter:
    @patch("tests.cache.test_client.RedisPyAdapter.get_client")
    @patch("tests.cache.test_client.RedisPyAdapter.__init__", return_value=None)
    def test_delete_pattern_calls_get_client_given_no_client(
        self,
        init_mock,
        get_client_mock,
    ):
        mock_client = Mock()
        mock_client.scan_iter.return_value = []
        get_client_mock.return_value = mock_client

        client = RedisPyAdapter.__new__(RedisPyAdapter)
        client._default_scan_itersize = 10

        client.delete_pattern(pattern="foo*")
        get_client_mock.assert_called_once_with(write=True)

    @patch("tests.cache.test_client.RedisPyAdapter.get_client")
    @patch("tests.cache.test_client.RedisPyAdapter.__init__", return_value=None)
    def test_delete_pattern_calls_scan_iter_with_pattern(
        self,
        init_mock,
        get_client_mock,
    ):
        mock_client = Mock()
        mock_client.scan_iter.return_value = []
        get_client_mock.return_value = mock_client

        client = RedisPyAdapter.__new__(RedisPyAdapter)
        client._default_scan_itersize = 10

        client.delete_pattern(pattern="prefix:1:foo*")

        mock_client.scan_iter.assert_called_once_with(
            count=10,
            match="prefix:1:foo*",
        )

    @patch("tests.cache.test_client.RedisPyAdapter.get_client")
    @patch("tests.cache.test_client.RedisPyAdapter.__init__", return_value=None)
    def test_delete_pattern_calls_scan_iter_with_count_if_itersize_given(
        self,
        init_mock,
        get_client_mock,
    ):
        mock_client = Mock()
        mock_client.scan_iter.return_value = []
        get_client_mock.return_value = mock_client

        client = RedisPyAdapter.__new__(RedisPyAdapter)
        client._default_scan_itersize = 10

        client.delete_pattern(pattern="prefix:1:foo*", itersize=90210)

        mock_client.scan_iter.assert_called_once_with(
            count=90210,
            match="prefix:1:foo*",
        )

    @patch("tests.cache.test_client.RedisPyAdapter.get_client")
    @patch("tests.cache.test_client.RedisPyAdapter.__init__", return_value=None)
    def test_delete_pattern_deletes_found_keys(
        self,
        init_mock,
        get_client_mock,
    ):
        mock_client = Mock()
        mock_client.scan_iter.return_value = [":1:foo", ":1:foo-a"]
        mock_client.delete.return_value = 2
        get_client_mock.return_value = mock_client

        client = RedisPyAdapter.__new__(RedisPyAdapter)
        client._default_scan_itersize = 10

        result = client.delete_pattern(pattern="prefix:1:foo*")

        # delete is called once with all keys
        mock_client.delete.assert_called_once_with(":1:foo", ":1:foo-a")
        assert result == 2
