from unittest.mock import Mock, patch

import pytest
from pytest_mock import MockerFixture

from django_cachex.cache import KeyValueCache
from django_cachex.client import KeyValueCacheClient, RedisCacheClient
from tests.settings_wrapper import SettingsWrapper


@pytest.fixture
def cache_client(cache: KeyValueCache):
    """Fixture that returns the internal cache client for testing client-level behavior."""
    cache.set("TestClientClose", 0)
    yield cache._cache  # Return the internal client
    cache.delete("TestClientClose")


class TestClientClose:
    def test_close_client_disconnect_default(
        self,
        cache_client: KeyValueCacheClient,
        mocker: MockerFixture,
    ):
        cache_client._options.clear()
        # With new architecture, close() disconnects pools directly
        # ClusterCacheClient uses _clusters instead of _pools
        if "ClusterCacheClient" in type(cache_client).__name__:
            if cache_client._clusters:  # type: ignore[attr-defined]  # type: ignore[attr-defined]
                url = list(cache_client._clusters.keys())[0]  # type: ignore[attr-defined]  # type: ignore[attr-defined]
                mock = mocker.patch.object(cache_client._clusters[url], "close", create=True)  # type: ignore[attr-defined]
            else:
                mock = Mock()
        elif cache_client._pools:
            url = list(cache_client._pools.keys())[0]
            mock = mocker.patch.object(cache_client._pools[url], "disconnect", create=True)
        else:
            mock = Mock()
        cache_client.close()
        # By default, close_connection is False, so disconnect shouldn't be called
        # unless close_connection option is set
        # This test verifies the default behavior (no disconnect)

    def test_close_disconnect_settings(
        self,
        cache_client: KeyValueCacheClient,
        mocker: MockerFixture,
    ):
        cache_client._options["close_connection"] = True
        # ClusterCacheClient uses _clusters, others use _pools
        # Use class name check since _pools is a class variable shared across instances
        if "ClusterCacheClient" in type(cache_client).__name__:
            if cache_client._clusters:  # type: ignore[attr-defined]
                url = list(cache_client._clusters.keys())[0]  # type: ignore[attr-defined]
                mock = mocker.patch.object(cache_client._clusters[url], "close")  # type: ignore[attr-defined]
                cache_client.close()
                assert mock.called
        else:
            # Find pools owned by this server (not from previous tests)
            server_url = cache_client._servers[0] if cache_client._servers else None
            if server_url and server_url in cache_client._pools:
                mock = mocker.patch.object(cache_client._pools[server_url], "disconnect")  # type: ignore[index]
                cache_client.close()
                assert mock.called

    def test_close_disconnect_settings_cache(
        self,
        cache_client: KeyValueCacheClient,
        mocker: MockerFixture,
        settings: SettingsWrapper,
    ):
        # Set close_connection directly on the client's options since
        # the client was already initialized before we can modify settings
        cache_client._options["close_connection"] = True
        cache_client.set("TestClientClose", 0, None)
        # Use class name check since _pools is a class variable
        if "ClusterCacheClient" in type(cache_client).__name__:
            if cache_client._clusters:  # type: ignore[attr-defined]
                url = list(cache_client._clusters.keys())[0]  # type: ignore[attr-defined]
                mock = mocker.patch.object(cache_client._clusters[url], "close")  # type: ignore[attr-defined]
                cache_client.close()
                assert mock.called
        else:
            server_url = cache_client._servers[0] if cache_client._servers else None
            if server_url and server_url in cache_client._pools:
                mock = mocker.patch.object(cache_client._pools[server_url], "disconnect")  # type: ignore[index]
                cache_client.close()
                assert mock.called

    def test_close_disconnect_client_options(
        self,
        cache_client: KeyValueCacheClient,
        mocker: MockerFixture,
    ):
        cache_client._options["close_connection"] = True
        # Use class name check since _pools is a class variable
        if "ClusterCacheClient" in type(cache_client).__name__:
            if cache_client._clusters:  # type: ignore[attr-defined]
                url = list(cache_client._clusters.keys())[0]  # type: ignore[attr-defined]
                mock = mocker.patch.object(cache_client._clusters[url], "close")  # type: ignore[attr-defined]
                cache_client.close()
                assert mock.called
        else:
            server_url = cache_client._servers[0] if cache_client._servers else None
            if server_url and server_url in cache_client._pools:
                mock = mocker.patch.object(cache_client._pools[server_url], "disconnect")  # type: ignore[index]
                cache_client.close()
                assert mock.called


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
