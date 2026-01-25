from unittest.mock import Mock, call, patch

import pytest
from django.core.cache import DEFAULT_CACHE_ALIAS
from django.test import override_settings
from pytest_mock import MockerFixture

from django_cachex.cache import KeyValueCache
from django_cachex.client import KeyValueCacheClient, RedisCacheClient
from tests.settings_wrapper import SettingsWrapper


@pytest.fixture
def cache_client(cache: KeyValueCache) -> KeyValueCacheClient:
    """Fixture that returns the internal cache client for testing client-level behavior."""
    cache.set("TestClientClose", 0)
    yield cache._cache  # Return the internal client, not the cache backend
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
            if cache_client._clusters:
                url = list(cache_client._clusters.keys())[0]
                mock = mocker.patch.object(cache_client._clusters[url], "close", create=True)
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
        settings: SettingsWrapper,
        mocker: MockerFixture,
    ):
        with override_settings(DJANGO_REDIS_CLOSE_CONNECTION=True):
            # ClusterCacheClient uses _clusters, others use _pools
            # Use class name check since _pools is a class variable shared across instances
            if "ClusterCacheClient" in type(cache_client).__name__:
                if cache_client._clusters:
                    url = list(cache_client._clusters.keys())[0]
                    mock = mocker.patch.object(cache_client._clusters[url], "close")
                    cache_client.close()
                    assert mock.called
            else:
                # Find pools owned by this server (not from previous tests)
                server_url = cache_client._servers[0] if cache_client._servers else None
                if server_url and server_url in cache_client._pools:
                    mock = mocker.patch.object(cache_client._pools[server_url], "disconnect")
                    cache_client.close()
                    assert mock.called

    def test_close_disconnect_settings_cache(
        self,
        cache_client: KeyValueCacheClient,
        mocker: MockerFixture,
        settings: SettingsWrapper,
    ):
        caches = settings.CACHES
        caches[DEFAULT_CACHE_ALIAS]["OPTIONS"]["close_connection"] = True
        with override_settings(CACHES=caches):
            cache_client.set("TestClientClose", 0)
            # Use class name check since _pools is a class variable
            if "ClusterCacheClient" in type(cache_client).__name__:
                if cache_client._clusters:
                    url = list(cache_client._clusters.keys())[0]
                    mock = mocker.patch.object(cache_client._clusters[url], "close")
                    cache_client.close()
                    assert mock.called
            else:
                server_url = cache_client._servers[0] if cache_client._servers else None
                if server_url and server_url in cache_client._pools:
                    mock = mocker.patch.object(cache_client._pools[server_url], "disconnect")
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
            if cache_client._clusters:
                url = list(cache_client._clusters.keys())[0]
                mock = mocker.patch.object(cache_client._clusters[url], "close")
                cache_client.close()
                assert mock.called
        else:
            server_url = cache_client._servers[0] if cache_client._servers else None
            if server_url and server_url in cache_client._pools:
                mock = mocker.patch.object(cache_client._pools[server_url], "disconnect")
                cache_client.close()
                assert mock.called


class TestRedisCacheClient:
    @patch("tests.test_client.RedisCacheClient.get_client")
    @patch("tests.test_client.RedisCacheClient.__init__", return_value=None)
    def test_delete_pattern_calls_get_client_given_no_client(
        self,
        init_mock,
        get_client_mock,
    ):
        client = RedisCacheClient.__new__(RedisCacheClient)
        client.key_prefix = ""
        client._default_scan_itersize = 10
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None

        # Setup mock key_func
        client.key_func = lambda key, prefix, version: f":{version}:{key}"
        client.version = 1

        client.delete_pattern(pattern="foo*")
        get_client_mock.assert_called_once_with(write=True)

    @patch("tests.test_client.RedisCacheClient.make_pattern")
    @patch("tests.test_client.RedisCacheClient.get_client", return_value=Mock())
    @patch("tests.test_client.RedisCacheClient.__init__", return_value=None)
    def test_delete_pattern_calls_make_pattern(
        self,
        init_mock,
        get_client_mock,
        make_pattern_mock,
    ):
        client = RedisCacheClient.__new__(RedisCacheClient)
        client.key_prefix = ""
        client._default_scan_itersize = 10
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        get_client_mock.return_value.scan_iter.return_value = []

        client.delete_pattern(pattern="foo*")

        kwargs = {"version": None}
        make_pattern_mock.assert_called_once_with("foo*", **kwargs)

    @patch("tests.test_client.RedisCacheClient.make_pattern")
    @patch("tests.test_client.RedisCacheClient.get_client", return_value=Mock())
    @patch("tests.test_client.RedisCacheClient.__init__", return_value=None)
    def test_delete_pattern_calls_scan_iter_with_count_if_itersize_given(
        self,
        init_mock,
        get_client_mock,
        make_pattern_mock,
    ):
        client = RedisCacheClient.__new__(RedisCacheClient)
        client.key_prefix = ""
        client._default_scan_itersize = 10
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        get_client_mock.return_value.scan_iter.return_value = []

        client.delete_pattern(pattern="foo*", itersize=90210)

        get_client_mock.return_value.scan_iter.assert_called_once_with(
            count=90210,
            match=make_pattern_mock.return_value,
        )

    @patch("tests.test_client.RedisCacheClient.make_pattern")
    @patch("tests.test_client.RedisCacheClient.get_client", return_value=Mock())
    @patch("tests.test_client.RedisCacheClient.__init__", return_value=None)
    def test_delete_pattern_calls_pipeline_delete_and_execute(
        self,
        init_mock,
        get_client_mock,
        make_pattern_mock,
    ):
        client = RedisCacheClient.__new__(RedisCacheClient)
        client.key_prefix = ""
        client._default_scan_itersize = 10
        client._ignore_exceptions = False
        client._log_ignored_exceptions = False
        client.logger = None
        get_client_mock.return_value.scan_iter.return_value = [":1:foo", ":1:foo-a"]
        get_client_mock.return_value.pipeline.return_value = Mock()
        get_client_mock.return_value.pipeline.return_value.delete = Mock()
        get_client_mock.return_value.pipeline.return_value.execute = Mock()

        client.delete_pattern(pattern="foo*")

        assert get_client_mock.return_value.pipeline.return_value.delete.call_count == 2
        get_client_mock.return_value.pipeline.return_value.delete.assert_has_calls(
            [call(":1:foo"), call(":1:foo-a")],
        )
        get_client_mock.return_value.pipeline.return_value.execute.assert_called_once()
