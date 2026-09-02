from unittest.mock import Mock, patch

from django_cachex.adapters import RedisPyAdapter


class TestRedisPyAdapter:
    @patch("django_cachex.adapters.redis_py.RedisPyAdapter.get_client")
    @patch("django_cachex.adapters.redis_py.RedisPyAdapter.__init__", return_value=None)
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

    @patch("django_cachex.adapters.redis_py.RedisPyAdapter.get_client")
    @patch("django_cachex.adapters.redis_py.RedisPyAdapter.__init__", return_value=None)
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

    @patch("django_cachex.adapters.redis_py.RedisPyAdapter.get_client")
    @patch("django_cachex.adapters.redis_py.RedisPyAdapter.__init__", return_value=None)
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

    @patch("django_cachex.adapters.redis_py.RedisPyAdapter.get_client")
    @patch("django_cachex.adapters.redis_py.RedisPyAdapter.__init__", return_value=None)
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

        mock_client.delete.assert_called_once_with(":1:foo", ":1:foo-a")
        assert result == 2
