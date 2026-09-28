from django_cachex.adapters import RedisPyAdapter


def test_delete_pattern_calls_get_client_given_no_client(mocker):
    mocker.patch("django_cachex.adapters.redis_py.RedisPyAdapter.__init__", return_value=None)
    get_client_mock = mocker.patch("django_cachex.adapters.redis_py.RedisPyAdapter.get_client")
    mock_client = mocker.Mock()
    mock_client.scan_iter.return_value = []
    get_client_mock.return_value = mock_client

    client = RedisPyAdapter.__new__(RedisPyAdapter)
    client._default_scan_itersize = 10

    client.delete_pattern(pattern="foo*")
    get_client_mock.assert_called_once_with(write=True)


def test_delete_pattern_calls_scan_iter_with_pattern(mocker):
    mocker.patch("django_cachex.adapters.redis_py.RedisPyAdapter.__init__", return_value=None)
    get_client_mock = mocker.patch("django_cachex.adapters.redis_py.RedisPyAdapter.get_client")
    mock_client = mocker.Mock()
    mock_client.scan_iter.return_value = []
    get_client_mock.return_value = mock_client

    client = RedisPyAdapter.__new__(RedisPyAdapter)
    client._default_scan_itersize = 10

    client.delete_pattern(pattern="prefix:1:foo*")

    mock_client.scan_iter.assert_called_once_with(
        count=10,
        match="prefix:1:foo*",
    )


def test_delete_pattern_calls_scan_iter_with_count_if_itersize_given(mocker):
    mocker.patch("django_cachex.adapters.redis_py.RedisPyAdapter.__init__", return_value=None)
    get_client_mock = mocker.patch("django_cachex.adapters.redis_py.RedisPyAdapter.get_client")
    mock_client = mocker.Mock()
    mock_client.scan_iter.return_value = []
    get_client_mock.return_value = mock_client

    client = RedisPyAdapter.__new__(RedisPyAdapter)
    client._default_scan_itersize = 10

    client.delete_pattern(pattern="prefix:1:foo*", itersize=90210)

    mock_client.scan_iter.assert_called_once_with(
        count=90210,
        match="prefix:1:foo*",
    )


def test_delete_pattern_deletes_found_keys(mocker):
    mocker.patch("django_cachex.adapters.redis_py.RedisPyAdapter.__init__", return_value=None)
    get_client_mock = mocker.patch("django_cachex.adapters.redis_py.RedisPyAdapter.get_client")
    mock_client = mocker.Mock()
    mock_client.scan_iter.return_value = [":1:foo", ":1:foo-a"]
    mock_client.unlink.return_value = 2
    get_client_mock.return_value = mock_client

    client = RedisPyAdapter.__new__(RedisPyAdapter)
    client._default_scan_itersize = 10

    result = client.delete_pattern(pattern="prefix:1:foo*")

    mock_client.unlink.assert_called_once_with(":1:foo", ":1:foo-a")
    assert result == 2
