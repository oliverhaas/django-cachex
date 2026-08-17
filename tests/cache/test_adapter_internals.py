"""Mock-based unit tests for redis-py / valkey-py adapter internals that the
parametrized container fixtures cannot observe (registry keys, driver-callback
suppression, stub-shaped driver pipelines)."""

import weakref
from typing import Any

import pytest

from django_cachex.adapters.valkey_py import (
    ValkeyPyAdapter,
    ValkeyPyAsyncPipelineAdapter,
    ValkeyPyClusterAdapter,
    ValkeyPySentinelAdapter,
)

SERVER_URL = "rediss://user:secret@example.com:7000/0?socket_timeout=5"


class TestClusterClientConstruction:
    """get_client() must hand the full server URL to the driver's from_url()."""

    def test_get_client_builds_cluster_from_full_url(self, monkeypatch: pytest.MonkeyPatch):
        # Regression: only host/port were extracted from the URL; TLS scheme,
        # auth, db and query params were dropped on the floor.
        captured: dict[str, Any] = {}

        class StubCluster:
            @classmethod
            def from_url(cls, url: str, **kwargs: Any) -> StubCluster:
                captured["url"] = url
                captured["kwargs"] = kwargs
                return cls()

        monkeypatch.setattr(ValkeyPyClusterAdapter, "_cluster_class", StubCluster)
        monkeypatch.setattr(ValkeyPyClusterAdapter, "_clusters", {})
        adapter = ValkeyPyClusterAdapter.__new__(ValkeyPyClusterAdapter)
        adapter._servers = [SERVER_URL]
        adapter._options = {"socket_connect_timeout": 3}

        client = adapter.get_client()

        assert captured["url"] == SERVER_URL
        assert captured["kwargs"] == {"socket_connect_timeout": 3}
        assert isinstance(client, StubCluster)

    def test_get_client_shares_cluster_across_instances(self, monkeypatch: pytest.MonkeyPatch):
        class StubCluster:
            @classmethod
            def from_url(cls, url: str, **kwargs: Any) -> StubCluster:
                return cls()

        monkeypatch.setattr(ValkeyPyClusterAdapter, "_cluster_class", StubCluster)
        monkeypatch.setattr(ValkeyPyClusterAdapter, "_clusters", {})

        def make_adapter() -> ValkeyPyClusterAdapter:
            adapter = ValkeyPyClusterAdapter.__new__(ValkeyPyClusterAdapter)
            adapter._servers = [SERVER_URL]
            adapter._options = {}
            return adapter

        assert make_adapter().get_client() is make_adapter().get_client()

    @pytest.mark.asyncio
    async def test_get_async_client_builds_cluster_from_full_url(self, monkeypatch: pytest.MonkeyPatch):
        captured: dict[str, Any] = {}

        class StubAsyncCluster:
            @classmethod
            def from_url(cls, url: str, **kwargs: Any) -> StubAsyncCluster:
                captured["url"] = url
                captured["kwargs"] = kwargs
                return cls()

        monkeypatch.setattr(ValkeyPyClusterAdapter, "_async_cluster_class", StubAsyncCluster)
        monkeypatch.setattr(ValkeyPyClusterAdapter, "_async_clusters", weakref.WeakKeyDictionary())
        adapter = ValkeyPyClusterAdapter.__new__(ValkeyPyClusterAdapter)
        adapter._servers = [SERVER_URL]
        adapter._options = {"socket_connect_timeout": 3}

        client = await adapter.get_async_client()

        assert captured["url"] == SERVER_URL
        assert captured["kwargs"] == {"socket_connect_timeout": 3}
        assert isinstance(client, StubAsyncCluster)


class TestSentinelAsyncPoolRegistry:
    """The async sentinel pool registry must hit across adapter instances."""

    @pytest.mark.asyncio
    async def test_pool_shared_across_adapter_instances(self, monkeypatch: pytest.MonkeyPatch):
        # Regression: the registry key contained id(sentinel manager), rebuilt
        # per adapter instance, so every asgiref task leaked a fresh pool.
        created_pools: list[Any] = []

        class StubSentinelPool:
            @classmethod
            def from_url(cls, url: str, **kwargs: Any) -> StubSentinelPool:
                pool = cls()
                created_pools.append(pool)
                return pool

        class StubSentinel:
            def __init__(self, sentinels: Any, sentinel_kwargs: Any = None, **kwargs: Any) -> None:
                pass

        monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_sentinel_pool_class", StubSentinelPool)
        monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_sentinel_class", StubSentinel)
        monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_pools", weakref.WeakKeyDictionary())

        def make_adapter() -> ValkeyPySentinelAdapter:
            adapter = ValkeyPySentinelAdapter.__new__(ValkeyPySentinelAdapter)
            adapter._servers = ["redis://mymaster/0?is_master=1"]
            adapter._options = {"sentinels": [("localhost", 26379)]}
            adapter._pool_options = {"socket_timeout": 5}
            adapter._async_sentinels = weakref.WeakKeyDictionary()
            return adapter

        pool_one = make_adapter()._get_async_connection_pool(write=True)
        pool_two = make_adapter()._get_async_connection_pool(write=True)

        assert pool_one is pool_two
        assert len(created_pools) == 1

    @pytest.mark.asyncio
    async def test_pools_not_shared_across_sentinel_fleets(self, monkeypatch: pytest.MonkeyPatch):
        # Regression: the key omitted the fleet, so two caches on the same
        # service name but different sentinels shared one pool.
        class StubSentinelPool:
            @classmethod
            def from_url(cls, url: str, **kwargs: Any) -> StubSentinelPool:
                return cls()

        class StubSentinel:
            def __init__(self, sentinels: Any, sentinel_kwargs: Any = None, **kwargs: Any) -> None:
                pass

        monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_sentinel_pool_class", StubSentinelPool)
        monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_sentinel_class", StubSentinel)
        monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_pools", weakref.WeakKeyDictionary())

        def make_adapter(sentinels: list[Any], sentinel_kwargs: dict[str, Any]) -> ValkeyPySentinelAdapter:
            adapter = ValkeyPySentinelAdapter.__new__(ValkeyPySentinelAdapter)
            adapter._servers = ["redis://mymaster/0?is_master=1"]
            adapter._options = {"sentinels": sentinels, "sentinel_kwargs": sentinel_kwargs}
            adapter._pool_options = {"socket_timeout": 5}
            adapter._async_sentinels = weakref.WeakKeyDictionary()
            return adapter

        fleet_a = [("sentinel-a", 26379)]
        fleet_b = [("sentinel-b", 26379)]

        pool_a = make_adapter(fleet_a, {})._get_async_connection_pool(write=True)
        pool_b = make_adapter(fleet_b, {})._get_async_connection_pool(write=True)
        pool_a_again = make_adapter(fleet_a, {})._get_async_connection_pool(write=True)
        pool_a_other_password = make_adapter(fleet_a, {"password": "s3cret"})._get_async_connection_pool(
            write=True,
        )

        assert pool_a is not pool_b
        assert pool_a is not pool_a_other_password
        assert pool_a is pool_a_again


class TestAsyncPipelineAdapterReset:
    """reset() must discard buffered commands for every driver pipeline shape."""

    @pytest.mark.asyncio
    async def test_reset_awaits_coroutine_reset(self):
        class StubPipeline:
            def __init__(self) -> None:
                self.reset_calls = 0

            async def reset(self) -> None:
                self.reset_calls += 1

        raw = StubPipeline()
        await ValkeyPyAsyncPipelineAdapter(raw).reset()
        assert raw.reset_calls == 1

    @pytest.mark.asyncio
    async def test_reset_clears_stack_when_reset_is_a_server_command(self):
        # Regression: valkey's async ClusterPipeline has no reset(); the name
        # resolved to the RESET command and re-initialized the shared client.
        class StubClusterPipeline:
            """Shaped like valkey.asyncio.cluster.ClusterPipeline."""

            def __init__(self) -> None:
                self._command_stack: list[str] = ["queued-command"]
                self.initialized = False

            def reset(self) -> StubClusterPipeline:
                self._command_stack.append("RESET")
                return self

            def __await__(self) -> Any:
                async def _initialize() -> StubClusterPipeline:
                    self.initialized = True
                    self._command_stack = ["wiped-by-initialize"]
                    return self

                return _initialize().__await__()

        raw = StubClusterPipeline()
        await ValkeyPyAsyncPipelineAdapter(raw).reset()
        assert raw._command_stack == []
        assert not raw.initialized


class _JustidClient:
    """Driver-shaped stub: JUSTID replies collapse to a flat ID list unless a
    passthrough XAUTOCLAIM response callback is registered."""

    RAW_REPLY = (
        [b"5-1", [b"1-0", b"2-0"], [b"3-0"]],
        [b"1-0", b"2-0"],
    )

    def __init__(self) -> None:
        self.callbacks: dict[str, Any] = {}

    def set_response_callback(self, command: str, callback: Any) -> None:
        self.callbacks[command] = callback

    def _reply(self) -> Any:
        raw, parsed = self.RAW_REPLY
        callback = self.callbacks.get("XAUTOCLAIM")
        if callback is not None:
            return callback(raw, parse_justid=True)
        return parsed

    def xautoclaim(self, *args: Any, **kwargs: Any) -> Any:
        return self._reply()


class _AsyncJustidClient(_JustidClient):
    async def xautoclaim(self, *args: Any, **kwargs: Any) -> Any:
        return self._reply()


class TestXAutoclaimJustid:
    """justid=True must preserve the cursor and deleted IDs where possible."""

    def test_justid_preserves_cursor_and_deleted(self):
        # Regression: the driver-parsed flat ID list forced a "" cursor, so
        # callers could never resume iteration past the first page.
        client = _JustidClient()
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
        adapter.get_client = lambda key=None, *, write=False: client

        result = adapter.xautoclaim("stream", "group", "consumer", 0, justid=True)

        assert result == ("5-1", ["1-0", "2-0"], ["3-0"])

    @pytest.mark.asyncio
    async def test_async_justid_preserves_cursor_and_deleted(self):
        client = _AsyncJustidClient()
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)

        async def get_async_client(key: Any = None, *, write: bool = False) -> Any:
            return client

        adapter.get_async_client = get_async_client

        result = await adapter.axautoclaim("stream", "group", "consumer", 0, justid=True)

        assert result == ("5-1", ["1-0", "2-0"], ["3-0"])

    def test_cluster_justid_keeps_shared_client_untouched(self):
        # The cluster client is shared process-wide, so the adapter must not
        # override its response callbacks; the lossy "" cursor stays.
        class StubClusterClient:
            def set_response_callback(self, command: str, callback: Any) -> None:
                msg = "shared cluster client must not be mutated"
                raise AssertionError(msg)

            def xautoclaim(self, *args: Any, **kwargs: Any) -> Any:
                return [b"1-0", b"2-0"]

        client = StubClusterClient()
        adapter = ValkeyPyClusterAdapter.__new__(ValkeyPyClusterAdapter)
        adapter.get_client = lambda key=None, *, write=False: client

        result = adapter.xautoclaim("stream", "group", "consumer", 0, justid=True)

        assert result == ("", ["1-0", "2-0"], [])

    def test_non_justid_parses_entries(self):
        class StubClient:
            def set_response_callback(self, command: str, callback: Any) -> None:
                msg = "non-justid calls must not override driver callbacks"
                raise AssertionError(msg)

            def xautoclaim(self, *args: Any, **kwargs: Any) -> Any:
                return [b"0-0", [(b"1-0", {b"field": b"value"})], [b"2-0"]]

        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
        adapter.get_client = lambda key=None, *, write=False: StubClient()

        result = adapter.xautoclaim("stream", "group", "consumer", 0)

        # Field values stay raw at the adapter layer; the cache decodes them.
        assert result == ("0-0", [("1-0", {"field": b"value"})], ["2-0"])


# ---------------------------------------------- redis-rs protocol-stub gaps


def test_redis_rs_slowlog_raises_not_supported():
    """Regression: RespAdapterProtocol sits last in the MRO, so a command the
    Rust class does not implement resolved to the protocol's ``...`` body and
    returned None. slowlog_get/slowlog_len were the only two such gaps, and
    callers got ``TypeError: 'NoneType' object is not iterable`` instead of a
    catchable NotSupportedError.
    """
    from django_cachex.adapters.redis_rs import RedisRsAdapter
    from django_cachex.exceptions import NotSupportedError

    # The Rust __new__ demands a live server, so call the overrides unbound;
    # neither touches self.
    with pytest.raises(NotSupportedError):
        RedisRsAdapter.slowlog_get(None)
    with pytest.raises(NotSupportedError):
        RedisRsAdapter.slowlog_len(None)
