"""Mock-based unit tests for redis-py / valkey-py adapter internals that the
parametrized container fixtures cannot observe (registry keys, driver-callback
suppression, stub-shaped driver pipelines)."""

import gc
import importlib
import weakref
from typing import Any

import pytest
from django.core.exceptions import ImproperlyConfigured

from django_cachex.adapters.redis_py import RedisPySentinelAdapter
from django_cachex.adapters.valkey_py import (
    _VALKEY_AVAILABLE,
    ValkeyPyAdapter,
    ValkeyPyAsyncPipelineAdapter,
    ValkeyPyClusterAdapter,
    ValkeyPyPipelineAdapter,
    ValkeyPySentinelAdapter,
    _options_key,
)
from django_cachex.exceptions import NotSupportedError, WrongTypeError, translate_server_error
from django_cachex.types import KeyType

SERVER_URL = "rediss://user:secret@example.com:7000/0?socket_timeout=5"

requires_valkey = pytest.mark.skipif(not _VALKEY_AVAILABLE, reason="valkey-py is not installed")


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
        adapter._get_connection_pool = lambda *, write=False: None
        adapter._new_client = lambda pool: client

        result = adapter.xautoclaim("stream", "group", "consumer", 0, justid=True)

        assert result == ("5-1", ["1-0", "2-0"], ["3-0"])

    def test_justid_does_not_mutate_the_pooled_client(self):
        # get_client() hands out a client shared by every other operation, so
        # the XAUTOCLAIM callback override must land on a throwaway one.
        pooled = _JustidClient()
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
        adapter._get_connection_pool = lambda *, write=False: None
        adapter._new_client = lambda pool: _JustidClient()
        adapter.get_client = lambda key=None, *, write=False: pooled

        adapter.xautoclaim("stream", "group", "consumer", 0, justid=True)

        assert pooled.callbacks == {}

    @pytest.mark.asyncio
    async def test_async_justid_preserves_cursor_and_deleted(self):
        client = _AsyncJustidClient()
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
        adapter._get_async_connection_pool = lambda *, write=False: None
        adapter._new_async_client = lambda pool: client

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


class _StubPool:
    """Stand-in for a driver connection pool (weak-referenceable, hashable)."""


class _StubClient:
    def __init__(self, connection_pool: Any) -> None:
        self.connection_pool = connection_pool


def _pooled_adapter(pool: Any) -> ValkeyPyAdapter:
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    adapter._client_class = _StubClient
    adapter._async_client_class = _StubClient
    adapter._get_connection_pool = lambda *, write=False: pool
    adapter._get_async_connection_pool = lambda *, write=False: pool
    return adapter


class TestClientCaching:
    """One client per pool, not one per command."""

    def test_get_client_reuses_one_client_per_pool(self):
        # Regression: every cache operation built a fresh client whose
        # WRONGTYPE patch made it uncollectable cyclic garbage.
        adapter = _pooled_adapter(_StubPool())

        assert adapter.get_client("key") is adapter.get_client("key", write=True)

    def test_get_client_builds_one_client_per_distinct_pool(self):
        pools = [_StubPool(), _StubPool()]
        adapter = _pooled_adapter(pools[0])
        adapter._get_connection_pool = lambda *, write: pools[0] if write else pools[1]

        assert adapter.get_client("key", write=True) is not adapter.get_client("key", write=False)

    def test_clients_are_shared_across_adapter_instances(self):
        # asgiref hands each task its own adapter; the client hangs off the
        # pool, so per-task instances still land on the same client.
        pool = _StubPool()

        assert _pooled_adapter(pool).get_client() is _pooled_adapter(pool).get_client()

    def test_client_dies_with_its_pool(self):
        # A pool-keyed registry would hold the client strongly and the client
        # holds the pool, so dead loops' pools would never be freed.
        adapter = _pooled_adapter(_StubPool())
        client_ref = weakref.ref(adapter.get_client())

        del adapter
        gc.collect()

        assert client_ref() is None

    @pytest.mark.asyncio
    async def test_get_async_client_reuses_one_client_per_pool(self):
        adapter = _pooled_adapter(_StubPool())

        assert await adapter.get_async_client("key") is await adapter.get_async_client("key", write=True)

    def test_new_client_is_never_the_pooled_one(self):
        pool = _StubPool()
        adapter = _pooled_adapter(pool)

        assert adapter._new_client(pool) is not adapter.get_client()


class _PopClient:
    """Driver stub whose LPOP/RPOP hand back a canned reply."""

    def __init__(self, reply: Any) -> None:
        self.reply = reply

    def lpop(self, key: str, count: int | None = None) -> Any:
        return self.reply

    rpop = lpop


class _AsyncPopClient(_PopClient):
    async def lpop(self, key: str, count: int | None = None) -> Any:  # type: ignore[override]
        return self.reply

    arpop = lpop
    rpop = lpop


def _pop_adapter(client: Any) -> ValkeyPyAdapter:
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    adapter.get_client = lambda key=None, *, write=False: client

    async def get_async_client(key: Any = None, *, write: bool = False) -> Any:
        return client

    adapter.get_async_client = get_async_client
    return adapter


class TestCountFormPopMissingKey:
    """A nil reply is a missing key, not an empty pop."""

    @pytest.mark.parametrize("method", ["lpop", "rpop"])
    def test_missing_key_returns_none(self, method: str):
        adapter = _pop_adapter(_PopClient(None))
        assert getattr(adapter, method)("missing", count=2) is None

    @pytest.mark.parametrize("method", ["lpop", "rpop"])
    def test_empty_array_stays_an_empty_list(self, method: str):
        adapter = _pop_adapter(_PopClient([]))
        assert getattr(adapter, method)("key", count=2) == []

    @pytest.mark.parametrize("method", ["alpop", "arpop"])
    @pytest.mark.asyncio
    async def test_async_missing_key_returns_none(self, method: str):
        adapter = _pop_adapter(_AsyncPopClient(None))
        assert await getattr(adapter, method)("missing", count=2) is None

    @pytest.mark.parametrize("method", ["alpop", "arpop"])
    @pytest.mark.asyncio
    async def test_async_empty_array_stays_an_empty_list(self, method: str):
        adapter = _pop_adapter(_AsyncPopClient([]))
        assert await getattr(adapter, method)("key", count=2) == []


class TestHmgetWithoutFields:
    """``HMGET key`` with no fields is a wire-level syntax error."""

    def test_sync_hmget_returns_empty_without_touching_the_client(self):
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)

        def unreachable(*args: Any, **kwargs: Any) -> Any:
            msg = "hmget() with no fields must not reach the server"
            raise AssertionError(msg)

        adapter.get_client = unreachable
        assert adapter.hmget("key") == []

    @pytest.mark.asyncio
    async def test_async_hmget_returns_empty_without_touching_the_client(self):
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)

        async def unreachable(*args: Any, **kwargs: Any) -> Any:
            msg = "ahmget() with no fields must not reach the server"
            raise AssertionError(msg)

        adapter.get_async_client = unreachable
        assert await adapter.ahmget("key") == []


class _XPendingClient:
    def __init__(self) -> None:
        self.range_kwargs: dict[str, Any] | None = None
        self.summary_calls = 0

    def xpending_range(self, key: str, group: str, **kwargs: Any) -> Any:
        self.range_kwargs = kwargs
        return []

    def xpending(self, key: str, group: str) -> Any:
        self.summary_calls += 1
        return {"pending": 0}


class _AsyncXPendingClient(_XPendingClient):
    async def xpending_range(self, key: str, group: str, **kwargs: Any) -> Any:  # type: ignore[override]
        self.range_kwargs = kwargs
        return []

    async def xpending(self, key: str, group: str) -> Any:  # type: ignore[override]
        self.summary_calls += 1
        return {"pending": 0}


class TestXPendingArguments:
    """Range and filter arguments must not be dropped on the floor."""

    @pytest.mark.parametrize(
        "kwargs",
        [{"start": "-"}, {"end": "+"}, {"start": "-", "end": "+"}, {"consumer": "c"}, {"idle": 100}],
    )
    def test_filters_without_count_raise(self, kwargs: dict[str, Any]):
        adapter = _pop_adapter(_XPendingClient())
        with pytest.raises(ValueError, match="xpending\\(\\) requires count"):
            adapter.xpending("stream", "group", **kwargs)

    def test_summary_form_still_works(self):
        client = _XPendingClient()
        adapter = _pop_adapter(client)

        assert adapter.xpending("stream", "group") == {"pending": 0}
        assert client.summary_calls == 1

    def test_count_alone_scans_the_whole_range(self):
        client = _XPendingClient()
        adapter = _pop_adapter(client)

        adapter.xpending("stream", "group", count=10)

        assert client.range_kwargs is not None
        assert client.range_kwargs["min"] == "-"
        assert client.range_kwargs["max"] == "+"

    @pytest.mark.asyncio
    async def test_async_filters_without_count_raise(self):
        adapter = _pop_adapter(_AsyncXPendingClient())
        with pytest.raises(ValueError, match="xpending\\(\\) requires count"):
            await adapter.axpending("stream", "group", consumer="c")

    @pytest.mark.asyncio
    async def test_async_count_alone_scans_the_whole_range(self):
        client = _AsyncXPendingClient()
        adapter = _pop_adapter(client)

        await adapter.axpending("stream", "group", count=10)

        assert client.range_kwargs is not None
        assert client.range_kwargs["min"] == "-"
        assert client.range_kwargs["max"] == "+"


_STREAM_ENTRIES = [(b"1-0", {b"field": b"value"})]
_DECODED_STREAM = {"stream": [("1-0", {"field": b"value"})]}

# What the server sends for XREAD under RESP3, before the driver parses it.
_RESP3_XREAD_REPLY = {b"stream": [[b"1-0", [b"field", b"value"]]]}


def _resp3_xread_parsers() -> list[Any]:
    from redis._parsers.helpers import parse_xread_resp3 as redis_parse

    parsers = [redis_parse]
    if _VALKEY_AVAILABLE:
        from valkey._parsers.helpers import parse_xread_resp3 as valkey_parse

        parsers.append(valkey_parse)
    return parsers


class TestDecodeStreamResults:
    """xread/xreadgroup replies arrive as pairs on RESP2 and a map on RESP3."""

    def test_resp2_pair_list(self):
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
        assert adapter._decode_stream_results([(b"stream", _STREAM_ENTRIES)]) == _DECODED_STREAM

    @pytest.mark.parametrize("parse_xread_resp3", _resp3_xread_parsers())
    def test_resp3_mapping(self, parse_xread_resp3: Any):
        # Regression: under OPTIONS {"protocol": 3} the driver returns
        # {stream: [entries]}, and reading it as {stream: entries} raised.
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)

        assert adapter._decode_stream_results(parse_xread_resp3(_RESP3_XREAD_REPLY)) == _DECODED_STREAM

    @pytest.mark.parametrize("parse_xread_resp3", _resp3_xread_parsers())
    def test_resp3_mapping_with_several_entries(self, parse_xread_resp3: Any):
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
        reply = {b"stream": [[b"1-0", [b"field", b"one"]], [b"2-0", [b"field", b"two"]]]}

        assert adapter._decode_stream_results(parse_xread_resp3(reply)) == {
            "stream": [("1-0", {"field": b"one"}), ("2-0", {"field": b"two"})],
        }

    def test_resp3_empty_stream(self):
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
        assert adapter._decode_stream_results({b"stream": []}) == {"stream": []}


class TestDecodeStreamEntries:
    """A nil entry must not take the whole reply down."""

    def test_nil_entry_decodes_to_empty_fields(self):
        # Redis 6 XCLAIM answers nil for a pending id that has been XDEL'd,
        # and the drivers' parse_stream_list turns that into (None, None).
        adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)

        assert adapter._decode_stream_entries([(None, None), (b"1-0", {b"f": b"v"})]) == [
            (None, {}),
            ("1-0", {"f": b"v"}),
        ]


class _ResponseError(Exception):
    """Stands in for the driver's ResponseError, which cachex matches by message."""


class _WrongTypePipeline:
    """Driver pipeline stub that fails the way a WRONGTYPE batch does."""

    ERROR = "WRONGTYPE Operation against a key holding the wrong kind of value"

    def __init__(self, error: Exception | None = None) -> None:
        self._error = error or _ResponseError(self.ERROR)

    def execute(self) -> Any:
        raise self._error

    def execute_command(self, *args: Any) -> Any:
        raise self._error


class _AsyncWrongTypePipeline(_WrongTypePipeline):
    async def execute(self) -> Any:  # type: ignore[override]
        raise self._error


class TestPipelineWrongTypeTranslation:
    """Pipelines are fresh driver objects, so they need their own translation."""

    def test_execute_raises_wrongtype_error(self):
        # Regression: the client-instance patch never reached the pipeline, so
        # a batched type error surfaced as the raw driver ResponseError.
        pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline())
        with pytest.raises(WrongTypeError):
            pipeline.execute()

    def test_execute_command_raises_wrongtype_error(self):
        pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline())
        with pytest.raises(WrongTypeError):
            pipeline.execute_command("LPUSH", "key", "value")

    def test_other_errors_pass_through_untouched(self):
        original = _ResponseError("ERR syntax error")
        pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline(original))
        with pytest.raises(_ResponseError) as excinfo:
            pipeline.execute()
        assert excinfo.value is original

    @pytest.mark.asyncio
    async def test_async_execute_raises_wrongtype_error(self):
        pipeline = ValkeyPyAsyncPipelineAdapter(_AsyncWrongTypePipeline())
        with pytest.raises(WrongTypeError):
            await pipeline.execute()


class TestUnknownCommandTranslation:
    """An unknown-command reply means the server predates the command, so it becomes NotSupportedError.

    The cluster client raises its own lookup error before anything reaches
    the wire, while resolving the routing key from the server's COMMAND table.
    """

    SERVER_REPLY = "unknown command 'HEXPIRE', with args beginning with: 'k' '10' 'FIELDS' '1' 'f' "
    CLUSTER_LOOKUP = "HSETEX command doesn't exist in Redis commands"

    def test_server_reply_becomes_not_supported(self):
        original = _ResponseError(self.SERVER_REPLY)
        pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline(original))
        with pytest.raises(NotSupportedError) as excinfo:
            pipeline.execute()
        assert excinfo.value.operation == "hexpire"
        assert excinfo.value.__cause__ is original
        assert "requires Redis 7.4+ or Valkey 9.0+" in str(excinfo.value)

    def test_execute_command_translates_too(self):
        pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline(_ResponseError(self.SERVER_REPLY)))
        with pytest.raises(NotSupportedError):
            pipeline.execute_command("HEXPIRE", "k", 10, "FIELDS", 1, "f")

    def test_cluster_command_lookup_becomes_not_supported(self):
        original = _ResponseError(self.CLUSTER_LOOKUP)
        pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline(original))
        with pytest.raises(NotSupportedError) as excinfo:
            pipeline.execute()
        assert excinfo.value.operation == "hsetex"
        assert "requires Redis 8.0+ or Valkey 9.0+" in str(excinfo.value)

    def test_redis_6_backtick_quoting(self):
        wrapped = translate_server_error(_ResponseError("unknown command `FOOBAR`, with args beginning with: "))
        assert isinstance(wrapped, NotSupportedError)
        assert wrapped.operation == "foobar"
        assert wrapped.backend is None
        assert wrapped.detail == "the server does not know this command"

    @pytest.mark.asyncio
    async def test_async_execute_raises_not_supported(self):
        pipeline = ValkeyPyAsyncPipelineAdapter(_AsyncWrongTypePipeline(_ResponseError(self.SERVER_REPLY)))
        with pytest.raises(NotSupportedError):
            await pipeline.execute()


class _Retry:
    """Stand-in for a driver Retry object rebuilt per cache instance."""

    def __init__(self, retries: int) -> None:
        self.retries = retries


class _SlottedRetry:
    __slots__ = ("retries",)

    def __init__(self, retries: int) -> None:
        self.retries = retries


class TestOptionsKeyStability:
    """Pool keys must not vary with object identity."""

    @pytest.mark.parametrize("factory", [_Retry, _SlottedRetry])
    def test_equal_objects_produce_equal_keys(self, factory: Any):
        # Regression: repr() of a plain object embeds its id(), so a Retry
        # rebuilt per instance opened a brand-new pool every time.
        assert _options_key({"retry": factory(3)}) == _options_key({"retry": factory(3)})

    @pytest.mark.parametrize("factory", [_Retry, _SlottedRetry])
    def test_different_configuration_produces_different_keys(self, factory: Any):
        assert _options_key({"retry": factory(3)}) != _options_key({"retry": factory(5)})

    def test_nested_objects_are_digested(self):
        outer_a = _Retry(3)
        outer_a.backoff = _Retry(1)  # type: ignore[attr-defined]
        outer_b = _Retry(3)
        outer_b.backoff = _Retry(1)  # type: ignore[attr-defined]
        outer_c = _Retry(3)
        outer_c.backoff = _Retry(2)  # type: ignore[attr-defined]

        assert _options_key({"retry": outer_a}) == _options_key({"retry": outer_b})
        assert _options_key({"retry": outer_a}) != _options_key({"retry": outer_c})

    def test_key_stays_hashable_for_container_options(self):
        key = _options_key({"nodes": [{"host": "a"}, {"host": "b"}], "flags": {"x", "y"}})
        assert hash(key)

    def test_self_referencing_value_does_not_recurse_forever(self):
        looped = _Retry(3)
        looped.self_ref = looped  # type: ignore[attr-defined]

        assert hash(_options_key({"retry": looped}))


@requires_valkey
class TestServerListValidation:
    """An empty LOCATION must fail loudly at construction."""

    def test_empty_server_list_raises_improperly_configured(self):
        # Regression: reads reached random.randint(1, -1) and writes reached
        # _servers[0], both far from the misconfiguration that caused them.
        with pytest.raises(ImproperlyConfigured, match="at least one server URL"):
            ValkeyPyAdapter([])


@requires_valkey
class TestSentinelKwargs:
    """sentinel_kwargs must stay None so the driver inherits socket_* settings."""

    @staticmethod
    def _capture(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
        captured: dict[str, Any] = {}

        class StubSentinel:
            def __init__(self, sentinels: Any, sentinel_kwargs: Any = None, **kwargs: Any) -> None:
                captured["sentinels"] = sentinels
                captured["sentinel_kwargs"] = sentinel_kwargs
                captured["kwargs"] = kwargs

        monkeypatch.setattr(ValkeyPySentinelAdapter, "_sentinel_class", StubSentinel)
        return captured

    def test_missing_sentinel_kwargs_is_passed_as_none(self, monkeypatch: pytest.MonkeyPatch):
        # Regression: an empty dict suppressed the driver's socket_* fallback,
        # so a blackholing sentinel blocked discovery instead of timing out.
        captured = self._capture(monkeypatch)

        ValkeyPySentinelAdapter(
            ["redis://mymaster/0"],
            sentinels=[("sentinel-a", 26379)],
            socket_timeout=0.5,
        )

        assert captured["sentinel_kwargs"] is None

    def test_explicit_sentinel_kwargs_are_forwarded(self, monkeypatch: pytest.MonkeyPatch):
        captured = self._capture(monkeypatch)

        ValkeyPySentinelAdapter(
            ["redis://mymaster/0"],
            sentinels=[("sentinel-a", 26379)],
            sentinel_kwargs={"socket_timeout": 0.1},
        )

        assert captured["sentinel_kwargs"] == {"socket_timeout": 0.1}

    def test_sentinel_options_never_reach_the_pool(self, monkeypatch: pytest.MonkeyPatch):
        self._capture(monkeypatch)

        adapter = ValkeyPySentinelAdapter(
            ["redis://mymaster/0"],
            sentinels=[("sentinel-a", 26379)],
            sentinel_kwargs={"socket_timeout": 0.1},
            socket_timeout=0.5,
        )

        assert "sentinels" not in adapter._pool_options
        assert "sentinel_kwargs" not in adapter._pool_options
        assert adapter._pool_options["socket_timeout"] == 0.5


@requires_valkey
class TestConnectionOptionsReachThePool:
    """Credentials and TLS settings from OPTIONS must survive into the pool."""

    @staticmethod
    def _pool_kwargs(**options: Any) -> dict[str, Any]:
        captured: dict[str, Any] = {}

        class StubPoolClass:
            @staticmethod
            def from_url(url: str, **kwargs: Any) -> Any:
                captured["url"] = url
                captured["kwargs"] = kwargs
                return object()

        adapter = ValkeyPyAdapter([SERVER_URL], **options)
        adapter._pool_class = StubPoolClass
        adapter._get_connection_pool(write=True)
        return captured

    def test_username_and_password_are_forwarded(self):
        captured = self._pool_kwargs(username="alice", password="s3cret")  # noqa: S106

        assert captured["kwargs"]["username"] == "alice"
        assert captured["kwargs"]["password"] == "s3cret"

    def test_ssl_settings_are_forwarded(self):
        captured = self._pool_kwargs(ssl_cert_reqs="required", ssl_ca_certs="/etc/ssl/ca.pem")

        assert captured["kwargs"]["ssl_cert_reqs"] == "required"
        assert captured["kwargs"]["ssl_ca_certs"] == "/etc/ssl/ca.pem"

    def test_client_only_options_stay_out_of_the_pool(self):
        captured = self._pool_kwargs(username="alice", serializer="pickle", pool_class="valkey.ConnectionPool")

        assert "serializer" not in captured["kwargs"]
        assert "pool_class" not in captured["kwargs"]

    def test_tls_scheme_selects_the_tls_connection_class(self):
        import valkey

        adapter = ValkeyPyAdapter([SERVER_URL])

        pool = adapter._get_connection_pool(write=True)

        assert pool.connection_class is valkey.SSLConnection
        assert pool.connection_kwargs["username"] == "user"
        assert pool.connection_kwargs["password"] == "secret"

    def test_pool_tuning_options_are_forwarded(self):
        captured = self._pool_kwargs(max_connections=42, socket_timeout=1.5, retry_on_timeout=True)

        assert captured["kwargs"]["max_connections"] == 42
        assert captured["kwargs"]["socket_timeout"] == 1.5
        assert captured["kwargs"]["retry_on_timeout"] is True

    def test_parser_class_is_imported_and_forwarded(self):
        from valkey._parsers.resp2 import _RESP2Parser

        captured = self._pool_kwargs(parser_class="valkey._parsers.resp2._RESP2Parser")

        assert captured["kwargs"]["parser_class"] is _RESP2Parser

    def test_parser_class_defaults_to_the_driver_parser(self):
        import valkey

        captured = self._pool_kwargs()

        assert captured["kwargs"]["parser_class"] is valkey.connection.DefaultParser

    def test_pool_class_is_imported_and_used(self):
        import valkey

        adapter = ValkeyPyAdapter([SERVER_URL], pool_class="valkey.connection.BlockingConnectionPool")

        assert isinstance(adapter._get_connection_pool(write=True), valkey.BlockingConnectionPool)

    @pytest.mark.asyncio
    async def test_async_pool_class_is_imported_and_used(self, monkeypatch: pytest.MonkeyPatch):
        import valkey.asyncio

        monkeypatch.setattr(ValkeyPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
        adapter = ValkeyPyAdapter(
            [SERVER_URL],
            async_pool_class="valkey.asyncio.BlockingConnectionPool",
            max_connections=7,
        )

        pool = adapter._get_async_connection_pool(write=True)
        try:
            assert isinstance(pool, valkey.asyncio.BlockingConnectionPool)
            assert pool.max_connections == 7
        finally:
            await adapter.aclose()

    @pytest.mark.asyncio
    async def test_parser_class_stays_out_of_the_async_pool(self, monkeypatch: pytest.MonkeyPatch):
        # parser_class is sync-only; an async connection raises AttributeError on it.
        monkeypatch.setattr(ValkeyPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
        adapter = ValkeyPyAdapter(
            [SERVER_URL],
            parser_class="valkey._parsers.resp2._RESP2Parser",
            socket_connect_timeout=2.5,
            retry_on_timeout=True,
        )

        pool = adapter._get_async_connection_pool(write=True)
        try:
            assert "parser_class" not in pool.connection_kwargs
            assert pool.connection_kwargs["socket_connect_timeout"] == 2.5
            assert pool.connection_kwargs["retry_on_timeout"] is True
        finally:
            await adapter.aclose()


@requires_valkey
class TestSentinelPoolClass:
    """pool_class on a Sentinel cache selects the Sentinel-managed pool."""

    @staticmethod
    def _build(pool_class: Any) -> ValkeyPySentinelAdapter:
        return ValkeyPySentinelAdapter(
            ["redis://mymaster/0"],
            pool_class=pool_class,
            sentinels=[("sentinel-a", 26379)],
        )

    def test_a_plain_pool_class_is_rejected(self):
        with pytest.raises(ImproperlyConfigured, match="cannot serve a Sentinel cache"):
            self._build("valkey.connection.ConnectionPool")

    def test_a_sentinel_pool_subclass_is_honoured(self):
        from valkey.sentinel import SentinelConnectionPool

        class CustomSentinelPool(SentinelConnectionPool):
            pass

        adapter = self._build(CustomSentinelPool)

        assert adapter._sentinel_pool_class is CustomSentinelPool

    def test_omitting_pool_class_keeps_the_driver_default(self):
        from valkey.sentinel import SentinelConnectionPool

        adapter = ValkeyPySentinelAdapter(
            ["redis://mymaster/0"],
            sentinels=[("sentinel-a", 26379)],
        )

        assert adapter._sentinel_pool_class is SentinelConnectionPool


class _TypeClient:
    """Driver stub whose TYPE reply is whatever the test hands it."""

    def __init__(self, reply: str) -> None:
        self.reply = reply

    def type(self, key: str) -> str:
        del key
        return self.reply


class _AsyncTypeClient(_TypeClient):
    async def type(self, key: str) -> str:  # type: ignore[override]
        return super().type(key)


def _type_adapter(client: Any) -> ValkeyPyAdapter:
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    adapter.get_client = lambda key=None, *, write=False: client  # type: ignore[method-assign]

    async def get_async_client(key: Any = None, *, write: bool = False) -> Any:
        del key, write
        return client

    adapter.get_async_client = get_async_client  # type: ignore[method-assign]
    return adapter


class TestKeyTypeMapping:
    @pytest.mark.parametrize("reply", ["string", "list", "set", "zset", "hash", "stream"])
    def test_modelled_types_map_to_their_member(self, reply: str):
        assert _type_adapter(_TypeClient(reply)).type("key") == KeyType(reply)

    def test_a_missing_key_is_none(self):
        assert _type_adapter(_TypeClient("none")).type("key") is None

    @pytest.mark.parametrize("reply", ["ReJSON-RL", "TSDB-TYPE", "MBbloom--"])
    def test_module_types_map_to_unknown(self, reply: str):
        assert _type_adapter(_TypeClient(reply)).type("key") is KeyType.UNKNOWN

    @pytest.mark.asyncio
    async def test_async_module_types_map_to_unknown(self):
        assert await _type_adapter(_AsyncTypeClient("ReJSON-RL")).atype("key") is KeyType.UNKNOWN

    @pytest.mark.asyncio
    async def test_async_missing_key_is_none(self):
        assert await _type_adapter(_AsyncTypeClient("none")).atype("key") is None


def _sentinel_modules(adapter_class: Any) -> tuple[Any, Any]:
    lib = adapter_class._lib.__name__
    return importlib.import_module(f"{lib}.sentinel"), importlib.import_module(f"{lib}.asyncio.sentinel")


_SENTINEL_DRIVERS = [
    pytest.param(RedisPySentinelAdapter, "redis", "rediss", id="redis-py"),
    pytest.param(ValkeyPySentinelAdapter, "valkey", "valkeys", id="valkey-py"),
]


@requires_valkey
@pytest.mark.parametrize(("adapter_class", "plain_scheme", "tls_scheme"), _SENTINEL_DRIVERS)
class TestSentinelTlsUrls:
    """A TLS LOCATION must keep Sentinel discovery instead of dialling the service name."""

    @staticmethod
    def _adapter(adapter_class: Any, scheme: str) -> Any:
        return adapter_class([f"{scheme}://mymaster/0"], sentinels=[("sentinel-a", 26379)])

    def test_tls_url_keeps_a_sentinel_managed_connection(
        self,
        adapter_class: Any,
        plain_scheme: str,
        tls_scheme: str,
    ):
        # Regression: the driver's parse_url injected a plain SSLConnection,
        # whose host is the literal service name, so Sentinel was never asked
        # for the primary and failover went unnoticed.
        del plain_scheme
        sync_sentinel, _ = _sentinel_modules(adapter_class)

        pool = self._adapter(adapter_class, tls_scheme)._get_connection_pool(write=True)

        assert pool.connection_class is sync_sentinel.SentinelManagedSSLConnection
        assert issubclass(pool.connection_class, sync_sentinel.SentinelManagedConnection)

    def test_plain_url_keeps_the_plain_managed_connection(
        self,
        adapter_class: Any,
        plain_scheme: str,
        tls_scheme: str,
    ):
        del tls_scheme
        sync_sentinel, _ = _sentinel_modules(adapter_class)

        pool = self._adapter(adapter_class, plain_scheme)._get_connection_pool(write=True)

        assert pool.connection_class is sync_sentinel.SentinelManagedConnection

    @pytest.mark.asyncio
    async def test_async_tls_url_keeps_a_sentinel_managed_connection(
        self,
        adapter_class: Any,
        plain_scheme: str,
        tls_scheme: str,
        monkeypatch: pytest.MonkeyPatch,
    ):
        del plain_scheme
        _, async_sentinel = _sentinel_modules(adapter_class)
        monkeypatch.setattr(adapter_class, "_async_pools", weakref.WeakKeyDictionary())
        adapter = self._adapter(adapter_class, tls_scheme)

        pool = adapter._get_async_connection_pool(write=True)
        try:
            assert pool.connection_class is async_sentinel.SentinelManagedSSLConnection
            assert issubclass(pool.connection_class, async_sentinel.SentinelManagedConnection)
        finally:
            await adapter.aclose()

    @pytest.mark.asyncio
    async def test_async_plain_url_keeps_the_plain_managed_connection(
        self,
        adapter_class: Any,
        plain_scheme: str,
        tls_scheme: str,
        monkeypatch: pytest.MonkeyPatch,
    ):
        del tls_scheme
        _, async_sentinel = _sentinel_modules(adapter_class)
        monkeypatch.setattr(adapter_class, "_async_pools", weakref.WeakKeyDictionary())
        adapter = self._adapter(adapter_class, plain_scheme)

        pool = adapter._get_async_connection_pool(write=True)
        try:
            assert pool.connection_class is async_sentinel.SentinelManagedConnection
        finally:
            await adapter.aclose()


class _AsyncPoolStub:
    def __init__(self) -> None:
        self.closed = 0

    async def aclose(self) -> None:
        self.closed += 1


class _AsyncPoolClassStub:
    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> _AsyncPoolStub:
        del url, kwargs
        return _AsyncPoolStub()


@requires_valkey
class TestAcloseScope:
    """aclose() disconnects its own pools and leaves every other alias alone."""

    @staticmethod
    def _adapter(*servers: str) -> ValkeyPyAdapter:
        adapter = ValkeyPyAdapter(list(servers))
        adapter._async_pool_class = _AsyncPoolClassStub
        return adapter

    @pytest.mark.asyncio
    async def test_other_aliases_keep_their_pools(self, monkeypatch: pytest.MonkeyPatch):
        # Regression: the whole loop slot was popped, so closing one alias
        # disconnected the pool another alias had connections checked out of.
        monkeypatch.setattr(ValkeyPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
        first = self._adapter("valkey://one:6379/0")
        second = self._adapter("valkey://two:6379/0")

        first_pool = first._get_async_connection_pool(write=True)
        second_pool = second._get_async_connection_pool(write=True)
        await first.aclose()

        assert first_pool.closed == 1
        assert second_pool.closed == 0
        assert second._get_async_connection_pool(write=True) is second_pool

    @pytest.mark.asyncio
    async def test_every_server_of_the_alias_is_closed(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(ValkeyPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
        adapter = self._adapter("valkey://primary:6379/0", "valkey://replica:6379/0")

        pools = [adapter._get_async_connection_pool(write=write) for write in (True, False)]
        await adapter.aclose()

        assert [pool.closed for pool in pools] == [1, 1]

    @pytest.mark.asyncio
    async def test_second_aclose_is_a_no_op(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(ValkeyPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
        adapter = self._adapter("valkey://one:6379/0")

        pool = adapter._get_async_connection_pool(write=True)
        await adapter.aclose()
        await adapter.aclose()

        assert pool.closed == 1


class _StubDiscoveryClient:
    def __init__(self) -> None:
        self.closed = 0

    async def aclose(self) -> None:
        self.closed += 1


class _StubAsyncSentinel:
    def __init__(self, sentinels: Any, sentinel_kwargs: Any = None, **kwargs: Any) -> None:
        del sentinels, sentinel_kwargs, kwargs
        self.sentinels = [_StubDiscoveryClient()]


class _StubAsyncSentinelPool:
    def __init__(self, sentinel_manager: Any) -> None:
        self.sentinel_manager = sentinel_manager
        self.closed = 0

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> _StubAsyncSentinelPool:
        del url
        return cls(kwargs["sentinel_manager"])

    async def aclose(self) -> None:
        self.closed += 1


@requires_valkey
class TestSentinelAcloseClosesDiscoveryClients:
    """The discovery clients must close with the pool that owns their manager."""

    @staticmethod
    def _adapter() -> ValkeyPySentinelAdapter:
        adapter = ValkeyPySentinelAdapter.__new__(ValkeyPySentinelAdapter)
        adapter._servers = ["redis://mymaster/0?is_master=1"]
        adapter._options = {"sentinels": [("sentinel-a", 26379)]}
        adapter._pool_options = {"socket_timeout": 5}
        adapter._async_sentinels = weakref.WeakKeyDictionary()
        return adapter

    @pytest.mark.asyncio
    async def test_another_instance_closes_the_creators_clients(self, monkeypatch: pytest.MonkeyPatch):
        # Regression: aclose() read the per-instance _async_sentinels, but
        # asgiref hands every task a fresh adapter, so the discovery clients
        # of the instance that built the pool stayed open.
        monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_sentinel_pool_class", _StubAsyncSentinelPool)
        monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_sentinel_class", _StubAsyncSentinel)
        monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_pools", weakref.WeakKeyDictionary())

        pool = self._adapter()._get_async_connection_pool(write=True)
        await self._adapter().aclose()

        assert pool.closed == 1
        assert [client.closed for client in pool.sentinel_manager.sentinels] == [1]
