"""Mock-based unit tests for redis-py / valkey-py adapter internals that the
parametrized container fixtures cannot observe (registry keys, driver-callback
suppression, stub-shaped driver pipelines)."""

import gc
import importlib
import weakref
from collections import deque
from typing import Any

import pytest
from django.core.exceptions import ImproperlyConfigured

from django_cachex.adapters import valkey_py
from django_cachex.adapters.protocols import Invalidation
from django_cachex.adapters.redis_py import RedisPyAdapter, RedisPyClusterAdapter, RedisPySentinelAdapter
from django_cachex.adapters.valkey_py import (
    _VALKEY_AVAILABLE,
    ValkeyPyAdapter,
    ValkeyPyAsyncPipelineAdapter,
    ValkeyPyClusterAdapter,
    ValkeyPyPipelineAdapter,
    ValkeyPySentinelAdapter,
    _options_key,
    _ValkeyPyInvalidationListener,
)
from django_cachex.exceptions import NotSupportedError, WrongTypeError, translate_server_error
from django_cachex.script import script_sha
from django_cachex.types import KeyType
from tests.cache.support import run_forked_while_held

SERVER_URL = "rediss://user:secret@example.com:7000/0?socket_timeout=5"

requires_valkey = pytest.mark.skipif(not _VALKEY_AVAILABLE, reason="valkey-py is not installed")


def test_get_client_builds_cluster_from_full_url(monkeypatch: pytest.MonkeyPatch):
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


def test_get_client_shares_cluster_across_instances(monkeypatch: pytest.MonkeyPatch):
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
async def test_get_async_client_builds_cluster_from_full_url(monkeypatch: pytest.MonkeyPatch):
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


@requires_valkey
@pytest.mark.parametrize(
    "option",
    [
        {"parser_class": "valkey._parsers.resp2._RESP2Parser"},
        {"pool_class": "valkey.connection.BlockingConnectionPool"},
        {"async_pool_class": "valkey.asyncio.BlockingConnectionPool"},
    ],
    ids=["parser_class", "pool_class", "async_pool_class"],
)
def test_pool_and_parser_options_are_rejected(option: dict[str, str]):
    # Regression: the options were accepted and silently dropped; the
    # cluster client has no pool to configure and cannot take a parser.
    with pytest.raises(ImproperlyConfigured, match=f"does not take {next(iter(option))}"):
        ValkeyPyClusterAdapter([SERVER_URL], **option)


@requires_valkey
def test_plain_options_still_build():
    adapter = ValkeyPyClusterAdapter([SERVER_URL], socket_connect_timeout=3)

    assert adapter._cluster_options[0] == {"socket_connect_timeout": 3}


CLUSTER_LOCATION = ["redis://node-a:7000", "redis://node-b:7001/0", "redis://node-c:7002"]
CLUSTER_ADAPTERS = [
    pytest.param(ValkeyPyClusterAdapter, marks=requires_valkey, id="valkey-py"),
    pytest.param(RedisPyClusterAdapter, id="redis-py"),
]


@pytest.mark.parametrize("adapter_class", CLUSTER_ADAPTERS)
def test_cluster_get_client_seeds_every_location_url(mocker, adapter_class: Any):
    cluster_class = mocker.patch.object(adapter_class, "_cluster_class")
    mocker.patch.object(adapter_class, "_clusters", {})
    adapter = adapter_class(CLUSTER_LOCATION, socket_timeout=5)

    adapter.get_client()

    (url,), kwargs = cluster_class.from_url.call_args
    nodes = kwargs.pop("startup_nodes")
    assert url == CLUSTER_LOCATION[0]
    assert kwargs == {"socket_timeout": 5}
    assert [(node.host, node.port) for node in nodes] == [("node-a", 7000), ("node-b", 7001), ("node-c", 7002)]
    assert {type(node) for node in nodes} == {adapter._lib.cluster.ClusterNode}


@pytest.mark.parametrize("adapter_class", CLUSTER_ADAPTERS)
@pytest.mark.asyncio
async def test_cluster_get_async_client_seeds_every_location_url(mocker, adapter_class: Any):
    cluster_class = mocker.patch.object(adapter_class, "_async_cluster_class")
    mocker.patch.object(adapter_class, "_async_clusters", weakref.WeakKeyDictionary())
    adapter = adapter_class(CLUSTER_LOCATION, socket_timeout=5)

    await adapter.get_async_client()

    (url,), kwargs = cluster_class.from_url.call_args
    nodes = kwargs.pop("startup_nodes")
    assert url == CLUSTER_LOCATION[0]
    assert kwargs == {"socket_timeout": 5}
    assert [(node.host, node.port) for node in nodes] == [("node-a", 7000), ("node-b", 7001), ("node-c", 7002)]
    assert {type(node) for node in nodes} == {adapter._lib.asyncio.cluster.ClusterNode}


@requires_valkey
def test_cluster_locations_sharing_a_first_url_get_separate_clients(mocker):
    cluster_class = mocker.patch.object(ValkeyPyClusterAdapter, "_cluster_class")
    mocker.patch.object(ValkeyPyClusterAdapter, "_clusters", {})

    ValkeyPyClusterAdapter(["redis://node-a:7000", "redis://node-b:7001"]).get_client()
    ValkeyPyClusterAdapter(["redis://node-a:7000", "redis://node-c:7002"]).get_client()

    assert cluster_class.from_url.call_count == 2


@pytest.mark.asyncio
async def test_pool_shared_across_adapter_instances(monkeypatch: pytest.MonkeyPatch):
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
async def test_pools_not_shared_across_sentinel_fleets(monkeypatch: pytest.MonkeyPatch):
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


@requires_valkey
@pytest.mark.parametrize(
    ("sentinels", "sentinel_kwargs"),
    [([("sentinel-b", 26379)], None), ([("sentinel-a", 26379)], {"password": "s3cret"})],
)
def test_sync_pools_not_shared_across_sentinel_fleets(
    monkeypatch: pytest.MonkeyPatch,
    sentinels: list[Any],
    sentinel_kwargs: dict[str, Any] | None,
):
    monkeypatch.setattr(ValkeyPySentinelAdapter, "_sync_pools", {})
    client = ValkeyPySentinelAdapter(["redis://mymaster/0"], sentinels=[("sentinel-a", 26379)]).get_client(write=True)

    other = ValkeyPySentinelAdapter(["redis://mymaster/0"], sentinels=sentinels, sentinel_kwargs=sentinel_kwargs)

    assert other.get_client(write=True).connection_pool is not client.connection_pool


SENTINEL_ADAPTERS = [
    pytest.param(ValkeyPySentinelAdapter, marks=requires_valkey, id="valkey-py"),
    pytest.param(RedisPySentinelAdapter, id="redis-py"),
]


def _retrying_sentinel_options(retry_on_error: list[type[Exception]]) -> dict[str, Any]:
    return {
        "sentinels": [("sentinel-a", 26379), ("sentinel-b", 26379)],
        "sentinel_kwargs": {"retry_on_timeout": True, "retry_on_error": retry_on_error},
    }


RETRY_ON_TIMEOUT_IS_DEPRECATED = pytest.mark.filterwarnings(
    "ignore:Call to '__init__' function with deprecated usage of input argument/s 'retry_on_timeout':DeprecationWarning",
)


@RETRY_ON_TIMEOUT_IS_DEPRECATED
@pytest.mark.parametrize("adapter_class", SENTINEL_ADAPTERS)
def test_sentinel_kwargs_retry_list_keeps_one_sync_pool(monkeypatch: pytest.MonkeyPatch, adapter_class: Any):
    monkeypatch.setattr(adapter_class, "_sync_pools", {})
    retry_on_error: list[type[Exception]] = [ConnectionError]
    options = _retrying_sentinel_options(retry_on_error)
    pool = adapter_class(["redis://mymaster/0"], **options).get_client(write=True).connection_pool

    other = adapter_class(["redis://mymaster/0"], **options)

    assert other.get_client(write=True).connection_pool is pool
    assert retry_on_error == [ConnectionError]


@RETRY_ON_TIMEOUT_IS_DEPRECATED
@pytest.mark.parametrize("adapter_class", SENTINEL_ADAPTERS)
@pytest.mark.asyncio
async def test_sentinel_kwargs_retry_list_keeps_one_async_pool(monkeypatch: pytest.MonkeyPatch, adapter_class: Any):
    monkeypatch.setattr(adapter_class, "_async_pools", weakref.WeakKeyDictionary())
    retry_on_error: list[type[Exception]] = [ConnectionError]
    options = _retrying_sentinel_options(retry_on_error)
    adapter = adapter_class(["redis://mymaster/0"], **options)
    client = await adapter.get_async_client(write=True)
    try:
        other = adapter_class(["redis://mymaster/0"], **options)

        assert (await other.get_async_client(write=True)).connection_pool is client.connection_pool
        assert retry_on_error == [ConnectionError]
    finally:
        await adapter.aclose()


FORK_LOCATION = ["redis://fork:6379/0"]


@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
@pytest.mark.parametrize(
    ("held_lock", "call"),
    [
        pytest.param(
            lambda: valkey_py._SYNC_POOLS_LOCK,
            lambda: ValkeyPyAdapter(FORK_LOCATION).get_client(write=True),
            marks=requires_valkey,
            id="pools",
        ),
        pytest.param(
            lambda: valkey_py._ASYNC_REGISTRY_LOCK,
            lambda: ValkeyPyAdapter(FORK_LOCATION).close(),
            marks=requires_valkey,
            id="async-pools",
        ),
        pytest.param(
            lambda: ValkeyPyClusterAdapter._clusters_lock,
            lambda: ValkeyPyClusterAdapter(FORK_LOCATION).get_client(),
            marks=requires_valkey,
            id="valkey-py-clusters",
        ),
        pytest.param(
            lambda: RedisPyClusterAdapter._clusters_lock,
            lambda: RedisPyClusterAdapter(FORK_LOCATION).get_client(),
            id="redis-py-clusters",
        ),
    ],
)
def test_a_forked_child_skips_a_lock_held_at_the_fork(mocker, held_lock, call):
    mocker.patch.object(ValkeyPyClusterAdapter, "_cluster_class")
    mocker.patch.object(RedisPyClusterAdapter, "_cluster_class")

    run_forked_while_held(held_lock(), call)


@pytest.mark.asyncio
async def test_reset_awaits_coroutine_reset():
    class StubPipeline:
        def __init__(self) -> None:
            self.reset_calls = 0

        async def reset(self) -> None:
            self.reset_calls += 1

    raw = StubPipeline()
    await ValkeyPyAsyncPipelineAdapter(raw).reset()
    assert raw.reset_calls == 1


@pytest.mark.asyncio
async def test_reset_clears_stack_when_reset_is_a_server_command():
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


def test_justid_preserves_cursor_and_deleted():
    # Regression: the driver-parsed flat ID list forced a "" cursor, so
    # callers could never resume iteration past the first page.
    client = _JustidClient()
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    adapter._get_connection_pool = lambda *, write=False: None
    adapter._new_client = lambda pool: client

    result = adapter.xautoclaim("stream", "group", "consumer", 0, justid=True)

    assert result == ("5-1", ["1-0", "2-0"], ["3-0"])


def test_justid_does_not_mutate_the_pooled_client():
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
async def test_async_justid_preserves_cursor_and_deleted():
    client = _AsyncJustidClient()
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    adapter._get_async_connection_pool = lambda *, write=False: None
    adapter._new_async_client = lambda pool: client

    result = await adapter.axautoclaim("stream", "group", "consumer", 0, justid=True)

    assert result == ("5-1", ["1-0", "2-0"], ["3-0"])


def test_cluster_justid_is_rejected():
    class StubClusterClient:
        def set_response_callback(self, command: str, callback: Any) -> None:
            msg = "shared cluster client must not be mutated"
            raise AssertionError(msg)

        def xautoclaim(self, *args: Any, **kwargs: Any) -> Any:
            msg = "no command may reach the server"
            raise AssertionError(msg)

    client = StubClusterClient()
    adapter = ValkeyPyClusterAdapter.__new__(ValkeyPyClusterAdapter)
    adapter.get_client = lambda key=None, *, write=False: client

    with pytest.raises(NotSupportedError, match=r"xautoclaim\(justid=True\).*cluster"):
        adapter.xautoclaim("stream", "group", "consumer", 0, justid=True)


@pytest.mark.asyncio
async def test_async_cluster_justid_is_rejected():
    adapter = ValkeyPyClusterAdapter.__new__(ValkeyPyClusterAdapter)

    with pytest.raises(NotSupportedError, match=r"xautoclaim\(justid=True\).*cluster"):
        await adapter.axautoclaim("stream", "group", "consumer", 0, justid=True)


def test_cluster_justid_false_still_works():
    class StubClusterClient:
        def xautoclaim(self, *args: Any, **kwargs: Any) -> Any:
            return [b"0-0", [(b"1-0", {b"field": b"value"})], []]

    adapter = ValkeyPyClusterAdapter.__new__(ValkeyPyClusterAdapter)
    adapter.get_client = lambda key=None, *, write=False: StubClusterClient()

    assert adapter.xautoclaim("stream", "group", "consumer", 0) == ("0-0", [("1-0", {"field": b"value"})], [])


def test_non_justid_parses_entries():
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


def test_get_client_reuses_one_client_per_pool():
    # Regression: every cache operation built a fresh client whose
    # WRONGTYPE patch made it uncollectable cyclic garbage.
    adapter = _pooled_adapter(_StubPool())

    assert adapter.get_client("key") is adapter.get_client("key", write=True)


def test_get_client_builds_one_client_per_distinct_pool():
    pools = [_StubPool(), _StubPool()]
    adapter = _pooled_adapter(pools[0])
    adapter._get_connection_pool = lambda *, write: pools[0] if write else pools[1]

    assert adapter.get_client("key", write=True) is not adapter.get_client("key", write=False)


def test_clients_are_shared_across_adapter_instances():
    # asgiref hands each task its own adapter; the client hangs off the
    # pool, so per-task instances still land on the same client.
    pool = _StubPool()

    assert _pooled_adapter(pool).get_client() is _pooled_adapter(pool).get_client()


def test_client_dies_with_its_pool():
    # A pool-keyed registry would hold the client strongly and the client
    # holds the pool, so dead loops' pools would never be freed.
    adapter = _pooled_adapter(_StubPool())
    client_ref = weakref.ref(adapter.get_client())

    del adapter
    gc.collect()

    assert client_ref() is None


@pytest.mark.asyncio
async def test_get_async_client_reuses_one_client_per_pool():
    adapter = _pooled_adapter(_StubPool())

    assert await adapter.get_async_client("key") is await adapter.get_async_client("key", write=True)


def test_new_client_is_never_the_pooled_one():
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
    async def lpop(self, key: str, count: int | None = None) -> Any:
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


# A nil reply is a missing key, not an empty pop.
@pytest.mark.parametrize("method", ["lpop", "rpop"])
def test_missing_key_returns_none(method: str):
    adapter = _pop_adapter(_PopClient(None))
    assert getattr(adapter, method)("missing", count=2) is None


@pytest.mark.parametrize("method", ["lpop", "rpop"])
def test_empty_array_stays_an_empty_list(method: str):
    adapter = _pop_adapter(_PopClient([]))
    assert getattr(adapter, method)("key", count=2) == []


@pytest.mark.parametrize("method", ["alpop", "arpop"])
@pytest.mark.asyncio
async def test_async_missing_key_returns_none(method: str):
    adapter = _pop_adapter(_AsyncPopClient(None))
    assert await getattr(adapter, method)("missing", count=2) is None


@pytest.mark.parametrize("method", ["alpop", "arpop"])
@pytest.mark.asyncio
async def test_async_empty_array_stays_an_empty_list(method: str):
    adapter = _pop_adapter(_AsyncPopClient([]))
    assert await getattr(adapter, method)("key", count=2) == []


@pytest.mark.parametrize("method", ["zpopmin", "zpopmax"])
@pytest.mark.parametrize(
    ("reply", "pairs"),
    [
        ([b"a", 1.5], [(b"a", 1.5)]),
        ([[b"a", 1.5], [b"b", 2.0]], [(b"a", 1.5), (b"b", 2.0)]),
        ([(b"a", 1.5)], [(b"a", 1.5)]),
        ([], []),
    ],
    ids=["resp3-without-count", "resp3-with-count", "resp2", "missing-key"],
)
def test_zpop_reply_becomes_member_score_pairs(mocker, method: str, reply: list[Any], pairs: list[Any]):
    client = mocker.Mock()
    getattr(client, method).return_value = reply

    assert getattr(_pop_adapter(client), method)("key") == pairs


@pytest.mark.parametrize("method", ["zpopmin", "zpopmax"])
@pytest.mark.asyncio
async def test_async_zpop_resp3_reply_without_count_becomes_one_pair(mocker, method: str):
    client = mocker.AsyncMock()
    getattr(client, method).return_value = [b"a", 1.5]

    assert await getattr(_pop_adapter(client), f"a{method}")("key") == [(b"a", 1.5)]


# ``HMGET key`` with no fields is a wire-level syntax error.
def test_sync_hmget_returns_empty_without_touching_the_client():
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)

    def unreachable(*args: Any, **kwargs: Any) -> Any:
        msg = "hmget() with no fields must not reach the server"
        raise AssertionError(msg)

    adapter.get_client = unreachable
    assert adapter.hmget("key") == []


@pytest.mark.asyncio
async def test_async_hmget_returns_empty_without_touching_the_client():
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
    async def xpending_range(self, key: str, group: str, **kwargs: Any) -> Any:
        self.range_kwargs = kwargs
        return []

    async def xpending(self, key: str, group: str) -> Any:
        self.summary_calls += 1
        return {"pending": 0}


@pytest.mark.parametrize(
    "kwargs",
    [{"start": "-"}, {"end": "+"}, {"start": "-", "end": "+"}, {"consumer": "c"}, {"idle": 100}],
)
def test_filters_without_count_raise(kwargs: dict[str, Any]):
    adapter = _pop_adapter(_XPendingClient())
    with pytest.raises(ValueError, match="xpending\\(\\) requires count"):
        adapter.xpending("stream", "group", **kwargs)


def test_summary_form_still_works():
    client = _XPendingClient()
    adapter = _pop_adapter(client)

    assert adapter.xpending("stream", "group") == {"pending": 0}
    assert client.summary_calls == 1


def test_count_alone_scans_the_whole_range():
    client = _XPendingClient()
    adapter = _pop_adapter(client)

    adapter.xpending("stream", "group", count=10)

    assert client.range_kwargs is not None
    assert client.range_kwargs["min"] == "-"
    assert client.range_kwargs["max"] == "+"


@pytest.mark.asyncio
async def test_async_filters_without_count_raise():
    adapter = _pop_adapter(_AsyncXPendingClient())
    with pytest.raises(ValueError, match="xpending\\(\\) requires count"):
        await adapter.axpending("stream", "group", consumer="c")


@pytest.mark.asyncio
async def test_async_count_alone_scans_the_whole_range():
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


# xread/xreadgroup replies arrive as pairs on RESP2 and a map on RESP3.
def test_resp2_pair_list():
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    assert adapter._decode_stream_results([(b"stream", _STREAM_ENTRIES)]) == _DECODED_STREAM


@pytest.mark.parametrize("parse_xread_resp3", _resp3_xread_parsers())
def test_resp3_mapping(parse_xread_resp3: Any):
    # Regression: under OPTIONS {"protocol": 3} the driver returns
    # {stream: [entries]}, and reading it as {stream: entries} raised.
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)

    assert adapter._decode_stream_results(parse_xread_resp3(_RESP3_XREAD_REPLY)) == _DECODED_STREAM


@pytest.mark.parametrize("parse_xread_resp3", _resp3_xread_parsers())
def test_resp3_mapping_with_several_entries(parse_xread_resp3: Any):
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    reply = {b"stream": [[b"1-0", [b"field", b"one"]], [b"2-0", [b"field", b"two"]]]}

    assert adapter._decode_stream_results(parse_xread_resp3(reply)) == {
        "stream": [("1-0", {"field": b"one"}), ("2-0", {"field": b"two"})],
    }


def test_resp3_empty_stream():
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    assert adapter._decode_stream_results({b"stream": []}) == {"stream": []}


def test_nil_entry_decodes_to_empty_fields():
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

    def execute(self, raise_on_error: bool = True) -> Any:
        raise self._error

    def execute_command(self, *args: Any) -> Any:
        raise self._error


class _AsyncWrongTypePipeline(_WrongTypePipeline):
    async def execute(self, raise_on_error: bool = True) -> Any:
        raise self._error


# Pipelines are fresh driver objects, so they need their own translation.
def test_execute_raises_wrongtype_error():
    # Regression: the client-instance patch never reached the pipeline, so
    # a batched type error surfaced as the raw driver ResponseError.
    pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline())
    with pytest.raises(WrongTypeError):
        pipeline.execute()


def test_execute_command_raises_wrongtype_error():
    pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline())
    with pytest.raises(WrongTypeError):
        pipeline.execute_command("LPUSH", "key", "value")


def test_other_errors_pass_through_untouched():
    original = _ResponseError("ERR syntax error")
    pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline(original))
    with pytest.raises(_ResponseError) as excinfo:
        pipeline.execute()
    assert excinfo.value is original


@pytest.mark.asyncio
async def test_async_execute_raises_wrongtype_error():
    pipeline = ValkeyPyAsyncPipelineAdapter(_AsyncWrongTypePipeline())
    with pytest.raises(WrongTypeError):
        await pipeline.execute()


SERVER_REPLY = "unknown command 'HEXPIRE', with args beginning with: 'k' '10' 'FIELDS' '1' 'f' "
CLUSTER_LOOKUP = "HSETEX command doesn't exist in Redis commands"


# An unknown-command reply means the server predates the command, so it becomes NotSupportedError.
def test_server_reply_becomes_not_supported():
    original = _ResponseError(SERVER_REPLY)
    pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline(original))
    with pytest.raises(NotSupportedError) as excinfo:
        pipeline.execute()
    assert excinfo.value.operation == "hexpire"
    assert excinfo.value.__cause__ is original
    assert "requires Redis 7.4+ or Valkey 9.0+" in str(excinfo.value)


def test_execute_command_translates_too():
    pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline(_ResponseError(SERVER_REPLY)))
    with pytest.raises(NotSupportedError):
        pipeline.execute_command("HEXPIRE", "k", 10, "FIELDS", 1, "f")


def test_cluster_command_lookup_becomes_not_supported():
    original = _ResponseError(CLUSTER_LOOKUP)
    pipeline = ValkeyPyPipelineAdapter(_WrongTypePipeline(original))
    with pytest.raises(NotSupportedError) as excinfo:
        pipeline.execute()
    assert excinfo.value.operation == "hsetex"
    assert "requires Redis 8.0+ or Valkey 9.0+" in str(excinfo.value)


def test_redis_6_backtick_quoting():
    wrapped = translate_server_error(_ResponseError("unknown command `FOOBAR`, with args beginning with: "))
    assert isinstance(wrapped, NotSupportedError)
    assert wrapped.operation == "foobar"
    assert wrapped.backend is None
    assert wrapped.detail == "the server does not know this command"


@pytest.mark.asyncio
async def test_async_execute_raises_not_supported():
    pipeline = ValkeyPyAsyncPipelineAdapter(_AsyncWrongTypePipeline(_ResponseError(SERVER_REPLY)))
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


@pytest.mark.parametrize("factory", [_Retry, _SlottedRetry])
def test_equal_objects_produce_equal_keys(factory: Any):
    # Regression: repr() of a plain object embeds its id(), so a Retry
    # rebuilt per instance opened a brand-new pool every time.
    assert _options_key({"retry": factory(3)}) == _options_key({"retry": factory(3)})


@pytest.mark.parametrize("factory", [_Retry, _SlottedRetry])
def test_different_configuration_produces_different_keys(factory: Any):
    assert _options_key({"retry": factory(3)}) != _options_key({"retry": factory(5)})


def test_nested_objects_are_digested():
    outer_a = _Retry(3)
    outer_a.backoff = _Retry(1)
    outer_b = _Retry(3)
    outer_b.backoff = _Retry(1)
    outer_c = _Retry(3)
    outer_c.backoff = _Retry(2)

    assert _options_key({"retry": outer_a}) == _options_key({"retry": outer_b})
    assert _options_key({"retry": outer_a}) != _options_key({"retry": outer_c})


def test_key_stays_hashable_for_container_options():
    options = {"nodes": [{"host": "a"}, {"host": "b"}], "flags": {"x", "y"}}
    key = _options_key(options)
    assert {key: "pool"}[_options_key({"flags": {"y", "x"}, "nodes": [{"host": "a"}, {"host": "b"}]})] == "pool"


def test_self_referencing_value_does_not_recurse_forever():
    looped = _Retry(3)
    looped.self_ref = looped

    key = _options_key({"retry": looped})
    assert {key: "pool"}[key] == "pool"


@requires_valkey
@pytest.mark.parametrize("factory", [_Retry, _SlottedRetry])
def test_option_object_keeps_its_sync_pool_after_its_state_changes(monkeypatch: pytest.MonkeyPatch, factory: Any):
    # Regression: a provider caching its renewed token keyed a second pool per thread.
    monkeypatch.setattr(ValkeyPyAdapter, "_sync_pools", {})
    retry = factory(3)
    pool = ValkeyPyAdapter([SERVER_URL], retry=retry).get_client(write=True).connection_pool

    retry.retries = 5

    assert ValkeyPyAdapter([SERVER_URL], retry=retry).get_client(write=True).connection_pool is pool


@requires_valkey
def test_empty_server_list_raises_improperly_configured():
    # Regression: reads reached random.randint(1, -1) and writes reached
    # _servers[0], both far from the misconfiguration that caused them.
    with pytest.raises(ImproperlyConfigured, match="at least one server URL"):
        ValkeyPyAdapter([])


def _capture(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    class StubSentinel:
        def __init__(self, sentinels: Any, sentinel_kwargs: Any = None, **kwargs: Any) -> None:
            captured["sentinels"] = sentinels
            captured["sentinel_kwargs"] = sentinel_kwargs
            captured["kwargs"] = kwargs

    monkeypatch.setattr(ValkeyPySentinelAdapter, "_sentinel_class", StubSentinel)
    return captured


@requires_valkey
def test_missing_sentinel_kwargs_gives_discovery_the_socket_options(monkeypatch: pytest.MonkeyPatch):
    # Regression: an empty dict suppressed the driver's socket_* fallback,
    # so a blackholing sentinel blocked discovery instead of timing out.
    captured = _capture(monkeypatch)

    ValkeyPySentinelAdapter(
        ["redis://mymaster/0"],
        sentinels=[("sentinel-a", 26379)],
        socket_timeout=0.5,
    )

    assert captured["sentinel_kwargs"] == {"socket_timeout": 0.5}


@requires_valkey
def test_explicit_sentinel_kwargs_are_forwarded(monkeypatch: pytest.MonkeyPatch):
    captured = _capture(monkeypatch)

    ValkeyPySentinelAdapter(
        ["redis://mymaster/0"],
        sentinels=[("sentinel-a", 26379)],
        sentinel_kwargs={"socket_timeout": 0.1},
    )

    assert captured["sentinel_kwargs"] == {"socket_timeout": 0.1}


@requires_valkey
def test_sentinel_options_never_reach_the_pool(monkeypatch: pytest.MonkeyPatch):
    _capture(monkeypatch)

    adapter = ValkeyPySentinelAdapter(
        ["redis://mymaster/0"],
        sentinels=[("sentinel-a", 26379)],
        sentinel_kwargs={"socket_timeout": 0.1},
        socket_timeout=0.5,
    )

    assert "sentinels" not in adapter._pool_options
    assert "sentinel_kwargs" not in adapter._pool_options
    assert adapter._pool_options["socket_timeout"] == 0.5


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


@requires_valkey
def test_client_only_options_stay_out_of_the_pool():
    captured = _pool_kwargs(username="alice", serializer="pickle", pool_class="valkey.ConnectionPool")

    assert "serializer" not in captured["kwargs"]
    assert "pool_class" not in captured["kwargs"]


@requires_valkey
def test_parser_class_is_imported_and_forwarded():
    from valkey._parsers.resp2 import _RESP2Parser

    captured = _pool_kwargs(parser_class="valkey._parsers.resp2._RESP2Parser")

    assert captured["kwargs"]["parser_class"] is _RESP2Parser


@requires_valkey
def test_parser_class_defaults_to_the_driver_parser():
    import valkey

    captured = _pool_kwargs()

    assert captured["kwargs"]["parser_class"] is valkey.connection.DefaultParser


@requires_valkey
def test_pool_class_is_imported_and_used():
    import valkey

    adapter = ValkeyPyAdapter([SERVER_URL], pool_class="valkey.connection.BlockingConnectionPool")

    assert isinstance(adapter._get_connection_pool(write=True), valkey.BlockingConnectionPool)


@requires_valkey
@pytest.mark.parametrize(
    "options",
    [
        {"socket_timeout": 2},
        {"pool_class": "valkey.connection.BlockingConnectionPool"},
        {"parser_class": "valkey._parsers.resp2._RESP2Parser"},
    ],
)
def test_differently_configured_adapters_get_their_own_sync_pool(monkeypatch: pytest.MonkeyPatch, options: dict):
    monkeypatch.setattr(ValkeyPyAdapter, "_sync_pools", {})
    pool = ValkeyPyAdapter([SERVER_URL]).get_client(write=True).connection_pool

    assert ValkeyPyAdapter([SERVER_URL], **options).get_client(write=True).connection_pool is not pool


@requires_valkey
@pytest.mark.asyncio
async def test_async_pool_class_is_imported_and_used(monkeypatch: pytest.MonkeyPatch):
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


@requires_valkey
@pytest.mark.asyncio
async def test_parser_class_stays_out_of_the_async_pool(monkeypatch: pytest.MonkeyPatch):
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


_CREDENTIALS_URL = "redis://alice:urlpw@example.com:7000/2?socket_timeout=5"

_STANDALONE_DRIVERS = [
    pytest.param(RedisPyAdapter, id="redis-py"),
    pytest.param(ValkeyPyAdapter, id="valkey-py"),
]


@requires_valkey
@pytest.mark.parametrize("adapter_class", _STANDALONE_DRIVERS)
@pytest.mark.asyncio
async def test_a_client_class_override_gets_its_own_clients(monkeypatch: pytest.MonkeyPatch, adapter_class: Any):
    monkeypatch.setattr(adapter_class, "_sync_pools", {})
    monkeypatch.setattr(adapter_class, "_async_pools", weakref.WeakKeyDictionary())
    client_class = type("Client", (adapter_class._client_class,), {})
    async_client_class = type("AsyncClient", (adapter_class._async_client_class,), {})
    override_class = type(
        "Adapter",
        (adapter_class,),
        {"_client_class": client_class, "_async_client_class": async_client_class},
    )
    base, override = adapter_class([SERVER_URL]), override_class([SERVER_URL])
    try:
        base.get_client(write=True)
        await base.get_async_client(write=True)

        clients = [override.get_client(write=True), await override.get_async_client(write=True)]

        assert [type(client) for client in clients] == [client_class, async_client_class]
    finally:
        await base.aclose()
        await override.aclose()


# Regression: the driver's from_url() applies URL values after keyword
# arguments, so the URL credentials silently beat OPTIONS on redis-py and valkey-py.


@requires_valkey
@pytest.mark.parametrize("adapter_class", _STANDALONE_DRIVERS)
def test_both_options_win_in_the_sync_pool(adapter_class: Any):
    adapter = adapter_class([_CREDENTIALS_URL], username="bob", password="optpw")  # noqa: S106

    kwargs = adapter._get_connection_pool(write=True).connection_kwargs

    assert (kwargs["username"], kwargs["password"]) == ("bob", "optpw")
    assert (kwargs["host"], kwargs["port"], kwargs["db"], kwargs["socket_timeout"]) == ("example.com", 7000, 2, 5)


@requires_valkey
@pytest.mark.parametrize("adapter_class", _STANDALONE_DRIVERS)
def test_password_alone_keeps_the_url_username(adapter_class: Any):
    adapter = adapter_class([_CREDENTIALS_URL], password="optpw")  # noqa: S106

    kwargs = adapter._get_connection_pool(write=True).connection_kwargs

    assert (kwargs["username"], kwargs["password"]) == ("alice", "optpw")


@requires_valkey
@pytest.mark.parametrize("adapter_class", _STANDALONE_DRIVERS)
@pytest.mark.parametrize(
    ("url", "options", "expected"),
    [
        pytest.param("redis://alice:p@ss@host:7000/0", {"password": "optpw"}, ("alice", "optpw"), id="raw-at"),
        pytest.param("redis://alice:p@ss@host:7000/0", {"username": "bob"}, ("bob", "p@ss"), id="raw-at-username"),
        pytest.param("redis://alice:p:w@host:7000/0", {"username": "bob"}, ("bob", "p:w"), id="raw-colon"),
        pytest.param(
            "redis://al%40ice:p%40ss%3Ax@host:7000/0",
            {"username": "bob"},
            ("bob", "p@ss:x"),
            id="encoded-password",
        ),
        pytest.param(
            "redis://al%40ice:p%40ss@host:7000/0",
            {"password": "optpw"},
            ("al@ice", "optpw"),
            id="encoded-username",
        ),
        pytest.param("redis://alice:urlpw@host:7000/0", {"username": "bob"}, ("bob", "urlpw"), id="username-only"),
        pytest.param(
            "redis://host:7000/0?username=alice&password=urlpw",
            {"username": "bob"},
            ("bob", "urlpw"),
            id="query",
        ),
        pytest.param(
            "redis://host:7000/0?username=alice&password=urlpw",
            {"username": "bob", "password": "optpw"},
            ("bob", "optpw"),
            id="query-both",
        ),
    ],
)
def test_awkward_url_shapes(adapter_class: Any, url: str, options: dict[str, str], expected: tuple[str, str]):
    """A raw or encoded ``@`` / ``:`` in the URL credentials and ``?username=`` survive the rebuild."""
    adapter = adapter_class([url], **options)

    kwargs = adapter._get_connection_pool(write=True).connection_kwargs

    assert (kwargs["username"], kwargs["password"]) == expected
    assert (kwargs["host"], kwargs["port"], kwargs["db"]) == ("host", 7000, 0)


@requires_valkey
def test_url_credentials_stay_when_options_has_none():
    adapter = ValkeyPyAdapter([_CREDENTIALS_URL], socket_connect_timeout=1)

    kwargs = adapter._get_connection_pool(write=True).connection_kwargs

    assert (kwargs["username"], kwargs["password"]) == ("alice", "urlpw")


@requires_valkey
def test_ipv6_host_and_query_credentials_are_handled():
    adapter = ValkeyPyAdapter(
        ["unix://alice@/run/valkey.sock?db=3&password=urlpw", "rediss://alice:urlpw@[::1]:7001/0?a=%2520"],
        username="bob",
        password="optpw",  # noqa: S106
    )

    assert adapter._servers == ["unix:///run/valkey.sock?db=3", "rediss://[::1]:7001/0?a=%2520"]


@requires_valkey
@pytest.mark.asyncio
async def test_options_win_in_the_async_pool(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(ValkeyPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
    adapter = ValkeyPyAdapter([_CREDENTIALS_URL], username="bob", password="optpw")  # noqa: S106

    pool = adapter._get_async_connection_pool(write=True)
    try:
        assert (pool.connection_kwargs["username"], pool.connection_kwargs["password"]) == ("bob", "optpw")
    finally:
        await adapter.aclose()


@requires_valkey
def test_options_win_in_the_sentinel_pool():
    adapter = ValkeyPySentinelAdapter(
        ["redis://alice:urlpw@mymaster/0"],
        sentinels=[("sentinel-a", 26379)],
        username="bob",
        password="optpw",  # noqa: S106
    )

    kwargs = adapter._get_connection_pool(write=True).connection_kwargs

    assert (kwargs["username"], kwargs["password"]) == ("bob", "optpw")
    assert adapter._parse_sentinel_url(0)[0] == "mymaster"


@requires_valkey
def test_options_win_in_the_cluster_client(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, Any] = {}

    class StubCluster:
        @classmethod
        def from_url(cls, url: str, **kwargs: Any) -> StubCluster:
            captured["url"] = url
            captured["kwargs"] = kwargs
            return cls()

    monkeypatch.setattr(ValkeyPyClusterAdapter, "_cluster_class", StubCluster)
    monkeypatch.setattr(ValkeyPyClusterAdapter, "_clusters", {})
    adapter = ValkeyPyClusterAdapter([_CREDENTIALS_URL], username="bob", password="optpw")  # noqa: S106

    adapter.get_client()

    assert captured["url"] == "redis://example.com:7000/2?socket_timeout=5"
    assert captured["kwargs"] == {"username": "bob", "password": "optpw"}


@requires_valkey
@pytest.mark.parametrize("adapter_class", _STANDALONE_DRIVERS)
@pytest.mark.parametrize(
    ("url", "options", "expected"),
    [
        pytest.param("redis://host:7000/0", {}, 5, id="unset"),
        pytest.param("redis://host:7000/0", {"socket_timeout": None}, 5, id="no-read-timeout"),
        pytest.param("redis://host:7000/0?socket_connect_timeout=2", {}, 2, id="url"),
        pytest.param("redis://host:7000/0", {"socket_timeout": 0.5}, None, id="read-timeout"),
        pytest.param("redis://host:7000/0?socket_timeout=0.5", {}, None, id="url-read-timeout"),
    ],
)
@pytest.mark.asyncio
async def test_pools_connect_under_a_default_timeout(
    monkeypatch: pytest.MonkeyPatch,
    adapter_class: Any,
    url: str,
    options: dict[str, Any],
    expected: float | None,
):
    monkeypatch.setattr(adapter_class, "_sync_pools", {})
    monkeypatch.setattr(adapter_class, "_async_pools", weakref.WeakKeyDictionary())
    adapter = adapter_class([url], **options)
    client = await adapter.get_async_client(write=True)
    try:
        pools = [adapter.get_client(write=True).connection_pool, client.connection_pool]

        assert [pool.connection_kwargs.get("socket_connect_timeout") for pool in pools] == [expected, expected]
    finally:
        await adapter.aclose()


@pytest.mark.parametrize("adapter_class", SENTINEL_ADAPTERS)
@pytest.mark.parametrize(
    ("url", "sentinel_kwargs"),
    [
        pytest.param("redis://mymaster/0", None, id="unset"),
        pytest.param("redis://mymaster/0?socket_connect_timeout=2", None, id="url"),
        pytest.param("redis://mymaster/0", {"password": "secret"}, id="sentinel-kwargs"),
    ],
)
@pytest.mark.asyncio
async def test_sentinel_discovery_connects_under_a_default_timeout(
    monkeypatch: pytest.MonkeyPatch,
    adapter_class: Any,
    url: str,
    sentinel_kwargs: dict[str, Any] | None,
):
    monkeypatch.setattr(adapter_class, "_sync_pools", {})
    monkeypatch.setattr(adapter_class, "_async_pools", weakref.WeakKeyDictionary())
    adapter = adapter_class([url], sentinels=[("sentinel-a", 26379)], sentinel_kwargs=sentinel_kwargs)
    client = await adapter.get_async_client(write=True)
    try:
        managers = [
            adapter.get_client(write=True).connection_pool.sentinel_manager,
            client.connection_pool.sentinel_manager,
        ]

        assert [manager.sentinel_kwargs.get("socket_connect_timeout") for manager in managers] == [5, 5]
    finally:
        await adapter.aclose()


@pytest.mark.parametrize("adapter_class", CLUSTER_ADAPTERS)
@pytest.mark.asyncio
async def test_cluster_clients_connect_under_a_default_timeout(mocker, adapter_class: Any):
    cluster_class = mocker.patch.object(adapter_class, "_cluster_class")
    async_cluster_class = mocker.patch.object(adapter_class, "_async_cluster_class")
    mocker.patch.object(adapter_class, "_clusters", {})
    mocker.patch.object(adapter_class, "_async_clusters", weakref.WeakKeyDictionary())
    adapter = adapter_class(["redis://node-a:7000"])

    adapter.get_client()
    await adapter.get_async_client()

    calls = [cluster_class.from_url.call_args, async_cluster_class.from_url.call_args]
    assert [call.kwargs["socket_connect_timeout"] for call in calls] == [5, 5]


def _build(pool_class: Any) -> ValkeyPySentinelAdapter:
    return ValkeyPySentinelAdapter(
        ["redis://mymaster/0"],
        pool_class=pool_class,
        sentinels=[("sentinel-a", 26379)],
    )


@requires_valkey
def test_a_plain_pool_class_is_rejected():
    with pytest.raises(ImproperlyConfigured, match="cannot serve a Sentinel cache"):
        _build("valkey.connection.ConnectionPool")


@requires_valkey
def test_a_sentinel_pool_subclass_is_honoured():
    from valkey.sentinel import SentinelConnectionPool

    class CustomSentinelPool(SentinelConnectionPool):
        pass

    adapter = _build(CustomSentinelPool)

    assert adapter._sentinel_pool_class is CustomSentinelPool


@requires_valkey
def test_omitting_pool_class_keeps_the_driver_default():
    from valkey.sentinel import SentinelConnectionPool

    adapter = ValkeyPySentinelAdapter(
        ["redis://mymaster/0"],
        sentinels=[("sentinel-a", 26379)],
    )

    assert adapter._sentinel_pool_class is SentinelConnectionPool


@requires_valkey
def test_a_plain_async_pool_class_is_rejected():
    with pytest.raises(ImproperlyConfigured, match=r"async_pool_class .* cannot serve a Sentinel cache"):
        ValkeyPySentinelAdapter(
            ["redis://mymaster/0"],
            async_pool_class="valkey.asyncio.ConnectionPool",
            sentinels=[("sentinel-a", 26379)],
        )


@requires_valkey
def test_an_async_sentinel_pool_subclass_is_honoured():
    from valkey.asyncio.sentinel import SentinelConnectionPool

    class CustomAsyncSentinelPool(SentinelConnectionPool):
        pass

    adapter = ValkeyPySentinelAdapter(
        ["redis://mymaster/0"],
        async_pool_class=CustomAsyncSentinelPool,
        sentinels=[("sentinel-a", 26379)],
    )

    assert adapter._async_sentinel_pool_class is CustomAsyncSentinelPool
    assert adapter._async_pool_class is None


@requires_valkey
@pytest.mark.asyncio
async def test_the_async_pool_class_builds_the_pool(monkeypatch: pytest.MonkeyPatch):
    from valkey.asyncio.sentinel import SentinelConnectionPool

    built: list[str] = []

    class CustomAsyncSentinelPool(SentinelConnectionPool):
        @classmethod
        def from_url(cls, url: str, **kwargs: Any) -> Any:
            built.append(url)
            return object()

    monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_pools", weakref.WeakKeyDictionary())
    adapter = ValkeyPySentinelAdapter(
        ["redis://mymaster/0"],
        async_pool_class=CustomAsyncSentinelPool,
        sentinels=[("sentinel-a", 26379)],
    )

    adapter._get_async_connection_pool(write=True)

    assert built == ["redis://mymaster/0"]


# LOCATION names one Sentinel service; Sentinel discovers the replicas.
@requires_valkey
def test_several_locations_are_rejected():
    with pytest.raises(ImproperlyConfigured, match=r"single LOCATION URL .* got 2 entries"):
        ValkeyPySentinelAdapter(
            ["redis://mymaster/0", "redis://other/0"],
            sentinels=[("sentinel-a", 26379)],
        )


@requires_valkey
def test_one_location_becomes_a_primary_and_a_replica_url():
    adapter = ValkeyPySentinelAdapter(["redis://mymaster/0"], sentinels=[("sentinel-a", 26379)])

    assert adapter._servers == ["redis://mymaster/0?is_master=1", "redis://mymaster/0?is_master=0"]


@requires_valkey
def test_async_pool_targets_are_computed_once():
    adapter = ValkeyPySentinelAdapter(["redis://mymaster/0"], sentinels=[("sentinel-a", 26379)])
    parsed: list[int] = []
    original = adapter._parse_sentinel_url
    adapter._parse_sentinel_url = lambda index: parsed.append(index) or original(index)

    keys = [adapter._async_pool_key(0), adapter._async_pool_key(0), adapter._async_pool_key(1)]

    assert parsed == [0, 1]
    assert keys[0] is keys[1]
    assert keys[0] != keys[2]


class _TypeClient:
    """Driver stub whose TYPE reply is whatever the test hands it."""

    def __init__(self, reply: str) -> None:
        self.reply = reply

    def type(self, key: str) -> str:
        del key
        return self.reply


class _AsyncTypeClient(_TypeClient):
    async def type(self, key: str) -> str:
        return super().type(key)


def _type_adapter(client: Any) -> ValkeyPyAdapter:
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    adapter.get_client = lambda key=None, *, write=False: client

    async def get_async_client(key: Any = None, *, write: bool = False) -> Any:
        del key, write
        return client

    adapter.get_async_client = get_async_client
    return adapter


@pytest.mark.parametrize("reply", ["string", "list", "set", "zset", "hash", "stream"])
def test_modelled_types_map_to_their_member(reply: str):
    assert _type_adapter(_TypeClient(reply)).type("key") == KeyType(reply)


def test_a_missing_key_is_none():
    assert _type_adapter(_TypeClient("none")).type("key") is None


@pytest.mark.parametrize("reply", ["ReJSON-RL", "TSDB-TYPE", "MBbloom--"])
def test_module_types_map_to_unknown(reply: str):
    assert _type_adapter(_TypeClient(reply)).type("key") is KeyType.UNKNOWN


@pytest.mark.asyncio
async def test_async_module_types_map_to_unknown():
    assert await _type_adapter(_AsyncTypeClient("ReJSON-RL")).atype("key") is KeyType.UNKNOWN


@pytest.mark.asyncio
async def test_async_missing_key_is_none():
    assert await _type_adapter(_AsyncTypeClient("none")).atype("key") is None


class _ScriptClient:
    """Driver stub with a server-side script cache: EVALSHA fails until SCRIPT LOAD."""

    def __init__(self) -> None:
        self.loaded: set[str] = set()
        self.calls: list[tuple[str, Any]] = []

    def evalsha(self, sha: str, numkeys: int, *keys_and_args: Any) -> Any:
        from valkey.exceptions import NoScriptError

        self.calls.append(("evalsha", sha))
        if sha not in self.loaded:
            raise NoScriptError("No matching script.")
        return (numkeys, keys_and_args)

    def script_load(self, script: str) -> str:
        sha = script_sha(script)
        self.calls.append(("script_load", sha))
        self.loaded.add(sha)
        return sha

    def eval(self, *args: Any) -> Any:
        raise AssertionError("EVAL must not be sent; the adapter uses EVALSHA")


class _AsyncScriptClient(_ScriptClient):
    async def evalsha(self, sha: str, numkeys: int, *keys_and_args: Any) -> Any:
        return super().evalsha(sha, numkeys, *keys_and_args)

    async def script_load(self, script: str) -> str:
        return super().script_load(script)


@requires_valkey
def test_loads_once_then_evalsha_only():
    client = _ScriptClient()
    adapter = _type_adapter(client)
    sha = script_sha("return 1")

    assert adapter.eval("return 1", 1, "k", "v") == (1, ("k", "v"))
    assert adapter.eval("return 1", 1, "k", "v") == (1, ("k", "v"))

    assert client.calls == [("evalsha", sha), ("script_load", sha), ("evalsha", sha), ("evalsha", sha)]


@requires_valkey
def test_reloads_after_the_server_forgets():
    client = _ScriptClient()
    adapter = _type_adapter(client)
    adapter.eval("return 1", 0)
    client.loaded.clear()  # SCRIPT FLUSH / restart / failover to a fresh node
    assert adapter.eval("return 1", 0) == (0, ())
    assert client.calls[-2:] == [("script_load", script_sha("return 1")), ("evalsha", script_sha("return 1"))]


@requires_valkey
@pytest.mark.asyncio
async def test_async_loads_once_then_evalsha_only():
    client = _AsyncScriptClient()
    adapter = _type_adapter(client)
    sha = script_sha("return 2")

    assert await adapter.aeval("return 2", 0) == (0, ())
    assert await adapter.aeval("return 2", 0) == (0, ())

    assert client.calls == [("evalsha", sha), ("script_load", sha), ("evalsha", sha), ("evalsha", sha)]


def _sentinel_modules(adapter_class: Any) -> tuple[Any, Any]:
    lib = adapter_class._lib.__name__
    return importlib.import_module(f"{lib}.sentinel"), importlib.import_module(f"{lib}.asyncio.sentinel")


_SENTINEL_DRIVERS = [
    pytest.param(RedisPySentinelAdapter, "redis", "rediss", id="redis-py"),
    pytest.param(ValkeyPySentinelAdapter, "valkey", "valkeys", id="valkey-py"),
]


def _sentinel_scheme_adapter(adapter_class: Any, scheme: str) -> Any:
    return adapter_class([f"{scheme}://mymaster/0"], sentinels=[("sentinel-a", 26379)])


@requires_valkey
@pytest.mark.parametrize(("adapter_class", "plain_scheme", "tls_scheme"), _SENTINEL_DRIVERS)
def test_tls_url_keeps_a_sentinel_managed_connection(
    adapter_class: Any,
    plain_scheme: str,
    tls_scheme: str,
):
    # Regression: the driver's parse_url injected a plain SSLConnection,
    # whose host is the literal service name, so Sentinel was never asked
    # for the primary and failover went unnoticed.
    del plain_scheme
    sync_sentinel, _ = _sentinel_modules(adapter_class)

    pool = _sentinel_scheme_adapter(adapter_class, tls_scheme)._get_connection_pool(write=True)

    assert pool.connection_class is sync_sentinel.SentinelManagedSSLConnection
    assert issubclass(pool.connection_class, sync_sentinel.SentinelManagedConnection)


@requires_valkey
@pytest.mark.parametrize(("adapter_class", "plain_scheme", "tls_scheme"), _SENTINEL_DRIVERS)
def test_plain_url_keeps_the_plain_managed_connection(
    adapter_class: Any,
    plain_scheme: str,
    tls_scheme: str,
):
    del tls_scheme
    sync_sentinel, _ = _sentinel_modules(adapter_class)

    pool = _sentinel_scheme_adapter(adapter_class, plain_scheme)._get_connection_pool(write=True)

    assert pool.connection_class is sync_sentinel.SentinelManagedConnection


@requires_valkey
@pytest.mark.parametrize(("adapter_class", "plain_scheme", "tls_scheme"), _SENTINEL_DRIVERS)
@pytest.mark.asyncio
async def test_async_tls_url_keeps_a_sentinel_managed_connection(
    adapter_class: Any,
    plain_scheme: str,
    tls_scheme: str,
    monkeypatch: pytest.MonkeyPatch,
):
    del plain_scheme
    _, async_sentinel = _sentinel_modules(adapter_class)
    monkeypatch.setattr(adapter_class, "_async_pools", weakref.WeakKeyDictionary())
    adapter = _sentinel_scheme_adapter(adapter_class, tls_scheme)

    pool = adapter._get_async_connection_pool(write=True)
    try:
        assert pool.connection_class is async_sentinel.SentinelManagedSSLConnection
        assert issubclass(pool.connection_class, async_sentinel.SentinelManagedConnection)
    finally:
        await adapter.aclose()


@requires_valkey
@pytest.mark.parametrize(("adapter_class", "plain_scheme", "tls_scheme"), _SENTINEL_DRIVERS)
@pytest.mark.asyncio
async def test_async_plain_url_keeps_the_plain_managed_connection(
    adapter_class: Any,
    plain_scheme: str,
    tls_scheme: str,
    monkeypatch: pytest.MonkeyPatch,
):
    del tls_scheme
    _, async_sentinel = _sentinel_modules(adapter_class)
    monkeypatch.setattr(adapter_class, "_async_pools", weakref.WeakKeyDictionary())
    adapter = _sentinel_scheme_adapter(adapter_class, plain_scheme)

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


def _aclose_adapter(*servers: str) -> ValkeyPyAdapter:
    adapter = ValkeyPyAdapter(list(servers))
    adapter._async_pool_class = _AsyncPoolClassStub
    return adapter


@requires_valkey
@pytest.mark.asyncio
async def test_other_aliases_keep_their_pools(monkeypatch: pytest.MonkeyPatch):
    # Regression: the whole loop slot was popped, so closing one alias
    # disconnected the pool another alias had connections checked out of.
    monkeypatch.setattr(ValkeyPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
    first = _aclose_adapter("valkey://one:6379/0")
    second = _aclose_adapter("valkey://two:6379/0")

    first_pool = first._get_async_connection_pool(write=True)
    second_pool = second._get_async_connection_pool(write=True)
    await first.aclose()

    assert first_pool.closed == 1
    assert second_pool.closed == 0
    assert second._get_async_connection_pool(write=True) is second_pool


@requires_valkey
@pytest.mark.asyncio
async def test_every_server_of_the_alias_is_closed(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(ValkeyPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
    adapter = _aclose_adapter("valkey://primary:6379/0", "valkey://replica:6379/0")

    pools = [adapter._get_async_connection_pool(write=write) for write in (True, False)]
    await adapter.aclose()

    assert [pool.closed for pool in pools] == [1, 1]


@requires_valkey
@pytest.mark.asyncio
async def test_second_aclose_is_a_no_op(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(ValkeyPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
    adapter = _aclose_adapter("valkey://one:6379/0")

    pool = adapter._get_async_connection_pool(write=True)
    await adapter.aclose()
    await adapter.aclose()

    assert pool.closed == 1


@requires_valkey
@pytest.mark.asyncio
async def test_a_failing_pool_leaves_the_rest_registered(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(ValkeyPyAdapter, "_async_pools", weakref.WeakKeyDictionary())
    adapter = _aclose_adapter("valkey://primary:6379/0", "valkey://replica:6379/0")
    primary, replica = (adapter._get_async_connection_pool(write=write) for write in (True, False))

    async def fail() -> None:
        msg = "socket already gone"
        raise OSError(msg)

    primary.aclose = fail

    with pytest.raises(OSError, match="socket already gone"):
        await adapter.aclose()

    assert replica.closed == 0
    assert adapter._get_async_connection_pool(write=False) is replica
    await adapter.aclose()
    assert replica.closed == 1


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


def _discovery_adapter() -> ValkeyPySentinelAdapter:
    adapter = ValkeyPySentinelAdapter.__new__(ValkeyPySentinelAdapter)
    adapter._servers = ["redis://mymaster/0?is_master=1"]
    adapter._options = {"sentinels": [("sentinel-a", 26379)]}
    adapter._pool_options = {"socket_timeout": 5}
    adapter._async_sentinels = weakref.WeakKeyDictionary()
    return adapter


@requires_valkey
@pytest.mark.asyncio
async def test_another_instance_closes_the_creators_clients(monkeypatch: pytest.MonkeyPatch):
    # Regression: aclose() read the per-instance _async_sentinels, but
    # asgiref hands every task a fresh adapter, so the discovery clients
    # of the instance that built the pool stayed open.
    monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_sentinel_pool_class", _StubAsyncSentinelPool)
    monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_sentinel_class", _StubAsyncSentinel)
    monkeypatch.setattr(ValkeyPySentinelAdapter, "_async_pools", weakref.WeakKeyDictionary())

    pool = _discovery_adapter()._get_async_connection_pool(write=True)
    await _discovery_adapter().aclose()

    assert pool.closed == 1
    assert [client.closed for client in pool.sentinel_manager.sentinels] == [1]


class _DeadConnection:
    """Driver connection stub whose socket has dropped."""

    _sock = None

    def can_read(self, timeout: float = 0) -> bool:
        msg = "can_read reconnects through the driver; poll must not get here"
        raise AssertionError(msg)


def test_dropped_socket_raises():
    # Regression: ``can_read`` on a connection without a socket calls
    # ``connect()``, which comes back without CLIENT TRACKING and under a
    # new client id, so invalidations were silently lost from then on.
    listener = _ValkeyPyInvalidationListener.__new__(_ValkeyPyInvalidationListener)
    listener._buffered = deque()
    listener._conn = _DeadConnection()

    with pytest.raises(ConnectionError, match="lost its connection"):
        listener.poll(0.01)


def test_buffered_pushes_are_still_drained():
    listener = _ValkeyPyInvalidationListener.__new__(_ValkeyPyInvalidationListener)
    listener._buffered = deque([Invalidation(keys=("k",))])
    listener._conn = _DeadConnection()

    assert listener.poll(0.01) == Invalidation(keys=("k",))


class _SetClient:
    """Driver stub recording SET / UNLINK calls."""

    def __init__(self, reply: Any = True) -> None:
        self.reply = reply
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def set(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("set", args, kwargs))
        return self.reply

    def unlink(self, *args: Any) -> int:
        self.calls.append(("unlink", args, {}))
        return 1


class _AsyncSetClient(_SetClient):
    async def set(self, *args: Any, **kwargs: Any) -> Any:
        return super().set(*args, **kwargs)

    async def unlink(self, *args: Any) -> int:
        return super().unlink(*args)


def _set_adapter(client: Any) -> ValkeyPyAdapter:
    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    adapter._stampede_config = None
    adapter.get_client = lambda key=None, *, write=False: client

    async def get_async_client(key: Any = None, *, write: bool = False) -> Any:
        return client

    adapter.get_async_client = get_async_client
    return adapter


# timeout=0 with NX/XX/GET is one SET with a past deadline, never SET then UNLINK.
def test_add_sends_one_set():
    client = _SetClient(reply=True)

    assert _set_adapter(client).add("k", b"v", 0) is True
    assert client.calls == [("set", ("k", b"v"), {"nx": True, "pxat": 1})]


def test_add_reports_an_existing_key():
    client = _SetClient(reply=None)

    assert _set_adapter(client).add("k", b"v", 0) is False
    assert [name for name, *_ in client.calls] == ["set"]


@pytest.mark.parametrize(
    ("flags", "reply", "expected"),
    [
        ({"nx": True}, True, True),
        ({"xx": True}, None, False),
        ({"get": True}, b"old", b"old"),
        ({"xx": True, "get": True}, None, None),
    ],
)
def test_set_with_flags_sends_one_set(flags: dict[str, Any], reply: Any, expected: Any):
    client = _SetClient(reply=reply)

    result = _set_adapter(client).set_with_flags("k", b"v", 0, **flags)

    assert result == expected
    ((name, args, kwargs),) = client.calls
    assert (name, args) == ("set", ("k", b"v"))
    assert kwargs == {
        "nx": flags.get("nx", False),
        "xx": flags.get("xx", False),
        "get": flags.get("get", False),
        "pxat": 1,
    }


@pytest.mark.asyncio
async def test_async_twins_send_one_set():
    client = _AsyncSetClient(reply=b"old")
    adapter = _set_adapter(client)

    assert await adapter.aadd("k", b"v", 0) is True
    assert await adapter.aset_with_flags("k", b"v", 0, get=True) == b"old"
    assert [name for name, *_ in client.calls] == ["set", "set"]
    assert all(kwargs["pxat"] == 1 for _, _, kwargs in client.calls)


class _UnlinkClusterClient:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def unlink(self, *keys: Any) -> int:
        self.calls.append(keys)
        return len(keys)


class _AsyncUnlinkClusterClient(_UnlinkClusterClient):
    async def unlink(self, *keys: Any) -> int:
        return super().unlink(*keys)


# The cluster client splits UNLINK by slot itself; the adapter sends every key at once.
def test_delete_many_is_one_call():
    client = _UnlinkClusterClient()
    adapter = ValkeyPyClusterAdapter.__new__(ValkeyPyClusterAdapter)
    adapter.get_client = lambda key=None, *, write=False: client

    assert adapter.delete_many(["{a}1", "{b}2", "{c}3"]) == 3
    assert client.calls == [("{a}1", "{b}2", "{c}3")]


@pytest.mark.asyncio
async def test_async_delete_many_is_one_call():
    client = _AsyncUnlinkClusterClient()
    adapter = ValkeyPyClusterAdapter.__new__(ValkeyPyClusterAdapter)

    async def get_async_client(key: Any = None, *, write: bool = False) -> Any:
        return client

    adapter.get_async_client = get_async_client

    assert await adapter.adelete_many(["{a}1", "{b}2"]) == 2
    assert client.calls == [("{a}1", "{b}2")]


# The driver maps a nil XREAD reply to an empty container; the adapter returns {}.
@pytest.mark.parametrize("reply", [[], {}], ids=["resp2", "resp3"])
def test_empty_read_is_an_empty_dict(reply: Any):
    class StubClient:
        def xread(self, **kwargs: Any) -> Any:
            return reply

        def xreadgroup(self, **kwargs: Any) -> Any:
            return reply

    adapter = ValkeyPyAdapter.__new__(ValkeyPyAdapter)
    adapter.get_client = lambda key=None, *, write=False: StubClient()

    assert adapter.xread({"s": "$"}, block=1) == {}
    assert adapter.xreadgroup("g", "c", {"s": ">"}, block=1) == {}
