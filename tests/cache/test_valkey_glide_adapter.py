"""Mock-based unit tests for the valkey-glide adapter (no server; skipped
on interpreters without a glide wheel, e.g. free-threaded cp314t)."""

import asyncio

import pytest

pytest.importorskip("glide_sync")
pytest.importorskip("glide")

from glide_sync import ServerCredentials

from django_cachex.adapters.protocols import _RespPipelineCommandsProtocol
from django_cachex.adapters.valkey_glide import (
    ValkeyGlideAdapter,
    ValkeyGlidePipelineAdapter,
    _AsyncGlideLock,
    _glide_config_kwargs,
    _GlideLock,
    _WrongTypeClient,
)


def _adapter(mocker):
    """Adapter with a mocked sync client (no __init__: no registry, no server)."""
    adapter = ValkeyGlideAdapter.__new__(ValkeyGlideAdapter)
    client = mocker.Mock()
    mocker.patch.object(ValkeyGlideAdapter, "_client", return_value=client)
    return adapter, client


# ------------------------------------------------------ zadd GT/LT forwarding


def test_zadd_forwards_gt_flag(mocker):
    adapter, client = _adapter(mocker)
    adapter.zadd("k", {b"m": 2.0}, gt=True, ch=True)
    assert client.custom_command.call_args[0][0] == [b"ZADD", "k", b"GT", b"CH", b"2.0", b"m"]


def test_zadd_forwards_lt_flag(mocker):
    adapter, client = _adapter(mocker)
    adapter.zadd("k", {b"m": 2.0}, lt=True)
    assert client.custom_command.call_args[0][0] == [b"ZADD", "k", b"LT", b"2.0", b"m"]


def test_zadd_orders_gt_after_xx(mocker):
    adapter, client = _adapter(mocker)
    adapter.zadd("k", {b"m": 2.0}, xx=True, gt=True)
    assert client.custom_command.call_args[0][0] == [b"ZADD", "k", b"XX", b"GT", b"2.0", b"m"]


@pytest.mark.asyncio
async def test_azadd_forwards_gt_flag(mocker):
    adapter = ValkeyGlideAdapter.__new__(ValkeyGlideAdapter)
    client = mocker.AsyncMock()
    mocker.patch.object(ValkeyGlideAdapter, "get_async_client", mocker.AsyncMock(return_value=client))
    await adapter.azadd("k", {b"m": 2.0}, gt=True)
    assert client.custom_command.await_args[0][0] == [b"ZADD", "k", b"GT", b"2.0", b"m"]


def test_pipeline_zadd_forwards_gt_flag(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.zadd("k", {b"m": 2.0}, gt=True)
    assert pipe._batch.commands[-1][1] == [b"ZADD", "k", b"GT", b"2.0", b"m"]


# ------------------------------------------- Pipeline stream command coverage

# A method left to the protocol's ``...`` stub enqueues nothing while
# satisfying attribute lookup, desyncing the per-command decoder queue.
_PROTOCOL_METHODS = sorted(
    name for name, value in vars(_RespPipelineCommandsProtocol).items() if callable(value) and not name.startswith("_")
)


@pytest.mark.parametrize("name", _PROTOCOL_METHODS)
def test_pipeline_implements_protocol_method(name):
    # Regression: stream commands fell through to the protocol's stubs.
    assert name in vars(ValkeyGlidePipelineAdapter), f"{name} falls through to the protocol stub"


_STREAM_CALLS = [
    ("xack", ("k", "g", "1-1")),
    ("xclaim", ("k", "g", "c", 0, ["1-1"])),
    ("xautoclaim", ("k", "g", "c", 0)),
    ("xgroup_create", ("k", "g")),
    ("xgroup_destroy", ("k", "g")),
    ("xgroup_setid", ("k", "g", "0-0")),
    ("xgroup_delconsumer", ("k", "g", "c")),
    ("xinfo_stream", ("k",)),
    ("xinfo_groups", ("k",)),
    ("xinfo_consumers", ("k", "g")),
]


@pytest.mark.parametrize(("name", "args"), _STREAM_CALLS)
def test_pipeline_stream_command_enqueues_one_command(mocker, name, args):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    result = getattr(pipe, name)(*args)
    assert result is pipe
    assert len(pipe._batch.commands) == 1


def test_pipeline_stream_commands_keep_decoders_aligned(mocker):
    # Regression: a stub enqueueing nothing shifted every later result.
    client = mocker.Mock()
    client.exec.return_value = [b"v1", "OK", 1, b"v2"]
    pipe = ValkeyGlidePipelineAdapter(client, transaction=False)
    pipe.get("a").xgroup_create("s", "g", mkstream=True).xack("s", "g", "1-1").get("b")
    assert pipe.execute() == [b"v1", True, 1, b"v2"]


def test_pipeline_xclaim_decodes_entries(mocker):
    client = mocker.Mock()
    client.exec.return_value = [{b"1-1": [[b"f", b"v"]]}]
    pipe = ValkeyGlidePipelineAdapter(client, transaction=False)
    pipe.xclaim("k", "g", "c", 0, ["1-1"])
    assert pipe.execute() == [[("1-1", {"f": b"v"})]]


def test_pipeline_xclaim_justid_decodes_ids(mocker):
    client = mocker.Mock()
    client.exec.return_value = [[b"1-1", b"2-2"]]
    pipe = ValkeyGlidePipelineAdapter(client, transaction=False)
    pipe.xclaim("k", "g", "c", 0, ["1-1"], justid=True)
    assert pipe.execute() == [["1-1", "2-2"]]


def test_pipeline_xautoclaim_shapes_result(mocker):
    client = mocker.Mock()
    client.exec.return_value = [[b"3-0", {b"1-1": [[b"f", b"v"]]}, [b"2-2"]]]
    pipe = ValkeyGlidePipelineAdapter(client, transaction=False)
    pipe.xautoclaim("k", "g", "c", 0)
    assert pipe.execute() == [["3-0", [("1-1", {"f": b"v"})], ["2-2"]]]


def test_pipeline_xautoclaim_justid_returns_flat_ids(mocker):
    client = mocker.Mock()
    client.exec.return_value = [[b"3-0", [b"1-1", b"2-2"], []]]
    pipe = ValkeyGlidePipelineAdapter(client, transaction=False)
    pipe.xautoclaim("k", "g", "c", 0, justid=True)
    assert pipe.execute() == [["1-1", "2-2"]]


def test_pipeline_xadd_honors_kwargs(mocker):
    # Regression: maxlen/approximate/nomkstream/limit were dropped.
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.xadd("s", {"f": b"v"}, id="1-1", maxlen=5, approximate=True, nomkstream=True, limit=2)
    assert pipe._batch.commands[-1][1] == [
        b"XADD",
        "s",
        b"NOMKSTREAM",
        b"MAXLEN",
        b"~",
        b"5",
        b"LIMIT",
        b"2",
        "1-1",
        "f",
        b"v",
    ]


def test_pipeline_xadd_exact_maxlen(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.xadd("s", {"f": b"v"}, maxlen=5, approximate=False)
    assert pipe._batch.commands[-1][1] == [b"XADD", "s", b"MAXLEN", b"5", "*", "f", b"v"]


# -------------------------------------------- Lock token and blocking timeout


def test_lock_generates_fresh_token_per_acquire(mocker):
    # Regression: a token minted once in __init__ let a stale holder
    # release a lock re-acquired under the same token.
    client = mocker.Mock()
    client.set.return_value = "OK"
    client.custom_command.return_value = 1
    lock = _GlideLock(client, "k", lease=1.0)

    assert lock.acquire()
    first = lock._token
    lock.release()
    assert lock.acquire()
    second = lock._token

    assert first != second
    assert client.set.call_args[0][1] == second


def test_lock_blocking_timeout_zero_tries_once(mocker):
    client = mocker.Mock()
    client.set.return_value = None
    sleep = mocker.patch("django_cachex.adapters.valkey_glide.time.sleep")
    lock = _GlideLock(client, "k", blocking=True, timeout=0)

    assert lock.acquire() is False
    assert client.set.call_count == 1
    sleep.assert_not_called()


def test_lock_blocking_sleeps_between_attempts(mocker):
    client = mocker.Mock()
    client.set.side_effect = [None, "OK"]
    sleep = mocker.patch("django_cachex.adapters.valkey_glide.time.sleep")
    lock = _GlideLock(client, "k", sleep=0.05, blocking=True)

    assert lock.acquire() is True
    sleep.assert_called_once_with(0.05)


def test_async_lock_generates_fresh_token_per_acquire(mocker):
    client = mocker.AsyncMock()
    client.set.return_value = "OK"
    client.custom_command.return_value = 1
    adapter = mocker.Mock()
    adapter.get_async_client = mocker.AsyncMock(return_value=client)
    lock = _AsyncGlideLock(adapter, "k", lease=1.0)

    async def scenario():
        assert await lock.acquire()
        first = lock._token
        await lock.release()
        assert await lock.acquire()
        return first, lock._token

    first, second = asyncio.run(scenario())
    assert first != second


def test_async_lock_blocking_timeout_zero_tries_once(mocker):
    client = mocker.AsyncMock()
    client.set.return_value = None
    adapter = mocker.Mock()
    adapter.get_async_client = mocker.AsyncMock(return_value=client)
    lock = _AsyncGlideLock(adapter, "k", blocking=True, timeout=0)

    assert asyncio.run(lock.acquire()) is False
    assert client.set.await_count == 1


# ---------------------------------------------- URL / OPTIONS to glide config


def test_config_kwargs_parses_url_tls_auth_db():
    kwargs = _glide_config_kwargs(
        ["valkeys://user:secret@example.com:7000/3"],
        {},
        credentials_cls=ServerCredentials,
    )
    assert kwargs["use_tls"] is True
    assert kwargs["database_id"] == 3
    assert kwargs["credentials"].username == "user"
    assert kwargs["credentials"].password == "secret"


def test_config_kwargs_options_override_url():
    kwargs = _glide_config_kwargs(
        ["redis://user:urlpw@h:6379?db=2"],
        {"password": "optpw", "db": 9, "request_timeout": 250, "client_name": "cx"},
        credentials_cls=ServerCredentials,
    )
    assert "use_tls" not in kwargs
    assert kwargs["database_id"] == 9
    assert kwargs["credentials"].username == "user"
    assert kwargs["credentials"].password == "optpw"
    assert kwargs["request_timeout"] == 250
    assert kwargs["client_name"] == "cx"


def test_config_kwargs_ssl_option_enables_tls():
    kwargs = _glide_config_kwargs(["redis://h:6379"], {"ssl": True}, credentials_cls=ServerCredentials)
    assert kwargs["use_tls"] is True


def test_config_kwargs_cluster_drops_database():
    kwargs = _glide_config_kwargs(
        ["redis://h:6379/3"],
        {},
        credentials_cls=ServerCredentials,
        include_database=False,
    )
    assert "database_id" not in kwargs


def test_sync_client_applies_config_kwargs(mocker):
    import django_cachex.adapters.valkey_glide as vg

    mocker.patch.dict(vg._GLIDE_SYNC_CLIENTS, clear=True)
    config_cls = mocker.patch.object(vg, "GlideClientConfiguration")
    mocker.patch.object(vg, "GlideClient")
    adapter = ValkeyGlideAdapter(["valkeys://user:urlpw@example.com:7000/2"], password="secret")  # noqa: S106

    adapter._client()

    kwargs = config_cls.call_args.kwargs
    assert kwargs["use_tls"] is True
    assert kwargs["database_id"] == 2
    assert kwargs["credentials"].username == "user"
    assert kwargs["credentials"].password == "secret"


# ---------------------------------------------------- xpending IDLE placement


def test_xpending_range_places_idle_before_range(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = []
    adapter.xpending("k", "g", start="-", end="+", count=10, idle=5000)
    assert client.custom_command.call_args[0][0] == [b"XPENDING", "k", "g", b"IDLE", b"5000", "-", "+", b"10"]


def test_xpending_summary_ignores_idle(mocker):
    # Regression: XPENDING key group IDLE n (no range) is a syntax error.
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = [0, None, None, []]
    adapter.xpending("k", "g", idle=5000)
    assert client.custom_command.call_args[0][0] == [b"XPENDING", "k", "g"]


# ------------------------------------------------- sscan cursor normalization


def test_sscan_returns_int_cursor(mocker):
    adapter, client = _adapter(mocker)
    client.sscan.return_value = [b"42", [b"a", b"b"]]
    cursor, members = adapter.sscan("k")
    assert cursor == 42
    assert isinstance(cursor, int)
    assert members == {b"a", b"b"}


# -------------------------------------------------- slowlog_get normalization


def test_slowlog_get_returns_normalized_dicts(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = [
        [7, 1710000000, 15, [b"GET", b"x"], b"127.0.0.1:50", b"worker"],
        [8, 1710000001, 20, [b"SET", b"y", b"1"]],
    ]
    assert adapter.slowlog_get(10) == [
        {
            "id": 7,
            "start_time": 1710000000,
            "duration": 15,
            "command": ["GET", "x"],
            "client_address": "127.0.0.1:50",
            "client_name": "worker",
        },
        {
            "id": 8,
            "start_time": 1710000001,
            "duration": 20,
            "command": ["SET", "y", "1"],
            "client_address": None,
            "client_name": None,
        },
    ]


def test_slowlog_get_empty(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = None
    assert adapter.slowlog_get() == []


# ------------------------------------------------- WRONGTYPE proxy dunders


def test_wrongtype_client_supports_sync_with(mocker):
    """Regression: dunders resolve on the type, so __getattr__ never saw
    __enter__/__exit__ and ``with cache.get_client()`` raised TypeError even
    though hasattr() reported the methods as present.
    """
    inner = mocker.MagicMock()
    proxy = _WrongTypeClient(inner)

    with proxy as entered:
        assert entered is proxy

    inner.__enter__.assert_called_once()
    inner.__exit__.assert_called_once()


def test_wrongtype_client_supports_async_with(mocker):
    inner = mocker.MagicMock()
    inner.__aenter__ = mocker.AsyncMock()
    inner.__aexit__ = mocker.AsyncMock()
    proxy = _WrongTypeClient(inner)

    async def run():
        async with proxy as entered:
            assert entered is proxy

    asyncio.run(run())

    inner.__aenter__.assert_awaited_once()
    inner.__aexit__.assert_awaited_once()
