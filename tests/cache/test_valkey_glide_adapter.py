"""Mock-based unit tests for the valkey-glide adapter (no server; skipped
on interpreters without a glide wheel, e.g. free-threaded cp314t)."""

import asyncio
import datetime
import inspect

import pytest

pytest.importorskip("glide_sync")
pytest.importorskip("glide")

from django.core.exceptions import ImproperlyConfigured
from glide_sync import (
    ClusterBatch,
    NodeAddress,
    RandomNode,
    ReadFrom,
    RequestError,
    ServerCredentials,
)

from django_cachex.adapters.protocols import (
    RespAdapterProtocol,
    RespPipelineProtocol,
    _RespPipelineCommandsProtocol,
)
from django_cachex.adapters.valkey_glide import (
    ValkeyGlideAdapter,
    ValkeyGlideClusterAdapter,
    ValkeyGlidePipelineAdapter,
    _AsyncGlideLock,
    _coerce_info_value,
    _glide_config_kwargs,
    _GlideLock,
    _node_addresses,
    _object_type,
    _parse_info,
    _WrongTypeClient,
)
from django_cachex.exceptions import CachexError, NotSupportedError
from django_cachex.lock import LockError
from django_cachex.types import KeyType


def _adapter(mocker):
    """Adapter with a mocked sync client (no __init__: no registry, no server)."""
    adapter = ValkeyGlideAdapter.__new__(ValkeyGlideAdapter)
    adapter._stampede_config = None
    client = mocker.Mock()
    mocker.patch.object(ValkeyGlideAdapter, "_client", return_value=client)
    return adapter, client


def _async_adapter(mocker, cls=ValkeyGlideAdapter):
    adapter = cls.__new__(cls)
    adapter._stampede_config = None
    client = mocker.AsyncMock()
    mocker.patch.object(cls, "get_async_client", mocker.AsyncMock(return_value=client))
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


def test_zadd_without_flags_keeps_bytes_members(mocker):
    # Regression: glide's native ``zadd`` builds args with ``str(member)``, so a
    # serialized member was stored as its repr and never matched again.
    adapter, client = _adapter(mocker)
    adapter.zadd("k", {b"\x80m": 2.0})
    assert client.zadd.call_count == 0
    assert client.custom_command.call_args[0][0] == [b"ZADD", "k", b"2.0", b"\x80m"]


@pytest.mark.asyncio
async def test_azadd_without_flags_keeps_bytes_members(mocker):
    adapter = ValkeyGlideAdapter.__new__(ValkeyGlideAdapter)
    client = mocker.AsyncMock()
    mocker.patch.object(ValkeyGlideAdapter, "get_async_client", mocker.AsyncMock(return_value=client))
    await adapter.azadd("k", {b"\x80m": 2.0})
    assert client.zadd.await_count == 0
    assert client.custom_command.await_args[0][0] == [b"ZADD", "k", b"2.0", b"\x80m"]


def test_pipeline_zadd_without_flags_keeps_bytes_members(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.zadd("k", {b"\x80m": 2.0})
    assert pipe._batch.commands[-1][1] == [b"ZADD", "k", b"2.0", b"\x80m"]


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
# satisfying attribute lookup, desyncing the decoder queue.
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
    # Regression: a token minted once in __init__ let a stale holder release a
    # lock re-acquired under the same token.
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


def test_config_kwargs_use_tls_option_enables_tls():
    kwargs = _glide_config_kwargs(["redis://h:6379"], {"use_tls": True}, credentials_cls=ServerCredentials)
    assert kwargs["use_tls"] is True


def test_config_kwargs_use_tls_option_overrides_the_url_scheme():
    kwargs = _glide_config_kwargs(["rediss://h:6379"], {"use_tls": False}, credentials_cls=ServerCredentials)
    assert "use_tls" not in kwargs


def test_config_kwargs_use_tls_wins_over_ssl():
    kwargs = _glide_config_kwargs(
        ["redis://h:6379"],
        {"use_tls": False, "ssl": True},
        credentials_cls=ServerCredentials,
    )
    assert "use_tls" not in kwargs


def test_config_kwargs_cluster_drops_database():
    kwargs = _glide_config_kwargs(
        ["redis://h:6379/3"],
        {},
        credentials_cls=ServerCredentials,
        standalone=False,
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


def test_xpending_rejects_a_filter_without_a_count(mocker):
    # XPENDING key group IDLE n with no range is a syntax error on the wire.
    adapter, _client = _adapter(mocker)
    with pytest.raises(ValueError, match="requires count"):
        adapter.xpending("k", "g", idle=5000)


def test_xpending_defaults_the_range_when_only_count_is_given(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = []
    adapter.xpending("k", "g", count=10)
    assert client.custom_command.call_args[0][0] == [b"XPENDING", "k", "g", "-", "+", b"10"]


def test_xpending_summary_returns_the_protocol_dict(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = [2, b"1-1", b"2-2", [[b"c1", b"2"]]]
    assert adapter.xpending("k", "g") == {
        "pending": 2,
        "min": "1-1",
        "max": "2-2",
        "consumers": [{"name": "c1", "pending": 2}],
    }


def test_xpending_range_returns_protocol_dicts(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = [[b"1-1", b"c1", 120, 3]]
    assert adapter.xpending("k", "g", count=10) == [
        {"message_id": "1-1", "consumer": "c1", "time_since_delivered": 120, "times_delivered": 3},
    ]


@pytest.mark.asyncio
async def test_axpending_summary_returns_the_protocol_dict(mocker):
    adapter, client = _async_adapter(mocker)
    client.custom_command.return_value = [0, None, None, []]
    assert await adapter.axpending("k", "g") == {"pending": 0, "min": None, "max": None, "consumers": []}


def test_pipeline_xpending_decodes_the_summary(mocker):
    client = mocker.Mock()
    client.exec.return_value = [[2, b"1-1", b"2-2", [[b"c1", b"2"]]]]
    pipe = ValkeyGlidePipelineAdapter(client, transaction=False)
    pipe.xpending("k", "g")
    assert pipe.execute() == [
        {"pending": 2, "min": "1-1", "max": "2-2", "consumers": [{"name": "c1", "pending": 2}]},
    ]


def test_pipeline_xpending_range_decodes_the_rows(mocker):
    client = mocker.Mock()
    client.exec.return_value = [[[b"1-1", b"c1", 120, 3]]]
    pipe = ValkeyGlidePipelineAdapter(client, transaction=False)
    pipe.xpending_range("k", "g", min="-", max="+", count=10)
    assert pipe.execute() == [
        [{"message_id": "1-1", "consumer": "c1", "time_since_delivered": 120, "times_delivered": 3}],
    ]


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


# ------------------------------------------ unknown-command translation

# What a pre-7.4 server answers when the adapter sends a hash field TTL command.
_UNKNOWN_HEXPIRE = (
    "An error was signalled by the server - ResponseError: unknown command "
    "'HEXPIRE', with args beginning with: h, 60, FIELDS, 1, a,"
)


def test_wrongtype_client_translates_unknown_command(mocker):
    original = RequestError(_UNKNOWN_HEXPIRE)
    inner = mocker.Mock()
    inner.custom_command.side_effect = original
    proxy = _WrongTypeClient(inner)

    with pytest.raises(NotSupportedError) as excinfo:
        proxy.custom_command([b"HEXPIRE", "h", b"60", b"FIELDS", b"1", "a"])

    assert excinfo.value.operation == "hexpire"
    assert "Redis 7.4+ or Valkey 9.0+" in str(excinfo.value)
    assert excinfo.value.__cause__ is original


def test_wrongtype_client_translates_unknown_command_on_await(mocker):
    """The async client raises on await, not on call, so the awaitable needs the same seam."""
    original = RequestError(_UNKNOWN_HEXPIRE)
    inner = mocker.MagicMock()
    inner.custom_command = mocker.AsyncMock(side_effect=original)
    proxy = _WrongTypeClient(inner)

    async def run():
        with pytest.raises(NotSupportedError) as excinfo:
            await proxy.custom_command([b"HEXPIRE", "h", b"60", b"FIELDS", b"1", "a"])
        return excinfo.value

    error = asyncio.run(run())

    assert error.operation == "hexpire"
    assert error.__cause__ is original


# ------------------------------------------------------ timedelta expiry args


def test_expire_converts_timedelta_to_seconds(mocker):
    adapter, client = _adapter(mocker)
    adapter.expire("k", datetime.timedelta(minutes=5))
    assert client.expire.call_args[0][1] == 300


def test_pexpire_converts_timedelta_to_milliseconds(mocker):
    adapter, client = _adapter(mocker)
    adapter.pexpire("k", datetime.timedelta(seconds=1.5))
    assert client.pexpire.call_args[0][1] == 1500


@pytest.mark.asyncio
async def test_aexpire_converts_timedelta_to_seconds(mocker):
    adapter, client = _async_adapter(mocker)
    await adapter.aexpire("k", datetime.timedelta(minutes=5))
    assert client.expire.await_args[0][1] == 300


@pytest.mark.asyncio
async def test_apexpire_converts_timedelta_to_milliseconds(mocker):
    adapter, client = _async_adapter(mocker)
    await adapter.apexpire("k", datetime.timedelta(seconds=2))
    assert client.pexpire.await_args[0][1] == 2000


def test_pipeline_expire_converts_timedelta(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.expire("k", datetime.timedelta(minutes=5))
    assert pipe._batch.commands[-1][1] == ["k", "300"]


def test_pipeline_pexpire_converts_timedelta(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.pexpire("k", datetime.timedelta(seconds=1))
    assert pipe._batch.commands[-1][1] == ["k", "1000"]


# ------------------------------------------------------- pipeline hset desync


def test_pipeline_hset_empty_payload_raises(mocker):
    # Regression: returning without queueing shifted every later result;
    # execute() zips results against decoders with strict=True.
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    with pytest.raises(ValueError, match="at least one field/value pair"):
        pipe.hset("h", mapping={})
    assert pipe._batch.commands == []


# Both branches of a queueing method must enqueue, or the decoder queue
# desyncs against the driver's results.
_BRANCHING_CALLS = [
    ("hset", ("h", "f", b"v"), {}),
    ("hset", ("h",), {"mapping": {"f": b"v"}}),
    ("hset", ("h",), {"items": ["f", b"v"]}),
    ("zadd", ("z", {b"m": 1.0}), {}),
    ("zadd", ("z", {b"m": 1.0}), {"gt": True}),
    ("spop", ("s",), {}),
    ("spop", ("s",), {"count": 2}),
    ("srandmember", ("s",), {}),
    ("srandmember", ("s",), {"count": 2}),
    ("lpop", ("l",), {}),
    ("lpop", ("l",), {"count": 2}),
    ("rpop", ("l",), {}),
    ("rpop", ("l",), {"count": 2}),
    ("zpopmin", ("z",), {}),
    ("zpopmax", ("z",), {"count": 2}),
    ("hmget", ("h", ["f1", "f2"]), {}),
    ("hmget", ("h", "f1", "f2"), {}),
    ("zrange", ("z", 0, -1), {"withscores": True, "desc": True}),
    ("zrevrange", ("z", 0, -1), {}),
    ("expire", ("k", 60), {}),
    ("pexpire", ("k", 60000), {}),
]


@pytest.mark.parametrize(("name", "args", "kwargs"), _BRANCHING_CALLS)
def test_pipeline_branching_method_enqueues_one_command(mocker, name, args, kwargs):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    assert getattr(pipe, name)(*args, **kwargs) is pipe
    assert len(pipe._batch.commands) == 1


# ------------------------------------------------- pipeline attribute typos


def test_pipeline_rejects_dunder_attribute(mocker):
    # ``copy.deepcopy`` looks __deepcopy__ up on the instance; a fallthrough
    # would queue a ``__DEEPCOPY__`` command.
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    with pytest.raises(AttributeError):
        pipe.__deepcopy__  # noqa: B018
    assert pipe._batch.commands == []


def test_pipeline_rejects_underscored_typo(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    with pytest.raises(AttributeError):
        pipe.hget_all("h")
    assert pipe._batch.commands == []


def test_pipeline_still_forwards_unknown_single_word_command(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.getdel("k")
    assert pipe._batch.commands[-1][1] == ["GETDEL", "k"]


# ---------------------------------------------------------- set_many atomicity


def test_set_many_with_timeout_sets_each_key_with_its_ttl(mocker):
    # Regression: MSET plus N EXPIREs left keys resident forever when the batch
    # broke partway.
    adapter, client = _adapter(mocker)
    adapter.set_many({"a": b"1", "b": b"2"}, 60)
    batch = client.exec.call_args[0][0]
    assert [args for _, args in batch.commands] == [
        ["a", b"1", "PX", "60000"],
        ["b", b"2", "PX", "60000"],
    ]


@pytest.mark.asyncio
async def test_aset_many_with_timeout_sets_each_key_with_its_ttl(mocker):
    adapter, client = _async_adapter(mocker)
    await adapter.aset_many({"a": b"1"}, 60)
    batch = client.exec.await_args[0][0]
    assert [args for _, args in batch.commands] == [["a", b"1", "PX", "60000"]]


# ------------------------------------------------------------ INFO parsing


def test_parse_info_nests_keyspace_rows():
    # The admin's Keyspace panel looks for ``isinstance(v, dict)`` under db*.
    parsed = _parse_info("# Keyspace\r\ndb0:keys=12,expires=3,avg_ttl=0\r\n")
    assert parsed["db0"] == {"keys": 12, "expires": 3, "avg_ttl": 0}


def test_coerce_info_value_prefers_int_over_float():
    assert _coerce_info_value("-1") == -1
    assert isinstance(_coerce_info_value("-1"), int)
    assert _coerce_info_value("1.5") == 1.5
    assert _coerce_info_value("6.2.1") == "6.2.1"


def test_parse_info_merges_multi_node_response():
    parsed = _parse_info(
        {
            b"127.0.0.1:7000": b"# Server\r\nredis_version:7.2.0\r\n",
            b"127.0.0.1:7001": b"# Keyspace\r\ndb0:keys=4,expires=1\r\n",
        },
    )
    assert parsed["redis_version"] == "7.2.0"
    assert parsed["db0"] == {"keys": 4, "expires": 1}


def test_cluster_info_pins_the_command_to_one_node(mocker):
    # Regression: an unrouted INFO takes glide's all-primaries default and
    # answers {node: payload}, which the string parser choked on.
    adapter = ValkeyGlideClusterAdapter.__new__(ValkeyGlideClusterAdapter)
    client = mocker.Mock()
    client.custom_command.return_value = b"# Server\r\nredis_version:7.2.0\r\n"
    mocker.patch.object(ValkeyGlideClusterAdapter, "_client", return_value=client)

    assert adapter.info()["redis_version"] == "7.2.0"
    args, route = client.custom_command.call_args[0]
    assert args == [b"INFO"]
    assert isinstance(route, RandomNode)


# --------------------------------------------------------------- SCAN sizing


def test_scan_applies_the_default_itersize(mocker):
    # count=None leaves the server default of 10; valkey-py uses 100.
    adapter, client = _adapter(mocker)
    client.scan.return_value = [b"0", []]
    adapter.scan()
    assert client.scan.call_args.kwargs["count"] == ValkeyGlideAdapter._default_scan_itersize


def test_iter_keys_applies_the_default_itersize(mocker):
    adapter, client = _adapter(mocker)
    client.scan.return_value = [b"0", []]
    list(adapter.iter_keys("*"))
    assert client.scan.call_args.kwargs["count"] == ValkeyGlideAdapter._default_scan_itersize


def test_scan_rejects_an_unknown_key_type(mocker):
    adapter, _client = _adapter(mocker)
    with pytest.raises(ValueError, match="Unknown key type"):
        adapter.scan(_type="strin")


def test_object_type_maps_known_names():
    assert _object_type(None) is None
    assert _object_type("string").value.lower() == "string"


# ------------------------------------------------------------ URL credentials


def test_config_kwargs_percent_decodes_credentials():
    # ``p@ss`` can only be expressed encoded; a literal ``p%40ss`` is a
    # WRONGPASS on the first command.
    kwargs = _glide_config_kwargs(
        ["rediss://us%2Fer:p%40ss@host:6379/0"],
        {},
        credentials_cls=ServerCredentials,
    )
    assert kwargs["credentials"].username == "us/er"
    assert kwargs["credentials"].password == "p@ss"


# ----------------------------------------------------------------- lock errors


def test_lock_enter_raises_lock_error(mocker):
    client = mocker.Mock()
    client.set.return_value = None
    lock = _GlideLock(client, "k", blocking=False)
    with pytest.raises(LockError, match="Could not acquire lock"):
        lock.__enter__()


def test_lock_extend_refuses_a_leaseless_lock(mocker):
    # Regression: PTTL is -1 without a lease and the Lua clamped it to 0, so
    # extend() made a never-expiring lock self-release.
    client = mocker.Mock()
    client.set.return_value = "OK"
    lock = _GlideLock(client, "k")
    assert lock.acquire()
    with pytest.raises(LockError, match="no lease"):
        lock.extend(30)
    client.custom_command.assert_not_called()


def test_lock_extend_still_works_with_a_lease(mocker):
    client = mocker.Mock()
    client.set.return_value = "OK"
    client.custom_command.return_value = 1
    lock = _GlideLock(client, "k", lease=10.0)
    assert lock.acquire()
    assert lock.extend(30) is True


def test_async_lock_enter_raises_lock_error(mocker):
    client = mocker.AsyncMock()
    client.set.return_value = None
    adapter = mocker.Mock()
    adapter.get_async_client = mocker.AsyncMock(return_value=client)
    lock = _AsyncGlideLock(adapter, "k", blocking=False)
    with pytest.raises(LockError, match="Could not acquire lock"):
        asyncio.run(lock.__aenter__())


def test_async_lock_extend_refuses_a_leaseless_lock(mocker):
    client = mocker.AsyncMock()
    client.set.return_value = "OK"
    adapter = mocker.Mock()
    adapter.get_async_client = mocker.AsyncMock(return_value=client)
    lock = _AsyncGlideLock(adapter, "k")

    async def scenario():
        assert await lock.acquire()
        await lock.extend(30)

    with pytest.raises(LockError, match="no lease"):
        asyncio.run(scenario())
    client.custom_command.assert_not_awaited()


# --------------------------------------------------------- async client close


def test_aclose_closes_and_drops_the_per_loop_client(mocker):
    # Regression: glide clients define no __del__, so a client dropped from the
    # registry never released its connection.
    import django_cachex.adapters.valkey_glide as vg

    adapter = ValkeyGlideAdapter.__new__(ValkeyGlideAdapter)
    adapter._config_key = ("cfg",)
    client = mocker.AsyncMock()

    async def scenario():
        loop = asyncio.get_running_loop()
        vg._GLIDE_ASYNC_CLIENTS[loop] = {("cfg",): client}
        await adapter.aclose()
        return vg._GLIDE_ASYNC_CLIENTS.get(loop)

    remaining = asyncio.run(scenario())
    client.close.assert_awaited_once()
    assert remaining == {}


def test_aclose_without_a_registered_client_is_quiet(mocker):
    adapter = ValkeyGlideAdapter.__new__(ValkeyGlideAdapter)
    adapter._config_key = ("missing",)
    del mocker
    asyncio.run(adapter.aclose())


def test_cluster_aclose_uses_the_cluster_registry(mocker):
    import django_cachex.adapters.valkey_glide as vg

    adapter = ValkeyGlideClusterAdapter.__new__(ValkeyGlideClusterAdapter)
    adapter._config_key = ("cfg",)
    client = mocker.AsyncMock()

    async def scenario():
        loop = asyncio.get_running_loop()
        vg._GLIDE_ASYNC_CLUSTER_CLIENTS[loop] = {("cfg",): client}
        await adapter.aclose()
        return vg._GLIDE_ASYNC_CLUSTER_CLIENTS.get(loop)

    remaining = asyncio.run(scenario())
    client.close.assert_awaited_once()
    assert remaining == {}


# ------------------------------------------------------- LOCATION validation


def test_empty_location_is_rejected():
    with pytest.raises(ImproperlyConfigured, match="at least one server URL"):
        ValkeyGlideAdapter([])


def test_empty_cluster_location_is_rejected():
    with pytest.raises(ImproperlyConfigured, match="at least one server URL"):
        ValkeyGlideClusterAdapter([])


# ------------------------------------------------------- nopass ACL usernames


def test_config_kwargs_skips_credentials_without_a_password():
    # glide's ServerCredentials rejects a username with no password, which a
    # nopass ACL user has.
    kwargs = _glide_config_kwargs(["redis://user@host:6379/0"], {}, credentials_cls=ServerCredentials)
    assert "credentials" not in kwargs


def test_config_kwargs_keeps_a_username_given_with_a_password():
    kwargs = _glide_config_kwargs(["redis://user:pw@host:6379/0"], {}, credentials_cls=ServerCredentials)
    assert kwargs["credentials"].username == "user"
    assert kwargs["credentials"].password == "pw"


# ------------------------------------------------------- empty field payloads


def test_hmget_without_fields_returns_empty(mocker):
    # HMGET key with no field is a syntax error on the wire.
    adapter, client = _adapter(mocker)
    assert adapter.hmget("h") == []
    client.hmget.assert_not_called()


@pytest.mark.asyncio
async def test_ahmget_without_fields_returns_empty(mocker):
    adapter, client = _async_adapter(mocker)
    assert await adapter.ahmget("h") == []
    client.hmget.assert_not_awaited()


def test_hset_without_a_payload_raises(mocker):
    adapter, client = _adapter(mocker)
    with pytest.raises(ValueError, match="at least one field/value pair"):
        adapter.hset("h", mapping={})
    client.hset.assert_not_called()


@pytest.mark.asyncio
async def test_ahset_without_a_payload_raises(mocker):
    adapter, client = _async_adapter(mocker)
    with pytest.raises(ValueError, match="at least one field/value pair"):
        await adapter.ahset("h", mapping={})
    client.hset.assert_not_awaited()


# ----------------------------------------------------------- pop count misses


@pytest.mark.parametrize("name", ["lpop", "rpop"])
def test_pop_count_returns_none_for_a_missing_key(mocker, name):
    # The cache layer tells a miss from an empty pop by the None.
    adapter, client = _adapter(mocker)
    getattr(client, f"{name}_count").return_value = None
    assert getattr(adapter, name)("l", count=2) is None


@pytest.mark.parametrize("name", ["lpop", "rpop"])
def test_pop_count_returns_a_list_when_the_key_exists(mocker, name):
    adapter, client = _adapter(mocker)
    getattr(client, f"{name}_count").return_value = [b"a", b"b"]
    assert getattr(adapter, name)("l", count=2) == [b"a", b"b"]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["alpop", "arpop"])
async def test_async_pop_count_returns_none_for_a_missing_key(mocker, name):
    adapter, client = _async_adapter(mocker)
    getattr(client, f"{name[1:]}_count").return_value = None
    assert await getattr(adapter, name)("l", count=2) is None


# --------------------------------------------------------- XINFO STREAM FULL


def test_xinfo_stream_full_decodes_nested_group_keys(mocker):
    # FULL nests the group and consumer dicts inside list values.
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = {
        b"length": 1,
        b"groups": [{b"name": b"g", b"consumers": [{b"name": b"c"}]}],
    }
    info = adapter.xinfo_stream("s", full=True)
    assert info["groups"][0]["name"] == b"g"
    assert info["groups"][0]["consumers"][0]["name"] == b"c"


# ------------------------------------------------------------- TYPE mapping


def test_type_maps_an_unmodelled_type_to_unknown(mocker):
    adapter, client = _adapter(mocker)
    client.type.return_value = b"ReJSON-RL"
    assert adapter.type("k") is KeyType.UNKNOWN


def test_type_maps_none_to_none(mocker):
    adapter, client = _adapter(mocker)
    client.type.return_value = b"none"
    assert adapter.type("k") is None


@pytest.mark.asyncio
async def test_atype_maps_an_unmodelled_type_to_unknown(mocker):
    adapter, client = _async_adapter(mocker)
    client.type.return_value = b"ReJSON-RL"
    assert await adapter.atype("k") is KeyType.UNKNOWN


# -------------------------------------------------------------- SET options


def test_pipeline_set_sends_px(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.set("k", b"v", px=60000)
    assert pipe._batch.commands[-1][1] == ["k", b"v", "PX", "60000"]


def test_pipeline_set_sends_keepttl(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.set("k", b"v", keepttl=True)
    assert pipe._batch.commands[-1][1] == ["k", b"v", "KEEPTTL"]


def test_pipeline_set_rejects_two_expiry_options(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    with pytest.raises(ValueError, match="at most one of"):
        pipe.set("k", b"v", ex=60, px=60000)


def test_pipeline_set_with_get_returns_the_old_value(mocker):
    # Without the tracker skip, the old value collapsed to False.
    client = mocker.Mock()
    client.exec.return_value = [b"old"]
    pipe = ValkeyGlidePipelineAdapter(client, transaction=False)
    pipe.set("k", b"v", get=True)
    assert pipe.execute() == [b"old"]


# ---------------------------------------------- protocol parameter spellings


def test_xadd_takes_entry_id(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = b"1-1"
    adapter.xadd("s", {"f": b"v"}, entry_id="1-1")
    assert client.custom_command.call_args[0][0] == [b"XADD", "s", "1-1", "f", b"v"]


def test_xrange_takes_start_and_end(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = {}
    adapter.xrange("s", start="1-1", end="2-2", count=5)
    assert client.custom_command.call_args[0][0] == [b"XRANGE", "s", "1-1", "2-2", b"COUNT", b"5"]


def test_xrevrange_takes_end_and_start(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = {}
    adapter.xrevrange("s", end="2-2", start="1-1")
    assert client.custom_command.call_args[0][0] == [b"XREVRANGE", "s", "2-2", "1-1"]


def test_slowlog_get_sends_its_count(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = []
    adapter.slowlog_get(5)
    assert client.custom_command.call_args[0][0] == [b"SLOWLOG", b"GET", b"5"]


# ------------------------------------------------------- lock sleep clamping


def test_lock_blocking_sleep_stops_at_the_deadline(mocker):
    client = mocker.Mock()
    client.set.return_value = None
    sleep = mocker.patch("django_cachex.adapters.valkey_glide.time.sleep")
    mocker.patch("django_cachex.adapters.valkey_glide.time.monotonic", side_effect=[0.0, 0.0, 0.05])
    lock = _GlideLock(client, "k", sleep=0.1, blocking=True, timeout=0.05)

    assert lock.acquire() is False
    assert sleep.call_args_list == [mocker.call(0.05)]


# ------------------------------------------------------------ cluster batches


def test_cluster_pipeline_uses_a_cluster_batch(mocker):
    adapter = ValkeyGlideClusterAdapter.__new__(ValkeyGlideClusterAdapter)
    mocker.patch.object(ValkeyGlideClusterAdapter, "_client", return_value=mocker.Mock())
    assert isinstance(adapter.pipeline()._batch, ClusterBatch)


# --------------------------------------------------- per-loop registry growth


def test_closed_loops_do_not_accumulate_in_the_async_registry(mocker):
    import django_cachex.adapters.valkey_glide as vg

    adapter = ValkeyGlideAdapter.__new__(ValkeyGlideAdapter)
    adapter._config_key = ("growth",)
    adapter._stampede_config = None

    async def make_client():
        # Glide's client holds its loop, so the registry key is never collected
        # on its own and a closed loop only leaves through the sweep.
        client = mocker.AsyncMock()
        client.loop = asyncio.get_running_loop()
        return client

    mocker.patch.object(ValkeyGlideAdapter, "_create_async_client", mocker.AsyncMock(side_effect=make_client))

    asyncio.run(adapter.aget("k"))
    after_first = len(vg._GLIDE_ASYNC_CLIENTS)
    for _ in range(300):
        asyncio.run(adapter.aget("k"))

    assert len(vg._GLIDE_ASYNC_CLIENTS) <= after_first


# ------------------------------------------------------ UNLINK instead of DEL


def test_delete_sends_unlink(mocker):
    adapter, client = _adapter(mocker)
    client.unlink.return_value = 1

    assert adapter.delete("k") is True

    assert client.unlink.call_args[0][0] == ["k"]
    client.delete.assert_not_called()


def test_delete_many_sends_unlink(mocker):
    adapter, client = _adapter(mocker)
    client.unlink.return_value = 2

    assert adapter.delete_many(["a", "b"]) == 2

    assert client.unlink.call_args[0][0] == ["a", "b"]


def test_pipeline_delete_queues_unlink(mocker):
    batch = mocker.Mock()
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), batch_factory=lambda *, atomic: batch)
    pipe.delete("a", "b")

    assert batch.unlink.call_args[0][0] == ["a", "b"]
    batch.delete.assert_not_called()


# ---------------------------------------------------- hash field expiration


def test_hexpire_builds_the_wire_form(mocker):
    adapter, client = _adapter(mocker)
    adapter.hexpire("h", datetime.timedelta(minutes=2), "a", "b", gt=True)

    assert client.custom_command.call_args[0][0] == [
        "HEXPIRE",
        "h",
        "120",
        "GT",
        "FIELDS",
        "2",
        "a",
        "b",
    ]


def test_hpexpire_converts_to_milliseconds(mocker):
    adapter, client = _adapter(mocker)
    adapter.hpexpire("h", datetime.timedelta(seconds=1.5), "a")

    assert client.custom_command.call_args[0][0][:3] == ["HPEXPIRE", "h", "1500"]


def test_hexpireat_renders_a_datetime_as_unix_seconds(mocker):
    adapter, client = _adapter(mocker)
    when = datetime.datetime(2030, 1, 1, tzinfo=datetime.UTC)

    adapter.hexpireat("h", when, "a", nx=True)

    assert client.custom_command.call_args[0][0] == [
        "HEXPIREAT",
        "h",
        str(int(when.timestamp())),
        "NX",
        "FIELDS",
        "1",
        "a",
    ]


def test_hpexpireat_renders_a_datetime_as_unix_milliseconds(mocker):
    adapter, client = _adapter(mocker)
    when = datetime.datetime(2030, 1, 1, tzinfo=datetime.UTC)

    adapter.hpexpireat("h", when, "a")

    assert client.custom_command.call_args[0][0][2] == str(int(when.timestamp() * 1000))


def test_hexpire_rejects_two_conditions(mocker):
    adapter, _client = _adapter(mocker)

    with pytest.raises(ValueError, match="at most one of nx, xx, gt and lt"):
        adapter.hexpire("h", 60, "a", nx=True, xx=True)


def test_httl_normalizes_no_expiry_to_none(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = [60, -1, -2]

    assert adapter.httl("h", "a", "b", "missing") == [60, None, -2]
    assert client.custom_command.call_args[0][0] == ["HTTL", "h", "FIELDS", "3", "a", "b", "missing"]


def test_hpersist_leaves_its_codes_alone(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = [1, -1, -2]

    assert adapter.hpersist("h", "a", "b", "missing") == [1, -1, -2]


def test_hsetex_builds_the_wire_form(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = 1

    assert adapter.hsetex("h", {"a": b"1", "b": 2}, ex=60, fnx=True) is True

    assert client.custom_command.call_args[0][0] == [
        "HSETEX",
        "h",
        "FNX",
        "EX",
        "60",
        "FIELDS",
        "2",
        "a",
        b"1",
        "b",
        b"2",
    ]


def test_hsetex_zero_timeout_is_passed_through(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = 1

    adapter.hsetex("h", {"a": b"1"}, ex=0)

    assert client.custom_command.call_args[0][0][:4] == ["HSETEX", "h", "EX", "0"]


def test_hsetex_keepttl_replaces_the_ex_argument(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = 1

    adapter.hsetex("h", {"a": b"1"}, keepttl=True)

    assert client.custom_command.call_args[0][0] == ["HSETEX", "h", "KEEPTTL", "FIELDS", "1", "a", b"1"]


def test_hsetex_rejects_fnx_with_fxx(mocker):
    adapter, _client = _adapter(mocker)

    with pytest.raises(ValueError, match="at most one of fnx and fxx"):
        adapter.hsetex("h", {"a": b"1"}, fnx=True, fxx=True)


def test_hgetex_builds_the_wire_form(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = [b"1", None]

    assert adapter.hgetex("h", "a", "missing", ex=30) == [b"1", None]
    assert client.custom_command.call_args[0][0] == [
        "HGETEX",
        "h",
        "EX",
        "30",
        "FIELDS",
        "2",
        "a",
        "missing",
    ]


def test_hgetex_persist_replaces_the_ex_argument(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = [b"1"]

    adapter.hgetex("h", "a", persist=True)

    assert client.custom_command.call_args[0][0] == ["HGETEX", "h", "PERSIST", "FIELDS", "1", "a"]


@pytest.mark.asyncio
async def test_ahttl_normalizes_no_expiry_to_none(mocker):
    adapter, client = _async_adapter(mocker)
    client.custom_command.return_value = [-1, -2]

    assert await adapter.ahttl("h", "a", "missing") == [None, -2]


@pytest.mark.asyncio
async def test_ahsetex_builds_the_wire_form(mocker):
    adapter, client = _async_adapter(mocker)
    client.custom_command.return_value = 0

    assert await adapter.ahsetex("h", {"a": b"1"}, ex=60, fxx=True) is False

    assert client.custom_command.await_args[0][0] == [
        "HSETEX",
        "h",
        "FXX",
        "EX",
        "60",
        "FIELDS",
        "1",
        "a",
        b"1",
    ]


def test_pipeline_queues_the_hash_ttl_wire_forms(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)

    pipe.hexpire("h", 60, "a", lt=True)
    pipe.httl("h", "a")
    pipe.hsetex("h", {"a": b"1"}, ex=60)
    pipe.hgetex("h", "a", persist=True)

    assert [args for _, args in pipe._batch.commands] == [
        ["HEXPIRE", "h", "60", "LT", "FIELDS", "1", "a"],
        ["HTTL", "h", "FIELDS", "1", "a"],
        ["HSETEX", "h", "EX", "60", "FIELDS", "1", "a", b"1"],
        ["HGETEX", "h", "PERSIST", "FIELDS", "1", "a"],
    ]


# ------------------------------------------------- primary plus replica URLs


def test_config_kwargs_prefers_replicas_for_a_multi_url_location():
    kwargs = _glide_config_kwargs(
        ["redis://primary:6379/1", "redis://replica:6380/1"],
        {},
        credentials_cls=ServerCredentials,
    )
    assert kwargs["read_from"] is ReadFrom.PREFER_REPLICA


def test_config_kwargs_leaves_read_from_unset_for_one_url():
    kwargs = _glide_config_kwargs(["redis://h:6379/1"], {}, credentials_cls=ServerCredentials)
    assert "read_from" not in kwargs


def test_config_kwargs_ignores_a_repeated_url():
    # The same node listed twice is not a replica, and glide rejects an address
    # list in which two entries answer as primary.
    kwargs = _glide_config_kwargs(
        ["redis://h:6379/1", "redis://h:6379/1"],
        {},
        credentials_cls=ServerCredentials,
    )
    assert "read_from" not in kwargs


def test_node_addresses_drops_a_repeated_url():
    addresses = _node_addresses(
        ["redis://h:6379/1", "redis://h:6379/1", "redis://h2:6379/1"],
        NodeAddress,
    )

    assert [(a.host, a.port) for a in addresses] == [("h", 6379), ("h2", 6379)]


def test_config_kwargs_cluster_does_not_prefer_replicas():
    # Cluster routes its own reads; the extra URLs are discovery seeds.
    kwargs = _glide_config_kwargs(
        ["redis://a:7000", "redis://b:7000"],
        {},
        credentials_cls=ServerCredentials,
        standalone=False,
    )
    assert "read_from" not in kwargs


@pytest.mark.parametrize(
    ("servers", "expected"),
    [
        (["redis://h:6379/1", "rediss://h2:6379/1"], "TLS"),
        (["redis://u:pw@h:6379/1", "redis://other:pw@h2:6379/1"], "username"),
        (["redis://u:pw@h:6379/1", "redis://u:pw2@h2:6379/1"], "password"),
        (["redis://h:6379/1", "redis://h2:6379/2"], "database"),
    ],
)
def test_config_kwargs_rejects_urls_that_disagree(servers, expected):
    with pytest.raises(ImproperlyConfigured, match=expected):
        _glide_config_kwargs(servers, {}, credentials_cls=ServerCredentials)


def test_config_kwargs_cluster_tolerates_a_database_mismatch():
    kwargs = _glide_config_kwargs(
        ["redis://a:7000", "redis://b:7000/0"],
        {},
        credentials_cls=ServerCredentials,
        standalone=False,
    )
    assert "database_id" not in kwargs


def test_sync_client_passes_every_url_as_an_address(mocker):
    # Regression: only servers[0] reached glide, so documented replica URLs
    # were dropped without a word.
    import django_cachex.adapters.valkey_glide as vg

    mocker.patch.dict(vg._GLIDE_SYNC_CLIENTS, clear=True)
    config_cls = mocker.patch.object(vg, "GlideClientConfiguration")
    mocker.patch.object(vg, "GlideClient")
    adapter = ValkeyGlideAdapter(["redis://primary:6379/1", "redis://replica:6380/1"])

    adapter._client()

    kwargs = config_cls.call_args.kwargs
    assert [(a.host, a.port) for a in kwargs["addresses"]] == [("primary", 6379), ("replica", 6380)]
    assert kwargs["read_from"] is ReadFrom.PREFER_REPLICA


def test_async_client_passes_every_url_as_an_address(mocker):
    import django_cachex.adapters.valkey_glide as vg

    config_cls = mocker.patch.object(vg, "AsyncGlideClientConfiguration")
    mocker.patch.object(vg, "AsyncGlideClient", mocker.AsyncMock())
    adapter = ValkeyGlideAdapter(["redis://primary:6379/1", "redis://replica:6380/1"])

    asyncio.run(adapter._create_async_client())

    kwargs = config_cls.call_args.kwargs
    assert [(a.host, a.port) for a in kwargs["addresses"]] == [("primary", 6379), ("replica", 6380)]
    assert kwargs["read_from"] is ReadFrom.PREFER_REPLICA


def test_cluster_client_passes_every_seed_url(mocker):
    import django_cachex.adapters.valkey_glide as vg

    mocker.patch.dict(vg._GLIDE_SYNC_CLUSTER_CLIENTS, clear=True)
    config_cls = mocker.patch.object(vg, "GlideClusterClientConfiguration")
    mocker.patch.object(vg, "GlideClusterClient")
    adapter = ValkeyGlideClusterAdapter(["redis://a:7000", "redis://b:7001"])

    adapter._client()

    addresses = config_cls.call_args.kwargs["addresses"]
    assert [(a.host, a.port) for a in addresses] == [("a", 7000), ("b", 7001)]


# ------------------------------------------------ protocol signature conformance


def _param_names(fn):
    # Read the code object rather than ``inspect.signature``: protocols.py
    # annotates with names it imports under TYPE_CHECKING only.
    code = fn.__code__
    n_pos, n_kw = code.co_argcount, code.co_kwonlyargcount
    names = code.co_varnames
    positional = list(names[1:n_pos])
    keyword = set(names[n_pos : n_pos + n_kw])
    var_positional = names[n_pos + n_kw] if code.co_flags & inspect.CO_VARARGS else None
    return positional, var_positional, keyword


def _protocol_methods(protocol, impl):
    return sorted(
        name
        for name, value in vars(protocol).items()
        if callable(value) and not name.startswith("_") and name in vars(impl)
    )


_ADAPTER_METHODS = _protocol_methods(RespAdapterProtocol, ValkeyGlideAdapter)
_PIPELINE_METHODS = sorted(
    set(_protocol_methods(_RespPipelineCommandsProtocol, ValkeyGlidePipelineAdapter))
    | set(_protocol_methods(RespPipelineProtocol, ValkeyGlidePipelineAdapter)),
)


def _assert_signature_matches(declared, implemented):
    # Callers reach the adapter through the protocol, so a parameter the
    # protocol names by keyword has to answer to that name here.
    proto_pos, proto_var, proto_kw = _param_names(declared)
    impl_pos, impl_var, impl_kw = _param_names(implemented)
    assert impl_pos == proto_pos
    assert impl_var == proto_var
    assert proto_kw <= impl_kw


@pytest.mark.parametrize("name", _ADAPTER_METHODS)
def test_adapter_signature_matches_the_protocol(name):
    _assert_signature_matches(getattr(RespAdapterProtocol, name), getattr(ValkeyGlideAdapter, name))


@pytest.mark.parametrize("name", _PIPELINE_METHODS)
def test_pipeline_signature_matches_the_protocol(name):
    declared = getattr(_RespPipelineCommandsProtocol, name, None) or getattr(RespPipelineProtocol, name)
    _assert_signature_matches(declared, getattr(ValkeyGlidePipelineAdapter, name))


# --------------------------------------------- protocol keyword call forms


def test_zcount_takes_the_score_keywords(mocker):
    adapter, client = _adapter(mocker)
    adapter.zcount("k", min_score=1, max_score=5)
    assert client.custom_command.call_args[0][0] == [b"ZCOUNT", "k", b"1", b"5"]


def test_zremrangebyscore_takes_the_score_keywords(mocker):
    adapter, client = _adapter(mocker)
    adapter.zremrangebyscore("k", min_score="-inf", max_score="(3")
    assert client.custom_command.call_args[0][0] == [b"ZREMRANGEBYSCORE", "k", "-inf", "(3"]


def test_zrangebyscore_takes_the_score_keywords(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = []
    adapter.zrangebyscore("k", min_score=1, max_score=5, start=0, num=2)
    assert client.custom_command.call_args[0][0] == [b"ZRANGEBYSCORE", "k", b"1", b"5", b"LIMIT", b"0", b"2"]


def test_zrevrangebyscore_takes_the_score_keywords(mocker):
    adapter, client = _adapter(mocker)
    client.custom_command.return_value = []
    adapter.zrevrangebyscore("k", max_score=5, min_score=1)
    assert client.custom_command.call_args[0][0] == [b"ZREVRANGEBYSCORE", "k", b"5", b"1"]


@pytest.mark.asyncio
async def test_azcount_takes_the_score_keywords(mocker):
    adapter, client = _async_adapter(mocker)
    await adapter.azcount("k", min_score=1, max_score=5)
    assert client.custom_command.await_args[0][0] == [b"ZCOUNT", "k", b"1", b"5"]


@pytest.mark.asyncio
async def test_azrangebyscore_takes_the_score_keywords(mocker):
    adapter, client = _async_adapter(mocker)
    client.custom_command.return_value = []
    await adapter.azrangebyscore("k", min_score=1, max_score=5)
    assert client.custom_command.await_args[0][0] == [b"ZRANGEBYSCORE", "k", b"1", b"5"]


def test_set_store_commands_take_the_dest_keyword(mocker):
    adapter, client = _adapter(mocker)
    adapter.sinterstore(dest="d", keys=["a", "b"])
    adapter.sunionstore(dest="d", keys=["a"])
    adapter.sdiffstore(dest="d", keys=["a"])
    assert client.sinterstore.call_args[0] == ("d", ["a", "b"])
    assert client.sunionstore.call_args[0] == ("d", ["a"])
    assert client.sdiffstore.call_args[0] == ("d", ["a"])


def test_zadd_rejects_a_flag_outside_the_protocol(mocker):
    adapter, _client = _adapter(mocker)
    with pytest.raises(TypeError):
        adapter.zadd("k", {b"m": 1.0}, nx_=True)


def test_pipeline_zcount_takes_the_bound_keywords(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.zcount("k", min=1, max=5)
    assert pipe._batch.commands[-1][1] == [b"ZCOUNT", "k", b"1", b"5"]


def test_pipeline_zremrangebyscore_takes_the_bound_keywords(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.zremrangebyscore("k", min=1, max=5)
    assert pipe._batch.commands[-1][1] == [b"ZREMRANGEBYSCORE", "k", b"1", b"5"]


def test_pipeline_pexpire_takes_the_milliseconds_keyword(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.pexpire("k", milliseconds=1500)
    assert pipe._batch.commands[-1][1] == ["k", "1500"]


def test_pipeline_xclaim_takes_the_message_ids_keyword(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.xclaim("k", "g", "c", 0, message_ids=["1-1"])
    assert pipe._batch.commands[-1][1] == [b"XCLAIM", "k", "g", "c", b"0", "1-1"]


def test_pipeline_xgroup_create_takes_the_id_keyword(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.xgroup_create("k", "g", id="0-0")
    assert pipe._batch.commands[-1][1] == [b"XGROUP", b"CREATE", "k", "g", "0-0"]


def test_pipeline_xgroup_setid_takes_the_id_keyword(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.xgroup_setid("k", "g", id="0-0")
    assert pipe._batch.commands[-1][1] == [b"XGROUP", b"SETID", "k", "g", "0-0"]


def test_pipeline_xpending_range_takes_the_protocol_keywords(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    pipe.xpending_range("k", "g", min="-", max="+", count=5, consumername="c", idle=10)
    assert pipe._batch.commands[-1][1] == [
        b"XPENDING",
        "k",
        "g",
        b"IDLE",
        b"10",
        "-",
        "+",
        b"5",
        "c",
    ]


# ----------------------------------------------------- MAXLEN with MINID


def test_xadd_rejects_maxlen_together_with_minid(mocker):
    adapter, _client = _adapter(mocker)
    with pytest.raises(ValueError, match="Only one of"):
        adapter.xadd("s", {"f": b"v"}, maxlen=5, minid="1-1")


def test_xtrim_rejects_maxlen_together_with_minid(mocker):
    adapter, _client = _adapter(mocker)
    with pytest.raises(ValueError, match="Only one of"):
        adapter.xtrim("s", maxlen=5, minid="1-1")


def test_pipeline_xadd_rejects_maxlen_together_with_minid(mocker):
    pipe = ValkeyGlidePipelineAdapter(mocker.Mock(), transaction=False)
    with pytest.raises(ValueError, match="Only one of"):
        pipe.xadd("s", {"f": b"v"}, maxlen=5, minid="1-1")


# ------------------------------------------------------- aborted transaction


def test_pipeline_execute_reports_an_aborted_transaction(mocker):
    # glide answers None when the server discarded the MULTI.
    client = mocker.Mock()
    client.exec.return_value = None
    pipe = ValkeyGlidePipelineAdapter(client, transaction=True)
    pipe.get("a")
    with pytest.raises(CachexError, match="aborted"):
        pipe.execute()


# ------------------------------------------------------------ registry sweep


def test_sweep_skips_a_client_that_closed_itself(mocker):
    import django_cachex.adapters.valkey_glide as vg

    client = mocker.Mock(_is_closed=True)
    loop = asyncio.new_event_loop()
    loop.close()
    vg._GLIDE_ASYNC_CLIENTS[loop] = {("swept",): client}

    ValkeyGlideAdapter._sweep_async_clients()

    assert loop not in vg._GLIDE_ASYNC_CLIENTS
    client.close.assert_not_called()


def test_sweep_closes_a_live_client_of_a_dead_loop(mocker):
    import django_cachex.adapters.valkey_glide as vg

    closed = []

    async def close():
        closed.append(True)

    client = mocker.Mock(_is_closed=False, close=close)
    loop = asyncio.new_event_loop()
    loop.close()
    vg._GLIDE_ASYNC_CLIENTS[loop] = {("swept",): client}

    ValkeyGlideAdapter._sweep_async_clients()

    assert closed == [True]


def test_get_async_client_sweeps_only_when_it_creates(mocker):
    adapter = ValkeyGlideAdapter.__new__(ValkeyGlideAdapter)
    adapter._config_key = ("sweep-on-create",)
    mocker.patch.object(
        ValkeyGlideAdapter,
        "_create_async_client",
        mocker.AsyncMock(return_value=mocker.AsyncMock()),
    )
    sweep = mocker.patch.object(ValkeyGlideAdapter, "_sweep_async_clients")

    async def scenario():
        await adapter.get_async_client()
        after_create = sweep.call_count
        await adapter.get_async_client()
        return after_create, sweep.call_count

    assert asyncio.run(scenario()) == (1, 1)
