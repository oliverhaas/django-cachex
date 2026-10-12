"""Client tracking: local copies of string values that the server's invalidations keep current."""

import asyncio
import inspect
import itertools
import logging
import os
import random
import socket
import threading
import time
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import pytest
import redis
import valkey
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from django_cachex.adapters import ValkeyPyAdapter
from django_cachex.cache import RedisCache
from django_cachex.orm.store import RespStore
from django_cachex.script import keys_only_pre
from django_cachex.tracking import listener as listener_module
from django_cachex.tracking import tracker as tracker_module
from django_cachex.tracking.adapter import PIPELINE_READ_METHODS, READ_COMMANDS, READ_METHODS
from django_cachex.tracking.listener import Listener
from django_cachex.tracking.resp3 import NEED_MORE, ErrorReply, Map, Parser, ProtocolError, Push, Set, SimpleString
from django_cachex.tracking.store import ENTRY_OVERHEAD, LocalStore
from tests.cache.support import Op, TcpProxy, make_cache, run_forked_while_held, stale_reads, wait_until
from tests.fixtures.cache import (
    ADAPTER_IMAGES,
    BACKENDS,
    _get_client_library_options,
    build_sentinel_cache_config,
    skip_below_server,
)


@pytest.mark.parametrize(
    ("options", "match"),
    [
        ({"tracking": "yes"}, "must be a bool or a dict"),
        ({"tracking": {"size": 100}}, "keys: 'size'"),
        ({"tracking": {"prefix": ""}}, "must be a non-empty str"),
        ({"tracking": {"prefix": b"app:"}}, "must be a non-empty str"),
        ({"tracking": {"max_staleness": 0}}, "must be a finite number of seconds above 0"),
        ({"tracking": {"max_staleness": -1}}, "must be a finite number of seconds above 0"),
        ({"tracking": {"max_staleness": float("inf")}}, "must be a finite number of seconds above 0"),
        ({"tracking": {"max_staleness": float("nan")}}, "must be a finite number of seconds above 0"),
        ({"tracking": {"max_staleness": True}}, "must be a finite number of seconds above 0"),
        ({"tracking": {"max_staleness": "1"}}, "must be a finite number of seconds above 0"),
        ({"tracking": {"max_bytes": 0}}, "must be an int above 0"),
        ({"tracking": {"max_bytes": 1.5}}, "must be an int above 0"),
        ({"tracking": {"max_bytes": True}}, "must be an int above 0"),
        ({"tracking": True, "cache_config": object()}, "client-side caching: cache_config"),
        ({"tracking": True, "encoding": "latin-1"}, "needs the UTF-8 encoding"),
    ],
    ids=[
        "not-a-dict",
        "unknown-key",
        "empty-prefix",
        "bytes-prefix",
        "zero-staleness",
        "negative-staleness",
        "infinite-staleness",
        "nan-staleness",
        "bool-staleness",
        "str-staleness",
        "zero-bytes",
        "float-bytes",
        "bool-bytes",
        "driver-client-cache",
        "latin-1",
    ],
)
def test_an_invalid_tracking_setting_is_rejected(options, match):
    with pytest.raises(ImproperlyConfigured, match=match):
        make_cache(key_prefix="app", **options)


@pytest.mark.parametrize(
    ("location", "match"),
    [
        ("redis://localhost:6379/0?encoding=latin-1", "needs the UTF-8 encoding"),
        ("redis://localhost:6379/0,redis://localhost:6380/0?cache_enabled=1", "client-side caching: cache_enabled"),
    ],
    ids=["latin-1", "driver-client-cache-on-a-second-url"],
)
def test_a_location_query_is_checked_like_the_options(location, match):
    with pytest.raises(ImproperlyConfigured, match=match):
        RedisCache(location, {"OPTIONS": {"tracking": True}})


def test_a_custom_key_function_needs_a_tracking_prefix():
    params = {"KEY_FUNCTION": lambda key, prefix, version: f"{prefix}/{key}", "OPTIONS": {"tracking": True}}

    with pytest.raises(ImproperlyConfigured, match="needs a 'prefix' with a custom KEY_FUNCTION"):
        RedisCache("redis://localhost:6379/0", params)


@pytest.mark.parametrize(
    "backend",
    [
        "django_cachex.cache.RedisClusterCache",
        "django_cachex.cache.ValkeyClusterCache",
        "django_cachex.cache.ValkeyGlideCache",
        "django_cachex.cache.ValkeyGlideClusterCache",
    ],
)
def test_backends_without_client_tracking_reject_it(backend):
    with pytest.raises(ImproperlyConfigured, match="does not support OPTIONS"):
        import_string(backend)("redis://localhost:6379/0", {"OPTIONS": {"tracking": True}})


STREAM = b"".join(
    [
        b">2\r\n$10\r\ninvalidate\r\n*2\r\n$3\r\nt:a\r\n$3\r\nt:b\r\n",
        b">2\r\n$10\r\ninvalidate\r\n*1\r\n$7\r\nt:\r\n+OK\r\n",
        b">2\r\n$10\r\ninvalidate\r\n_\r\n",
        b"+PONG\r\n",
        b"-BUSY Redis is busy running a script.\r\n",
        b"!21\r\nSYNTAX invalid syntax\r\n",
        b":-42\r\n",
        b"(3492890328409238509324850943850943825024385\r\n",
        b",1.5\r\n",
        b"#t\r\n",
        b"#f\r\n",
        b"$-1\r\n",
        b"*-1\r\n",
        b"=15\r\ntxt:Some string\r\n",
        b"%2\r\n+first\r\n:1\r\n+second\r\n*2\r\n:2\r\n:3\r\n",
        b"~2\r\n+a\r\n+b\r\n",
        b"|1\r\n+key-popularity\r\n%1\r\n$1\r\na\r\n,0.1923\r\n*1\r\n:7\r\n",
        b"*0\r\n",
        b"$0\r\n\r\n",
    ],
)

FRAMES = [
    Push([b"invalidate", [b"t:a", b"t:b"]]),
    Push([b"invalidate", [b"t:\r\n+OK"]]),
    Push([b"invalidate", None]),
    SimpleString(b"PONG"),
    ErrorReply(b"BUSY Redis is busy running a script."),
    ErrorReply(b"SYNTAX invalid syntax"),
    -42,
    3492890328409238509324850943850943825024385,
    1.5,
    True,
    False,
    None,
    None,
    b"Some string",
    Map([(SimpleString(b"first"), 1), (SimpleString(b"second"), [2, 3])]),
    Set([SimpleString(b"a"), SimpleString(b"b")]),
    [7],
    [],
    b"",
]


def _split(seed: int) -> list[bytes]:
    cuts = sorted(random.Random(seed).sample(range(1, len(STREAM)), 12))
    return [STREAM[start:end] for start, end in zip([0, *cuts], [*cuts, len(STREAM)], strict=True)]


CHUNKINGS = {
    "whole": [STREAM],
    "bytewise": [STREAM[index : index + 1] for index in range(len(STREAM))],
    **{f"seed{seed}": _split(seed) for seed in range(20)},
}


@pytest.mark.parametrize("chunks", CHUNKINGS.values(), ids=CHUNKINGS.keys())
def test_parser_reads_frames_split_anywhere(chunks):
    parser = Parser()
    frames = []
    for chunk in chunks:
        parser.feed(chunk)
        while (frame := parser.gets()) is not NEED_MORE:
            frames.append(frame)

    assert frames == FRAMES
    assert [type(frame) for frame in frames] == [type(frame) for frame in FRAMES]


@pytest.mark.parametrize(
    "data",
    [
        b"?1\r\n",
        b"$3\r\nabcde\r\n",
        b":12a\r\n",
        b"#x\r\n",
        b"_x\r\n",
        b"*-2\r\n",
        b">-1\r\n",
        b"$-2\r\n",
        b"+" + b"a" * (64 * 1024 + 1),
    ],
    ids=[
        "unknown-type",
        "blob-without-crlf",
        "bad-integer",
        "bad-boolean",
        "bad-null",
        "bad-count",
        "null-push",
        "bad-length",
        "long-line",
    ],
)
def test_parser_rejects_malformed_frames(data):
    parser = Parser()
    parser.feed(data)

    with pytest.raises(ProtocolError):
        parser.gets()


def _store(max_bytes: int = 1024 * 1024) -> LocalStore:
    store = LocalStore(max_bytes)
    store.activate()
    return store


def _absorb(store: LocalStore, key: str, value: bytes | None, pttl: int = -1, started: float = 100.0) -> None:
    store.absorb(key, store.begin_fetch(key), value, pttl, started)


def _sized(key: str, size: int) -> bytes:
    return b"x" * (size - len(key) - ENTRY_OVERHEAD)


def test_a_copy_lives_until_the_server_would_expire_it():
    store = _store()
    _absorb(store, "t:a", b"v", pttl=2000, started=100.0)

    assert store.lookup("t:a", 101.9).value == b"v"
    assert store.lookup("t:a", 102.0) is None


def test_a_copy_without_ttl_never_expires_locally():
    store = _store()
    _absorb(store, "t:a", b"v", pttl=-1)

    assert store.lookup("t:a", 10.0**9).value == b"v"


@pytest.mark.parametrize(
    ("value", "pttl", "kept"),
    [(None, -2, True), (None, -1, False), (None, 500, False), (b"v", -2, False)],
    ids=["missing-key", "nil-without-ttl", "nil-with-ttl", "deleted-between-commands"],
)
def test_only_a_missing_key_is_kept_as_missing(value, pttl, kept):
    store = _store()
    _absorb(store, "t:a", value, pttl=pttl)

    assert store.stats()["entries"] == int(kept)
    copy = store.lookup("t:a", 100.0)
    assert (copy is not None) is kept
    if kept:
        assert copy.value is None


def test_an_inactive_store_keeps_nothing():
    store = LocalStore(1024)
    _absorb(store, "t:a", b"v")

    assert store.lookup("t:a", 100.0) is None


@pytest.mark.parametrize(
    "interleave",
    [
        lambda store: (store.invalidate(["t:a"]), store.begin_fetch("t:a")),
        lambda store: store.discard(["t:a"]),
        lambda store: store.flush(),
        lambda store: store.forget("t:a", store.begin_fetch("t:a")),
        lambda store: (store.deactivate(), store.activate()),
    ],
    ids=["push", "own-write", "flush", "concurrent-fetch-failed", "reconnect"],
)
def test_a_fetch_overtaken_before_it_returns_keeps_nothing(interleave):
    store = _store()
    token = store.begin_fetch("t:a")
    interleave(store)
    store.absorb("t:a", token, b"old", -1, 100.0)

    assert store.lookup("t:a", 100.0) is None


def test_concurrent_fetches_of_a_key_keep_the_first_reply():
    store = _store()
    first = store.begin_fetch("t:a")
    second = store.begin_fetch("t:a")
    store.absorb("t:a", first, b"first", -1, 100.0)
    store.absorb("t:a", second, b"second", -1, 100.0)

    assert store.lookup("t:a", 100.0).value == b"first"


def test_a_push_after_the_fetch_drops_the_copy():
    store = _store()
    _absorb(store, "t:a", b"v")
    store.invalidate(["t:a"])

    assert store.lookup("t:a", 100.0) is None


def test_a_push_before_the_fetch_does_not_block_it():
    store = _store()
    store.invalidate(["t:a"])
    _absorb(store, "t:a", b"v")

    assert store.lookup("t:a", 100.0).value == b"v"


def test_an_expired_copy_leaves_a_fetch_in_flight_alone():
    store = _store()
    _absorb(store, "t:a", b"old", pttl=1000, started=100.0)
    token = store.begin_fetch("t:a")
    assert store.lookup("t:a", 101.5) is None
    store.absorb("t:a", token, b"new", -1, 101.0)

    assert store.lookup("t:a", 101.5).value == b"new"


def test_the_least_recently_used_copies_go_first():
    store = _store(max_bytes=8000)
    for index in range(8):
        _absorb(store, f"t:{index}", _sized(f"t:{index}", 1000))
    store.lookup("t:0", 100.0)
    _absorb(store, "t:8", _sized("t:8", 1000))

    assert store.lookup("t:0", 100.0) is not None
    assert store.lookup("t:1", 100.0) is None
    assert store.lookup("t:8", 100.0) is not None
    assert store.stats()["bytes"] == 8000


def test_a_value_above_an_eighth_of_the_bound_is_not_kept():
    store = _store(max_bytes=8000)
    _absorb(store, "t:a", _sized("t:a", 1000))
    _absorb(store, "t:b", _sized("t:b", 1001))

    assert store.lookup("t:a", 100.0) is not None
    assert store.lookup("t:b", 100.0) is None


def test_stats_count_hits_misses_invalidations_and_flushes():
    store = _store()
    _absorb(store, "t:a", b"v")
    store.lookup("t:a", 100.0)
    store.note_miss()
    store.invalidate(["t:a", "t:b"])
    store.flush()
    store.deactivate()
    store.deactivate()

    stats = store.stats()
    assert (stats["hits"], stats["misses"], stats["invalidations"], stats["flushes"]) == (1, 2, 2, 2)


def test_stats_count_the_bytes_of_the_copies_kept():
    store = _store()
    _absorb(store, "t:a", _sized("t:a", 300), pttl=1000, started=100.0)
    for key, size in [("t:b", 200), ("t:c", 400), ("t:d", 500)]:
        _absorb(store, key, _sized(key, size))
    totals = [store.stats()["bytes"]]

    _absorb(store, "t:b", _sized("t:b", 250))
    totals.append(store.stats()["bytes"])
    store.invalidate(["t:c"])
    totals.append(store.stats()["bytes"])
    store.discard(["t:d"])
    totals.append(store.stats()["bytes"])
    store.lookup("t:a", 101.0)
    totals.append(store.stats()["bytes"])
    store.flush()
    totals.append(store.stats()["bytes"])

    assert totals == [1400, 1450, 1050, 550, 250, 0]


DB = 15


def test_tracking_defaults_to_the_key_prefix_one_second_and_32_mib():
    info = make_cache(key_prefix="app", tracking=True).adapter.tracking_info()

    assert (info["prefix"], info["max_staleness"], info["max_bytes"]) == ("app:", 1.0, 32 * 1024 * 1024)


@pytest.fixture(params=["redis-py", "valkey-py"])
def resp_adapter(request):
    return request.param


@pytest.fixture(autouse=True)
def registry(monkeypatch):
    fresh = tracker_module._Registry()
    monkeypatch.setattr(tracker_module, "_registry", fresh)
    yield fresh
    for tracker in fresh.trackers.values():
        tracker.listener.stop()


@pytest.fixture
def server(redis_container):
    client = redis.Redis(host=redis_container.host, port=redis_container.port, db=DB)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def proxy(redis_container):
    proxy = TcpProxy(redis_container.host, redis_container.port)
    yield proxy
    proxy.close()


@pytest.fixture
def build(resp_adapter, redis_container):
    prefix = f"ct{uuid4().hex[:8]}"
    built = []

    def build(
        *,
        container=redis_container,
        port=None,
        scheme="redis",
        userinfo="",
        tracking=False,
        warm_key="k",
        **options,
    ):
        host, port = ("127.0.0.1", port) if port else (container.host, container.port)
        options = {**_get_client_library_options(ADAPTER_IMAGES[resp_adapter][1]), **options, "tracking": tracking}
        backend = import_string(BACKENDS[("default", resp_adapter)])
        cache = backend(f"{scheme}://{userinfo}{host}:{port}?db={DB}", {"KEY_PREFIX": prefix, "OPTIONS": options})
        built.append(cache)
        if tracking and warm_key is not None:
            cache.get(warm_key)
            wait_until(lambda: _fresh(cache))
        return cache

    yield build
    for cache in built:
        cache.close()


@pytest.fixture
def plain(build):
    return build()


@pytest.fixture
def tracked(build, proxy):
    cache = build(port=proxy.port, tracking={"max_staleness": 3600})
    proxy.pause()
    return cache


def _fresh(cache):
    return cache.adapter.tracking_info()["fresh"]


def _commands(server):
    sent = {name.removeprefix("cmdstat_") for name, stats in server.info("commandstats").items() if stats["calls"]}
    return sent - {"config", "config|resetstat"}


def _data_commands(server):
    return _commands(server) - {"info", "ping", "publish"}


def test_keys_outside_the_tracking_prefix_are_read_from_the_server(build, proxy, plain):
    plain.set_many({"a": "old", "b": "old"})
    cache = build(port=proxy.port, tracking={"prefix": plain.make_key("a"), "max_staleness": 3600}, warm_key="a")
    proxy.pause()
    assert cache.get_many(["a", "b"]) == {"a": "old", "b": "old"}
    assert not cache.has_key("c")
    misses = cache.adapter.tracking_info()["misses"]

    plain.set_many({"a": "new", "b": "new", "c": "new"})

    assert cache.get("b") == "new"
    assert cache.get_many(["a", "b"]) == {"a": "old", "b": "new"}
    assert cache.has_key("c")
    assert cache.adapter.tracking_info()["misses"] == misses


def test_info_counts_the_reads_served_from_copies(tracked):
    tracked.set("k", 1)
    tracked.get("k")
    hits = tracked.info()["tracking"]["hits"]

    tracked.get("k")

    assert tracked.info()["tracking"]["hits"] == hits + 1


def test_stampede_prevention_recomputes_from_a_kept_copy(build, proxy, plain, server):
    plain.set("k", 1)
    plain.expire("k", 50)
    cache = build(port=proxy.port, tracking={"max_staleness": 3600}, stampede_prevention=True, warm_key="other")
    proxy.pause()
    assert cache.get("k") is None
    server.config_resetstat()

    assert cache.get("k") is None
    assert _data_commands(server) == set()


@pytest.mark.parametrize(("ttl", "present"), [(50, False), (3600, True)])
def test_has_key_applies_the_stampede_buffer_to_a_kept_copy(ttl, present, build, proxy, plain, server):
    plain.set("k", 1)
    plain.expire("k", ttl)
    cache = build(port=proxy.port, tracking={"max_staleness": 3600}, stampede_prevention=True, warm_key="other")
    proxy.pause()
    cache.get("k")
    server.config_resetstat()

    assert cache.has_key("k") is present
    assert _data_commands(server) == set()


def test_has_key_without_a_copy_treats_a_key_inside_the_stampede_buffer_as_missing(build, plain):
    plain.set("k", 1)
    plain.expire("k", 50)
    cache = build(tracking=True, stampede_prevention=True, warm_key="other")

    assert not cache.has_key("k")


def _read_raw(cache, key):
    return cache.adapter.read_many([cache.make_and_validate_key(key)])[0]


GATED_READS = [
    pytest.param(lambda cache, key: cache.get(key), 1, None, id="get"),
    pytest.param(lambda cache, key: cache.get_many([key]).get(key), 1, None, id="get_many"),
    pytest.param(lambda cache, key: cache.has_key(key), True, False, id="has_key"),
    pytest.param(_read_raw, b"1", None, id="read_many"),
]


@pytest.mark.parametrize(("read", "present", "gone"), GATED_READS)
def test_a_write_by_another_client_reaches_reads_within_max_staleness(read, present, gone, build, plain):
    plain.set("k", 1)
    cache = build(tracking=True)
    cache.get("k")
    assert read(cache, "k") == present

    plain.delete("k")

    wait_until(lambda: read(cache, "k") == gone, timeout=1.0)


@pytest.mark.parametrize(("read", "present", "gone"), GATED_READS)
@pytest.mark.parametrize("key", ["café", "bad\udcff"], ids=["utf-8", "not-utf-8"])
def test_a_write_by_another_client_ends_the_copy_of_a_non_ascii_key(key, read, present, gone, build, plain):
    plain.set(key, 1)
    cache = build(tracking=True)
    cache.get(key)
    assert read(cache, key) == present

    plain.delete(key)

    wait_until(lambda: read(cache, key) == gone, timeout=1.0)


def test_copies_stay_current_with_the_native_parser_on_the_data_path(resp_adapter, build, plain):
    parser = _get_client_library_options(ADAPTER_IMAGES[resp_adapter][1], native_parser=True)["parser_class"]
    plain.set("k", "old")
    cache = build(tracking=True, parser_class=parser)
    assert cache.get("k") == "old"

    plain.set("k", "new")

    wait_until(lambda: cache.get("k") == "new", timeout=1.0)


@pytest.mark.parametrize(("read", "present", "gone"), GATED_READS)
def test_reads_go_to_the_server_once_the_connection_is_unconfirmed_for_max_staleness(
    read,
    present,
    gone,
    build,
    proxy,
    plain,
):
    plain.set("k", 1)
    cache = build(port=proxy.port, tracking=True)
    cache.get("k")
    assert read(cache, "k") == present
    proxy.pause()
    plain.delete("k")
    assert read(cache, "k") == present

    time.sleep(1.05)

    assert read(cache, "k") == gone


@pytest.mark.parametrize(("read", "present", "gone"), GATED_READS)
def test_a_read_that_fails_raises_the_driver_error(read, present, gone, build, proxy, plain):
    plain.set("k", 1)
    cache = build(port=proxy.port, tracking=True)

    proxy.cut()

    with pytest.raises((redis.ConnectionError, valkey.ConnectionError)):
        read(cache, "k")


def _warnings(caplog):
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "django_cachex.tracking" and record.levelno >= logging.WARNING
    ]


@pytest.mark.parametrize(("read", "present", "gone"), GATED_READS)
def test_reads_go_to_the_server_when_the_tracking_thread_cannot_start(
    read,
    present,
    gone,
    build,
    plain,
    monkeypatch,
    caplog,
):
    start = threading.Thread.start
    refused = []

    def refuse(self):
        if self.name == "django-cachex-tracking":
            refused.append(self)
            raise RuntimeError("can't start new thread")
        start(self)

    monkeypatch.setattr(threading.Thread, "start", refuse)
    caplog.set_level(logging.WARNING, logger="django_cachex.tracking")
    plain.set("k", 1)
    cache = build(tracking=True, warm_key=None)
    assert read(cache, "k") == present
    descriptors = len(list(Path("/dev/fd").iterdir()))

    plain.delete("k")

    assert [read(cache, "k") for _ in range(10)] == [gone] * 10
    assert len(list(Path("/dev/fd").iterdir())) == descriptors
    assert len(_warnings(caplog)) == 1
    assert len(refused) == 1


@pytest.mark.parametrize("command", ["FLUSHDB", "FLUSHALL"])
def test_a_flush_by_another_client_ends_every_copy(command, build, plain, server):
    plain.set_many({"a": 1, "b": 2})
    cache = build(tracking=True)
    assert cache.get_many(["a", "b"]) == {"a": 1, "b": 2}

    server.execute_command(command)

    wait_until(lambda: cache.get_many(["a", "b"]) == {}, timeout=1.0)


def test_a_busy_server_closes_the_gate_and_keeps_the_connection(build, server):
    cache = build(tracking={"max_staleness": 0.2})
    script = (
        "local start = redis.call('TIME') "
        "repeat local now = redis.call('TIME') "
        "until (now[1] - start[1]) * 1000000 + now[2] - start[2] > 1500000 "
        "return 1"
    )
    busy = threading.Thread(target=server.eval, args=(script, 0))
    try:
        server.config_set("lua-time-limit", 50)
        server.config_resetstat()
        busy.start()
        try:
            wait_until(lambda: not _fresh(cache), timeout=1.0)
        finally:
            busy.join()
    finally:
        server.config_set("lua-time-limit", 5000)

    assert 0 < server.info("commandstats")["cmdstat_ping"]["rejected_calls"] < 30
    wait_until(lambda: _fresh(cache))
    assert cache.adapter.tracking_info()["reconnects"] == 0


def test_reads_process_invalidations_while_the_tracker_thread_is_held_up(build, plain, monkeypatch):
    entered, release = threading.Event(), threading.Event()

    def held(self):
        entered.set()
        release.wait(5)

    monkeypatch.setattr(ValkeyPyAdapter, "tracking_primary", held)
    plain.set("k", "old")
    cache = build(tracking={"max_staleness": 1.0})
    assert cache.get("k") == "old"
    try:
        assert entered.wait(5)
        plain.set("k", "new")

        wait_until(lambda: cache.get("k") == "new", timeout=0.3)
    finally:
        release.set()


def _take_the_pong(listener, sent_before):
    listener.drain()
    return listener.status()[1] > sent_before


def test_a_read_that_takes_the_pong_keeps_the_ping_cadence(plain, monkeypatch):
    max_staleness = 0.5
    listener = Listener(plain.adapter, plain.make_key(""), max_staleness, LocalStore(1024 * 1024))
    real_select = listener._selector.select
    waits = []

    def select(timeout):
        wait_until(lambda: _take_the_pong(listener, connected_at))
        ready = real_select(0)
        waits.append(0.0 if ready else timeout)
        return ready

    monkeypatch.setattr(listener._selector, "select", select)
    try:
        connected_at = listener._connect()
        time.sleep(max_staleness)
        listener._step()
    finally:
        listener.stop()

    assert len(waits) == 1
    assert waits[0] <= max_staleness / 2


def _tracking_ids(server, username="default"):
    return {
        client["id"]
        for client in server.client_list()
        if client["name"] == "django-cachex-tracking" and client["user"] == username
    }


def test_instances_with_the_same_settings_share_one_connection_and_its_copies(build, plain, server):
    before = _tracking_ids(server)
    plain.set("k", "v")
    first = build(tracking={"max_staleness": 3600})
    assert first.get("k") == "v"
    second = build(tracking={"max_staleness": 3600}, warm_key="other")
    server.config_resetstat()

    assert second.get("k") == "v"
    assert _data_commands(server) == set()
    assert len(_tracking_ids(server) - before) == 1


def test_a_refused_reconnect_warns_once_and_recovers(build, plain, server, caplog):
    skip_below_server(plain, redis=(7, 0), feature="ACL rules for subcommands")
    username, password = f"ct-{uuid4().hex[:8]}", uuid4().hex
    server.execute_command("ACL", "SETUSER", username, "on", f">{password}", "~*", "+@all")
    try:
        plain.set("k", "old")
        cache = build(userinfo=f"{username}:{password}@", tracking={"max_staleness": 0.2})
        assert cache.get("k") == "old"
        caplog.set_level(logging.INFO, logger="django_cachex.tracking")
        server.execute_command("ACL", "SETUSER", username, "-client|tracking")
        for client_id in _tracking_ids(server, username):
            server.execute_command("CLIENT", "KILL", "ID", client_id)
        wait_until(lambda: not cache.adapter.tracking_info()["connected"])
        time.sleep(1.0)
        assert not cache.adapter.tracking_info()["connected"]
        plain.set("k", "new")
        assert cache.get("k") == "new"

        server.execute_command("ACL", "SETUSER", username, "+client|tracking")

        wait_until(
            lambda: any(record.getMessage() == "Client tracking reconnected." for record in caplog.records),
            timeout=10.0,
        )
        records = [record for record in caplog.records if record.name.startswith("django_cachex.tracking")]
        assert len([record for record in records if record.levelno >= logging.WARNING]) == 1
        assert _fresh(cache)
    finally:
        server.execute_command("ACL", "DELUSER", username)


def _refusals(server, username):
    return sum(entry["count"] for entry in server.acl_log() if entry["username"] == username)


def test_a_refused_first_connection_warns_once_and_reads_go_to_the_server(build, plain, server, caplog):
    skip_below_server(plain, redis=(7, 0), feature="ACL rules for subcommands")
    username, password = f"ct-{uuid4().hex[:8]}", uuid4().hex
    server.execute_command("ACL", "SETUSER", username, "on", f">{password}", "~*", "+@all", "-client|tracking")
    try:
        caplog.set_level(logging.WARNING, logger="django_cachex.tracking")
        plain.set("k", "old")
        cache = build(userinfo=f"{username}:{password}@", tracking=True, warm_key=None)
        assert cache.get("k") == "old"
        wait_until(lambda: _refusals(server, username) >= 3)

        plain.set("k", "new")

        assert cache.get("k") == "new"
        assert len(_warnings(caplog)) == 1
    finally:
        server.execute_command("ACL", "DELUSER", username)


@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_a_forked_child_tracks_on_its_own_connection(build, plain, server, registry):
    plain.set("k", "old")
    cache = build(tracking=True)
    assert cache.get("k") == "old"
    before = _tracking_ids(server)

    def child():
        cache.get("k")
        wait_until(lambda: _fresh(cache) and _tracking_ids(server) - before)
        assert cache.get("k") == "old"
        plain.set("k", "new")
        wait_until(lambda: cache.get("k") == "new", timeout=1.0)

    run_forked_while_held(registry.lock, child)

    wait_until(lambda: cache.get("k") == "new", timeout=1.0)
    assert _tracking_ids(server) >= before
    assert cache.adapter.tracking_info()["reconnects"] == 0


def test_copies_from_before_an_outage_are_not_served_after_it(build, proxy, plain):
    plain.set("k", "old")
    cache = build(port=proxy.port, tracking=True)
    assert cache.get("k") == "old"
    proxy.cut()
    wait_until(lambda: not cache.adapter.tracking_info()["connected"])
    plain.set("k", "new")

    proxy.restore()

    wait_until(lambda: _fresh(cache), timeout=10.0)
    assert cache.get("k") == "new"


def test_an_unanswered_ping_ends_the_connection_and_the_thread_reconnects(build, proxy, monkeypatch):
    monkeypatch.setattr(listener_module, "_PING_TIMEOUT", 0.3)
    cache = build(port=proxy.port, tracking={"max_staleness": 0.2})
    proxy.pause()
    wait_until(lambda: not cache.adapter.tracking_info()["connected"])

    proxy.resume()

    wait_until(lambda: _fresh(cache))
    assert cache.adapter.tracking_info()["reconnects"] == 1


def test_copies_end_once_sentinel_names_another_primary(build, plain, monkeypatch):
    plain.set("k", "old")
    cache = build(tracking={"max_staleness": 0.2})
    assert cache.get("k") == "old"

    monkeypatch.setattr(ValkeyPyAdapter, "tracking_primary", lambda self: ("192.0.2.1", 6379))

    wait_until(lambda: cache.adapter.tracking_info()["entries"] == 0, timeout=1.0)


def test_a_failing_primary_check_warns_once_and_keeps_the_connection(build, monkeypatch, caplog):
    checks = []

    def unreachable(self):
        checks.append(self)
        raise ConnectionError("no Sentinel answered")

    monkeypatch.setattr(ValkeyPyAdapter, "tracking_primary", unreachable)
    caplog.set_level(logging.WARNING, logger="django_cachex.tracking")
    cache = build(tracking={"max_staleness": 0.2})

    wait_until(lambda: len(checks) >= 3)

    assert len(_warnings(caplog)) == 1
    info = cache.adapter.tracking_info()
    assert (info["connected"], info["reconnects"]) == (True, 0)


def test_a_sentinel_alias_serves_copies_across_primary_checks(resp_adapter, sentinel_container, plain, server):
    config = build_sentinel_cache_config(
        sentinel_container.host,
        sentinel_container.port,
        resp_adapter=resp_adapter,
        db=DB,
    )["default"]
    options = {**config["OPTIONS"], "tracking": {"max_staleness": 0.2}}
    cache = import_string(config["BACKEND"])(config["LOCATION"], {"KEY_PREFIX": plain.key_prefix, "OPTIONS": options})
    try:
        plain.set("k", "old")
        cache.get("other")
        wait_until(lambda: _fresh(cache))
        assert cache.get("k") == "old"
        time.sleep(0.5)
        server.config_resetstat()

        assert cache.get("k") == "old"
        assert _data_commands(server) == set()
        plain.set("k", "new")
        wait_until(lambda: cache.get("k") == "new", timeout=1.0)
        assert cache.adapter.tracking_info()["reconnects"] == 0
    finally:
        cache.close()


def test_a_burst_of_invalidations_over_tls_reaches_the_copies(build, tls_container, tls_certificates):
    tls = {"container": tls_container, "scheme": "rediss", "ssl_ca_certs": str(tls_certificates.ca)}
    keys = [f"k{index}" for index in range(5000)]
    writer = build(**tls)
    writer.set_many(dict.fromkeys(keys, "old"))
    cache = build(**tls, tracking=True, warm_key=keys[0])
    assert cache.get_many(keys) == dict.fromkeys(keys, "old")
    assert cache.adapter.tracking_info()["entries"] == len(keys)

    writer.set_many(dict.fromkeys(keys, "new"))

    wait_until(lambda: cache.adapter.tracking_info()["entries"] == 0)
    assert cache.get_many(keys) == dict.fromkeys(keys, "new")
    info = cache.adapter.tracking_info()
    assert info["invalidations"] >= len(keys)
    assert info["reconnects"] == 0


class _Connection:
    def __init__(self, address):
        self._sock = socket.create_connection(address, timeout=5.0)

    def disconnect(self):
        self._sock.close()


class _Adapter:
    def __init__(self, address):
        self.address = address

    def tracking_connection(self):
        return _Connection(self.address)

    def tracking_primary(self):
        return None


def _answer_ok_and_close(server, accepted):
    while True:
        try:
            conn, address = server.accept()
        except OSError:
            return
        accepted.append(address)
        parser = Parser()
        with conn:
            while parser.gets() is NEED_MORE and (data := conn.recv(65536)):
                parser.feed(data)
            conn.sendall(b"+OK\r\n")


def test_a_server_that_drops_each_new_tracking_connection_is_retried_with_backoff():
    server = socket.create_server(("127.0.0.1", 0))
    accepted = []
    serving = threading.Thread(target=_answer_ok_and_close, args=(server, accepted), daemon=True)
    serving.start()
    listener = Listener(_Adapter(server.getsockname()), "p:", 1.0, LocalStore(1024 * 1024))

    listener.start()
    time.sleep(1.5)
    listener.stop()

    with suppress(OSError):
        server.shutdown(socket.SHUT_RDWR)
    server.close()
    serving.join(5.0)
    assert 2 <= len(accepted) < 20


MODES = ["sync", "async"]
SET_SCRIPT = "return redis.call('SET', KEYS[1], ARGV[1])"


def _write(cache, mode, name, *args, **kwargs):
    pipelined = name.startswith("pipeline.")
    method = name.removeprefix("pipeline.")
    if mode == "sync":
        if not pipelined:
            return getattr(cache, method)(*args, **kwargs)
        pipe = cache.pipeline()
        getattr(pipe, method)(*args, **kwargs)
        return pipe.execute()

    async def write():
        try:
            if not pipelined:
                return await getattr(cache, f"a{method}")(*args, **kwargs)
            pipe = await cache.apipeline()
            getattr(pipe, method)(*args, **kwargs)
            return await pipe.execute()
        finally:
            await cache.aclose()

    return asyncio.run(write())


CREATES = [
    pytest.param(None, "set", ("k", 1), {}, id="set"),
    pytest.param(None, "add", ("k", 1), {}, id="add"),
    pytest.param(None, "set_many", ({"k": 1},), {}, id="set_many"),
    pytest.param(None, "get_or_set", ("k", 1), {}, id="get_or_set"),
    pytest.param(None, "incr", ("k",), {}, id="incr"),
    pytest.param(None, "decr", ("k",), {}, id="decr"),
    pytest.param(lambda plain: plain.set("k", 1, version=0), "incr_version", ("k",), {"version": 0}, id="incr_version"),
    pytest.param(lambda plain: plain.set("k", 1, version=2), "decr_version", ("k",), {"version": 2}, id="decr_version"),
    pytest.param(lambda plain: plain.set("src", 1), "rename", ("src", "k"), {}, id="rename"),
    pytest.param(lambda plain: plain.set("src", 1), "renamenx", ("src", "k"), {}, id="renamenx"),
    pytest.param(None, "hset", ("k", "f", 1), {}, id="hset"),
    pytest.param(None, "hsetnx", ("k", "f", 1), {}, id="hsetnx"),
    pytest.param(None, "hincrby", ("k", "f"), {}, id="hincrby"),
    pytest.param(None, "hincrbyfloat", ("k", "f"), {}, id="hincrbyfloat"),
    pytest.param(None, "hsetex", ("k", "f", 1), {}, id="hsetex"),
    pytest.param(None, "lpush", ("k", 1), {}, id="lpush"),
    pytest.param(None, "rpush", ("k", 1), {}, id="rpush"),
    pytest.param(lambda plain: plain.rpush("src", 1), "lmove", ("src", "k", "LEFT", "RIGHT"), {}, id="lmove"),
    pytest.param(lambda plain: plain.rpush("src", 1), "blmove", ("src", "k", 1), {}, id="blmove"),
    pytest.param(None, "sadd", ("k", 1), {}, id="sadd"),
    pytest.param(lambda plain: plain.sadd("src", 1), "smove", ("src", "k", 1), {}, id="smove"),
    pytest.param(lambda plain: plain.sadd("src", 1), "sdiffstore", ("k", ["src"]), {}, id="sdiffstore"),
    pytest.param(lambda plain: plain.sadd("src", 1), "sinterstore", ("k", ["src"]), {}, id="sinterstore"),
    pytest.param(lambda plain: plain.sadd("src", 1), "sunionstore", ("k", ["src"]), {}, id="sunionstore"),
    pytest.param(None, "zadd", ("k", {"m": 1}), {}, id="zadd"),
    pytest.param(None, "zincrby", ("k", 1, "m"), {}, id="zincrby"),
    pytest.param(None, "xadd", ("k", {"f": "v"}), {}, id="xadd"),
    pytest.param(None, "xgroup_create", ("k", "g"), {"mkstream": True}, id="xgroup_create"),
    pytest.param(
        None,
        "eval_script",
        (SET_SCRIPT,),
        {"keys": ["k"], "args": [1], "pre_hook": keys_only_pre},
        id="eval_script",
    ),
    pytest.param(None, "pipeline.set", ("k", 1), {}, id="pipeline"),
]


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(("arrange", "name", "args", "kwargs"), CREATES)
def test_a_write_that_creates_the_key_ends_its_missing_copy(mode, arrange, name, args, kwargs, tracked, plain):
    if name == "hsetex":
        skip_below_server(plain, redis=(8, 0), valkey=(9, 0), feature="HSETEX")
    if arrange:
        arrange(plain)
    assert tracked.get("k") is None

    _write(tracked, mode, name, *args, **kwargs)

    assert tracked.has_key("k")


CHANGES = [
    pytest.param("set", ("k", 2), {}, id="set"),
    pytest.param("set", ("k", 2), {"timeout": 0}, id="set-timeout-0"),
    pytest.param("set_many", ({"k": 2},), {}, id="set_many"),
    pytest.param("delete", ("k",), {}, id="delete"),
    pytest.param("delete_many", (["k"],), {}, id="delete_many"),
    pytest.param("touch", ("k", 0), {}, id="touch"),
    pytest.param("expire", ("k", 0), {}, id="expire"),
    pytest.param("pexpire", ("k", 0), {}, id="pexpire"),
    pytest.param("expireat", ("k", 0), {}, id="expireat"),
    pytest.param("pexpireat", ("k", 0), {}, id="pexpireat"),
    pytest.param("incr", ("k",), {}, id="incr"),
    pytest.param("decr", ("k",), {}, id="decr"),
    pytest.param("incr_version", ("k",), {}, id="incr_version"),
    pytest.param("decr_version", ("k",), {}, id="decr_version"),
    pytest.param("rename", ("k", "dst"), {}, id="rename"),
    pytest.param("renamenx", ("k", "dst"), {}, id="renamenx"),
    pytest.param("clear", (), {}, id="clear"),
    pytest.param("flush_db", (), {}, id="flush_db"),
    pytest.param("delete_pattern", ("k*",), {}, id="delete_pattern"),
    pytest.param("clear_all_versions", (), {}, id="clear_all_versions"),
    pytest.param(
        "eval_script",
        (SET_SCRIPT,),
        {"keys": ["k"], "args": [2], "pre_hook": keys_only_pre},
        id="eval_script",
    ),
    pytest.param("pipeline.set", ("k", 2), {}, id="pipeline"),
]


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(("name", "args", "kwargs"), CHANGES)
def test_a_write_that_changes_the_key_ends_its_copy(mode, name, args, kwargs, tracked, plain):
    tracked.set("k", 1)
    assert tracked.get("k") == 1

    _write(tracked, mode, name, *args, **kwargs)

    assert plain.get("k") != 1
    assert tracked.get("k") == plain.get("k")


@pytest.mark.parametrize("mode", MODES)
def test_a_write_ends_the_copies_once_the_server_applied_it(mode, tracked, server):
    tracked.set("k", "old")
    assert tracked.get("k") == "old"
    server.client_pause(10_000, all=False)
    writer = threading.Thread(target=_write, args=(tracked, mode, "set", "k", "new"))
    try:
        writer.start()
        wait_until(lambda: server.info("clients")["blocked_clients"] == 1)
        assert tracked.get("k") == "old"
    finally:
        server.client_unpause()
        writer.join()

    assert tracked.get("k") == "new"


def test_a_write_given_an_iterator_ends_every_copy(tracked, plain):
    tracked.set_many({"a": 1, "b": 2})
    assert tracked.get_many(["a", "b"]) == {"a": 1, "b": 2}
    plain.set("b", 3)
    assert tracked.get("b") == 2

    tracked.adapter.delete_many(key for key in [tracked.make_key("a")])

    assert tracked.get_many(["a", "b"]) == {"b": 3}


def test_a_flush_queued_on_the_adapter_pipeline_ends_every_copy(tracked):
    tracked.set_many({"a": 1, "b": 2})
    assert tracked.get_many(["a", "b"]) == {"a": 1, "b": 2}
    pipe = tracked.adapter.pipeline(transaction=False)
    pipe.execute_command("FLUSHDB")

    pipe.execute()

    assert tracked.get_many(["a", "b"]) == {}


@pytest.mark.parametrize("mode", MODES)
def test_a_flush_that_raises_after_the_server_applied_it_still_ends_every_copy(mode, tracked, plain, monkeypatch):
    tracked.set_many({"a": 1, "b": 2})
    assert tracked.get_many(["a", "b"]) == {"a": 1, "b": 2}
    delete_pattern, adelete_pattern = ValkeyPyAdapter.delete_pattern, ValkeyPyAdapter.adelete_pattern

    def lose_the_reply(self, *args, **kwargs):
        delete_pattern(self, *args, **kwargs)
        raise ConnectionError("the reply was lost")

    async def alose_the_reply(self, *args, **kwargs):
        await adelete_pattern(self, *args, **kwargs)
        raise ConnectionError("the reply was lost")

    monkeypatch.setattr(ValkeyPyAdapter, "delete_pattern", lose_the_reply)
    monkeypatch.setattr(ValkeyPyAdapter, "adelete_pattern", alose_the_reply)

    with pytest.raises(ConnectionError, match="the reply was lost"):
        _write(tracked, mode, "clear")

    assert plain.get_many(["a", "b"]) == {}
    assert tracked.get_many(["a", "b"]) == {}


def test_a_write_ends_the_copies_of_an_alias_with_other_options(build, proxy, server):
    before = _tracking_ids(server)
    reader = build(port=proxy.port, tracking={"max_staleness": 3600}, warm_key="other")
    writer = build(port=proxy.port, tracking={"max_staleness": 3600}, socket_timeout=5)
    proxy.pause()
    assert len(_tracking_ids(server) - before) == 2
    writer.set("k", 1)
    assert writer.get("k") == 1
    assert reader.get("k") == 1

    writer.set("k", 2)
    assert writer.get("k") == 2
    assert reader.get("k") == 2

    writer.clear()
    assert writer.get("k") is None
    assert reader.get("k") is None


SET_THEN_PUSH = "redis.call('SET', KEYS[1], ARGV[1]) return redis.call('LPUSH', KEYS[1], ARGV[1])"


def _set_then_push(cache, mode, via):
    if via == "script":
        return _write(cache, mode, "eval_script", SET_THEN_PUSH, keys=["k"], args=[2], pre_hook=keys_only_pre)
    if mode == "sync":
        return cache.pipeline().set("k", 2).lpush("k", 2).execute()

    async def write():
        try:
            pipe = await cache.apipeline()
            return await pipe.set("k", 2).lpush("k", 2).execute()
        finally:
            await cache.aclose()

    return asyncio.run(write())


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("via", ["script", "pipeline"])
def test_a_write_that_fails_after_changing_the_key_ends_its_copy(via, mode, tracked):
    tracked.set("k", 1)
    assert tracked.get("k") == 1

    with pytest.raises(TypeError, match="WRONGTYPE"):
        _set_then_push(tracked, mode, via)

    assert tracked.get("k") == 2


READS = [
    pytest.param(lambda cache: cache.get("k"), 1, id="get"),
    pytest.param(lambda cache: cache.get("gone"), None, id="get-missing"),
    pytest.param(lambda cache: cache.get_many(["k", "gone"]), {"k": 1}, id="get_many"),
    pytest.param(lambda cache: cache.has_key("k"), True, id="has_key"),
    pytest.param(lambda cache: cache.has_key("gone"), False, id="has_key-missing"),
]


@pytest.mark.parametrize(("read", "expected"), READS)
def test_a_read_of_a_kept_copy_sends_no_command(read, expected, tracked, server):
    tracked.set("k", 1)
    tracked.get_many(["k", "gone"])
    server.config_resetstat()

    assert read(tracked) == expected
    assert _data_commands(server) == set()


def _writing_commands(server):
    return {
        name if isinstance(name, str) else name.decode()
        for name in server.acl_cat("write") + server.acl_cat("scripting")
    }


def test_a_read_after_close_is_served_from_the_copy(tracked, server):
    tracked.set("k", 1)
    assert tracked.get("k") == 1
    tracked.close()
    server.config_resetstat()

    assert tracked.get("k") == 1
    assert _data_commands(server) == set()


NOT_COMMANDS = frozenset(
    {
        "get_client",
        "get_async_client",
        "resolve_stampede",
        "get_timeout_with_buffer",
        "close",
        "aclose",
        "lock",
        "alock",
        "tracking_connection",
        "tracking_primary",
    },
)

READ_CALLS = {
    "aget": (("str",), {}),
    "aget_many": ((["str"],), {}),
    "ahas_key": (("str",), {}),
    "info": ((), {}),
    "slowlog_get": ((), {}),
    "slowlog_len": ((), {}),
    "keys": (("*",), {}),
    "iter_keys": (("*",), {}),
    "scan": ((), {}),
    "sscan": (("set",), {}),
    "sscan_iter": (("set",), {}),
    "ttl": (("str",), {}),
    "pttl": (("str",), {}),
    "expiretime": (("str",), {}),
    "type": (("str",), {}),
    "memory_usage": (("str",), {}),
    "hget": (("hash", "f"), {}),
    "hgetall": (("hash",), {}),
    "hmget": (("hash", "f"), {}),
    "hlen": (("hash",), {}),
    "hkeys": (("hash",), {}),
    "hvals": (("hash",), {}),
    "hexists": (("hash", "f"), {}),
    "hexpiretime": (("hash", "f"), {}),
    "hpttl": (("hash", "f"), {}),
    "httl": (("hash", "f"), {}),
    "lindex": (("list", 0), {}),
    "llen": (("list",), {}),
    "lpos": (("list", "v"), {}),
    "lrange": (("list", 0, -1), {}),
    "scard": (("set",), {}),
    "sismember": (("set", "v"), {}),
    "smismember": (("set", "v"), {}),
    "smembers": (("set",), {}),
    "srandmember": (("set",), {}),
    "sdiff": ((["set", "set2"],), {}),
    "sinter": ((["set", "set2"],), {}),
    "sunion": ((["set", "set2"],), {}),
    "zcard": (("zset",), {}),
    "zcount": (("zset", "-inf", "+inf"), {}),
    "zmscore": (("zset", "v"), {}),
    "zrange": (("zset", 0, -1), {}),
    "zrangebyscore": (("zset", "-inf", "+inf"), {}),
    "zrank": (("zset", "v"), {}),
    "zrevrange": (("zset", 0, -1), {}),
    "zrevrangebyscore": (("zset", "+inf", "-inf"), {}),
    "zrevrank": (("zset", "v"), {}),
    "zscore": (("zset", "v"), {}),
    "xinfo_consumers": (("stream", "g"), {}),
    "xinfo_groups": (("stream",), {}),
    "xinfo_stream": (("stream",), {}),
    "xlen": (("stream",), {}),
    "xpending": (("stream", "g"), {}),
    "xrange": (("stream",), {}),
    "xread": (({"stream": "0"},), {}),
    "xrevrange": (("stream",), {}),
}

PIPELINE_READ_CALLS = {
    **{name: call for name, call in READ_CALLS.items() if name in PIPELINE_READ_METHODS},
    "exists": (("str",), {}),
    "get": (("str",), {}),
    "sdiff": (("set", "set2"), {}),
    "sinter": (("set", "set2"), {}),
    "sunion": (("set", "set2"), {}),
    "zmscore": (("zset", ["v"]), {}),
    "xpending_range": (("stream", "g"), {"min": "-", "max": "+", "count": 10}),
}

FIELD_TTL_READS = frozenset({"hexpiretime", "hpttl", "httl"})


def _arrange_keys(server):
    server.set("str", "v")
    server.hset("hash", "f", "v")
    server.rpush("list", "v")
    server.sadd("set", "v")
    server.sadd("set2", "v")
    server.zadd("zset", {"v": 1})
    server.xadd("stream", {"f": "v"})
    server.xgroup_create("stream", "g", id="0")
    server.xreadgroup("g", "c", {"stream": ">"})
    server.config_resetstat()


def _skip_missing_command(name, cache):
    if name.removeprefix("a") in FIELD_TTL_READS:
        skip_below_server(cache, redis=(7, 4), valkey=(9, 0), feature="hash field TTLs")
    if name.removeprefix("a") == "expiretime":
        skip_below_server(cache, redis=(7, 0), feature="EXPIRETIME")


def _run_read(adapter, name, args, kwargs):
    result = getattr(adapter, name)(*args, **kwargs)
    if not (inspect.isawaitable(result) or hasattr(result, "__aiter__")):
        return list(result) if isinstance(result, Iterator) else result

    async def consume():
        try:
            if hasattr(result, "__aiter__"):
                return [item async for item in result]
            return await result
        finally:
            await adapter.aclose()

    return asyncio.run(consume())


@pytest.mark.parametrize("name", sorted(READ_METHODS - NOT_COMMANDS))
def test_an_allowlisted_method_sends_no_writing_command(name, plain, server):
    call = READ_CALLS.get(name) or READ_CALLS.get(name.removeprefix("a"))
    assert call is not None
    _skip_missing_command(name, plain)
    _arrange_keys(server)

    _run_read(plain.adapter, name, *call)

    sent = _commands(server)
    assert sent
    assert not sent & _writing_commands(server)


@pytest.mark.parametrize("name", sorted(PIPELINE_READ_METHODS))
def test_an_allowlisted_pipeline_method_queues_no_writing_command(name, plain, server):
    call = PIPELINE_READ_CALLS.get(name)
    assert call is not None
    _skip_missing_command(name, plain)
    _arrange_keys(server)

    pipe = plain.adapter.pipeline(transaction=False)
    getattr(pipe, name)(*call[0], **call[1])
    pipe.execute()

    sent = _commands(server)
    assert sent
    assert not sent & _writing_commands(server)


@pytest.mark.usefixtures("resp_adapter")
def test_the_pipeline_read_commands_write_nothing(server):
    assert not {name.lower() for name in READ_COMMANDS} & _writing_commands(server)


def test_an_orm_bump_reaches_the_next_lookup(tracked):
    store = RespStore(tracked)
    store.store("default", "q", store.lookup("default", "q", ["t"]).token, ["row"], 60)
    assert store.lookup("default", "q", ["t"]).hit

    store.bump("default", ["t"])

    assert not store.lookup("default", "q", ["t"]).hit


def test_an_orm_lookup_of_kept_copies_sends_no_command(tracked, server):
    store = RespStore(tracked)
    store.store("default", "q", store.lookup("default", "q", ["t"]).token, ["row"], 60)
    store.lookup("default", "q", ["t"])
    server.config_resetstat()

    assert store.lookup("default", "q", ["t"]).value == ["row"]
    assert _data_commands(server) == set()


def test_an_orm_lookup_outside_the_tracking_prefix_opens_no_tracking_connection(build, plain, server):
    before = _tracking_ids(server)
    cache = build(tracking={"prefix": plain.make_key("sessions:")}, warm_key=None)
    store = RespStore(cache)
    store.store("default", "q", store.lookup("default", "q", ["t"]).token, ["row"], 60)
    server.config_resetstat()

    assert store.lookup("default", "q", ["t"]).value == ["row"]
    assert _data_commands(server) == {"mget"}
    assert tracker_module.trackers() == ()
    assert not _tracking_ids(server) - before


def test_an_orm_lookup_reads_no_ttl_of_a_key_outside_the_tracking_prefix(build, proxy, plain, server):
    tracking = {"prefix": plain.make_key("orm:{default}:g:"), "max_staleness": 3600}
    cache = build(port=proxy.port, tracking=tracking, warm_key="orm:{default}:g:other")
    proxy.pause()
    store = RespStore(cache)
    store.store("default", "q", store.lookup("default", "q", ["t"]).token, ["row"], 60)
    store.lookup("default", "q", ["t"])
    server.config_resetstat()

    assert store.lookup("default", "q", ["t"]).value == ["row"]
    assert _data_commands(server) == {"mget"}


CHECK_KEYS = [f"r{index}" for index in range(30)]
CHECK_STALENESS = 0.1


@pytest.fixture
def slow_proxy(redis_container):
    proxy = TcpProxy(redis_container.host, redis_container.port, reply_delay=0.002)
    yield proxy
    proxy.close()


def _write_once(cache, rng, values, *, own):
    first, second = rng.sample(CHECK_KEYS, 2)
    choice = rng.random()
    if choice < 0.1:
        cache.delete(first)
        return {first: None}
    if not own or choice < 0.6:
        value = next(values)
        cache.set(first, value, timeout=None)
        return {first: value}
    if choice < 0.8:
        written = {first: next(values), second: next(values)}
        cache.set_many(written, timeout=None)
        return written
    written = {first: next(values), second: None}
    pipe = cache.pipeline()
    pipe.set(first, written[first], timeout=None)
    pipe.delete(second)
    pipe.execute()
    return written


def _writer(cache, values, seed, *, own):
    def run(stop, log):
        rng = random.Random(seed)
        while not stop.is_set():
            start = time.monotonic()
            written = _write_once(cache, rng, values, own=own)
            end = time.monotonic()
            log.extend(Op(key, value, start, end, own) for key, value in written.items())
            stop.wait(rng.uniform(0, 0.01))

    return run


def _read_once(cache, rng):
    keys = rng.sample(CHECK_KEYS, 3)
    choice = rng.random()
    if choice < 0.5:
        return "get", {keys[0]: cache.get(keys[0])}
    if choice < 0.75:
        many = cache.get_many(keys)
        return "get_many", {key: many.get(key) for key in keys}
    if choice < 0.9:
        values = cache.adapter.read_many([cache.make_and_validate_key(key) for key in keys])
        found = {key: None if value is None else cache.decode(value) for key, value in zip(keys, values, strict=True)}
        return "read_many", found
    return "has_key", {keys[0]: cache.has_key(keys[0])}


def _reader(cache, seed, seen):
    def run(stop, log):
        rng = random.Random(seed)
        while not stop.is_set():
            start = time.monotonic()
            kind, found = _read_once(cache, rng)
            end = time.monotonic()
            log.extend(Op(key, value, start, end) for key, value in found.items())
            seen.add(kind)
            time.sleep(0.0005)

    return run


def _chaos(proxy, server, seed, seen):
    def run(stop, log):
        rng = random.Random(seed)
        for action in itertools.cycle(["pause", "kill", "pause-kill", "idle"]):
            if stop.is_set():
                return
            if "pause" in action:
                proxy.pause()
            if "kill" in action:
                for client_id in _tracking_ids(server):
                    server.execute_command("CLIENT", "KILL", "ID", client_id)
            stop.wait(rng.uniform(0.05, 0.3))
            proxy.resume()
            seen.add(action)
            stop.wait(rng.uniform(0.05, 0.2))

    return run


def _run_until(done, *workers, seed, minimum=3.0, cap=30.0):
    stop = threading.Event()
    logs = [[] for _ in workers]
    errors = []

    def run(worker, log):
        try:
            worker(stop, log)
        except Exception as e:  # noqa: BLE001
            errors.append(e)
            stop.set()

    threads = [threading.Thread(target=run, args=pair, daemon=True) for pair in zip(workers, logs, strict=True)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + cap
    stop.wait(minimum)
    while not stop.is_set() and not done(logs) and time.monotonic() < deadline:
        stop.wait(0.1)
    stop.set()
    for thread in threads:
        thread.join(10)
    assert not [thread for thread in threads if thread.is_alive()], f"seed {seed}: a worker did not stop"
    assert errors == [], f"seed {seed}"
    return logs


CHAOS_EVENTS = frozenset({"pause", "kill", "pause-kill", "idle", "read_many"})


def test_no_read_returns_a_value_staler_than_the_guarantees_allow(build, plain, server, slow_proxy):
    seed = int(os.environ.get("CACHEX_TRACKING_TEST_SEED") or random.randrange(2**32))
    print(f"CACHEX_TRACKING_TEST_SEED={seed}")  # noqa: T201
    cache = build(port=slow_proxy.port, tracking={"max_staleness": CHECK_STALENESS})
    seen = set()

    def done(logs):
        reads = sum(len(log) for log in logs[2:5])
        return seen >= CHAOS_EVENTS and cache.adapter.tracking_info()["hits"] > reads // 4

    own, other, *read_logs, _ = _run_until(
        done,
        _writer(cache, itertools.count(2, 2), seed, own=True),
        _writer(plain, itertools.count(3, 2), seed + 1, own=False),
        _reader(cache, seed + 2, seen),
        _reader(cache, seed + 3, seen),
        _reader(cache, seed + 4, seen),
        _chaos(slow_proxy, server, seed + 5, seen),
        seed=seed,
    )

    reads = [read for log in read_logs for read in log]
    assert seen >= CHAOS_EVENTS, f"seed {seed}: only {sorted(seen)} within the cap"
    assert cache.adapter.tracking_info()["hits"] > len(reads) // 4, f"seed {seed}"
    stale = stale_reads(own + other, reads, CHECK_STALENESS)
    assert not stale, f"seed {seed}: {len(stale)} stale reads, the first: {stale[:5]}"


STALENESS_WRITES = [Op("k", 1, 0.0, 1.0), Op("k", 2, 5.0, 6.0), Op("k", 3, 8.0, 9.0, own=True)]


@pytest.mark.parametrize(
    ("read", "stale"),
    [
        (Op("k", None, 0.2, 0.3), False),
        (Op("k", 1, 6.2, 6.3), False),
        (Op("k", 2, 6.5, 7.0), False),
        (Op("k", 1, 7.0, 7.5), True),
        (Op("k", 2, 9.1, 9.2), True),
        (Op("other", True, 1.0, 2.0), True),
        (Op("k", 2, 1.5, 2.0), True),
    ],
    ids=[
        "missing-before-the-first-write",
        "overwritten-within-max-staleness",
        "current",
        "overwritten-more-than-max-staleness-ago",
        "overwritten-by-an-own-write",
        "has-key-of-a-key-never-written",
        "value-of-a-later-write",
    ],
)
def test_the_staleness_oracle_flags_overwritten_reads_and_reads_without_a_source(read, stale):
    assert (stale_reads(STALENESS_WRITES, [read], 0.5) == [read]) is stale
