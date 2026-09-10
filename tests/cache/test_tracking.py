"""Tests for the CLIENT TRACKING backed local cache (TrackingCache)."""

import math
import threading
import time
import uuid
from contextlib import closing, contextmanager
from itertools import pairwise
from typing import TYPE_CHECKING, Any

import pytest
from django.core.cache import caches
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings
from django.utils.module_loading import import_string
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError as RedisResponseError
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import ResponseError as ValkeyResponseError

from django_cachex.adapters.protocols import Invalidation
from django_cachex.cache.tracking import _TRACKING_REGISTRY, TrackingCache, _TrackingState
from django_cachex.exceptions import NotSupportedError
from tests.fixtures.cache import (
    ADAPTER_IMAGES,
    BACKENDS,
    _adapter_library_available,
    _get_client_library_options,
    build_cluster_cache_config,
    build_sentinel_cache_config,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator, Sequence

    from django_cachex.cache.resp import RespCache
    from tests.fixtures.containers import RedisContainerInfo

TRANSPORT_PREFIX = "trk"
TRACKED_PREFIXES = (f"{TRANSPORT_PREFIX}:",)

# A killed listener connection surfaces either as the driver's own connection
# error or, once the listener notices the socket is gone, as its own.
LISTENER_ERRORS: tuple[type[Exception], ...] = (
    RedisConnectionError,
    ValkeyConnectionError,
    ConnectionError,
    TimeoutError,
)

# A command the server refuses comes back as the driver's own error type.
SERVER_ERRORS: tuple[type[Exception], ...] = (RedisResponseError, ValkeyResponseError)


def _wait_for(predicate: Callable[[], bool], timeout: float = 5.0, interval: float = 0.02) -> bool:
    """Poll ``predicate`` until it holds or ``timeout`` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _skip_unless_trackable(resp_adapter: str) -> None:
    if resp_adapter == "valkey-glide":
        pytest.skip("valkey-glide cannot host an invalidation listener")
    if not _adapter_library_available(resp_adapter):
        pytest.skip(f"{resp_adapter} library not installed")


def _transport_config(redis_container: RedisContainerInfo, resp_adapter: str) -> dict:
    if resp_adapter in {"redis-py", "valkey-py"}:
        options = _get_client_library_options(ADAPTER_IMAGES[resp_adapter][1])
    else:
        options = {"request_timeout": 5000}
    return {
        "BACKEND": BACKENDS[("default", resp_adapter)],
        "LOCATION": f"redis://{redis_container.host}:{redis_container.port}?db=14",
        "OPTIONS": options,
        "KEY_PREFIX": TRANSPORT_PREFIX,
    }


def _build_tracking_config(
    redis_container: RedisContainerInfo,
    resp_adapter: str,
    *,
    options: dict | None = None,
) -> dict:
    """CACHES with a RESP transport plus one TrackingCache alias under a unique LOCATION."""
    return {
        "transport": _transport_config(redis_container, resp_adapter),
        "default": {
            "BACKEND": "django_cachex.cache.TrackingCache",
            "LOCATION": f"tracking:{uuid.uuid4().hex[:8]}",
            "OPTIONS": {"transport": "transport", "MAX_ENTRIES": 1000, "poll_timeout": 0.1, **(options or {})},
        },
    }


def _kill_client(transport: RespCache, client_id: int) -> None:
    transport.adapter.get_client(write=True).execute_command("CLIENT", "KILL", "ID", client_id)


@contextmanager
def _listener(transport: RespCache, prefixes: Sequence[str] = TRACKED_PREFIXES, timeout: float = 5.0) -> Iterator[Any]:
    listener = transport.adapter.invalidation_listener(prefixes, timeout=timeout)
    try:
        yield listener
    finally:
        listener.close()


class _StubListener:
    """Stands in for the adapter's listener so the loop can be driven without a server."""

    client_ids = (1, 2)

    def __init__(self, message: Invalidation | None = None) -> None:
        self.message = message
        self.poll_timeouts: list[float] = []
        self.pings = 0
        self.closed = False

    def poll(self, timeout: float) -> Invalidation | None:
        self.poll_timeouts.append(timeout)
        time.sleep(0.01)
        return self.message

    def ping(self) -> None:
        self.pings += 1

    def close(self) -> None:
        self.closed = True


@contextmanager
def _offline_cache(**options: Any) -> Iterator[TrackingCache]:
    """A TrackingCache whose listener plumbing can be driven without a transport."""
    location = f"tracking:offline:{uuid.uuid4().hex[:8]}"
    try:
        yield TrackingCache(location, {"OPTIONS": {"transport": "unused", **options}})
    finally:
        _cleanup_registry(location)


@contextmanager
def _pumping(target: Callable[..., Any], *args: Any) -> Iterator[None]:
    """Run a listener loop in a thread and stop it when the block ends."""
    stop_event = threading.Event()
    thread = threading.Thread(target=target, args=(stop_event, *args), daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop_event.set()
        thread.join(5.0)


@pytest.fixture
def listener_transport(redis_container: RedisContainerInfo, resp_adapter: str) -> Iterator[RespCache]:
    """A RESP transport whose adapter can host an invalidation listener."""
    _skip_unless_trackable(resp_adapter)
    with override_settings(CACHES={"transport": _transport_config(redis_container, resp_adapter)}):
        transport = caches["transport"]
        transport.flush_db()
        yield transport
        transport.close()


class TestInvalidationListener:
    def test_write_under_prefix_is_reported(self, listener_transport: RespCache):
        with _listener(listener_transport) as listener:
            listener_transport.set("reported", 1)
            message = listener.poll(5.0)
            assert isinstance(message, Invalidation)
            assert message.keys == (listener_transport.make_key("reported"),)

    def test_flush_is_reported_with_keys_none(self, listener_transport: RespCache):
        with _listener(listener_transport) as listener:
            listener_transport.set("flushed", 1)
            assert listener.poll(5.0).keys == (listener_transport.make_key("flushed"),)
            listener_transport.flush_db()
            assert listener.poll(5.0).keys is None

    def test_write_outside_prefix_is_not_reported(self, listener_transport: RespCache):
        with _listener(listener_transport, ["unrelated:"]) as listener:
            listener_transport.set("silent", 1)
            assert listener.poll(0.3) is None

    def test_empty_prefix_reports_every_key(self, listener_transport: RespCache):
        with _listener(listener_transport, [""]) as listener:
            listener_transport.adapter.get_client(write=True).set("no-prefix-at-all", 1)
            assert listener.poll(5.0).keys == ("no-prefix-at-all",)

    def test_ping_keeps_invalidations_that_arrive_meanwhile(self, listener_transport: RespCache):
        with _listener(listener_transport) as listener:
            listener_transport.set("during-ping", 1)
            time.sleep(0.1)
            listener.ping()
            assert listener.poll(5.0).keys == (listener_transport.make_key("during-ping"),)

    def test_ping_raises_once_the_subscriber_is_gone(self, listener_transport: RespCache):
        with _listener(listener_transport, timeout=1.0) as listener:
            subscriber_id, tracker_id = listener.client_ids
            assert subscriber_id != tracker_id
            _kill_client(listener_transport, subscriber_id)
            with pytest.raises(LISTENER_ERRORS):
                listener.ping()

    def test_ping_raises_when_the_tracker_dropped_its_socket(self, listener_transport: RespCache):
        with _listener(listener_transport, timeout=1.0) as listener:
            listener._track.disconnect()
            with pytest.raises(ConnectionError):
                listener.ping()

    def test_ping_drops_a_tracker_that_stopped_answering(self, listener_transport: RespCache, mocker):
        with _listener(listener_transport, timeout=0.1) as listener:
            mocker.patch.object(listener._track, "can_read", return_value=False)
            with pytest.raises(TimeoutError):
                listener.ping()
            with pytest.raises(ConnectionError):
                listener.ping()

    def test_overlapping_prefixes_are_rejected_by_the_server(self, listener_transport: RespCache):
        with pytest.raises(SERVER_ERRORS, match=r"(?i)overlap"):
            listener_transport.adapter.invalidation_listener(["a:", "a:b:"], timeout=5.0)


class TestInvalidationListenerUnsupported:
    @pytest.mark.parametrize(
        "adapter_path",
        [
            "django_cachex.adapters.redis_py.RedisPyClusterAdapter",
            "django_cachex.adapters.valkey_py.ValkeyPyClusterAdapter",
            "django_cachex.adapters.valkey_glide.ValkeyGlideAdapter",
        ],
    )
    def test_raises_not_supported_before_any_io(self, adapter_path: str):
        adapter_cls = import_string(adapter_path)
        try:
            adapter = adapter_cls(["redis://127.0.0.1:1/0"])
        except ImportError:
            pytest.skip(f"{adapter_path} driver not installed")
        with pytest.raises(NotSupportedError, match="invalidation_listener"):
            adapter.invalidation_listener(["p:"], timeout=1.0)


def _custom_key_func(key: str, prefix: str, version: int) -> str:
    """KEY_FUNCTION whose layout the default ``prefix:`` guess does not match."""
    return f"{prefix}#{version}#{key}"


def _cleanup_registry(*storage_keys: str) -> None:
    for storage_key in storage_keys:
        state = _TRACKING_REGISTRY.pop(storage_key, None)
        if state is not None:
            state.shutdown()


def _tracking_section(cache) -> dict:
    return cache.info()["tracking"]


def _settled(cache, action: Callable[[], Any], count: int = 1) -> Any:
    """Run a write and, under tracking coherence, wait until its own invalidation(s) reached the listener."""
    before = _tracking_section(cache)
    result = action()
    if before["coherence"] == "tracking":
        assert _wait_for(lambda: _tracking_section(cache)["invalidations"] >= before["invalidations"] + count)
    return result


async def _asettled(cache, action: Callable[[], Awaitable[Any]], count: int = 1) -> Any:
    """Async twin of :func:`_settled`."""
    before = _tracking_section(cache)
    result = await action()
    if before["coherence"] == "tracking":
        assert _wait_for(lambda: _tracking_section(cache)["invalidations"] >= before["invalidations"] + count)
    return result


def _run_during_fetch(cache, mocker, hook: Callable[[], Any]) -> None:
    """Run ``hook`` once, after the next fetch pipeline has executed and before its reply is absorbed."""
    adapter = cache._transport.adapter
    real_pipeline = adapter.pipeline
    fired = False

    def racing_pipeline(*args, **kwargs):
        pipe = real_pipeline(*args, **kwargs)
        real_execute = pipe.execute

        def execute():
            nonlocal fired
            results = real_execute()
            if not fired:
                fired = True
                hook()
            return results

        pipe.execute = execute
        return pipe

    mocker.patch.object(adapter, "pipeline", side_effect=racing_pipeline)


def _stampede_transport_config(redis_container: RedisContainerInfo, resp_adapter: str, *, delta: float = 0) -> dict:
    """Tracking config whose transport keeps a 60s stampede buffer; ``delta=0`` never rolls early."""
    config = _build_tracking_config(redis_container, resp_adapter)
    config["transport"]["OPTIONS"]["stampede_prevention"] = {"buffer": 60, "delta": delta}
    return config


@contextmanager
def _tracking(config: dict) -> Iterator[Any]:
    """Serve the ``default`` alias of ``config`` and stop its listener afterwards, pass or fail."""
    entry = config["default"]
    location = entry.get("LOCATION") or entry["OPTIONS"]["transport"]
    with override_settings(CACHES=config):
        try:
            yield caches["default"]
        finally:
            _cleanup_registry(location)


@contextmanager
def _connected(config: dict) -> Iterator[Any]:
    """Like :func:`_tracking`, over a flushed transport and with the listener connected."""
    with _tracking(config) as cache:
        caches["transport"].flush_db()
        cache.get("warm-up")
        assert _wait_for(lambda: cache._state.connected)
        yield cache


BOTH_MODES = pytest.mark.parametrize("coherence", ["tracking", "ttl"])
TTL_MODE = pytest.mark.parametrize("coherence", ["ttl"])


def _coherence_options(coherence: str) -> dict:
    return {"coherence": "ttl", "local_timeout": 60} if coherence == "ttl" else {}


@pytest.fixture
def coherence() -> str:
    """Coherence of the shared fixtures; ``BOTH_MODES`` and ``TTL_MODE`` override it."""
    return "tracking"


@pytest.fixture
def tracking_config(redis_container: RedisContainerInfo, resp_adapter: str, coherence: str) -> dict:
    if coherence == "tracking":
        _skip_unless_trackable(resp_adapter)
    elif not _adapter_library_available(resp_adapter):
        pytest.skip(f"{resp_adapter} library not installed")
    return _build_tracking_config(redis_container, resp_adapter, options=_coherence_options(coherence))


@pytest.fixture
def tracking_cache(tracking_config: dict) -> Iterator:
    with _connected(tracking_config) as cache:
        yield cache


@pytest.fixture
def tracking_pair(redis_container: RedisContainerInfo, resp_adapter: str) -> Iterator[tuple]:
    """Two TrackingCache aliases with their own local stores sharing one transport (two pods)."""
    _skip_unless_trackable(resp_adapter)
    config = _build_tracking_config(redis_container, resp_adapter)
    entry = config.pop("default")
    locations = [f"tracking:pod{i}:{uuid.uuid4().hex[:6]}" for i in (1, 2)]
    for pod, location in zip(("pod1", "pod2"), locations, strict=True):
        config[pod] = {**entry, "LOCATION": location}
    with override_settings(CACHES=config):
        try:
            caches["transport"].flush_db()
            pod1, pod2 = caches["pod1"], caches["pod2"]
            pod1.get("warm-up")
            pod2.get("warm-up")
            assert _wait_for(lambda: pod1._state.connected and pod2._state.connected)
            yield pod1, pod2
        finally:
            _cleanup_registry(*locations)


class TestTrackingConfig:
    def test_missing_transport_raises(self):
        with pytest.raises(ImproperlyConfigured, match="transport"):
            TrackingCache("", {"OPTIONS": {}})

    def test_key_prefix_on_the_tracking_alias_is_rejected(self):
        with pytest.raises(ImproperlyConfigured, match="KEY_PREFIX"):
            TrackingCache("", {"OPTIONS": {"transport": "t"}, "KEY_PREFIX": "x"})
        with pytest.raises(ImproperlyConfigured, match="KEY_PREFIX"):
            TrackingCache("", {"OPTIONS": {"transport": "t", "KEY_PREFIX": "x"}})

    def test_overlapping_prefixes_are_rejected(self):
        with pytest.raises(ImproperlyConfigured, match="overlap"):
            TrackingCache("", {"OPTIONS": {"transport": "t", "prefixes": ["a:", "a:b:"]}})

    def test_local_timeout_is_coerced_to_float(self):
        try:
            cache = TrackingCache("", {"OPTIONS": {"transport": "t", "local_timeout": "2"}})
            assert cache._local_timeout == 2.0
        finally:
            _cleanup_registry("t")

    def test_non_iterable_prefixes_are_rejected(self):
        with pytest.raises(ImproperlyConfigured, match="prefixes"):
            TrackingCache("", {"OPTIONS": {"transport": "t", "prefixes": 3}})

    def test_unknown_coherence_is_rejected(self):
        with pytest.raises(ImproperlyConfigured, match="coherence"):
            TrackingCache("", {"OPTIONS": {"transport": "t", "coherence": "eventual"}})

    def test_ttl_coherence_requires_local_timeout(self):
        with pytest.raises(ImproperlyConfigured, match="local_timeout"):
            TrackingCache("", {"OPTIONS": {"transport": "t", "coherence": "ttl"}})

    def test_one_storage_key_cannot_mix_coherence_modes(self):
        try:
            TrackingCache("tracking:mixed", {"OPTIONS": {"transport": "t"}})
            with pytest.raises(ImproperlyConfigured, match="coherence"):
                TrackingCache("tracking:mixed", {"OPTIONS": {"transport": "t", "coherence": "ttl", "local_timeout": 1}})
        finally:
            _cleanup_registry("tracking:mixed")

    def test_non_resp_transport_is_rejected_on_first_use(self):
        config = {
            "plain": {"BACKEND": "django_cachex.cache.LocMemCache"},
            "default": {
                "BACKEND": "django_cachex.cache.TrackingCache",
                "LOCATION": "tracking:non-resp",
                "OPTIONS": {"transport": "plain"},
            },
        }
        with _tracking(config) as cache, pytest.raises(ImproperlyConfigured, match=r"Redis|Valkey"):
            cache.get("k")

    def test_glide_transport_is_rejected_on_first_use(self, redis_container: RedisContainerInfo, resp_adapter: str):
        if resp_adapter != "valkey-glide":
            pytest.skip("glide-specific")
        if not _adapter_library_available(resp_adapter):
            pytest.skip("valkey-glide not installed")
        config = _build_tracking_config(redis_container, resp_adapter)
        with _tracking(config) as cache, pytest.raises(ImproperlyConfigured, match="valkey-glide"):
            cache.get("k")

    def test_cluster_transport_is_rejected_on_first_use(self, resp_adapter: str):
        _skip_unless_trackable(resp_adapter)
        config = {
            "transport": {
                "BACKEND": BACKENDS[("cluster", resp_adapter)],
                "LOCATION": "redis://127.0.0.1:1",
            },
            "default": {
                "BACKEND": "django_cachex.cache.TrackingCache",
                "LOCATION": "tracking:cluster",
                "OPTIONS": {"transport": "transport"},
            },
        }
        with _tracking(config) as cache, pytest.raises(ImproperlyConfigured, match="cluster"):
            cache.get("k")

    def test_default_prefix_follows_the_transport_key_prefix(self, tracking_cache):
        assert _tracking_section(tracking_cache)["prefixes"] == [f"{TRANSPORT_PREFIX}:"]

    def test_custom_key_function_defaults_to_every_key(self, redis_container: RedisContainerInfo, resp_adapter: str):
        _skip_unless_trackable(resp_adapter)
        config = _build_tracking_config(redis_container, resp_adapter)
        config["transport"]["KEY_FUNCTION"] = _custom_key_func
        with _connected(config) as cache:
            cache.set("k", 1)
            assert cache.get("k") == 1
            assert _tracking_section(cache)["prefixes"] == [""]

    def test_key_outside_every_prefix_is_refused(self, redis_container: RedisContainerInfo, resp_adapter: str):
        _skip_unless_trackable(resp_adapter)
        config = _build_tracking_config(redis_container, resp_adapter, options={"prefixes": [f"{TRANSPORT_PREFIX}:"]})
        config["transport"]["KEY_FUNCTION"] = _custom_key_func
        with _tracking(config) as cache, pytest.raises(ImproperlyConfigured, match="prefix"):
            cache.get("k")

    def test_location_defaults_to_the_transport_alias(self, redis_container: RedisContainerInfo, resp_adapter: str):
        _skip_unless_trackable(resp_adapter)
        config = _build_tracking_config(redis_container, resp_adapter)
        del config["default"]["LOCATION"]
        with _connected(config):
            assert "transport" in _TRACKING_REGISTRY

    def test_cachex_support_level(self, tracking_cache):
        assert tracking_cache._cachex_support == "limited"


@TTL_MODE
class TestTrackingTTLCoherence:
    def test_serves_locally_without_a_listener(self, tracking_cache):
        tracking_cache.set("k", 1)
        assert tracking_cache.get("k") == 1
        assert tracking_cache.get("k") == 1
        section = _tracking_section(tracking_cache)
        assert section["coherence"] == "ttl"
        assert section["hits"] == 1
        assert section["listener_alive"] is False
        assert section["prefixes"] == []
        assert tracking_cache._state.listener_thread is None
        assert not [t for t in threading.enumerate() if t.name == f"tracking-cache-{tracking_cache._storage_key}"]

    def test_remote_writes_stay_invisible_until_the_local_copy_expires(self, tracking_config: dict):
        tracking_config["default"]["OPTIONS"]["local_timeout"] = 1
        with _connected(tracking_config) as cache:
            cache.set("k", "old")
            assert cache.get("k") == "old"
            caches["transport"].set("k", "new")
            assert cache.get("k") == "old"
            assert _wait_for(lambda: cache.get("k") == "new")

    def test_keys_outside_the_tracked_prefixes_are_accepted(self, tracking_config: dict):
        tracking_config["default"]["OPTIONS"]["prefixes"] = ["nope:"]
        with _connected(tracking_config) as cache:
            cache.set("k", 1)
            assert cache.get("k") == 1

    def test_shutdown_then_use_caches_again_without_a_thread(self, tracking_cache):
        tracking_cache.set("k", 1)
        assert tracking_cache.get("k") == 1
        tracking_cache.shutdown()
        assert _tracking_section(tracking_cache)["entries"] == 0
        assert tracking_cache.get("k") == 1
        assert tracking_cache.get("k") == 1
        section = _tracking_section(tracking_cache)
        assert section["hits"] == 1
        assert section["listener_alive"] is False

    def test_cluster_transport_is_accepted(self, cluster_container, resp_adapter: str, coherence: str):
        if not _adapter_library_available(resp_adapter):
            pytest.skip(f"{resp_adapter} library not installed")
        host, port = cluster_container
        transport = build_cluster_cache_config(host, port, resp_adapter=resp_adapter)["default"]
        transport["KEY_PREFIX"] = TRANSPORT_PREFIX
        config = {
            "transport": transport,
            "default": {
                "BACKEND": "django_cachex.cache.TrackingCache",
                "LOCATION": f"tracking:cluster:{uuid.uuid4().hex[:6]}",
                "OPTIONS": {"transport": "transport", **_coherence_options(coherence)},
            },
        }
        with _connected(config) as cache:
            cache.set("c", 1)
            assert cache.get("c") == 1
            assert cache.get("c") == 1
            assert _tracking_section(cache)["hits"] == 1


@BOTH_MODES
class TestTrackingReads:
    def test_roundtrip(self, tracking_cache):
        tracking_cache.set("rt", {"a": [1, 2]})
        assert tracking_cache.get("rt") == {"a": [1, 2]}

    def test_second_get_is_served_locally(self, tracking_cache, mocker):
        _settled(tracking_cache, lambda: tracking_cache.set("local", "v"))
        spy = mocker.spy(tracking_cache._transport.adapter, "pipeline")
        assert tracking_cache.get("local") == "v"
        assert tracking_cache.get("local") == "v"
        assert spy.call_count == 1
        section = _tracking_section(tracking_cache)
        assert section["hits"] == 1
        assert section["entries"] == 1

    def test_local_hits_return_fresh_objects(self, tracking_cache):
        tracking_cache.set("obj", {"a": 1})
        first = tracking_cache.get("obj")
        first["a"] = 2
        assert tracking_cache.get("obj") == {"a": 1}

    def test_missing_key_returns_default_and_is_not_cached(self, tracking_cache):
        assert tracking_cache.get("absent", "dflt") == "dflt"
        assert _tracking_section(tracking_cache)["entries"] == 0

    def test_stored_none_is_a_hit(self, tracking_cache):
        tracking_cache.set("none", None)
        assert tracking_cache.get("none", "dflt") is None
        assert tracking_cache.get("none", "dflt") is None

    def test_integers_roundtrip_through_the_local_store(self, tracking_cache):
        tracking_cache.set("n", 42)
        assert tracking_cache.get("n") == 42
        assert tracking_cache.get("n") == 42
        tracking_cache.incr("n", 8)
        assert tracking_cache.get("n") == 50

    def test_get_many_mixes_local_and_remote(self, tracking_cache, mocker):
        _settled(tracking_cache, lambda: tracking_cache.set_many({"m1": 1, "m2": 2, "m3": 3}), count=3)
        assert tracking_cache.get("m1") == 1
        spy = mocker.spy(tracking_cache._transport.adapter, "pipeline")
        assert tracking_cache.get_many(["m1", "m2", "m3", "m4"]) == {"m1": 1, "m2": 2, "m3": 3}
        assert spy.call_count == 1
        assert tracking_cache.get_many(["m1", "m2", "m3"]) == {"m1": 1, "m2": 2, "m3": 3}
        assert spy.call_count == 1

    def test_has_key_uses_the_local_copy_then_the_transport(self, tracking_cache, mocker):
        _settled(tracking_cache, lambda: tracking_cache.set("hk", 1))
        assert tracking_cache.has_key("hk")
        assert not tracking_cache.has_key("hk-missing")
        assert tracking_cache.get("hk") == 1
        spy = mocker.spy(tracking_cache._transport.adapter, "has_key")
        assert tracking_cache.has_key("hk")
        assert spy.call_count == 0

    def test_get_or_set_populates(self, tracking_cache):
        assert tracking_cache.get_or_set("gos", "computed") == "computed"
        assert tracking_cache.get("gos") == "computed"

    def test_version_is_part_of_the_local_key(self, tracking_cache):
        tracking_cache.set("ver", "v1", version=1)
        tracking_cache.set("ver", "v2", version=2)
        assert tracking_cache.get("ver", version=1) == "v1"
        assert tracking_cache.get("ver", version=2) == "v2"
        assert tracking_cache.get("ver", version=1) == "v1"


@BOTH_MODES
class TestTrackingWrites:
    def test_set_replaces_the_local_copy(self, tracking_cache):
        tracking_cache.set("w", 1)
        assert tracking_cache.get("w") == 1
        tracking_cache.set("w", 2)
        assert tracking_cache.get("w") == 2

    def test_set_flags_pass_through(self, tracking_cache):
        assert tracking_cache.set("flag", 1, nx=True) is True
        assert tracking_cache.get("flag") == 1
        assert tracking_cache.set("flag", 2, nx=True) is False
        assert tracking_cache.get("flag") == 1
        assert tracking_cache.set("flag", 3, get=True) == 1
        assert tracking_cache.get("flag") == 3

    def test_delete_evicts(self, tracking_cache):
        tracking_cache.set("d", 1)
        assert tracking_cache.get("d") == 1
        assert tracking_cache.delete("d") is True
        assert tracking_cache.get("d") is None

    def test_delete_many_and_set_many_evict(self, tracking_cache):
        tracking_cache.set_many({"a": 1, "b": 2})
        assert tracking_cache.get_many(["a", "b"]) == {"a": 1, "b": 2}
        tracking_cache.set_many({"a": 10})
        assert tracking_cache.get("a") == 10
        tracking_cache.delete_many(["a", "b"])
        assert tracking_cache.get_many(["a", "b"]) == {}

    def test_add_touch_incr_decr_evict(self, tracking_cache):
        assert tracking_cache.add("c", 5) is True
        assert tracking_cache.get("c") == 5
        assert tracking_cache.incr("c") == 6
        assert tracking_cache.get("c") == 6
        assert tracking_cache.decr("c", 2) == 4
        assert tracking_cache.get("c") == 4
        assert tracking_cache.touch("c", 1) is True
        assert tracking_cache.ttl("c") == 1
        assert tracking_cache.get("c") == 4

    def test_expire_and_persist_evict(self, tracking_cache):
        tracking_cache.set("e", 1, timeout=None)
        assert tracking_cache.get("e") == 1
        assert tracking_cache.expire("e", 1) is True
        assert tracking_cache._state.store == {}
        assert tracking_cache.get("e") == 1
        assert tracking_cache.persist("e") is True
        assert tracking_cache._state.store == {}

    def test_clear_flushes_the_local_store(self, tracking_cache):
        tracking_cache.set("x", 1)
        assert tracking_cache.get("x") == 1
        assert tracking_cache.clear() is True
        assert _tracking_section(tracking_cache)["entries"] == 0
        assert tracking_cache.get("x") is None

    def test_delete_pattern_evicts_matching_keys_only(self, tracking_cache):
        _settled(tracking_cache, lambda: tracking_cache.set_many({"user:1": 1, "user:2": 2, "other": 3}), count=3)
        assert tracking_cache.get_many(["user:1", "user:2", "other"]) == {"user:1": 1, "user:2": 2, "other": 3}
        assert tracking_cache.delete_pattern("user:*") == 2
        assert set(tracking_cache._state.store) == {tracking_cache.make_key("other")}
        assert tracking_cache.get("user:1") is None
        assert tracking_cache.get("other") == 3

    def test_delete_pattern_reads_the_pattern_as_a_redis_glob(self, tracking_cache):
        # ``[^0]`` is negation for Redis and a two-member class for fnmatch; the
        # local store has to read the pattern the way the transport does.
        _settled(tracking_cache, lambda: tracking_cache.set_many({"user0x": 0, "user1x": 1}), count=2)
        assert tracking_cache.get_many(["user0x", "user1x"]) == {"user0x": 0, "user1x": 1}
        assert tracking_cache.delete_pattern("user[^0]*") == 1
        assert set(tracking_cache._state.store) == {tracking_cache.make_key("user0x")}
        assert tracking_cache.get("user1x") is None

    def test_delete_pattern_drops_a_fetch_in_flight_with_the_listener_muted(self, tracking_cache, mocker):
        tracking_cache.set("user:1", "old")
        mocker.patch.object(_TrackingState, "apply", return_value=None)
        _run_during_fetch(tracking_cache, mocker, lambda: tracking_cache.delete_pattern("user:*"))
        assert tracking_cache.get("user:1") == "old"
        assert tracking_cache.make_key("user:1") not in tracking_cache._state.store


@BOTH_MODES
class TestTrackingVersions:
    """``VERSION`` lives on the transport alias, which is where the keys are made."""

    @pytest.fixture
    def versioned_cache(self, tracking_config: dict) -> Iterator[Any]:
        tracking_config["transport"]["VERSION"] = 2
        with _connected(tracking_config) as cache:
            yield cache

    def test_incr_version_moves_the_key_on_the_transport(self, versioned_cache):
        versioned_cache.set("v", "value")
        assert versioned_cache.get("v") == "value"
        assert versioned_cache.incr_version("v") == 3
        assert versioned_cache._state.store == {}
        assert versioned_cache.get("v") is None
        assert versioned_cache.get("v", version=3) == "value"
        assert caches["transport"].get("v", version=3) == "value"

    def test_decr_version_moves_the_key_back(self, versioned_cache):
        versioned_cache.set("v", "value", version=3)
        assert versioned_cache.decr_version("v", version=3) == 2
        assert versioned_cache.get("v") == "value"

    @pytest.mark.asyncio
    async def test_aincr_version_moves_the_key_on_the_transport(self, versioned_cache):
        await versioned_cache.aset("v", "value")
        assert await versioned_cache.aget("v") == "value"
        assert await versioned_cache.aincr_version("v") == 3
        assert versioned_cache._state.store == {}
        assert await versioned_cache.aget("v") is None
        assert await versioned_cache.aget("v", version=3) == "value"

    @pytest.mark.asyncio
    async def test_adecr_version_moves_the_key_back(self, versioned_cache):
        await versioned_cache.aset("v", "value", version=3)
        assert await versioned_cache.adecr_version("v", version=3) == 2
        assert await versioned_cache.aget("v") == "value"


class TestTrackingInvalidation:
    def test_write_on_another_pod_evicts_the_local_copy(self, tracking_pair):
        pod1, pod2 = tracking_pair
        pod1.set("shared", "old")
        assert pod1.get("shared") == "old"
        assert pod2.get("shared") == "old"
        pod2.set("shared", "new")
        assert pod2.get("shared") == "new"
        assert _wait_for(lambda: pod1.get("shared") == "new")

    def test_transport_delete_evicts(self, tracking_pair):
        pod1, _pod2 = tracking_pair
        pod1.set("gone", 1)
        assert pod1.get("gone") == 1
        caches["transport"].delete("gone")
        assert _wait_for(lambda: pod1.get("gone") is None)

    def test_transport_expire_evicts(self, tracking_pair):
        pod1, _pod2 = tracking_pair
        pod1.set("exp", 1, timeout=None)
        assert pod1.get("exp") == 1
        before = _tracking_section(pod1)["invalidations"]
        caches["transport"].expire("exp", 100)
        assert _wait_for(lambda: _tracking_section(pod1)["invalidations"] > before)
        assert pod1._state.store == {}

    def test_flush_db_flushes_every_pod(self, tracking_pair):
        pod1, pod2 = tracking_pair
        pod1.set_many({"f1": 1, "f2": 2})
        assert pod1.get_many(["f1", "f2"]) == {"f1": 1, "f2": 2}
        assert pod2.get_many(["f1", "f2"]) == {"f1": 1, "f2": 2}
        caches["transport"].flush_db()
        assert _wait_for(lambda: _tracking_section(pod1)["entries"] == 0 and _tracking_section(pod2)["entries"] == 0)
        assert _tracking_section(pod1)["flushes"] >= 1
        assert pod1.get("f1") is None

    def test_own_writes_are_counted_as_invalidations_too(self, tracking_cache):
        before = _tracking_section(tracking_cache)["invalidations"]
        tracking_cache.set("mine", 1)
        assert _wait_for(lambda: _tracking_section(tracking_cache)["invalidations"] > before)


class TestTrackingTTL:
    @BOTH_MODES
    def test_local_expiry_never_exceeds_the_key_ttl(self, tracking_cache):
        _settled(tracking_cache, lambda: tracking_cache.set("ttl", 1, timeout=2))
        assert tracking_cache.get("ttl") == 1
        _raw, expires_at = tracking_cache._state.store[tracking_cache.make_key("ttl")]
        assert expires_at is not None
        assert expires_at <= time.monotonic() + 2.0

    def test_key_without_expiry_has_no_local_expiry(self, tracking_cache):
        _settled(tracking_cache, lambda: tracking_cache.set("forever", 1, timeout=None))
        assert tracking_cache.get("forever") == 1
        _raw, expires_at = tracking_cache._state.store[tracking_cache.make_key("forever")]
        assert expires_at is None

    @BOTH_MODES
    def test_expired_local_entry_is_refetched(self, tracking_cache):
        tracking_cache.set("short", 1, timeout=1)
        assert tracking_cache.get("short") == 1
        assert _wait_for(lambda: tracking_cache.get("short") is None, timeout=3.0)

    def test_local_timeout_caps_the_local_lifetime(
        self,
        redis_container: RedisContainerInfo,
        resp_adapter: str,
        mocker,
    ):
        _skip_unless_trackable(resp_adapter)
        config = _build_tracking_config(redis_container, resp_adapter, options={"local_timeout": 0.2})
        with _connected(config) as cache:
            _settled(cache, lambda: cache.set("capped", 1))
            spy = mocker.spy(cache._transport.adapter, "pipeline")
            assert cache.get("capped") == 1
            assert cache.get("capped") == 1
            time.sleep(0.3)
            assert cache.get("capped") == 1
            assert spy.call_count == 2

    def test_stampede_buffer_is_stripped_from_the_local_expiry(
        self,
        redis_container: RedisContainerInfo,
        resp_adapter: str,
    ):
        _skip_unless_trackable(resp_adapter)
        with _connected(_stampede_transport_config(redis_container, resp_adapter)) as cache:
            _settled(cache, lambda: cache.set("buffered", 1, timeout=30))
            assert cache.get("buffered") == 1
            _raw, expires_at = cache._state.store[cache.make_key("buffered")]
            assert expires_at <= time.monotonic() + 30.0
            caches["transport"].expire("buffered", 50, stampede_prevention=False)
            assert _wait_for(lambda: cache._state.store == {})
            assert cache.get("buffered") is None
            assert cache._state.store == {}

    def test_local_hits_roll_the_stampede_dice(self, redis_container: RedisContainerInfo, resp_adapter: str, mocker):
        _skip_unless_trackable(resp_adapter)
        with _connected(_stampede_transport_config(redis_container, resp_adapter, delta=1.0)) as cache:
            _settled(cache, lambda: cache.set("rolled", 1, timeout=30))
            assert cache.get("rolled") == 1
            assert _tracking_section(cache)["entries"] == 1
            mocker.patch("random.expovariate", return_value=math.inf)
            assert cache.get("rolled") is None
            assert _tracking_section(cache)["entries"] == 0

    def test_a_local_hit_rolls_the_dice_once(self, redis_container: RedisContainerInfo, resp_adapter: str, mocker):
        # Rolling again in ``_absorb`` after a refetch would square the odds,
        # making early recompute far rarer locally than on the transport.
        _skip_unless_trackable(resp_adapter)
        with _connected(_stampede_transport_config(redis_container, resp_adapter, delta=1.0)) as cache:
            _settled(cache, lambda: cache.set("once", 1, timeout=30))
            assert cache.get("once") == 1
            local_roll = mocker.patch(
                "django_cachex.cache.tracking.should_recompute_remaining",
                return_value=True,
            )
            transport_roll = mocker.patch("django_cachex.cache.tracking.should_recompute")
            assert cache.get("once") is None
            assert local_roll.call_count == 1
            assert transport_roll.call_count == 0
            assert _tracking_section(cache)["entries"] == 0

    def test_a_local_hit_that_rolls_drops_out_of_get_many(
        self,
        redis_container: RedisContainerInfo,
        resp_adapter: str,
        mocker,
    ):
        _skip_unless_trackable(resp_adapter)
        with _connected(_stampede_transport_config(redis_container, resp_adapter, delta=1.0)) as cache:
            _settled(cache, lambda: cache.set_many({"g1": 1, "g2": 2}, timeout=30), count=2)
            assert cache.get_many(["g1", "g2"]) == {"g1": 1, "g2": 2}
            mocker.patch("django_cachex.cache.tracking.should_recompute_remaining", return_value=True)
            transport_roll = mocker.patch("django_cachex.cache.tracking.should_recompute")
            assert cache.get_many(["g1", "g2"]) == {}
            assert transport_roll.call_count == 0

    def test_get_or_set_refreshes_a_logically_expired_key(self, redis_container: RedisContainerInfo, resp_adapter: str):
        _skip_unless_trackable(resp_adapter)
        with _connected(_stampede_transport_config(redis_container, resp_adapter)) as cache:
            _settled(cache, lambda: cache.set("gos", "stale", timeout=30))
            _settled(cache, lambda: caches["transport"].expire("gos", 50, stampede_prevention=False))
            assert cache.get_or_set("gos", "fresh", timeout=30) == "fresh"
            assert caches["transport"].get("gos", stampede_prevention=False) == "fresh"

    @BOTH_MODES
    def test_unexpected_pttl_is_served_but_not_kept(self, tracking_cache):
        state = tracking_cache._state
        made_key = tracking_cache.make_key("odd")
        tokens = state.begin_fetch([made_key])
        assert tracking_cache._absorb([made_key], [b"raw", None], tokens) == {made_key: b"raw"}
        assert made_key not in state.store
        assert made_key not in state.pending


class TestTrackingRace:
    def test_invalidation_during_the_fetch_drops_the_result(self, tracking_cache, mocker):
        transport = tracking_cache._transport
        state = tracking_cache._state
        tracking_cache.set("raced", "old")

        def overwrite_and_settle() -> None:
            before = state.invalidations
            transport.set("raced", "new")
            assert _wait_for(lambda: state.invalidations > before)

        _run_during_fetch(tracking_cache, mocker, overwrite_and_settle)
        assert tracking_cache.get("raced") == "old"
        assert tracking_cache.make_key("raced") not in state.store
        assert tracking_cache.get("raced") == "new"


class TestTrackingListenerLifecycle:
    def test_nothing_is_cached_until_the_listener_is_connected(self, tracking_config: dict, mocker):
        with _tracking(tracking_config) as cache:
            transport = caches["transport"]
            transport.flush_db()
            transport.set("early", 1)
            patched = mocker.patch.object(
                type(transport.adapter),
                "invalidation_listener",
                side_effect=ConnectionError("simulated outage"),
            )
            assert cache.get("early") == 1
            assert cache._state.connected is False
            assert _tracking_section(cache)["entries"] == 0
            mocker.stop(patched)
            assert _wait_for(lambda: cache._state.connected)
            assert cache.get("early") == 1
            assert _tracking_section(cache)["entries"] == 1

    def test_losing_the_subscriber_flushes_and_reconnects(self, tracking_cache):
        transport = tracking_cache._transport
        _settled(tracking_cache, lambda: tracking_cache.set("survivor", 1))
        assert tracking_cache.get("survivor") == 1
        subscriber_id, _tracker_id = tracking_cache._state.listener.client_ids
        _kill_client(transport, subscriber_id)
        assert _wait_for(lambda: _tracking_section(tracking_cache)["reconnects"] >= 1)
        assert _wait_for(lambda: tracking_cache._state.connected)
        assert _tracking_section(tracking_cache)["flushes"] >= 1
        assert tracking_cache.get("survivor") == 1
        assert _tracking_section(tracking_cache)["entries"] == 1

    def test_losing_the_tracker_is_caught_by_the_health_check(
        self,
        redis_container: RedisContainerInfo,
        resp_adapter: str,
    ):
        _skip_unless_trackable(resp_adapter)
        config = _build_tracking_config(
            redis_container,
            resp_adapter,
            options={"health_check_interval": 0.2, "poll_timeout": 0.05},
        )
        with _connected(config) as cache:
            _subscriber_id, tracker_id = cache._state.listener.client_ids
            _kill_client(cache._transport, tracker_id)
            assert _wait_for(lambda: _tracking_section(cache)["reconnects"] >= 1)
            assert _wait_for(lambda: cache._state.connected)

    def test_shutdown_then_use_restarts_one_listener(self, tracking_cache):
        storage_key = tracking_cache._storage_key
        tracking_cache.shutdown()
        assert tracking_cache._state.connected is False
        assert tracking_cache.get("after-shutdown") is None
        assert _wait_for(lambda: tracking_cache._state.connected)
        listeners = [t for t in threading.enumerate() if t.name == f"tracking-cache-{storage_key}"]
        assert len(listeners) == 1

    def test_failed_thread_start_leaves_nothing_connected(self, tracking_cache, mocker):
        state = tracking_cache._state
        tracking_cache.shutdown()
        mocker.patch.object(threading.Thread, "start", side_effect=RuntimeError("no threads"))
        with pytest.raises(RuntimeError, match="no threads"):
            tracking_cache.get("k")
        assert state.connected is False
        assert state.listener is None
        mocker.stopall()
        assert tracking_cache.get("k") is None
        assert _wait_for(lambda: state.connected)

    def test_shutdown_settles_the_state_before_releasing_the_start_lock(self, tracking_cache, mocker):
        state = tracking_cache._state
        tracking_cache.shutdown()
        hang = threading.Event()
        mocker.patch.object(TrackingCache, "_serve", side_effect=lambda *_: hang.wait())
        assert tracking_cache.get("k") is None
        assert state.connected
        mocker.patch("django_cachex.cache.tracking._LISTENER_TIMEOUT", 0.0)
        connected_at_release = []
        real_lock = state.start_lock

        class ObservedLock:
            def __enter__(self):
                real_lock.acquire()

            def __exit__(self, *exc):
                connected_at_release.append(state.connected)
                real_lock.release()

        state.start_lock = ObservedLock()
        try:
            tracking_cache.shutdown()
        finally:
            hang.set()
        assert connected_at_release == [False]

    def test_close_keeps_the_listener(self, tracking_cache):
        tracking_cache.close()
        assert tracking_cache._state.connected is True

    def test_threads_share_one_state(self, tracking_config: dict):
        location = tracking_config["default"]["LOCATION"]
        states: list[object] = []
        barrier = threading.Barrier(4)
        with _tracking(tracking_config):

            def worker() -> None:
                barrier.wait(10.0)
                cache = caches["default"]
                cache.get("shared")
                states.append(cache._state)

            threads = [threading.Thread(target=worker) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10.0)
            assert len({id(state) for state in states}) == 1
            listeners = [t for t in threading.enumerate() if t.name == f"tracking-cache-{location}"]
            assert len(listeners) == 1

    def test_info_reports_the_tracking_section(self, tracking_cache):
        _settled(tracking_cache, lambda: tracking_cache.set("i", 1))
        tracking_cache.get("i")
        tracking_cache.get("i")
        tracking_cache.get("missing")
        info = tracking_cache.info()
        assert "redis_version" in info
        section = info["tracking"]
        assert section["coherence"] == "tracking"
        assert section["connected"] is True
        assert section["listener_alive"] is True
        assert section["entries"] == 1
        assert section["hits"] == 1
        assert section["misses"] >= 2
        assert section["reconnects"] == 0
        assert isinstance(section["last_message_age_seconds"], float | type(None))


class TestTrackingListenerLoop:
    """The listener loop and the state it keeps, driven without a server."""

    def test_health_check_pings_while_invalidations_keep_arriving(self):
        # The tracking connection sees no traffic of its own, so a server that
        # closes idle clients would drop it unnoticed if the ping waited for
        # the subscriber to fall idle.
        with _offline_cache(poll_timeout=0.01, health_check_interval=0.05) as cache:
            listener = _StubListener(Invalidation(keys=("busy",)))
            with _pumping(cache._serve, listener):
                assert _wait_for(lambda: listener.pings >= 2, timeout=5.0)
            assert cache._state.invalidations > 0

    def test_poll_timeout_is_what_the_listener_waits_on(self):
        with _offline_cache(poll_timeout=0.25, health_check_interval=60.0) as cache:
            listener = _StubListener()
            with _pumping(cache._serve, listener):
                assert _wait_for(lambda: len(listener.poll_timeouts) >= 2, timeout=5.0)
            assert set(listener.poll_timeouts) == {0.25}
            assert listener.pings == 0

    def test_reconnect_delay_paces_reconnect_attempts(self, mocker):
        with _offline_cache(reconnect_delay=0.3) as cache:
            assert cache._reconnect_delay == 0.3
            attempts: list[float] = []

            def failing_open() -> None:
                attempts.append(time.monotonic())
                msg = "transport still down"
                raise ConnectionError(msg)

            mocker.patch.object(cache, "_open_listener", side_effect=failing_open)
            with _pumping(cache._listener_loop, None):
                assert _wait_for(lambda: len(attempts) >= 3, timeout=10.0)
            gaps = [later - earlier for earlier, later in pairwise(attempts)]
            assert min(gaps) >= 0.25

    def test_an_abandoned_listener_does_not_disconnect_its_replacement(self):
        state = _TrackingState(max_entries=10, poll_timeout=0.1, coherence="tracking")
        abandoned, replacement = _StubListener(), _StubListener()
        state.on_connect(abandoned, reconnect=False)
        state.on_connect(replacement, reconnect=True)
        state.store["k"] = (b"v", None)

        state.on_disconnect(abandoned)
        assert state.connected is True
        assert state.listener is replacement
        assert state.store == {"k": (b"v", None)}

        state.on_disconnect(replacement)
        assert state.connected is False
        assert state.listener is None
        assert state.store == {}


class TestTrackingLRU:
    def test_max_entries_evicts_the_least_recently_read(self, redis_container: RedisContainerInfo, resp_adapter: str):
        _skip_unless_trackable(resp_adapter)
        config = _build_tracking_config(redis_container, resp_adapter, options={"MAX_ENTRIES": 3})
        with _connected(config) as cache:
            _settled(cache, lambda: cache.set_many({"k1": 1, "k2": 2, "k3": 3, "k4": 4}), count=4)
            for key in ("k1", "k2", "k3"):
                cache.get(key)
            cache.get("k1")  # k1 is now the most recently read
            cache.get("k4")
            assert set(cache._state.store) == {cache.make_key(k) for k in ("k1", "k3", "k4")}


class TestTrackingAsync:
    @pytest.mark.asyncio
    async def test_the_first_listener_connect_runs_off_the_event_loop(self, tracking_config: dict, mocker):
        # Two TCP connects, two CLIENT IDs, SUBSCRIBE and CLIENT TRACKING, each
        # bounded by 5s: blocking socket work no ASGI worker may sit through.
        threads: list[int] = []
        real_ensure = TrackingCache._ensure_listener

        def recording_ensure(self) -> None:
            threads.append(threading.get_ident())
            real_ensure(self)

        mocker.patch.object(TrackingCache, "_ensure_listener", recording_ensure)
        with _tracking(tracking_config) as cache:
            caches["transport"].flush_db()
            assert await cache.aget("off-loop") is None
            assert len(threads) == 1
            assert threads[0] != threading.get_ident()
            # The listener is up now, so the fast path never leaves the loop.
            assert await cache.aget("off-loop") is None
            assert len(threads) == 1

    @BOTH_MODES
    @pytest.mark.asyncio
    async def test_aget_is_served_locally_after_the_first_fetch(self, tracking_cache, mocker):
        await _asettled(tracking_cache, lambda: tracking_cache.aset("a", {"v": 1}))
        spy = mocker.spy(tracking_cache._transport.adapter, "apipeline")
        assert await tracking_cache.aget("a") == {"v": 1}
        assert await tracking_cache.aget("a") == {"v": 1}
        assert spy.call_count == 1
        assert tracking_cache.get("a") == {"v": 1}

    @BOTH_MODES
    @pytest.mark.asyncio
    async def test_async_writes_evict(self, tracking_cache):
        await tracking_cache.aset("b", 1)
        assert await tracking_cache.aget("b") == 1
        await tracking_cache.aset("b", 2)
        assert await tracking_cache.aget("b") == 2
        assert await tracking_cache.aadd("b", 3) is False
        assert await tracking_cache.aincr("b") == 3
        assert await tracking_cache.aget("b") == 3
        assert await tracking_cache.adelete("b") is True
        assert await tracking_cache.aget("b") is None

    @BOTH_MODES
    @pytest.mark.asyncio
    async def test_aget_many_aset_many_adelete_many_aclear(self, tracking_cache):
        await tracking_cache.aset_many({"c1": 1, "c2": 2})
        assert await tracking_cache.aget_many(["c1", "c2", "c3"]) == {"c1": 1, "c2": 2}
        assert await tracking_cache.aget_many(["c1", "c2"]) == {"c1": 1, "c2": 2}
        assert await tracking_cache.ahas_key("c1")
        await tracking_cache.adelete_many(["c1"])
        assert await tracking_cache.aget_many(["c1", "c2"]) == {"c2": 2}
        await tracking_cache.aclear()
        assert _tracking_section(tracking_cache)["entries"] == 0
        assert await tracking_cache.aget("c2") is None

    @pytest.mark.asyncio
    async def test_aget_or_set_refreshes_a_logically_expired_key(
        self,
        redis_container: RedisContainerInfo,
        resp_adapter: str,
    ):
        _skip_unless_trackable(resp_adapter)
        with _connected(_stampede_transport_config(redis_container, resp_adapter)) as cache:
            await _asettled(cache, lambda: cache.aset("gos", "stale", timeout=30))
            _settled(cache, lambda: caches["transport"].expire("gos", 50, stampede_prevention=False))
            assert await cache.aget_or_set("gos", "fresh", timeout=30) == "fresh"
            assert caches["transport"].get("gos", stampede_prevention=False) == "fresh"


@BOTH_MODES
class TestTrackingSurface:
    def test_data_structure_ops_raise_not_supported(self, tracking_cache):
        with pytest.raises(NotSupportedError, match="TrackingCache"):
            tracking_cache.hset("h", "f", 1)
        with pytest.raises(NotSupportedError, match="TrackingCache"):
            tracking_cache.lpush("l", 1)

    def test_admin_metadata_delegates_to_the_transport(self, tracking_cache):
        tracking_cache.set("meta", 1, timeout=100)
        assert tracking_cache.make_key("meta") == caches["transport"].make_key("meta")
        assert tracking_cache.reverse_key(tracking_cache.make_key("meta")) == "meta"
        assert tracking_cache.keys("meta*") == ["meta"]
        assert 95 <= tracking_cache.ttl("meta") <= 100
        assert tracking_cache.type("meta") == "string"
        _cursor, keys = tracking_cache.scan(pattern="meta*")
        assert "meta" in keys
        assert list(tracking_cache.iter_keys("meta*")) == ["meta"]

    def test_slowlog_delegates_to_the_transport(self, tracking_cache, mocker):
        get_spy = mocker.spy(tracking_cache._transport, "slowlog_get")
        len_spy = mocker.spy(tracking_cache._transport, "slowlog_len")
        assert tracking_cache.slowlog_get(1) == get_spy.spy_return
        get_spy.assert_called_once_with(1)
        assert tracking_cache.slowlog_len() == len_spy.spy_return
        len_spy.assert_called_once_with()

    def test_transport_close_is_not_forwarded_by_close(self, tracking_cache, mocker):
        spy = mocker.spy(tracking_cache._transport, "close")
        tracking_cache.close()
        assert spy.call_count == 0


def _sentinel_transport(sentinel_container, resp_adapter: str) -> dict:
    entry = build_sentinel_cache_config(sentinel_container.host, sentinel_container.port, resp_adapter=resp_adapter)
    entry["default"]["KEY_PREFIX"] = TRANSPORT_PREFIX
    return entry["default"]


class TestTrackingSentinel:
    def test_invalidation_through_a_sentinel_transport(self, sentinel_container, resp_adapter: str):
        _skip_unless_trackable(resp_adapter)
        config = {
            "transport": _sentinel_transport(sentinel_container, resp_adapter),
            "default": {
                "BACKEND": "django_cachex.cache.TrackingCache",
                "LOCATION": f"tracking:sentinel:{uuid.uuid4().hex[:6]}",
                "OPTIONS": {"transport": "transport", "poll_timeout": 0.1},
            },
        }
        with _connected(config) as cache:
            cache.set("s", "old")
            assert cache.get("s") == "old"
            caches["transport"].set("s", "new")
            assert _wait_for(lambda: cache.get("s") == "new")

    def test_health_check_notices_a_new_primary(self, sentinel_container, resp_adapter: str, mocker):
        _skip_unless_trackable(resp_adapter)
        with (
            override_settings(CACHES={"transport": _sentinel_transport(sentinel_container, resp_adapter)}),
            closing(caches["transport"]) as transport,
            _listener(transport) as listener,
        ):
            listener.ping()
            pool = transport.adapter._get_connection_pool(write=True)
            mocker.patch.object(pool, "get_master_address", return_value=("elsewhere", 1))
            with pytest.raises(ConnectionError, match="primary"):
                listener.ping()
