"""Tests for stream-synchronized local cache (StreamCache)."""

import uuid
from typing import TYPE_CHECKING

import pytest
from django.core.cache import caches
from django.core.cache.backends.locmem import _caches, _expire_info, _locks
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from django_cachex.cache.stream import StreamCache
from django_cachex.exceptions import NotSupportedError
from tests.fixtures.cache import BACKENDS, _get_client_library_options

if TYPE_CHECKING:
    from collections.abc import Iterator

    from django.core.cache.backends.base import BaseCache

    from tests.fixtures.containers import RedisContainerInfo


def _build_sync_config(
    host: str,
    port: int,
    client_library: str = "redis",
    driver: str = "py",
    stream_key: str | None = None,
    max_entries: int = 1000,
) -> dict:
    """Build CACHES config with redis transport + StreamCache."""
    options = _get_client_library_options(client_library)
    location = f"redis://{host}:{port}?db=13"
    backend_class = BACKENDS[("default", client_library, driver)]
    sk = stream_key or f"test:sync:{uuid.uuid4().hex[:8]}"

    return {
        "transport": {
            "BACKEND": backend_class,
            "LOCATION": location,
            "OPTIONS": options,
        },
        "default": {
            "BACKEND": "django_cachex.cache.StreamCache",
            "OPTIONS": {
                "TRANSPORT": "transport",
                "STREAM_KEY": sk,
                "MAX_ENTRIES": max_entries,
                "MAXLEN": 10000,
                "BLOCK_TIMEOUT": 100,
            },
        },
    }


def _cleanup_globals(stream_key: str) -> None:
    """Remove module-level globals for a stream key."""
    _caches.pop(stream_key, None)
    _expire_info.pop(stream_key, None)
    _locks.pop(stream_key, None)


@pytest.fixture
def stream_cache(redis_container: RedisContainerInfo, driver: str) -> Iterator[BaseCache]:
    """Single StreamCache instance for basic operations.

    Parametrized over the transport driver (``py`` and ``rust``) via the
    shared ``driver`` fixture.
    """
    config = _build_sync_config(
        redis_container.host,
        redis_container.port,
        client_library=redis_container.client_library,
        driver=driver,
    )
    stream_key = config["default"]["OPTIONS"]["STREAM_KEY"]

    with override_settings(CACHES=config):
        cache = caches["default"]
        cache.clear()
        yield cache
        cache.shutdown()
        _cleanup_globals(stream_key)


@pytest.fixture
def stream_pair(redis_container: RedisContainerInfo, driver: str) -> Iterator[tuple[StreamCache, StreamCache]]:
    """Two StreamCache instances sharing one stream (simulates two pods).

    Uses separate cache aliases with the same STREAM_KEY but different
    _STORAGE_KEY values so each pod has its own local dict (simulating
    separate processes sharing one Redis Stream). Parametrized over the
    transport driver.
    """
    options = _get_client_library_options(redis_container.client_library)
    location = f"redis://{redis_container.host}:{redis_container.port}?db=13"
    backend_class = BACKENDS[("default", redis_container.client_library, driver)]
    stream_key = f"test:sync-pair:{uuid.uuid4().hex[:8]}"
    storage_key_1 = f"{stream_key}:pod1"
    storage_key_2 = f"{stream_key}:pod2"

    config = {
        "transport": {
            "BACKEND": backend_class,
            "LOCATION": location,
            "OPTIONS": options,
        },
        "pod1": {
            "BACKEND": "django_cachex.cache.StreamCache",
            "OPTIONS": {
                "TRANSPORT": "transport",
                "STREAM_KEY": stream_key,
                "_STORAGE_KEY": storage_key_1,
                "MAX_ENTRIES": 1000,
                "MAXLEN": 10000,
                "BLOCK_TIMEOUT": 100,
            },
        },
        "pod2": {
            "BACKEND": "django_cachex.cache.StreamCache",
            "OPTIONS": {
                "TRANSPORT": "transport",
                "STREAM_KEY": stream_key,
                "_STORAGE_KEY": storage_key_2,
                "MAX_ENTRIES": 1000,
                "MAXLEN": 10000,
                "BLOCK_TIMEOUT": 100,
            },
        },
    }

    with override_settings(CACHES=config):
        pod1 = caches["pod1"]
        pod2 = caches["pod2"]
        pod1.clear()
        pod1._flush_publishes()
        pod2._drain()
        yield pod1, pod2
        pod1.shutdown()
        pod2.shutdown()
        _cleanup_globals(storage_key_1)
        _cleanup_globals(storage_key_2)


# =============================================================================
# Configuration tests
# =============================================================================


class TestSyncConfig:
    def test_missing_transport_raises(self):
        with pytest.raises(ImproperlyConfigured, match="TRANSPORT"):
            StreamCache("", {"OPTIONS": {}})

    def test_default_stream_key(self, redis_container: RedisContainerInfo, driver: str):
        config = _build_sync_config(
            redis_container.host,
            redis_container.port,
            client_library=redis_container.client_library,
            driver=driver,
        )
        # Remove STREAM_KEY to test default
        del config["default"]["OPTIONS"]["STREAM_KEY"]
        with override_settings(CACHES=config):
            cache = caches["default"]
            assert cache._stream_key == "cache:sync"
            cache.shutdown()
            _cleanup_globals("cache:sync")

    def test_cachex_support_level(self, stream_cache: BaseCache):
        assert stream_cache._cachex_support == "cachex"


# =============================================================================
# Basic operations
# =============================================================================


class TestSyncBasicOps:
    def test_get_set_roundtrip(self, stream_cache: BaseCache):
        stream_cache.set("key1", "value1")
        assert stream_cache.get("key1") == "value1"

    def test_get_missing_returns_default(self, stream_cache: BaseCache):
        assert stream_cache.get("missing") is None
        assert stream_cache.get("missing", "fallback") == "fallback"

    def test_set_many_get_many(self, stream_cache: BaseCache):
        data = {"k1": "v1", "k2": "v2", "k3": "v3"}
        stream_cache.set_many(data)
        result = stream_cache.get_many(["k1", "k2", "k3"])
        assert result == data

    def test_delete(self, stream_cache: BaseCache):
        stream_cache.set("del_key", "val")
        assert stream_cache.get("del_key") == "val"
        assert stream_cache.delete("del_key") is True
        assert stream_cache.get("del_key") is None

    def test_delete_returns_false_for_missing(self, stream_cache: BaseCache):
        assert stream_cache.delete("never_existed") is False

    def test_delete_many(self, stream_cache: BaseCache):
        stream_cache.set_many({"dm1": 1, "dm2": 2})
        stream_cache.delete_many(["dm1", "dm2"])
        assert stream_cache.get("dm1") is None
        assert stream_cache.get("dm2") is None

    def test_add_raises_not_supported(self, stream_cache: BaseCache):
        with pytest.raises(NotSupportedError):
            stream_cache.add("add_key", "first")

    def test_has_key(self, stream_cache: BaseCache):
        stream_cache.set("exists", 1)
        assert stream_cache.has_key("exists") is True
        assert stream_cache.has_key("nope") is False

    def test_incr_raises_not_supported(self, stream_cache: BaseCache):
        with pytest.raises(NotSupportedError):
            stream_cache.incr("counter")

    def test_decr_raises_not_supported(self, stream_cache: BaseCache):
        with pytest.raises(NotSupportedError):
            stream_cache.decr("dcounter")

    def test_touch(self, stream_cache: BaseCache):
        stream_cache.set("touch_key", "val", timeout=10)
        assert stream_cache.touch("touch_key", timeout=60) is True
        assert stream_cache.get("touch_key") == "val"

    def test_touch_missing_returns_false(self, stream_cache: BaseCache):
        assert stream_cache.touch("no_such_key") is False

    def test_get_or_set(self, stream_cache: BaseCache):
        stream_cache.delete("gos")
        result = stream_cache.get_or_set("gos", lambda: "computed")
        assert result == "computed"
        result = stream_cache.get_or_set("gos", lambda: "other")
        assert result == "computed"

    def test_clear(self, stream_cache: BaseCache):
        stream_cache.set("c1", 1)
        stream_cache.set("c2", 2)
        stream_cache.clear()
        assert stream_cache.get("c1") is None
        assert stream_cache.get("c2") is None

    def test_various_value_types(self, stream_cache: BaseCache):
        stream_cache.set("str", "hello")
        stream_cache.set("int", 42)
        stream_cache.set("float", 3.14)
        stream_cache.set("list", [1, 2, 3])
        stream_cache.set("dict", {"a": 1})
        stream_cache.set("none", None)
        stream_cache.set("bool", True)

        assert stream_cache.get("str") == "hello"
        assert stream_cache.get("int") == 42
        assert stream_cache.get("float") == 3.14
        assert stream_cache.get("list") == [1, 2, 3]
        assert stream_cache.get("dict") == {"a": 1}
        assert stream_cache.get("none") is None
        assert stream_cache.get("bool") is True

    def test_none_value_distinguishable(self, stream_cache: BaseCache):
        """Stored None is distinguishable from cache miss via default sentinel."""
        stream_cache.set("none_val", None)
        sentinel = object()
        assert stream_cache.get("none_val", sentinel) is None  # stored None, not sentinel

    def test_versioned_keys(self, stream_cache: BaseCache):
        stream_cache.set("vk", "v1", version=1)
        stream_cache.set("vk", "v2", version=2)
        assert stream_cache.get("vk", version=1) == "v1"
        assert stream_cache.get("vk", version=2) == "v2"


# =============================================================================
# Expiry tests
# =============================================================================


class TestSyncExpiry:
    def test_expired_key_returns_default(self, stream_cache: BaseCache):
        stream_cache.set("exp_key", "val", timeout=1)
        assert stream_cache.get("exp_key") == "val"
        stream_cache.expire("exp_key", 0)
        assert stream_cache.get("exp_key") is None

    def test_ttl_returns_remaining(self, stream_cache: BaseCache):
        stream_cache.set("ttl_key", "val", timeout=60)
        ttl = stream_cache.ttl("ttl_key")
        assert ttl is not None
        assert 50 <= ttl <= 60

    def test_ttl_returns_none_for_persistent(self, stream_cache: BaseCache):
        stream_cache.set("persist_key", "val", timeout=None)
        assert stream_cache.ttl("persist_key") is None

    def test_ttl_returns_minus_two_for_missing(self, stream_cache: BaseCache):
        assert stream_cache.ttl("no_key") == -2

    def test_has_key_false_for_expired(self, stream_cache: BaseCache):
        stream_cache.set("exp_hk", "val", timeout=1)
        stream_cache.expire("exp_hk", 0)
        assert stream_cache.has_key("exp_hk") is False

    def test_touch_updates_expiry(self, stream_cache: BaseCache):
        stream_cache.set("touch_exp", "val", timeout=5)
        stream_cache.touch("touch_exp", timeout=120)
        ttl = stream_cache.ttl("touch_exp")
        assert ttl is not None
        assert ttl > 60

    def test_persist_removes_expiry(self, stream_cache: BaseCache):
        stream_cache.set("persist_test", "val", timeout=60)
        stream_cache.persist("persist_test")
        assert stream_cache.ttl("persist_test") is None

    def test_expire_sets_new_ttl(self, stream_cache: BaseCache):
        stream_cache.set("expire_test", "val", timeout=None)
        stream_cache.expire("expire_test", 30)
        ttl = stream_cache.ttl("expire_test")
        assert ttl is not None
        assert 20 <= ttl <= 30


# =============================================================================
# Cross-instance sync tests (the key feature)
# =============================================================================


class TestSyncCrossInstance:
    def test_set_propagates(self, stream_pair: tuple[StreamCache, StreamCache]):
        pod1, pod2 = stream_pair
        pod1.set("cross_key", "hello")
        pod1._flush_publishes()
        pod2._drain()
        assert pod2.get("cross_key") == "hello"

    def test_delete_propagates(self, stream_pair: tuple[StreamCache, StreamCache]):
        pod1, pod2 = stream_pair
        pod1.set("del_cross", "val")
        pod1._flush_publishes()
        pod2._drain()
        assert pod2.get("del_cross") == "val"

        pod1.delete("del_cross")
        pod1._flush_publishes()
        pod2._drain()
        assert pod2.get("del_cross") is None

    def test_clear_propagates(self, stream_pair: tuple[StreamCache, StreamCache]):
        pod1, pod2 = stream_pair
        pod1.set("cl1", "a")
        pod1.set("cl2", "b")
        pod1._flush_publishes()
        pod2._drain()
        assert pod2.get("cl1") == "a"

        pod1.clear()
        pod1._flush_publishes()
        pod2._drain()
        assert pod2.get("cl1") is None
        assert pod2.get("cl2") is None

    def test_delete_many_propagates(self, stream_pair: tuple[StreamCache, StreamCache]):
        pod1, pod2 = stream_pair
        pod1.set_many({"dm1": 1, "dm2": 2, "dm3": 3})
        pod1._flush_publishes()
        pod2._drain()
        assert pod2.get("dm1") == 1

        pod1.delete_many(["dm1", "dm2"])
        pod1._flush_publishes()
        pod2._drain()
        assert pod2.get("dm1") is None
        assert pod2.get("dm2") is None
        assert pod2.get("dm3") == 3

    def test_touch_propagates(self, stream_pair: tuple[StreamCache, StreamCache]):
        pod1, pod2 = stream_pair
        pod1.set("touch_cross", "val", timeout=10)
        pod1._flush_publishes()
        pod2._drain()

        pod1.touch("touch_cross", timeout=120)
        pod1._flush_publishes()
        pod2._drain()
        ttl = pod2.ttl("touch_cross")
        assert ttl is not None
        assert ttl > 60

    def test_writer_sees_own_write_immediately(
        self,
        stream_pair: tuple[StreamCache, StreamCache],
    ):
        pod1, _pod2 = stream_pair
        pod1.set("imm", "instant")
        # No drain needed — writer has the value locally
        assert pod1.get("imm") == "instant"

    def test_various_types_propagate(self, stream_pair: tuple[StreamCache, StreamCache]):
        pod1, pod2 = stream_pair
        pod1.set("p_str", "hello")
        pod1.set("p_int", 42)
        pod1.set("p_list", [1, 2, 3])
        pod1.set("p_dict", {"a": 1})
        pod1._flush_publishes()
        pod2._drain()
        assert pod2.get("p_str") == "hello"
        assert pod2.get("p_int") == 42
        assert pod2.get("p_list") == [1, 2, 3]
        assert pod2.get("p_dict") == {"a": 1}

    def test_bidirectional_sync(self, stream_pair: tuple[StreamCache, StreamCache]):
        pod1, pod2 = stream_pair
        pod1.set("from1", "a")
        pod1._flush_publishes()
        pod2._drain()
        assert pod2.get("from1") == "a"

        pod2.set("from2", "b")
        pod2._flush_publishes()
        pod1._drain()
        assert pod1.get("from2") == "b"


# =============================================================================
# Cull tests
# =============================================================================


class TestSyncCull:
    def test_cull_evicts_when_full(self, redis_container: RedisContainerInfo, driver: str):
        config = _build_sync_config(
            redis_container.host,
            redis_container.port,
            client_library=redis_container.client_library,
            driver=driver,
            max_entries=10,
        )
        stream_key = config["default"]["OPTIONS"]["STREAM_KEY"]

        with override_settings(CACHES=config):
            cache = caches["default"]
            # Fill beyond capacity
            for i in range(15):
                cache.set(f"cull_{i}", f"v{i}")
            # Should have culled some entries
            count = sum(1 for i in range(15) if cache.get(f"cull_{i}") is not None)
            assert count <= 10
            # Most recent entries should still be present
            assert cache.get("cull_14") == "v14"
            cache.shutdown()
            _cleanup_globals(stream_key)


# =============================================================================
# Admin method tests
# =============================================================================


class TestSyncAdmin:
    def test_keys(self, stream_cache: BaseCache):
        stream_cache.set("admin_k1", "v1")
        stream_cache.set("admin_k2", "v2")
        result = stream_cache.keys("admin_*")
        assert "admin_k1" in result
        assert "admin_k2" in result

    def test_keys_wildcard(self, stream_cache: BaseCache):
        stream_cache.set("wc_a", 1)
        stream_cache.set("wc_b", 2)
        stream_cache.set("other", 3)
        result = stream_cache.keys("wc_*")
        assert len(result) == 2

    def test_info(self, stream_cache: BaseCache):
        stream_cache.set("info_key", "val")
        info = stream_cache.info()
        assert "server" in info
        assert "keyspace" in info
        assert info["keyspace"]["db0"]["keys"] >= 1

    def test_type_returns_string(self, stream_cache: BaseCache):
        stream_cache.set("type_key", "val")
        assert stream_cache.type("type_key") == "string"

    def test_type_missing_returns_none(self, stream_cache: BaseCache):
        assert stream_cache.type("no_such") == "none"

    def test_pttl(self, stream_cache: BaseCache):
        stream_cache.set("pttl_key", "val", timeout=60)
        pttl = stream_cache.pttl("pttl_key")
        assert pttl is not None
        assert pttl > 50000  # > 50 seconds in ms

    def test_scan(self, stream_cache: BaseCache):
        stream_cache.set("scan1", "a")
        stream_cache.set("scan2", "b")
        cursor, page = stream_cache.scan(cursor=0, count=100)
        assert "scan1" in page or any("scan" in k for k in page)
        assert cursor == 0  # All results in one page

    def test_iter_keys(self, stream_cache: BaseCache):
        stream_cache.set("ik1", "a")
        stream_cache.set("ik2", "b")
        result = stream_cache.iter_keys("ik*")
        assert "ik1" in result
        assert "ik2" in result

    def test_delete_pattern(self, stream_cache: BaseCache):
        stream_cache.set("dp_a", 1)
        stream_cache.set("dp_b", 2)
        stream_cache.set("keep", 3)
        count = stream_cache.delete_pattern("dp_*")
        assert count == 2
        assert stream_cache.get("dp_a") is None
        assert stream_cache.get("keep") == 3

    def test_make_key_reverse_key(self, stream_cache: BaseCache):
        mk = stream_cache.make_key("test_key")
        rk = stream_cache.reverse_key(mk)
        assert rk == "test_key"


class TestSyncReplay:
    def test_replay_warms_cache_on_startup(self, redis_container: RedisContainerInfo, driver: str):
        """A new StreamCache with REPLAY > 0 picks up entries from the stream."""
        stream_key = f"test:replay:{uuid.uuid4().hex[:8]}"
        storage_key_1 = f"{stream_key}:producer"
        storage_key_2 = f"{stream_key}:consumer"

        options = _get_client_library_options(redis_container.client_library)
        location = f"redis://{redis_container.host}:{redis_container.port}?db=13"
        backend_class = BACKENDS[("default", redis_container.client_library, driver)]

        config = {
            "transport": {
                "BACKEND": backend_class,
                "LOCATION": location,
                "OPTIONS": options,
            },
            "producer": {
                "BACKEND": "django_cachex.cache.StreamCache",
                "OPTIONS": {
                    "TRANSPORT": "transport",
                    "STREAM_KEY": stream_key,
                    "_STORAGE_KEY": storage_key_1,
                    "MAXLEN": 10000,
                    "BLOCK_TIMEOUT": 100,
                },
            },
            "consumer": {
                "BACKEND": "django_cachex.cache.StreamCache",
                "OPTIONS": {
                    "TRANSPORT": "transport",
                    "STREAM_KEY": stream_key,
                    "_STORAGE_KEY": storage_key_2,
                    "MAXLEN": 10000,
                    "BLOCK_TIMEOUT": 100,
                    "REPLAY": 100,
                },
            },
        }

        with override_settings(CACHES=config):
            # Producer writes data to the stream
            producer = caches["producer"]
            producer.set("replay_a", "alpha")
            producer.set("replay_b", "beta")
            producer.set("replay_c", "gamma")
            producer._flush_publishes()
            producer.shutdown()

            # Consumer starts fresh with REPLAY=100 — should warm from stream
            consumer = caches["consumer"]
            assert consumer.get("replay_a") == "alpha"
            assert consumer.get("replay_b") == "beta"
            assert consumer.get("replay_c") == "gamma"
            consumer.shutdown()

        _cleanup_globals(storage_key_1)
        _cleanup_globals(storage_key_2)

    def test_replay_zero_starts_empty(self, redis_container: RedisContainerInfo, driver: str):
        """With REPLAY=0 (default), a new pod starts with an empty cache."""
        stream_key = f"test:noreplay:{uuid.uuid4().hex[:8]}"
        storage_key_1 = f"{stream_key}:producer"
        storage_key_2 = f"{stream_key}:consumer"

        options = _get_client_library_options(redis_container.client_library)
        location = f"redis://{redis_container.host}:{redis_container.port}?db=13"
        backend_class = BACKENDS[("default", redis_container.client_library, driver)]

        config = {
            "transport": {
                "BACKEND": backend_class,
                "LOCATION": location,
                "OPTIONS": options,
            },
            "producer": {
                "BACKEND": "django_cachex.cache.StreamCache",
                "OPTIONS": {
                    "TRANSPORT": "transport",
                    "STREAM_KEY": stream_key,
                    "_STORAGE_KEY": storage_key_1,
                    "MAXLEN": 10000,
                    "BLOCK_TIMEOUT": 100,
                },
            },
            "consumer": {
                "BACKEND": "django_cachex.cache.StreamCache",
                "OPTIONS": {
                    "TRANSPORT": "transport",
                    "STREAM_KEY": stream_key,
                    "_STORAGE_KEY": storage_key_2,
                    "MAXLEN": 10000,
                    "BLOCK_TIMEOUT": 100,
                    # REPLAY defaults to 0
                },
            },
        }

        with override_settings(CACHES=config):
            producer = caches["producer"]
            producer.set("no_replay_key", "value")
            producer._flush_publishes()
            producer.shutdown()

            consumer = caches["consumer"]
            # No replay — consumer starts empty
            assert consumer.get("no_replay_key") is None
            consumer.shutdown()

        _cleanup_globals(storage_key_1)
        _cleanup_globals(storage_key_2)


class TestSyncShutdown:
    def test_shutdown_stops_consumer(self, redis_container: RedisContainerInfo, driver: str):
        config = _build_sync_config(
            redis_container.host,
            redis_container.port,
            client_library=redis_container.client_library,
            driver=driver,
        )
        stream_key = config["default"]["OPTIONS"]["STREAM_KEY"]

        with override_settings(CACHES=config):
            cache = caches["default"]
            cache.set("k", "v")  # triggers consumer start
            assert cache._consumer_alive()
            cache.shutdown()
            assert not cache._consumer_alive()
            _cleanup_globals(stream_key)
