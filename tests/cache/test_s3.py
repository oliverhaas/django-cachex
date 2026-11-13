"""Tests for ``django_cachex.cache.s3.S3Cache`` against a VersityGW S3 server."""

import asyncio
import gzip
import os
import pickle
import signal
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any

import obstore
import pytest
from django.core.cache import caches
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings
from obstore.store import S3Store
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from django_cachex.cache import s3 as s3_module
from django_cachex.cache.s3 import S3Cache
from django_cachex.exceptions import CompressorError, NotSupportedError
from tests.fixtures.containers import _start_container

if TYPE_CHECKING:
    from collections.abc import Iterator

VERSITYGW_IMAGE = "versity/versitygw:v1.11.0"
BUCKET = "cachex"
ACCESS_KEY = "cachex"
SECRET_KEY = "cachex-secret"


@pytest.fixture(scope="module")
def s3_endpoint() -> Iterator[str]:
    container = (
        DockerContainer(VERSITYGW_IMAGE)
        .with_env("ROOT_ACCESS_KEY", ACCESS_KEY)
        .with_env("ROOT_SECRET_KEY", SECRET_KEY)
        .with_env("VGW_BACKEND", "posix")
        .with_env("VGW_BACKEND_ARG", "/tmp")  # noqa: S108
        .with_env("VGW_KEEP_ALIVE", "true")  # as S3 does; off, every request opens a connection
        .with_exposed_ports(7070)
        .waiting_for(LogMessageWaitStrategy("service listening"))
    )
    info = _start_container(container, 7070)
    container.exec(f"mkdir -p /tmp/{BUCKET}")
    yield f"http://{info.host}:{info.port}"
    container.stop()


def _store_options(endpoint: str) -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "region": "us-east-1",
        "access_key_id": ACCESS_KEY,
        "secret_access_key": SECRET_KEY,
        "client_options": {"allow_http": True},
    }


def _caches(endpoint: str, prefix: str, **options: Any) -> dict[str, Any]:
    return {
        "default": {
            "BACKEND": "django_cachex.cache.S3Cache",
            "LOCATION": f"s3://{BUCKET}/{prefix}",
            "OPTIONS": {**_store_options(endpoint), **options},
        },
    }


@pytest.fixture(autouse=True)
def stores(monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
    """A store registry of the test's own, whose thread pools stop after it."""
    registry: dict[str, Any] = {}
    monkeypatch.setattr(s3_module, "_stores", registry)
    yield registry
    for _, executor in registry.values():
        executor.shutdown()


@pytest.fixture
def prefix() -> str:
    return f"test-{uuid.uuid4().hex}"


@pytest.fixture
def raw_store(s3_endpoint: str, prefix: str) -> S3Store:
    """The objects under the test cache's prefix, read without the cache."""
    return S3Store.from_url(f"s3://{BUCKET}/{prefix}", **_store_options(s3_endpoint))


@pytest.fixture
def s3_cache(s3_endpoint: str, prefix: str) -> Iterator[S3Cache]:
    with override_settings(CACHES=_caches(s3_endpoint, prefix)):
        yield caches["default"]


@pytest.fixture
def gzip_cache(s3_endpoint: str, prefix: str) -> Iterator[S3Cache]:
    compressor = "django_cachex.compressors.gzip.GzipCompressor"
    with override_settings(CACHES=_caches(s3_endpoint, prefix, compressor=compressor)):
        yield caches["default"]


@pytest.fixture
def plain_key_cache(s3_endpoint: str, prefix: str) -> Iterator[S3Cache]:
    """A cache whose keys reach S3 without the ``:1:`` version prefix."""
    caches_config = _caches(s3_endpoint, prefix)
    caches_config["default"]["KEY_FUNCTION"] = lambda key, key_prefix, version: key
    with override_settings(CACHES=caches_config):
        yield caches["default"]


def _paths(store: S3Store) -> list[str]:
    return sorted(meta["path"] for batch in obstore.list(store) for meta in batch)


def test_values_round_trip(s3_cache: S3Cache):
    values = {"str": "text", "int": 42, "none": None, "dict": {"a": [1, 2.5]}, "bytes": b"\x00\xff"}
    for key, value in values.items():
        s3_cache.set(key, value)
    assert {key: s3_cache.get(key) for key in values} == values
    assert s3_cache.get("missing", "fallback") == "fallback"


def test_bytes_are_stored_as_is_and_other_values_serialized(s3_cache: S3Cache, raw_store: S3Store):
    s3_cache.set("raw", b'{"id": 1}')
    s3_cache.set("obj", {"id": 1})

    raw = obstore.get(raw_store, ":1:raw")
    assert bytes(raw.bytes()) == b'{"id": 1}'
    assert raw.attributes["cachex-raw"] == "1"
    obj = obstore.get(raw_store, ":1:obj")
    assert pickle.loads(bytes(obj.bytes())) == {"id": 1}
    assert "cachex-raw" not in obj.attributes


def test_compressed_body_is_the_compressor_output(gzip_cache: S3Cache, raw_store: S3Store):
    payload = b"x" * 1000
    gzip_cache.set("big", payload)

    stored = obstore.get(raw_store, ":1:big")
    assert stored.attributes["cachex-compressed"] == "1"
    assert gzip.decompress(bytes(stored.bytes())) == payload
    assert gzip_cache.get("big") == payload


def test_compressed_value_without_its_compressor_raises(
    gzip_cache: S3Cache,
    s3_endpoint: str,
    prefix: str,
):
    gzip_cache.set("big", b"x" * 1000)
    plain = S3Cache(f"s3://{BUCKET}/{prefix}", {"OPTIONS": _store_options(s3_endpoint)})

    with pytest.raises(CompressorError):
        plain.get("big")


def test_small_raw_value_that_looks_compressed_reads_back_unchanged(gzip_cache: S3Cache, raw_store: S3Store):
    frame = gzip.compress(b"inner")
    gzip_cache.set("frame", frame)

    assert "cachex-compressed" not in obstore.get(raw_store, ":1:frame").attributes
    assert gzip_cache.get("frame") == frame


def test_expiry_is_stored_as_metadata(s3_cache: S3Cache, raw_store: S3Store):
    s3_cache.set("ttl", 1, timeout=100)
    s3_cache.set("forever", 1, timeout=None)

    expires = float(obstore.get(raw_store, ":1:ttl").attributes["cachex-expires"])
    assert expires == pytest.approx(time.time() + 100, abs=5)
    assert "cachex-expires" not in obstore.get(raw_store, ":1:forever").attributes


def test_expired_object_reads_as_a_miss(s3_cache: S3Cache):
    s3_cache.set("k", "v", timeout=0.2)
    time.sleep(0.3)

    assert s3_cache.get("k", "gone") == "gone"
    assert not s3_cache.has_key("k")
    assert s3_cache.get_many(["k"]) == {}


def test_set_with_zero_timeout_deletes(s3_cache: S3Cache, raw_store: S3Store):
    s3_cache.set("k", "v")
    s3_cache.set("k", "v", timeout=0)

    assert _paths(raw_store) == []


def test_delete_removes_the_object_and_accepts_a_missing_key(s3_cache: S3Cache, raw_store: S3Store):
    s3_cache.set("k", 1)

    assert s3_cache.delete("k")
    assert s3_cache.delete("missing")
    assert _paths(raw_store) == []


def test_many_key_operations(s3_cache: S3Cache, raw_store: S3Store):
    data = {f"item:{n}": {"n": n} for n in range(50)}

    assert s3_cache.set_many(data) == []
    assert s3_cache.get_many([*data, "missing"]) == data
    s3_cache.delete_many(["item:0", "item:1", "missing"])
    s3_cache.delete_many([])
    assert len(_paths(raw_store)) == 48


def test_set_many_with_zero_timeout_deletes(s3_cache: S3Cache, raw_store: S3Store):
    s3_cache.set_many({"a": 1, "b": 2})
    s3_cache.set_many({"a": 1, "b": 2}, timeout=0)

    assert _paths(raw_store) == []


def test_clear_deletes_only_its_prefix(s3_cache: S3Cache, s3_endpoint: str):
    other = S3Store.from_url(f"s3://{BUCKET}/other-{uuid.uuid4().hex}", **_store_options(s3_endpoint))
    obstore.put(other, "keep", b"1")
    s3_cache.set_many({f"k{n}": n for n in range(5)})

    assert s3_cache.clear()
    assert s3_cache.get_many([f"k{n}" for n in range(5)]) == {}
    assert _paths(other) == ["keep"]


def test_incr_version(s3_cache: S3Cache):
    s3_cache.set("k", "v")

    assert s3_cache.incr_version("k") == 2
    assert s3_cache.get("k", version=2) == "v"
    assert s3_cache.get("k") is None


@pytest.mark.parametrize("flag", ["nx", "xx", "get"])
def test_set_flags_are_not_supported(s3_cache: S3Cache, flag: str):
    with pytest.raises(NotSupportedError):
        s3_cache.set("k", 1, **{flag: True})


def test_keys_map_to_distinct_paths(plain_key_cache: S3Cache, raw_store: S3Store):
    keys = ["a", "a/", "/", "/a", "a//b", ".", "..", "%2E", "%2F", "%", "tab\tkey", "\x7f"]

    plain_key_cache.set_many({key: key for key in keys})

    assert plain_key_cache.get_many(keys) == {key: key for key in keys}
    assert len(_paths(raw_store)) == len(keys)


def test_empty_key_raises(plain_key_cache: S3Cache):
    with pytest.raises(ValueError, match="empty key"):
        plain_key_cache.set("", 1)


def test_key_longer_than_s3_allows_raises(s3_cache: S3Cache, prefix: str):
    too_long = "k" * (1024 - len(prefix) - 1 - len(":1:") + 1)

    with pytest.raises(ValueError, match="longer than"):
        s3_cache.set(too_long, 1)


@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
@pytest.mark.parametrize(("method", "argument"), [("get", "k"), ("get_many", ["a", "b"])])
def test_requests_in_a_child_forked_after_a_request_raise(s3_cache: S3Cache, method: str, argument: object):
    s3_cache.get_many(["a", "b"])

    pid = os.fork()
    if pid == 0:
        signal.alarm(10)
        try:
            getattr(s3_cache, method)(argument)
        except RuntimeError:
            os._exit(0)
        os._exit(1)
    _, status = os.waitpid(pid, 0)

    assert os.waitstatus_to_exitcode(status) == 0


@pytest.mark.asyncio
async def test_async_operations(s3_cache: S3Cache):
    data = {f"item:{n}": n for n in range(20)}

    await s3_cache.aset("k", b"raw")
    assert await s3_cache.aget("k") == b"raw"
    assert await s3_cache.ahas_key("k")
    assert await s3_cache.adelete("k")
    assert await s3_cache.adelete("k")
    assert await s3_cache.aget("k") is None
    assert await s3_cache.aset_many(data) == []
    assert await s3_cache.aget_many([*data, "missing"]) == data
    await s3_cache.adelete_many(["item:0", "missing"])
    assert await s3_cache.aget_many(["item:0", "item:1"]) == {"item:1": 1}
    assert await s3_cache.aclear()
    assert await s3_cache.aget_many(list(data)) == {}


@pytest.mark.asyncio
async def test_async_expiry(s3_cache: S3Cache, raw_store: S3Store):
    await s3_cache.aset("k", b"body", timeout=0.2)
    await s3_cache.aset("gone", 1)
    await s3_cache.aset_many({"a": 1, "b": 2})

    await asyncio.sleep(0.3)
    assert not await s3_cache.ahas_key("k")
    assert await s3_cache.aget("k") is None
    await s3_cache.aset("gone", 1, timeout=0)
    await s3_cache.aset_many({"a": 1, "b": 2}, timeout=0)
    with pytest.raises(NotSupportedError):
        await s3_cache.aset("k", 1, nx=True)
    assert _paths(raw_store) == [":1:k"]


@pytest.mark.asyncio
async def test_many_key_calls_keep_to_max_concurrency(monkeypatch: pytest.MonkeyPatch):
    cache = S3Cache("s3://bucket/p", {"OPTIONS": {"region": "us-east-1", "max_concurrency": 2}})
    in_flight: list[str] = []
    peaks: list[int] = []
    lock = threading.Lock()

    def track(path: str) -> None:
        with lock:
            in_flight.append(path)
            peaks.append(len(in_flight))

    def untrack(path: str) -> None:
        with lock:
            in_flight.remove(path)

    def get(store: S3Store, path: str, options: object = None) -> None:
        track(path)
        time.sleep(0.02)
        untrack(path)
        raise FileNotFoundError(path)

    async def get_async(store: S3Store, path: str, options: object = None) -> None:
        track(path)
        await asyncio.sleep(0.02)
        untrack(path)
        raise FileNotFoundError(path)

    monkeypatch.setattr(obstore, "get", get)
    monkeypatch.setattr(obstore, "get_async", get_async)
    keys = [f"k{n}" for n in range(8)]

    assert cache.get_many(keys) == {}
    assert max(peaks) == 2
    peaks.clear()
    assert await cache.aget_many(keys) == {}
    assert max(peaks) == 2


@pytest.mark.asyncio
async def test_failed_async_many_key_call_cancels_its_other_requests(monkeypatch: pytest.MonkeyPatch):
    cache = S3Cache("s3://bucket/p", {"OPTIONS": {"region": "us-east-1"}})
    finished: list[str] = []

    async def get_async(store: S3Store, path: str, options: object = None) -> None:
        if path.endswith("bad"):
            raise PermissionError(path)
        await asyncio.sleep(0.05)
        finished.append(path)

    monkeypatch.setattr(obstore, "get_async", get_async)

    with pytest.raises(PermissionError):
        await cache.aget_many(["bad", *(f"k{n}" for n in range(5))])
    await asyncio.sleep(0.1)
    assert finished == []


@pytest.mark.asyncio
async def test_methods_s3_cannot_back_are_not_supported(s3_cache: S3Cache):
    s3_cache.set("k", 1)

    for operation, args in [
        (s3_cache.incr, ("k",)),
        (s3_cache.decr, ("k",)),
        (s3_cache.add, ("k", 2)),
        (s3_cache.get_or_set, ("k", 2)),
        (s3_cache.touch, ("k",)),
    ]:
        with pytest.raises(NotSupportedError):
            operation(*args)
    for async_operation, args in [
        (s3_cache.aincr, ("k",)),
        (s3_cache.adecr, ("k",)),
        (s3_cache.aadd, ("k", 2)),
        (s3_cache.aget_or_set, ("k", 2)),
        (s3_cache.atouch, ("k",)),
    ]:
        with pytest.raises(NotSupportedError):
            await async_operation(*args)
    assert s3_cache.get("k") == 1


def test_requires_a_location():
    with pytest.raises(ImproperlyConfigured, match="requires a LOCATION"):
        S3Cache("", {})


@pytest.mark.parametrize("value", [0, -1, "8", True])
def test_rejects_a_bad_max_concurrency(value: object):
    with pytest.raises(ImproperlyConfigured, match="max_concurrency"):
        S3Cache("s3://bucket/prefix", {"OPTIONS": {"max_concurrency": value}})


@pytest.mark.parametrize("options", [{"bogus": 1}, {"allow_http": True}])
def test_rejects_an_option_obstore_does_not_take(options: dict[str, Any]):
    with pytest.raises(ImproperlyConfigured, match="could not create a store"):
        S3Cache("s3://bucket/unknown-option", {"OPTIONS": options})


def test_caches_with_one_configuration_share_a_store(mocker: Any):
    from_url = mocker.patch.object(s3_module, "S3Store").from_url
    from_url.return_value.prefix = "p"

    S3Cache("s3://bucket/p", {"OPTIONS": {"region": "us-east-1"}})
    S3Cache("s3://bucket/p", {"OPTIONS": {"region": "us-east-1"}})
    S3Cache("s3://bucket/p", {"OPTIONS": {"region": "eu-central-1"}})

    assert from_url.call_count == 2


def test_requests_get_short_timeouts_unless_configured(mocker: Any):
    from_url = mocker.patch.object(s3_module, "S3Store").from_url
    from_url.return_value.prefix = "p"

    S3Cache("s3://bucket/default", {"OPTIONS": {"client_options": {"allow_http": True}}})
    S3Cache("s3://bucket/own", {"OPTIONS": {"client_options": {"timeout": "30s"}, "retry_config": {"max_retries": 5}}})

    default, own = (call.kwargs for call in from_url.call_args_list)
    assert default["client_options"] == {"timeout": "5s", "connect_timeout": "1s", "allow_http": True}
    assert default["retry_config"]["max_retries"] == 2
    assert own["client_options"]["timeout"] == "30s"
    assert own["retry_config"]["max_retries"] == 5


@pytest.mark.parametrize(
    ("location", "options", "express"),
    [
        ("s3://cache--usw2-az1--x-s3/p", {}, True),
        ("s3://cache--usw2-az1--x-s3/p", {"s3_express": False}, False),
        ("s3://plain-bucket/p", {}, None),
    ],
)
def test_s3_express_follows_the_bucket_name(
    location: str,
    options: dict[str, Any],
    express: bool | None,
    mocker: Any,
):
    from_url = mocker.patch.object(s3_module, "S3Store").from_url
    from_url.return_value.prefix = "p"

    S3Cache(location, {"OPTIONS": {"region": "us-west-2", **options}})

    assert from_url.call_args.kwargs.get("s3_express") is express
