"""Connection settings from LOCATION and OPTIONS, checked against real servers."""

import importlib
from typing import TYPE_CHECKING, Any

import pytest
import redis
from django.core.cache import caches
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from tests.fixtures.cache import (
    ADAPTER_IMAGES,
    BACKENDS,
    POOL_OPTION_ADAPTERS,
    RESP_PROTOCOL_OPTIONS,
    _adapter_library_available,
)
from tests.fixtures.containers import ACL_PASSWORD, ACL_USERNAME

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from django_cachex.cache import RespCache
    from tests.fixtures.containers import RedisContainerInfo, TlsCertificates

POOL_ADAPTERS = pytest.mark.parametrize("resp_adapter", sorted(POOL_OPTION_ADAPTERS), indirect=True)
GLIDE_ADAPTER = pytest.mark.parametrize("resp_adapter", ["valkey-glide"], indirect=True)


@pytest.fixture
def build_cache(resp_adapter: str, settings) -> Callable[..., RespCache]:
    """Build the ``default`` cache of ``resp_adapter`` from a LOCATION and OPTIONS."""
    if not _adapter_library_available(resp_adapter):
        pytest.skip(f"{resp_adapter} library not installed")

    def build(location: str, **options: Any) -> RespCache:
        if resp_adapter in POOL_OPTION_ADAPTERS:
            options = RESP_PROTOCOL_OPTIONS | options
        backend = BACKENDS[("default", resp_adapter)]
        settings.CACHES = {"default": {"BACKEND": backend, "LOCATION": location, "OPTIONS": options}}
        return caches["default"]

    return build


@pytest.fixture
def pause_server(redis_container: RedisContainerInfo) -> Iterator[Callable[[int], None]]:
    """Hold every client's commands on ``redis_container`` for the given milliseconds."""
    with redis.Redis(host=redis_container.host, port=redis_container.port) as server:
        yield lambda milliseconds: server.execute_command("CLIENT", "PAUSE", milliseconds, "ALL")
        server.execute_command("CLIENT", "UNPAUSE")


@pytest.mark.parametrize(
    ("location", "options"),
    [("redis://{host}:{port}/3", {}), ("redis://{host}:{port}?db=3", {}), ("redis://{host}:{port}", {"db": 3})],
    ids=["url-path", "url-query", "options"],
)
def test_db_setting_picks_the_database_that_receives_writes(build_cache, redis_container, location, options):
    cache = build_cache(location.format(host=redis_container.host, port=redis_container.port), **options)

    cache.set("selected-db", "value")

    with redis.Redis(host=redis_container.host, port=redis_container.port, db=3) as server:
        assert server.exists(cache.make_key("selected-db")) == 1
        server.delete(cache.make_key("selected-db"))


@pytest.mark.parametrize(
    ("userinfo", "options"),
    [
        (f"{ACL_USERNAME}:{ACL_PASSWORD}@", {}),
        ("", {"username": ACL_USERNAME, "password": ACL_PASSWORD}),
        (f"{ACL_USERNAME}:wrong@", {"password": ACL_PASSWORD}),
    ],
    ids=["url", "options", "options-over-url"],
)
def test_acl_user_with_the_right_password_gets_in(build_cache, acl_container, userinfo, options):
    cache = build_cache(f"redis://{userinfo}{acl_container.host}:{acl_container.port}/0", **options)

    cache.set("acl", "value")

    assert cache.get("acl") == "value"


def test_acl_user_with_a_wrong_password_is_refused(build_cache, acl_container):
    cache = build_cache(f"redis://{ACL_USERNAME}:wrong@{acl_container.host}:{acl_container.port}/0")

    with pytest.raises(Exception, match="invalid username-password pair"):
        cache.set("acl", "value")


@POOL_ADAPTERS
def test_tls_location_trusts_the_ca_in_ssl_ca_certs(build_cache, tls_container, tls_certificates: TlsCertificates):
    cache = build_cache(f"rediss://{tls_container.host}:{tls_container.port}/0", ssl_ca_certs=str(tls_certificates.ca))

    cache.set("tls", "value")

    assert cache.get("tls") == "value"


@POOL_ADAPTERS
def test_tls_location_refuses_a_certificate_from_another_ca(
    build_cache,
    tls_container,
    tls_certificates: TlsCertificates,
    resp_adapter,
):
    driver = importlib.import_module(ADAPTER_IMAGES[resp_adapter][1])
    cache = build_cache(
        f"rediss://{tls_container.host}:{tls_container.port}/0",
        ssl_ca_certs=str(tls_certificates.other_ca),
    )

    with pytest.raises(driver.exceptions.ConnectionError, match="certificate verify failed"):
        cache.set("tls", "value")


@GLIDE_ADAPTER
@pytest.mark.parametrize(
    ("scheme", "options"),
    [("valkeys", {}), ("redis", {"use_tls": True})],
    ids=["scheme", "use_tls"],
)
def test_glide_tls_setting_connects_to_a_tls_server(
    build_cache,
    tls_container,
    tls_certificates: TlsCertificates,
    monkeypatch,
    scheme,
    options,
):
    """valkey-glide has no option for a CA file, so the throwaway CA comes in through ``SSL_CERT_FILE``."""
    monkeypatch.setenv("SSL_CERT_FILE", str(tls_certificates.ca))
    cache = build_cache(f"{scheme}://{tls_container.host}:{tls_container.port}/0", **options)

    cache.set("tls", "value")

    assert cache.get("tls") == "value"


@POOL_ADAPTERS
def test_socket_timeout_cuts_a_longer_blocking_command_short(build_cache, redis_container, resp_adapter):
    driver = importlib.import_module(ADAPTER_IMAGES[resp_adapter][1])
    cache = build_cache(f"redis://{redis_container.host}:{redis_container.port}/0", socket_timeout=0.2)

    with pytest.raises(driver.exceptions.TimeoutError):
        cache.blpop("empty-queue", timeout=2)


@POOL_ADAPTERS
def test_retry_on_timeout_resends_a_command_the_server_answered_too_late(build_cache, redis_container, pause_server):
    cache = build_cache(
        f"redis://{redis_container.host}:{redis_container.port}/0",
        socket_timeout=0.5,
        retry_on_timeout=True,
    )
    cache.set("retried", "value")

    pause_server(750)

    assert cache.get("retried") == "value"


@POOL_ADAPTERS
def test_max_connections_caps_the_connections_a_cache_opens(build_cache, redis_container, resp_adapter):
    driver = importlib.import_module(ADAPTER_IMAGES[resp_adapter][1])
    cache = build_cache(f"redis://{redis_container.host}:{redis_container.port}/0", max_connections=1)

    with cache.get_client(write=True).pubsub() as pubsub:
        pubsub.subscribe("holds-the-only-connection")

        with pytest.raises(driver.exceptions.ConnectionError, match="Too many connections"):
            cache.get("capped")


@GLIDE_ADAPTER
def test_glide_request_timeout_outlasts_a_paused_server(build_cache, redis_container, pause_server):
    cache = build_cache(f"redis://{redis_container.host}:{redis_container.port}/0", request_timeout=2000)
    cache.set("patient", "value")

    pause_server(500)

    assert cache.get("patient") == "value"


def test_client_name_names_the_connection_on_the_server(build_cache, redis_container, resp_adapter):
    name = f"cachex-{resp_adapter}"
    cache = build_cache(f"redis://{redis_container.host}:{redis_container.port}/0", client_name=name)

    cache.set("named", "value")

    with redis.Redis(host=redis_container.host, port=redis_container.port) as server:
        assert name in {client["name"] for client in server.client_list()}


@pytest.mark.parametrize("location", ["", "  ", [], [""]], ids=["empty", "blank", "empty-list", "blank-list"])
def test_blank_location_is_rejected_at_construction(location):
    """A missing URL fails with ImproperlyConfigured on ``caches[...]``, not inside the driver on the first command."""
    caches_config = {"default": {"BACKEND": "django_cachex.cache.RedisCache", "LOCATION": location}}

    with override_settings(CACHES=caches_config), pytest.raises(ImproperlyConfigured, match="requires a LOCATION"):
        caches["default"]


def test_missing_location_is_rejected_at_construction():
    with (
        override_settings(CACHES={"default": {"BACKEND": "django_cachex.cache.RedisCache"}}),
        pytest.raises(ImproperlyConfigured, match="requires a LOCATION"),
    ):
        caches["default"]
