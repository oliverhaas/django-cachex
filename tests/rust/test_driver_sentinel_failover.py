import time
from contextlib import suppress
from typing import TYPE_CHECKING, NamedTuple

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from django_cachex._driver import RustValkeyDriver
from tests.fixtures.containers import (
    BITNAMI_REDIS_IMAGE,
    _get_container_internal_ip,
    _start_bitnami_master,
    _start_bitnami_replica,
)

if TYPE_CHECKING:
    from collections.abc import Generator


class FailoverSetup(NamedTuple):
    sentinel_host: str
    sentinel_port: int
    master: DockerContainer
    replica: DockerContainer
    sentinel: DockerContainer


@pytest.fixture
def failover_setup() -> Generator[FailoverSetup]:
    """Spin up master + replica + sentinel pointed at master with quorum=1.

    Function-scoped because the test kills the master.
    """
    master = _start_bitnami_master()
    master_ip = _get_container_internal_ip(master.container)
    replica = _start_bitnami_replica(master_ip)

    sentinel_conf = (
        f"sentinel monitor mymaster {master_ip} 6379 1\n"
        "sentinel down-after-milliseconds mymaster 2000\n"
        "sentinel failover-timeout mymaster 10000\n"
        "sentinel parallel-syncs mymaster 1\n"
    )
    sentinel = DockerContainer(BITNAMI_REDIS_IMAGE)
    sentinel.with_exposed_ports(26379)
    sentinel.with_env("ALLOW_EMPTY_PASSWORD", "yes")
    sentinel.with_command(
        f"sh -c 'echo \"{sentinel_conf}\" > /tmp/sentinel.conf && redis-sentinel /tmp/sentinel.conf --port 26379'",
    )
    sentinel.start()
    wait_for_logs(sentinel, r"\+monitor master")

    yield FailoverSetup(
        sentinel_host=sentinel.get_container_host_ip(),
        sentinel_port=int(sentinel.get_exposed_port(26379)),
        master=master.container,
        replica=replica.container,
        sentinel=sentinel,
    )

    for c in (sentinel, replica.container, master.container):
        with suppress(Exception):
            c.stop()


def test_sentinel_rediscovers_master_after_failover(failover_setup):
    driver = RustValkeyDriver.connect_sentinel(
        [f"redis://{failover_setup.sentinel_host}:{failover_setup.sentinel_port}"],
        "mymaster",
        0,
    )
    driver.set_sync("k", b"before")
    assert driver.get_sync("k") == b"before"

    failover_setup.master.stop()

    # Sentinel needs to detect down-after-ms (2s) and complete failover.
    # The driver's first call after the master dies will hit the failover error
    # set, run rediscover(), pick up the promoted replica, and retry.
    deadline = time.monotonic() + 30
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            driver.set_sync("k", b"after")
        except (ConnectionError, RuntimeError) as e:
            last_err = e
            time.sleep(0.5)
            continue
        if driver.get_sync("k") == b"after":
            return
        time.sleep(0.5)
    pytest.fail(f"failover did not complete within 30s; last error: {last_err}")
