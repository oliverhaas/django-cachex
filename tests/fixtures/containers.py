"""Container fixtures for Redis and Sentinel using testcontainers."""

import time
from collections.abc import Callable, Generator
from contextlib import suppress
from os import environ
from typing import NamedTuple

import docker
import pytest
import redis
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

# Available Redis-compatible images with their corresponding client library
# Format: (image, client_library) where client_library is "redis" or "valkey"
REDIS_IMAGES = [
    ("redis:latest", "redis"),
    ("redis/redis-stack-server:latest", "redis"),
    ("valkey/valkey:latest", "valkey"),
]
DEFAULT_REDIS_IMAGE = "redis:latest"
DEFAULT_CLIENT_LIBRARY = "redis"

# Redis Cluster image (runs 6 nodes in single container: 3 masters + 3 replicas)
# See: https://github.com/Grokzen/docker-redis-cluster
REDIS_CLUSTER_IMAGE = "grokzen/redis-cluster:7.0.10"
CLUSTER_NODE_COUNT = 6  # 3 masters + 3 replicas
CLUSTER_BASE_PORT = 17000  # High port to avoid conflicts with example containers
CLUSTER_PORT_SPACING = 10  # Gap between worker port ranges for xdist
CLUSTER_RETRY_OFFSET = 100  # Port offset between retry attempts


class ContainerInfo(NamedTuple):
    """Container connection info plus the container object for internal operations."""

    host: str
    port: int
    container: DockerContainer


def _get_container_internal_ip(container: DockerContainer) -> str:
    """Get the internal Docker network IP of a container."""
    client = docker.from_env()
    container_info = client.containers.get(container.get_wrapped_container().id)
    return container_info.attrs["NetworkSettings"]["IPAddress"]


def _start_redis_container(image: str) -> ContainerInfo:
    """Start a Redis container with the given image."""
    container = DockerContainer(image)
    container.with_exposed_ports(6379)
    container.with_command("redis-server --enable-debug-command yes --protected-mode no")
    container.start()
    wait_for_logs(container, "Ready to accept connections")
    return ContainerInfo(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(6379)),
        container=container,
    )


def _get_xdist_worker_id() -> int:
    """Get the xdist worker ID from environment, or 0 if not running under xdist."""
    worker = environ.get("PYTEST_XDIST_WORKER", "")
    if worker.startswith("gw"):
        return int(worker[2:])
    return 0


CLUSTER_START_RETRIES = 3  # Number of retry attempts with different port ranges
CLUSTER_READY_TIMEOUT = 30.0  # Seconds to wait for cluster to accept commands
CLUSTER_READY_INTERVAL = 0.5  # Seconds between readiness checks


def _wait_for_cluster_ready(host: str, port: int, *, timeout: float = CLUSTER_READY_TIMEOUT) -> None:
    """Wait for Redis Cluster to be fully ready by verifying cluster_state via CLUSTER INFO.

    The container log message "Cluster state changed: ok" can appear before all
    nodes are ready to serve commands. This performs an actual connection check
    to avoid ClusterDownError on the first operation.
    """
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            client = redis.Redis(host=host, port=port, socket_connect_timeout=2)
            info = client.execute_command("CLUSTER", "INFO")
            client.close()
            # Response is bytes like b"cluster_state:ok\r\n..."
            if isinstance(info, bytes) and b"cluster_state:ok" in info:
                return
        except Exception as e:  # noqa: BLE001
            last_error = e
        time.sleep(CLUSTER_READY_INTERVAL)

    msg = f"Cluster not ready after {timeout}s"
    if last_error:
        msg += f": {last_error}"
    raise RuntimeError(msg)


def _start_cluster_container(base_port: int) -> ContainerInfo:
    """Start a Redis Cluster container (grokzen/redis-cluster).

    This image runs 6 Redis nodes (3 masters + 3 replicas) in a single container.
    Ports must be bound to fixed ports because Redis Cluster uses gossip protocol
    where nodes announce their IPs and ports to clients.

    Retries with a different port range if ports are already allocated (e.g.,
    from leftover containers of a previous test run).

    Args:
        base_port: Starting port for cluster nodes (e.g., 7000 -> ports 7000-7005)

    """
    last_error: Exception | None = None
    for attempt in range(CLUSTER_START_RETRIES):
        current_base = base_port + (attempt * CLUSTER_RETRY_OFFSET)
        container = DockerContainer(REDIS_CLUSTER_IMAGE)
        # Bind to 0.0.0.0 so cluster nodes are accessible from host
        container.with_env("IP", "0.0.0.0")  # noqa: S104
        # Set the starting port for cluster nodes
        container.with_env("INITIAL_PORT", str(current_base))
        # Use fixed port bindings (required for cluster gossip to work)
        for i in range(CLUSTER_NODE_COUNT):
            port = current_base + i
            container.with_bind_ports(port, port)
        try:
            container.start()
        except Exception as e:  # noqa: BLE001
            last_error = e
            with suppress(Exception):
                container.stop()
            continue
        # Wait for cluster to be ready (log message)
        wait_for_logs(container, "Cluster state changed: ok")
        host = container.get_container_host_ip()
        # Verify cluster actually accepts commands (avoids ClusterDownError race)
        _wait_for_cluster_ready(host, current_base)
        return ContainerInfo(
            host=host,
            port=current_base,  # First master node
            container=container,
        )
    msg = f"Failed to start cluster container after {CLUSTER_START_RETRIES} attempts"
    raise RuntimeError(msg) from last_error


def _start_sentinel_container(image: str, redis_internal_ip: str) -> ContainerInfo:
    """Start a Redis Sentinel container with the given image."""
    sentinel_conf = f"""
sentinel monitor mymaster {redis_internal_ip} 6379 1
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel parallel-syncs mymaster 1
"""
    container = DockerContainer(image)
    container.with_exposed_ports(26379)
    container.with_command(
        f"sh -c 'echo \"{sentinel_conf}\" > /tmp/sentinel.conf && redis-sentinel /tmp/sentinel.conf --port 26379'",
    )
    container.start()
    wait_for_logs(container, r"\+monitor master")
    return ContainerInfo(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(26379)),
        container=container,
    )


# Type alias for container factory functions
ContainerFactory = Callable[[str], tuple[str, int]]


@pytest.fixture(params=REDIS_IMAGES, ids=lambda x: f"{x[0].split('/')[0]}-{x[1]}")
def redis_images(request) -> tuple[str, str]:
    """Parametrized Redis image fixture. Request this to test all Redis-compatible images.

    Returns:
        Tuple of (image_name, client_library) where client_library is "redis" or "valkey".

    """
    return request.param


@pytest.fixture(scope="session")
def redis_container_factory() -> Generator[tuple[ContainerFactory, dict[str, ContainerInfo]]]:
    """Session-scoped factory that creates and caches Redis containers by image.

    Returns both the factory function and the cache dict (for sentinel to access containers).
    """
    cache: dict[str, ContainerInfo] = {}

    def get_container(image: str) -> tuple[str, int]:
        if image not in cache:
            cache[image] = _start_redis_container(image)
        info = cache[image]
        return info.host, info.port

    yield get_container, cache

    # Cleanup all containers at session end
    for info in cache.values():
        with suppress(Exception):
            info.container.stop()


@pytest.fixture(scope="session")
def sentinel_container_factory(
    redis_container_factory: tuple[ContainerFactory, dict[str, ContainerInfo]],
) -> Generator[ContainerFactory]:
    """Session-scoped factory that creates and caches Sentinel containers by image."""
    redis_factory, redis_cache = redis_container_factory
    sentinel_cache: dict[str, ContainerInfo] = {}

    def get_container(image: str) -> tuple[str, int]:
        # Ensure redis container exists first
        redis_factory(image)

        if image not in sentinel_cache:
            redis_internal_ip = _get_container_internal_ip(redis_cache[image].container)
            sentinel_cache[image] = _start_sentinel_container(image, redis_internal_ip)

        info = sentinel_cache[image]
        return info.host, info.port

    yield get_container

    # Cleanup sentinel containers at session end
    for info in sentinel_cache.values():
        with suppress(Exception):
            info.container.stop()


class RedisContainerInfo(NamedTuple):
    """Redis container connection info with client library."""

    host: str
    port: int
    client_library: str  # "redis" or "valkey"


@pytest.fixture
def redis_container(
    redis_container_factory: tuple[ContainerFactory, dict[str, ContainerInfo]],
    request: pytest.FixtureRequest,
) -> RedisContainerInfo:
    """Get a Redis container, using redis_images if opted in.

    Returns:
        RedisContainerInfo with host, port, and client_library.

    """
    factory, _ = redis_container_factory
    if "redis_images" in request.fixturenames:
        image, client_library = request.getfixturevalue("redis_images")
    else:
        image = DEFAULT_REDIS_IMAGE
        client_library = DEFAULT_CLIENT_LIBRARY

    host, port = factory(image)
    environ["REDIS_HOST"] = host
    environ["REDIS_PORT"] = str(port)
    environ["CLIENT_LIBRARY"] = client_library
    return RedisContainerInfo(host, port, client_library)


class SentinelContainerInfo(NamedTuple):
    """Sentinel container connection info with client library."""

    host: str
    port: int
    client_library: str  # "redis" or "valkey"


@pytest.fixture
def sentinel_container(
    sentinel_container_factory: ContainerFactory,
    request: pytest.FixtureRequest,
) -> SentinelContainerInfo:
    """Get a Sentinel container, using redis_images if opted in.

    Returns:
        SentinelContainerInfo with host, port, and client_library.

    """
    if "redis_images" in request.fixturenames:
        image, client_library = request.getfixturevalue("redis_images")
    else:
        image = DEFAULT_REDIS_IMAGE
        client_library = DEFAULT_CLIENT_LIBRARY

    host, port = sentinel_container_factory(image)
    environ["SENTINEL_HOST"] = host
    environ["SENTINEL_PORT"] = str(port)
    environ["CLIENT_LIBRARY"] = client_library
    return SentinelContainerInfo(host, port, client_library)


@pytest.fixture(scope="session")
def cluster_container_factory() -> Generator[tuple[ContainerFactory, ContainerInfo | None]]:
    """Session-scoped factory for Redis Cluster container.

    Returns a factory function and the cached container info.
    Uses dynamic port allocation based on xdist worker ID to allow parallel runs.
    """
    cached_info: list[ContainerInfo | None] = [None]

    def get_container(_image: str = "") -> tuple[str, int]:
        # Image parameter ignored - cluster always uses grokzen/redis-cluster
        if cached_info[0] is None:
            # Calculate base port from xdist worker ID (gw0 -> 7000, gw1 -> 7010, etc.)
            worker_id = _get_xdist_worker_id()
            base_port = CLUSTER_BASE_PORT + (worker_id * CLUSTER_PORT_SPACING)
            cached_info[0] = _start_cluster_container(base_port)
        info = cached_info[0]
        assert info is not None
        return info.host, info.port

    yield get_container, cached_info[0]

    # Cleanup at session end
    if cached_info[0] is not None:
        with suppress(Exception):
            cached_info[0].container.stop()


@pytest.fixture
def cluster_container(
    cluster_container_factory: tuple[ContainerFactory, ContainerInfo | None],
) -> tuple[str, int]:
    """Get a Redis Cluster container."""
    factory, _ = cluster_container_factory
    host, port = factory("")  # Empty string - cluster ignores image param
    environ["CLUSTER_HOST"] = host
    environ["CLUSTER_PORT"] = str(port)
    return host, port


# =============================================================================
# Redis Master-Replica Setup (using bitnami/redis)
# =============================================================================

BITNAMI_REDIS_IMAGE = "bitnami/redis:latest"
REPLICA_COUNT = 2  # Number of replicas to create


class ReplicaSetContainerInfo(NamedTuple):
    """Master-replica set connection info."""

    master_host: str
    master_port: int
    replica_hosts: list[str]
    replica_ports: list[int]
    containers: list[DockerContainer]  # All containers for cleanup


def _start_bitnami_master() -> ContainerInfo:
    """Start a bitnami/redis master container."""
    container = DockerContainer(BITNAMI_REDIS_IMAGE)
    container.with_exposed_ports(6379)
    container.with_env("REDIS_REPLICATION_MODE", "master")
    container.with_env("ALLOW_EMPTY_PASSWORD", "yes")
    container.start()
    wait_for_logs(container, "Ready to accept connections", timeout=60)
    return ContainerInfo(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(6379)),
        container=container,
    )


def _start_bitnami_replica(master_internal_ip: str) -> ContainerInfo:
    """Start a bitnami/redis replica container."""
    container = DockerContainer(BITNAMI_REDIS_IMAGE)
    container.with_exposed_ports(6379)
    container.with_env("REDIS_REPLICATION_MODE", "slave")
    container.with_env("REDIS_MASTER_HOST", master_internal_ip)
    container.with_env("REDIS_MASTER_PORT_NUMBER", "6379")
    container.with_env("ALLOW_EMPTY_PASSWORD", "yes")
    container.start()
    wait_for_logs(container, "Ready to accept connections", timeout=60)
    return ContainerInfo(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(6379)),
        container=container,
    )


@pytest.fixture(scope="session")
def replica_container_factory() -> Generator[
    tuple[Callable[[], ReplicaSetContainerInfo], ReplicaSetContainerInfo | None]
]:
    """Session-scoped factory for master-replica Redis setup.

    Creates one master and multiple replicas using bitnami/redis image.
    """
    cached_info: list[ReplicaSetContainerInfo | None] = [None]

    def get_containers() -> ReplicaSetContainerInfo:
        if cached_info[0] is not None:
            return cached_info[0]

        containers: list[DockerContainer] = []

        # Start master
        master_info = _start_bitnami_master()
        containers.append(master_info.container)
        master_internal_ip = _get_container_internal_ip(master_info.container)

        # Start replicas
        replica_hosts: list[str] = []
        replica_ports: list[int] = []
        for _ in range(REPLICA_COUNT):
            replica_info = _start_bitnami_replica(master_internal_ip)
            containers.append(replica_info.container)
            replica_hosts.append(replica_info.host)
            replica_ports.append(replica_info.port)

        cached_info[0] = ReplicaSetContainerInfo(
            master_host=master_info.host,
            master_port=master_info.port,
            replica_hosts=replica_hosts,
            replica_ports=replica_ports,
            containers=containers,
        )
        return cached_info[0]

    yield get_containers, cached_info[0]

    # Cleanup all containers at session end
    if cached_info[0] is not None:
        for container in cached_info[0].containers:
            with suppress(Exception):
                container.stop()


@pytest.fixture
def replica_containers(
    replica_container_factory: tuple[Callable[[], ReplicaSetContainerInfo], ReplicaSetContainerInfo | None],
) -> ReplicaSetContainerInfo:
    """Get a master-replica Redis setup."""
    factory, _ = replica_container_factory
    return factory()
