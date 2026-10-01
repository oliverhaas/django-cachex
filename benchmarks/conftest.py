"""Benchmark fixtures: session-scoped Redis, Valkey and PostgreSQL containers, results sinks."""

import os
from collections.abc import Callable, Iterable, Iterator

import docker
import pytest
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from benchmarks.orm_runner import (
    LatencyResult,
    MigrateResult,
    MixedResult,
    OrmEnv,
    RaceResult,
    ScorecardResult,
    format_latency,
    format_migrate,
    format_mixed,
    format_races,
    format_scorecard,
    format_sizes,
    format_skew,
    setup,
)
from benchmarks.runner import (
    AsgiResult,
    BenchmarkResult,
    MicroResult,
    format_asgi_table,
    format_micro_table,
    format_table,
)

NET_DELAY_US = int(os.environ.get("BENCH_NET_DELAY_US", "0"))


def _delay(container: DockerContainer) -> None:
    """Delay everything the container sends by ``BENCH_NET_DELAY_US``, which adds that much to each round trip."""
    if not NET_DELAY_US:
        return
    netem = f"tc qdisc add dev eth0 root netem delay {NET_DELAY_US}us limit 100000"
    docker.from_env().containers.run(
        "alpine:3",
        ["sh", "-c", f"apk add --no-cache -q iproute2-tc && {netem}"],
        network_mode=f"container:{container.get_wrapped_container().id}",
        cap_add=["NET_ADMIN"],
        remove=True,
    )


def _start(image: str) -> tuple[str, DockerContainer]:
    container = DockerContainer(image)
    container.with_exposed_ports(6379)
    container.with_command("redis-server --protected-mode no")
    container.start()
    wait_for_logs(container, "Ready to accept connections")
    _delay(container)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(6379)
    url = f"redis://{host}:{port}?db=0"
    return url, container


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    url, container = _start("redis:8")
    try:
        yield url
    finally:
        container.stop()


@pytest.fixture(scope="session")
def valkey_url() -> Iterator[str]:
    url, container = _start("valkey/valkey:9")
    try:
        yield url
    finally:
        container.stop()


@pytest.fixture(scope="session")
def server_url(redis_url: str, valkey_url: str) -> Callable[[str], str]:
    """Returns a callable that picks the correct URL for an adapter config."""

    def pick(server: str) -> str:
        return redis_url if server == "redis" else valkey_url

    return pick


@pytest.fixture(scope="session")
def orm_env(valkey_url: str) -> Iterator[OrmEnv]:
    """PostgreSQL, with fsync off, and Valkey for the ORM cache benchmark, with the database migrated and seeded."""
    container = PostgresContainer("postgres:18", username="bench", password="bench", dbname="bench", driver=None)
    container.with_command("postgres -c fsync=off")
    container.start()
    try:
        _delay(container)
        database = {
            "HOST": container.get_container_host_ip(),
            "PORT": str(container.get_exposed_port(5432)),
            "NAME": "bench",
            "USER": "bench",
            "PASSWORD": "bench",
        }
        env = OrmEnv(database, valkey_url)
        setup(env)
        yield env
    finally:
        container.stop()


class _Sink[T]:
    def __init__(self, title: str, formatter: Callable[[Iterable[T]], str]) -> None:
        self.items: list[T] = []
        self._title = title
        self._formatter = formatter

    def add(self, item: T) -> None:
        self.items.append(item)

    def render(self) -> None:
        if not self.items:
            return
        bar = "=" * 80
        print(f"\n{bar}\n{self._title}\n{bar}")
        print(self._formatter(self.items))
        print(bar)


def _sink_fixture[T](title: str, formatter: Callable[[Iterable[T]], str]) -> Iterator[_Sink[T]]:
    sink: _Sink[T] = _Sink(title, formatter)
    yield sink
    sink.render()


@pytest.fixture(scope="session")
def results() -> Iterator[_Sink[BenchmarkResult]]:
    yield from _sink_fixture("BENCHMARK SUMMARY", format_table)


@pytest.fixture(scope="session")
def micro_results() -> Iterator[_Sink[MicroResult]]:
    yield from _sink_fixture("COMPRESSOR MICRO SUMMARY", format_micro_table)


@pytest.fixture(scope="session")
def asgi_results() -> Iterator[_Sink[AsgiResult]]:
    yield from _sink_fixture("ASGI BENCHMARK SUMMARY", format_asgi_table)


@pytest.fixture(scope="session")
def orm_scorecard() -> Iterator[_Sink[ScorecardResult]]:
    yield from _sink_fixture("ORM CACHE CORRECTNESS", format_scorecard)


@pytest.fixture(scope="session")
def orm_races() -> Iterator[_Sink[RaceResult]]:
    yield from _sink_fixture("ORM CACHE RACES", format_races)


@pytest.fixture(scope="session")
def orm_skew() -> Iterator[_Sink[RaceResult]]:
    yield from _sink_fixture("ORM CACHE CLOCK SKEW", format_skew)


@pytest.fixture(scope="session")
def orm_latency() -> Iterator[_Sink[LatencyResult]]:
    yield from _sink_fixture("ORM CACHE LATENCY", format_latency)


@pytest.fixture(scope="session")
def orm_sizes() -> Iterator[_Sink[LatencyResult]]:
    yield from _sink_fixture("ORM CACHE HITS BY RESULT SIZE", format_sizes)


@pytest.fixture(scope="session")
def orm_mixed() -> Iterator[_Sink[MixedResult]]:
    yield from _sink_fixture("ORM CACHE MIXED WORKLOAD", format_mixed)


@pytest.fixture(scope="session")
def orm_migrate() -> Iterator[_Sink[MigrateResult]]:
    yield from _sink_fixture("ORM CACHE AFTER A NO-OP MIGRATE", format_migrate)
