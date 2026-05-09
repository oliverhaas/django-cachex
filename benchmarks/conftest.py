"""Benchmark fixtures: session-scoped Redis + Valkey containers, results sinks."""

from collections.abc import Callable, Iterable, Iterator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from benchmarks.runner import (
    AsgiResult,
    BenchmarkResult,
    MicroResult,
    format_asgi_table,
    format_micro_table,
    format_table,
)


def _start(image: str) -> tuple[str, DockerContainer]:
    container = DockerContainer(image)
    container.with_exposed_ports(6379)
    container.with_command("redis-server --enable-debug-command yes --protected-mode no")
    container.start()
    wait_for_logs(container, "Ready to accept connections")
    host = container.get_container_host_ip()
    port = container.get_exposed_port(6379)
    url = f"redis://{host}:{port}?db=0"
    return url, container


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    url, container = _start("redis:latest")
    try:
        yield url
    finally:
        container.stop()


@pytest.fixture(scope="session")
def valkey_url() -> Iterator[str]:
    url, container = _start("valkey/valkey:latest")
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
