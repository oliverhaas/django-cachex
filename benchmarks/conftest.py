"""Benchmark fixtures: session-scoped Redis + Valkey containers, results sink."""

from __future__ import annotations

from collections.abc import Iterator

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


def _start(image: str) -> tuple[str, str]:
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
def server_url(redis_url: str, valkey_url: str):
    """Returns a callable that picks the correct URL for a driver config."""

    def pick(server: str) -> str:
        return redis_url if server == "redis" else valkey_url

    return pick


class _Results:
    def __init__(self) -> None:
        self.items: list[BenchmarkResult] = []

    def add(self, r: BenchmarkResult) -> None:
        self.items.append(r)


@pytest.fixture(scope="session")
def results() -> Iterator[_Results]:
    sink = _Results()
    yield sink
    if sink.items:
        print()
        print("=" * 80)
        print("BENCHMARK SUMMARY")
        print("=" * 80)
        print(format_table(sink.items))
        print("=" * 80)


class _MicroResults:
    def __init__(self) -> None:
        self.items: list[MicroResult] = []

    def add(self, r: MicroResult) -> None:
        self.items.append(r)


@pytest.fixture(scope="session")
def micro_results() -> Iterator[_MicroResults]:
    sink = _MicroResults()
    yield sink
    if sink.items:
        print()
        print("=" * 80)
        print("COMPRESSOR MICRO SUMMARY")
        print("=" * 80)
        print(format_micro_table(sink.items))
        print("=" * 80)


class _AsgiResults:
    def __init__(self) -> None:
        self.items: list[AsgiResult] = []

    def add(self, r: AsgiResult) -> None:
        self.items.append(r)


@pytest.fixture(scope="session")
def asgi_results() -> Iterator[_AsgiResults]:
    sink = _AsgiResults()
    yield sink
    if sink.items:
        print()
        print("=" * 80)
        print("ASGI BENCHMARK SUMMARY")
        print("=" * 80)
        print(format_asgi_table(sink.items))
        print("=" * 80)
