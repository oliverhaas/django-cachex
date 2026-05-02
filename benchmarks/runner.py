"""Benchmark workload + measurement helpers.

Single source of truth for what an "operation" means in this harness. The
goal is explicit and readable, not generic — adjust the workload phases here
when the questions you care about change.
"""

from __future__ import annotations

import asyncio
import gc
import time
import tracemalloc
import warnings
from dataclasses import dataclass, field
from statistics import mean, median
from typing import TYPE_CHECKING, Any

from django.test import override_settings

from benchmarks.configs import CompressorConfig, DriverConfig, SerializerConfig

if TYPE_CHECKING:
    from collections.abc import Callable


# Workload sizing — kept here so all phases share the knob.
N_OPS = 1000
K_RUNS = 10
WARMUP_KEYS = 100
MGET_BATCH = 10


@dataclass
class PhaseTiming:
    name: str
    seconds_per_run: list[float] = field(default_factory=list)

    @property
    def ops_per_sec(self) -> float:
        if not self.seconds_per_run:
            return 0.0
        return N_OPS / median(self.seconds_per_run)

    @property
    def median_seconds(self) -> float:
        return median(self.seconds_per_run) if self.seconds_per_run else 0.0

    @property
    def p95_seconds(self) -> float:
        if not self.seconds_per_run:
            return 0.0
        s = sorted(self.seconds_per_run)
        idx = int(round(0.95 * (len(s) - 1)))
        return s[idx]


@dataclass
class BenchmarkResult:
    driver_id: str
    serializer_id: str
    server: str
    compressor_id: str = ""
    phases: dict[str, PhaseTiming] = field(default_factory=dict)
    py_peak_kb_per_run: list[float] = field(default_factory=list)
    server_used_memory_delta_kb_per_run: list[float] = field(default_factory=list)
    server_connections_baseline: int = 0
    server_connections_samples: list[int] = field(default_factory=list)

    @property
    def label(self) -> str:
        compressor = f"+{self.compressor_id}" if self.compressor_id else ""
        return f"{self.driver_id}+{self.serializer_id}{compressor}@{self.server}"

    @property
    def server_connections_peak(self) -> int:
        return max([self.server_connections_baseline, *self.server_connections_samples], default=0)

    @property
    def server_connections_delta(self) -> int:
        """Peak connections opened by the workload above the baseline."""
        return max(0, self.server_connections_peak - self.server_connections_baseline)


@dataclass
class AsgiResult:
    """Result of a full-stack ASGI benchmark.

    Mirrors the metrics django-vcache's ``bench_compare.py`` reports so the
    two harnesses produce comparable numbers.
    """

    driver_id: str
    serializer_id: str
    server: str
    requests_per_sec: float
    avg_latency_ms: float
    p99_latency_ms: float
    total_requests: int
    errors: int
    initial_rss_mb: float
    peak_rss_mb: float
    final_rss_mb: float
    settled_rss_mb: float
    initial_conns: int
    peak_conns: int
    final_conns: int
    settled_conns: int
    duration_s: float
    concurrency: int

    @property
    def label(self) -> str:
        return f"{self.driver_id}+{self.serializer_id}@{self.server}"

    @property
    def rss_growth_mb(self) -> float:
        return self.final_rss_mb - self.initial_rss_mb


@dataclass
class MicroResult:
    """Pure compress/decompress numbers — no driver, no network, no Django."""

    compressor_id: str
    input_bytes: int
    output_bytes: int
    compress_mb_s: float
    decompress_mb_s: float

    @property
    def ratio(self) -> float:
        return self.output_bytes / self.input_bytes if self.input_bytes else 0.0


def build_caches(
    driver: DriverConfig,
    serializer: SerializerConfig,
    location: str,
    compressor: CompressorConfig | None = None,
) -> dict[str, dict[str, Any]]:
    options = dict(driver.options)
    if serializer.dotted_path is not None:
        options["serializer"] = serializer.dotted_path
    if compressor is not None and compressor.dotted_path is not None:
        options["compressor"] = compressor.dotted_path
    return {
        "default": {
            "BACKEND": driver.backend,
            "LOCATION": location,
            "OPTIONS": options,
        },
    }


def _phase(name: str, fn: Callable[[], None]) -> tuple[str, float]:
    start = time.perf_counter()
    fn()
    return name, time.perf_counter() - start


def _build_payload() -> dict[str, Any]:
    # ~150 bytes pickled — small enough that serializer cost dominates over network.
    return {
        "id": 12345,
        "name": "benchmark-item",
        "tags": ["alpha", "beta", "gamma"],
        "active": True,
        "score": 3.14159,
        "nested": {"a": 1, "b": 2, "c": [1, 2, 3]},
    }


def _build_payload_large() -> list[dict[str, Any]]:
    # ~14 KiB pickled — queryset-shaped, well above the 256 B compression
    # threshold. Used for compressor benchmarks where a small payload would
    # bypass compression entirely.
    return [
        {
            "id": i,
            "name": f"user-{i:05d}",
            "email": f"user{i}@example.com",
            "is_active": i % 7 != 0,
            "created_at": "2026-01-01T12:00:00",
            "tags": ["alpha", "beta", "gamma", "delta"][: (i % 4) + 1],
            "score": float(i * 0.137),
            "bio": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * (i % 3 + 1),
        }
        for i in range(80)
    ]


def _bench_get(cache, n: int) -> None:
    for i in range(n):
        cache.get(f"warm:{i % WARMUP_KEYS}")


def _bench_get_miss(cache, n: int) -> None:
    for i in range(n):
        cache.get(f"miss:{i}")


def _bench_set(cache, n: int, payload: Any) -> None:
    for i in range(n):
        cache.set(f"set:{i}", payload)


def _bench_mget(cache, n: int) -> None:
    keys = [f"warm:{i}" for i in range(MGET_BATCH)]
    for _ in range(n):
        cache.get_many(keys)


def _bench_mset(cache, n: int, payload: Any) -> None:
    batch = {f"mset:{i}": payload for i in range(MGET_BATCH)}
    for _ in range(n):
        cache.set_many(batch)


def _bench_incr(cache, n: int) -> None:
    cache.set("counter", 0)
    for _ in range(n):
        cache.incr("counter")


def _bench_delete(cache, n: int) -> None:
    # Pre-populate so deletes hit existing keys.
    for i in range(n):
        cache.set(f"del:{i}", 1)
    for i in range(n):
        cache.delete(f"del:{i}")


# Async phase helpers. ``concurrency=1`` mirrors the sync workload one op
# at a time. ``concurrency>1`` issues that many ops in flight via
# ``asyncio.gather``, which is where per-call client patterns can stress
# the connection pool.


async def _abench_get(cache, n: int, concurrency: int) -> None:
    if concurrency <= 1:
        for i in range(n):
            await cache.aget(f"warm:{i % WARMUP_KEYS}")
        return
    for chunk in range(0, n, concurrency):
        size = min(concurrency, n - chunk)
        await asyncio.gather(*(cache.aget(f"warm:{(chunk + j) % WARMUP_KEYS}") for j in range(size)))


async def _abench_get_miss(cache, n: int, concurrency: int) -> None:
    if concurrency <= 1:
        for i in range(n):
            await cache.aget(f"miss:{i}")
        return
    for chunk in range(0, n, concurrency):
        size = min(concurrency, n - chunk)
        await asyncio.gather(*(cache.aget(f"miss:{chunk + j}") for j in range(size)))


async def _abench_set(cache, n: int, concurrency: int, payload: Any) -> None:
    if concurrency <= 1:
        for i in range(n):
            await cache.aset(f"set:{i}", payload)
        return
    for chunk in range(0, n, concurrency):
        size = min(concurrency, n - chunk)
        await asyncio.gather(*(cache.aset(f"set:{chunk + j}", payload) for j in range(size)))


async def _abench_mget(cache, n: int, concurrency: int) -> None:
    keys = [f"warm:{j}" for j in range(MGET_BATCH)]
    if concurrency <= 1:
        for _ in range(n):
            await cache.aget_many(keys)
        return
    for chunk in range(0, n, concurrency):
        size = min(concurrency, n - chunk)
        await asyncio.gather(*(cache.aget_many(keys) for _ in range(size)))


async def _abench_mset(cache, n: int, concurrency: int, payload: Any) -> None:
    batch = {f"mset:{j}": payload for j in range(MGET_BATCH)}
    if concurrency <= 1:
        for _ in range(n):
            await cache.aset_many(batch)
        return
    for chunk in range(0, n, concurrency):
        size = min(concurrency, n - chunk)
        await asyncio.gather(*(cache.aset_many(batch) for _ in range(size)))


async def _abench_incr(cache, n: int, concurrency: int) -> None:
    # Concurrent INCRs on the same key are still well-defined (Redis serializes
    # them server-side); we just want pool pressure on the client side.
    await cache.aset("counter", 0)
    if concurrency <= 1:
        for _ in range(n):
            await cache.aincr("counter")
        return
    for chunk in range(0, n, concurrency):
        size = min(concurrency, n - chunk)
        await asyncio.gather(*(cache.aincr("counter") for _ in range(size)))


async def _abench_delete(cache, n: int, concurrency: int) -> None:
    for i in range(n):
        await cache.aset(f"del:{i}", 1)
    if concurrency <= 1:
        for i in range(n):
            await cache.adelete(f"del:{i}")
        return
    for chunk in range(0, n, concurrency):
        size = min(concurrency, n - chunk)
        await asyncio.gather(*(cache.adelete(f"del:{chunk + j}") for j in range(size)))


def _open_info_client(location: str):
    """Side-channel redis-py client for INFO sampling.

    Used regardless of the cache backend under test, so backends without an
    ``info()`` method (Django's built-in ``RedisCache``) still report memory
    and connection metrics. Works against Valkey too — same RESP protocol.
    """
    import redis

    return redis.Redis.from_url(location)


def _server_used_memory(info_client) -> int | None:
    import redis

    try:
        info = info_client.info("memory")
    except (redis.RedisError, OSError) as exc:
        warnings.warn(f"server INFO memory sample failed: {exc!r}", stacklevel=2)
        return None
    if not isinstance(info, dict):
        return None
    val = info.get("used_memory")
    return int(val) if val is not None else None


def _server_connections(info_client) -> int | None:
    import redis

    try:
        info = info_client.info("clients")
    except (redis.RedisError, OSError) as exc:
        warnings.warn(f"server INFO clients sample failed: {exc!r}", stacklevel=2)
        return None
    if not isinstance(info, dict):
        return None
    val = info.get("connected_clients")
    return int(val) if val is not None else None


def _flush_cache(cache) -> None:
    """Uniform flush across django-cachex backends and Django's RedisCache."""
    flush_db = getattr(cache, "flush_db", None)
    if callable(flush_db):
        flush_db()
    else:
        cache.clear()


def run_benchmark(  # noqa: PLR0915 — phase-by-phase flow is the readable shape here
    driver: DriverConfig,
    serializer: SerializerConfig,
    location: str,
    *,
    compressor: CompressorConfig | None = None,
    payload_kind: str = "small",
) -> BenchmarkResult:
    """Run the workload K_RUNS times and return aggregated metrics."""

    result = BenchmarkResult(
        driver_id=driver.id,
        serializer_id=serializer.id,
        server=driver.server,
        compressor_id=compressor.id if compressor is not None else "",
    )
    for name in ("get", "get-miss", "set", "mget", "mset", "incr", "delete"):
        result.phases[name] = PhaseTiming(name=name)

    caches = build_caches(driver, serializer, location, compressor=compressor)
    payload = _build_payload_large() if payload_kind == "large" else _build_payload()

    info_client = _open_info_client(location)
    try:
        with override_settings(CACHES=caches):
            from django.core.cache import cache

            _flush_cache(cache)

            # Warmup: populate known keys for get/mget phases.
            for i in range(WARMUP_KEYS):
                cache.set(f"warm:{i}", payload)

            # One untimed pass through every phase to prime connections, server
            # working set, and any lazy serializer state.
            _bench_get(cache, 100)
            _bench_set(cache, 100, payload)
            _bench_mget(cache, 10)
            _bench_mset(cache, 10, payload)
            _bench_incr(cache, 100)
            _bench_delete(cache, 100)

            # Record baseline connection count after warmup but before any timed
            # phases, so per-phase samples reveal what the workload itself opens.
            baseline_conns = _server_connections(info_client)
            if baseline_conns is not None:
                result.server_connections_baseline = baseline_conns

            def _sample_conns() -> None:
                v = _server_connections(info_client)
                if v is not None:
                    result.server_connections_samples.append(v)

            for _ in range(K_RUNS):
                gc.collect()
                tracemalloc.start()

                run_before_used = _server_used_memory(info_client) or 0

                _, t = _phase("get", lambda: _bench_get(cache, N_OPS))
                result.phases["get"].seconds_per_run.append(t)
                _sample_conns()

                _, t = _phase("get-miss", lambda: _bench_get_miss(cache, N_OPS))
                result.phases["get-miss"].seconds_per_run.append(t)
                _sample_conns()

                _, t = _phase("set", lambda: _bench_set(cache, N_OPS, payload))
                result.phases["set"].seconds_per_run.append(t)
                _sample_conns()

                _, t = _phase("mget", lambda: _bench_mget(cache, N_OPS // MGET_BATCH))
                result.phases["mget"].seconds_per_run.append(t * MGET_BATCH)  # normalize to per-key
                _sample_conns()

                _, t = _phase("mset", lambda: _bench_mset(cache, N_OPS // MGET_BATCH, payload))
                result.phases["mset"].seconds_per_run.append(t * MGET_BATCH)  # normalize to per-key
                _sample_conns()

                _, t = _phase("incr", lambda: _bench_incr(cache, N_OPS))
                result.phases["incr"].seconds_per_run.append(t)
                _sample_conns()

                _, t = _phase("delete", lambda: _bench_delete(cache, N_OPS))
                result.phases["delete"].seconds_per_run.append(t)
                _sample_conns()

                _, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()
                result.py_peak_kb_per_run.append(peak / 1024)

                run_after_used = _server_used_memory(info_client) or 0
                result.server_used_memory_delta_kb_per_run.append(
                    max(0, (run_after_used - run_before_used) / 1024),
                )

            _flush_cache(cache)
    finally:
        info_client.close()

    return result


# Minimal middleware list for the request-cycle benchmark. CommonMiddleware
# is the only one a typical Django app always has; SessionMiddleware /
# AuthenticationMiddleware would pull in DB queries that distort cache timing.
_REQUEST_CYCLE_MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
]


def run_request_cycle_benchmark(  # noqa: PLR0915 — phase-by-phase flow is readable
    driver: DriverConfig,
    serializer: SerializerConfig,
    location: str,
    *,
    compressor: CompressorConfig | None = None,
    payload_kind: str = "small",
) -> BenchmarkResult:
    """Same workload as ``run_benchmark``, but every cache op is wrapped in a
    real Django request cycle (URL resolve, middleware, view dispatch,
    ``request_started`` / ``request_finished`` signals).

    Throughput is reported in ops/sec on the same scale as ``run_benchmark``,
    so the two are directly comparable — the gap is the per-request overhead
    Django itself adds when cache work happens inside a view.
    """

    from django.test import Client

    from benchmarks import urls as benchmark_urls

    benchmark_urls.set_payload_kind(payload_kind)

    result = BenchmarkResult(
        driver_id=driver.id,
        serializer_id=serializer.id,
        server=driver.server,
        compressor_id=compressor.id if compressor is not None else "",
    )
    for name in ("get", "get-miss", "set", "mget", "mset", "incr", "delete"):
        result.phases[name] = PhaseTiming(name=name)

    caches = build_caches(driver, serializer, location, compressor=compressor)
    payload = _build_payload_large() if payload_kind == "large" else _build_payload()

    overrides = {
        "CACHES": caches,
        "ROOT_URLCONF": "benchmarks.urls",
        "MIDDLEWARE": _REQUEST_CYCLE_MIDDLEWARE,
        "ALLOWED_HOSTS": ["*"],
        "DEBUG": False,
    }

    def _drive(client: Client, url_template: str, n: int) -> None:
        for i in range(n):
            client.get(url_template.format(i=i))

    info_client = _open_info_client(location)
    try:
        with override_settings(**overrides):
            from django.core.cache import cache

            _flush_cache(cache)

            # Populate the keys read by the get/mget views.
            for i in range(WARMUP_KEYS):
                cache.set(f"warm:{i}", payload)

            # Django's built-in RedisCache.incr() raises if the key is missing,
            # so the counter has to exist before the incr view ever runs.
            cache.set("counter", 0)

            client = Client()

            # Untimed warmup so the connection pool, URL resolver cache, and
            # serializer all see the workload before timing begins.
            _drive(client, "/bench/get/{i}/", 100)
            _drive(client, "/bench/set/{i}/", 100)
            _drive(client, "/bench/mget/{i}/", 10)
            _drive(client, "/bench/mset/{i}/", 10)
            _drive(client, "/bench/incr/{i}/", 100)
            _drive(client, "/bench/delete/{i}/", 100)

            baseline_conns = _server_connections(info_client)
            if baseline_conns is not None:
                result.server_connections_baseline = baseline_conns

            def _sample_conns() -> None:
                v = _server_connections(info_client)
                if v is not None:
                    result.server_connections_samples.append(v)

            for _ in range(K_RUNS):
                gc.collect()
                tracemalloc.start()
                run_before_used = _server_used_memory(info_client) or 0

                _, t = _phase("get", lambda: _drive(client, "/bench/get/{i}/", N_OPS))
                result.phases["get"].seconds_per_run.append(t)
                _sample_conns()

                _, t = _phase("get-miss", lambda: _drive(client, "/bench/get-miss/{i}/", N_OPS))
                result.phases["get-miss"].seconds_per_run.append(t)
                _sample_conns()

                _, t = _phase("set", lambda: _drive(client, "/bench/set/{i}/", N_OPS))
                result.phases["set"].seconds_per_run.append(t)
                _sample_conns()

                # Batch ops: one HTTP request per batch; normalize timing per-key
                # so ops/sec is on the same scale as the direct benchmark.
                _, t = _phase("mget", lambda: _drive(client, "/bench/mget/{i}/", N_OPS // MGET_BATCH))
                result.phases["mget"].seconds_per_run.append(t * MGET_BATCH)
                _sample_conns()

                _, t = _phase("mset", lambda: _drive(client, "/bench/mset/{i}/", N_OPS // MGET_BATCH))
                result.phases["mset"].seconds_per_run.append(t * MGET_BATCH)
                _sample_conns()

                # incr resets the counter inside the view's cache call; pre-set it
                # once per run to mirror the direct benchmark.
                cache.set("counter", 0)
                _, t = _phase("incr", lambda: _drive(client, "/bench/incr/{i}/", N_OPS))
                result.phases["incr"].seconds_per_run.append(t)
                _sample_conns()

                # delete needs keys to delete; populate first.
                for i in range(N_OPS):
                    cache.set(f"del:{i}", 1)
                _, t = _phase("delete", lambda: _drive(client, "/bench/delete/{i}/", N_OPS))
                result.phases["delete"].seconds_per_run.append(t)
                _sample_conns()

                _, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()
                result.py_peak_kb_per_run.append(peak / 1024)

                run_after_used = _server_used_memory(info_client) or 0
                result.server_used_memory_delta_kb_per_run.append(
                    max(0, (run_after_used - run_before_used) / 1024),
                )

            _flush_cache(cache)
    finally:
        info_client.close()

    return result


def _total_rss_kb(parent_pid: int) -> float:
    """Sum RSS of a process and its direct children, in KiB.

    Granian spawns one parent + N workers; we want the total resident set
    so growth shows up regardless of which worker grew.
    """
    import shutil
    import subprocess
    from pathlib import Path

    total = 0.0
    try:
        with Path(f"/proc/{parent_pid}/status").open() as f:
            for raw in f:
                if raw.startswith("VmRSS:"):
                    total += float(raw.split()[1])
                    break
    except FileNotFoundError, ProcessLookupError:
        return 0.0

    ps = shutil.which("ps")
    if ps is None:
        return total
    try:
        result = subprocess.run(  # noqa: S603 — args are constants, no user input
            [ps, "--ppid", str(parent_pid), "-o", "rss="],
            capture_output=True,
            text=True,
            check=False,
            timeout=2,
        )
    except (subprocess.SubprocessError, OSError) as exc:
        warnings.warn(f"ps RSS sample failed: {exc!r}", stacklevel=2)
        return total
    for raw in result.stdout.strip().splitlines():
        stripped = raw.strip()
        if stripped:
            total += float(stripped)
    return total


def _wait_for_port(host: str, port: int, timeout_s: float = 15.0) -> bool:
    import socket

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def run_asgi_benchmark(  # noqa: C901, PLR0915 — orchestrates many phases in one function for readability
    driver: DriverConfig,
    serializer: SerializerConfig,
    location: str,
    *,
    duration_s: int = 30,
    concurrency: int = 300,
    workers: int = 4,
    port: int = 8787,
    sample_every_s: float = 5.0,
    cooldown_s: float = 5.0,
) -> AsgiResult:
    """Full-stack ASGI benchmark: granian + httpx + 6-op view.

    Mirrors django-vcache's ``bench_compare.py`` methodology: spawn a real
    ASGI server, hit it with a configurable number of concurrent HTTP
    clients for a fixed duration, sample peak server RSS and Valkey/Redis
    ``connected_clients`` along the way. The view does six async cache ops
    per request — get / aget_many / aset / aset (large) / aincr / aget
    (large) — to exercise the full backend surface in one workload.

    Latency simulation (e.g. ``tc qdisc add dev eth0 root netem delay 1ms``)
    is the missing ingredient versus django-vcache's claims about
    `RedisCache` connection growth — without RTT, sync_to_async threads
    finish too quickly to pile up. Run this benchmark inside a Docker
    container with ``--cap-add NET_ADMIN`` and apply ``netem`` against the
    Valkey/Redis interface to reproduce vcache's numbers.
    """

    import asyncio as _asyncio
    import json
    import os
    import subprocess
    import sys

    import httpx

    options_for_env = dict(driver.options)
    if serializer.dotted_path is not None:
        options_for_env["serializer"] = serializer.dotted_path

    env = {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "benchmarks.asgi_settings",
        "BENCH_CACHE_BACKEND": driver.backend,
        "BENCH_CACHE_LOCATION": location,
        "BENCH_CACHE_OPTIONS_JSON": json.dumps(options_for_env),
    }

    info_client = _open_info_client(location)
    info_client.flushdb()

    proc = subprocess.Popen(  # noqa: S603 — args are sys.executable + constants
        [
            sys.executable,
            "-m",
            "granian",
            "--interface",
            "asgi",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            str(workers),
            "--no-ws",
            "benchmarks.asgi:application",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        if not _wait_for_port("127.0.0.1", port, timeout_s=15.0):
            raise RuntimeError("granian did not start in time")

        # Seed cache + give workers a moment to spawn.
        time.sleep(1.0)
        with httpx.Client(timeout=5.0) as c:
            r = c.get(f"http://127.0.0.1:{port}/bench/seed/")
            r.raise_for_status()

        initial_rss = _total_rss_kb(proc.pid) / 1024.0
        initial_conns = _server_connections(info_client) or 0
        peak_rss = initial_rss
        peak_conns = initial_conns

        async def _load() -> tuple[int, int, list[float]]:
            """Generate load with ``concurrency`` httpx clients for ``duration_s``.

            Returns (total_requests, errors, per-request latency samples).
            """

            url = f"http://127.0.0.1:{port}/bench/mixed/"
            stop_at = time.perf_counter() + duration_s
            latencies: list[float] = []
            total = 0
            errors = 0

            async def _client_loop(client: httpx.AsyncClient) -> tuple[int, int, list[float]]:
                local_lat: list[float] = []
                t = 0
                e = 0
                while time.perf_counter() < stop_at:
                    start = time.perf_counter()
                    try:
                        resp = await client.get(url)
                        local_lat.append((time.perf_counter() - start) * 1000)
                        if resp.status_code >= 400:
                            e += 1
                    except httpx.HTTPError:
                        # Transport / protocol / status errors are part of what we measure.
                        # Programming bugs (NameError, TypeError, etc.) intentionally propagate
                        # so they crash the run instead of being counted as request errors.
                        e += 1
                    t += 1
                return t, e, local_lat

            limits = httpx.Limits(
                max_connections=concurrency * 2,
                max_keepalive_connections=concurrency * 2,
            )
            async with httpx.AsyncClient(timeout=30.0, limits=limits) as client:
                tasks = [_asyncio.create_task(_client_loop(client)) for _ in range(concurrency)]

                # Sampler runs alongside the load.
                sample_task = _asyncio.create_task(_sampler())
                results = await _asyncio.gather(*tasks)
                sample_task.cancel()

            for t, e, lats in results:
                total += t
                errors += e
                latencies.extend(lats)
            return total, errors, latencies

        async def _sampler() -> None:
            nonlocal peak_rss, peak_conns
            try:
                while True:
                    await _asyncio.sleep(sample_every_s)
                    rss = _total_rss_kb(proc.pid) / 1024.0
                    conns = _server_connections(info_client) or 0
                    peak_rss = max(peak_rss, rss)
                    peak_conns = max(peak_conns, conns)
            except _asyncio.CancelledError:
                return

        load_started = time.perf_counter()
        total_requests, errors, latencies = _asyncio.run(_load())
        actual_duration = time.perf_counter() - load_started

        final_rss = _total_rss_kb(proc.pid) / 1024.0
        final_conns = _server_connections(info_client) or 0
        peak_rss = max(peak_rss, final_rss)
        peak_conns = max(peak_conns, final_conns)

        # Cooldown: let GC settle and connections close, then re-sample.
        time.sleep(cooldown_s)
        settled_rss = _total_rss_kb(proc.pid) / 1024.0
        settled_conns = _server_connections(info_client) or 0

        rps = total_requests / actual_duration if actual_duration else 0.0
        avg_lat = mean(latencies) if latencies else 0.0
        p99_lat = sorted(latencies)[int(round(0.99 * (len(latencies) - 1)))] if latencies else 0.0

        return AsgiResult(
            driver_id=driver.id,
            serializer_id=serializer.id,
            server=driver.server,
            requests_per_sec=rps,
            avg_latency_ms=avg_lat,
            p99_latency_ms=p99_lat,
            total_requests=total_requests,
            errors=errors,
            initial_rss_mb=initial_rss,
            peak_rss_mb=peak_rss,
            final_rss_mb=final_rss,
            settled_rss_mb=settled_rss,
            initial_conns=initial_conns,
            peak_conns=peak_conns,
            final_conns=final_conns,
            settled_conns=settled_conns,
            duration_s=actual_duration,
            concurrency=concurrency,
        )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        info_client.close()


def format_asgi_summary(result: AsgiResult) -> str:
    return (
        f"  driver={result.driver_id}  serializer={result.serializer_id}  server={result.server}\n"
        f"  duration={result.duration_s:.1f}s  concurrency={result.concurrency}  "
        f"workers via granian (started by runner)\n"
        f"  requests={result.total_requests:,}  errors={result.errors}  "
        f"req/s={result.requests_per_sec:,.0f}\n"
        f"  latency avg={result.avg_latency_ms:.1f}ms  p99={result.p99_latency_ms:.1f}ms\n"
        f"  RSS init={result.initial_rss_mb:.1f}  peak={result.peak_rss_mb:.1f}  "
        f"final={result.final_rss_mb:.1f}  settled={result.settled_rss_mb:.1f} MB  "
        f"(growth {result.rss_growth_mb:+.1f} MB)\n"
        f"  conns init={result.initial_conns}  peak={result.peak_conns}  "
        f"final={result.final_conns}  settled={result.settled_conns}"
    )


def format_asgi_table(results: list[AsgiResult]) -> str:
    if not results:
        return "(no asgi results)"
    header = ["config", "req/s", "avg ms", "p99 ms", "RSS init", "RSS peak", "RSS final", "conns peak", "conns settled"]
    rows: list[list[str]] = []
    for r in results:
        rows.append(
            [
                r.label,
                f"{r.requests_per_sec:,.0f}",
                f"{r.avg_latency_ms:.1f}",
                f"{r.p99_latency_ms:.1f}",
                f"{r.initial_rss_mb:.0f}",
                f"{r.peak_rss_mb:.0f}",
                f"{r.final_rss_mb:.0f}",
                str(r.peak_conns),
                str(r.settled_conns),
            ],
        )
    widths = [len(h) for h in header]
    for row in rows:
        widths = [max(w, len(c)) for w, c in zip(widths, row, strict=True)]
    sep = "  ".join("-" * w for w in widths)
    lines = ["  ".join(h.ljust(w) for h, w in zip(header, widths, strict=True)), sep]
    for row in rows:
        lines.append(
            "  ".join(c.rjust(w) if i else c.ljust(w) for i, (c, w) in enumerate(zip(row, widths, strict=True))),
        )
    return "\n".join(lines)


async def _run_async_workload(
    cache,
    info_client,
    payload: Any,
    concurrency: int,
    result: BenchmarkResult,
) -> None:
    await cache.aclear()

    # Warmup: populate known keys for get/mget phases.
    for i in range(WARMUP_KEYS):
        await cache.aset(f"warm:{i}", payload)

    # Pre-set the incr counter for backends (e.g. Django's built-in
    # RedisCache) that raise on incr against a missing key.
    await cache.aset("counter", 0)

    # One untimed pass per phase to warm up async transports / serializers.
    await _abench_get(cache, 100, concurrency)
    await _abench_set(cache, 100, concurrency, payload)
    await _abench_mget(cache, 10, concurrency)
    await _abench_mset(cache, 10, concurrency, payload)
    await _abench_incr(cache, 100, concurrency)
    await _abench_delete(cache, 100, concurrency)

    baseline_conns = _server_connections(info_client)
    if baseline_conns is not None:
        result.server_connections_baseline = baseline_conns

    def _sample_conns() -> None:
        v = _server_connections(info_client)
        if v is not None:
            result.server_connections_samples.append(v)

    async def _phase_async(name: str, coro_factory) -> None:
        start = time.perf_counter()
        await coro_factory()
        result.phases[name].seconds_per_run.append(time.perf_counter() - start)
        _sample_conns()

    for _ in range(K_RUNS):
        gc.collect()
        tracemalloc.start()
        run_before_used = _server_used_memory(info_client) or 0

        await _phase_async("get", lambda: _abench_get(cache, N_OPS, concurrency))
        await _phase_async("get-miss", lambda: _abench_get_miss(cache, N_OPS, concurrency))
        await _phase_async("set", lambda: _abench_set(cache, N_OPS, concurrency, payload))

        # Batch ops: same per-key normalisation as the sync path.
        n_batches = N_OPS // MGET_BATCH
        start = time.perf_counter()
        await _abench_mget(cache, n_batches, concurrency)
        result.phases["mget"].seconds_per_run.append((time.perf_counter() - start) * MGET_BATCH)
        _sample_conns()

        start = time.perf_counter()
        await _abench_mset(cache, n_batches, concurrency, payload)
        result.phases["mset"].seconds_per_run.append((time.perf_counter() - start) * MGET_BATCH)
        _sample_conns()

        await _phase_async("incr", lambda: _abench_incr(cache, N_OPS, concurrency))
        await _phase_async("delete", lambda: _abench_delete(cache, N_OPS, concurrency))

        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        result.py_peak_kb_per_run.append(peak / 1024)

        run_after_used = _server_used_memory(info_client) or 0
        result.server_used_memory_delta_kb_per_run.append(
            max(0, (run_after_used - run_before_used) / 1024),
        )

    await cache.aclear()


def run_async_benchmark(
    driver: DriverConfig,
    serializer: SerializerConfig,
    location: str,
    *,
    compressor: CompressorConfig | None = None,
    payload_kind: str = "small",
    concurrency: int = 1,
) -> BenchmarkResult:
    """Async equivalent of ``run_benchmark``.

    ``concurrency=1`` is serial async — every ``aget`` is awaited before the
    next is issued, giving a one-to-one comparison with the sync benchmark
    (the gap reveals overhead of the async path: native ``aget`` vs
    ``sync_to_async`` fallback).

    ``concurrency>1`` issues that many ops in flight via ``asyncio.gather``,
    stressing the connection pool and surfacing per-call client patterns
    (e.g. Django's built-in ``RedisCache.get_client()``) that hold a fresh
    ``redis.Redis`` instance per concurrent op.
    """

    result = BenchmarkResult(
        driver_id=driver.id,
        serializer_id=serializer.id,
        server=driver.server,
        compressor_id=compressor.id if compressor is not None else "",
    )
    for name in ("get", "get-miss", "set", "mget", "mset", "incr", "delete"):
        result.phases[name] = PhaseTiming(name=name)

    caches = build_caches(driver, serializer, location, compressor=compressor)
    payload = _build_payload_large() if payload_kind == "large" else _build_payload()

    info_client = _open_info_client(location)
    try:
        with override_settings(CACHES=caches):
            from django.core.cache import cache

            asyncio.run(_run_async_workload(cache, info_client, payload, concurrency, result))
    finally:
        info_client.close()

    return result


def run_compressor_micro(
    compressor: CompressorConfig,
    *,
    n_runs: int = 20,
    n_ops: int = 200,
) -> MicroResult | None:
    """Pure compress/decompress throughput on the large benchmark payload.

    No driver, no Django, no network — just the algorithm against a fixed
    blob. Returns None for the no-compression baseline.
    """
    from django.utils.module_loading import import_string

    if compressor.dotted_path is None:
        return None

    import pickle as _pickle

    payload = _pickle.dumps(_build_payload_large())
    cls = import_string(compressor.dotted_path)
    instance = cls()
    compressed = instance._compress(payload)

    def _median_seconds(fn: Callable[[], None]) -> float:
        samples = []
        for _ in range(n_runs):
            start = time.perf_counter()
            for _ in range(n_ops):
                fn()
            samples.append(time.perf_counter() - start)
        return median(samples)

    t_comp = _median_seconds(lambda: instance._compress(payload))
    t_decomp = _median_seconds(lambda: instance.decompress(compressed))

    return MicroResult(
        compressor_id=compressor.id,
        input_bytes=len(payload),
        output_bytes=len(compressed),
        compress_mb_s=(len(payload) * n_ops) / t_comp / 1_000_000,
        decompress_mb_s=(len(payload) * n_ops) / t_decomp / 1_000_000,
    )


def format_micro_table(results: list[MicroResult]) -> str:
    """Table of compressor micro results — absolute MB/s plus output ratio."""
    if not results:
        return "(no micro results)"
    header = ["compressor", "out %", "compress (MB/s)", "decompress (MB/s)"]
    rows: list[list[str]] = []
    for r in results:
        rows.append(
            [
                r.compressor_id,
                f"{r.ratio:.1%}",
                f"{r.compress_mb_s:,.1f}",
                f"{r.decompress_mb_s:,.1f}",
            ],
        )
    widths = [len(h) for h in header]
    for row in rows:
        widths = [max(w, len(c)) for w, c in zip(widths, row, strict=True)]
    sep = "  ".join("-" * w for w in widths)
    lines = ["  ".join(h.ljust(w) for h, w in zip(header, widths, strict=True)), sep]
    for row in rows:
        lines.append(
            "  ".join(c.rjust(w) if i else c.ljust(w) for i, (c, w) in enumerate(zip(row, widths, strict=True))),
        )
    return "\n".join(lines)


def format_summary(result: BenchmarkResult) -> str:
    compressor = f"  compressor={result.compressor_id}" if result.compressor_id else ""
    lines = [
        f"  driver={result.driver_id}  serializer={result.serializer_id}{compressor}  server={result.server}",
        f"  {'phase':<10} {'med (ms)':>10} {'p95 (ms)':>10} {'ops/sec':>12}",
    ]
    for phase in result.phases.values():
        lines.append(
            f"  {phase.name:<10} {phase.median_seconds * 1000:>10.2f} "
            f"{phase.p95_seconds * 1000:>10.2f} {phase.ops_per_sec:>12,.0f}",
        )
    lines.append(
        f"  py-peak-mem ~ {mean(result.py_peak_kb_per_run):.1f} KiB/run  "
        f"server-mem-delta ~ {mean(result.server_used_memory_delta_kb_per_run):.1f} KiB/run  "
        f"server-conns base/peak/Δ = {result.server_connections_baseline}/"
        f"{result.server_connections_peak}/{result.server_connections_delta}",
    )
    return "\n".join(lines)


def format_table(results: list[BenchmarkResult]) -> str:
    """Compact one-row-per-config summary across all phases."""
    if not results:
        return "(no benchmark results)"
    phase_names = list(results[0].phases.keys())
    header = ["config"] + [f"{p} (ops/s)" for p in phase_names] + ["py-mem KiB", "srv-mem KiB", "conns peak", "conns Δ"]
    widths = [len(h) for h in header]
    rows: list[list[str]] = []
    for r in results:
        row = [r.label]
        for p in phase_names:
            row.append(f"{r.phases[p].ops_per_sec:,.0f}")
        row.append(f"{mean(r.py_peak_kb_per_run):.0f}")
        row.append(f"{mean(r.server_used_memory_delta_kb_per_run):.0f}")
        row.append(str(r.server_connections_peak))
        row.append(str(r.server_connections_delta))
        rows.append(row)
        widths = [max(w, len(c)) for w, c in zip(widths, row, strict=True)]

    sep = "  ".join("-" * w for w in widths)
    lines = ["  ".join(h.ljust(w) for h, w in zip(header, widths, strict=True)), sep]
    for row in rows:
        lines.append(
            "  ".join(c.rjust(w) if i else c.ljust(w) for i, (c, w) in enumerate(zip(row, widths, strict=True))),
        )
    return "\n".join(lines)
