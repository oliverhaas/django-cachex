"""Benchmark workload + measurement helpers.

Single source of truth for what an "operation" means in this harness. The
goal is explicit and readable, not generic — adjust the workload phases here
when the questions you care about change.
"""

from __future__ import annotations

import gc
import time
import tracemalloc
from dataclasses import dataclass, field
from statistics import mean, median
from typing import TYPE_CHECKING, Any

from django.test import override_settings

from benchmarks.configs import DriverConfig, SerializerConfig

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
    phases: dict[str, PhaseTiming] = field(default_factory=dict)
    py_peak_kb_per_run: list[float] = field(default_factory=list)
    server_used_memory_delta_kb_per_run: list[float] = field(default_factory=list)
    server_connections_after: int = 0

    @property
    def label(self) -> str:
        return f"{self.driver_id}+{self.serializer_id}@{self.server}"


def build_caches(
    driver: DriverConfig,
    serializer: SerializerConfig,
    location: str,
) -> dict[str, dict[str, Any]]:
    options = dict(driver.options)
    if serializer.dotted_path is not None:
        options["serializer"] = serializer.dotted_path
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


def _server_used_memory(cache) -> int | None:
    try:
        info = cache.info()
    except Exception:
        return None
    used = info.get("used_memory") if isinstance(info, dict) else None
    if used is None and isinstance(info, dict):
        # Some backends nest under "memory" section.
        used = info.get("memory", {}).get("used_memory") if isinstance(info.get("memory"), dict) else None
    return int(used) if used is not None else None


def _server_connections(cache) -> int | None:
    try:
        info = cache.info()
    except Exception:
        return None
    if not isinstance(info, dict):
        return None
    val = info.get("connected_clients")
    if val is None and isinstance(info.get("clients"), dict):
        val = info["clients"].get("connected_clients")
    return int(val) if val is not None else None


def run_benchmark(
    driver: DriverConfig,
    serializer: SerializerConfig,
    location: str,
) -> BenchmarkResult:
    """Run the workload K_RUNS times and return aggregated metrics."""

    result = BenchmarkResult(
        driver_id=driver.id,
        serializer_id=serializer.id,
        server=driver.server,
    )
    for name in ("get", "get-miss", "set", "mget", "mset", "incr", "delete"):
        result.phases[name] = PhaseTiming(name=name)

    caches = build_caches(driver, serializer, location)
    payload = _build_payload()

    with override_settings(CACHES=caches):
        from django.core.cache import cache

        cache.flush_db()

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

        before_used = _server_used_memory(cache) or 0

        for _ in range(K_RUNS):
            gc.collect()
            tracemalloc.start()

            run_before_used = _server_used_memory(cache) or 0

            _, t = _phase("get", lambda: _bench_get(cache, N_OPS))
            result.phases["get"].seconds_per_run.append(t)

            _, t = _phase("get-miss", lambda: _bench_get_miss(cache, N_OPS))
            result.phases["get-miss"].seconds_per_run.append(t)

            _, t = _phase("set", lambda: _bench_set(cache, N_OPS, payload))
            result.phases["set"].seconds_per_run.append(t)

            _, t = _phase("mget", lambda: _bench_mget(cache, N_OPS // MGET_BATCH))
            result.phases["mget"].seconds_per_run.append(t * MGET_BATCH)  # normalize to per-key

            _, t = _phase("mset", lambda: _bench_mset(cache, N_OPS // MGET_BATCH, payload))
            result.phases["mset"].seconds_per_run.append(t * MGET_BATCH)  # normalize to per-key

            _, t = _phase("incr", lambda: _bench_incr(cache, N_OPS))
            result.phases["incr"].seconds_per_run.append(t)

            _, t = _phase("delete", lambda: _bench_delete(cache, N_OPS))
            result.phases["delete"].seconds_per_run.append(t)

            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            result.py_peak_kb_per_run.append(peak / 1024)

            run_after_used = _server_used_memory(cache) or 0
            result.server_used_memory_delta_kb_per_run.append(
                max(0, (run_after_used - run_before_used) / 1024),
            )

        connections = _server_connections(cache)
        if connections is not None:
            result.server_connections_after = connections

        cache.flush_db()
        # Avoid reporting startup churn — record absolute used_memory once at end.
        _ = before_used

    return result


def format_summary(result: BenchmarkResult) -> str:
    lines = [
        f"  driver={result.driver_id}  serializer={result.serializer_id}  server={result.server}",
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
        f"server-conns-after = {result.server_connections_after}",
    )
    return "\n".join(lines)


def format_table(results: list[BenchmarkResult]) -> str:
    """Compact one-row-per-config summary across all phases."""
    if not results:
        return "(no benchmark results)"
    phase_names = list(results[0].phases.keys())
    header = ["config"] + [f"{p} (ops/s)" for p in phase_names] + ["py-mem KiB", "srv-mem KiB", "conns"]
    widths = [len(h) for h in header]
    rows: list[list[str]] = []
    for r in results:
        row = [r.label]
        for p in phase_names:
            row.append(f"{r.phases[p].ops_per_sec:,.0f}")
        row.append(f"{mean(r.py_peak_kb_per_run):.0f}")
        row.append(f"{mean(r.server_used_memory_delta_kb_per_run):.0f}")
        row.append(str(r.server_connections_after))
        rows.append(row)
        widths = [max(w, len(c)) for w, c in zip(widths, row, strict=True)]

    sep = "  ".join("-" * w for w in widths)
    lines = ["  ".join(h.ljust(w) for h, w in zip(header, widths, strict=True)), sep]
    for row in rows:
        lines.append(
            "  ".join(c.rjust(w) if i else c.ljust(w) for i, (c, w) in enumerate(zip(row, widths, strict=True))),
        )
    return "\n".join(lines)
