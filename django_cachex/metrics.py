"""Optional Prometheus metrics for cache operations.

Requires ``prometheus-client``::

    pip install django-cachex[prometheus]

Metrics are only created if ``prometheus_client`` is importable. When it is
not installed, all public functions are safe no-ops.

Exposed metrics:

- ``django_cache_ops_total`` (Counter): Total cache operations,
  labeled by ``cache`` (alias), ``operation``, and ``status``.
- ``django_cache_op_duration_seconds`` (Histogram): Operation latency,
  labeled by ``cache`` and ``operation``.
"""

from __future__ import annotations

import json
import time
from collections import deque
from contextlib import contextmanager
from threading import Lock
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

try:
    from prometheus_client import Counter, Histogram  # type: ignore[import-not-found]  # ty: ignore[unresolved-import]

    CACHE_OPS: Counter | None = Counter(
        "django_cache_ops_total",
        "Total cache operations",
        ["cache", "operation", "status"],
    )
    CACHE_LATENCY: Histogram | None = Histogram(
        "django_cache_op_duration_seconds",
        "Cache operation latency in seconds",
        ["cache", "operation"],
    )
    HAS_PROMETHEUS = True

except ImportError:
    HAS_PROMETHEUS = False
    CACHE_OPS = None
    CACHE_LATENCY = None


def record(cache_alias: str, operation: str, status: str) -> None:
    """Record a cache operation counter increment."""
    if HAS_PROMETHEUS and CACHE_OPS is not None:
        CACHE_OPS.labels(cache=cache_alias, operation=operation, status=status).inc()


@contextmanager
def record_latency(cache_alias: str, operation: str) -> Generator[None]:
    """Context manager to record operation latency."""
    if not HAS_PROMETHEUS:
        yield
        return
    if CACHE_LATENCY is None:
        yield
        return
    start = time.perf_counter()
    try:
        yield
    finally:
        CACHE_LATENCY.labels(cache=cache_alias, operation=operation).observe(
            time.perf_counter() - start,
        )


def get_stats() -> dict[str, dict[str, Any]]:
    """Read current metric values for the admin dashboard.

    Returns a dict keyed by cache alias with counters and computed rates::

        {
            "default": {
                "gets": 150, "hits": 120, "misses": 30,
                "sets": 40, "deletes": 5,
                "hit_rate": 80.0,
                "avg_latency_ms": 0.12,
            },
            ...
        }
    """
    if not HAS_PROMETHEUS:
        return {}

    from django.conf import settings

    stats: dict[str, dict[str, Any]] = {}
    aliases = list(settings.CACHES.keys())

    for alias in aliases:
        gets = _counter_value(alias, "get", "hit") + _counter_value(alias, "get", "miss")
        hits = _counter_value(alias, "get", "hit")
        misses = _counter_value(alias, "get", "miss")
        sets = _counter_value(alias, "set", "ok")
        deletes = _counter_value(alias, "delete", "ok")
        get_many = _counter_value(alias, "get_many", "ok")
        set_many = _counter_value(alias, "set_many", "ok")
        total = gets + sets + deletes + get_many + set_many

        hit_rate = round(hits / gets * 100, 1) if gets > 0 else 0.0

        # Average latency from histogram
        avg_latency_ms = _avg_latency_ms(alias)

        stats[alias] = {
            "total": int(total),
            "gets": int(gets),
            "hits": int(hits),
            "misses": int(misses),
            "sets": int(sets),
            "deletes": int(deletes),
            "get_many": int(get_many),
            "set_many": int(set_many),
            "hit_rate": hit_rate,
            "avg_latency_ms": avg_latency_ms,
        }

    return stats


def _counter_value(cache: str, operation: str, status: str) -> float:
    """Read a single counter value from the registry."""
    if CACHE_OPS is None:
        return 0.0
    try:
        return CACHE_OPS.labels(cache=cache, operation=operation, status=status)._value.get()
    except Exception:  # noqa: BLE001
        return 0.0


def _avg_latency_ms(cache: str) -> float:
    """Compute average latency in ms across all operations for a cache."""
    if CACHE_LATENCY is None:
        return 0.0
    total_sum = 0.0
    total_count = 0.0
    for op in ("get", "set", "delete", "get_many", "set_many"):
        try:
            h = CACHE_LATENCY.labels(cache=cache, operation=op)
            total_sum += h._sum.get()
            # Sum bucket values to get total count (buckets are non-cumulative internally)
            total_count += sum(b.get() for b in h._buckets)
        except Exception:  # noqa: BLE001, S110
            pass
    if total_count > 0:
        return round(total_sum / total_count * 1000, 3)
    return 0.0


# ======================================================================
# Time-series history (in-memory ring buffer)
# ======================================================================

# Each snapshot: (timestamp, {alias: {hits, misses, total}})
_history: deque[tuple[float, dict[str, dict[str, float]]]] = deque(maxlen=360)
_history_lock = Lock()
_MIN_SNAPSHOT_INTERVAL = 2.0  # seconds between snapshots


def snapshot() -> None:
    """Take a snapshot of current counters and append to the ring buffer.

    Called on each dashboard page load. Snapshots are throttled to at
    most one every ``_MIN_SNAPSHOT_INTERVAL`` seconds.
    """
    if not HAS_PROMETHEUS:
        return
    now = time.time()
    with _history_lock:
        if _history and (now - _history[-1][0]) < _MIN_SNAPSHOT_INTERVAL:
            return
    from django.conf import settings

    point: dict[str, dict[str, float]] = {}
    for alias in settings.CACHES:
        hits = _counter_value(alias, "get", "hit")
        misses = _counter_value(alias, "get", "miss")
        sets = _counter_value(alias, "set", "ok")
        deletes = _counter_value(alias, "delete", "ok")
        point[alias] = {
            "hits": hits,
            "misses": misses,
            "sets": sets,
            "deletes": deletes,
            "total": hits + misses + sets + deletes,
        }
    with _history_lock:
        _history.append((now, point))


_PERIOD_SECONDS = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
}


def get_throughput_json(period: str = "") -> str:
    """Compute ops/sec rates from the snapshot history for Chart.js.

    Args:
        period: Filter to last "5m", "15m", or "30m". Empty = all history.

    Returns a JSON string with labels (timestamps) and datasets
    (hits/sec, misses/sec, sets/sec) aggregated across all caches.
    """
    with _history_lock:
        points = list(_history)

    if len(points) < 2:
        return ""

    # Filter by period
    cutoff_seconds = _PERIOD_SECONDS.get(period, 0)
    if cutoff_seconds:
        cutoff = time.time() - cutoff_seconds
        points = [(t, p) for t, p in points if t >= cutoff]
        if len(points) < 2:
            return ""

    labels: list[str] = []
    hits_rate: list[float] = []
    misses_rate: list[float] = []
    sets_rate: list[float] = []

    for i in range(1, len(points)):
        t_prev, prev = points[i - 1]
        t_curr, curr = points[i]
        dt = t_curr - t_prev
        if dt <= 0:
            continue

        # Aggregate deltas across all caches
        d_hits = sum(curr.get(a, {}).get("hits", 0) - prev.get(a, {}).get("hits", 0) for a in curr)
        d_misses = sum(curr.get(a, {}).get("misses", 0) - prev.get(a, {}).get("misses", 0) for a in curr)
        d_sets = sum(curr.get(a, {}).get("sets", 0) - prev.get(a, {}).get("sets", 0) for a in curr)

        # Format timestamp as HH:MM:SS
        t = time.localtime(t_curr)
        labels.append(f"{t.tm_hour:02d}:{t.tm_min:02d}:{t.tm_sec:02d}")
        hits_rate.append(round(d_hits / dt, 1))
        misses_rate.append(round(d_misses / dt, 1))
        sets_rate.append(round(d_sets / dt, 1))

    if not labels:
        return ""

    return json.dumps(
        {
            "labels": labels,
            "hits": hits_rate,
            "misses": misses_rate,
            "sets": sets_rate,
        },
    )
