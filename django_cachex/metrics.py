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
import logging
import time
from collections import deque
from contextlib import contextmanager
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

logger = logging.getLogger(__name__)

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
        _ensure_flush_thread()
        _check_threshold()


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


# --- Persistent stats storage (Redis-backed, cross-process) ---

_METRICS_KEY_PREFIX = "cachex:m:"
_METRICS_TTL = 3540  # 59 minutes

_flush_thread: Thread | None = None
_flush_stop = Event()
_flush_lock = Lock()
_ops_since_flush = 0
_last_flush_snapshot: dict[str, dict[str, float]] = {}


def _get_metrics_config() -> dict[str, Any]:
    """Read CACHEX metrics config from Django settings."""
    from django.conf import settings

    cachex = getattr(settings, "CACHEX", {})
    return {
        "cache": cachex.get("METRICS_CACHE", ""),
        "flush_interval": cachex.get("METRICS_FLUSH_INTERVAL", 60),
        "flush_threshold": cachex.get("METRICS_FLUSH_THRESHOLD", 1000),
    }


def _get_metrics_client() -> Any | None:
    """Get the raw Redis client for metrics storage."""
    config = _get_metrics_config()
    cache_alias = config["cache"]
    if not cache_alias:
        return None
    try:
        from django.core.cache import caches

        cache = caches[cache_alias]
        return cache.get_client(write=True)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    except Exception:  # noqa: BLE001
        return None


def _ensure_flush_thread() -> None:
    """Start the background flush thread if METRICS_CACHE is configured."""
    global _flush_thread  # noqa: PLW0603
    if _flush_thread is not None and _flush_thread.is_alive():
        return
    config = _get_metrics_config()
    if not config["cache"]:
        return
    with _flush_lock:
        if _flush_thread is not None and _flush_thread.is_alive():
            return
        _flush_stop.clear()
        _flush_thread = Thread(
            target=_flush_loop,
            args=(config["flush_interval"],),
            name="cachex-metrics-flush",
            daemon=True,
        )
        _flush_thread.start()


def _flush_loop(interval: int) -> None:
    """Background loop that flushes stats to Redis periodically."""
    while not _flush_stop.is_set():
        _flush_stop.wait(interval)
        if not _flush_stop.is_set():
            _flush_to_redis()


def _check_threshold() -> None:
    """Check if ops threshold is reached and trigger flush if so."""
    global _ops_since_flush  # noqa: PLW0603
    _ops_since_flush += 1
    config = _get_metrics_config()
    if config["cache"] and _ops_since_flush >= config["flush_threshold"]:
        _ops_since_flush = 0
        _flush_to_redis()


def _flush_to_redis() -> None:
    """Flush current counter deltas to Redis as per-minute hash buckets."""
    global _ops_since_flush, _last_flush_snapshot  # noqa: PLW0603
    if not HAS_PROMETHEUS:
        return
    client = _get_metrics_client()
    if client is None:
        return

    from django.conf import settings

    now = time.time()
    minute_key = _METRICS_KEY_PREFIX + time.strftime("%Y%m%d%H%M", time.localtime(now))

    # Snapshot current counters
    current: dict[str, dict[str, float]] = {}
    for alias in settings.CACHES:
        current[alias] = {
            "h": _counter_value(alias, "get", "hit"),
            "m": _counter_value(alias, "get", "miss"),
            "s": _counter_value(alias, "set", "ok"),
            "d": _counter_value(alias, "delete", "ok"),
        }

    # Compute deltas from last flush
    try:
        pipe = client.pipeline(transaction=False)
        has_deltas = False
        for alias, vals in current.items():
            prev = _last_flush_snapshot.get(alias, {})
            for field_key, field_val in vals.items():
                delta = int(field_val - prev.get(field_key, 0))
                if delta > 0:
                    pipe.hincrby(minute_key, f"{alias}:{field_key}", delta)
                    has_deltas = True
        if has_deltas:
            pipe.expire(minute_key, _METRICS_TTL)
            pipe.execute()
    except Exception:  # noqa: BLE001
        logger.warning("cachex: metrics flush to Redis failed", exc_info=True)

    _last_flush_snapshot = current
    _ops_since_flush = 0


def _parse_minute_hash(data: dict) -> tuple[int, int, int]:
    """Parse a Redis minute hash into (hits, misses, sets) totals."""
    total_h, total_m, total_s = 0, 0, 0
    for raw_field, raw_val in data.items():
        field = raw_field.decode() if isinstance(raw_field, bytes) else str(raw_field)
        val = int(raw_val.decode() if isinstance(raw_val, bytes) else raw_val)
        if field.endswith(":h"):
            total_h += val
        elif field.endswith(":m"):
            total_m += val
        elif field.endswith(":s"):
            total_s += val
    return total_h, total_m, total_s


def get_stored_throughput_json(period: str = "") -> str:
    """Fetch per-minute stats from Redis and format for Chart.js.

    Returns a JSON string with per-minute hits/misses/sets rates, or
    empty string if no stored data is available.
    """
    client = _get_metrics_client()
    if client is None:
        return ""

    now = time.time()
    cutoff_seconds = _PERIOD_SECONDS.get(period, 3600)  # default 1 hour
    cutoff = now - cutoff_seconds

    # Walk minute-by-minute from cutoff to now
    t = cutoff
    minute_keys: list[str] = []
    while t <= now:
        minute_keys.append(_METRICS_KEY_PREFIX + time.strftime("%Y%m%d%H%M", time.localtime(t)))
        t += 60

    if not minute_keys:
        return ""

    # Batch fetch all minute hashes via pipeline
    try:
        pipe = client.pipeline(transaction=False)
        for key in minute_keys:
            pipe.hgetall(key)
        results = pipe.execute()
    except Exception:  # noqa: BLE001
        return ""

    labels: list[str] = []
    hits: list[int] = []
    misses: list[int] = []
    sets: list[int] = []

    for i, data in enumerate(results):
        if not data:
            continue
        total_h, total_m, total_s = _parse_minute_hash(data)
        key_suffix = minute_keys[i][len(_METRICS_KEY_PREFIX) :]
        labels.append(f"{key_suffix[8:10]}:{key_suffix[10:12]}")
        hits.append(total_h)
        misses.append(total_m)
        sets.append(total_s)

    if not labels:
        return ""

    return json.dumps({"labels": labels, "hits": hits, "misses": misses, "sets": sets})
