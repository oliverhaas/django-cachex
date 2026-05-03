"""URL config + views for the request-cycle benchmark.

Each view does exactly one cache operation matching one of the seven
benchmark phases. The runner drives them via ``django.test.Client``, which
exercises the full WSGI handler — middleware, URL resolution, request/response
construction, ``request_started`` / ``request_finished`` signals — so the
numbers reflect what a real Django request paying for the same cache work
looks like, not just the cache call in isolation.
"""

from typing import Any

from django.core.cache import cache
from django.http import HttpResponse
from django.urls import path

from benchmarks.runner import MGET_BATCH, WARMUP_KEYS, _build_payload, _build_payload_large

_PAYLOAD: Any = _build_payload()


def set_payload_kind(kind: str) -> None:
    """Set the payload used by ``set`` / ``mset`` views. Called once per benchmark run."""
    global _PAYLOAD  # noqa: PLW0603 — module-level state intentional for benchmark setup
    _PAYLOAD = _build_payload_large() if kind == "large" else _build_payload()


def get_view(_request: Any, i: int) -> HttpResponse:
    cache.get(f"warm:{i % WARMUP_KEYS}")
    return HttpResponse(b"", status=204)


def get_miss_view(_request: Any, i: int) -> HttpResponse:
    cache.get(f"miss:{i}")
    return HttpResponse(b"", status=204)


def set_view(_request: Any, i: int) -> HttpResponse:
    cache.set(f"set:{i}", _PAYLOAD)
    return HttpResponse(b"", status=204)


def mget_view(_request: Any, i: int) -> HttpResponse:
    cache.get_many([f"warm:{j}" for j in range(MGET_BATCH)])
    return HttpResponse(b"", status=204)


def mset_view(_request: Any, i: int) -> HttpResponse:
    cache.set_many({f"mset:{j}": _PAYLOAD for j in range(MGET_BATCH)})
    return HttpResponse(b"", status=204)


def incr_view(_request: Any, i: int) -> HttpResponse:
    cache.incr("counter")
    return HttpResponse(b"", status=204)


def delete_view(_request: Any, i: int) -> HttpResponse:
    cache.delete(f"del:{i}")
    return HttpResponse(b"", status=204)


# ASGI-bench views. ``bench_mixed`` does six async cache ops per request,
# matching django-vcache's ``bench_compare.py`` workload so the numbers are
# comparable. ``bench_seed`` populates the keys ``bench_mixed`` reads.

_BENCH_SMALL: dict[str, Any] = {"user": "alice", "role": "admin", "ts": 1709900000}
_BENCH_LARGE: dict[str, Any] = {
    "event": {
        "id": "abc123",
        "message": "NullPointerException in com.example.App",
        "level": "error",
        "platform": "java",
        "tags": {f"tag_{i}": f"value_{i}" for i in range(30)},
        "extra": {f"key_{i}": "x" * 50 for i in range(20)},
    },
}


async def bench_seed(_request: Any) -> HttpResponse:
    await cache.aset("bench:s1", _BENCH_SMALL, 300)
    await cache.aset("bench:s2", _BENCH_SMALL, 300)
    await cache.aset("bench:s3", _BENCH_SMALL, 300)
    await cache.aset("bench:large", _BENCH_LARGE, 300)
    await cache.aset("bench:counter", 0, 300)
    return HttpResponse(b"seeded", status=200)


async def bench_mixed(_request: Any) -> HttpResponse:
    """Six async cache ops per request — matches django-vcache's workload."""
    await cache.aget("bench:s1")  # 1. small get
    await cache.aget_many(["bench:s1", "bench:s2", "bench:s3"])  # 2. batch get
    await cache.aset("bench:s1", _BENCH_SMALL, 300)  # 3. small set
    await cache.aset("bench:large", _BENCH_LARGE, 300)  # 4. large set
    await cache.aincr("bench:counter")  # 5. atomic incr
    await cache.aget("bench:large")  # 6. large get
    return HttpResponse(b"", status=204)


urlpatterns = [
    path("bench/get/<int:i>/", get_view),
    path("bench/get-miss/<int:i>/", get_miss_view),
    path("bench/set/<int:i>/", set_view),
    path("bench/mget/<int:i>/", mget_view),
    path("bench/mset/<int:i>/", mset_view),
    path("bench/incr/<int:i>/", incr_view),
    path("bench/delete/<int:i>/", delete_view),
    path("bench/seed/", bench_seed),
    path("bench/mixed/", bench_mixed),
]
