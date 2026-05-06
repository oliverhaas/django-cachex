"""Runnable demos for django-cachex features.

Each view exercises one feature against the ``default`` cache. Hit them with
curl after ``./run.sh server`` to see the output:

    curl http://127.0.0.1:8000/demo/pipeline/
    curl http://127.0.0.1:8000/demo/async/
    curl http://127.0.0.1:8000/demo/lua/
    curl http://127.0.0.1:8000/demo/apipeline/
"""

# Django wires `request` into every view; many demos here don't read it.
# ruff: noqa: ARG001

from __future__ import annotations

from django.core.cache import caches
from django.http import HttpRequest, JsonResponse


def pipeline_demo(request: HttpRequest) -> JsonResponse:
    """Batch four ops in a single round trip via the sync pipeline."""
    cache = caches["default"]
    with cache.pipeline() as pipe:
        pipe.set("demo:a", 1)
        pipe.set("demo:b", 2)
        pipe.incr("demo:a")
        pipe.get("demo:a")
        results = pipe.execute()
    return JsonResponse({"results": [str(r) for r in results]})


async def async_demo(request: HttpRequest) -> JsonResponse:
    """Read/write through the async surface — only useful under ASGI."""
    cache = caches["default"]
    await cache.aset("demo:async", "hello")
    value = await cache.aget("demo:async")
    return JsonResponse({"value": value})


async def apipeline_demo(request: HttpRequest) -> JsonResponse:
    """Async pipeline: queueing is sync, only execute() awaits."""
    cache = caches["default"]
    async with await cache.apipeline() as pipe:
        pipe.set("demo:p1", "x")
        pipe.set("demo:p2", "y")
        pipe.get("demo:p1")
        pipe.get("demo:p2")
        results = await pipe.execute()
    return JsonResponse({"results": [str(r) for r in results]})


_INCR_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current == false then current = 0 end
local next = tonumber(current) + tonumber(ARGV[1])
redis.call('SET', KEYS[1], next)
return next
"""


def lua_demo(request: HttpRequest) -> JsonResponse:
    """Atomic increment-by-N implemented as a Lua script."""
    cache = caches["default"]
    delta = int(request.GET.get("by", "1"))
    new_value = cache.eval_script(
        _INCR_SCRIPT,
        keys=("demo:lua_counter",),
        args=(delta,),
    )
    return JsonResponse({"counter": new_value})
