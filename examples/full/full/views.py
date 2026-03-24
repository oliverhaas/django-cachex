"""Traffic generator view for testing cache metrics."""

# ruff: noqa: BLE001, S110, ANN001, ANN201

import random
import uuid

from django.conf import settings
from django.core.cache import caches
from django.http import JsonResponse

# Skip caches that shouldn't receive random traffic
_SKIP = {"dummy", "celery", "sync_transport"}


def generate_traffic(request):
    """Generate random cache traffic across all configured caches.

    Usage: GET /traffic/?n=100
    """
    n = min(int(request.GET.get("n", 100)), 10000)
    aliases = [a for a in settings.CACHES if a not in _SKIP]
    ops: dict[str, dict[str, int]] = {a: {"hit": 0, "miss": 0, "set": 0, "delete": 0} for a in aliases}

    # Pre-populate some keys so we get hits
    for alias in aliases:
        try:
            cache = caches[alias]
            for i in range(20):
                cache.set(f"traffic:{i}", f"value-{uuid.uuid4().hex[:8]}", timeout=300)
        except Exception:
            pass

    for _ in range(n):
        alias = random.choice(aliases)  # noqa: S311
        try:
            cache = caches[alias]
            op = random.choices(["get", "set", "delete"], weights=[60, 30, 10])[0]  # noqa: S311
            key = f"traffic:{random.randint(0, 50)}"  # noqa: S311

            if op == "get":
                result = cache.get(key)
                ops[alias]["hit" if result is not None else "miss"] += 1
            elif op == "set":
                cache.set(key, f"val-{uuid.uuid4().hex[:8]}", timeout=300)
                ops[alias]["set"] += 1
            else:
                cache.delete(key)
                ops[alias]["delete"] += 1
        except Exception:
            pass

    total = sum(sum(v.values()) for v in ops.values())
    return JsonResponse({"generated": n, "total_ops": total, "per_cache": ops})
