"""Django settings for the processes of the ORM cache benchmark, read from the environment the parent sets."""

import json
import os
from typing import Any

from benchmarks.configs import ADAPTER_BY_ID

CONTENDER = os.environ.get("BENCH_ORM_CONTENDER", "none")
LIBRARY, _, VARIANT = CONTENDER.partition("+")
if LIBRARY not in {"none", "cachalot", "cachex"} or VARIANT not in {"", "tracking"}:
    msg = f"BENCH_ORM_CONTENDER must be none, cachalot, cachex, cachalot+tracking or cachex+tracking, not {CONTENDER!r}"
    raise ValueError(msg)

SECRET_KEY = "django_benchmarks_secret_key"  # noqa: S105

INSTALLED_APPS = ["benchmarks.ormbench"]
if LIBRARY == "cachalot":
    INSTALLED_APPS.append("cachalot")
elif LIBRARY == "cachex":
    INSTALLED_APPS.append("django_cachex.orm")

DATABASES: dict[str, dict[str, Any]] = {
    "default": {"ENGINE": "django.db.backends.postgresql", **json.loads(os.environ.get("BENCH_ORM_PG_JSON", "{}"))},
}
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"
USE_TZ = True
TIME_ZONE = "UTC"

# Both ORM caches run on the same backend, client and parser, so only the ORM layer differs.
_adapter = ADAPTER_BY_ID["valkey-py+libvalkey"]
_VALKEY: dict[str, Any] = {
    "BACKEND": _adapter.backend,
    "LOCATION": os.environ.get("BENCH_ORM_VALKEY_URL", ""),
    "OPTIONS": dict(_adapter.options),
}
if VARIANT == "tracking":
    CACHES = {
        "default": {"BACKEND": "django_cachex.cache.TrackingCache", "OPTIONS": {"transport": "transport"}},
        "transport": _VALKEY,
    }
else:
    CACHES = {"default": _VALKEY}

# One lifetime for both: cachalot keeps results for ever by default, the ORM cache for the cache's default timeout.
CACHALOT_TIMEOUT = 3600
CACHEX_ORM = {"TIMEOUT": 3600}
