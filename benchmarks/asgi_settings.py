"""Django settings for the ASGI-bench subprocess.

Reads CACHES from environment variables so the parent benchmark process can
parametrize the backend without writing per-adapter settings modules:

- ``BENCH_CACHE_BACKEND`` — dotted path of the cache backend class.
- ``BENCH_CACHE_LOCATION`` — Redis/Valkey URL.
- ``BENCH_CACHE_OPTIONS_JSON`` — optional, JSON-encoded ``OPTIONS`` dict.
"""

import json
import os

SECRET_KEY = "django_benchmarks_secret_key"  # noqa: S105

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    },
}

USE_TZ = False

ROOT_URLCONF = "benchmarks.urls"

ALLOWED_HOSTS = ["*"]

DEBUG = False

# Match the request-cycle test: minimal middleware so we measure cache work,
# not unrelated framework overhead.
MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
]

CACHES = {
    "default": {
        "BACKEND": os.environ["BENCH_CACHE_BACKEND"],
        "LOCATION": os.environ["BENCH_CACHE_LOCATION"],
        "OPTIONS": json.loads(os.environ.get("BENCH_CACHE_OPTIONS_JSON", "{}")),
    },
}
