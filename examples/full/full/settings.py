"""
Django settings for full example project.

Comprehensive setup showcasing ALL supported cache backends:
- Standalone: Valkey, Redis
- Cluster: Redis Cluster (6 nodes)
- Sentinel: Redis Sentinel (1 master + 2 replicas + 3 sentinels)
- Django builtins: LocMem, Database, File, Dummy
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


# Pass-through key functions for viewing raw Redis keys (e.g. Celery broker).
# Django's default key_func adds a "prefix:version:" wrapper around every key,
# but Celery writes bare keys like "celery" and "_kombu.binding.celery".
# These functions bypass the prefix so the admin sees (and operates on) the
# actual keys that Celery created.
def passthrough_key(key: str, prefix: str, version: int) -> str:  # noqa: ARG001
    return key


def passthrough_reverse_key(key: str) -> str:
    return key


SECRET_KEY = "django-insecure-example-key-do-not-use-in-production"  # noqa: S105

DEBUG = True

ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    # django-cachex cache admin
    "django_cachex.admin",
    # Example app (populates sample cache data on startup - see startup.py)
    "full.apps.FullExampleConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "full.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "full.wsgi.application"

# SQLite for simplicity
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    },
}

# =============================================================================
# CACHE CONFIGURATION - All supported backends
# =============================================================================

CACHES = {
    # -------------------------------------------------------------------------
    # STANDALONE BACKENDS
    # -------------------------------------------------------------------------
    "default": {
        "BACKEND": "django_cachex.cache.ValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6381/0",
        "KEY_PREFIX": "valkey",
    },
    "redis": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6380/0",
        "KEY_PREFIX": "redis",
    },
    # Celery broker + result backend (Redis db 1, same server as "redis" cache).
    # Uses pass-through key functions so the admin sees raw Celery keys
    # (celery, _kombu.binding.*, celery-task-meta-*) without prefix mangling.
    "celery": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6380/1",
        "KEY_FUNCTION": "full.settings.passthrough_key",
        "OPTIONS": {
            "reverse_key_function": "full.settings.passthrough_reverse_key",
        },
    },
    # -------------------------------------------------------------------------
    # CLUSTER BACKEND
    # Note: ValkeyClusterCache unavailable due to upstream bug, use RedisClusterCache
    # -------------------------------------------------------------------------
    "cluster": {
        "BACKEND": "django_cachex.cache.cluster.RedisClusterCache",
        "LOCATION": "redis://127.0.0.1:7001",
        "KEY_PREFIX": "cluster",
    },
    # -------------------------------------------------------------------------
    # SENTINEL BACKEND
    # Note: ValkeySentinelCache unavailable due to upstream bug, use RedisSentinelCache
    # -------------------------------------------------------------------------
    "sentinel": {
        "BACKEND": "django_cachex.cache.sentinel.RedisSentinelCache",
        "LOCATION": "redis://mymaster/0",
        "KEY_PREFIX": "sentinel",
        "OPTIONS": {
            "sentinels": [
                ("127.0.0.1", 26379),
                ("127.0.0.1", 26380),
                ("127.0.0.1", 26381),
            ],
        },
    },
    # -------------------------------------------------------------------------
    # DJANGO BUILTIN BACKENDS (for comparison/wrapped support)
    # -------------------------------------------------------------------------
    "locmem": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "full-example-locmem",
    },
    "database": {
        "BACKEND": "django.core.cache.backends.db.DatabaseCache",
        "LOCATION": "django_cache_table",
    },
    "file": {
        "BACKEND": "django.core.cache.backends.filebased.FileBasedCache",
        "LOCATION": BASE_DIR / "cache_files",
    },
    "dummy": {
        "BACKEND": "django.core.cache.backends.dummy.DummyCache",
    },
}

# =============================================================================
# CELERY CONFIGURATION
# =============================================================================
# Uses Redis standalone (port 6380) db 1 for both broker and results.
# Separate from the "redis" cache (db 0) so Celery keys don't mix with cache data.
# The "celery" CACHES entry above points at the same db so the admin can browse
# queue lists, binding sets, and task-result strings.

CELERY_BROKER_URL = "redis://127.0.0.1:6380/1"
CELERY_RESULT_BACKEND = "redis://127.0.0.1:6380/1"
CELERY_RESULT_EXPIRES = 3600  # keep results for 1 hour
CELERY_TASK_TRACK_STARTED = True  # track STARTED state for visibility

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
