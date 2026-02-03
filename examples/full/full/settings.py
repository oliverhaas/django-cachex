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
    # -------------------------------------------------------------------------
    # CLUSTER BACKEND
    # Note: ValkeyClusterCache unavailable due to upstream bug, use RedisClusterCache
    # -------------------------------------------------------------------------
    "cluster": {
        "BACKEND": "django_cachex.client.cluster.RedisClusterCache",
        "LOCATION": "redis://127.0.0.1:7001",
        "KEY_PREFIX": "cluster",
    },
    # -------------------------------------------------------------------------
    # SENTINEL BACKEND
    # Note: ValkeySentinelCache unavailable due to upstream bug, use RedisSentinelCache
    # -------------------------------------------------------------------------
    "sentinel": {
        "BACKEND": "django_cachex.client.sentinel.RedisSentinelCache",
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

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
