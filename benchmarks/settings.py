"""Minimal Django settings for benchmark runs."""

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

# Placeholder; benchmark tests override this per parametrization.
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    },
}
