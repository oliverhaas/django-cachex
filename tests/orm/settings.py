"""Django settings for the ORM cache tests: ``pytest tests/orm --ds=tests.orm.settings``.

CACHEX_ORM_TEST_DB picks the default database (sqlite or postgresql) and CACHEX_ORM_TEST_CACHE the cache
backend (locmem, redis or tracking). tests/orm/conftest.py starts their containers.
"""

import os
from typing import Any

TEST_DB = os.environ.get("CACHEX_ORM_TEST_DB", "sqlite")
TEST_CACHE = os.environ.get("CACHEX_ORM_TEST_CACHE", "locmem")

SECRET_KEY = "django_tests_secret_key"

INSTALLED_APPS = [
    "django_cachex.orm",
    "tests.orm.app",
    "tests.orm.admin_tests",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.postgres",  # Enables the unaccent lookup.
    "django.contrib.sessions",
    "django.contrib.admin",
    "django.contrib.messages",
]

_SQLITE: dict[str, Any] = {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}
_POSTGRESQL: dict[str, Any] = {
    "ENGINE": "django.db.backends.postgresql",
    "NAME": "orm",
    "USER": "orm",
    "PASSWORD": "orm",
    # The conftest starts a container and fills these in unless they are set.
    "HOST": os.environ.get("CACHEX_ORM_TEST_PG_HOST", ""),
    "PORT": os.environ.get("CACHEX_ORM_TEST_PG_PORT", ""),
}
if TEST_DB not in {"sqlite", "postgresql"}:
    msg = f"CACHEX_ORM_TEST_DB must be 'sqlite' or 'postgresql', not {TEST_DB!r}"
    raise ValueError(msg)

DATABASES: dict[str, dict[str, Any]] = {
    "default": _POSTGRESQL if TEST_DB == "postgresql" else _SQLITE,
    "second": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
}
DATABASE_ROUTERS = ["tests.orm.router.PostgresRouter"]
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

# The query counts in the tests assume nothing is ever culled.
_UNCULLED = {"MAX_ENTRIES": 10**9}
_REDIS_LOCATION = os.environ.get("CACHEX_ORM_TEST_REDIS_URL", "")
if TEST_CACHE == "locmem":
    _default_cache: dict[str, Any] = {"BACKEND": "django_cachex.cache.LocMemCache", "OPTIONS": _UNCULLED}
elif TEST_CACHE == "redis":
    _default_cache = {"BACKEND": "django_cachex.cache.RedisCache", "LOCATION": _REDIS_LOCATION}
elif TEST_CACHE == "tracking":
    _default_cache = {"BACKEND": "django_cachex.cache.TrackingCache", "OPTIONS": {"transport": "transport"}}
else:
    msg = f"CACHEX_ORM_TEST_CACHE must be 'locmem', 'redis' or 'tracking', not {TEST_CACHE!r}"
    raise ValueError(msg)

CACHES: dict[str, dict[str, Any]] = {
    "default": _default_cache,
    "other": {"BACKEND": "django_cachex.cache.LocMemCache", "LOCATION": "other", "OPTIONS": _UNCULLED},
}
if TEST_CACHE == "tracking":
    CACHES["transport"] = {"BACKEND": "django_cachex.cache.RedisCache", "LOCATION": _REDIS_LOCATION}

MIGRATION_MODULES = {"ormtest": "tests.orm.app.migrations"}

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]
PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]
ROOT_URLCONF = "tests.settings.urls"

# Individual tests enable time zones where they need them.
USE_TZ = False
TIME_ZONE = "UTC"
