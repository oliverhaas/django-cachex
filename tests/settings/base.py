"""Base Django settings for tests."""

SECRET_KEY = "django_tests_secret_key"

# Include django.contrib.auth and django.contrib.contenttypes for mypy/django-stubs
# See: https://github.com/typeddjango/django-stubs/issues/318
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.messages",
    "django.contrib.sessions",
    "django_cachex.admin",
]

# Database configuration for tests
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    },
}

# Required for admin
ROOT_URLCONF = "tests.settings.urls"

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

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

USE_TZ = False

EMAIL_BACKEND = "django.core.mail.backends.locmem.EmailBackend"

# Base CACHES configuration - overridden by test fixtures for parametrized tests.
# The 'doesnotexist' cache points to an invalid port for testing exception handling.
CACHES = {
    "default": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379?db=1",
        "OPTIONS": {"client_class": "django_cachex.client.DefaultClient"},
    },
    "doesnotexist": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:56379?db=1",
        "OPTIONS": {"client_class": "django_cachex.client.DefaultClient"},
    },
    "with_prefix": {
        "BACKEND": "django_cachex.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379?db=1",
        "OPTIONS": {"client_class": "django_cachex.client.DefaultClient"},
        "KEY_PREFIX": "test-prefix",
    },
}
