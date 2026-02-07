from django.apps import AppConfig


class CacheAdminConfig(AppConfig):
    """Django app configuration for the cache admin interface."""

    name = "django_cachex.admin"
    label = "django_cachex"
    verbose_name = "django-cachex"
