from django.apps import AppConfig


class CacheAdminConfig(AppConfig):
    """Django app configuration for the cache admin interface."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "django_cachex.admin"
    label = "django_cachex"
    verbose_name = "django-cachex"
