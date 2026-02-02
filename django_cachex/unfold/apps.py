from django.apps import AppConfig


class UnfoldCacheAdminConfig(AppConfig):
    """Django app configuration for the unfold-styled cache admin interface."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "django_cachex.unfold"
    label = "django_cachex"
    verbose_name = "django-cachex"
