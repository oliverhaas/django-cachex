"""Django app configuration for PostgreSQL cache tables."""

from __future__ import annotations

from django.apps import AppConfig


class PostgresCacheConfig(AppConfig):
    name = "django_cachex.postgres"
    label = "cachex_postgres"
    verbose_name = "Django Cachex PostgreSQL"
    default_auto_field = "django.db.models.BigAutoField"
