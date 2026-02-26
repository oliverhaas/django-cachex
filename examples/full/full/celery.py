"""
Celery application for the full example project.

Uses Redis as both broker and result backend (db 1 on the standalone Redis).
The Celery queue keys show up in the django-cachex admin via the "celery"
cache backend, which points at the same Redis database.
"""

import os

from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "full.settings")

app = Celery("full")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
