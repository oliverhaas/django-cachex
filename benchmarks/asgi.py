"""ASGI entrypoint for the ASGI benchmark — granian imports this."""

from __future__ import annotations

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "benchmarks.asgi_settings")
django.setup()

from django.core.asgi import get_asgi_application  # noqa: E402

application = get_asgi_application()
