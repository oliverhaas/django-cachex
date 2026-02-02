"""WSGI config for full example project."""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "full.settings")

application = get_wsgi_application()
