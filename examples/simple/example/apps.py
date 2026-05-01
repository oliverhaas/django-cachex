"""Example project app configuration.

Populates sample cache data on Django startup so the cache admin has
something to display (especially for LocMemCache, which doesn't persist
between restarts). See startup.py for the population logic.
"""

# ruff: noqa: T201, BLE001
# T201: print statements are intentional for visibility
# BLE001: broad exception catching is intentional for robustness

from django.apps import AppConfig


class ExampleConfig(AppConfig):
    name = "example"
    verbose_name = "Example Project"

    def ready(self) -> None:
        from example.startup import ensure_sample_data

        try:
            ensure_sample_data()
        except Exception as e:
            print(f"Warning: Failed to populate sample cache data: {e}")
