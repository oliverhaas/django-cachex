"""
Full example project app configuration.

This AppConfig ensures sample cache data is populated on Django startup.
See startup.py for the data population logic.
"""

# ruff: noqa: T201, BLE001
# T201: print statements are intentional for visibility
# BLE001: broad exception catching is intentional for robustness

import os

from django.apps import AppConfig


class FullExampleConfig(AppConfig):
    """App configuration that populates sample cache data on startup."""

    name = "full"
    verbose_name = "Full Example Project"

    def ready(self) -> None:
        """
        Called when Django starts up.

        We populate sample cache data here to ensure the cache admin
        has data to display. This is especially important for LocMemCache
        which doesn't persist between restarts.

        Note: We check RUN_MAIN to avoid running twice during auto-reload.
        """
        # Only run in the main process (not in the reloader child)
        # During runserver with auto-reload, Django spawns two processes.
        # RUN_MAIN is set to 'true' in the child process that actually serves.
        if os.environ.get("RUN_MAIN") == "true" or not os.environ.get("RUN_MAIN"):
            # Import here to avoid circular imports
            from full.startup import ensure_sample_data

            try:
                ensure_sample_data()
            except Exception as e:
                # Don't crash on startup if cache population fails
                print(f"Warning: Failed to populate sample cache data: {e}")
