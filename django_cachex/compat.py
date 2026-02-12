"""Utilities for serializer/compressor instantiation."""

from __future__ import annotations

from typing import Any

from django.utils.module_loading import import_string


def create_serializer(config: str | type | Any | None, **kwargs: Any) -> Any:
    """Create a serializer from a dotted path, class, instance, or None (default: pickle)."""
    if config is None:
        config = "django_cachex.serializers.pickle.PickleSerializer"

    if isinstance(config, str):
        config = import_string(config)

    if callable(config):
        return config(**kwargs)

    return config


def create_compressor(config: str | type | Any, **kwargs: Any) -> Any:
    """Create a compressor from a dotted path, class, or instance."""
    if isinstance(config, str):
        config = import_string(config)

    if callable(config):
        return config(**kwargs)

    return config
