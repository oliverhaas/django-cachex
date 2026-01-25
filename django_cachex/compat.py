"""Utilities for serializer/compressor instantiation."""

from __future__ import annotations

from typing import Any

from django.utils.module_loading import import_string


def create_serializer(config: str | type | Any | None, **kwargs: Any) -> Any:
    """Create a serializer instance from config.

    Args:
        config: A dotted path string, a class, an instance, or None for default pickle
        **kwargs: Keyword arguments to pass to serializer constructor
    """
    if config is None:
        config = "django_cachex.serializers.pickle.PickleSerializer"

    if isinstance(config, str):
        config = import_string(config)

    if callable(config):
        return config(**kwargs)

    return config


def create_compressor(config: str | type | Any, **kwargs: Any) -> Any:
    """Create a compressor instance from config.

    Args:
        config: A dotted path string, a class, or an instance
        **kwargs: Keyword arguments to pass to compressor constructor
    """
    if isinstance(config, str):
        config = import_string(config)

    if callable(config):
        return config(**kwargs)

    return config
