"""Utilities for serializer/compressor instantiation."""

from __future__ import annotations

from typing import Any

from django.utils.module_loading import import_string


def is_serializer_instance(obj: Any) -> bool:
    """Check if an object is a serializer instance (has dumps/loads methods)."""
    if isinstance(obj, type):
        return False
    return hasattr(obj, "dumps") and hasattr(obj, "loads") and callable(obj.dumps) and callable(obj.loads)


def is_compressor_instance(obj: Any) -> bool:
    """Check if an object is a compressor instance (has compress/decompress methods)."""
    if isinstance(obj, type):
        return False
    return (
        hasattr(obj, "compress") and hasattr(obj, "decompress") and callable(obj.compress) and callable(obj.decompress)
    )


def create_serializer(config: str | type | Any | None, **kwargs: Any) -> Any:
    """Create a serializer instance from config.

    Args:
        config: A dotted path string, a class, an instance, or None for default pickle
        **kwargs: Keyword arguments to pass to serializer constructor
    """
    # None means default pickle serializer
    if config is None:
        config = "django_cachex.serializers.pickle.PickleSerializer"

    # Already an instance
    if is_serializer_instance(config):
        return config

    # A class (not a string path)
    if isinstance(config, type):
        return config(**kwargs)

    # Dotted path string
    cls = import_string(config)
    return cls(**kwargs)


def create_compressor(config: str | type | Any | None, **kwargs: Any) -> Any:
    """Create a compressor instance from config.

    Args:
        config: A dotted path string, a class, an instance, or None for identity compressor
        **kwargs: Keyword arguments to pass to compressor constructor
    """
    # None means identity compressor (no compression)
    if config is None:
        config = "django_cachex.compressors.identity.IdentityCompressor"

    # Already an instance
    if is_compressor_instance(config):
        return config

    # A class (not a string path)
    if isinstance(config, type):
        return config(**kwargs)

    # Dotted path string
    cls = import_string(config)
    return cls(**kwargs)
