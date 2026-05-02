"""Spike: Django cache backend backed by ``valkey-glide-sync``."""

from __future__ import annotations

from django_cachex.adapter.glide import ValkeyGlideAdapter
from django_cachex.cache.default import KeyValueCache


class ValkeyGlideCache(KeyValueCache):
    """Django cache backend using ``valkey-glide-sync``."""

    _cachex_support = "cachex"
    _adapter_class = ValkeyGlideAdapter


__all__ = ["ValkeyGlideCache"]
