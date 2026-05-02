"""Spike: Django cache backend backed by ``valkey-glide-sync``."""

from __future__ import annotations

from django_cachex.cache.default import KeyValueCache
from django_cachex.client.glide import ValkeyGlideCacheClient


class ValkeyGlideCache(KeyValueCache):
    """Django cache backend using ``valkey-glide-sync``."""

    _cachex_support = "cachex"
    _class = ValkeyGlideCacheClient


__all__ = ["ValkeyGlideCache"]
