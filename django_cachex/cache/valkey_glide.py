"""Spike: Django cache backend backed by ``valkey-glide-sync``."""

from django_cachex.adapters.valkey_glide import ValkeyGlideAdapter
from django_cachex.cache.resp import RespCache


class ValkeyGlideCache(RespCache):
    """Django cache backend using ``valkey-glide-sync``."""

    _cachex_support = "cachex"
    _adapter_class = ValkeyGlideAdapter


__all__ = ["ValkeyGlideCache"]
