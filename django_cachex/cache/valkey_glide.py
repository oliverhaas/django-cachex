"""Django cache backends backed by ``valkey-glide``.

``valkey-glide`` is the Valkey project's official Rust-cored client. It
ships standalone (single node, optionally with replicas) and cluster
clients but no Sentinel client — Sentinel-managed deployments would have
to discover the master out-of-band and point a standalone backend at it.
"""

from django_cachex.adapters.valkey_glide import (
    ValkeyGlideAdapter,
    ValkeyGlideClusterAdapter,
)
from django_cachex.cache.resp import RespCache, RespClusterCache


class ValkeyGlideCache(RespCache):
    """Django cache backend using ``valkey-glide-sync`` standalone."""

    _cachex_support = "cachex"
    _adapter_class = ValkeyGlideAdapter


class ValkeyGlideClusterCache(RespClusterCache):
    """``valkey-glide`` cluster-mode cache backend.

    Multi-key operations require keys to hash to a single slot — use
    ``{tag}`` hash tags on related keys.
    """

    _cachex_support = "cachex"
    _adapter_class = ValkeyGlideClusterAdapter


__all__ = ["ValkeyGlideCache", "ValkeyGlideClusterCache"]
