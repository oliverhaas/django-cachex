"""Concrete cache backends for the ``valkey-py`` driver.

Three topologies live here, each a one-line subclass that swaps in the
matching ``valkey-py`` adapter:

- :class:`ValkeyCache` — single-node / replicated.
- :class:`ValkeySentinelCache` — Sentinel-managed primary/replicas.
- :class:`ValkeyClusterCache` — Cluster mode.

See :mod:`django_cachex.cache.redis_py` for the parallel set built on
``redis-py`` (the two libraries share an API surface).
"""

from django_cachex.adapters.valkey_py import (
    ValkeyPyAdapter,
    ValkeyPyClusterAdapter,
    ValkeyPySentinelAdapter,
)
from django_cachex.cache.resp import RespCache, RespClusterCache, RespSentinelCache


class ValkeyCache(RespCache):
    """Django cache backend using the ``valkey-py`` library."""

    _adapter_class = ValkeyPyAdapter


class ValkeySentinelCache(RespSentinelCache):
    """Django cache backend for Valkey Sentinel high availability (valkey-py).

    Failover and service discovery happen through Valkey Sentinel; the
    ``LOCATION`` hostname is the Sentinel service name. Raises
    :class:`ImportError` on instantiation if ``valkey-py`` isn't installed.
    """

    _adapter_class = ValkeyPySentinelAdapter


class ValkeyClusterCache(RespClusterCache):
    """Django cache backend for Valkey Cluster mode (valkey-py).

    Keys are sharded across nodes by hash slot. Raises :class:`ImportError`
    on instantiation if ``valkey-py`` isn't installed.
    """

    _adapter_class = ValkeyPyClusterAdapter


__all__ = [
    "ValkeyCache",
    "ValkeyClusterCache",
    "ValkeySentinelCache",
]
