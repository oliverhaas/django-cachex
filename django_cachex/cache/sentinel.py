"""Sentinel cache backends for Redis-compatible backends."""

from __future__ import annotations

from django_cachex.adapters.redis_py import RedisSentinelAdapter
from django_cachex.adapters.sentinel import BaseKeyValueSentinelAdapter
from django_cachex.adapters.valkey_py import ValkeySentinelAdapter
from django_cachex.cache.default import KeyValueCache


class KeyValueSentinelCache(KeyValueCache):
    """Sentinel cache backend base class.

    Extends KeyValueCache for sentinel-specific behavior.
    Subclasses set `_adapter_class` class attribute to their specific SentinelCacheClient.
    """

    _adapter_class: type[BaseKeyValueSentinelAdapter] = BaseKeyValueSentinelAdapter


class RedisSentinelCache(KeyValueSentinelCache):
    """Django cache backend for Redis Sentinel high availability (redis-py).

    Failover and service discovery happen through Redis Sentinel; the
    ``LOCATION`` hostname is the Sentinel service name. Raises
    :class:`ImportError` on instantiation if ``redis-py`` isn't installed.
    """

    _adapter_class = RedisSentinelAdapter


class ValkeySentinelCache(KeyValueSentinelCache):
    """Django cache backend for Valkey Sentinel high availability (valkey-py).

    Failover and service discovery happen through Valkey Sentinel; the
    ``LOCATION`` hostname is the Sentinel service name. Raises
    :class:`ImportError` on instantiation if ``valkey-py`` isn't installed.
    """

    _adapter_class = ValkeySentinelAdapter


__all__ = [
    "KeyValueSentinelCache",
    "RedisSentinelCache",
    "ValkeySentinelCache",
]
