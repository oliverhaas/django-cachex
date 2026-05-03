"""Sentinel cache backend base class.

Driver-agnostic sentinel behavior; per-driver concrete subclasses live in
:mod:`django_cachex.cache.valkey_py` (``valkey-py``) and
:mod:`django_cachex.cache.redis_py` (``redis-py``).
"""

from django_cachex.cache.default import KeyValueCache


class KeyValueSentinelCache(KeyValueCache):
    """Sentinel cache backend base class.

    Subclasses set ``_adapter_class`` to their specific sentinel adapter.
    """


__all__ = ["KeyValueSentinelCache"]
