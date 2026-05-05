"""Experimental cache backends — not part of the public API.

Currently houses :class:`UnifiedValkeyCache`, a spike that routes both sync
and async ``valkey-py`` calls through a single async client running on a
process-wide daemon-thread event loop. See
:mod:`django_cachex.adapters._unified_experiment` for the rationale.
"""

from django_cachex.adapters._unified_experiment import UnifiedValkeyPyAdapter
from django_cachex.cache.resp import RespCache


class UnifiedValkeyCache(RespCache):
    """Spike: ``valkey-py`` with a single async client per process."""

    _adapter_class = UnifiedValkeyPyAdapter


__all__ = ["UnifiedValkeyCache"]
