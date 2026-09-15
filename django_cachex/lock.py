"""Lock exceptions raised by ``cache.lock()`` and ``cache.alock()`` on every adapter.

The valkey-glide adapter's lock raises these directly. The redis-py and
valkey-py adapters hand back their driver's own ``Lock`` wrapped so that
``redis.exceptions.LockError`` / ``valkey.exceptions.LockError`` (and their
``LockNotOwnedError`` subclasses) surface as the classes below, with the
driver error kept as ``__cause__``.
"""

from django_cachex.exceptions import CachexError


class LockError(CachexError, ValueError):
    """Raised when a lock operation fails.

    Subclasses :class:`ValueError` like ``threading.Lock`` and the driver
    lock errors do, so existing ``except ValueError`` callers keep working.
    """


class LockNotOwnedError(LockError):
    """Raised when releasing or extending a lock the caller no longer owns."""
