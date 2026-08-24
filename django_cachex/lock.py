"""Lock exceptions raised by the valkey-glide adapter's ``cache.lock()`` only.

There is no single catchable base across backends: redis-py and valkey-py
hand back their driver's own ``Lock``, which raises
``redis.exceptions.LockError`` / ``valkey.exceptions.LockError`` (and their
``LockNotOwnedError`` subclasses). Neither subclasses the classes below, so
portable code has to catch all three (see ``tests/cache/test_locks.py``).
"""

from django_cachex.exceptions import CachexError


class LockError(CachexError):
    """Raised when a lock operation fails."""


class LockNotOwnedError(LockError):
    """Raised when releasing or extending a lock the caller no longer owns."""
