"""Exceptions every adapter's ``cache.lock()`` object raises, whichever it is."""

from django_cachex.exceptions import CachexError


class LockError(CachexError):
    """Raised when a lock operation fails."""


class LockNotOwnedError(LockError):
    """Raised when releasing or extending a lock the caller no longer owns."""
