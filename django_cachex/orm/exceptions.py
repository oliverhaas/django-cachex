"""Exceptions raised by the ORM cache."""

from django.db import DatabaseError

from django_cachex.exceptions import CachexError


class InvalidationError(CachexError, DatabaseError):
    """The ORM cache could not invalidate the cached queries of some tables."""
