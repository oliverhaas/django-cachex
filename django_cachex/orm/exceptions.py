"""Exceptions raised by the ORM cache."""

from django.db import DatabaseError


class InvalidationError(DatabaseError):
    """The ORM cache could not invalidate the cached queries of some tables."""
