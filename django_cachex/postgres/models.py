"""Django models for PostgreSQL cache tables.

These models define the schema for the five tables used by the PostgreSQL cache
backend. The tables are created as UNLOGGED by the initial migration, using
:class:`~django_cachex.postgres.operations.CreateUnloggedModel`.

Usage::

    INSTALLED_APPS = [
        ...,
        "django_cachex.postgres",
    ]

Then run ``manage.py migrate`` to create the tables.

The ``LOCATION`` in your ``CACHES`` setting must match the table prefix
(default ``"cachex"``).
"""

from __future__ import annotations

from django.db import models


class CacheEntry(models.Model):
    """Main key registry — stores string values and type/expiry metadata for all key types."""

    key = models.TextField(primary_key=True)
    type = models.SmallIntegerField(default=0)
    value = models.BinaryField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "cachex"
        indexes = [
            models.Index(
                fields=["expires_at"],
                condition=models.Q(expires_at__isnull=False),
                name="idx_cachex_expires",
            ),
        ]


class CacheHash(models.Model):
    """Hash field storage — one row per (key, field) pair."""

    pk = models.CompositePrimaryKey("key", "field")
    key = models.TextField()
    field = models.TextField()
    value = models.BinaryField()

    class Meta:
        db_table = "cachex_hashes"


class CacheList(models.Model):
    """List item storage — sparse BIGINT positions for O(1) push/pop."""

    pk = models.CompositePrimaryKey("key", "pos")
    key = models.TextField()
    pos = models.BigIntegerField()
    value = models.BinaryField()

    class Meta:
        db_table = "cachex_lists"


class CacheSet(models.Model):
    """Set member storage — one row per (key, member) pair."""

    pk = models.CompositePrimaryKey("key", "member")
    key = models.TextField()
    member = models.BinaryField()

    class Meta:
        db_table = "cachex_sets"


class CacheSortedSet(models.Model):
    """Sorted set storage — one row per (key, member) with a score for ordering."""

    pk = models.CompositePrimaryKey("key", "member")
    key = models.TextField()
    member = models.BinaryField()
    score = models.FloatField()

    class Meta:
        db_table = "cachex_zsets"
        indexes = [
            models.Index(fields=["key", "score"], name="idx_cachex_zsets_score"),
        ]
