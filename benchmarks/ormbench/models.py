"""Tables of the ORM cache benchmark: the rows the timed phases read, the races' counters and the scorecard's cases."""

from django.db import models


class Item(models.Model):
    """The rows the timed phases read and write."""

    name = models.CharField(max_length=40)
    qty = models.IntegerField(default=0)
    description = models.CharField(max_length=200, default="")
    due = models.DateTimeField(null=True)
    data = models.JSONField(null=True)


class Counter(models.Model):
    """A row the races write and read; ``version`` counts the writes."""

    version = models.BigIntegerField(default=0)


class Threshold(models.Model):
    level = models.IntegerField()


class Parent(models.Model):
    pass


class Child(models.Model):
    # Deleted by the database itself, without a statement of Django's.
    parent = models.ForeignKey(Parent, models.DB_CASCADE)


class Note(models.Model):
    text = models.CharField(max_length=40)


class Legacy(models.Model):
    """A table named in mixed case, as tables of older schemas often are."""

    value = models.IntegerField(default=0)

    class Meta:
        db_table = "OrmBench_Legacy"


class Ledger(models.Model):
    """A table the migration creates with raw SQL, so Django does not manage it."""

    amount = models.IntegerField()

    class Meta:
        managed = False
        db_table = "ormbench_ledger"


class NoteCount(models.Model):
    """A materialized view counting the notes."""

    notes = models.IntegerField()

    class Meta:
        managed = False
        db_table = "ormbench_notecount"
