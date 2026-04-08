"""Initial migration — creates the five PostgreSQL cache tables as UNLOGGED."""

import django.db.models
from django.db import migrations, models

from django_cachex.postgres.operations import CreateUnloggedModel


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        CreateUnloggedModel(
            name="CacheEntry",
            fields=[
                ("key", models.TextField(primary_key=True, serialize=False)),
                ("type", models.SmallIntegerField(default=0)),
                ("value", models.BinaryField(blank=True, null=True)),
                ("expires_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "cachex",
                "indexes": [
                    models.Index(
                        fields=["expires_at"],
                        condition=django.db.models.Q(expires_at__isnull=False),
                        name="idx_cachex_expires",
                    ),
                ],
            },
        ),
        CreateUnloggedModel(
            name="CacheHash",
            fields=[
                ("pk", models.CompositePrimaryKey("key", "field")),
                ("key", models.TextField()),
                ("field", models.TextField()),
                ("value", models.BinaryField()),
            ],
            options={
                "db_table": "cachex_hashes",
            },
        ),
        CreateUnloggedModel(
            name="CacheList",
            fields=[
                ("pk", models.CompositePrimaryKey("key", "pos")),
                ("key", models.TextField()),
                ("pos", models.BigIntegerField()),
                ("value", models.BinaryField()),
            ],
            options={
                "db_table": "cachex_lists",
            },
        ),
        CreateUnloggedModel(
            name="CacheSet",
            fields=[
                ("pk", models.CompositePrimaryKey("key", "member")),
                ("key", models.TextField()),
                ("member", models.BinaryField()),
            ],
            options={
                "db_table": "cachex_sets",
            },
        ),
        CreateUnloggedModel(
            name="CacheSortedSet",
            fields=[
                ("pk", models.CompositePrimaryKey("key", "member")),
                ("key", models.TextField()),
                ("member", models.BinaryField()),
                ("score", models.FloatField()),
            ],
            options={
                "db_table": "cachex_zsets",
                "indexes": [
                    models.Index(fields=["key", "score"], name="idx_cachex_zsets_score"),
                ],
            },
        ),
    ]
