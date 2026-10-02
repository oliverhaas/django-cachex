import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("auth", "0012_alter_user_first_name_max_length"),
        ("contenttypes", "0002_remove_content_type_name"),
        ("ormtest", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="MultiColumnRelationModel",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codename", models.CharField(max_length=100)),
                (
                    "content_type",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="contenttypes.contenttype"),
                ),
                (
                    "permission",
                    models.ForeignObject(
                        from_fields=("content_type", "codename"),
                        on_delete=django.db.models.deletion.CASCADE,
                        to="auth.permission",
                        to_fields=("content_type", "codename"),
                    ),
                ),
            ],
        ),
    ]
