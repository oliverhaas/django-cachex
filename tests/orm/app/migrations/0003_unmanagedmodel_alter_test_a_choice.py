from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ormtest", "0002_multicolumnrelationmodel"),
    ]

    operations = [
        migrations.CreateModel(
            name="UnmanagedModel",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=50)),
            ],
            options={
                "managed": False,
            },
        ),
        migrations.AlterField(
            model_name="test",
            name="a_choice",
            field=models.CharField(choices=[("foo", "Foo"), ("bar", "Bar")], max_length=3, null=True),
        ),
    ]
