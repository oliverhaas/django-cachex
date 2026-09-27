from django.apps import AppConfig


class OrmTestConfig(AppConfig):
    name = "tests.orm.app"
    label = "ormtest"
    default_auto_field = "django.db.models.AutoField"
