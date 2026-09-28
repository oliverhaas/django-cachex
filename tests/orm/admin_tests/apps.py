from django.apps import AppConfig


class AdminTestsConfig(AppConfig):
    name = "tests.orm.admin_tests"
    label = "admin_tests"
    default_auto_field = "django.db.models.AutoField"
