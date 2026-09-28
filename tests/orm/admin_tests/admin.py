from django.contrib import admin

from tests.orm.admin_tests.models import TestModel


@admin.register(TestModel)
class TestModelAdmin(admin.ModelAdmin):
    list_display = ("name", "owner")
