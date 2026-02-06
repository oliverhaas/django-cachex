"""URL configuration for unfold example project."""

from django.contrib import admin
from django.urls import path

from unfold_example import admin as project_admin  # noqa: F401 - registers User/Group with unfold

urlpatterns = [
    # Unfold admin uses the same admin URLs as the standard admin
    # The django_cachex.unfold app registers its admin classes which use unfold templates
    path("admin/", admin.site.urls),
]
