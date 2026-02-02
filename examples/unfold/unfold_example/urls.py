"""URL configuration for unfold example project."""

from django.contrib import admin
from django.urls import include, path

from unfold_example import admin as project_admin  # noqa: F401 - registers User/Group with unfold

urlpatterns = [
    # Cache admin URLs with unfold-styled templates
    path("admin/django_cachex/cache/", include("django_cachex.unfold.urls")),
    path("admin/", admin.site.urls),
]
