"""URL configuration for full example project."""

from django.contrib import admin
from django.urls import path

from full.views import generate_traffic

urlpatterns = [
    path("admin/", admin.site.urls),
    path("traffic/", generate_traffic),
]
