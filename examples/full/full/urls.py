"""URL configuration for full example project."""

from django.contrib import admin
from django.urls import path

from full import demo

urlpatterns = [
    path("admin/", admin.site.urls),
    path("demo/pipeline/", demo.pipeline_demo),
    path("demo/async/", demo.async_demo),
    path("demo/apipeline/", demo.apipeline_demo),
    path("demo/lua/", demo.lua_demo),
]
