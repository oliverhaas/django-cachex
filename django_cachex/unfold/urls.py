from django.urls import path

from . import views

app_name = "django_cachex"

urlpatterns = [
    path("", views.index, name="index"),
    path("help/", views.help_view, name="help"),
    path("<str:cache_name>/keys/", views.key_search, name="key_search"),
    path("<str:cache_name>/keys/<path:key>/", views.key_detail, name="key_detail"),
    path("<str:cache_name>/add/", views.key_add, name="key_add"),
    path("<str:cache_name>/info/", views.cache_info, name="cache_info"),
    path("<str:cache_name>/slowlog/", views.cache_slowlog, name="cache_slowlog"),
]
